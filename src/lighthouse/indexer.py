from __future__ import annotations

import logging
import os
import queue
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from typing import Callable

from .db import connect, tx

from .embeddings import ClipEmbedder
from .exif import get_date_taken
from .filetypes import is_supported_image
from .scanner import iter_images_recursive
from .thumbnails import ensure_thumbnail

# Use uvicorn's error logger so messages show up in the server console by default.
logger = logging.getLogger("uvicorn.error")


@dataclass(frozen=True)
class IndexTask:
    photo_path: Path
    root_id: int
    root_path: Path


@dataclass(frozen=True)
class ScanTask:
    root_id: int
    root_path: Path


@dataclass(frozen=True)
class IndexerStats:
    scan_queue_size: int
    ingest_queue_size: int
    active_scan_root_id: Optional[int]
    active_scan_root_path: Optional[str]
    active_scan_current_path: Optional[str]
    active_scan_found: int
    active_scan_enqueued: int
    active_scan_processed: int
    active_scan_started_at: Optional[float]
    active_ingest_path: Optional[str]
    last_ingested_at: Optional[float]
    failed_total: int
    last_failed_path: Optional[str]
    last_failed_at: Optional[float]
    last_failed_error: Optional[str]
    last_scan_root_id: Optional[int]
    last_scan_root_path: Optional[str]
    last_scan_found: int
    last_scan_enqueued: int
    last_scan_processed: int
    last_scan_started_at: Optional[float]
    last_scan_ended_at: Optional[float]
    last_scan_had_errors: bool
    last_scan_wave_roots: int
    last_scan_wave_found: int
    last_scan_wave_enqueued: int
    last_scan_wave_started_at: Optional[float]
    last_scan_wave_ended_at: Optional[float]
    last_scan_wave_had_errors: bool


class PhotoIndexer:
    def __init__(
        self,
        *,
        db_path: Path,
        thumbs_dir: Path,
        get_embedder: Callable[[], Optional[ClipEmbedder]],
        get_vector_index: Callable[[], Optional[object]],
    ) -> None:
        self.db_path = db_path
        self.thumbs_dir = thumbs_dir
        self._get_embedder = get_embedder
        self._get_vector_index = get_vector_index

        # SQLite connections must not be used across threads. Keep one connection
        # per thread (scanner, ingest worker, catchup thread).
        self._db_local = threading.local()
        self._db_conns_lock = threading.Lock()
        self._db_conns: list[sqlite3.Connection] = []

        ingest_max = int(os.environ.get("LIGHTHOUSE_INGEST_QUEUE_MAX", "3000"))
        scan_max = int(os.environ.get("LIGHTHOUSE_SCAN_QUEUE_MAX", "32"))
        self._q: queue.Queue[IndexTask] = queue.Queue(maxsize=max(1, ingest_max))
        self._scan_q: queue.Queue[ScanTask] = queue.Queue(maxsize=max(1, scan_max))
        self._stop = threading.Event()
        self._worker = threading.Thread(target=self._run, name="lighthouse-indexer", daemon=True)
        self._scanner = threading.Thread(target=self._run_scans, name="lighthouse-scanner", daemon=True)
        self._last_persist = 0.0
        self._persist_interval_s = 5.0
        self._stats_lock = threading.Lock()
        self._active_scan: Optional[ScanTask] = None
        self._active_ingest: Optional[IndexTask] = None
        self._last_ingested_at: Optional[float] = None
        self._recent_ingest: list[dict[str, object]] = []
        self._recent_max = 50
        self._scan_progress: dict[int, dict[str, object]] = {}
        self._last_root_error_at: dict[int, float] = {}
        self._failed_total = 0
        self._last_failed: Optional[dict[str, object]] = None
        self._last_scan_summary: Optional[dict[str, object]] = None
        self._scan_wave: Optional[dict[str, object]] = None
        self._last_scan_wave_summary: Optional[dict[str, object]] = None

    def _record_failure(self, *, root_id: int, path: Path, error: str) -> None:
        with self._stats_lock:
            self._failed_total += 1
            self._last_failed = {
                "ts": time.time(),
                "path": str(path),
                "root_id": int(root_id),
                "error": error,
            }

    def _conn(self) -> sqlite3.Connection:
        conn = getattr(self._db_local, "conn", None)
        if conn is None:
            conn = connect(self.db_path, check_same_thread=True)
            self._db_local.conn = conn
            with self._db_conns_lock:
                self._db_conns.append(conn)
        return conn

    def start(self) -> None:
        self._worker.start()
        self._scanner.start()

    def stop(self, timeout_s: float = 2.0) -> None:
        self._stop.set()
        self._worker.join(timeout=timeout_s)
        self._scanner.join(timeout=timeout_s)
        vector_index = self._get_vector_index()
        if vector_index:
            try:
                vector_index.persist()
            except Exception:
                pass
        with self._db_conns_lock:
            conns = list(self._db_conns)
        for c in conns:
            try:
                c.close()
            except Exception:
                pass

    def enqueue(self, task: IndexTask) -> None:
        while not self._stop.is_set():
            try:
                self._q.put(task, timeout=0.25)
                return
            except queue.Full:
                continue

    def enqueue_scan_root(self, root_id: int, root_path: Path) -> None:
        task = ScanTask(root_id=root_id, root_path=root_path)
        while not self._stop.is_set():
            try:
                self._scan_q.put(task, timeout=0.25)
                return
            except queue.Full:
                continue

    def enqueue_missing_embeddings(self, model_id: str) -> int:
        """
        Enqueue indexing tasks for photos missing embeddings for `model_id`.
        Uses DB paths (no filesystem walk) and is throttled by the bounded ingest queue.
        """
        enqueued = 0
        try:
            conn = self._conn()
            rows = conn.execute(
                """
                SELECT p.id AS photo_id, p.path AS photo_path, p.root_id AS root_id, r.path AS root_path
                FROM photos p
                JOIN tracked_roots r ON r.id = p.root_id
                WHERE p.embedding_model IS NULL OR p.embedding_model != ?
                ORDER BY p.id
                """,
                (model_id,),
            ).fetchall()
            for r in rows:
                # Avoid blocking the caller (e.g., FastAPI startup) when the ingest
                # queue is full. A background "catchup" loop should call this
                # periodically to keep feeding the queue in small batches.
                task = IndexTask(
                    photo_path=Path(r["photo_path"]),
                    root_id=int(r["root_id"]),
                    root_path=Path(r["root_path"]),
                )
                try:
                    self._q.put_nowait(task)
                    enqueued += 1
                except queue.Full:
                    break
        except Exception:
            logger.exception("Failed to enqueue missing embeddings for %s", model_id)
        return enqueued

    def stats(self) -> IndexerStats:
        with self._stats_lock:
            scan = self._active_scan
            ingest = self._active_ingest
            # When scan enumeration finishes, ingest can still be running for that root.
            # Keep reporting the last scan's progress while ingest catches up.
            focus_root_id = scan.root_id if scan else (ingest.root_id if ingest else None)
            scan_prog = self._scan_progress.get(int(focus_root_id), {}) if focus_root_id is not None else {}
            focus_root_path = scan.root_path if scan else (ingest.root_path if ingest else None)
            last_failed = self._last_failed or {}
            last_scan = self._last_scan_summary or {}
            last_wave = self._last_scan_wave_summary or {}
            return IndexerStats(
                scan_queue_size=int(self._scan_q.qsize()),
                ingest_queue_size=int(self._q.qsize()),
                active_scan_root_id=int(focus_root_id) if focus_root_id is not None else None,
                active_scan_root_path=str(focus_root_path) if focus_root_path else None,
                active_scan_current_path=scan_prog.get("current_path") if focus_root_id is not None else None,
                active_scan_found=int(scan_prog.get("found", 0) or 0) if focus_root_id is not None else 0,
                active_scan_enqueued=int(scan_prog.get("enqueued", 0) or 0) if focus_root_id is not None else 0,
                active_scan_processed=int(scan_prog.get("processed", 0) or 0) if focus_root_id is not None else 0,
                active_scan_started_at=float(scan_prog.get("started_at") or 0.0)
                if focus_root_id is not None
                else None,
                active_ingest_path=str(ingest.photo_path) if ingest else None,
                last_ingested_at=self._last_ingested_at,
                failed_total=int(self._failed_total),
                last_failed_path=str(last_failed.get("path")) if last_failed.get("path") else None,
                last_failed_at=float(last_failed.get("ts")) if last_failed.get("ts") else None,
                last_failed_error=str(last_failed.get("error")) if last_failed.get("error") else None,
                last_scan_root_id=int(last_scan["root_id"]) if last_scan.get("root_id") is not None else None,
                last_scan_root_path=str(last_scan.get("root_path")) if last_scan.get("root_path") else None,
                last_scan_found=int(last_scan.get("found", 0) or 0),
                last_scan_enqueued=int(last_scan.get("enqueued", 0) or 0),
                last_scan_processed=int(last_scan.get("processed", 0) or 0),
                last_scan_started_at=float(last_scan.get("started_at")) if last_scan.get("started_at") else None,
                last_scan_ended_at=float(last_scan.get("ended_at")) if last_scan.get("ended_at") else None,
                last_scan_had_errors=bool(last_scan.get("had_errors")),
                last_scan_wave_roots=int(last_wave.get("roots", 0) or 0),
                last_scan_wave_found=int(last_wave.get("found", 0) or 0),
                last_scan_wave_enqueued=int(last_wave.get("enqueued", 0) or 0),
                last_scan_wave_started_at=float(last_wave.get("started_at")) if last_wave.get("started_at") else None,
                last_scan_wave_ended_at=float(last_wave.get("ended_at")) if last_wave.get("ended_at") else None,
                last_scan_wave_had_errors=bool(last_wave.get("had_errors")),
            )

    def recent_activity(self, limit: int = 50) -> list[dict[str, object]]:
        limit = max(1, min(int(limit), self._recent_max))
        with self._stats_lock:
            return list(reversed(self._recent_ingest[-limit:]))

    def _run_scans(self) -> None:
        conn = self._conn()
        while not self._stop.is_set():
            try:
                task = self._scan_q.get(timeout=0.2)
            except queue.Empty:
                continue
            try:
                with self._stats_lock:
                    self._active_scan = task
                    self._scan_progress[task.root_id] = {
                        "started_at": time.time(),
                        "current_path": None,
                        "found": 0,
                        "enqueued": 0,
                        "processed": 0,
                        "scan_done": False,
                        "had_errors": False,
                    }
                    if self._scan_wave is None:
                        self._scan_wave = {
                            "started_at": time.time(),
                            "roots": set(),
                            "found": 0,
                            "enqueued": 0,
                            "had_errors": False,
                        }
                    roots = self._scan_wave.get("roots")
                    if isinstance(roots, set):
                        roots.add(int(task.root_id))
                with tx(conn):
                    conn.execute(
                        """
                        UPDATE tracked_roots
                        SET last_scan_started_at=datetime('now'),
                            last_error=NULL
                        WHERE id=?
                        """,
                        (task.root_id,),
                    )
                if not task.root_path.exists():
                    raise OSError(f"Root path unavailable: {task.root_path}")

                def _on_scan_error(err: OSError) -> None:
                    # os.walk calls this on directory access failures.
                    # Keep scanning other directories/files even if one path is bad.
                    # We still mark this scan as having errors and surface the failure.
                    bad_path = Path(str(getattr(err, "filename", "") or task.root_path))
                    err_name = type(err).__name__
                    msg = f"Scan path error ({err_name}): {bad_path} ({err})"
                    logger.warning(msg)
                    with self._stats_lock:
                        prog = self._scan_progress.get(task.root_id)
                        if prog is not None:
                            prog["had_errors"] = True
                        if self._scan_wave is not None:
                            self._scan_wave["had_errors"] = True
                    self._record_failure(root_id=task.root_id, path=bad_path, error=msg)
                    now = time.time()
                    last = float(self._last_root_error_at.get(task.root_id, 0.0) or 0.0)
                    if now - last > 2.0:
                        self._last_root_error_at[task.root_id] = now
                        try:
                            root_missing = False
                            try:
                                root_missing = not task.root_path.exists()
                            except Exception:
                                root_missing = True
                            with tx(conn):
                                if root_missing:
                                    conn.execute(
                                        "UPDATE tracked_roots SET status='offline', last_error=? WHERE id=?",
                                        (msg, task.root_id),
                                    )
                                else:
                                    conn.execute(
                                        "UPDATE tracked_roots SET last_error=? WHERE id=?",
                                        (msg, task.root_id),
                                    )
                        except Exception:
                            pass
                    return

                for img_path in iter_images_recursive(task.root_path, on_error=_on_scan_error):
                    with self._stats_lock:
                        prog = self._scan_progress.get(task.root_id)
                        if prog is not None:
                            prog["current_path"] = str(img_path)
                            prog["found"] = int(prog.get("found", 0) or 0) + 1
                        if self._scan_wave is not None:
                            self._scan_wave["found"] = int(self._scan_wave.get("found", 0) or 0) + 1
                    self.enqueue(
                        IndexTask(photo_path=img_path, root_id=task.root_id, root_path=task.root_path)
                    )
                    with self._stats_lock:
                        prog = self._scan_progress.get(task.root_id)
                        if prog is not None:
                            prog["enqueued"] = int(prog.get("enqueued", 0) or 0) + 1
                        if self._scan_wave is not None:
                            self._scan_wave["enqueued"] = int(self._scan_wave.get("enqueued", 0) or 0) + 1
                with tx(conn):
                    conn.execute(
                        "UPDATE tracked_roots SET last_scan_enumerated_at=datetime('now') WHERE id=?",
                        (task.root_id,),
                    )
                with self._stats_lock:
                    prog = self._scan_progress.get(task.root_id)
                    if prog is not None:
                        prog["scan_done"] = True
                self._maybe_mark_scan_finished(task.root_id)
            except Exception as e:
                logger.exception("Scan failed for root_id=%s path=%s", task.root_id, task.root_path)
                failed_path = task.root_path
                with self._stats_lock:
                    prog = self._scan_progress.get(task.root_id)
                    if prog is not None:
                        prog["had_errors"] = True
                        cur = str(prog.get("current_path") or "").strip()
                        if cur:
                            failed_path = Path(cur)
                    if self._scan_wave is not None:
                        self._scan_wave["had_errors"] = True
                err_name = type(e).__name__
                err_msg = f"Scan failed ({err_name}): {e}"
                self._record_failure(root_id=task.root_id, path=failed_path, error=err_msg)
                try:
                    root_missing = False
                    try:
                        root_missing = not task.root_path.exists()
                    except Exception:
                        root_missing = True
                    with tx(conn):
                        if root_missing:
                            conn.execute(
                                "UPDATE tracked_roots SET status='offline', last_error=? WHERE id=?",
                                (err_msg, task.root_id),
                            )
                        else:
                            conn.execute(
                                "UPDATE tracked_roots SET last_error=? WHERE id=?",
                                (err_msg, task.root_id),
                            )
                except Exception:
                    pass
            finally:
                with self._stats_lock:
                    prog = self._scan_progress.get(task.root_id)
                    if prog is not None:
                        self._last_scan_summary = {
                            "root_id": int(task.root_id),
                            "root_path": str(task.root_path),
                            "found": int(prog.get("found", 0) or 0),
                            "enqueued": int(prog.get("enqueued", 0) or 0),
                            "processed": int(prog.get("processed", 0) or 0),
                            "started_at": float(prog.get("started_at") or 0.0) or None,
                            "ended_at": time.time(),
                            "had_errors": bool(prog.get("had_errors")),
                        }
                        prog["current_path"] = None
                    self._active_scan = None
                    if self._scan_q.qsize() == 0 and self._scan_wave is not None:
                        wave = self._scan_wave
                        roots = wave.get("roots")
                        roots_count = len(roots) if isinstance(roots, set) else 0
                        self._last_scan_wave_summary = {
                            "roots": int(roots_count),
                            "found": int(wave.get("found", 0) or 0),
                            "enqueued": int(wave.get("enqueued", 0) or 0),
                            "started_at": float(wave.get("started_at") or 0.0) or None,
                            "ended_at": time.time(),
                            "had_errors": bool(wave.get("had_errors")),
                        }
                        self._scan_wave = None
                self._scan_q.task_done()

    def _maybe_mark_scan_finished(self, root_id: int) -> None:
        """
        Mark last_scan_finished_at only when:
        - enumeration finished (scan_done), and
        - ingest has processed everything enqueued by that scan, and
        - no errors were observed during scan/ingest.
        """
        with self._stats_lock:
            prog = self._scan_progress.get(root_id)
            if not prog:
                return
            scan_done = bool(prog.get("scan_done"))
            had_errors = bool(prog.get("had_errors"))
            enq = int(prog.get("enqueued", 0) or 0)
            proc = int(prog.get("processed", 0) or 0)
        if not scan_done:
            return
        if had_errors:
            return
        if proc < enq:
            return
        try:
            conn = self._conn()
            with tx(conn):
                conn.execute(
                    "UPDATE tracked_roots SET last_scan_finished_at=datetime('now') WHERE id=?",
                    (root_id,),
                )
        except Exception:
            pass

    def _run(self) -> None:
        conn = self._conn()
        while not self._stop.is_set():
            try:
                task = self._q.get(timeout=0.2)
            except queue.Empty:
                continue
            try:
                with self._stats_lock:
                    self._active_ingest = task
                self._index_one(task)
                now = time.time()
                with self._stats_lock:
                    self._last_ingested_at = now
                    prog = self._scan_progress.get(task.root_id)
                    if prog is not None:
                        prog["processed"] = int(prog.get("processed", 0) or 0) + 1
                self._maybe_mark_scan_finished(task.root_id)
            except Exception as e:
                # Best-effort indexer: don't crash background thread, but do log.
                logger.exception("Indexing failed for %s", task.photo_path)
                with self._stats_lock:
                    prog = self._scan_progress.get(task.root_id)
                    if prog is not None:
                        prog["had_errors"] = True
                err_msg = f"Ingest failed ({type(e).__name__}): {e}"
                self._record_failure(root_id=task.root_id, path=task.photo_path, error=err_msg)
                now = time.time()
                last = float(self._last_root_error_at.get(task.root_id, 0.0) or 0.0)
                if now - last > 2.0:
                    self._last_root_error_at[task.root_id] = now
                    try:
                        with tx(conn):
                            conn.execute(
                                "UPDATE tracked_roots SET last_error=? WHERE id=?",
                                (err_msg, task.root_id),
                            )
                    except Exception:
                        pass
            finally:
                with self._stats_lock:
                    self._active_ingest = None
                self._q.task_done()

    def _index_one(self, task: IndexTask) -> None:
        conn = self._conn()
        embedder = self._get_embedder()
        vector_index = self._get_vector_index()
        path = task.photo_path
        if not is_supported_image(path):
            return
        try:
            st = path.stat()
        except OSError as e:
            # If the root disappeared (e.g. SSD unplugged), surface as an error so the
            # root shows last_error and scans don't look "finished".
            try:
                if not task.root_path.exists():
                    raise e
            except Exception:
                raise e
            return

        rel = None
        try:
            rel = str(path.relative_to(task.root_path))
        except Exception:
            rel = os.path.basename(path)

        ext = path.suffix.lower().lstrip(".")
        size_bytes = int(st.st_size)
        mtime_ns = int(getattr(st, "st_mtime_ns", int(st.st_mtime * 1e9)))

        existing = conn.execute(
            "SELECT id, size_bytes, mtime_ns, embedding_model FROM photos WHERE path = ?",
            (str(path),),
        ).fetchone()

        photo_id: Optional[int] = int(existing["id"]) if existing else None
        unchanged = bool(
            existing and int(existing["size_bytes"]) == size_bytes and int(existing["mtime_ns"]) == mtime_ns
        )

        # Even if the file is unchanged, we may need to compute embeddings for a new model.
        needs_embedding = False
        model_id = embedder.model_id if embedder else None
        if embedder and vector_index and photo_id is not None and model_id:
            has_model_embedding = bool(existing and str(existing["embedding_model"] or "") == model_id)
            needs_embedding = not has_model_embedding
            # Repair case: DB says embedding exists, but vector label is missing
            # (e.g., partial/corrupt index state from a prior failure).
            if has_model_embedding:
                try:
                    has_label = getattr(vector_index, "has_label", None)
                    if callable(has_label) and not bool(has_label(photo_id)):
                        needs_embedding = True
                except Exception:
                    # If the vector backend can't answer presence checks, keep prior behavior.
                    pass

        # If the on-disk thumbnail was deleted, treat it as needing work even if the source file is unchanged.
        needs_thumb = False
        if photo_id is not None:
            try:
                from .thumbnails import get_thumb_path

                thumb_path = get_thumb_path(self.thumbs_dir, photo_id)
                needs_thumb = not thumb_path.exists()
            except Exception:
                needs_thumb = False

        if unchanged and not needs_embedding and not needs_thumb:
            return

        # Optimization: if only thumbnails are missing (file unchanged, embeddings already present), skip metadata.
        if unchanged and needs_thumb and not needs_embedding and existing:
            assert photo_id is not None
            thumb_info = ensure_thumbnail(
                thumbs_dir=self.thumbs_dir, photo_id=photo_id, src_path=path, max_size=384
            )
            if thumb_info:
                _, w, h = thumb_info
                with tx(conn):
                    conn.execute("UPDATE photos SET width=?, height=? WHERE id=?", (w, h, photo_id))
            return

        # Optimization: if only embeddings are missing for this model, skip metadata/thumbnails.
        if unchanged and needs_embedding and existing:
            assert photo_id is not None
            if needs_thumb:
                thumb_info = ensure_thumbnail(
                    thumbs_dir=self.thumbs_dir, photo_id=photo_id, src_path=path, max_size=384
                )
                if thumb_info:
                    _, w, h = thumb_info
                    with tx(conn):
                        conn.execute("UPDATE photos SET width=?, height=? WHERE id=?", (w, h, photo_id))
            embedder = self._get_embedder()
            vector_index = self._get_vector_index()
            if embedder and vector_index:
                vec = embedder.embed_image(path)
                vector_index.add_or_update(photo_id, vec)
                now = time.time()
                if now - self._last_persist >= self._persist_interval_s:
                    vector_index.persist()
                    self._last_persist = now
                with tx(conn):
                    conn.execute(
                        "UPDATE photos SET embedding_dim=?, embedding_model=? WHERE id=?",
                        (int(vec.shape[0]), embedder.model_id, photo_id),
                    )
            return

        exif_dt, date_source = get_date_taken(path)
        if not exif_dt:
            exif_dt = datetime.fromtimestamp(st.st_mtime, tz=timezone.utc)
            date_source = "mtime"
        date_taken = exif_dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        with tx(conn):
            if existing:
                assert photo_id is not None
                conn.execute(
                    """
                    UPDATE photos
                    SET rel_path=?, ext=?, size_bytes=?, mtime_ns=?, date_taken=?, date_source=?, indexed_at=datetime('now')
                    WHERE id=?
                    """,
                    (rel, ext, size_bytes, mtime_ns, date_taken, date_source, photo_id),
                )
            else:
                cur = conn.execute(
                    """
                    INSERT INTO photos (root_id, path, rel_path, ext, size_bytes, mtime_ns, date_taken, date_source)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (task.root_id, str(path), rel, ext, size_bytes, mtime_ns, date_taken, date_source),
                )
                photo_id = int(cur.lastrowid)

        thumb_info = ensure_thumbnail(
            thumbs_dir=self.thumbs_dir, photo_id=photo_id, src_path=path, max_size=384
        )
        if thumb_info:
            _, w, h = thumb_info
            with tx(conn):
                conn.execute("UPDATE photos SET width=?, height=? WHERE id=?", (w, h, photo_id))

        if embedder and vector_index and photo_id is not None:
            vec = embedder.embed_image(path)
            vector_index.add_or_update(photo_id, vec)
            now = time.time()
            if now - self._last_persist >= self._persist_interval_s:
                vector_index.persist()
                self._last_persist = now
            with tx(conn):
                conn.execute(
                    "UPDATE photos SET embedding_dim=?, embedding_model=? WHERE id=?",
                    (int(vec.shape[0]), embedder.model_id, photo_id),
                )

        with self._stats_lock:
            t = time.time()
            self._recent_ingest.append(
                {
                    "ts": t,
                    "photo_id": photo_id,
                    "path": str(path),
                    "root_id": task.root_id,
                }
            )
            if len(self._recent_ingest) > self._recent_max:
                self._recent_ingest = self._recent_ingest[-self._recent_max :]
