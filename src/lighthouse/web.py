from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.parse import quote

from fastapi import FastAPI, Form, HTTPException, Query, Request
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from starlette.responses import StreamingResponse

from .config import get_app_paths
from .db import connect, migrate, tx
from .embeddings import build_embedder
from .indexer import IndexTask, PhotoIndexer
from .picker import pick_directory
from .vector_index import VectorIndex
from .watcher import RootWatcher

logger = logging.getLogger("uvicorn.error")

def _parse_int(v: Optional[str]) -> Optional[int]:
    if v is None or v == "":
        return None
    try:
        return int(v)
    except Exception:
        return None


def create_app(
    *,
    enable_clip: bool = True,
    clip_model: str = "ViT-B-32",
    clip_pretrained: str = "openai",
    clip_device: str = "auto",
) -> FastAPI:
    paths = get_app_paths()
    paths.data_dir.mkdir(parents=True, exist_ok=True)
    paths.thumbs_dir.mkdir(parents=True, exist_ok=True)
    paths.index_dir.mkdir(parents=True, exist_ok=True)

    # SQLite connections must not be used across threads. Uvicorn runs sync handlers
    # in a threadpool; keep one connection per thread.
    conn_boot = connect(paths.db_path, check_same_thread=True)
    migrate(conn_boot)
    conn_boot.close()

    web_local = threading.local()

    def _web_conn():
        conn = getattr(web_local, "conn", None)
        if conn is None:
            conn = connect(paths.db_path, check_same_thread=True)
            web_local.conn = conn
        return conn

    def _web_fetchone(sql: str, params=()):
        return _web_conn().execute(sql, params).fetchone()

    def _web_fetchall(sql: str, params=()):
        return _web_conn().execute(sql, params).fetchall()

    embedder = build_embedder(enable_clip, model_name=clip_model, pretrained=clip_pretrained, device=clip_device)
    vector_index = None
    if embedder:
        vector_index = VectorIndex(index_base_dir=paths.index_dir, dim=embedder.image_dim(), model_id=embedder.model_id)

    indexer = PhotoIndexer(
        db_path=paths.db_path,
        thumbs_dir=paths.thumbs_dir,
        get_embedder=lambda: embedder,
        get_vector_index=lambda: vector_index,
    )

    root_paths: dict[int, Path] = {}
    root_paths_lock = threading.Lock()

    def _get_root_path(root_id: int) -> Optional[Path]:
        with root_paths_lock:
            return root_paths.get(root_id)

    def _set_root_path(root_id: int, path: Path) -> None:
        with root_paths_lock:
            root_paths[root_id] = path

    def _refresh_root_paths() -> None:
        rows = _web_fetchall("SELECT id, path FROM tracked_roots")
        with root_paths_lock:
            root_paths.clear()
            for r in rows:
                root_paths[int(r["id"])] = Path(r["path"])

    def _make_watcher() -> RootWatcher:
        return RootWatcher(
            on_photo_path=lambda rid, p: indexer.enqueue(
                IndexTask(photo_path=p, root_id=rid, root_path=_get_root_path(rid) or p.parent)
            )
        )

    watcher_ref: dict[str, RootWatcher] = {"watcher": _make_watcher()}

    def _watcher() -> RootWatcher:
        return watcher_ref["watcher"]

    def _restart_watcher() -> None:
        try:
            _watcher().stop()
        except Exception:
            pass
        watcher_ref["watcher"] = _make_watcher()
        watcher_ref["watcher"].start()
        # Re-add online roots.
        rows = _web_fetchall("SELECT id, path, status FROM tracked_roots")
        for r in rows:
            if str(r["status"]) != "online":
                continue
            try:
                watcher_ref["watcher"].add_root(int(r["id"]), Path(r["path"]))
            except Exception:
                continue

    monitor_stop = threading.Event()
    monitor_thread: Optional[threading.Thread] = None
    catchup_stop = threading.Event()
    catchup_thread: Optional[threading.Thread] = None
    picker_lock = threading.Lock()
    counts_lock = threading.Lock()
    counts_cache = {
        "at": 0.0,
        "photos_total": 0,
        "photos_indexed": 0,
        "roots_total": 0,
        "roots_online": 0,
    }
    counts_interval_s = float(os.environ.get("LIGHTHOUSE_STATUS_COUNTS_INTERVAL_S", "30"))
    status_busy_interval_s = float(os.environ.get("LIGHTHOUSE_STATUS_BUSY_INTERVAL_S", "0.25"))
    status_idle_interval_s = float(os.environ.get("LIGHTHOUSE_STATUS_IDLE_INTERVAL_S", "0.25"))

    app = FastAPI(title="Light House", docs_url=None, redoc_url=None)
    static_dir = Path(__file__).parent / "static"
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")
    templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))

    @app.on_event("startup")
    def _startup() -> None:
        _refresh_root_paths()
        indexer.start()
        _watcher().start()

        def _catchup_loop() -> None:
            """
            Periodically enqueue photos that are missing embeddings for the active model.
            This must not block FastAPI startup or request threads.
            """
            while not catchup_stop.is_set():
                if not (embedder and vector_index):
                    time.sleep(5.0)
                    continue

                # Fill the ingest queue opportunistically, then sleep a bit to avoid
                # hammering the DB when everything is already embedded.
                try:
                    n = indexer.enqueue_missing_embeddings(embedder.model_id)
                except Exception:
                    n = 0
                time.sleep(0.15 if n else 3.0)

        def _monitor_loop() -> None:
            conn_monitor = connect(paths.db_path, check_same_thread=True)
            while not monitor_stop.is_set():
                if not _watcher().is_alive():
                    _restart_watcher()
                rows = conn_monitor.execute("SELECT id, path, status FROM tracked_roots").fetchall()
                for r in rows:
                    root_id = int(r["id"])
                    p = Path(r["path"])
                    prev_status = str(r["status"])
                    try:
                        exists = p.exists()
                    except OSError as e:
                        exists = False
                        with tx(conn_monitor):
                            conn_monitor.execute(
                                "UPDATE tracked_roots SET status='offline', last_error=? WHERE id=?",
                                (str(e), root_id),
                            )
                    if exists:
                        _set_root_path(root_id, p)
                        with tx(conn_monitor):
                            conn_monitor.execute(
                                "UPDATE tracked_roots SET status='online', last_seen_at=datetime('now'), last_error=NULL WHERE id=?",
                                (root_id,),
                            )
                        # If drive was offline/unknown and is back, trigger a scan.
                        if prev_status != "online":
                            indexer.enqueue_scan_root(root_id, p)
                        try:
                            _watcher().add_root(root_id, p)
                        except Exception:
                            pass
                    else:
                        with tx(conn_monitor):
                            conn_monitor.execute(
                                "UPDATE tracked_roots SET status='offline' WHERE id=?",
                                (root_id,),
                            )

                time.sleep(30.0)
            try:
                conn_monitor.close()
            except Exception:
                pass

        nonlocal monitor_thread
        monitor_thread = threading.Thread(target=_monitor_loop, name="lighthouse-monitor", daemon=True)
        monitor_thread.start()

        nonlocal catchup_thread
        catchup_thread = threading.Thread(target=_catchup_loop, name="lighthouse-catchup", daemon=True)
        catchup_thread.start()

    @app.on_event("shutdown")
    def _shutdown() -> None:
        monitor_stop.set()
        catchup_stop.set()
        if monitor_thread:
            monitor_thread.join(timeout=2.0)
        if catchup_thread:
            catchup_thread.join(timeout=2.0)
        _watcher().stop()
        indexer.stop()
        if vector_index:
            vector_index.persist()

    def _library_rows(
        *,
        q: str,
        folder: Optional[str],
        year: Optional[str],
        month: Optional[str],
        day: Optional[str],
        offset: int,
        limit: int,
    ):
        def _pick_relevance_split(scores_desc: list[float]) -> int:
            """
            Pick an "elbow" in the similarity curve so we can separate highly relevant
            results from the long tail. Returns a count (>=1).
            """
            n = len(scores_desc)
            if n <= 0:
                return 0
            if n < 30:
                return min(n, 18)
            max_i = min(n - 1, 160)
            min_i = min(12, max_i)
            best_drop = 0.0
            best_i = min(48, max_i)
            # Look for the largest drop between adjacent scores within the top range.
            for i in range(1, max_i + 1):
                if i < min_i:
                    continue
                drop = float(scores_desc[i - 1]) - float(scores_desc[i])
                if drop > best_drop:
                    best_drop = drop
                    best_i = i
            # Require a meaningful gap, otherwise fall back to a sane default.
            top = float(scores_desc[0])
            if best_drop < max(0.02, top * 0.06):
                best_i = min(48, max_i)
            return max(1, int(best_i))

        def _path_token_match_ids(
            query_text: str,
            *,
            exclude_ids: set[int],
        ) -> list[int]:
            # Hybrid fallback: include path/name token matches so textual folder/file
            # queries are not lost due to semantic score cutoffs.
            tokens = re.findall(r"[a-z0-9]{2,}", query_text.lower())
            if not tokens:
                return []
            # Keep SQL bounded for very long queries.
            tokens = tokens[:8]
            clauses: list[str] = []
            params3: list[object] = []
            for tok in tokens:
                pat = f"%{tok}%"
                clauses.append("(lower(path) LIKE ? OR lower(rel_path) LIKE ?)")
                params3.extend([pat, pat])
            q3 = "SELECT id FROM photos WHERE " + " AND ".join(clauses)
            if where:
                q3 += " AND " + " AND ".join(where)
                params3 += params
            # Keep fallback bounded; these are appended after semantic hits.
            q3 += " ORDER BY date_taken DESC LIMIT 4000"
            rows3 = _web_fetchall(q3, params3)
            out: list[int] = []
            for r in rows3:
                pid = int(r["id"])
                if pid in exclude_ids:
                    continue
                out.append(pid)
            return out

        year_i = _parse_int(year)
        month_i = _parse_int(month)
        day_i = _parse_int(day)
        offset = max(0, int(offset))
        limit = max(1, min(int(limit), 800))

        where = []
        params = []
        if year_i is not None:
            where.append("strftime('%Y', date_taken) = ?")
            params.append(f"{year_i:04d}")
        if month_i is not None:
            where.append("strftime('%m', date_taken) = ?")
            params.append(f"{month_i:02d}")
        if day_i is not None:
            where.append("strftime('%d', date_taken) = ?")
            params.append(f"{day_i:02d}")
        folder_q = (folder or "").strip()
        if folder_q:
            sep = "\\" if ("\\" in folder_q and "/" not in folder_q) else "/"
            folder_base = folder_q.rstrip("/\\")
            if folder_base:
                folder_prefix = folder_base + sep
                where.append("path LIKE ?")
                params.append(f"{folder_prefix}%")
                # Keep only direct children of this folder, not nested subfolders.
                where.append("instr(substr(path, ?), ?) = 0")
                params.append(len(folder_prefix) + 1)
                params.append(sep)
                folder_q = folder_base
            else:
                folder_q = ""

        photo_rows = []
        scores: dict[int, float] = {}
        total_matches: Optional[int] = None
        most_relevant_count: Optional[int] = None

        q2 = q.strip()
        if q2:
            if not (embedder and vector_index):
                raise HTTPException(status_code=400, detail="Content search disabled (install clip extras).")
            query_tokens = re.findall(r"[a-z0-9]{2,}", q2.lower())
            t0 = time.perf_counter()
            # Fetch hits and map them back to existing photos.
            # The vector index can contain stale ids (e.g., after untracking a root);
            # prune those on the fly so search keeps working.
            t1 = time.perf_counter()
            vec = embedder.embed_text(q2)
            t2 = time.perf_counter()
            # Keep k modest to avoid hnswlib errors on some indices; if there are stale ids,
            # we delete them and re-query, which naturally surfaces the next-best results.
            base_k = min(2500, max(200, offset + limit + 50))
            max_k = 20000
            k = base_k
            attempts = 0
            filtered_hits: list[tuple[int, float]] = []
            relevant_cutoff: Optional[float] = None
            min_keep_score: Optional[float] = None
            eligible_hits: list[tuple[int, float]] = []
            semantic_total = 0
            lexical_total = 0
            # Timings for the last attempt (best-effort for logging).
            t_search0 = t2
            t_search1 = t2
            t_db1 = t2
            t_db2 = t2
            while attempts < 10:
                attempts += 1
                t_search0 = time.perf_counter()
                hits = vector_index.search(vec, k=k)
                t_search1 = time.perf_counter()
                # Ensure descending similarity order (hnswlib is usually ordered but not guaranteed).
                hits = sorted(hits, key=lambda x: float(x[1]), reverse=True)
                ids_all = [int(pid) for pid, _ in hits]
                if not ids_all:
                    break

                placeholders = ",".join(["?"] * len(ids_all))
                existing_rows = _web_fetchall(
                    f"SELECT id FROM photos WHERE id IN ({placeholders})",
                    ids_all,
                )
                t_db1 = time.perf_counter()
                existing_ids = {int(r["id"]) for r in existing_rows}
                missing_ids = [pid for pid in ids_all if pid not in existing_ids]
                if missing_ids:
                    try:
                        vector_index.delete_many(missing_ids)
                    except Exception:
                        for mid in missing_ids:
                            try:
                                vector_index.delete(mid)
                            except Exception:
                                pass

                # Apply date filters (if any) and preserve vector ranking order.
                ids_exist = [pid for pid in ids_all if pid in existing_ids]
                if not ids_exist:
                    # If we just deleted stale ids, try again with the same k.
                    if missing_ids:
                        continue
                    if k >= max_k:
                        break
                    k = min(max_k, k + base_k)
                    continue

                placeholders2 = ",".join(["?"] * len(ids_exist))
                query = f"SELECT * FROM photos WHERE id IN ({placeholders2})"
                params2 = list(ids_exist)
                if where:
                    query += " AND " + " AND ".join(where)
                    params2 += params
                rows = _web_fetchall(query, params2)
                t_db2 = time.perf_counter()
                by_id = {int(r["id"]): r for r in rows}
                filtered_hits = [(int(pid), float(score)) for pid, score in hits if int(pid) in by_id]
                if not filtered_hits:
                    # If we just deleted stale ids, try again with the same k.
                    if missing_ids:
                        continue
                    if k >= max_k:
                        break
                    k = min(max_k, k + base_k)
                    continue

                if min_keep_score is None:
                    # Compute a "most relevant" cutoff from the similarity curve, then
                    # a "min keep" cutoff that trims the long tail so results don't go on forever.
                    scores_desc = [s for _, s in filtered_hits[: min(300, len(filtered_hits))]]
                    split_n = _pick_relevance_split(scores_desc)
                    split_n = max(1, min(split_n, len(scores_desc)))
                    relevant_cutoff = float(scores_desc[split_n - 1])
                    top = float(scores_desc[0])
                    # Short object queries (e.g., "snake") can be semantically noisy;
                    # use a slightly lower floor to improve recall.
                    if len(query_tokens) <= 2 and top < 0.30:
                        floor = 0.16
                    else:
                        floor = 0.18 if top < 0.28 else 0.22
                    min_keep_score = float(max(floor, relevant_cutoff - 0.10))
                    # Never keep below our "most relevant" cutoff.
                    min_keep_score = float(min(min_keep_score, relevant_cutoff))

                eligible_hits = [(pid, score) for pid, score in filtered_hits if score >= float(min_keep_score)]
                last_score = float(filtered_hits[-1][1])
                # We consider the list "complete enough" when either:
                # - we've reached the index cap, OR
                # - the tail already dropped below the keep threshold (so further results won't qualify).
                complete = bool(k >= max_k or last_score < float(min_keep_score))

                if complete or len(eligible_hits) >= offset + limit:
                    # Use eligible_hits to avoid returning endless low-relevance results.
                    slice_hits = eligible_hits[offset : offset + limit]
                    photo_rows = [by_id[int(pid)] for pid, _ in slice_hits if int(pid) in by_id]
                    scores = {int(pid): float(score) for pid, score in slice_hits}
                    total_matches = len(eligible_hits)
                    if offset == 0 and relevant_cutoff is not None:
                        # Cap the highlighted section so the "more results" section is visible.
                        most_relevant_count = min(
                            len(photo_rows),
                            max(12, min(72, sum(1 for _, sc in eligible_hits[:200] if sc >= float(relevant_cutoff)))),
                        )
                    break

                # If we pruned stale ids, re-query at the same k first.
                if missing_ids:
                    continue
                k = min(max_k, k + base_k)

            semantic_ids = [int(pid) for pid, _ in eligible_hits]
            semantic_scores = {int(pid): float(score) for pid, score in eligible_hits}
            semantic_total = len(semantic_ids)
            lexical_ids = _path_token_match_ids(q2, exclude_ids=set(semantic_ids))
            lexical_total = len(lexical_ids)
            combined_ids = semantic_ids + lexical_ids
            total_matches = len(combined_ids)
            page_ids = combined_ids[offset : offset + limit]
            if page_ids:
                placeholders_page = ",".join(["?"] * len(page_ids))
                page_rows = _web_fetchall(f"SELECT * FROM photos WHERE id IN ({placeholders_page})", page_ids)
                by_id_page = {int(r["id"]): r for r in page_rows}
                photo_rows = [by_id_page[int(pid)] for pid in page_ids if int(pid) in by_id_page]
            else:
                photo_rows = []
            scores = {int(pid): float(semantic_scores[int(pid)]) for pid in page_ids if int(pid) in semantic_scores}
            if offset == 0 and relevant_cutoff is not None and eligible_hits:
                # Cap the highlighted section so the "more results" section is visible.
                top_sem = sum(1 for _, sc in eligible_hits[:200] if sc >= float(relevant_cutoff))
                most_relevant_count = min(
                    len(photo_rows),
                    max(12, min(72, top_sem)),
                )
            t3 = time.perf_counter()
            logger.info(
                "Semantic search q=%r offset=%s limit=%s rows=%s total=%s sem=%s path=%s attempts=%s k=%s keep>=%.3f rel>=%.3f embed=%.2fs hnsw=%.2fs db1=%.2fs db2=%.2fs total_time=%.2fs",
                q2,
                offset,
                limit,
                len(photo_rows),
                total_matches,
                semantic_total,
                lexical_total,
                attempts,
                k,
                float(min_keep_score or 0.0),
                float(relevant_cutoff or 0.0),
                (t2 - t1),
                (t_search1 - t_search0),
                (t_db1 - t_search1),
                (t_db2 - t_db1),
                (t3 - t0),
            )
        else:
            query = "SELECT * FROM photos"
            count_query = "SELECT COUNT(*) AS c FROM photos"
            if where:
                query += " WHERE " + " AND ".join(where)
                count_query += " WHERE " + " AND ".join(where)
            total_matches = int(_web_fetchone(count_query, params)["c"])
            # Deterministic daily shuffle (stable across pagination within a day,
            # rotates automatically each day for rediscovery).
            shuffle_seed = int(datetime.now().strftime("%Y%m%d"))
            query += " ORDER BY (((id * 1103515245) + ?) & 2147483647), id LIMIT ? OFFSET ?"
            params2 = list(params) + [shuffle_seed, limit, offset]
            photo_rows = _web_fetchall(query, params2)

        has_more = len(photo_rows) == limit and (total_matches is None or (offset + limit) < total_matches)
        return {
            "rows": photo_rows,
            "scores": scores,
            "offset": offset,
            "limit": limit,
            "has_more": has_more,
            "next_offset": offset + limit,
            "total_matches": total_matches,
            "most_relevant_count": most_relevant_count,
            "year": year or "",
            "month": month or "",
            "day": day or "",
            "q": q,
            "folder": folder_q,
        }

    @app.get("/", response_class=HTMLResponse)
    def library(
        request: Request,
        q: str = Query(default=""),
        folder: Optional[str] = Query(default=None),
        year: Optional[str] = Query(default=None),
        month: Optional[str] = Query(default=None),
        day: Optional[str] = Query(default=None),
        offset: int = Query(default=0),
        limit: int = Query(default=300),
    ) -> HTMLResponse:
        ctx = _library_rows(q=q, folder=folder, year=year, month=month, day=day, offset=offset, limit=limit)
        return templates.TemplateResponse("search.html", {"request": request, **ctx})

    def _status_payload() -> dict:
        s = indexer.stats()
        now = time.time()
        with counts_lock:
            if now - float(counts_cache["at"]) > counts_interval_s:
                try:
                    counts_cache["photos_total"] = int(_web_fetchone("SELECT COUNT(*) AS c FROM photos")["c"])
                    if embedder:
                        counts_cache["photos_indexed"] = int(
                            _web_fetchone(
                                "SELECT COUNT(*) AS c FROM photos WHERE embedding_model = ?",
                                (embedder.model_id,),
                            )["c"]
                        )
                    else:
                        counts_cache["photos_indexed"] = int(
                            _web_fetchone("SELECT COUNT(*) AS c FROM photos WHERE embedding_model IS NOT NULL")["c"]
                        )
                    counts_cache["roots_total"] = int(_web_fetchone("SELECT COUNT(*) AS c FROM tracked_roots")["c"])
                    counts_cache["roots_online"] = int(
                        _web_fetchone("SELECT COUNT(*) AS c FROM tracked_roots WHERE status='online'")["c"]
                    )
                    counts_cache["at"] = now
                except Exception:
                    # Keep previous cached counts if the DB is temporarily busy/unavailable.
                    pass
        return {
            "scan_queue_size": s.scan_queue_size,
            "ingest_queue_size": s.ingest_queue_size,
            "active_scan_root_id": s.active_scan_root_id,
            "active_scan_root_path": s.active_scan_root_path,
            "active_scan_current_path": s.active_scan_current_path,
            "active_scan_found": s.active_scan_found,
            "active_scan_enqueued": s.active_scan_enqueued,
            "active_scan_processed": s.active_scan_processed,
            "active_scan_started_at": s.active_scan_started_at,
            "active_ingest_path": s.active_ingest_path,
            "last_ingested_at": s.last_ingested_at,
            "failed_total": s.failed_total,
            "last_failed_path": s.last_failed_path,
            "last_failed_at": s.last_failed_at,
            "last_failed_error": s.last_failed_error,
            "last_scan_root_id": s.last_scan_root_id,
            "last_scan_root_path": s.last_scan_root_path,
            "last_scan_found": s.last_scan_found,
            "last_scan_enqueued": s.last_scan_enqueued,
            "last_scan_processed": s.last_scan_processed,
            "last_scan_started_at": s.last_scan_started_at,
            "last_scan_ended_at": s.last_scan_ended_at,
            "last_scan_had_errors": s.last_scan_had_errors,
            "last_scan_wave_roots": s.last_scan_wave_roots,
            "last_scan_wave_found": s.last_scan_wave_found,
            "last_scan_wave_enqueued": s.last_scan_wave_enqueued,
            "last_scan_wave_started_at": s.last_scan_wave_started_at,
            "last_scan_wave_ended_at": s.last_scan_wave_ended_at,
            "last_scan_wave_had_errors": s.last_scan_wave_had_errors,
            "photos_total": int(counts_cache["photos_total"]),
            "photos_indexed": int(counts_cache["photos_indexed"]),
            "roots_total": int(counts_cache["roots_total"]),
            "roots_online": int(counts_cache["roots_online"]),
            "now": now,
        }

    @app.get("/api/status")
    def api_status() -> dict:
        return _status_payload()

    @app.get("/api/status/stream")
    async def api_status_stream(request: Request) -> StreamingResponse:
        async def event_stream():
            # One long-lived connection; push faster while busy, slower when idle.
            while True:
                if await request.is_disconnected():
                    break
                try:
                    payload = _status_payload()
                except Exception as e:
                    payload = {"error": str(e), "now": time.time()}
                yield f"data: {json.dumps(payload, separators=(',', ':'))}\n\n"
                busy = (
                    (payload.get("scan_queue_size", 0) or 0) > 0
                    or (payload.get("ingest_queue_size", 0) or 0) > 0
                    or bool(payload.get("active_scan_root_id"))
                    or bool(payload.get("active_ingest_path"))
                )
                await asyncio.sleep(status_busy_interval_s if busy else status_idle_interval_s)

        headers = {
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        }
        return StreamingResponse(event_stream(), media_type="text/event-stream", headers=headers)

    @app.get("/api/activity")
    def api_activity(limit: int = Query(default=30)) -> dict:
        return {"recent_ingest": indexer.recent_activity(limit=limit)}

    @app.get("/api/roots")
    def api_roots() -> dict:
        rows = _web_fetchall("SELECT * FROM tracked_roots ORDER BY id DESC")
        out: list[dict[str, object]] = []
        for r in rows:
            out.append(
                {
                    "id": int(r["id"]),
                    "path": str(r["path"]),
                    "status": str(r["status"] or ""),
                    "last_seen_at": r["last_seen_at"],
                    "last_scan_started_at": r["last_scan_started_at"],
                    "last_scan_enumerated_at": r["last_scan_enumerated_at"],
                    "last_scan_finished_at": r["last_scan_finished_at"],
                    "last_error": r["last_error"],
                }
            )
        return {"roots": out}

    @app.get("/roots", response_class=HTMLResponse)
    def roots_page(request: Request) -> HTMLResponse:
        roots = _web_fetchall("SELECT * FROM tracked_roots ORDER BY id DESC")
        return templates.TemplateResponse("roots.html", {"request": request, "roots": roots})

    @app.get("/diagnostics", response_class=HTMLResponse)
    def diagnostics(request: Request) -> HTMLResponse:
        paths_cfg = get_app_paths()
        stats = indexer.stats()

        clip = {"enabled": False}
        if embedder:
            clip = {
                "enabled": True,
                "model_id": embedder.model_id,
                "dim": embedder.image_dim(),
                "device": embedder.device,
                "available": True,
                "error": None,
            }
            try:
                # Test whether deps/weights are actually loadable.
                embedder._ensure_loaded()  # type: ignore[attr-defined]
            except Exception as e:
                clip["available"] = False
                clip["error"] = str(e)

        index_meta = None
        if vector_index:
            index_meta = {
                "dir": str(paths_cfg.index_dir),
                "meta_path": str(vector_index.index_dir / "meta.json"),
                "bin_path": str(vector_index.index_dir / "hnsw.bin"),
            }

        roots = _web_fetchall("SELECT * FROM tracked_roots ORDER BY id DESC")
        return templates.TemplateResponse(
            "diagnostics.html",
            {
                "request": request,
                "paths": paths_cfg,
                "clip": clip,
                "index_meta": index_meta,
                "stats": stats,
                "recent": indexer.recent_activity(limit=30),
                "roots": roots,
            },
        )

    def _add_root_and_scan(p: Path) -> None:
        conn = _web_conn()
        with tx(conn):
            conn.execute(
                "INSERT OR IGNORE INTO tracked_roots(path, status, last_seen_at) VALUES(?, 'online', datetime('now'))",
                (str(p),),
            )
        root = _web_fetchone("SELECT id, path FROM tracked_roots WHERE path=?", (str(p),))
        if root:
            root_id = int(root["id"])
            root_path = Path(root["path"])
            _set_root_path(root_id, root_path)
            try:
                _watcher().add_root(root_id, root_path)
            except Exception:
                pass
            # Full recursive scan to ensure we index everything, including subfolders.
            indexer.enqueue_scan_root(root_id, root_path)

    @app.get("/fs", response_class=HTMLResponse)
    def fs_browse(
        request: Request,
        path: str = Query(default="/Volumes"),
        show_hidden: bool = Query(default=False),
    ) -> HTMLResponse:
        p = Path(path).expanduser()
        error: Optional[str] = None
        try:
            p = p.resolve()
        except Exception:
            pass
        if not p.exists() or not p.is_dir():
            error = "Path must be an existing directory."
            p = Path("/Volumes")

        parent = p.parent if p.parent != p else p
        items: list[dict[str, str]] = []
        try:
            for child in sorted(p.iterdir(), key=lambda x: x.name.lower()):
                if not child.is_dir():
                    continue
                if not show_hidden and child.name.startswith("."):
                    continue
                items.append({"name": child.name, "path": str(child), "q": quote(str(child))})
        except Exception as e:
            error = str(e)

        return templates.TemplateResponse(
            "fs.html",
            {
                "request": request,
                "path": str(p),
                "path_q": quote(str(p)),
                "parent": str(parent),
                "parent_q": quote(str(parent)),
                "items": items,
                "error": error,
                "show_hidden": show_hidden,
            },
        )

    @app.post("/roots/add")
    def roots_add(path: str = Form(...)) -> RedirectResponse:
        p = Path(path).expanduser()
        if not p.exists() or not p.is_dir():
            raise HTTPException(status_code=400, detail="Path must be an existing directory")
        _add_root_and_scan(p)
        return RedirectResponse(url="/roots", status_code=303)

    @app.post("/roots/pick")
    def roots_pick() -> RedirectResponse:
        # Opens a native folder picker on the machine running the server.
        with picker_lock:
            p = pick_directory(title="Select a photo root folder")
        if not p:
            return RedirectResponse(url="/roots", status_code=303)
        if not p.exists() or not p.is_dir():
            return RedirectResponse(url="/roots", status_code=303)
        _add_root_and_scan(p)
        return RedirectResponse(url="/roots", status_code=303)

    @app.post("/roots/scan")
    def roots_scan_all() -> RedirectResponse:
        roots = _web_fetchall("SELECT id, path FROM tracked_roots")
        for r in roots:
            p = Path(r["path"])
            if p.exists():
                indexer.enqueue_scan_root(int(r["id"]), p)
        return RedirectResponse(url="/roots", status_code=303)

    @app.post("/roots/{root_id}/remove")
    def roots_remove(root_id: int) -> RedirectResponse:
        conn = _web_conn()
        with tx(conn):
            conn.execute("DELETE FROM tracked_roots WHERE id=?", (root_id,))
        with root_paths_lock:
            root_paths.pop(root_id, None)
        _restart_watcher()
        return RedirectResponse(url="/roots", status_code=303)

    @app.get("/thumb/{photo_id}")
    def thumb(photo_id: int):
        from .thumbnails import get_thumb_path

        p = get_thumb_path(paths.thumbs_dir, photo_id)
        if not p.exists():
            raise HTTPException(status_code=404)
        # Thumbnails are addressed with a cache-busting `?v=<mtime_ns>` in the UI,
        # so we can cache them aggressively for fast scrolling/page loads.
        return FileResponse(
            p,
            media_type="image/jpeg",
            headers={"Cache-Control": "public, max-age=31536000, immutable"},
        )

    @app.get("/image/{photo_id}")
    def image(request: Request, photo_id: int):
        row = _web_fetchone("SELECT path FROM photos WHERE id=?", (photo_id,))
        if not row:
            raise HTTPException(status_code=404)
        p = Path(row["path"])
        if not p.exists():
            raise HTTPException(status_code=404)
        # Serve original bytes (browser handles jpeg/png).
        media_type = "image/jpeg" if p.suffix.lower() in {".jpg", ".jpeg"} else "image/png"
        has_version = bool(request.query_params.get("v"))
        cache_control = "public, max-age=31536000, immutable" if has_version else "public, max-age=0, must-revalidate"
        return FileResponse(p, media_type=media_type, headers={"Cache-Control": cache_control})

    @app.get("/search")
    def search_alias(
        request: Request,
        q: str = Query(default=""),
        folder: Optional[str] = Query(default=None),
        year: Optional[str] = Query(default=None),
        month: Optional[str] = Query(default=None),
        day: Optional[str] = Query(default=None),
        offset: int = Query(default=0),
        limit: int = Query(default=300),
    ) -> RedirectResponse:
        # Back-compat: keep /search but it behaves like the library page.
        params = []
        if q:
            params.append(("q", q))
        if folder:
            params.append(("folder", folder))
        if year:
            params.append(("year", year))
        if month:
            params.append(("month", month))
        if day:
            params.append(("day", day))
        if offset:
            params.append(("offset", str(offset)))
        if limit != 300:
            params.append(("limit", str(limit)))
        qs = "&".join([f"{quote(k)}={quote(v)}" for k, v in params])
        url = "/" + (("?" + qs) if qs else "")
        return RedirectResponse(url=url, status_code=302)

    return app
