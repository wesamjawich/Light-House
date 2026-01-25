from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional


@dataclass(frozen=True)
class IndexMeta:
    dim: int
    model_id: str


class VectorIndexUnavailable(RuntimeError):
    pass


class VectorIndex:
    def __init__(self, *, index_base_dir: Path, dim: int, model_id: str) -> None:
        # Single index directory (no multi-model subdirectories).
        self.index_base_dir = index_base_dir
        self.index_dir = index_base_dir
        self.index_dir.mkdir(parents=True, exist_ok=True)
        self.dim = dim
        self.model_id = model_id

        self._index = None
        self._loaded = False
        import threading

        self._lock = threading.RLock()

        self._meta_path = self.index_dir / "meta.json"
        self._bin_path = self.index_dir / "hnsw.bin"

    def _default_max_elements(self) -> int:
        # Keep this reasonably high; HNSW needs a fixed capacity, but can be resized later.
        try:
            return max(10_000, int(os.environ.get("PHOTOBROWSE_MAX_ELEMENTS", "250000")))
        except Exception:
            return 250_000

    def _maybe_resize(self, *, min_elements: int) -> None:
        assert self._index is not None
        try:
            current_max = int(self._index.get_max_elements())
        except Exception:
            current_max = 0
        if current_max and current_max >= min_elements:
            return
        if not hasattr(self._index, "resize_index"):
            raise RuntimeError("Vector index is full and cannot be resized (hnswlib.resize_index unavailable).")
        new_max = max(self._default_max_elements(), current_max or 0)
        while new_max < min_elements:
            new_max *= 2
        self._index.resize_index(int(new_max))

    def _ensure_loaded(self) -> None:
        with self._lock:
            if self._loaded:
                return
            try:
                import hnswlib
            except Exception as e:
                raise VectorIndexUnavailable(
                    "Vector index dependencies not installed. Install with: pip install -e \".[clip]\""
                ) from e

            index = hnswlib.Index(space="cosine", dim=self.dim)
            if self._bin_path.exists() and not self._meta_path.exists():
                # A missing meta file means we can't validate the contents; rebuild this
                # index directory from scratch (does not affect photos).
                self._bin_path.unlink(missing_ok=True)

            if self._bin_path.exists() and self._meta_path.exists():
                meta = json.loads(self._meta_path.read_text())
                if meta.get("dim") != self.dim or meta.get("model_id") != self.model_id:
                    # Single index directory; mismatch means stale/corrupt. Start fresh.
                    self._bin_path.unlink(missing_ok=True)
                else:
                    index.load_index(str(self._bin_path))
            if not self._bin_path.exists():
                max_elements = self._default_max_elements()
                index.init_index(
                    max_elements=max_elements,
                    ef_construction=200,
                    M=16,
                    allow_replace_deleted=True,
                )
            else:
                # If the index was created with a smaller cap, grow it to at least the
                # current default so ongoing scans don't hit the hard limit.
                try:
                    self._maybe_resize(min_elements=self._default_max_elements())
                except Exception:
                    # Best-effort: don't fail loading because resize isn't available.
                    pass
            index.set_ef(64)
            self._index = index
            self._loaded = True
            self._persist_meta()

    def _persist_meta(self) -> None:
        max_elements: Optional[int] = None
        if self._index is not None:
            try:
                max_elements = int(self._index.get_max_elements())
            except Exception:
                max_elements = None
        meta = {"dim": self.dim, "model_id": self.model_id, "max_elements": max_elements}
        try:
            self._meta_path.write_text(json.dumps(meta, indent=2))
        except Exception:
            # Meta is helpful but not critical; avoid breaking indexing/search due to a filesystem hiccup.
            pass

    def _as_2d(self, vector):
        import numpy as np

        v = np.asarray(vector, dtype="float32")
        if v.ndim == 1:
            v = v.reshape(1, -1)
        return v

    def persist(self) -> None:
        self._ensure_loaded()
        with self._lock:
            assert self._index is not None
            self._persist_meta()
            self._index.save_index(str(self._bin_path))

    def add_or_update(self, label: int, vector) -> None:
        self._ensure_loaded()
        with self._lock:
            assert self._index is not None
            vec2 = self._as_2d(vector)
            try:
                # hnswlib supports updating an existing label by re-adding it.
                # Avoid `replace_deleted=True` because older on-disk indices may have
                # been created without `allow_replace_deleted`, which would crash
                # indexing with: "Replacement of deleted elements is disabled in constructor".
                self._index.add_items(vec2, [label])
            except RuntimeError as e:
                # hnswlib index has a fixed max_elements; grow it when needed.
                if "exceeds the specified limit" in str(e).lower():
                    try:
                        current = int(self._index.get_current_count())
                    except Exception:
                        current = 0
                    self._maybe_resize(min_elements=max(current + 1, self._default_max_elements()))
                    self._index.add_items(vec2, [label])
                else:
                    raise

    def delete(self, label: int) -> None:
        """
        Mark an item deleted so it no longer appears in search results.
        (This keeps the index structure but removes the label from retrieval.)
        """
        self._ensure_loaded()
        with self._lock:
            assert self._index is not None
            try:
                self._index.mark_deleted(int(label))
            except Exception:
                pass

    def delete_many(self, labels: Iterable[int]) -> None:
        """
        Mark many items deleted in one lock hold. This is much faster than calling
        delete() in a loop from request code.
        """
        self._ensure_loaded()
        with self._lock:
            assert self._index is not None
            for lbl in labels:
                try:
                    self._index.mark_deleted(int(lbl))
                except Exception:
                    continue

    def add_many(self, labels: Iterable[int], vectors) -> None:
        self._ensure_loaded()
        with self._lock:
            assert self._index is not None
            label_list = list(labels)
            vec2 = self._as_2d(vectors)
            try:
                self._index.add_items(vec2, label_list)
            except RuntimeError as e:
                if "exceeds the specified limit" in str(e).lower():
                    try:
                        current = int(self._index.get_current_count())
                    except Exception:
                        current = 0
                    n = len(label_list)
                    self._maybe_resize(min_elements=max(current + n, self._default_max_elements()))
                    self._index.add_items(vec2, label_list)
                else:
                    raise

    def search(self, vector, *, k: int = 50) -> list[tuple[int, float]]:
        self._ensure_loaded()
        with self._lock:
            assert self._index is not None
            vec2 = self._as_2d(vector)
            try:
                current = int(self._index.get_current_count())
            except Exception:
                current = 0
            if current <= 0:
                return []

            k2 = max(1, min(int(k), current))
            # hnswlib requires ef >= k for reliable retrieval; otherwise it can raise:
            # "Cannot return the results in a contiguous 2D array. Probably ef or M is too small".
            try:
                max_e = int(self._index.get_max_elements())
            except Exception:
                max_e = current
            # If the index has many deleted items (or was built with poor connectivity),
            # requesting a large k can error. Back off k until it succeeds instead of
            # crashing the server.
            while True:
                try:
                    try:
                        self._index.set_ef(max(64, min(k2, max_e)))
                    except Exception:
                        pass
                    labels, distances = self._index.knn_query(vec2, k=k2)
                    break
                except RuntimeError as e:
                    if "contiguous 2d array" in str(e).lower():
                        if k2 <= 1:
                            return []
                        k2 = max(1, k2 // 2)
                        continue
                    raise
        out: list[tuple[int, float]] = []
        for lbl, dist in zip(labels[0].tolist(), distances[0].tolist()):
            # For cosine distance, similarity ~= 1 - dist
            out.append((int(lbl), float(1.0 - dist)))
        return out


def list_available_indices(index_base_dir: Path) -> list[IndexMeta]:
    # Legacy (multi-model) API; keep as a compatibility stub.
    # The single-model app uses only the base directory.
    meta_path = index_base_dir / "meta.json"
    if not meta_path.exists():
        return []
    try:
        meta = json.loads(meta_path.read_text())
        dim = int(meta.get("dim"))
        model_id = str(meta.get("model_id"))
        return [IndexMeta(dim=dim, model_id=model_id)]
    except Exception:
        return []
