from __future__ import annotations

import os
from pathlib import Path
from typing import Callable, Iterable, Iterator, Optional

from .filetypes import is_supported_image


def iter_images_recursive(
    root: Path,
    *,
    on_error: Optional[Callable[[OSError], None]] = None,
) -> Iterator[Path]:
    def _onerror(err: OSError) -> None:
        if on_error:
            on_error(err)

    for dirpath, dirnames, filenames in os.walk(root, followlinks=False, onerror=_onerror):
        # Skip hidden dirs like ".git" or ".Trash"
        dirnames[:] = [d for d in dirnames if not d.startswith(".")]
        for name in filenames:
            if name.startswith("."):
                continue
            p = Path(dirpath) / name
            if is_supported_image(p):
                yield p


def existing_paths(paths: Iterable[Path]) -> list[Path]:
    out: list[Path] = []
    for p in paths:
        try:
            if p.exists():
                out.append(p)
        except OSError:
            continue
    return out

