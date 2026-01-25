from __future__ import annotations

from pathlib import Path


SUPPORTED_EXTS = {".jpg", ".jpeg", ".png"}


def is_supported_image(path: Path) -> bool:
    return path.suffix.lower() in SUPPORTED_EXTS

