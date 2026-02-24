from __future__ import annotations

from pathlib import Path


SUPPORTED_EXTS = {".jpg", ".jpeg", ".png"}


def is_supported_image(path: Path) -> bool:
    name = path.name
    if name.startswith("."):
        return False
    return path.suffix.lower() in SUPPORTED_EXTS
