from __future__ import annotations

from pathlib import Path
from typing import Optional, Tuple

from PIL import Image, ImageOps


def get_thumb_path(thumbs_dir: Path, photo_id: int) -> Path:
    # Simple sharding avoids huge single dirs.
    shard = f"{photo_id % 1000:03d}"
    return thumbs_dir / shard / f"{photo_id}.jpg"


def ensure_thumbnail(
    *,
    thumbs_dir: Path,
    photo_id: int,
    src_path: Path,
    max_size: int = 384,
) -> Optional[Tuple[Path, int, int]]:
    dst = get_thumb_path(thumbs_dir, photo_id)
    dst.parent.mkdir(parents=True, exist_ok=True)

    try:
        with Image.open(src_path) as img:
            img = ImageOps.exif_transpose(img)
            img.thumbnail((max_size, max_size))
            width, height = img.size
            img = img.convert("RGB")
            img.save(dst, format="JPEG", quality=82, optimize=True)
            return dst, width, height
    except Exception:
        return None

