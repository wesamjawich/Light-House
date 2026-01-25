from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple

from PIL import Image, ExifTags


_EXIF_TAGS = {v: k for k, v in ExifTags.TAGS.items()}


def _parse_exif_datetime(value: str) -> Optional[datetime]:
    # Common EXIF: "YYYY:MM:DD HH:MM:SS"
    try:
        dt = datetime.strptime(value, "%Y:%m:%d %H:%M:%S")
        return dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def get_date_taken(path: Path) -> Tuple[Optional[datetime], str]:
    try:
        with Image.open(path) as img:
            exif = getattr(img, "getexif", lambda: None)()
            if not exif:
                return None, "unknown"
            tag = _EXIF_TAGS.get("DateTimeOriginal")
            if tag is None:
                return None, "unknown"
            raw = exif.get(tag)
            if not raw:
                return None, "unknown"
            dt = _parse_exif_datetime(str(raw))
            if not dt:
                return None, "unknown"
            return dt, "exif"
    except Exception:
        return None, "unknown"

