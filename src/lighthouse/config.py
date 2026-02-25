from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from platformdirs import user_data_dir


@dataclass(frozen=True)
class AppPaths:
    data_dir: Path
    db_path: Path
    thumbs_dir: Path
    index_dir: Path


def get_app_paths(app_name: str = "lighthouse") -> AppPaths:
    base = Path(user_data_dir(app_name))
    return AppPaths(
        data_dir=base,
        db_path=base / "lighthouse.sqlite3",
        thumbs_dir=base / "thumbs",
        index_dir=base / "index",
    )
