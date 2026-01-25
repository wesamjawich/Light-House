from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


SCHEMA_V1 = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;
PRAGMA synchronous=NORMAL;
PRAGMA busy_timeout=5000;

CREATE TABLE IF NOT EXISTS tracked_roots (
  id INTEGER PRIMARY KEY,
  path TEXT NOT NULL UNIQUE,
  added_at TEXT NOT NULL DEFAULT (datetime('now')),
  status TEXT NOT NULL DEFAULT 'unknown',
  last_seen_at TEXT,
  last_error TEXT,
  last_scan_started_at TEXT,
  last_scan_enumerated_at TEXT,
  last_scan_finished_at TEXT
);

CREATE TABLE IF NOT EXISTS photos (
  id INTEGER PRIMARY KEY,
  root_id INTEGER NOT NULL REFERENCES tracked_roots(id) ON DELETE CASCADE,
  path TEXT NOT NULL UNIQUE,
  rel_path TEXT NOT NULL,
  ext TEXT NOT NULL,
  size_bytes INTEGER NOT NULL,
  mtime_ns INTEGER NOT NULL,
  width INTEGER,
  height INTEGER,
  date_taken TEXT,
  date_source TEXT NOT NULL DEFAULT 'unknown',
  indexed_at TEXT NOT NULL DEFAULT (datetime('now')),
  embedding_dim INTEGER,
  embedding_model TEXT
);

CREATE INDEX IF NOT EXISTS idx_photos_root_id ON photos(root_id);
CREATE INDEX IF NOT EXISTS idx_photos_date_taken ON photos(date_taken);
"""

SCHEMA_V2 = """
-- Add scan bookkeeping fields (scan enumeration vs full ingest completion).
ALTER TABLE tracked_roots ADD COLUMN last_scan_enumerated_at TEXT;
"""


def connect(db_path: Path, *, check_same_thread: bool = True) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, check_same_thread=check_same_thread)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA busy_timeout=5000;")
    return conn


def migrate(conn: sqlite3.Connection) -> None:
    version = conn.execute("PRAGMA user_version;").fetchone()[0]
    if version == 0:
        conn.executescript(SCHEMA_V1)
        conn.execute("PRAGMA user_version=1;")
        conn.commit()
        version = 1
    if version == 1:
        # Best-effort schema update; ignore if column already exists.
        try:
            conn.executescript(SCHEMA_V2)
        except Exception:
            pass
        conn.execute("PRAGMA user_version=2;")
        conn.commit()
        version = 2
    if version == 2:
        # Idempotent performance indexes (safe to run on every startup).
        try:
            conn.execute("CREATE INDEX IF NOT EXISTS idx_photos_embedding_model ON photos(embedding_model);")
        except Exception:
            pass
        try:
            conn.execute("CREATE INDEX IF NOT EXISTS idx_photos_mtime_ns ON photos(mtime_ns);")
        except Exception:
            pass
        conn.commit()
        return
    raise RuntimeError(f"Unsupported DB schema version: {version}")


@contextmanager
def tx(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
