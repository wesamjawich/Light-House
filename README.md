# Light House (local/offline photo search)

Light House is a local-first system to index and browse photos **without moving them**. It recursively scans tracked directories for `*.jpg`, `*.jpeg`, and `*.png`, extracts dates (EXIF when available, otherwise file mtime), generates thumbnails, and (optionally) builds a semantic search index using CLIP embeddings.

## Goals

- Track multiple photo directories (including external SSDs)
- Tolerate safe eject / re-attach (keeps metadata; resumes indexing when paths reappear)
- Continuously indexes new/changed photos via filesystem watching + periodic checks
- Search by:
  - content (text → similar photos, via CLIP)
  - date filters (year/month/day)

## Quickstart

### 1) Install

Requires Python 3.9+.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip

# Minimal (metadata/date + thumbnails + web UI)
pip install -e ".[minimal]"

# Semantic search (adds torch + open_clip + hnswlib)
pip install -e ".[clip]"
```

If your `pip` is too old for editable installs with `pyproject.toml`, use a non-editable install:

```bash
pip install ".[clip]"
```

### 2) Run

```bash
lighthouse serve --port 8787
```

Open `http://127.0.0.1:8787`.

### 3) Track directories

Use the web UI on `/roots`, or:

```bash
lighthouse track "/Volumes/MySSD/Photos"
```

## Notes

- Light House never rearranges your photos. It writes its own data under your user data directory:
  - macOS: `~/Library/Application Support/lighthouse`
  - Linux: `~/.local/share/lighthouse`
  - Windows: `%APPDATA%\\lighthouse`
- You can inspect the database and indexing state at `http://127.0.0.1:8787/diagnostics` while the server is running.
- Content search requires model weights. Light House will try to load CLIP via `open_clip`. If weights are not available locally, you may need to download them once (then the system runs offline).
