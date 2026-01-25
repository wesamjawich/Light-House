from __future__ import annotations

from pathlib import Path

import typer
import uvicorn

from .config import get_app_paths
from .db import connect, migrate, tx
from .web import create_app


app = typer.Typer(add_completion=False)


@app.command()
def serve(
    host: str = "127.0.0.1",
    port: int = 8787,
    enable_clip: bool = True,
    clip_model: str = typer.Option("ViT-B-32", help="CLIP model name (open_clip)."),
    clip_pretrained: str = typer.Option("openai", help="CLIP pretrained tag (open_clip)."),
    clip_device: str = typer.Option("auto", help="Device: auto|cpu|cuda|mps."),
    access_log: bool = typer.Option(
        False,
        "--access-log/--no-access-log",
        help="Enable/disable HTTP access logs (can be noisy due to status polling).",
    ),
) -> None:
    """Run the local web app."""
    uvicorn.run(
        create_app(
            enable_clip=enable_clip,
            clip_model=clip_model,
            clip_pretrained=clip_pretrained,
            clip_device=clip_device,
        ),
        host=host,
        port=port,
        log_level="info",
        access_log=access_log,
    )


@app.command()
def track(path: Path) -> None:
    """Track a directory and start scanning it."""
    p = path.expanduser()
    if not p.exists() or not p.is_dir():
        raise typer.BadParameter("path must be an existing directory")

    paths = get_app_paths()
    conn = connect(paths.db_path)
    migrate(conn)

    with tx(conn):
        conn.execute(
            "INSERT OR IGNORE INTO tracked_roots(path, status, last_seen_at) VALUES(?, 'online', datetime('now'))",
            (str(p),),
        )
    typer.echo(f"Tracking: {p}")
    typer.echo("Start the server and press 'Scan all now' on /roots to index, or run: photobrowse serve")


@app.command()
def status() -> None:
    """Show tracked roots and counts."""
    paths = get_app_paths()
    conn = connect(paths.db_path)
    migrate(conn)
    roots = conn.execute("SELECT * FROM tracked_roots ORDER BY id").fetchall()
    photos = conn.execute("SELECT COUNT(*) AS c FROM photos").fetchone()["c"]
    typer.echo(f"Photos indexed: {photos}")
    for r in roots:
        typer.echo(f"[{r['id']}] {r['status']}: {r['path']}")


@app.command()
def untrack(
    root_id: int = typer.Option(None, "--id", help="Root id to stop tracking"),
    path: Path = typer.Option(None, "--path", help="Root path to stop tracking"),
) -> None:
    """Stop tracking a directory (does not delete any photos)."""
    if root_id is None and path is None:
        raise typer.BadParameter("provide --id or --path")
    if root_id is not None and path is not None:
        raise typer.BadParameter("provide only one of --id or --path")

    paths_cfg = get_app_paths()
    conn = connect(paths_cfg.db_path)
    migrate(conn)

    with tx(conn):
        if root_id is not None:
            conn.execute("DELETE FROM tracked_roots WHERE id=?", (root_id,))
            typer.echo(f"Untracked root id: {root_id}")
        else:
            p = path.expanduser()
            conn.execute("DELETE FROM tracked_roots WHERE path=?", (str(p),))
            typer.echo(f"Untracked: {p}")
