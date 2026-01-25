from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from .filetypes import is_supported_image


@dataclass(frozen=True)
class RootWatch:
    root_id: int
    path: Path


class _Handler(FileSystemEventHandler):
    def __init__(self, on_path: Callable[[Path], None]) -> None:
        self.on_path = on_path

    def on_created(self, event):  # type: ignore[no-untyped-def]
        if event.is_directory:
            return
        p = Path(event.src_path)
        if is_supported_image(p):
            self.on_path(p)

    def on_modified(self, event):  # type: ignore[no-untyped-def]
        if event.is_directory:
            return
        p = Path(event.src_path)
        if is_supported_image(p):
            self.on_path(p)

    def on_moved(self, event):  # type: ignore[no-untyped-def]
        if event.is_directory:
            return
        p = Path(event.dest_path)
        if is_supported_image(p):
            self.on_path(p)


class RootWatcher:
    def __init__(self, on_photo_path: Callable[[int, Path], None]) -> None:
        self._observer = Observer()
        self._on_photo_path = on_photo_path
        self._watches: dict[int, RootWatch] = {}

    def start(self) -> None:
        self._observer.start()

    def stop(self, timeout_s: float = 2.0) -> None:
        self._observer.stop()
        self._observer.join(timeout=timeout_s)

    def add_root(self, root_id: int, root_path: Path) -> None:
        if root_id in self._watches:
            return
        handler = _Handler(lambda p: self._on_photo_path(root_id, p))
        self._observer.schedule(handler, str(root_path), recursive=True)
        self._watches[root_id] = RootWatch(root_id=root_id, path=root_path)

    def remove_root(self, root_id: int) -> None:
        # watchdog doesn't support unscheduling by key cleanly; simplest is restart observer on changes.
        # For MVP, just forget and let periodic scan be authoritative.
        self._watches.pop(root_id, None)

    def is_alive(self) -> bool:
        return self._observer.is_alive()

    def wait_forever(self) -> None:
        while True:
            time.sleep(3600)

