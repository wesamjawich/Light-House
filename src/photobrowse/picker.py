from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Optional


def pick_directory(*, title: str = "Select a folder") -> Optional[Path]:
    """
    Open a native OS folder picker on the machine running the server.

    Returns a Path or None if the user cancelled / unsupported.
    """
    if os.environ.get("PHOTOBROWSE_NO_GUI") == "1":
        return None

    # macOS: prefer AppleScript (reliable, no extra deps).
    if sys.platform == "darwin":
        try:
            script = f'POSIX path of (choose folder with prompt "{title}")'
            res = subprocess.run(
                ["osascript", "-e", script],
                check=False,
                capture_output=True,
                text=True,
            )
            if res.returncode != 0:
                return None
            p = res.stdout.strip()
            if not p:
                return None
            return Path(p).expanduser()
        except Exception:
            pass

    # Cross-platform fallback: Tkinter (may not be available in some Python builds).
    try:
        import tkinter as tk
        from tkinter import filedialog

        root = tk.Tk()
        try:
            root.withdraw()
            try:
                root.attributes("-topmost", True)
            except Exception:
                pass
            selected = filedialog.askdirectory(title=title, mustexist=True)
        finally:
            try:
                root.destroy()
            except Exception:
                pass
        if not selected:
            return None
        return Path(selected).expanduser()
    except Exception:
        return None
