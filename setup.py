from __future__ import annotations

import re
from pathlib import Path

from setuptools import find_packages, setup


def _read_version() -> str:
    init_py = Path(__file__).parent / "src" / "lighthouse" / "__init__.py"
    m = re.search(r"^__version__\s*=\s*\"([^\"]+)\"\s*$", init_py.read_text(), re.M)
    if not m:
        raise RuntimeError("Could not find __version__ in src/lighthouse/__init__.py")
    return m.group(1)


setup(
    name="lighthouse",
    version=_read_version(),
    description="Local/offline photo indexing and semantic search (no rearranging).",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    include_package_data=True,
    package_data={"lighthouse": ["templates/*.html", "static/*.css", "static/*.js"]},
    install_requires=[
        "fastapi>=0.109",
        "uvicorn>=0.25",
        "jinja2>=3.1",
        "pillow>=10.0",
        "platformdirs>=4.0",
        "typer>=0.9",
        "watchdog>=3.0",
    ],
    extras_require={
        "minimal": [],
        "clip": [
            "numpy>=1.24",
            "hnswlib>=0.8.0",
            "torch>=2.0",
            "open_clip_torch>=2.24.0",
        ],
    },
    entry_points={"console_scripts": ["lighthouse=lighthouse.cli:app"]},
)
