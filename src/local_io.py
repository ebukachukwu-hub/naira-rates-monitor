"""Local-only helper. Spark's own directory delete is unreliable on Windows,
so output folders are cleared with Python before each write. Not used on
Databricks, where storage paths are handled by the platform."""

import os
import shutil
import stat
from pathlib import Path


def _make_writable_and_retry(func, path, exc):
    os.chmod(path, stat.S_IWRITE)
    func(path)


def clear_local_output(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path, onexc=_make_writable_and_retry)
