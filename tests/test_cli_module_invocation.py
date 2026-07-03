#!/usr/bin/env python3
from __future__ import annotations

import subprocess
import sys
import tempfile
from pathlib import Path


def test_release_root_supports_python_m_qbt_orchestrator_cli_without_install():
    root = Path(__file__).resolve().parents[1]
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        result = subprocess.run(
            [sys.executable, "-m", "qbt_orchestrator.cli", "migrate", "--dry-run", "--state-db", str(db)],
            cwd=root,
            text=True,
            capture_output=True,
            timeout=30,
        )
        assert result.returncode == 0, result.stderr
        assert "migration dry-run" in result.stdout


if __name__ == "__main__":
    inspect = __import__("inspect")
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn) and not inspect.signature(fn).parameters:
            fn()
    print("ok")
