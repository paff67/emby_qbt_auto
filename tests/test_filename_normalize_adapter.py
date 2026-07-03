#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))


class Proc:
    def __init__(self, returncode=0, stdout=""):
        self.returncode = returncode
        self.stdout = stdout


def test_filename_normalizer_script_runs_vps_json_interface_and_parses_confidence():
    from qbt_orchestrator.integrations.filename_normalize import FilenameNormalizeScript

    calls = []

    def runner(cmd, env, timeout):
        calls.append((cmd, env, timeout))
        assert cmd == ["/opt/qbt/gdrive-backfill/bin/jav_name_normalize.py", "489155.com@BBAN-582.mp4"]
        return Proc(
            0,
            json.dumps(
                {
                    "raw_name": "489155.com@BBAN-582.mp4",
                    "raw_basename": "489155.com@BBAN-582",
                    "cleaned_name": "BBAN-582",
                    "normalized_id": "BBAN-582",
                    "scrape_filename": "BBAN-582.mp4",
                    "confidence": 0.95,
                    "reason": "domain_prefix_removed_and_standard_jav_id_matched",
                }
            ),
        )

    normalizer = FilenameNormalizeScript(
        script_path="/opt/qbt/gdrive-backfill/bin/jav_name_normalize.py",
        runner=runner,
        timeout_sec=12,
    )

    result = normalizer.normalize("489155.com@BBAN-582.mp4")

    assert result["normalized_id"] == "BBAN-582"
    assert result["confidence"] == 0.95
    assert result["reason"] == "domain_prefix_removed_and_standard_jav_id_matched"
    assert calls[0][2] == 12


def test_cli_wires_filename_normalizer_script_into_media_pipeline(monkeypatch):
    from qbt_orchestrator.cli import _build_runtime
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.integrations.filename_normalize import FilenameNormalizeScript

    class Ns:
        config = None
        dry_run = False
        safety_interval = 0

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        script = Path(td) / "jav_name_normalize.py"
        script.write_text("#!/usr/bin/env python3\n", encoding="utf-8")
        migrate(db, dry_run=False)
        monkeypatch.setenv("QBT_ORCH_STATE_DB", str(db))
        monkeypatch.setenv("QBT_ORCH_DRY_RUN", "0")
        monkeypatch.setenv("QBT_ORCH_FILENAME_NORMALIZE", "1")
        monkeypatch.setenv("QBT_ORCH_FILENAME_NORMALIZE_SCRIPT", str(script))
        monkeypatch.setenv("QBT_ORCH_FILENAME_NORMALIZE_MIN_CONFIDENCE", "0.75")

        runtime, _ = _build_runtime(Ns(), db)

        service = runtime.media_pipeline_runner.service
        assert isinstance(service.normalizer, FilenameNormalizeScript)
        assert str(service.normalizer.script_path) == str(script)
        assert service.min_normalize_confidence == 0.75


if __name__ == "__main__":
    inspect = __import__("inspect")
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn) and not inspect.signature(fn).parameters:
            fn()
    print("ok")
