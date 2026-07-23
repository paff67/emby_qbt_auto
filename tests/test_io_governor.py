#!/usr/bin/env python3
from __future__ import annotations

import json
import sqlite3
import tempfile
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))


def _rows(db: Path, sql: str):
    con = sqlite3.connect(db)
    con.row_factory = sqlite3.Row
    rows = [dict(r) for r in con.execute(sql)]
    con.close()
    return rows


def test_upload_backpressure_does_not_block_disk_releasing_full_upload_and_records_bypass():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.file_batch import FileBatchService
    from qbt_orchestrator.io_governor import UploadBackpressurePolicy
    from qbt_orchestrator.runtime import TorrentJobRepository

    gib = 1024**3
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        repo = TorrentJobRepository(db, now=lambda: 1000)
        repo.enqueue(
            "old",
            None,
            "upload",
            {"local": "/tmp/old", "remote": "gcrypt:/old", "size": 21 * gib, "full_torrent": True},
            priority=1,
        )
        policy = UploadBackpressurePolicy(max_backlog_bytes=20 * gib, max_oldest_pending_sec=3600, now=lambda: 5000)
        service = FileBatchService(
            state_db=db,
            dry_run=False,
            host_downloads="/data/downloads",
            container_downloads="/downloads",
            remote="gcrypt:",
            backpressure_policy=policy,
        )
        snapshots = {
            "newhash": {
                "hash": "newhash",
                "name": "New",
                "category": "auto",
                "tags": "auto",
                "state": "uploading",
                "amount_left": 0,
                "size": 100,
                "progress": 1.0,
                "content_path": "/downloads/active/New",
            }
        }

        result = service.sync_completed(snapshots)

        assert result.eligible == 1
        assert result.enqueued == 1
        jobs = _rows(db, "select hash,job_type,state from torrent_jobs order by id")
        assert [j["hash"] for j in jobs] == ["old", "newhash"]
        metrics = _rows(
            db,
            "select component,metrics_json from metrics_snapshots "
            "where component='upload_backpressure' order by id",
        )
        assert metrics[-1]["component"] == "upload_backpressure"
        data = json.loads(metrics[-1]["metrics_json"])
        assert data["allow_new_upload_jobs"] is True
        assert data["pending_bytes"] == 21 * gib
        assert data["reason"] == "disk_releasing_bypass"


def test_io_governor_defaults_to_full_speed_without_bwlimit_even_under_pressure():
    from qbt_orchestrator.integrations.rclone import RcloneClient
    from qbt_orchestrator.io_governor import IoGovernor

    calls = []

    def runner(argv, input_text=None, timeout=None):
        calls.append((list(argv), input_text, timeout))
        return 0, json.dumps([{"Name": "a.mp4", "Size": 123}]), ""

    governor = IoGovernor(iowait_provider=lambda: 40.0, free_bytes_provider=lambda: 2 * 1024**3)
    client = RcloneClient(
        config_path="/root/.config/rclone/rclone.conf",
        transfers=4,
        checkers=8,
        limits_provider=governor.rclone_limits,
        runner=runner,
    )

    assert client.lsjson_size("gcrypt:/A/a.mp4") == 123
    argv = calls[0][0]
    assert "--transfers" in argv
    assert argv[argv.index("--transfers") + 1] == "4"
    assert argv[argv.index("--checkers") + 1] == "8"
    assert "--bwlimit" not in argv
    assert governor.last_snapshot()["iowait_percent"] == 40.0
    assert governor.last_snapshot()["state"] == "disabled"


def test_io_governor_can_throttle_when_explicitly_enabled():
    from qbt_orchestrator.integrations.rclone import RcloneClient
    from qbt_orchestrator.io_governor import IoGovernor

    calls = []

    def runner(argv, input_text=None, timeout=None):
        calls.append((list(argv), input_text, timeout))
        return 0, json.dumps([{"Name": "a.mp4", "Size": 123}]), ""

    governor = IoGovernor(iowait_provider=lambda: 40.0, free_bytes_provider=lambda: 6 * 1024**3, enabled=True)
    client = RcloneClient(
        config_path="/root/.config/rclone/rclone.conf",
        transfers=4,
        checkers=8,
        limits_provider=governor.rclone_limits,
        runner=runner,
    )

    assert client.lsjson_size("gcrypt:/A/a.mp4") == 123
    argv = calls[0][0]
    assert argv[argv.index("--transfers") + 1] == "1"
    assert argv[argv.index("--checkers") + 1] == "2"
    assert argv[argv.index("--bwlimit") + 1] == "2M"
    assert governor.last_snapshot()["state"] == "critical"


if __name__ == "__main__":
    inspect = __import__("inspect")
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn) and not inspect.signature(fn).parameters:
            fn()
    print("ok")
