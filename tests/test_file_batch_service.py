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


def test_file_batch_service_enqueues_completed_managed_torrent_once_with_vps_path_mapping():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.file_batch import FileBatchService

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        service = FileBatchService(
            state_db=db,
            dry_run=False,
            host_downloads="/data/downloads",
            container_downloads="/downloads",
            remote="gcrypt:",
        )
        snapshots = {
            "abcdef1234567890": {
                "hash": "abcdef1234567890",
                "name": "Movie:One",
                "category": "auto",
                "tags": "auto",
                "state": "uploading",
                "amount_left": 0,
                "size": 123,
                "progress": 1.0,
                "content_path": "/downloads/active/Movie One",
            },
            "holdhash": {"hash": "holdhash", "name": "Hold", "category": "auto", "tags": "hold", "amount_left": 0, "size": 50, "progress": 1.0},
            "incomplete": {"hash": "incomplete", "name": "Inc", "category": "auto", "tags": "auto", "amount_left": 1, "size": 50, "progress": 0.9},
        }

        result1 = service.sync_completed(snapshots)
        result2 = service.sync_completed(snapshots)

        assert result1.enqueued == 1
        assert result2.enqueued == 0
        jobs = _rows(db, "select hash,job_type,state,payload_json from torrent_jobs order by id")
        assert len(jobs) == 1
        assert jobs[0]["hash"] == "abcdef1234567890"
        payload = json.loads(jobs[0]["payload_json"])
        assert payload["local"] == "/data/downloads/active/Movie One"
        assert payload["remote"] == "gcrypt:/Movie_One-abcdef123456"
        assert payload["size"] == 123
        assert payload["full_torrent"] is True
        assert payload["source"] == "file_batch_completed_full_torrent"
        events = _rows(db, "select component,event_type,hash from events_v2 order by id")
        assert events[-1] == {"component": "file_batch", "event_type": "upload_queued", "hash": "abcdef1234567890"}


def test_file_batch_service_dry_run_records_without_inserting_job():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.file_batch import FileBatchService

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        service = FileBatchService(state_db=db, dry_run=True, host_downloads="/data/downloads", container_downloads="/downloads", remote="gcrypt:")
        snapshots = {"h1": {"hash": "h1", "name": "A", "category": "auto", "tags": "auto", "amount_left": 0, "size": 10, "progress": 1.0, "save_path": "/downloads/active"}}

        result = service.sync_completed(snapshots)

        assert result.enqueued == 0
        assert result.dry_run == 1
        assert _rows(db, "select * from torrent_jobs") == []
        actions = _rows(db, "select action_type,path,status,dry_run from action_log")
        assert actions == [{"action_type": "enqueue_upload", "path": "torrent_jobs/upload", "status": "dry_run", "dry_run": 1}]


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
    print("ok")
