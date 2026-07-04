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


def test_file_batch_service_builds_local_manifest_and_media_files_for_completed_directory():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.file_batch import FileBatchService

    with tempfile.TemporaryDirectory() as td:
        root = Path(td) / "downloads"
        movie = root / "active" / "Movie One"
        (movie / "extras").mkdir(parents=True)
        (movie / "A.mp4").write_bytes(b"a" * 100)
        (movie / "extras" / "B.nfo").write_bytes(b"b" * 10)
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        service = FileBatchService(state_db=db, dry_run=False, host_downloads=str(root), container_downloads="/downloads", remote="gcrypt:")

        result = service.sync_completed({
            "abcdef1234567890": {
                "hash": "abcdef1234567890",
                "name": "Movie:One",
                "category": "auto",
                "tags": "auto",
                "state": "uploading",
                "amount_left": 0,
                "size": 110,
                "progress": 1.0,
                "content_path": "/downloads/active/Movie One",
            }
        })

        assert result.enqueued == 1
        payload = json.loads(_rows(db, "select payload_json from torrent_jobs")[0]["payload_json"])
        assert payload["remote"] == "gcrypt:/Movie_One-abcdef123456"
        assert payload["size"] == 110
        assert [(f["relative_path"], f["size"]) for f in payload["files"]] == [("A.mp4", 100), ("extras/B.nfo", 10)]
        assert payload["files"][0]["remote_path"] == "gcrypt:/Movie_One-abcdef123456/A.mp4"
        assert payload["media_files"] == [{"remote_path": "gcrypt:/Movie_One-abcdef123456/A.mp4", "size": 100}]
        assert payload["copy_mode"] == "copy"


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


class BatchQbt:
    def __init__(self, files, piece_size=16 * 1024 * 1024, fail_on_post=False):
        self.files = files
        self.piece_size = piece_size
        self.fail_on_post = fail_on_post
        self.calls = []

    def torrent_files(self, hash):
        self.calls.append(("torrent_files", hash))
        return list(self.files)

    def torrent_properties(self, hash):
        self.calls.append(("torrent_properties", hash))
        return {"piece_size": self.piece_size}

    def qbt_post(self, path, payload):
        self.calls.append((path, payload))
        if self.fail_on_post:
            raise RuntimeError("qbt filePrio failed")


def test_file_batch_service_creates_pipeline_batch_with_reservation_and_file_priorities():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.file_batch import FileBatchService

    gib = 1024**3
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        qbt = BatchQbt([
            {"index": 0, "name": "A.mp4", "size": gib, "progress": 0, "priority": 0},
            {"index": 1, "name": "B.mp4", "size": gib, "progress": 0, "priority": 0},
            {"index": 2, "name": "C.mp4", "size": 5 * gib, "progress": 0, "priority": 0},
        ])
        service = FileBatchService(
            state_db=db,
            dry_run=False,
            host_downloads="/data/downloads",
            container_downloads="/downloads",
            remote="gcrypt:",
            qbt=qbt,
            disk_floor_bytes=2 * gib,
            filesystem_slack_bytes=128 * 1024**2,
            now=lambda: 10_000,
        )

        result = service.sync_completed(
            {
                "big": {
                    "hash": "big",
                    "name": "Big",
                    "category": "auto",
                    "tags": "auto",
                    "state": "stoppedDL",
                    "amount_left": 7 * gib,
                    "size": 7 * gib,
                    "progress": 0.0,
                }
            },
            free_bytes=6 * gib,
            sync_healthy=True,
        )

        assert result.batches_created == 1
        assert ("/api/v2/torrents/filePrio", {"hash": "big", "id": "0|1", "priority": "1"}) in qbt.calls
        assert ("/api/v2/torrents/filePrio", {"hash": "big", "id": "2", "priority": "0"}) in qbt.calls
        assert ("/api/v2/torrents/start", {"hashes": "big"}) in qbt.calls
        assert not any(call[0] == "/api/v2/torrents/delete" for call in qbt.calls)

        batch = _rows(db, "select hash,batch_no,state,mode,indices_json,total_bytes,reserved_bytes,piece_size,selected_extents,piece_spill_overhead_bytes,priority_applied from torrent_batches")[0]
        assert batch["hash"] == "big"
        assert batch["batch_no"] == 1
        assert batch["state"] == "downloading"
        assert batch["mode"] == "pipeline"
        assert json.loads(batch["indices_json"]) == [0, 1]
        assert batch["total_bytes"] == 2 * gib
        assert batch["reserved_bytes"] == 2 * gib + 32 * 1024**2 + 128 * 1024**2
        assert batch["piece_size"] == 16 * 1024**2
        assert batch["selected_extents"] == 1
        assert batch["piece_spill_overhead_bytes"] == 32 * 1024**2
        assert batch["priority_applied"] == 1

        reservation = _rows(db, "select hash,batch_id,kind,bytes,state,expires_at,reason from resource_reservations where kind='batch'")[0]
        assert reservation["hash"] == "big"
        assert reservation["batch_id"] == 1
        assert reservation["bytes"] == batch["reserved_bytes"]
        assert reservation["state"] == "active"
        assert reservation["expires_at"] == 13_600
        assert reservation["reason"] == "batch_pipeline_reserved"


def test_file_batch_service_blocks_pipeline_batch_when_reservation_budget_is_insufficient():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.file_batch import FileBatchService

    gib = 1024**3
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        con.execute(
            "insert into resource_reservations(hash,kind,bytes,state,created_at,expires_at,reason) values(?,?,?,?,?,?,?)",
            ("other", "active_download", 2 * gib, "active", 100, 10_000, "existing"),
        )
        con.commit(); con.close()
        qbt = BatchQbt([{"index": 0, "name": "A.mp4", "size": gib, "progress": 0, "priority": 0}])
        service = FileBatchService(state_db=db, dry_run=False, qbt=qbt, disk_floor_bytes=2 * gib, now=lambda: 1_000)

        result = service.sync_completed(
            {"big": {"hash": "big", "category": "auto", "tags": "auto", "state": "stoppedDL", "amount_left": gib, "size": gib, "progress": 0.0}},
            free_bytes=4 * gib,
            sync_healthy=True,
        )

        assert result.batches_created == 0
        assert qbt.calls == [("torrent_files", "big"), ("torrent_properties", "big")]
        assert _rows(db, "select * from torrent_batches") == []
        assert _rows(db, "select * from resource_reservations where kind='batch'") == []
        decision = _rows(db, "select component,hash,decision,reason_code,data_json from decision_log where component='file_batch' order by id desc limit 1")[0]
        assert decision["hash"] == "big"
        assert decision["decision"] == "prefetch_blocked"
        assert decision["reason_code"] == "batch_budget_insufficient"


def test_file_batch_service_live_verify_blocks_new_batch_without_canary_before_file_probe():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.file_batch import FileBatchService

    gib = 1024**3
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        qbt = BatchQbt([{"index": 0, "name": "A.mp4", "size": gib, "progress": 0, "priority": 0}])
        service = FileBatchService(
            state_db=db,
            dry_run=False,
            qbt=qbt,
            disk_floor_bytes=2 * gib,
            batch_live_verify=True,
            now=lambda: 1_000,
        )

        result = service.sync_completed(
            {"big": {"hash": "big", "category": "auto", "tags": "auto", "state": "stoppedDL", "amount_left": gib, "size": gib, "progress": 0.0}},
            free_bytes=5 * gib,
            sync_healthy=True,
        )

        assert result.batches_created == 0
        assert result.batches_blocked == 1
        assert qbt.calls == []
        decision = _rows(db, "select component,hash,decision,reason_code from decision_log order by id desc limit 1")[0]
        assert decision == {"component": "file_batch", "hash": "big", "decision": "prefetch_blocked", "reason_code": "live_verify_no_canary_match"}


def test_file_batch_service_live_verify_canary_limits_new_batches_per_tick_before_second_probe():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.file_batch import FileBatchService

    gib = 1024**3
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        qbt = BatchQbt([{"index": 0, "name": "A.mp4", "size": gib, "progress": 0, "priority": 0}])
        service = FileBatchService(
            state_db=db,
            dry_run=False,
            qbt=qbt,
            disk_floor_bytes=2 * gib,
            batch_live_verify=True,
            batch_allow_tag="batch-canary",
            batch_max_new_per_tick=1,
            now=lambda: 1_000,
        )

        result = service.sync_completed(
            {
                "h1": {"hash": "h1", "category": "auto", "tags": "auto,batch-canary", "state": "stoppedDL", "amount_left": gib, "size": gib, "progress": 0.0},
                "h2": {"hash": "h2", "category": "auto", "tags": "auto,batch-canary", "state": "stoppedDL", "amount_left": gib, "size": gib, "progress": 0.0},
            },
            free_bytes=8 * gib,
            sync_healthy=True,
        )

        assert result.batches_created == 1
        assert result.batches_blocked == 1
        assert ("torrent_files", "h1") in qbt.calls
        assert ("torrent_files", "h2") not in qbt.calls
        batches = _rows(db, "select hash,state from torrent_batches order by id")
        assert batches == [{"hash": "h1", "state": "downloading"}]
        decision = _rows(db, "select hash,decision,reason_code from decision_log order by id desc limit 1")[0]
        assert decision == {"hash": "h2", "decision": "prefetch_blocked", "reason_code": "live_verify_new_batch_tick_cap"}


def test_file_batch_service_live_verify_blocks_batch_above_reserved_size_cap():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.file_batch import FileBatchService

    gib = 1024**3
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        qbt = BatchQbt([{"index": 0, "name": "A.mp4", "size": 2 * gib, "progress": 0, "priority": 0}], piece_size=16 * 1024**2)
        service = FileBatchService(
            state_db=db,
            dry_run=False,
            qbt=qbt,
            disk_floor_bytes=2 * gib,
            filesystem_slack_bytes=128 * 1024**2,
            batch_live_verify=True,
            batch_allow_hashes={"big"},
            batch_max_live_batch_bytes=gib,
            now=lambda: 1_000,
        )

        result = service.sync_completed(
            {"big": {"hash": "big", "category": "auto", "tags": "auto", "state": "stoppedDL", "amount_left": 2 * gib, "size": 2 * gib, "progress": 0.0}},
            free_bytes=8 * gib,
            sync_healthy=True,
        )

        assert result.batches_created == 0
        assert result.batches_blocked == 1
        assert ("torrent_files", "big") in qbt.calls
        assert not any(call[0] == "/api/v2/torrents/filePrio" for call in qbt.calls)
        decision = _rows(db, "select hash,decision,reason_code from decision_log order by id desc limit 1")[0]
        assert decision == {"hash": "big", "decision": "prefetch_blocked", "reason_code": "live_verify_batch_size_cap"}


def test_file_batch_service_pipeline_selects_real_media_and_skips_junk_candidates():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.file_batch import FileBatchService

    gib = 1024**3
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        qbt = BatchQbt(
            [
                {"index": 0, "name": "Movie/2026 世界杯.url", "size": 1024, "progress": 0, "priority": 0},
                {"index": 1, "name": "Movie/台湾uu美少女直播 20年信誉保证服务全球.mp4", "size": 24 * 1024**2, "progress": 0, "priority": 0},
                {"index": 2, "name": "Movie/Movie.mp4", "size": gib, "progress": 0, "priority": 0},
            ]
        )
        service = FileBatchService(
            state_db=db,
            dry_run=False,
            qbt=qbt,
            disk_floor_bytes=2 * gib,
            filesystem_slack_bytes=128 * 1024**2,
            now=lambda: 1_000,
        )

        result = service.sync_completed(
            {"movie": {"hash": "movie", "name": "Movie", "category": "auto", "tags": "auto", "state": "stoppedDL", "amount_left": 2 * gib, "size": 2 * gib, "progress": 0.0}},
            free_bytes=5 * gib,
            sync_healthy=True,
        )

        assert result.batches_created == 1
        batch = _rows(db, "select indices_json,total_bytes from torrent_batches")[0]
        assert json.loads(batch["indices_json"]) == [2]
        assert batch["total_bytes"] == gib
        assert ("/api/v2/torrents/filePrio", {"hash": "movie", "id": "2", "priority": "1"}) in qbt.calls
        assert ("/api/v2/torrents/filePrio", {"hash": "movie", "id": "0|1", "priority": "0"}) in qbt.calls


def test_file_batch_service_pipeline_reserves_remaining_bytes_for_partial_file():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.file_batch import FileBatchService

    gib = 1024**3
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        qbt = BatchQbt([{"index": 0, "name": "Big/Big.mp4", "size": 5 * gib, "progress": 0.8, "priority": 1}], piece_size=16 * 1024**2)
        service = FileBatchService(
            state_db=db,
            dry_run=False,
            qbt=qbt,
            disk_floor_bytes=2 * gib,
            filesystem_slack_bytes=128 * 1024**2,
            batch_live_verify=True,
            batch_allow_hashes={"big"},
            batch_max_live_batch_bytes=1536 * 1024**2,
            now=lambda: 1_000,
        )

        result = service.sync_completed(
            {"big": {"hash": "big", "name": "Big", "category": "auto", "tags": "auto", "state": "downloading", "amount_left": gib, "size": 5 * gib, "progress": 0.8}},
            free_bytes=4 * gib,
            sync_healthy=True,
        )

        assert result.batches_created == 1
        batch = _rows(db, "select indices_json,total_bytes,reserved_bytes,piece_spill_overhead_bytes from torrent_batches")[0]
        assert json.loads(batch["indices_json"]) == [0]
        assert batch["total_bytes"] == 5 * gib
        assert batch["piece_spill_overhead_bytes"] == 32 * 1024**2
        assert batch["reserved_bytes"] == gib + 32 * 1024**2 + 128 * 1024**2


def test_file_batch_service_pipeline_dry_run_and_qbt_failure_do_not_leave_active_reservation():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.file_batch import FileBatchService

    gib = 1024**3
    snapshots = {"big": {"hash": "big", "category": "auto", "tags": "auto", "state": "stoppedDL", "amount_left": gib, "size": gib, "progress": 0.0}}
    files = [{"index": 0, "name": "A.mp4", "size": gib, "progress": 0, "priority": 0}]
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        dry = FileBatchService(state_db=db, dry_run=True, qbt=BatchQbt(files), disk_floor_bytes=2 * gib, now=lambda: 1_000)

        result = dry.sync_completed(snapshots, free_bytes=5 * gib, sync_healthy=True)

        assert result.batches_created == 0
        assert _rows(db, "select * from torrent_batches") == []
        assert _rows(db, "select * from resource_reservations where kind='batch'") == []
        action = _rows(db, "select action_type,path,status,dry_run from action_log where action_type='batch_pipeline'")[0]
        assert action == {"action_type": "batch_pipeline", "path": "torrent_batches", "status": "dry_run", "dry_run": 1}

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        failing = FileBatchService(state_db=db, dry_run=False, qbt=BatchQbt(files, fail_on_post=True), disk_floor_bytes=2 * gib, now=lambda: 1_000)

        result = failing.sync_completed(snapshots, free_bytes=5 * gib, sync_healthy=True)

        assert result.batches_created == 0
        batch = _rows(db, "select state,priority_applied from torrent_batches")[0]
        assert batch == {"state": "failed", "priority_applied": 0}
        reservation = _rows(db, "select state,released_at,reason from resource_reservations where kind='batch'")[0]
        assert reservation == {"state": "released", "released_at": 1_000, "reason": "qbt_apply_failed"}


def test_file_batch_service_queues_downloaded_pipeline_batch_without_delete():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.file_batch import FileBatchService

    gib = 1024**3
    with tempfile.TemporaryDirectory() as td:
        root = Path(td) / "downloads"
        torrent_dir = root / "active" / "Big"
        (torrent_dir / "extras").mkdir(parents=True)
        (torrent_dir / "A.mp4").write_bytes(b"a" * 10)
        (torrent_dir / "extras" / "B.nfo").write_bytes(b"b" * 5)
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        con.execute(
            "insert into torrent_batches(id,hash,batch_no,state,mode,indices_json,total_bytes,reserved_bytes,created_at,updated_at) values(?,?,?,?,?,?,?,?,?,?)",
            (1, "big", 1, "downloading", "pipeline", "[0,1]", 15, 100, 900, 900),
        )
        con.execute(
            "insert into resource_reservations(hash,batch_id,kind,bytes,state,created_at,expires_at,reason) values(?,?,?,?,?,?,?,?)",
            ("big", 1, "batch", 100, "active", 900, 4600, "batch_pipeline_reserved"),
        )
        con.commit(); con.close()
        qbt = BatchQbt([
            {"index": 0, "name": "A.mp4", "size": 10, "progress": 1.0, "priority": 1},
            {"index": 1, "name": "extras/B.nfo", "size": 5, "progress": 1.0, "priority": 1},
            {"index": 2, "name": "C.mp4", "size": gib, "progress": 0.0, "priority": 0},
        ])
        service = FileBatchService(
            state_db=db,
            dry_run=False,
            host_downloads=str(root),
            container_downloads="/downloads",
            remote="gcrypt:",
            qbt=qbt,
            disk_floor_bytes=2 * gib,
            max_inflight_batches_per_torrent=1,
            now=lambda: 1_000,
        )

        result = service.sync_completed(
            {
                "big": {
                    "hash": "big",
                    "name": "Big",
                    "category": "auto",
                    "tags": "auto",
                    "state": "downloading",
                    "amount_left": gib,
                    "size": gib + 15,
                    "progress": 0.01,
                    "content_path": "/downloads/active/Big",
                }
            },
            free_bytes=6 * gib,
            sync_healthy=True,
        )

        assert result.enqueued == 1
        assert ("/api/v2/torrents/filePrio", {"hash": "big", "id": "0|1", "priority": "0"}) in qbt.calls
        assert not any(call[0] == "/api/v2/torrents/delete" for call in qbt.calls)

        job = _rows(db, "select id,hash,batch_id,job_type,state,payload_json from torrent_jobs")[0]
        assert job["hash"] == "big"
        assert job["batch_id"] == 1
        assert job["job_type"] == "upload"
        assert job["state"] == "queued"
        payload = json.loads(job["payload_json"])
        assert payload["full_torrent"] is False
        assert payload["copy_mode"] == "copy_files"
        assert payload["source"] == "file_batch_pipeline_batch"
        assert payload["local"] == str(torrent_dir)
        assert payload["remote"] == "gcrypt:/Big-big"
        assert payload["size"] == 15
        assert [(f["relative_path"], f["size"]) for f in payload["files"]] == [("A.mp4", 10), ("extras/B.nfo", 5)]
        assert payload["files"][0]["local_path"] == str(torrent_dir / "A.mp4")
        assert payload["files"][0]["remote_path"] == "gcrypt:/Big-big/A.mp4"
        assert payload["media_files"] == [{"remote_path": "gcrypt:/Big-big/A.mp4", "size": 10}]

        batch = _rows(db, "select state,downloaded_bytes,upload_job_id,local_pinned_bytes,upload_queued_at from torrent_batches where id=1")[0]
        assert batch == {
            "state": "upload_queued",
            "downloaded_bytes": 15,
            "upload_job_id": job["id"],
            "local_pinned_bytes": 15,
            "upload_queued_at": 1_000,
        }
        reservations = _rows(db, "select kind,bytes,state,released_at,reason from resource_reservations order by id")
        assert reservations == [
            {"kind": "batch", "bytes": 100, "state": "released", "released_at": 1_000, "reason": "batch_downloaded_upload_queued"},
            {"kind": "cleanup_pending", "bytes": 15, "state": "active", "released_at": None, "reason": "batch_upload_queued"},
        ]


if __name__ == "__main__":
    inspect = __import__("inspect")
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn) and not inspect.signature(fn).parameters:
            fn()
    print("ok")
