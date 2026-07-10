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


def test_file_batch_service_completed_manifest_excludes_qbt_priority_zero_files():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.file_batch import FileBatchService

    class CompletedQbt:
        def __init__(self):
            self.calls = []

        def torrent_files(self, h):
            self.calls.append(h)
            return [
                {"index": 0, "name": "dori-136/dori-136.mp4", "size": 100, "progress": 1.0, "priority": 1},
                {"index": 1, "name": "dori-136/台 妹 子 線 上 現 場 直 播.mp4", "size": 20, "progress": 0.003, "priority": 0},
                {"index": 2, "name": "dori-136/最 新 位 址 獲 取.txt", "size": 1, "progress": 0, "priority": 0},
            ]

    with tempfile.TemporaryDirectory() as td:
        root = Path(td) / "downloads"
        movie = root / "active" / "dori-136"
        movie.mkdir(parents=True)
        (movie / "dori-136.mp4").write_bytes(b"a" * 100)
        (movie / "台 妹 子 線 上 現 場 直 播.mp4").write_bytes(b"b" * 20)
        (movie / "最 新 位 址 獲 取.txt").write_bytes(b"c")
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        qbt = CompletedQbt()
        service = FileBatchService(state_db=db, dry_run=False, host_downloads=str(root), container_downloads="/downloads", remote="gcrypt:", qbt=qbt)

        result = service.sync_completed({
            "h1": {
                "hash": "h1",
                "name": "dori-136.torrent",
                "category": "auto",
                "tags": "auto,checked",
                "state": "stoppedUP",
                "amount_left": 0,
                "size": 121,
                "progress": 1.0,
                "content_path": "/downloads/active/dori-136",
            }
        })

        assert result.enqueued == 1
        assert qbt.calls == ["h1"]
        payload = json.loads(_rows(db, "select payload_json from torrent_jobs")[0]["payload_json"])
        assert payload["size"] == 100
        assert payload["copy_mode"] == "copy_files"
        assert [(f["relative_path"], f["size"]) for f in payload["files"]] == [("dori-136.mp4", 100)]
        assert payload["media_files"] == [{"remote_path": "gcrypt:/dori-136.torrent-h1/dori-136.mp4", "size": 100}]


def test_file_batch_service_enqueues_completed_single_file_when_qbt_content_path_is_file():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.file_batch import FileBatchService

    class SingleFileQbt:
        def torrent_files(self, h):
            assert h == "9307b3e6da70"
            return [
                {
                    "index": 0,
                    "name": "DASS-592/hhd800.com@DASS-592.mp4",
                    "size": 100,
                    "progress": 1.0,
                    "priority": 1,
                }
            ]

    with tempfile.TemporaryDirectory() as td:
        root = Path(td) / "downloads"
        movie = root / "active" / "DASS-592" / "hhd800.com@DASS-592.mp4"
        movie.parent.mkdir(parents=True)
        movie.write_bytes(b"a" * 100)
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        service = FileBatchService(
            state_db=db,
            dry_run=False,
            host_downloads=str(root),
            container_downloads="/downloads",
            remote="gcrypt:",
            qbt=SingleFileQbt(),
        )

        result = service.sync_completed(
            {
                "9307b3e6da70": {
                    "hash": "9307b3e6da70",
                    "name": "DASS-592",
                    "category": "auto",
                    "tags": "auto, checked",
                    "state": "stoppedUP",
                    "amount_left": 0,
                    "size": 100,
                    "progress": 1.0,
                    "save_path": "/downloads/active",
                    "content_path": "/downloads/active/DASS-592/hhd800.com@DASS-592.mp4",
                }
            }
        )

        assert result.enqueued == 1
        payload = json.loads(_rows(db, "select payload_json from torrent_jobs")[0]["payload_json"])
        assert payload["local"] == str(root / "active" / "DASS-592")
        assert payload["remote"] == "gcrypt:/DASS-592-9307b3e6da70"
        assert payload["copy_mode"] == "copy_files"
        assert [(f["relative_path"], f["local_path"], f["size"]) for f in payload["files"]] == [
            ("hhd800.com@DASS-592.mp4", str(movie), 100)
        ]
        assert payload["media_files"] == [{"remote_path": "gcrypt:/DASS-592-9307b3e6da70/hhd800.com@DASS-592.mp4", "size": 100}]
        events = _rows(db, "select event_type from events_v2 order by id")
        assert {"event_type": "completed_manifest_empty_after_qbt_filter"} not in events


def test_file_batch_service_queues_downloaded_single_file_batch_when_qbt_content_path_is_file():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.file_batch import FileBatchService

    class SingleFileBatchQbt:
        def __init__(self):
            self.posts = []

        def torrent_files(self, h):
            assert h == "d95978e5"
            return [
                {
                    "index": 0,
                    "name": "KIT-002/hhd800.com@KIT-002.mp4",
                    "size": 120,
                    "progress": 1.0,
                    "priority": 1,
                }
            ]

        def qbt_post(self, path, payload):
            self.posts.append((path, payload))

    with tempfile.TemporaryDirectory() as td:
        root = Path(td) / "downloads"
        movie = root / "active" / "KIT-002" / "hhd800.com@KIT-002.mp4"
        movie.parent.mkdir(parents=True)
        movie.write_bytes(b"b" * 120)
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        con.execute(
            """
            insert into torrent_batches(hash,batch_no,state,mode,indices_json,total_bytes,downloaded_bytes,reserved_bytes,piece_size,created_at,updated_at)
            values(?,?,?,?,?,?,?,?,?,?,?)
            """,
            ("d95978e5", 1, "downloading", "pipeline", "[0]", 120, 0, 120, 2097152, 1000, 1000),
        )
        con.commit()
        con.close()
        qbt = SingleFileBatchQbt()
        service = FileBatchService(
            state_db=db,
            dry_run=False,
            host_downloads=str(root),
            container_downloads="/downloads",
            remote="gcrypt:",
            qbt=qbt,
            batch_max_new_per_tick=0,
            now=lambda: 2000,
        )

        result = service.sync_completed(
            {
                "d95978e5": {
                    "hash": "d95978e5",
                    "name": "KIT-002",
                    "category": "auto",
                    "tags": "auto",
                    "state": "stalledDL",
                    "amount_left": 1,
                    "size": 120,
                    "progress": 0.99,
                    "save_path": "/downloads/active",
                    "content_path": "/downloads/active/KIT-002/hhd800.com@KIT-002.mp4",
                }
            },
            free_bytes=10 * 1024**3,
        )

        assert result.enqueued == 1
        payload = json.loads(_rows(db, "select payload_json from torrent_jobs where job_type='upload'")[0]["payload_json"])
        assert payload["local"] == str(root / "active" / "KIT-002")
        assert payload["copy_mode"] == "copy_files"
        assert [(f["relative_path"], f["local_path"], f["size"]) for f in payload["files"]] == [
            ("hhd800.com@KIT-002.mp4", str(movie), 120)
        ]
        assert qbt.posts == [("/api/v2/torrents/filePrio", {"hash": "d95978e5", "id": "0", "priority": "0"})]


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


def test_file_batch_skips_all_inventory_calls_when_scheduler_mode_disallows_batch():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.file_batch import FileBatchService

    gib = 1024**3
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        qbt = BatchQbt([{"index": 0, "name": "A.mp4", "size": 5 * gib, "progress": 0.0, "priority": 0}])
        service = FileBatchService(
            state_db=db,
            dry_run=False,
            qbt=qbt,
            disk_floor_bytes=2 * gib,
            filesystem_slack_bytes=128 * 1024**2,
        )

        result = service.sync_completed(
            {"h1": {"hash": "h1", "category": "auto", "state": "stoppedDL", "size": 5 * gib, "amount_left": 5 * gib}},
            free_bytes=6 * gib,
            sync_healthy=True,
            scheduler_mode="drain",
        )

        assert qbt.calls == []
        assert result.batches_created == 0
        assert result.batches_blocked == 1
        assert result.blocked_reasons == {"mode_disallows_batch": 1}


def test_file_batch_skips_all_inventory_calls_when_global_budget_is_below_minimum():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.file_batch import FileBatchService

    gib = 1024**3
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        qbt = BatchQbt([{"index": 0, "name": "A.mp4", "size": 5 * gib, "progress": 0.0, "priority": 0}])
        service = FileBatchService(
            state_db=db,
            dry_run=False,
            qbt=qbt,
            disk_floor_bytes=2 * gib,
            filesystem_slack_bytes=128 * 1024**2,
        )

        result = service.sync_completed(
            {"h1": {"hash": "h1", "category": "auto", "state": "stoppedDL", "size": 5 * gib, "amount_left": 5 * gib}},
            free_bytes=2 * gib + 120 * 1024**2,
            sync_healthy=True,
            scheduler_mode="normal",
        )

        assert qbt.calls == []
        assert result.batches_created == 0
        assert result.batches_blocked == 1
        assert result.blocked_reasons == {"global_batch_budget_below_minimum": 1}


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
        assert not any(call[0] == "/api/v2/torrents/start" for call in qbt.calls)
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
        intent = _rows(
            db,
            "select component,hash,intent,priority,expires_at,data_json from scheduler_intents where hash='big'",
        )[0]
        assert {
            "component": intent["component"],
            "hash": intent["hash"],
            "intent": intent["intent"],
            "priority": intent["priority"],
            "expires_at": intent["expires_at"],
            "data": json.loads(intent["data_json"]),
        } == {
            "component": "batch",
            "hash": "big",
            "intent": "protect_batch",
            "priority": 20,
            "expires_at": 13_600,
            "data": {"batch_id": 1},
        }


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
        assert result.batches_blocked == 1
        assert result.blocked_reasons == {"global_batch_budget_below_minimum": 1}
        assert qbt.calls == []
        assert _rows(db, "select * from torrent_batches") == []
        assert _rows(db, "select * from resource_reservations where kind='batch'") == []
        decision = _rows(db, "select component,hash,decision,reason_code,data_json from decision_log where component='file_batch' order by id desc limit 1")[0]
        assert decision["hash"] == "big"
        assert decision["decision"] == "prefetch_blocked"
        assert decision["reason_code"] == "global_batch_budget_below_minimum"


def test_file_batch_budget_reports_but_does_not_subtract_current_pinned_inventory():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.file_batch import FileBatchService

    gib = 1024**3
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        con.execute(
            "insert into resource_reservations("
            "hash,kind,accounting_class,bytes,state,created_at,reason) values(?,?,?,?,?,?,?)",
            ("pinned", "cleanup_pending", "current_pinned", 10 * gib, "active", 100, "verified_waiting_cleanup"),
        )
        con.commit(); con.close()
        qbt = BatchQbt([{"index": 0, "name": "A.mp4", "size": gib, "progress": 0, "priority": 0}])
        service = FileBatchService(state_db=db, dry_run=False, qbt=qbt, disk_floor_bytes=2 * gib, now=lambda: 1_000)

        result = service.sync_completed(
            {"new": {"hash": "new", "category": "auto", "tags": "auto", "state": "stoppedDL", "amount_left": gib, "size": gib, "progress": 0.0}},
            free_bytes=4 * gib,
            sync_healthy=True,
        )

        assert result.batches_created == 1
        claim = _rows(
            db,
            "select accounting_class,owner,lease_generation,last_observed_at "
            "from resource_reservations where kind='batch'",
        )[0]
        assert claim == {
            "accounting_class": "future_growth",
            "owner": "file_batch",
            "lease_generation": 0,
            "last_observed_at": 1_000,
        }


def test_file_batch_service_live_verify_with_explicit_canary_blocks_nonmatching_hash_before_file_probe():
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
            batch_allow_hashes={"other"},
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


def test_file_batch_service_pipeline_blocks_ad_txt_but_keeps_informational_txt_downloadable():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.file_batch import FileBatchService

    gib = 1024**3
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        qbt = BatchQbt(
            [
                {"index": 0, "name": "Movie/Movie.mp4", "size": gib, "progress": 0, "priority": 0},
                {"index": 1, "name": "Movie/最新地址 收藏不迷路.txt", "size": 1024, "progress": 0, "priority": 1},
                {"index": 2, "name": "Movie/广告.url", "size": 1024, "progress": 0, "priority": 1},
                {"index": 3, "name": "Movie/解压密码.txt", "size": 1024, "progress": 0, "priority": 1},
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
            {"movie": {"hash": "movie", "name": "Movie", "category": "auto", "tags": "auto", "state": "stoppedDL", "amount_left": gib, "size": gib, "progress": 0.0}},
            free_bytes=5 * gib,
            sync_healthy=True,
        )

        assert result.batches_created == 1
        assert ("/api/v2/torrents/filePrio", {"hash": "movie", "id": "0", "priority": "1"}) in qbt.calls
        assert ("/api/v2/torrents/filePrio", {"hash": "movie", "id": "1|2", "priority": "0"}) in qbt.calls
        assert not any(call == ("/api/v2/torrents/filePrio", {"hash": "movie", "id": "3", "priority": "0"}) for call in qbt.calls)
        assert not any(call == ("/api/v2/torrents/filePrio", {"hash": "movie", "id": "1|2|3", "priority": "0"}) for call in qbt.calls)


def test_file_batch_service_pipeline_uses_dynamic_programming_to_skip_oversized_early_file():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.file_batch import FileBatchService

    gib = 1024**3
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        qbt = BatchQbt(
            [
                {"index": 0, "name": "Big/Oversized.mp4", "size": 3 * gib, "progress": 0, "priority": 0},
                {"index": 1, "name": "Big/Fit-A.mp4", "size": gib, "progress": 0, "priority": 0},
                {"index": 2, "name": "Big/Fit-B.mp4", "size": gib, "progress": 0, "priority": 0},
            ],
            piece_size=16 * 1024**2,
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
            {"big": {"hash": "big", "name": "Big", "category": "auto", "tags": "auto", "state": "stoppedDL", "amount_left": 5 * gib, "size": 5 * gib, "progress": 0.0}},
            free_bytes=5 * gib,
            sync_healthy=True,
        )

        assert result.batches_created == 1
        batch = _rows(db, "select indices_json,total_bytes from torrent_batches")[0]
        assert json.loads(batch["indices_json"]) == [1, 2]
        assert batch["total_bytes"] == 2 * gib
        assert ("/api/v2/torrents/filePrio", {"hash": "big", "id": "1|2", "priority": "1"}) in qbt.calls
        assert ("/api/v2/torrents/filePrio", {"hash": "big", "id": "0", "priority": "0"}) in qbt.calls


def test_file_batch_service_pipeline_prefers_nearly_complete_high_payload_file():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.file_batch import FileBatchService

    gib = 1024**3
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        qbt = BatchQbt(
            [
                {"index": 0, "name": "Big/Small.mp4", "size": gib, "progress": 0.0, "priority": 0},
                {"index": 1, "name": "Big/NearlyDone.mp4", "size": 4 * gib, "progress": 0.75, "priority": 0},
            ],
            piece_size=16 * 1024**2,
        )
        service = FileBatchService(
            state_db=db,
            dry_run=False,
            qbt=qbt,
            disk_floor_bytes=2 * gib,
            filesystem_slack_bytes=128 * 1024**2,
            batch_live_verify=True,
            batch_max_live_batch_bytes=1280 * 1024**2,
            now=lambda: 1_000,
        )

        result = service.sync_completed(
            {"big": {"hash": "big", "name": "Big", "category": "auto", "tags": "auto", "state": "stoppedDL", "amount_left": 2 * gib, "size": 5 * gib, "progress": 0.6}},
            free_bytes=5 * gib,
            sync_healthy=True,
        )

        assert result.batches_created == 1
        batch = _rows(db, "select indices_json,total_bytes,reserved_bytes from torrent_batches")[0]
        assert json.loads(batch["indices_json"]) == [1]
        assert batch["total_bytes"] == 4 * gib
        assert batch["reserved_bytes"] == gib + 32 * 1024**2 + 128 * 1024**2


def test_file_batch_service_live_verify_without_allowlist_allows_multiple_hashes():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.file_batch import FileBatchService

    gib = 1024**3
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)

        class MultiQbt(BatchQbt):
            def __init__(self):
                super().__init__([], piece_size=16 * 1024**2)
                self.by_hash = {
                    "h1": [{"index": 0, "name": "A.mp4", "size": gib, "progress": 0, "priority": 0}],
                    "h2": [{"index": 0, "name": "B.mp4", "size": gib, "progress": 0, "priority": 0}],
                }

            def torrent_files(self, hash):
                self.calls.append(("torrent_files", hash))
                return list(self.by_hash[hash])

        qbt = MultiQbt()
        service = FileBatchService(
            state_db=db,
            dry_run=False,
            qbt=qbt,
            disk_floor_bytes=2 * gib,
            batch_live_verify=True,
            batch_max_new_per_tick=10,
            now=lambda: 1_000,
        )

        result = service.sync_completed(
            {
                "h1": {"hash": "h1", "category": "auto", "tags": "auto", "state": "stoppedDL", "amount_left": gib, "size": gib, "progress": 0.0},
                "h2": {"hash": "h2", "category": "auto", "tags": "auto", "state": "stoppedDL", "amount_left": gib, "size": gib, "progress": 0.0},
            },
            free_bytes=8 * gib,
            sync_healthy=True,
        )

        assert result.batches_created == 2
        assert ("torrent_files", "h1") in qbt.calls
        assert ("torrent_files", "h2") in qbt.calls
        batches = _rows(db, "select hash,state from torrent_batches order by id")
        assert batches == [{"hash": "h1", "state": "downloading"}, {"hash": "h2", "state": "downloading"}]


def test_file_batch_service_pipeline_allows_real_media_with_site_prefix():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.file_batch import FileBatchService

    gib = 1024**3
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        qbt = BatchQbt([{"index": 0, "name": "MVG-155-C/489155.com@MVG-155-C.mp4", "size": 5 * gib, "progress": 0.8, "priority": 1}])
        service = FileBatchService(
            state_db=db,
            dry_run=False,
            qbt=qbt,
            disk_floor_bytes=2 * gib,
            filesystem_slack_bytes=128 * 1024**2,
            batch_live_verify=True,
            batch_allow_hashes={"mvg"},
            batch_max_live_batch_bytes=1536 * 1024**2,
            now=lambda: 1_000,
        )

        result = service.sync_completed(
            {"mvg": {"hash": "mvg", "name": "MVG-155-C", "category": "auto", "tags": "auto", "state": "downloading", "amount_left": gib, "size": 5 * gib, "progress": 0.8}},
            free_bytes=4 * gib,
            sync_healthy=True,
        )

        assert result.batches_created == 1
        batch = _rows(db, "select indices_json from torrent_batches")[0]
        assert json.loads(batch["indices_json"]) == [0]


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


def test_file_batch_service_pipeline_does_not_reselect_inflight_batch_indices():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.file_batch import FileBatchService

    gib = 1024**3
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        con.execute(
            "insert into torrent_batches(hash,batch_no,state,mode,indices_json,total_bytes,reserved_bytes,created_at,updated_at) values(?,?,?,?,?,?,?,?,?)",
            ("big", 1, "downloading", "pipeline", "[0]", 5 * gib, gib, 900, 900),
        )
        con.commit(); con.close()
        qbt = BatchQbt(
            [
                {"index": 0, "name": "Big/A.mp4", "size": 5 * gib, "progress": 0.8, "priority": 1},
                {"index": 1, "name": "Big/B.mp4", "size": 512 * 1024**2, "progress": 0.0, "priority": 0},
            ]
        )
        service = FileBatchService(
            state_db=db,
            dry_run=False,
            qbt=qbt,
            disk_floor_bytes=2 * gib,
            max_inflight_batches_per_torrent=2,
            now=lambda: 1_000,
        )

        result = service.sync_completed(
            {"big": {"hash": "big", "name": "Big", "category": "auto", "tags": "auto", "state": "downloading", "amount_left": 2 * gib, "size": 6 * gib, "progress": 0.7}},
            free_bytes=6 * gib,
            sync_healthy=True,
        )

        assert result.batches_created == 1
        batches = _rows(db, "select batch_no,indices_json from torrent_batches order by id")
        assert [(row["batch_no"], json.loads(row["indices_json"])) for row in batches] == [(1, [0]), (2, [1])]
        assert ("/api/v2/torrents/filePrio", {"hash": "big", "id": "1", "priority": "1"}) in qbt.calls
        assert ("/api/v2/torrents/filePrio", {"hash": "big", "id": "0", "priority": "0"}) not in qbt.calls


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
        reservations = _rows(
            db,
            "select kind,accounting_class,owner,last_observed_at,bytes,state,released_at,reason "
            "from resource_reservations order by id",
        )
        assert reservations == [
            {
                "kind": "batch",
                "accounting_class": "future_growth",
                "owner": "file_batch",
                "last_observed_at": 1_000,
                "bytes": 100,
                "state": "released",
                "released_at": 1_000,
                "reason": "batch_downloaded_upload_queued",
            },
            {
                "kind": "cleanup_pending",
                "accounting_class": "current_pinned",
                "owner": "file_batch",
                "last_observed_at": 1_000,
                "bytes": 15,
                "state": "active",
                "released_at": None,
                "reason": "batch_upload_queued",
            },
        ]


def test_file_batch_service_reconciles_stopped_cooldown_batch_by_releasing_reservation():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.file_batch import FileBatchService

    gib = 1024**3
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        con.execute(
            "insert into torrent_batches(id,hash,batch_no,state,mode,indices_json,total_bytes,reserved_bytes,created_at,updated_at) values(?,?,?,?,?,?,?,?,?,?)",
            (7, "stale", 1, "downloading", "pipeline", "[0]", 5 * gib, gib, 900, 900),
        )
        con.execute(
            "insert into resource_reservations(hash,batch_id,kind,bytes,state,created_at,expires_at,reason) values(?,?,?,?,?,?,?,?)",
            ("stale", 7, "batch", gib, "active", 900, 4600, "batch_pipeline_reserved"),
        )
        con.execute(
            "insert into scheduler_allocations(hash,desired_state,applied_state,slot_kind,allocated_at,reason) values(?,?,?,?,?,?)",
            ("stale", "soak_cooldown", "soak_cooldown", "soak_cooldown", 900, "active_slow_3min"),
        )
        con.commit(); con.close()
        qbt = BatchQbt([{"index": 0, "name": "A.mp4", "size": 5 * gib, "progress": 0.4, "priority": 1}])
        service = FileBatchService(state_db=db, dry_run=False, qbt=qbt, disk_floor_bytes=2 * gib, now=lambda: 1_000)

        result = service.sync_completed(
            {"stale": {"hash": "stale", "name": "Stale", "category": "auto", "tags": "auto", "state": "stoppedDL", "amount_left": 3 * gib, "size": 5 * gib, "progress": 0.4}},
            free_bytes=6 * gib,
            sync_healthy=True,
        )

        assert result.batches_created == 0
        assert result.batches_blocked == 1
        assert not any(call[0] == "/api/v2/torrents/start" for call in qbt.calls)
        batch = _rows(db, "select state,updated_at from torrent_batches where id=7")[0]
        assert batch == {"state": "paused_by_planner", "updated_at": 1_000}
        reservation = _rows(db, "select state,released_at,reason from resource_reservations where batch_id=7 and kind='batch'")[0]
        assert reservation == {"state": "released", "released_at": 1_000, "reason": "batch_reconcile_planner_stopped"}
        decision = _rows(db, "select decision,reason_code from decision_log where component='file_batch' and hash='stale' order by id desc limit 1")[0]
        assert decision == {"decision": "batch_reconciled", "reason_code": "planner_stopped_batch"}


def test_file_batch_service_reconciles_stopped_cooldown_batch_even_after_reservation_expired():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.file_batch import FileBatchService

    gib = 1024**3
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        con.execute(
            "insert into torrent_batches(id,hash,batch_no,state,mode,indices_json,total_bytes,reserved_bytes,created_at,updated_at) values(?,?,?,?,?,?,?,?,?,?)",
            (8, "expired", 1, "downloading", "pipeline", "[0]", 5 * gib, gib, 900, 900),
        )
        con.execute(
            "insert into resource_reservations(hash,batch_id,kind,bytes,state,created_at,expires_at,released_at,reason) values(?,?,?,?,?,?,?,?,?)",
            ("expired", 8, "batch", gib, "expired", 900, 950, 1_000, "reservation_expired"),
        )
        con.execute(
            "insert into scheduler_allocations(hash,desired_state,applied_state,slot_kind,allocated_at,reason) values(?,?,?,?,?,?)",
            ("expired", "soak_cooldown", "soak_cooldown", "soak_cooldown", 900, "active_slow_3min"),
        )
        con.commit(); con.close()
        qbt = BatchQbt([{"index": 0, "name": "A.mp4", "size": 5 * gib, "progress": 0.4, "priority": 1}])
        service = FileBatchService(state_db=db, dry_run=False, qbt=qbt, disk_floor_bytes=2 * gib, now=lambda: 2_000)

        result = service.sync_completed(
            {"expired": {"hash": "expired", "name": "Expired", "category": "auto", "tags": "auto", "state": "stoppedDL", "amount_left": 3 * gib, "size": 5 * gib, "progress": 0.4}},
            free_bytes=6 * gib,
            sync_healthy=True,
        )

        assert result.batches_created == 0
        assert result.batches_blocked == 1
        batch = _rows(db, "select state,updated_at from torrent_batches where id=8")[0]
        assert batch == {"state": "paused_by_planner", "updated_at": 2_000}
        reservation = _rows(db, "select state,released_at,reason from resource_reservations where batch_id=8 and kind='batch'")[0]
        assert reservation == {"state": "expired", "released_at": 1_000, "reason": "reservation_expired"}


if __name__ == "__main__":
    inspect = __import__("inspect")
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn) and not inspect.signature(fn).parameters:
            fn()
    print("ok")
