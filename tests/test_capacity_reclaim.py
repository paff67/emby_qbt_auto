#!/usr/bin/env python3
from __future__ import annotations

import sqlite3
import sys
import tempfile
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))


class RecordingExecutor:
    def __init__(self):
        self.posts = []

    def qbt_post(self, path, payload):
        self.posts.append((path, payload))


def _dead_row(db: Path, torrent_hash: str, now: int) -> None:
    con = sqlite3.connect(db)
    con.execute(
        "insert into scheduler_allocations(hash,desired_state,applied_state,slot_kind,allocated_at,reason) "
        "values(?,'dead','dead','dead',?,'health_no_swarm_no_progress')",
        (torrent_hash, now - 10_000),
    )
    con.execute(
        "insert into torrent_health(hash,sampled_at,dlspeed_bps,completed_bytes,last_completed_bytes,progress,"
        "num_seeds,num_peers,no_swarm_since,no_progress_since,dead_since,updated_at) "
        "values(?,?,0,100,100,0.2,0,0,?,?,?,?)",
        (
            torrent_hash,
            now - 10_000,
            now - 10_000,
            now - 10_000,
            now - 10_000,
            now - 10_000,
        ),
    )
    con.commit()
    con.close()


def test_dead_partial_reclaimer_dry_run_lists_safe_path_without_mutation():
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    now = 20_000
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        managed = root / "incomplete"
        payload = managed / "dead-one"
        payload.mkdir(parents=True)
        (payload / "video.part").write_bytes(b"x" * 4096)
        db = root / "state.sqlite"
        migrate(db, dry_run=False)
        _dead_row(db, "dead-one", now)
        executor = RecordingExecutor()
        reclaimer = DeadPartialReclaimer(
            db,
            executor,
            host_downloads=root,
            container_downloads="/downloads",
            managed_root=managed,
            dry_run=True,
            min_dead_age_sec=3_600,
            min_reclaim_bytes=1,
            max_per_tick=1,
            now=lambda: now,
        )

        result = reclaimer.run(
            {
                "dead-one": {
                    "hash": "dead-one",
                    "category": "auto",
                    "tags": "auto",
                    "state": "stoppedDL",
                    "amount_left": 900,
                    "completed_bytes": 100,
                    "content_path": "/downloads/incomplete/dead-one",
                }
            },
            capacity_state="capacity_deadlock",
            free_bytes=0,
            target_free_bytes=10_000,
        )

        assert result.dry_run is True
        assert result.planned == 1
        assert result.reclaimed == 0
        assert result.candidates[0]["host_path"] == str(payload.resolve())
        assert result.candidates[0]["allocated_bytes"] >= 4096
        assert payload.exists()
        assert executor.posts == []


def test_dead_partial_reclaimer_live_resets_payload_but_keeps_torrent_record():
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    now = 20_000
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        managed = root / "incomplete"
        payload = managed / "dead-one"
        payload.mkdir(parents=True)
        (payload / "video.part").write_bytes(b"x" * 4096)
        db = root / "state.sqlite"
        migrate(db, dry_run=False)
        _dead_row(db, "dead-one", now)
        executor = RecordingExecutor()
        reclaimer = DeadPartialReclaimer(
            db,
            executor,
            host_downloads=root,
            container_downloads="/downloads",
            managed_root=managed,
            dry_run=False,
            min_dead_age_sec=3_600,
            min_reclaim_bytes=1,
            max_per_tick=1,
            now=lambda: now,
        )

        result = reclaimer.run(
            {
                "dead-one": {
                    "hash": "dead-one",
                    "category": "auto",
                    "tags": "auto",
                    "state": "stoppedDL",
                    "amount_left": 900,
                    "completed_bytes": 100,
                    "content_path": "/downloads/incomplete/dead-one",
                }
            },
            capacity_state="capacity_deadlock",
            free_bytes=0,
            target_free_bytes=10_000,
        )

        assert result.reclaimed == 1
        assert not payload.exists()
        assert executor.posts == [
            ("/api/v2/torrents/stop", {"hashes": "dead-one"}),
            ("/api/v2/torrents/recheck", {"hashes": "dead-one"}),
        ]
        assert not any(path == "/api/v2/torrents/delete" for path, _ in executor.posts)


def test_dead_partial_reclaimer_rejects_protected_active_or_overlapping_paths():
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    now = 20_000
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        managed = root / "incomplete"
        shared = managed / "shared"
        shared.mkdir(parents=True)
        (shared / "a.part").write_bytes(b"x" * 4096)
        db = root / "state.sqlite"
        migrate(db, dry_run=False)
        for torrent_hash in ("held", "running", "overlap"):
            _dead_row(db, torrent_hash, now)
        reclaimer = DeadPartialReclaimer(
            db,
            RecordingExecutor(),
            host_downloads=root,
            container_downloads="/downloads",
            managed_root=managed,
            dry_run=True,
            min_dead_age_sec=3_600,
            min_reclaim_bytes=1,
            max_per_tick=3,
            now=lambda: now,
        )
        snapshots = {
            "held": {
                "hash": "held", "category": "auto", "tags": "auto,hold",
                "state": "stoppedDL", "amount_left": 1, "completed_bytes": 100,
                "content_path": "/downloads/incomplete/held",
            },
            "running": {
                "hash": "running", "category": "auto", "tags": "auto",
                "state": "downloading", "amount_left": 1, "completed_bytes": 100,
                "content_path": "/downloads/incomplete/running",
            },
            "overlap": {
                "hash": "overlap", "category": "auto", "tags": "auto",
                "state": "stoppedDL", "amount_left": 1, "completed_bytes": 100,
                "content_path": "/downloads/incomplete/shared",
            },
            "other": {
                "hash": "other", "category": "auto", "tags": "auto",
                "state": "stoppedDL", "amount_left": 1, "completed_bytes": 100,
                "content_path": "/downloads/incomplete/shared/a.part",
            },
        }

        result = reclaimer.run(
            snapshots,
            capacity_state="capacity_deadlock",
            free_bytes=0,
            target_free_bytes=10_000,
        )

        assert result.planned == 0
        reasons = result.rejection_counts
        assert reasons["protected_tag"] == 1
        assert reasons["not_stopped"] == 1
        assert reasons["path_overlap"] == 1


def test_dead_partial_reclaimer_requires_all_dead_evidence_to_be_old():
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    now = 20_000
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        managed = root / "incomplete"
        payload = managed / "dead-one"
        payload.mkdir(parents=True)
        (payload / "video.part").write_bytes(b"x" * 4096)
        db = root / "state.sqlite"
        migrate(db, dry_run=False)
        _dead_row(db, "dead-one", now)
        con = sqlite3.connect(db)
        con.execute(
            "update torrent_health set no_progress_since=? where hash='dead-one'",
            (now - 30,),
        )
        con.commit()
        con.close()
        reclaimer = DeadPartialReclaimer(
            db,
            RecordingExecutor(),
            host_downloads=root,
            container_downloads="/downloads",
            managed_root=managed,
            dry_run=True,
            min_dead_age_sec=3_600,
            min_reclaim_bytes=1,
            now=lambda: now,
        )

        result = reclaimer.run(
            {
                "dead-one": {
                    "hash": "dead-one", "category": "auto", "tags": "auto",
                    "state": "stoppedDL", "amount_left": 1, "completed_bytes": 100,
                    "content_path": "/downloads/incomplete/dead-one",
                }
            },
            capacity_state="capacity_deadlock",
            free_bytes=0,
            target_free_bytes=10_000,
        )

        assert result.planned == 0
        assert result.rejection_counts["dead_age"] == 1


def test_dead_partial_reclaimer_accepts_missing_piece_dead_task_with_live_peers():
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    now = 20_000
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        managed = root / "incomplete"
        payload = managed / "dead-one"
        payload.mkdir(parents=True)
        (payload / "video.part").write_bytes(b"x" * 4096)
        db = root / "state.sqlite"
        migrate(db, dry_run=False)
        _dead_row(db, "dead-one", now)
        con = sqlite3.connect(db)
        con.execute(
            "update torrent_health set no_swarm_since=null,num_peers=3 where hash='dead-one'"
        )
        con.commit()
        con.close()
        reclaimer = DeadPartialReclaimer(
            db,
            RecordingExecutor(),
            host_downloads=root,
            container_downloads="/downloads",
            managed_root=managed,
            dry_run=True,
            min_dead_age_sec=3_600,
            min_reclaim_bytes=1,
            now=lambda: now,
        )

        result = reclaimer.run(
            {
                "dead-one": {
                    "hash": "dead-one", "category": "auto", "tags": "auto",
                    "state": "stoppedDL", "amount_left": 1, "completed_bytes": 100,
                    "availability": 0.8, "num_seeds": 0, "num_peers": 3,
                    "content_path": "/downloads/incomplete/dead-one",
                }
            },
            capacity_state="capacity_deadlock",
            free_bytes=0,
            target_free_bytes=10_000,
        )

        assert result.planned == 1


def test_dead_partial_reclaimer_rejects_task_with_complete_source():
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    now = 20_000
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        managed = root / "incomplete"
        payload = managed / "dead-one"
        payload.mkdir(parents=True)
        (payload / "video.part").write_bytes(b"x" * 4096)
        db = root / "state.sqlite"
        migrate(db, dry_run=False)
        _dead_row(db, "dead-one", now)
        reclaimer = DeadPartialReclaimer(
            db,
            RecordingExecutor(),
            host_downloads=root,
            container_downloads="/downloads",
            managed_root=managed,
            dry_run=True,
            min_dead_age_sec=3_600,
            min_reclaim_bytes=1,
            now=lambda: now,
        )

        result = reclaimer.run(
            {
                "dead-one": {
                    "hash": "dead-one", "category": "auto", "tags": "auto",
                    "state": "stoppedDL", "amount_left": 1, "completed_bytes": 100,
                    "availability": 1.0, "num_seeds": 1,
                    "content_path": "/downloads/incomplete/dead-one",
                }
            },
            capacity_state="capacity_deadlock",
            free_bytes=0,
            target_free_bytes=10_000,
        )

        assert result.planned == 0
        assert result.rejection_counts["complete_source"] == 1


def test_dead_partial_reclaimer_contains_path_inspection_failure(monkeypatch):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    now = 20_000
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        managed = root / "incomplete"
        payload = managed / "dead-one"
        payload.mkdir(parents=True)
        (payload / "video.part").write_bytes(b"x" * 4096)
        db = root / "state.sqlite"
        migrate(db, dry_run=False)
        _dead_row(db, "dead-one", now)
        reclaimer = DeadPartialReclaimer(
            db, RecordingExecutor(), host_downloads=root,
            container_downloads="/downloads", managed_root=managed,
            dry_run=True, min_dead_age_sec=3_600, min_reclaim_bytes=1,
            now=lambda: now,
        )

        def fail(_path):
            raise OSError("cannot inspect")

        monkeypatch.setattr(reclaimer, "_allocated_bytes", fail)
        result = reclaimer.run(
            {
                "dead-one": {
                    "hash": "dead-one", "category": "auto", "tags": "auto",
                    "state": "stoppedDL", "amount_left": 1, "completed_bytes": 100,
                    "availability": 0.5,
                    "content_path": "/downloads/incomplete/dead-one",
                }
            },
            capacity_state="capacity_deadlock", free_bytes=0, target_free_bytes=10_000,
        )

        assert result.planned == 0
        assert result.rejection_counts["path_inspection_failed"] == 1


def test_dead_partial_reclaimer_reports_reclaimed_bytes_when_recheck_fails():
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    class RecheckFailExecutor(RecordingExecutor):
        def qbt_post(self, path, payload):
            super().qbt_post(path, payload)
            if path.endswith("/recheck"):
                raise RuntimeError("recheck unavailable")

    now = 20_000
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        managed = root / "incomplete"
        payload = managed / "dead-one"
        payload.mkdir(parents=True)
        (payload / "video.part").write_bytes(b"x" * 4096)
        db = root / "state.sqlite"
        migrate(db, dry_run=False)
        _dead_row(db, "dead-one", now)
        reclaimer = DeadPartialReclaimer(
            db, RecheckFailExecutor(), host_downloads=root,
            container_downloads="/downloads", managed_root=managed,
            dry_run=False, min_dead_age_sec=3_600, min_reclaim_bytes=1,
            now=lambda: now,
        )

        result = reclaimer.run(
            {
                "dead-one": {
                    "hash": "dead-one", "category": "auto", "tags": "auto",
                    "state": "stoppedDL", "amount_left": 1, "completed_bytes": 100,
                    "availability": 0.5,
                    "content_path": "/downloads/incomplete/dead-one",
                }
            },
            capacity_state="capacity_deadlock", free_bytes=0, target_free_bytes=10_000,
        )

        assert not payload.exists()
        assert result.reclaimed == 1
        assert result.reclaimed_bytes >= 4096
        assert "recheck unavailable" in result.errors[0]


def test_live_reclaim_persists_torrent_identity_and_queues_magnet_notification():
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    now = 20_000
    magnet = "mag" + "net:?xt=urn:btih:DEADONE&dn=Dead%20Movie&tr=udp%3A%2F%2Ftracker.example%3A80"
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        managed = root / "incomplete"
        payload = managed / "dead-one"
        payload.mkdir(parents=True)
        (payload / "video.part").write_bytes(b"x" * 4096)
        db = root / "state.sqlite"
        migrate(db, dry_run=False)
        _dead_row(db, "dead-one", now)
        reclaimer = DeadPartialReclaimer(
            db,
            RecordingExecutor(),
            host_downloads=root,
            container_downloads="/downloads",
            managed_root=managed,
            dry_run=False,
            min_dead_age_sec=3_600,
            min_reclaim_bytes=1,
            notification_chat_ids=["1001", "1002"],
            now=lambda: now,
        )

        result = reclaimer.run(
            {
                "dead-one": {
                    "hash": "dead-one",
                    "name": "Dead Movie",
                    "magnet_uri": magnet,
                    "category": "auto",
                    "tags": "auto",
                    "state": "stoppedDL",
                    "amount_left": 900,
                    "completed_bytes": 100,
                    "progress": 0.1,
                    "availability": 0.5,
                    "content_path": "/downloads/incomplete/dead-one",
                }
            },
            capacity_state="capacity_deadlock",
            free_bytes=0,
            target_free_bytes=10_000,
        )

        assert result.reclaimed == 1
        con = sqlite3.connect(db)
        con.row_factory = sqlite3.Row
        reclaim = dict(con.execute("select * from capacity_reclaims").fetchone())
        notifications = [
            dict(row)
            for row in con.execute(
                "select * from bot_notifications where topic='capacity_reclaim' order by chat_id"
            )
        ]
        con.close()

        assert reclaim["hash"] == "dead-one"
        assert reclaim["name"] == "Dead Movie"
        assert reclaim["magnet_uri"] == magnet
        assert reclaim["host_path"] == str(payload.resolve())
        assert reclaim["state"] == "reclaimed"
        assert reclaim["recheck_state"] == "requested"
        assert reclaim["reclaimed_at"] == now
        assert len(json.loads(reclaim["notification_ids_json"])) == 2
        assert [row["chat_id"] for row in notifications] == ["1001", "1002"]
        assert all("Dead Movie" in row["message"] for row in notifications)
        assert all(magnet in row["message"] for row in notifications)
        assert all(json.loads(row["payload_json"])["magnet_uri"] == magnet for row in notifications)


def test_dry_run_does_not_persist_reclaim_or_queue_notification():
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    now = 20_000
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        managed = root / "incomplete"
        payload = managed / "dead-one"
        payload.mkdir(parents=True)
        (payload / "video.part").write_bytes(b"x" * 4096)
        db = root / "state.sqlite"
        migrate(db, dry_run=False)
        _dead_row(db, "dead-one", now)
        reclaimer = DeadPartialReclaimer(
            db,
            RecordingExecutor(),
            host_downloads=root,
            container_downloads="/downloads",
            managed_root=managed,
            dry_run=True,
            min_dead_age_sec=3_600,
            min_reclaim_bytes=1,
            notification_chat_ids=["1001"],
            now=lambda: now,
        )

        result = reclaimer.run(
            {
                "dead-one": {
                    "hash": "dead-one",
                    "name": "Dead Movie",
                    "magnet_uri": "mag" + "net:?xt=urn:btih:DEADONE",
                    "category": "auto",
                    "tags": "auto",
                    "state": "stoppedDL",
                    "amount_left": 900,
                    "completed_bytes": 100,
                    "availability": 0.5,
                    "content_path": "/downloads/incomplete/dead-one",
                }
            },
            capacity_state="capacity_deadlock",
            free_bytes=0,
            target_free_bytes=10_000,
        )

        assert result.planned == 1
        con = sqlite3.connect(db)
        assert con.execute("select count(*) from capacity_reclaims").fetchone()[0] == 0
        assert con.execute(
            "select count(*) from bot_notifications where topic='capacity_reclaim'"
        ).fetchone()[0] == 0
        con.close()


def test_recheck_failure_is_persisted_and_notified_after_payload_reclaim():
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    class RecheckFailExecutor(RecordingExecutor):
        def qbt_post(self, path, payload):
            super().qbt_post(path, payload)
            if path.endswith("/recheck"):
                raise RuntimeError("recheck unavailable")

    now = 20_000
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        managed = root / "incomplete"
        payload = managed / "dead-one"
        payload.mkdir(parents=True)
        (payload / "video.part").write_bytes(b"x" * 4096)
        db = root / "state.sqlite"
        migrate(db, dry_run=False)
        _dead_row(db, "dead-one", now)
        reclaimer = DeadPartialReclaimer(
            db,
            RecheckFailExecutor(),
            host_downloads=root,
            container_downloads="/downloads",
            managed_root=managed,
            dry_run=False,
            min_dead_age_sec=3_600,
            min_reclaim_bytes=1,
            notification_chat_ids=["1001"],
            now=lambda: now,
        )

        result = reclaimer.run(
            {
                "dead-one": {
                    "hash": "dead-one",
                    "name": "Dead Movie",
                    "magnet_uri": "mag" + "net:?xt=urn:btih:DEADONE",
                    "category": "auto",
                    "tags": "auto",
                    "state": "stoppedDL",
                    "amount_left": 900,
                    "completed_bytes": 100,
                    "availability": 0.5,
                    "content_path": "/downloads/incomplete/dead-one",
                }
            },
            capacity_state="capacity_deadlock",
            free_bytes=0,
            target_free_bytes=10_000,
        )

        assert result.reclaimed == 1
        con = sqlite3.connect(db)
        con.row_factory = sqlite3.Row
        reclaim = dict(con.execute("select * from capacity_reclaims").fetchone())
        notice = dict(
            con.execute(
                "select * from bot_notifications where topic='capacity_reclaim'"
            ).fetchone()
        )
        con.close()
        assert reclaim["recheck_state"] == "failed"
        assert "recheck unavailable" in reclaim["recheck_error"]
        assert "重新校验失败" in notice["message"]
