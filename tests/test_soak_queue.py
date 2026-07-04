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


def _rows(db: Path, sql: str, params: tuple = ()):
    con = sqlite3.connect(db)
    con.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in con.execute(sql, params)]
    finally:
        con.close()


class RecordingExecutor:
    def __init__(self):
        self.posts = []
        self.seq = []
        self.download_limits = []

    def qbt_post(self, path, payload):
        self.posts.append((path, payload))

    def set_seq_dl(self, hash, desired):
        self.seq.append((hash, desired))
        return True

    def set_download_limit(self, hash, limit_bps):
        self.download_limits.append((hash, limit_bps))


def _migrated_db() -> Path:
    from qbt_orchestrator.db import migrate

    td = tempfile.TemporaryDirectory()
    db = Path(td.name) / "state.sqlite"
    migrate(db, dry_run=False)
    db._td = td  # type: ignore[attr-defined]
    return db


def test_migration_adds_soak_state_table():
    from qbt_orchestrator.db import migrate

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)

        cols = _rows(db, "pragma table_info(soak_state)")
        assert [c["name"] for c in cols] == [
            "hash",
            "state",
            "ema_dlspeed_bps",
            "hot_since",
            "resident_since",
            "cooldown_until",
            "last_started_at",
            "last_stopped_at",
            "exposure_bytes",
            "last_sample_at",
            "updated_at",
            "reason",
        ]
        indexes = {r["name"] for r in _rows(db, "select name from sqlite_master where type='index'")}
        assert "idx_soak_state_state" in indexes
        assert "idx_soak_state_cooldown" in indexes


def test_soak_queue_calculates_bounded_exposure_from_piece_spill_and_speed():
    from qbt_orchestrator.soak_queue import SoakQueueConfig, SoakQueueService
    from qbt_orchestrator.db import migrate

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        cfg = SoakQueueConfig(
            min_exposure_bytes=128 * 1024**2,
            max_per_torrent_exposure_bytes=512 * 1024**2,
            exposure_horizon_sec=900,
        )
        svc = SoakQueueService(db, RecordingExecutor(), dry_run=True, config=cfg, now=lambda: 100)

        assert svc.calculate_exposure(
            {"hash": "h1", "amount_left": 10 * 1024**3, "piece_size": 4 * 1024**2},
            ema_dlspeed_bps=0,
        ) == 128 * 1024**2
        assert svc.calculate_exposure(
            {"hash": "h2", "amount_left": 10 * 1024**3, "piece_size": 4 * 1024**2},
            ema_dlspeed_bps=2 * 1024**2,
        ) == 512 * 1024**2
        assert svc.calculate_exposure(
            {"hash": "h3", "amount_left": 64 * 1024**2, "piece_size": 4 * 1024**2},
            ema_dlspeed_bps=2 * 1024**2,
        ) == 64 * 1024**2


def test_soak_queue_updates_ema_speed_persistently():
    from qbt_orchestrator.soak_queue import SoakQueueConfig, SoakQueueService
    from qbt_orchestrator.db import migrate

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        svc = SoakQueueService(db, RecordingExecutor(), dry_run=True, config=SoakQueueConfig(resident_slots=0), now=lambda: 100)
        snapshots = {
            "h1": {"hash": "h1", "category": "auto", "tags": "auto", "state": "pausedDL", "amount_left": 1024**3, "dlspeed": 1000, "progress": 0.1}
        }

        svc.run_once(snapshots, free_bytes=20 * 1024**3, sync_healthy=True)
        row = _rows(db, "select ema_dlspeed_bps,last_sample_at from soak_state where hash='h1'")[0]
        assert row == {"ema_dlspeed_bps": 1000, "last_sample_at": 100}

        svc2 = SoakQueueService(db, RecordingExecutor(), dry_run=True, config=SoakQueueConfig(resident_slots=0), now=lambda: 115)
        snapshots["h1"]["dlspeed"] = 2000
        svc2.run_once(snapshots, free_bytes=20 * 1024**3, sync_healthy=True)
        row = _rows(db, "select ema_dlspeed_bps,last_sample_at from soak_state where hash='h1'")[0]
        assert row == {"ema_dlspeed_bps": 1300, "last_sample_at": 115}


def test_soak_queue_starts_resident_torrents_with_seq_false_and_exposure_reservation():
    from qbt_orchestrator.soak_queue import SoakQueueConfig, SoakQueueService
    from qbt_orchestrator.db import migrate

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        executor = RecordingExecutor()
        cfg = SoakQueueConfig(resident_slots=2, min_free_bytes=0, disk_floor_bytes=0, max_total_exposure_bytes=1024**3)
        svc = SoakQueueService(db, executor, dry_run=False, config=cfg, now=lambda: 1000)
        snapshots = {
            "h1": {"hash": "h1", "category": "auto", "tags": "auto", "state": "pausedDL", "amount_left": 5 * 1024**3, "piece_size": 4 * 1024**2, "progress": 0.2, "num_seeds": 1, "num_peers": 2},
            "h2": {"hash": "h2", "category": "auto", "tags": "auto", "state": "stoppedDL", "amount_left": 6 * 1024**3, "piece_size": 4 * 1024**2, "progress": 0.1, "num_seeds": 1, "num_peers": 2},
        }

        result = svc.run_once(snapshots, free_bytes=20 * 1024**3, sync_healthy=True)

        assert result.started == ["h1", "h2"]
        assert result.resident_hashes == ["h1", "h2"]
        assert executor.seq == [("h1", False), ("h2", False)]
        assert executor.posts == [("/api/v2/torrents/start", {"hashes": "h1|h2"})]
        allocations = _rows(db, "select hash,desired_state,slot_kind,reserved_bytes,desired_seq_dl from scheduler_allocations order by hash")
        assert [(r["hash"], r["desired_state"], r["slot_kind"], r["desired_seq_dl"]) for r in allocations] == [
            ("h1", "soak_resident", "soak_resident", 0),
            ("h2", "soak_resident", "soak_resident", 0),
        ]
        assert all(r["reserved_bytes"] == 128 * 1024**2 for r in allocations)
        reservations = _rows(db, "select hash,kind,bytes,state,reason from resource_reservations order by hash")
        assert [(r["hash"], r["kind"], r["bytes"], r["state"], r["reason"]) for r in reservations] == [
            ("h1", "soak_probe", 128 * 1024**2, "active", "soak_resident"),
            ("h2", "soak_probe", 128 * 1024**2, "active", "soak_resident"),
        ]


def test_soak_queue_ignores_legacy_min_free_gate_and_uses_disk_floor_budget():
    from qbt_orchestrator.soak_queue import SoakQueueConfig, SoakQueueService
    from qbt_orchestrator.db import migrate

    gib = 1024**3
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        executor = RecordingExecutor()
        svc = SoakQueueService(
            db,
            executor,
            dry_run=False,
            config=SoakQueueConfig(
                resident_slots=1,
                min_free_bytes=8 * gib,
                disk_floor_bytes=3 * gib,
                max_total_exposure_bytes=4 * gib,
            ),
            now=lambda: 1000,
        )

        result = svc.run_once(
            {"h1": {"hash": "h1", "category": "auto", "tags": "auto", "state": "pausedDL", "amount_left": gib, "piece_size": 4 * 1024**2, "progress": 0.1}},
            free_bytes=6 * gib,
            sync_healthy=True,
        )

        assert result.blocked_reason is None
        assert result.started == ["h1"]
        assert executor.posts == [("/api/v2/torrents/start", {"hashes": "h1"})]
        decisions = _rows(db, "select reason_code from decision_log where component='soak_queue'")
        assert "min_free_block" not in {row["reason_code"] for row in decisions}


def test_soak_queue_blocks_new_resident_only_when_disk_floor_budget_is_exhausted():
    from qbt_orchestrator.soak_queue import SoakQueueConfig, SoakQueueService
    from qbt_orchestrator.db import migrate

    gib = 1024**3
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        executor = RecordingExecutor()
        svc = SoakQueueService(
            db,
            executor,
            dry_run=False,
            config=SoakQueueConfig(
                resident_slots=1,
                min_free_bytes=0,
                disk_floor_bytes=3 * gib,
                min_exposure_bytes=128 * 1024**2,
                max_total_exposure_bytes=4 * gib,
            ),
            now=lambda: 1000,
        )

        result = svc.run_once(
            {"h1": {"hash": "h1", "category": "auto", "tags": "auto", "state": "pausedDL", "amount_left": gib, "piece_size": 4 * 1024**2, "progress": 0.1}},
            free_bytes=3 * gib + 64 * 1024**2,
            sync_healthy=True,
        )

        assert result.started == []
        assert executor.posts == []
        decision = _rows(db, "select decision,reason_code from decision_log where hash='h1' order by id desc limit 1")[0]
        assert decision == {"decision": "blocked", "reason_code": "budget_insufficient"}


def test_soak_queue_respects_resident_slots_and_qbt_active_cap():
    from qbt_orchestrator.soak_queue import SoakQueueConfig, SoakQueueService
    from qbt_orchestrator.db import migrate

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        executor = RecordingExecutor()
        cfg = SoakQueueConfig(resident_slots=8, min_free_bytes=0, disk_floor_bytes=0, max_qbt_active_downloads=3)
        svc = SoakQueueService(db, executor, dry_run=False, config=cfg, now=lambda: 1000)
        snapshots = {
            "a1": {"hash": "a1", "category": "auto", "tags": "auto", "state": "downloading", "amount_left": 1024**3, "progress": 0.1},
            "a2": {"hash": "a2", "category": "auto", "tags": "auto", "state": "stalledDL", "amount_left": 1024**3, "progress": 0.1},
            "h1": {"hash": "h1", "category": "auto", "tags": "auto", "state": "pausedDL", "amount_left": 1024**3, "progress": 0.9},
            "h2": {"hash": "h2", "category": "auto", "tags": "auto", "state": "pausedDL", "amount_left": 1024**3, "progress": 0.8},
        }

        result = svc.run_once(snapshots, free_bytes=20 * 1024**3, sync_healthy=True)

        assert result.started == ["h1"]
        assert executor.posts == [("/api/v2/torrents/start", {"hashes": "h1"})]



def test_soak_queue_marks_hot_after_confirm_window():
    from qbt_orchestrator.soak_queue import SoakQueueConfig, SoakQueueService
    from qbt_orchestrator.db import migrate

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        con.execute("insert into soak_state(hash,state,ema_dlspeed_bps,hot_since,resident_since,exposure_bytes,last_sample_at,updated_at,reason) values('hot','soak_resident',2097152,900,800,134217728,900,900,'hot_confirming')")
        con.commit(); con.close()
        executor = RecordingExecutor()
        cfg = SoakQueueConfig(resident_slots=1, min_free_bytes=0, disk_floor_bytes=0, hot_bps=1024**2, hot_confirm_sec=60)
        svc = SoakQueueService(db, executor, dry_run=False, config=cfg, now=lambda: 1000)
        snapshots = {"hot": {"hash": "hot", "category": "auto", "tags": "auto", "state": "downloading", "amount_left": 5 * 1024**3, "dlspeed": 2 * 1024**2, "progress": 0.2}}

        result = svc.run_once(snapshots, free_bytes=20 * 1024**3, sync_healthy=True)

        assert result.hot_hashes == ["hot"]
        row = _rows(db, "select state,reason from soak_state where hash='hot'")[0]
        assert row == {"state": "soak_hot", "reason": "hot_promoted"}
        decision = _rows(db, "select decision,reason_code from decision_log where hash='hot' order by id desc limit 1")[0]
        assert decision == {"decision": "hot_promoted", "reason_code": "hot_confirmed"}


def test_soak_queue_hot_preempts_high_write_pressure_active_when_budget_needed():
    from qbt_orchestrator.soak_queue import SoakQueueConfig, SoakQueueService
    from qbt_orchestrator.db import migrate

    gib = 1024**3
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        con.execute("insert into scheduler_allocations(hash,desired_state,applied_state,slot_kind,reserved_bytes,allocated_at,reason) values('victim','active','active','stable',?,?,?)", (2 * gib, 900, 'budget_fit'))
        con.execute("insert into resource_reservations(hash,kind,bytes,state,created_at,expires_at,reason) values('victim','active_download',?,'active',900,1200,'planner_active_download')", (2 * gib,))
        con.execute("insert into soak_state(hash,state,ema_dlspeed_bps,hot_since,resident_since,exposure_bytes,last_sample_at,updated_at,reason) values('hot','soak_resident',2097152,900,800,536870912,900,900,'hot_confirming')")
        con.commit(); con.close()
        executor = RecordingExecutor()
        cfg = SoakQueueConfig(
            resident_slots=1,
            min_free_bytes=0,
            disk_floor_bytes=2 * gib,
            max_total_exposure_bytes=4 * gib,
            max_per_torrent_exposure_bytes=512 * 1024**2,
            hot_bps=1024**2,
            hot_confirm_sec=60,
        )
        svc = SoakQueueService(db, executor, dry_run=False, config=cfg, now=lambda: 1000)
        snapshots = {
            "hot": {"hash": "hot", "category": "auto", "tags": "auto", "state": "downloading", "amount_left": 5 * gib, "dlspeed": 2 * 1024**2, "progress": 0.2},
            "victim": {"hash": "victim", "category": "auto", "tags": "auto", "state": "downloading", "amount_left": 2 * gib, "dlspeed": 4 * 1024**2, "progress": 0.2},
        }

        result = svc.run_once(snapshots, free_bytes=4 * gib, sync_healthy=True)

        assert result.hot_hashes == ["hot"]
        assert result.preempted_hashes == ["victim"]
        assert ("/api/v2/torrents/stop", {"hashes": "victim"}) in executor.posts
        reservation = _rows(db, "select state,reason from resource_reservations where hash='victim' and kind='active_download'")[0]
        assert reservation == {"state": "released", "reason": "hot_soak_preempted_active"}
        decision = _rows(db, "select decision,reason_code from decision_log where hash='victim' order by id desc limit 1")[0]
        assert decision == {"decision": "active_preempted", "reason_code": "hot_soak_preempted_active"}


def test_soak_queue_hot_does_not_preempt_hold_or_near_complete_active():
    from qbt_orchestrator.soak_queue import SoakQueueConfig, SoakQueueService
    from qbt_orchestrator.db import migrate

    gib = 1024**3
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        for h in ["hold", "near"]:
            con.execute("insert into scheduler_allocations(hash,desired_state,applied_state,slot_kind,reserved_bytes,allocated_at,reason) values(?,'active','active','stable',?,?,?)", (h, 2 * gib, 900, 'budget_fit'))
            con.execute("insert into resource_reservations(hash,kind,bytes,state,created_at,expires_at,reason) values(?,'active_download',?,'active',900,1200,'planner_active_download')", (h, 2 * gib))
        con.execute("insert into soak_state(hash,state,ema_dlspeed_bps,hot_since,resident_since,exposure_bytes,last_sample_at,updated_at,reason) values('hot','soak_resident',2097152,900,800,536870912,900,900,'hot_confirming')")
        con.commit(); con.close()
        executor = RecordingExecutor()
        cfg = SoakQueueConfig(resident_slots=1, min_free_bytes=0, disk_floor_bytes=2 * gib, max_total_exposure_bytes=4 * gib, hot_bps=1024**2, hot_confirm_sec=60)
        svc = SoakQueueService(db, executor, dry_run=False, config=cfg, now=lambda: 1000)
        snapshots = {
            "hot": {"hash": "hot", "category": "auto", "tags": "auto", "state": "downloading", "amount_left": 5 * gib, "dlspeed": 2 * 1024**2, "progress": 0.2},
            "hold": {"hash": "hold", "category": "auto", "tags": "auto,hold", "state": "downloading", "amount_left": 2 * gib, "dlspeed": 5 * 1024**2, "progress": 0.2},
            "near": {"hash": "near", "category": "auto", "tags": "auto", "state": "downloading", "amount_left": 100 * 1024**2, "dlspeed": 5 * 1024**2, "progress": 0.98},
        }

        result = svc.run_once(snapshots, free_bytes=4 * gib, sync_healthy=True)

        assert result.preempted_hashes == []
        stop_payloads = [payload["hashes"] for path, payload in executor.posts if path == "/api/v2/torrents/stop"]
        assert "hold" not in "|".join(stop_payloads)
        assert "near" not in "|".join(stop_payloads)
        decisions = _rows(db, "select decision,reason_code from decision_log where hash='hot' order by id")
        assert {"decision": "blocked", "reason_code": "no_safe_victim"} in decisions


def test_soak_queue_rejects_preemption_when_no_safe_victim():
    from qbt_orchestrator.soak_queue import SoakQueueConfig, SoakQueueService
    from qbt_orchestrator.db import migrate

    gib = 1024**3
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        con.execute("insert into soak_state(hash,state,ema_dlspeed_bps,hot_since,resident_since,exposure_bytes,last_sample_at,updated_at,reason) values('hot','soak_resident',2097152,900,800,536870912,900,900,'hot_confirming')")
        con.commit(); con.close()
        executor = RecordingExecutor()
        cfg = SoakQueueConfig(resident_slots=1, min_free_bytes=0, disk_floor_bytes=2 * gib, max_total_exposure_bytes=4 * gib, hot_bps=1024**2, hot_confirm_sec=60)
        svc = SoakQueueService(db, executor, dry_run=False, config=cfg, now=lambda: 1000)
        snapshots = {"hot": {"hash": "hot", "category": "auto", "tags": "auto", "state": "downloading", "amount_left": 5 * gib, "dlspeed": 2 * 1024**2, "progress": 0.2}}

        result = svc.run_once(snapshots, free_bytes=2 * gib + 128 * 1024**2, sync_healthy=True)

        assert result.hot_hashes == []
        assert result.preempted_hashes == []
        decisions = _rows(db, "select decision,reason_code from decision_log where hash='hot' order by id")
        assert {"decision": "blocked", "reason_code": "no_safe_victim"} in decisions


def test_soak_queue_throttles_hot_resident_near_disk_floor_instead_of_stopping_it():
    from qbt_orchestrator.soak_queue import SoakQueueConfig, SoakQueueService
    from qbt_orchestrator.db import migrate

    gib = 1024**3
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        con.execute(
            "insert into soak_state(hash,state,ema_dlspeed_bps,hot_since,resident_since,exposure_bytes,last_sample_at,updated_at,reason) "
            "values('hot','soak_resident',2097152,900,800,536870912,900,900,'hot_confirming')"
        )
        con.commit(); con.close()
        executor = RecordingExecutor()
        cfg = SoakQueueConfig(
            resident_slots=1,
            min_free_bytes=0,
            disk_floor_bytes=3 * gib,
            low_capacity_throttle_margin_bytes=1 * gib,
            low_capacity_soak_limit_bps=256 * 1024,
            max_total_exposure_bytes=4 * gib,
            max_per_torrent_exposure_bytes=512 * 1024**2,
            hot_bps=1024**2,
            hot_confirm_sec=60,
        )
        svc = SoakQueueService(db, executor, dry_run=False, config=cfg, now=lambda: 1000)
        snapshots = {
            "hot": {"hash": "hot", "category": "auto", "tags": "auto", "state": "downloading", "amount_left": 5 * gib, "dlspeed": 2 * 1024**2, "progress": 0.2}
        }

        result = svc.run_once(snapshots, free_bytes=3 * gib + 512 * 1024**2, sync_healthy=True)

        assert result.hot_hashes == ["hot"]
        assert result.throttled_hashes == ["hot"]
        assert executor.download_limits == [("hot", 256 * 1024)]
        assert ("/api/v2/torrents/stop", {"hashes": "hot"}) not in executor.posts
        row = _rows(db, "select reason from soak_state where hash='hot'")[0]
        assert row == {"reason": "low_capacity_throttled"}


def test_soak_queue_clears_previous_low_capacity_limit_after_capacity_recovers():
    from qbt_orchestrator.soak_queue import SoakQueueConfig, SoakQueueService
    from qbt_orchestrator.db import migrate

    gib = 1024**3
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        con.execute(
            "insert into soak_state(hash,state,ema_dlspeed_bps,hot_since,resident_since,exposure_bytes,last_sample_at,updated_at,reason) "
            "values('hot','soak_hot',2097152,900,800,536870912,900,900,'low_capacity_throttled')"
        )
        con.commit(); con.close()
        executor = RecordingExecutor()
        cfg = SoakQueueConfig(
            resident_slots=1,
            min_free_bytes=0,
            disk_floor_bytes=3 * gib,
            low_capacity_throttle_margin_bytes=1 * gib,
            low_capacity_soak_limit_bps=256 * 1024,
            max_total_exposure_bytes=4 * gib,
            hot_bps=1024**2,
            hot_confirm_sec=60,
        )
        svc = SoakQueueService(db, executor, dry_run=False, config=cfg, now=lambda: 1000)
        snapshots = {
            "hot": {"hash": "hot", "category": "auto", "tags": "auto", "state": "downloading", "amount_left": 5 * gib, "dlspeed": 2 * 1024**2, "progress": 0.2}
        }

        result = svc.run_once(snapshots, free_bytes=8 * gib, sync_healthy=True)

        assert result.unthrottled_hashes == ["hot"]
        assert executor.download_limits == [("hot", 0)]


def test_soak_queue_clears_low_capacity_limit_when_resident_is_reallocated_out():
    from qbt_orchestrator.soak_queue import SoakQueueConfig, SoakQueueService
    from qbt_orchestrator.db import migrate

    gib = 1024**3
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        con.execute(
            "insert into soak_state(hash,state,ema_dlspeed_bps,hot_since,resident_since,exposure_bytes,last_sample_at,updated_at,reason) "
            "values('old','soak_resident',2097152,900,800,536870912,900,900,'low_capacity_throttled')"
        )
        con.commit(); con.close()
        executor = RecordingExecutor()
        cfg = SoakQueueConfig(
            resident_slots=1,
            min_free_bytes=0,
            disk_floor_bytes=3 * gib,
            max_per_torrent_exposure_bytes=512 * 1024**2,
        )
        svc = SoakQueueService(db, executor, dry_run=False, config=cfg, now=lambda: 1000)
        snapshots = {
            "old": {"hash": "old", "category": "auto", "tags": "auto", "state": "downloading", "amount_left": 5 * gib, "dlspeed": 2 * 1024**2, "progress": 0.2}
        }

        result = svc.run_once(snapshots, free_bytes=3 * gib + 64 * 1024**2, sync_healthy=True)

        assert result.stopped == ["old"]
        assert executor.download_limits == [("old", 0)]
        assert ("/api/v2/torrents/stop", {"hashes": "old"}) in executor.posts


if __name__ == "__main__":
    inspect = __import__("inspect")
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn) and not inspect.signature(fn).parameters:
            fn()
    print("ok")
