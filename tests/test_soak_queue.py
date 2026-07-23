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


def test_soak_queue_emits_probe_intents_without_direct_qbt_or_allocation_writes():
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
        assert executor.seq == []
        assert executor.posts == []
        assert _rows(db, "select * from scheduler_allocations") == []
        intents = _rows(
            db,
            "select component,hash,intent,priority,expires_at,data_json from scheduler_intents order by hash",
        )
        assert [
            (r["component"], r["hash"], r["intent"], r["priority"], r["expires_at"])
            for r in intents
        ] == [
            ("soak", "h1", "probe", 30, 1120),
            ("soak", "h2", "probe", 30, 1120),
        ]
        assert [json.loads(r["data_json"]) for r in intents] == [
            {"exposure_bytes": 128 * 1024**2},
            {"exposure_bytes": 128 * 1024**2},
        ]
        reservations = _rows(db, "select hash,kind,bytes,state,reason from resource_reservations order by hash")
        assert [(r["hash"], r["kind"], r["bytes"], r["state"], r["reason"]) for r in reservations] == [
            ("h1", "soak_probe", 128 * 1024**2, "active", "soak_resident"),
            ("h2", "soak_probe", 128 * 1024**2, "active", "soak_resident"),
        ]


def test_soak_never_starts_new_resident_in_drain_mode():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.soak_queue import SoakQueueConfig, SoakQueueService

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        svc = SoakQueueService(
            db,
            RecordingExecutor(),
            dry_run=False,
            config=SoakQueueConfig(disk_floor_bytes=0),
            now=lambda: 1_000,
        )

        result = svc.run_once(
            {
                "candidate": {
                    "hash": "candidate",
                    "category": "auto",
                    "tags": "auto",
                    "state": "stoppedDL",
                    "amount_left": 1024**3,
                    "size": 1024**3,
                    "num_seeds": 2,
                    "num_peers": 3,
                }
            },
            free_bytes=4 * 1024**3,
            sync_healthy=True,
            scheduler_mode="drain",
        )

        assert result.started == []
        assert result.blocked_reason == "mode_disallows_new_probe"
        assert _rows(db, "select * from scheduler_intents") == []


def test_soak_requires_swarm_and_selects_only_observed_swarm_candidate():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.soak_queue import SoakQueueConfig, SoakQueueService

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        svc = SoakQueueService(
            db,
            RecordingExecutor(),
            dry_run=False,
            config=SoakQueueConfig(
                resident_slots=2,
                disk_floor_bytes=0,
                require_swarm=True,
                max_cold_partial_bytes=10 * 1024**3,
                max_cold_partial_torrents=10,
            ),
            now=lambda: 1_000,
        )
        snapshots = {
            "zero": {"hash": "zero", "category": "auto", "tags": "auto", "state": "stoppedDL", "amount_left": 1024**3, "size": 1024**3, "num_seeds": 0, "num_peers": 0},
            "swarm": {"hash": "swarm", "category": "auto", "tags": "auto", "state": "stoppedDL", "amount_left": 1024**3, "size": 1024**3, "num_seeds": 2, "num_peers": 3},
        }

        result = svc.run_once(snapshots, free_bytes=10 * 1024**3, sync_healthy=True)

        assert result.started == ["swarm"]
        assert _rows(db, "select hash from scheduler_intents") == [{"hash": "swarm"}]
        assert {row["reason_code"] for row in _rows(db, "select reason_code from decision_log where hash='zero'")} == {"swarm_required"}


def test_soak_blocks_new_probe_at_cold_partial_debt_cap_and_records_metric():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.soak_queue import SoakQueueConfig, SoakQueueService

    gib = 1024**3
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        svc = SoakQueueService(
            db,
            RecordingExecutor(),
            dry_run=False,
            config=SoakQueueConfig(
                disk_floor_bytes=0,
                max_cold_partial_bytes=gib,
                max_cold_partial_torrents=8,
            ),
            now=lambda: 1_000,
        )

        result = svc.run_once(
            {
                "partial": {
                    "hash": "partial",
                    "category": "auto",
                    "tags": "auto",
                    "state": "stoppedDL",
                    "amount_left": gib,
                    "size": 2 * gib,
                    "completed_bytes": gib,
                    "progress": 0.5,
                    "num_seeds": 2,
                    "num_peers": 3,
                }
            },
            free_bytes=10 * gib,
            sync_healthy=True,
        )

        assert result.started == []
        assert result.blocked_reason == "cold_partial_debt_cap_reached"
        metric = json.loads(
            _rows(
                db,
                "select metrics_json from metrics_snapshots where component='soak_partial_debt' order by id desc limit 1",
            )[0]["metrics_json"]
        )
        assert metric["cold_partial_bytes"] == gib
        assert metric["cold_partial_torrents"] == 1
        assert metric["blocked_new_probes"] is True
        assert _rows(db, "select * from scheduler_intents") == []


def test_soak_respects_max_new_probes_per_hour():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.soak_queue import SoakQueueConfig, SoakQueueService

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        for index in range(4):
            con.execute(
                "insert into decision_log(ts,component,hash,decision,reason_code,data_json) values(?,?,?,?,?,?)",
                (900 + index, "soak_queue", f"old-{index}", "resident_start", "budget_fit", "{}"),
            )
        con.commit()
        con.close()
        svc = SoakQueueService(
            db,
            RecordingExecutor(),
            dry_run=False,
            config=SoakQueueConfig(disk_floor_bytes=0, max_new_per_hour=4),
            now=lambda: 1_000,
        )

        result = svc.run_once(
            {"new": {"hash": "new", "category": "auto", "tags": "auto", "state": "stoppedDL", "amount_left": 1, "size": 1, "num_seeds": 1}},
            free_bytes=10 * 1024**3,
            sync_healthy=True,
        )

        assert result.started == []
        assert result.blocked_reason == "hourly_probe_cap_reached"


def test_planner_created_soak_state_does_not_consume_probe_hourly_cap():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.soak_queue import SoakQueueConfig, SoakQueueService

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        con.execute(
            "insert into soak_state(hash,state,last_started_at,updated_at,reason) values(?,?,?,?,?)",
            ("planner-demoted", "soak_resident", 900, 900, "recovery_active_slow"),
        )
        con.commit()
        con.close()
        svc = SoakQueueService(
            db,
            RecordingExecutor(),
            dry_run=False,
            config=SoakQueueConfig(disk_floor_bytes=0, max_new_per_hour=1),
            now=lambda: 1_000,
        )

        result = svc.run_once(
            {"new": {"hash": "new", "category": "auto", "tags": "auto", "state": "stoppedDL", "amount_left": 1, "size": 1, "num_seeds": 1}},
            free_bytes=10 * 1024**3,
            sync_healthy=True,
        )

        assert result.started == ["new"]
        assert result.blocked_reason is None


def test_soak_config_reads_mode_swarm_and_partial_debt_env():
    from qbt_orchestrator.cli import _build_soak_config_from_env

    config = _build_soak_config_from_env(
        {
            "QBT_ORCH_SOAK_ALLOWED_MODES": "normal,explore,custom",
            "QBT_ORCH_SOAK_REQUIRE_SWARM": "0",
            "QBT_ORCH_MAX_COLD_PARTIAL_GB": "2.5",
            "QBT_ORCH_MAX_COLD_PARTIAL_TORRENTS": "6",
            "QBT_ORCH_SOAK_MAX_NEW_PER_HOUR": "3",
        }
    )

    assert config.allowed_modes == ("normal", "explore", "custom")
    assert config.require_swarm is False
    assert config.max_cold_partial_bytes == int(2.5 * 1024**3)
    assert config.max_cold_partial_torrents == 6
    assert config.max_new_per_hour == 3


def test_soak_dry_run_never_emits_actionable_intent_or_reservation():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.soak_queue import SoakQueueConfig, SoakQueueService

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        svc = SoakQueueService(
            db,
            RecordingExecutor(),
            dry_run=True,
            config=SoakQueueConfig(resident_slots=1, disk_floor_bytes=0),
            now=lambda: 1_000,
        )

        result = svc.run_once(
            {"preview": {"hash": "preview", "category": "auto", "tags": "auto", "state": "stoppedDL", "amount_left": 1, "size": 1, "num_seeds": 1}},
            free_bytes=10 * 1024**3,
            sync_healthy=True,
        )

        assert result.started == ["preview"]
        assert result.dry_run is True
        assert _rows(db, "select * from scheduler_intents") == []
        assert _rows(db, "select * from resource_reservations") == []

        live = SoakQueueService(
            db,
            RecordingExecutor(),
            dry_run=False,
            config=SoakQueueConfig(resident_slots=1, disk_floor_bytes=0, max_new_per_hour=1),
            now=lambda: 1_001,
        )
        live_result = live.run_once(
            {"live": {"hash": "live", "category": "auto", "tags": "auto", "state": "stoppedDL", "amount_left": 1, "size": 1, "num_seeds": 1}},
            free_bytes=10 * 1024**3,
            sync_healthy=True,
        )
        assert live_result.started == ["live"]


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
            {"h1": {"hash": "h1", "category": "auto", "tags": "auto", "state": "pausedDL", "amount_left": gib, "piece_size": 4 * 1024**2, "progress": 0.1, "num_seeds": 1}},
            free_bytes=6 * gib,
            sync_healthy=True,
        )

        assert result.blocked_reason is None
        assert result.started == ["h1"]
        assert executor.posts == []
        assert _rows(db, "select hash,intent from scheduler_intents") == [
            {"hash": "h1", "intent": "probe"}
        ]
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
            {"h1": {"hash": "h1", "category": "auto", "tags": "auto", "state": "pausedDL", "amount_left": gib, "piece_size": 4 * 1024**2, "progress": 0.1, "num_seeds": 1}},
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
            "h1": {"hash": "h1", "category": "auto", "tags": "auto", "state": "pausedDL", "amount_left": 1024**3, "progress": 0.9, "num_seeds": 1},
            "h2": {"hash": "h2", "category": "auto", "tags": "auto", "state": "pausedDL", "amount_left": 1024**3, "progress": 0.8, "num_seeds": 1},
        }

        result = svc.run_once(snapshots, free_bytes=20 * 1024**3, sync_healthy=True)

        assert result.started == ["h1"]
        assert executor.posts == []
        assert _rows(db, "select hash,intent from scheduler_intents") == [
            {"hash": "h1", "intent": "probe"}
        ]



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
        assert executor.posts == []
        victim_intent = _rows(
            db,
            "select component,hash,intent,expires_at from scheduler_intents where hash='victim'",
        )[0]
        assert victim_intent == {
            "component": "soak",
            "hash": "victim",
            "intent": "cooldown",
            "expires_at": 2800,
        }
        victim_allocation = _rows(
            db,
            "select desired_state,reason from scheduler_allocations where hash='victim'",
        )[0]
        assert victim_allocation == {"desired_state": "active", "reason": "budget_fit"}
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


def test_soak_queue_recovery_never_preserves_resident_with_zero_growth_claim():
    from qbt_orchestrator.soak_queue import SoakQueueConfig, SoakQueueService
    from qbt_orchestrator.db import migrate

    gib = 1024**3
    mib = 1024**2
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        con.execute(
            "insert into soak_state(hash,state,ema_dlspeed_bps,hot_since,resident_since,exposure_bytes,last_sample_at,updated_at,reason) "
            "values('soak-hot','soak_resident',2097152,null,900,536870912,900,900,'resident')"
        )
        con.execute(
            "insert into scheduler_allocations(hash,desired_state,applied_state,slot_kind,reserved_bytes,allocated_at,reason) "
            "values('soak-hot','soak_resident','soak_resident','soak_resident',536870912,900,'resident')"
        )
        con.execute(
            "insert into resource_reservations(hash,kind,bytes,state,created_at,expires_at,reason) "
            "values('soak-hot','soak_probe',536870912,'active',900,1200,'soak_resident')"
        )
        con.commit(); con.close()
        executor = RecordingExecutor()
        cfg = SoakQueueConfig(
            resident_slots=8,
            disk_floor_bytes=3 * gib,
            emergency_floor_bytes=int(1.5 * gib),
            recovery_margin_bytes=256 * mib,
            max_total_exposure_bytes=4 * gib,
            low_capacity_throttle_margin_bytes=1 * gib,
            low_capacity_throttle_trigger_bps=1024**2,
            low_capacity_soak_limit_bps=256 * 1024,
            hot_bps=1024**2,
            hot_confirm_sec=60,
        )
        svc = SoakQueueService(db, executor, dry_run=False, config=cfg, now=lambda: 1000)
        snapshots = {
            "soak-hot": {
                "hash": "soak-hot",
                "category": "auto",
                "tags": "auto",
                "state": "downloading",
                "amount_left": 2 * gib,
                "size": 4 * gib,
                "progress": 0.5,
                "dlspeed": 2 * 1024**2,
                "piece_size": 16 * mib,
            }
        }

        result = svc.run_once(snapshots, free_bytes=2 * gib, sync_healthy=True)

        assert result.stopped == ["soak-hot"]
        assert result.resident_hashes == []
        assert executor.download_limits == []
        reservations = _rows(db, "select hash,state,bytes,reason from resource_reservations where hash='soak-hot' and kind='soak_probe' order by id")
        assert reservations[-1] == {
            "hash": "soak-hot",
            "state": "released",
            "bytes": 536870912,
            "reason": "soak_reallocated",
        }
        assert _rows(db, "select hash,intent from scheduler_intents where hash='soak-hot'") == [
            {"hash": "soak-hot", "intent": "cooldown"}
        ]


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
        assert executor.posts == []
        assert _rows(
            db,
            "select hash,intent,expires_at from scheduler_intents where hash='old'",
        ) == [{"hash": "old", "intent": "cooldown", "expires_at": 2800}]


if __name__ == "__main__":
    inspect = __import__("inspect")
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn) and not inspect.signature(fn).parameters:
            fn()
    print("ok")
