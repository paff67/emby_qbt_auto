#!/usr/bin/env python3
from __future__ import annotations

import json
import sqlite3
import tempfile
import time
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))

GIB = 1024**3


def test_torrent_snapshot_preserves_capacity_viability_fields():
    from qbt_orchestrator.models import TorrentSnapshot

    snapshot = TorrentSnapshot.from_qbt(
        {
            "hash": "h1",
            "availability": 0.996,
            "last_activity": 123,
            "seen_complete": 99,
        }
    )

    assert snapshot.availability == 0.996
    assert snapshot.last_activity == 123
    assert snapshot.seen_complete == 99


def test_drain_mode_requires_exit_watermark_to_recover():
    from qbt_orchestrator.capacity_state import ModeController

    controller = ModeController(
        emergency_enter=int(1.5 * GIB),
        drain_enter=3 * GIB,
        drain_exit=5 * GIB,
        explore_enter=8 * GIB,
    )

    assert controller.next_mode("normal", int(2.9 * GIB)) == "drain"
    assert controller.next_mode("drain", int(4.9 * GIB)) == "drain"
    assert controller.next_mode("drain", int(5.1 * GIB)) == "normal"


def test_emergency_exits_through_drain_and_explore_requires_high_watermark():
    from qbt_orchestrator.capacity_state import ModeController

    controller = ModeController(1 * GIB, 3 * GIB, 5 * GIB, 8 * GIB)

    assert controller.next_mode("normal", GIB - 1) == "emergency"
    assert controller.next_mode("emergency", 2 * GIB) == "drain"
    assert controller.next_mode("drain", 8 * GIB) == "explore"
    assert controller.next_mode("explore", 7 * GIB) == "normal"


def test_capacity_deadlock_never_creates_delete_or_hold_actions():
    from qbt_orchestrator.capacity_state import detect_capacity_state

    result = detect_capacity_state(
        mode="drain",
        managed_incomplete=10,
        feasible_full_finish=0,
        disk_releasing_jobs=0,
    )

    assert result.state == "capacity_deadlock"
    assert result.reason == "no_finishable_or_releasing_work"
    assert result.actions == []


def test_progress_possible_for_non_drain_or_any_feasible_release_path():
    from qbt_orchestrator.capacity_state import detect_capacity_state

    assert detect_capacity_state(mode="normal", managed_incomplete=10, feasible_full_finish=0, disk_releasing_jobs=0).state == "progress_possible"
    assert detect_capacity_state(mode="drain", managed_incomplete=10, feasible_full_finish=1, disk_releasing_jobs=0).state == "progress_possible"
    assert detect_capacity_state(mode="drain", managed_incomplete=10, feasible_full_finish=0, disk_releasing_jobs=1).state == "progress_possible"


def test_capacity_observation_excludes_hold_and_orders_manual_candidates():
    from qbt_orchestrator.capacity_state import build_capacity_observation

    observation = build_capacity_observation(
        {
            "held": {"hash": "held", "category": "auto", "tags": "auto,hold", "amount_left": GIB},
            "big": {"hash": "big", "category": "auto", "tags": "auto", "amount_left": 5 * GIB},
            "small": {"hash": "small", "category": "auto", "tags": "auto", "amount_left": 2 * GIB},
            "unmanaged": {"hash": "unmanaged", "category": "", "tags": "", "amount_left": 1},
        },
        available_growth_bytes=2 * GIB,
        selected_hashes=set(),
        disk_releasing_jobs=0,
        free_bytes=4 * GIB,
    )

    assert observation.managed_incomplete == 2
    assert observation.feasible_full_finish == 1
    assert observation.required_minimum_growth_bytes == 2 * GIB
    assert [item["hash"] for item in observation.top_manual_candidates] == ["small", "big"]


def test_capacity_observation_does_not_count_stale_unavailable_finish_as_feasible():
    from qbt_orchestrator.capacity_state import build_capacity_observation

    observation = build_capacity_observation(
        {
            "stuck": {
                "hash": "stuck",
                "category": "auto",
                "tags": "auto",
                "amount_left": 8 * 1024**2,
                "availability": 0.996,
                "num_seeds": 0,
                "dlspeed_bps": 0,
            },
            "large-viable": {
                "hash": "large-viable",
                "category": "auto",
                "tags": "auto",
                "amount_left": 4 * GIB,
                "availability": 1.0,
                "num_seeds": 1,
                "dlspeed_bps": 0,
            },
        },
        available_growth_bytes=GIB,
        selected_hashes={"stuck"},
        disk_releasing_jobs=0,
        free_bytes=3 * GIB,
        health_by_hash={
            "stuck": {"no_progress_since": 100},
            "large-viable": {"no_progress_since": 100},
        },
        observed_at=4_000,
        viability_stale_sec=1_800,
    )

    assert observation.managed_incomplete == 2
    assert observation.viable_finish == 1
    assert observation.feasible_full_finish == 0
    assert observation.nonviable_finish == 1


def test_capacity_deadlock_can_be_detected_under_pressure_before_drain_entry():
    from qbt_orchestrator.capacity_state import detect_capacity_state

    result = detect_capacity_state(
        mode="normal",
        managed_incomplete=10,
        feasible_full_finish=0,
        disk_releasing_jobs=0,
        capacity_pressure=True,
    )

    assert result.state == "capacity_deadlock"


def test_capacity_state_store_preserves_entered_at_until_real_transition():
    from qbt_orchestrator.capacity_state import CapacityStateStore, detect_capacity_state
    from qbt_orchestrator.db import migrate

    clock = [100]
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        store = CapacityStateStore(db, now=lambda: clock[0])
        deadlock = detect_capacity_state(mode="drain", managed_incomplete=2, feasible_full_finish=0, disk_releasing_jobs=0)

        first = store.persist("drain", deadlock, {"managed_incomplete": 2})
        clock[0] = 110
        repeated = store.persist("drain", deadlock, {"managed_incomplete": 2})
        clock[0] = 120
        recovered = store.persist(
            "normal",
            detect_capacity_state(mode="normal", managed_incomplete=2, feasible_full_finish=1, disk_releasing_jobs=0),
            {"managed_incomplete": 2},
        )

        assert first.transitioned is True
        assert first.entered_at == 100
        assert repeated.transitioned is False
        assert repeated.entered_at == 100
        assert recovered.transitioned is True
        assert recovered.previous_state == "capacity_deadlock"
        assert recovered.entered_at == 120

        con = sqlite3.connect(db)
        con.row_factory = sqlite3.Row
        try:
            row = dict(con.execute("select * from capacity_state where id=1").fetchone())
        finally:
            con.close()
        assert row["scheduler_mode"] == "normal"
        assert row["state"] == "progress_possible"
        assert row["entered_at"] == 120
        assert row["last_evaluated_at"] == 120
        assert json.loads(row["details_json"]) == {"managed_incomplete": 2}


def test_capacity_deadlock_alert_is_episode_deduplicated_and_contains_no_actions():
    from qbt_orchestrator.alerts import SchedulerAlertConfig, SchedulerAlertService
    from qbt_orchestrator.capacity_state import CapacityTransition
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.runtime import BotNotificationRepository

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        service = SchedulerAlertService(
            BotNotificationRepository(db, now=lambda: 100),
            SchedulerAlertConfig(
                enabled=True,
                chat_ids=["123"],
                capacity_deadlock_enabled=True,
            ),
            now=lambda: 100,
        )
        transition = CapacityTransition(
            scheduler_mode="drain",
            state="capacity_deadlock",
            reason="no_finishable_or_releasing_work",
            entered_at=100,
            last_evaluated_at=100,
            details={
                "managed_incomplete": 10,
                "feasible_full_finish": 0,
                "disk_releasing_jobs": 0,
            },
            transitioned=True,
            previous_state="progress_possible",
        )
        candidates = [
            {"hash": "h1", "required_growth_bytes": 2 * GIB},
            {"hash": "h2", "required_growth_bytes": 3 * GIB},
            {"hash": "h3", "required_growth_bytes": 4 * GIB},
            {"hash": "h4", "required_growth_bytes": 5 * GIB},
        ]

        first = service.enqueue_capacity_deadlock(
            transition,
            required_minimum_growth_bytes=2 * GIB,
            top_manual_candidates=candidates,
        )
        repeated = service.enqueue_capacity_deadlock(
            transition,
            required_minimum_growth_bytes=2 * GIB,
            top_manual_candidates=candidates,
        )

        assert first == repeated
        con = sqlite3.connect(db)
        con.row_factory = sqlite3.Row
        try:
            rows = [dict(row) for row in con.execute("select * from bot_notifications")]
        finally:
            con.close()
        assert len(rows) == 1
        payload = json.loads(rows[0]["payload_json"])
        assert payload["managed_incomplete"] == 10
        assert payload["required_minimum_growth_bytes"] == 2 * GIB
        assert len(payload["top_manual_candidates"]) == 3
        forbidden = (rows[0]["message"] + rows[0]["payload_json"]).lower()
        assert all(word not in forbidden for word in ("delete", "cleanup", "hold", "remove", "config"))


def test_capacity_alert_ignores_repeated_or_recovered_state():
    from qbt_orchestrator.alerts import SchedulerAlertConfig, SchedulerAlertService
    from qbt_orchestrator.capacity_state import CapacityTransition
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.runtime import BotNotificationRepository

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        service = SchedulerAlertService(
            BotNotificationRepository(db),
            SchedulerAlertConfig(enabled=True, chat_ids=["123"], capacity_deadlock_enabled=True),
        )
        common = {
            "scheduler_mode": "drain",
            "reason": "no_finishable_or_releasing_work",
            "entered_at": 100,
            "last_evaluated_at": 110,
            "details": {},
            "previous_state": "capacity_deadlock",
        }

        assert service.enqueue_capacity_deadlock(
            CapacityTransition(state="capacity_deadlock", transitioned=False, **common),
            required_minimum_growth_bytes=0,
            top_manual_candidates=[],
        ) == []
        assert service.enqueue_capacity_deadlock(
            CapacityTransition(state="progress_possible", transitioned=True, **common),
            required_minimum_growth_bytes=0,
            top_manual_candidates=[],
        ) == []


def test_daemon_persists_deadlock_without_actions_and_alerts_once_until_recovery():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.service import DaemonRuntime
    from tests.test_daemon_runtime import FakeExecutor, FakeQbt

    class DeadlockedQbt(FakeQbt):
        def get_maindata(self, rid):
            self.rids.append(rid)
            return {
                "rid": rid + 1,
                "full_update": True,
                "torrents": {
                    "huge": {
                        "hash": "huge",
                        "name": "too-large",
                        "category": "auto",
                        "tags": "auto",
                        "state": "stoppedDL",
                        "amount_left": 10 * GIB,
                        "size": 12 * GIB,
                        "progress": 0.1,
                    }
                },
                "server_state": {},
            }

    free = [3 * GIB + 64 * 1024**2]
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        executor = FakeExecutor()
        daemon = DaemonRuntime(
            state_db=db,
            qbt=DeadlockedQbt(),
            executor=executor,
            free_bytes_provider=lambda: free[0],
            dry_run=True,
            safety_interval=0,
            disk_floor_bytes=3 * GIB,
            recovery_enter_bytes=int(3.5 * GIB),
            drain_exit_bytes=5 * GIB,
            explore_enter_bytes=8 * GIB,
            scheduler_alert_chat_ids=["123"],
            scheduler_alerts_enabled=True,
            capacity_deadlock_alerts_enabled=True,
        )

        daemon.tick_safety()
        first = daemon.planner_tick()
        repeated = daemon.planner_tick()
        free[0] = 6 * GIB
        recovered = daemon.planner_tick()

        assert first["capacity"]["state"] == "capacity_deadlock"
        assert first["capacity"]["actions"] == []
        assert repeated["capacity"]["transitioned"] is False
        assert recovered["capacity"]["state"] == "progress_possible"
        assert recovered["capacity"]["scheduler_mode"] == "normal"
        assert not any("delete" in path.lower() for path, _payload in executor.posts)

        con = sqlite3.connect(db)
        con.row_factory = sqlite3.Row
        try:
            capacity = dict(con.execute("select * from capacity_state where id=1").fetchone())
            notices = [
                dict(row)
                for row in con.execute(
                    "select topic,message,payload_json from bot_notifications where topic='capacity_deadlock'"
                )
            ]
        finally:
            con.close()
        assert capacity["state"] == "progress_possible"
        assert len(notices) == 1
        assert "manual intervention required" in notices[0]["message"]


def test_daemon_marks_stale_unavailable_finish_deadlocked_under_capacity_pressure():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.service import DaemonRuntime
    from tests.test_daemon_runtime import FakeExecutor, FakeQbt

    class StuckFinishQbt(FakeQbt):
        def get_maindata(self, rid):
            self.rids.append(rid)
            return {
                "rid": rid + 1,
                "full_update": True,
                "torrents": {
                    "stuck": {
                        "hash": "stuck",
                        "name": "stuck",
                        "category": "auto",
                        "tags": "auto",
                        "state": "stalledDL",
                        "amount_left": 8 * 1024**2,
                        "size": 6 * GIB,
                        "completed": 6 * GIB - 8 * 1024**2,
                        "progress": 0.999,
                        "availability": 0.996,
                        "num_seeds": 0,
                        "num_incomplete": 2,
                        "dlspeed": 0,
                    }
                },
                "server_state": {},
            }

    now = int(time.time())
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        con.execute(
            "insert into torrent_health(hash,sampled_at,dlspeed_bps,completed_bytes,last_completed_bytes,progress,"
            "num_seeds,num_peers,low_speed_since,no_progress_since,active_since,updated_at) "
            "values('stuck',?,0,?,?,0.999,0,2,?,?,?,?)",
            (
                now - 3_600,
                6 * GIB - 8 * 1024**2,
                6 * GIB - 8 * 1024**2,
                now - 3_600,
                now - 3_600,
                now - 3_600,
                now - 3_600,
            ),
        )
        con.commit()
        con.close()
        class RecordingReclaimer:
            def __init__(self):
                self.calls = []

            def run(self, snapshots, **kwargs):
                self.calls.append((snapshots, kwargs))

                class Result:
                    def as_dict(self):
                        return {"dry_run": True, "planned": 1, "candidates": [{"hash": "dead"}]}

                return Result()

        reclaimer = RecordingReclaimer()
        daemon = DaemonRuntime(
            state_db=db,
            qbt=StuckFinishQbt(),
            executor=FakeExecutor(),
            free_bytes_provider=lambda: int(3.25 * GIB),
            dry_run=True,
            safety_interval=0,
            disk_floor_bytes=3 * GIB,
            emergency_floor_bytes=2 * GIB,
            recovery_enter_bytes=3 * GIB,
            drain_exit_bytes=5 * GIB,
            finish_resident_max_remaining_bytes=256 * 1024**2,
            finish_resident_max_stall_sec=1_800,
            capacity_viability_stale_sec=1_800,
            capacity_reclaimer=reclaimer,
            scheduler_engine_mode="live",
        )

        daemon.tick_safety()
        result = daemon.planner_tick()

        assert result["capacity"]["scheduler_mode"] == "normal"
        assert result["capacity"]["state"] == "capacity_deadlock"
        assert result["capacity"]["details"]["feasible_full_finish"] == 0
        assert result["capacity"]["details"]["nonviable_finish"] == 1
        assert result["planner"]["selected_hashes"] == []
        assert result["capacity_reclaim"]["planned"] == 1
        assert reclaimer.calls[0][1] == {
            "capacity_state": "capacity_deadlock",
            "free_bytes": int(3.25 * GIB),
            "target_free_bytes": 5 * GIB,
        }


def test_daemon_records_redacted_effective_scheduler_config_at_startup():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.service import DaemonRuntime
    from tests.test_daemon_runtime import FakeExecutor, FakeQbt

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        daemon = DaemonRuntime(
            state_db=db,
            qbt=FakeQbt(),
            executor=FakeExecutor(),
            free_bytes_provider=lambda: 6 * GIB,
            dry_run=True,
            safety_interval=0,
            emergency_floor_bytes=int(1.5 * GIB),
            recovery_enter_bytes=3 * GIB,
            drain_exit_bytes=5 * GIB,
            explore_enter_bytes=8 * GIB,
            capacity_deadlock_alerts_enabled=True,
        )

        daemon.run(max_safety_ticks=1)

        con = sqlite3.connect(db)
        try:
            row = con.execute(
                "select data_json from events_v2 where component='daemon' and event_type='effective_config'"
            ).fetchone()
        finally:
            con.close()
        assert row is not None
        config = json.loads(row[0])
        assert config["thresholds"] == {
            "emergency_enter_bytes": int(1.5 * GIB),
            "drain_enter_bytes": 3 * GIB,
            "drain_exit_bytes": 5 * GIB,
            "explore_enter_bytes": 8 * GIB,
        }
        assert config["feature_flags"]["capacity_deadlock_alerts"] is True
        assert config["feature_flags"]["capacity_reclaim"] is False
        assert "token" not in row[0].lower()
