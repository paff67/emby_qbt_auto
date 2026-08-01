#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import sqlite3
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest


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


def test_capacity_state_reasons_prefer_admission_stats_over_stale_viable():
    from qbt_orchestrator.capacity_state import detect_capacity_state

    assert detect_capacity_state(
        mode="drain",
        managed_incomplete=3,
        feasible_full_finish=1,
        disk_releasing_jobs=0,
        capacity_pressure=True,
        planned_selected_count=1,
        active_probe_count=0,
        full_finish_runnable_count=1,
        probeable_count=0,
        cooldown_count=0,
    ).reason == "feasible_work_selected"
    assert detect_capacity_state(
        mode="drain",
        managed_incomplete=3,
        feasible_full_finish=0,
        disk_releasing_jobs=0,
        capacity_pressure=True,
        active_probe_count=1,
        planned_selected_count=0,
        full_finish_runnable_count=0,
        probeable_count=0,
        cooldown_count=0,
    ).reason == "availability_probe_active"
    # Speculative probeable / cooldown must not mask pressure deadlock.
    pressure_probeable = detect_capacity_state(
        mode="drain",
        managed_incomplete=3,
        feasible_full_finish=0,
        disk_releasing_jobs=0,
        capacity_pressure=True,
        probeable_count=2,
        planned_selected_count=0,
        active_probe_count=0,
        full_finish_runnable_count=0,
        cooldown_count=0,
    )
    assert pressure_probeable.state == "capacity_deadlock"
    assert detect_capacity_state(
        mode="drain",
        managed_incomplete=3,
        feasible_full_finish=1,
        disk_releasing_jobs=0,
        capacity_pressure=True,
        cooldown_count=3,
        planned_selected_count=0,
        active_probe_count=0,
        full_finish_runnable_count=0,
        probeable_count=0,
    ).state == "capacity_deadlock"
    assert detect_capacity_state(
        mode="normal",
        managed_incomplete=3,
        feasible_full_finish=0,
        disk_releasing_jobs=0,
        capacity_pressure=False,
        planned_selected_count=0,
        active_probe_count=0,
        probeable_count=2,
        full_finish_runnable_count=0,
        cooldown_count=0,
    ).reason == "availability_probe_pending"
    assert detect_capacity_state(
        mode="normal",
        managed_incomplete=3,
        feasible_full_finish=0,
        disk_releasing_jobs=0,
        capacity_pressure=False,
        planned_selected_count=0,
        active_probe_count=0,
        probeable_count=0,
        full_finish_runnable_count=0,
        cooldown_count=3,
    ).reason == "cooldown_wait"
    assert detect_capacity_state(
        mode="normal",
        managed_incomplete=3,
        feasible_full_finish=0,
        disk_releasing_jobs=0,
        capacity_pressure=False,
        planned_selected_count=0,
        active_probe_count=0,
        probeable_count=0,
        full_finish_runnable_count=0,
        cooldown_count=0,
    ).reason == "no_runnable_source"


def test_explicit_zero_admission_stats_are_not_treated_as_absent():
    from qbt_orchestrator.capacity_state import detect_capacity_state

    # Legacy path without admission kwargs still trusts feasible_full_finish.
    legacy = detect_capacity_state(
        mode="drain",
        managed_incomplete=3,
        feasible_full_finish=1,
        disk_releasing_jobs=0,
        capacity_pressure=True,
    )
    assert legacy.state == "progress_possible"
    assert legacy.reason == "feasible_work_exists"

    # Explicit zeros mean "no admitted work", even if observation feasible > 0.
    explicit = detect_capacity_state(
        mode="drain",
        managed_incomplete=3,
        feasible_full_finish=1,
        disk_releasing_jobs=0,
        capacity_pressure=True,
        planned_selected_count=0,
        active_probe_count=0,
        probeable_count=0,
        full_finish_runnable_count=0,
        cooldown_count=0,
    )
    assert explicit.state == "capacity_deadlock"


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


def test_capacity_observation_from_assessment_uses_recorded_viability():
    from qbt_orchestrator.capacity_assessment import (
        CapacityAssessment,
        TorrentCapacityEvidence,
    )
    from qbt_orchestrator.capacity_state import (
        build_capacity_observation_from_assessment,
    )

    evidence = TorrentCapacityEvidence(
        hash="h",
        managed=True,
        incomplete=True,
        amount_left=40,
        completed_bytes=60,
        availability=1.0,
        complete_sources=1,
        no_progress_since=None,
        viable=False,
        viability_reason="assessment_override",
    )
    assessment = CapacityAssessment(
        generation=9,
        observed_at=50,
        scheduler_mode="drain",
        free_bytes=70,
        target_free_bytes=100,
        available_growth_bytes=50,
        selected_hashes=frozenset({"h"}),
        disk_releasing_jobs=2,
        torrents={"h": evidence},
    )

    observation = build_capacity_observation_from_assessment(assessment)

    assert observation.managed_incomplete == 1
    assert observation.viable_finish == 0
    assert observation.nonviable_finish == 1
    assert observation.feasible_full_finish == 0
    assert observation.disk_releasing_jobs == 2
    assert observation.required_minimum_growth_bytes == 40
    assert observation.available_growth_bytes == 50
    assert observation.free_bytes == 70
    assert observation.top_manual_candidates == (
        {
            "hash": "h",
            "required_growth_bytes": 40,
            "viable": False,
            "viability_reason": "assessment_override",
        },
    )


def test_capacity_observation_from_assessment_uses_actual_planner_budget_and_selection():
    from qbt_orchestrator.capacity_assessment import (
        CapacityAssessment,
        TorrentCapacityEvidence,
    )
    from qbt_orchestrator.capacity_state import (
        build_capacity_observation_from_assessment,
    )

    evidence = TorrentCapacityEvidence(
        hash="planned",
        managed=True,
        incomplete=True,
        amount_left=1 * GIB,
        completed_bytes=1,
        availability=1.0,
        complete_sources=1,
        no_progress_since=None,
        viable=True,
        viability_reason="complete_source",
    )
    assessment = CapacityAssessment(
        generation=7,
        observed_at=50,
        scheduler_mode="drain",
        free_bytes=int(3.2 * GIB),
        target_free_bytes=5 * GIB,
        available_growth_bytes=100,
        selected_hashes=frozenset(),
        disk_releasing_jobs=0,
        torrents={"planned": evidence},
    )

    observation = build_capacity_observation_from_assessment(
        assessment,
        available_growth_bytes=int(1.45 * GIB),
        selected_hashes={"planned"},
    )

    assert observation.viable_finish == 1
    assert observation.feasible_full_finish == 1
    assert observation.available_growth_bytes == int(1.45 * GIB)


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


def test_capacity_state_persists_assessment_generation(tmp_path):
    from qbt_orchestrator.capacity_state import CapacityResult, CapacityStateStore
    from qbt_orchestrator.db import migrate

    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    store = CapacityStateStore(db, now=lambda: 50)

    transition = store.persist(
        "drain",
        CapacityResult("capacity_deadlock", "none"),
        {},
        assessment_generation=9,
    )

    assert transition.assessment_generation == 9
    con = sqlite3.connect(db)
    try:
        row = con.execute(
            "select assessment_generation from capacity_state where id=1"
        ).fetchone()
    finally:
        con.close()
    assert row == (9,)


def test_capacity_state_same_second_reentry_gets_a_new_episode_identity(tmp_path):
    from qbt_orchestrator.capacity_state import CapacityResult, CapacityStateStore
    from qbt_orchestrator.db import migrate

    db = tmp_path / "same-second.sqlite"
    migrate(db, dry_run=False)
    store = CapacityStateStore(db, now=lambda: 100)

    first = store.persist("drain", CapacityResult("capacity_deadlock", "none"))
    recovered = store.persist("normal", CapacityResult("progress_possible", "work"))
    reentered = store.persist("drain", CapacityResult("capacity_deadlock", "none"))

    assert first.entered_at == 100
    assert recovered.entered_at == 101
    assert reentered.entered_at == 102


@pytest.fixture
def alert_fixture(tmp_path):
    from qbt_orchestrator.alerts import (
        CapacityReclaimAlertContext,
        SchedulerAlertConfig,
        SchedulerAlertService,
    )
    from qbt_orchestrator.capacity_state import CapacityTransition
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.runtime import BotNotificationRepository

    db = tmp_path / "alerts.sqlite"
    migrate(db, dry_run=False)
    repo = BotNotificationRepository(db, now=lambda: 100)
    service = SchedulerAlertService(
        repo,
        SchedulerAlertConfig(
            enabled=True,
            chat_ids=["123"],
            capacity_deadlock_enabled=True,
        )
    )

    def transition(**overrides):
        values = {
            "scheduler_mode": "drain",
            "state": "capacity_deadlock",
            "reason": "no_finishable_or_releasing_work",
            "entered_at": 100,
            "last_evaluated_at": 100,
            "details": {
                "managed_incomplete": 10,
                "feasible_full_finish": 0,
                "disk_releasing_jobs": 0,
            },
            "transitioned": True,
            "previous_state": "progress_possible",
            "assessment_generation": 8,
        }
        values.update(overrides)
        return CapacityTransition(**values)

    def context(**overrides):
        values = {
            "evaluation_status": "live_evaluated",
            "dry_run": False,
            "planned": 0,
            "reclaimed": 0,
            "errors_count": 0,
            "errors_summary": (),
            "rejection_counts": {
                "availability_unknown": 3,
                "protected_tag": 1,
            },
            "rejection_fingerprint": "availability_unknown:3|protected_tag:1",
            "assessment_generation": 8,
            "capacity_pressure_remaining": True,
            "post_reclaim_free_bytes": None,
        }
        values.update(overrides)
        return CapacityReclaimAlertContext(**values)

    return SimpleNamespace(
        db=db,
        repo=repo,
        service=service,
        transition=transition,
        context=context,
        notifications=repo.list_all,
    )


@pytest.mark.parametrize(
    "context",
    [
        pytest.param(
            {"evaluation_status": "not_evaluated"},
            id="not-evaluated",
        ),
        pytest.param(
            {
                "evaluation_status": "dry_run",
                "dry_run": True,
                "planned": 1,
            },
            id="dry-run",
        ),
        pytest.param(
            {
                "evaluation_status": "live_evaluated",
                "reclaimed": 1,
                "capacity_pressure_remaining": False,
                "post_reclaim_free_bytes": 6 * GIB,
            },
            id="pressure-relieved",
        ),
    ],
)
def test_capacity_deadlock_does_not_alert_without_live_evaluation(alert_fixture, context):
    assert alert_fixture.service.enqueue_capacity_deadlock(
        alert_fixture.transition(),
        required_minimum_growth_bytes=1000,
        top_manual_candidates=[],
        reclaim_context=alert_fixture.context(**context),
    ) == []
    assert alert_fixture.notifications() == []


def test_live_capacity_evaluation_without_successful_reclaim_uses_manual_message_and_dedupes(alert_fixture):
    transition = alert_fixture.transition()
    context = alert_fixture.context(planned=1, reclaimed=0)

    first = alert_fixture.service.enqueue_capacity_deadlock(
        transition,
        required_minimum_growth_bytes=1000,
        top_manual_candidates=[],
        reclaim_context=context,
    )
    second = alert_fixture.service.enqueue_capacity_deadlock(
        transition,
        required_minimum_growth_bytes=1000,
        top_manual_candidates=[],
        reclaim_context=alert_fixture.context(
            planned=1,
            reclaimed=0,
            rejection_counts={
                "protected_tag": 1,
                "availability_unknown": 3,
            },
            rejection_fingerprint="protected_tag:1|availability_unknown:3",
        ),
    )

    assert len(first) == 1
    assert second == []
    row = alert_fixture.notifications()[0]
    dedupe_state = (
        "availability_unknown:3|protected_tag:1|"
        "message_state:manual|evaluation_status:live_evaluated"
    )
    digest = hashlib.sha256(
        dedupe_state.encode("utf-8")
    ).hexdigest()[:16]
    assert row["dedupe_key"] == f"scheduler-alert:capacity-deadlock:123:100:{digest}"
    assert row["message"] == "可用空间不足，当前没有能够安全回收的任务，需要人工处理。"
    payload = json.loads(row["payload_json"])
    assert payload["entered_at"] == 100
    assert payload["evaluation_status"] == "live_evaluated"
    assert payload["message_state"] == "manual"
    assert payload["dry_run"] is False
    assert payload["planned"] == 1
    assert payload["reclaimed"] == 0
    assert payload["errors_count"] == 0
    assert payload["errors_summary"] == []
    assert payload["rejection_counts"] == {
        "availability_unknown": 3,
        "protected_tag": 1,
    }
    assert payload["assessment_generation"] == 8
    assert "capacity_deadlock" not in row["message"]


def test_live_successful_reclaim_uses_reclaiming_message_and_state_change_is_not_deduped(alert_fixture):
    transition = alert_fixture.transition()

    manual = alert_fixture.service.enqueue_capacity_deadlock(
        transition,
        required_minimum_growth_bytes=2 * GIB,
        top_manual_candidates=[],
        reclaim_context=alert_fixture.context(planned=1, reclaimed=0),
    )
    reclaiming = alert_fixture.service.enqueue_capacity_deadlock(
        alert_fixture.transition(transitioned=False),
        required_minimum_growth_bytes=2 * GIB,
        top_manual_candidates=[],
        reclaim_context=alert_fixture.context(planned=1, reclaimed=1),
    )
    repeated = alert_fixture.service.enqueue_capacity_deadlock(
        alert_fixture.transition(transitioned=False),
        required_minimum_growth_bytes=2 * GIB,
        top_manual_candidates=[],
        reclaim_context=alert_fixture.context(planned=1, reclaimed=1),
    )

    assert len(manual) == 1
    assert len(reclaiming) == 1
    assert repeated == []
    rows = alert_fixture.notifications()
    assert len(rows) == 2
    assert rows[0]["message"] == "可用空间不足，当前没有能够安全回收的任务，需要人工处理。"
    assert rows[1]["message"] == "可用空间不足，已暂停启动新任务；系统正在安全释放无效文件。"
    assert rows[0]["dedupe_key"] != rows[1]["dedupe_key"]
    assert json.loads(rows[1]["payload_json"])["message_state"] == "reclaiming"


def test_live_reclaiming_to_manual_and_rejection_changes_create_new_notifications(alert_fixture):
    transition = alert_fixture.transition()

    reclaiming = alert_fixture.service.enqueue_capacity_deadlock(
        transition,
        required_minimum_growth_bytes=0,
        top_manual_candidates=[],
        reclaim_context=alert_fixture.context(planned=1, reclaimed=1),
    )
    manual = alert_fixture.service.enqueue_capacity_deadlock(
        alert_fixture.transition(transitioned=False),
        required_minimum_growth_bytes=0,
        top_manual_candidates=[],
        reclaim_context=alert_fixture.context(planned=1, reclaimed=0),
    )
    changed = alert_fixture.service.enqueue_capacity_deadlock(
        alert_fixture.transition(transitioned=False),
        required_minimum_growth_bytes=0,
        top_manual_candidates=[],
        reclaim_context=alert_fixture.context(
            planned=1,
            reclaimed=0,
            rejection_counts={"protected_tag": 2},
            rejection_fingerprint="protected_tag:2",
        ),
    )
    repeated = alert_fixture.service.enqueue_capacity_deadlock(
        alert_fixture.transition(transitioned=False),
        required_minimum_growth_bytes=0,
        top_manual_candidates=[],
        reclaim_context=alert_fixture.context(
            planned=1,
            reclaimed=0,
            rejection_counts={"protected_tag": 2},
            rejection_fingerprint="protected_tag:2",
        ),
    )

    assert all(len(result) == 1 for result in (reclaiming, manual, changed))
    assert repeated == []
    assert len(alert_fixture.notifications()) == 3


def test_capacity_alert_payload_bounds_and_redacts_error_summary(alert_fixture):
    errors = tuple(["Bearer abcdef"] + ["x" * 500] * 5)

    assert len(
        alert_fixture.service.enqueue_capacity_deadlock(
            alert_fixture.transition(),
            required_minimum_growth_bytes=0,
            top_manual_candidates=[],
            reclaim_context=alert_fixture.context(
                errors_count=len(errors),
                errors_summary=errors,
            ),
        )
    ) == 1
    payload = json.loads(alert_fixture.notifications()[0]["payload_json"])
    assert payload["errors_count"] == 6
    assert len(payload["errors_summary"]) == 3
    assert all(len(item) <= 200 for item in payload["errors_summary"])
    assert payload["errors_summary"][0] == "Bearer <redacted>"


def test_concurrent_capacity_alert_enqueue_returns_only_the_inserted_notification(alert_fixture):
    def enqueue(_index):
        return alert_fixture.service.enqueue_capacity_deadlock(
            alert_fixture.transition(),
            required_minimum_growth_bytes=0,
            top_manual_candidates=[],
            reclaim_context=alert_fixture.context(),
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(enqueue, range(2)))

    assert sorted(len(result) for result in results) == [0, 1]
    assert len(alert_fixture.notifications()) == 1


def test_capacity_deadlock_dedupe_is_per_episode_and_chat(tmp_path):
    from qbt_orchestrator.alerts import SchedulerAlertConfig, SchedulerAlertService
    from qbt_orchestrator.capacity_state import CapacityTransition
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.runtime import BotNotificationRepository

    db = tmp_path / "multi-chat.sqlite"
    migrate(db, dry_run=False)
    repo = BotNotificationRepository(db, now=lambda: 100)
    service = SchedulerAlertService(
        repo,
        SchedulerAlertConfig(
            enabled=True,
            chat_ids=["123", "456"],
            capacity_deadlock_enabled=True,
        ),
    )

    def transition(entered_at):
        return CapacityTransition(
            scheduler_mode="drain",
            state="capacity_deadlock",
            reason="no_finishable_or_releasing_work",
            entered_at=entered_at,
            last_evaluated_at=100,
            details={},
            transitioned=True,
            previous_state="progress_possible",
            assessment_generation=8,
        )

    kwargs = {
        "required_minimum_growth_bytes": 0,
        "top_manual_candidates": [],
        "reclaim_context": None,
    }
    from qbt_orchestrator.alerts import CapacityReclaimAlertContext

    kwargs["reclaim_context"] = CapacityReclaimAlertContext(
        evaluation_status="live_evaluated",
        dry_run=False,
        planned=1,
        reclaimed=0,
        errors_count=0,
        errors_summary=(),
        rejection_counts={},
        rejection_fingerprint="",
        assessment_generation=8,
        capacity_pressure_remaining=True,
        post_reclaim_free_bytes=None,
    )
    assert len(service.enqueue_capacity_deadlock(transition(100), **kwargs)) == 2
    assert service.enqueue_capacity_deadlock(transition(100), **kwargs) == []
    assert len(service.enqueue_capacity_deadlock(transition(101), **kwargs)) == 2
    rows = repo.list_all()
    assert len(rows) == 4
    assert {(row["chat_id"], row["created_at"]) for row in rows} == {
        ("123", 100),
        ("456", 100),
    }
    assert len({row["dedupe_key"] for row in rows}) == 4


def test_capacity_alert_ignores_recovered_state():
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
            CapacityTransition(state="progress_possible", transitioned=True, **common),
            required_minimum_growth_bytes=0,
            top_manual_candidates=[],
        ) == []


def test_daemon_persists_deadlock_without_actions_and_does_not_alert_without_reclaim_evaluation():
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
        assert notices == []


@pytest.mark.parametrize(
    ("payload", "expected_status", "expected_planned", "expected_reclaimed"),
    [
        (None, "not_evaluated", 0, 0),
        (
            {"dry_run": True, "planned": 1, "reclaimed": 0},
            "dry_run",
            1,
            0,
        ),
        (
            {
                "dry_run": False,
                "planned": 2,
                "reclaimed": 1,
                "rejection_counts": {
                    "protected_tag": 1,
                    "availability_unknown": 3,
                },
                "errors": ["first", "second"],
            },
            "live_evaluated",
            2,
            1,
        ),
    ],
)
def test_capacity_deadlock_alert_context_is_structured(
    payload, expected_status, expected_planned, expected_reclaimed
):
    from qbt_orchestrator.service import _capacity_deadlock_alert_context

    context = _capacity_deadlock_alert_context(
        payload,
        assessment_generation=9,
        post_reclaim_free_bytes=4 * GIB if expected_reclaimed else None,
        target_free_bytes=5 * GIB,
    )

    assert context.evaluation_status == expected_status
    assert context.planned == expected_planned
    assert context.reclaimed == expected_reclaimed
    assert context.assessment_generation == 9
    if expected_status == "live_evaluated":
        assert context.rejection_fingerprint == (
            "availability_unknown:3|protected_tag:1"
        )
        assert context.errors_count == 2
        assert context.errors_summary == ("first", "second")
        assert context.capacity_pressure_remaining is True


@pytest.mark.parametrize(
    (
        "planned",
        "reclaimed",
        "reclaimed_bytes",
        "errors",
        "post_reclaim_free_bytes",
        "expected_capacity_state",
        "expected_alerts",
        "expected_message_state",
    ),
    [
        (2, 0, 0, [], int(3.25 * GIB), "capacity_deadlock", 1, "manual"),
        (
            2,
            0,
            128 * 1024**2,
            ["recheck unavailable"],
            6 * GIB,
            "progress_possible",
            0,
            "manual",
        ),
        (0, 0, 0, [], 6 * GIB, "progress_possible", 0, "manual"),
        (2, 1, 128 * 1024**2, [], int(3.25 * GIB), "capacity_deadlock", 1, "reclaiming"),
        (2, 1, 128 * 1024**2, [], 6 * GIB, "progress_possible", 0, "reclaiming"),
    ],
)
def test_daemon_rechecks_pressure_after_live_reclaim(
    planned,
    reclaimed,
    reclaimed_bytes,
    errors,
    post_reclaim_free_bytes,
    expected_capacity_state,
    expected_alerts,
    expected_message_state,
):
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
        # Probe already failed and is backing off, so capacity state can enter
        # deadlock instead of availability_probe_pending.
        con.execute(
            "insert into carousel_state(hash,state,last_probe_at,backoff_until,backoff_level,updated_at) "
            "values('stuck','dead',?,?,1,?)",
            (now - 60, now + 3_600, now),
        )
        con.commit()
        con.close()
        free = [int(3.25 * GIB)]

        class RecordingReclaimer:
            def __init__(self):
                self.calls = []

            def run(self, snapshots, **kwargs):
                self.calls.append((snapshots, kwargs))
                free[0] = post_reclaim_free_bytes

                class Result:
                    def as_dict(self):
                        return {
                            "dry_run": False,
                            "planned": planned,
                            "reclaimed": reclaimed,
                            "reclaimed_bytes": reclaimed_bytes,
                            "candidates": [{"hash": "dead"}],
                            "rejection_counts": {
                                "protected_tag": 1,
                                "availability_unknown": 3,
                            },
                            "errors": errors,
                            "assessment_generation": 1,
                        }

                return Result()

        class RecordingAlerts:
            def __init__(self, delegate):
                self.delegate = delegate
                self.deadlocks = []

            def evaluate_and_enqueue(self, **kwargs):
                return self.delegate.evaluate_and_enqueue(**kwargs)

            def enqueue_capacity_deadlock(self, transition, **kwargs):
                self.deadlocks.append((transition, kwargs))
                return self.delegate.enqueue_capacity_deadlock(transition, **kwargs)

        reclaimer = RecordingReclaimer()
        daemon = DaemonRuntime(
            state_db=db,
            qbt=StuckFinishQbt(),
            executor=FakeExecutor(),
            free_bytes_provider=lambda: free[0],
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
            scheduler_alert_chat_ids=["123"],
            scheduler_alerts_enabled=True,
            capacity_deadlock_alerts_enabled=True,
        )
        alerts = RecordingAlerts(daemon.scheduler_alert_service)
        daemon.scheduler_alert_service = alerts

        daemon.tick_safety()
        result = daemon.planner_tick()

        assert result["capacity"]["scheduler_mode"] == "normal"
        assert result["capacity"]["state"] == expected_capacity_state
        assert result["capacity"]["details"]["feasible_full_finish"] == 0
        assert result["capacity"]["details"]["nonviable_finish"] == 1
        assert result["planner"]["selected_hashes"] == []
        assert result["capacity_reclaim"]["planned"] == planned
        reclaim_kwargs = reclaimer.calls[0][1]
        assert reclaim_kwargs["capacity_state"] == "capacity_deadlock"
        assert reclaim_kwargs["free_bytes"] == int(3.25 * GIB)
        assert reclaim_kwargs["target_free_bytes"] == 5 * GIB
        # Formal reclaimers always receive the committed assessment; incomplete
        # signature probing was removed in the P0 simplify pass.
        assert "assessment" in reclaim_kwargs
        assert int(reclaim_kwargs["assessment"].generation) == 1
        assert len(alerts.deadlocks) == 1
        transition, alert_kwargs = alerts.deadlocks[0]
        assert transition.state == expected_capacity_state
        context = alert_kwargs["reclaim_context"]
        assert context.evaluation_status == "live_evaluated"
        assert context.dry_run is False
        assert context.planned == planned
        assert context.reclaimed == reclaimed
        assert context.errors_count == len(errors)
        assert context.rejection_fingerprint == (
            "availability_unknown:3|protected_tag:1"
        )
        assert context.capacity_pressure_remaining is (expected_alerts == 1)
        assert result["capacity"]["assessment_generation"] == 1
        assert result["capacity_reclaim"]["assessment_generation"] == 1
        con = sqlite3.connect(db)
        try:
            capacity_alert_count = con.execute(
                "select count(*) from bot_notifications where topic='capacity_deadlock'"
            ).fetchone()[0]
            capacity_alert_row = con.execute(
                "select payload_json from bot_notifications "
                "where topic='capacity_deadlock'"
            ).fetchone()
        finally:
            con.close()
        assert capacity_alert_count == expected_alerts
        if expected_alerts:
            assert capacity_alert_row is not None
            payload = json.loads(capacity_alert_row[0])
            assert payload["message_state"] == expected_message_state


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
