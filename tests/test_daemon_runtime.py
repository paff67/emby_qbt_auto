#!/usr/bin/env python3
from __future__ import annotations

import json
import sqlite3
import tempfile
import threading
import time
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))


class FakeQbt:
    def __init__(self):
        self.rids = []

    def get_maindata(self, rid):
        self.rids.append(rid)
        return {"rid": rid + 1, "full_update": True, "torrents": {}, "server_state": {}}


class FakeExecutor:
    def __init__(self):
        self.posts = []

    def qbt_post(self, path, payload):
        self.posts.append((path, payload))


@pytest.fixture
def runtime_fixture(tmp_path, monkeypatch):
    from qbt_orchestrator.capacity_assessment import (
        CapacityAssessmentBuilder,
        CapacityAssessmentStore,
    )
    from qbt_orchestrator.capacity_reclaim import CapacityReclaimResult
    from qbt_orchestrator import service

    snapshots = {
        "viable": {
            "hash": "viable",
            "name": "Viable",
            "category": "auto",
            "tags": "auto",
            "state": "stoppedDL",
            "amount_left": 100,
            "size": 200,
            "progress": 0.5,
            "num_seeds": 1,
        },
        "dead": {
            "hash": "dead",
            "name": "Dead",
            "category": "auto",
            "tags": "auto",
            "state": "stoppedDL",
            "amount_left": 200,
            "size": 300,
            "progress": 0.25,
            "availability": 0.5,
            "num_seeds": 0,
        },
    }

    class SnapshotQbt(FakeQbt):
        def get_maindata(self, rid):
            self.rids.append(rid)
            return {
                "rid": rid + 1,
                "full_update": True,
                "torrents": snapshots,
                "server_state": {},
            }

    class RecordingBuilder(CapacityAssessmentBuilder):
        def __init__(self):
            super().__init__(viability_stale_sec=1_800)
            self.built = []

        def build(self, *args, **kwargs):
            assessment = super().build(*args, **kwargs)
            self.built.append(assessment)
            return assessment

    class RecordingStore(CapacityAssessmentStore):
        def __init__(self, state_db):
            super().__init__(state_db, min_no_progress_sec=21_600)
            self.committed = []

        def commit(self, assessment):
            committed = super().commit(assessment)
            self.committed.append((assessment, committed))
            return committed

    class RecordingReclaimer:
        dry_run = True

        def __init__(self):
            self.calls = []

        def run(
            self,
            snapshots,
            *,
            assessment=None,
            capacity_state,
            free_bytes,
            target_free_bytes,
        ):
            self.calls.append(
                {
                    "snapshots": snapshots,
                    "assessment": assessment,
                    "capacity_state": capacity_state,
                    "free_bytes": free_bytes,
                    "target_free_bytes": target_free_bytes,
                }
            )
            return CapacityReclaimResult(
                dry_run=True,
                assessment_generation=(
                    0 if assessment is None else assessment.generation
                ),
            )

    original_planner = service.DownloadPlanner
    fixture = type("RuntimeFixture", (), {})()
    fixture.planner_assessment = None

    class RecordingPlanner(original_planner):
        def plan_and_apply(self, *args, capacity_assessment=None, **kwargs):
            fixture.planner_assessment = capacity_assessment
            return super().plan_and_apply(
                *args,
                capacity_assessment=capacity_assessment,
                **kwargs,
            )

    monkeypatch.setattr(service, "DownloadPlanner", RecordingPlanner)
    fixture.db = tmp_path / "state.sqlite"
    fixture.builder = RecordingBuilder()
    fixture.store = RecordingStore(fixture.db)

    def build_with_recording_reclaimer():
        fixture.reclaimer = RecordingReclaimer()
        runtime = service.DaemonRuntime(
            state_db=fixture.db,
            qbt=SnapshotQbt(),
            executor=FakeExecutor(),
            free_bytes_provider=lambda: 6 * 1024**3,
            dry_run=False,
            planner_dry_run=False,
            safety_interval=0,
            capacity_assessment_builder=fixture.builder,
            capacity_assessment_store=fixture.store,
            capacity_reclaimer=fixture.reclaimer,
            scheduler_engine_mode="shadow",
        )
        runtime.tick_safety()
        return runtime

    fixture.build_with_recording_reclaimer = build_with_recording_reclaimer
    return fixture


def test_planner_capacity_and_reclaimer_share_generation(runtime_fixture):
    runtime = runtime_fixture.build_with_recording_reclaimer()

    payload = runtime.planner_tick()

    generation = payload["capacity"]["assessment_generation"]
    assert generation > 0
    assert runtime_fixture.planner_assessment.generation == generation
    reclaim_call = runtime_fixture.reclaimer.calls[0]
    assert reclaim_call["assessment"] is runtime_fixture.store.committed[0][1]
    assert reclaim_call["assessment"] is runtime_fixture.planner_assessment
    assert reclaim_call["assessment"].generation == generation
    assert reclaim_call["capacity_state"] == payload["capacity"]["state"]
    assert reclaim_call["free_bytes"] == reclaim_call["assessment"].free_bytes
    assert (
        reclaim_call["target_free_bytes"]
        == reclaim_call["assessment"].target_free_bytes
    )
    assert payload["capacity_reclaim"]["assessment_generation"] == generation
    assert payload["scheduler_engine"]["assessment_generation"] == generation
    assert len(runtime_fixture.builder.built) == 1
    assert len(runtime_fixture.store.committed) == 1

    con = sqlite3.connect(runtime_fixture.db)
    assessment_generation = con.execute(
        "select current_generation from capacity_assessment_state where id=1"
    ).fetchone()[0]
    capacity_generation = con.execute(
        "select assessment_generation from capacity_state where id=1"
    ).fetchone()[0]
    metric = con.execute(
        "select metrics_json from metrics_snapshots "
        "where component='scheduler_engine_shadow' order by id desc limit 1"
    ).fetchone()[0]
    con.close()
    assert assessment_generation == capacity_generation == generation
    assert json.loads(metric)["assessment_generation"] == generation


def test_scheduler_engine_canonicalizes_assessment_hash_lookup(
    tmp_path,
    monkeypatch,
):
    from qbt_orchestrator import service

    class MixedCaseQbt(FakeQbt):
        def get_maindata(self, rid):
            self.rids.append(rid)
            return {
                "rid": rid + 1,
                "full_update": True,
                "torrents": {
                    " ABCDEF ": {
                        "name": "mixed",
                        "category": "auto",
                        "tags": "auto",
                        "state": "stoppedDL",
                        "amount_left": 1,
                        "size": 2,
                        "progress": 0.5,
                        "num_seeds": 1,
                    }
                },
                "server_state": {},
            }

    original_planner = service.DownloadPlanner
    planner_assessments = []

    class RecordingPlanner(original_planner):
        def plan_and_apply(self, *args, capacity_assessment=None, **kwargs):
            planner_assessments.append(capacity_assessment)
            return super().plan_and_apply(
                *args,
                capacity_assessment=capacity_assessment,
                **kwargs,
            )

    monkeypatch.setattr(service, "DownloadPlanner", RecordingPlanner)
    daemon = service.DaemonRuntime(
        state_db=tmp_path / "state.sqlite",
        qbt=MixedCaseQbt(),
        executor=FakeExecutor(),
        free_bytes_provider=lambda: 6 * 1024**3,
        dry_run=True,
        scheduler_engine_mode="shadow",
    )
    daemon.tick_safety()

    payload = daemon.planner_tick()

    generation = payload["capacity"]["assessment_generation"]
    assert planner_assessments[0].generation == generation
    assert planner_assessments[0].torrents["abcdef"].viable is True
    assert payload["scheduler_engine"]["assessment_generation"] == generation


def test_capacity_state_uses_actual_planner_budget_and_selected_hashes(
    tmp_path,
    monkeypatch,
):
    from qbt_orchestrator import service
    from qbt_orchestrator.planner import PlannerResult

    gib = 1024**3

    class OneTorrentQbt(FakeQbt):
        def get_maindata(self, rid):
            self.rids.append(rid)
            return {
                "rid": rid + 1,
                "full_update": True,
                "torrents": {
                    "planned": {
                        "hash": "planned",
                        "name": "planned",
                        "category": "auto",
                        "tags": "auto",
                        "state": "stoppedDL",
                        "amount_left": gib,
                        "size": 2 * gib,
                        "progress": 0.5,
                        "num_seeds": 1,
                    }
                },
                "server_state": {},
            }

    class PlannedDownload:
        def __init__(self, *_args, **_kwargs):
            pass

        def plan_and_apply(self, *_args, **_kwargs):
            return PlannerResult(
                selected_hashes=["planned"],
                paused_hashes=[],
                budget_bytes=int(1.45 * gib),
                mode="recovery",
            )

    monkeypatch.setattr(service, "DownloadPlanner", PlannedDownload)
    daemon = service.DaemonRuntime(
        state_db=tmp_path / "state.sqlite",
        qbt=OneTorrentQbt(),
        executor=FakeExecutor(),
        free_bytes_provider=lambda: int(3.2 * gib),
        dry_run=True,
    )
    daemon.tick_safety()

    payload = daemon.planner_tick()

    assert payload["capacity"]["state"] == "progress_possible"
    assert payload["capacity"]["details"]["feasible_full_finish"] == 1
    assert payload["capacity"]["details"]["available_growth_bytes"] == int(
        1.45 * gib
    )
    assert payload["capacity"]["details"]["assessment_incumbent_count"] == 0
    assert payload["capacity"]["details"]["planned_selected_count"] == 1


def test_assessment_and_planner_share_current_progress_health(
    tmp_path,
    monkeypatch,
):
    from qbt_orchestrator import service

    class ProgressQbt(FakeQbt):
        def get_maindata(self, rid):
            self.rids.append(rid)
            return {
                "rid": rid + 1,
                "full_update": True,
                "torrents": {
                    " H ": {
                        "hash": " H ",
                        "name": "progressed",
                        "category": "auto",
                        "tags": "auto",
                        "state": "stoppedDL",
                        "amount_left": 100,
                        "size": 200,
                        "completed": 100,
                        "progress": 0.5,
                        "availability": 0.5,
                        "num_seeds": 0,
                        "dlspeed": 0,
                    }
                },
                "server_state": {},
            }

    monkeypatch.setattr(service.time, "time", lambda: 2_000)
    db = tmp_path / "state.sqlite"
    daemon = service.DaemonRuntime(
        state_db=db,
        qbt=ProgressQbt(),
        executor=FakeExecutor(),
        free_bytes_provider=lambda: 10 * 1024**3,
        dry_run=False,
        planner_dry_run=False,
        planner_active_slots=1,
    )
    con = sqlite3.connect(db)
    con.execute(
        "insert into torrent_health("
        "hash,sampled_at,completed_bytes,progress,no_progress_since,"
        "reclaimable_since,capacity_generation,updated_at) "
        "values('h',1,0,0.4,1,1,0,1)"
    )
    con.commit()
    con.close()
    daemon.tick_safety()

    payload = daemon.planner_tick()

    assert payload["selected"] == ["h"]
    con = sqlite3.connect(db)
    row = con.execute(
        "select completed_bytes,last_completed_bytes,progress,no_progress_since,"
        "reclaimable_since,capacity_generation,capacity_viable,capacity_reason "
        "from torrent_health where hash='h'"
    ).fetchone()
    con.close()
    assert row == (100, 0, 0.5, None, None, 1, 1, "recent_progress")


def test_first_planner_tick_samples_sync_and_commits_empty_unhealthy_generation(
    tmp_path,
    monkeypatch,
):
    from qbt_orchestrator import service

    class OfflineQbt(FakeQbt):
        def get_maindata(self, rid):
            self.rids.append(rid)
            raise RuntimeError("offline")

    class ForbiddenPlanner:
        def __init__(self, *_args, **_kwargs):
            raise AssertionError("unhealthy sync must not instantiate Planner")

    class ForbiddenEngine:
        def select(self, *_args, **_kwargs):
            raise AssertionError("unhealthy sync must not run scheduler engine")

    class ForbiddenReclaimer:
        def run(
            self,
            snapshots,
            *,
            assessment=None,
            capacity_state,
            free_bytes,
            target_free_bytes,
        ):
            raise AssertionError("unhealthy sync must not run live reclaimer")

    monkeypatch.setattr(service, "DownloadPlanner", ForbiddenPlanner)
    qbt = OfflineQbt()
    daemon = service.DaemonRuntime(
        state_db=tmp_path / "state.sqlite",
        qbt=qbt,
        executor=FakeExecutor(),
        free_bytes_provider=lambda: 2 * 1024**3,
        dry_run=False,
        planner_dry_run=False,
        capacity_reclaimer=ForbiddenReclaimer(),
        scheduler_engine=ForbiddenEngine(),
        scheduler_engine_mode="live",
    )

    payload = daemon.planner_tick()

    assert qbt.rids == [0]
    assert payload["capacity"]["state"] == "sync_unhealthy"
    assert payload["capacity"]["details"]["sync_healthy"] is False
    assert payload["selected"] == []
    assert payload["capacity_reclaim"] is None
    con = sqlite3.connect(daemon.state_db)
    generation, summary_json = con.execute(
        "select current_generation,summary_json from capacity_assessment_state where id=1"
    ).fetchone()
    metrics = con.execute(
        "select metrics_json from metrics_snapshots "
        "where component='capacity_assessment' order by id"
    ).fetchall()
    health_count = con.execute("select count(*) from torrent_health").fetchone()[0]
    con.close()
    assert generation == 1
    assert json.loads(summary_json)["torrent_count"] == 0
    assert health_count == 0
    assert len(metrics) == 1
    assert json.loads(metrics[0][0])["sync_healthy"] is False


def test_planner_safety_capture_is_atomic_while_next_sync_is_in_flight(tmp_path):
    from qbt_orchestrator.service import DaemonRuntime

    poll_started = threading.Event()
    release_poll = threading.Event()

    class OfflineThenBarrierQbt(FakeQbt):
        def get_maindata(self, rid):
            self.rids.append(rid)
            if len(self.rids) == 1:
                raise RuntimeError("initially offline")
            poll_started.set()
            assert release_poll.wait(2)
            return {
                "rid": rid + 1,
                "full_update": True,
                "torrents": {
                    "new": {
                        "hash": "new",
                        "category": "auto",
                        "tags": "auto",
                        "state": "stoppedDL",
                        "amount_left": 1,
                    }
                },
                "server_state": {},
            }

    daemon = DaemonRuntime(
        state_db=tmp_path / "state.sqlite",
        qbt=OfflineThenBarrierQbt(),
        executor=FakeExecutor(),
        free_bytes_provider=lambda: 6 * 1024**3,
        dry_run=True,
    )
    daemon.tick_safety()
    safety_thread = threading.Thread(target=daemon.tick_safety)
    safety_thread.start()
    assert poll_started.wait(1)

    captured = []
    capture_done = threading.Event()

    def capture_for_planner():
        captured.append(daemon._capture_safety_snapshot())
        capture_done.set()

    capture_thread = threading.Thread(target=capture_for_planner)
    capture_thread.start()
    assert capture_done.wait(0.5), "qBT network poll held the safety snapshot lock"
    old_snapshots, old_healthy, old_sampled = captured[0]
    assert old_snapshots == {}
    assert old_healthy is False
    assert old_sampled is True

    release_poll.set()
    safety_thread.join(timeout=2)
    capture_thread.join(timeout=2)
    assert not safety_thread.is_alive()
    new_snapshots, new_healthy, new_sampled = daemon._capture_safety_snapshot()
    assert set(new_snapshots) == {"new"}
    assert new_healthy is True
    assert new_sampled is True


def test_concurrent_safety_ticks_serialize_rid_and_publish_in_poll_order(tmp_path):
    from qbt_orchestrator.service import DaemonRuntime

    slow_poll_started = threading.Event()
    release_slow_poll = threading.Event()
    second_poll_started = threading.Event()

    class OrderedBarrierQbt(FakeQbt):
        def get_maindata(self, rid):
            self.rids.append(rid)
            call = len(self.rids)
            if call == 1:
                return {
                    "rid": 1,
                    "full_update": True,
                    "torrents": {
                        "baseline": {
                            "hash": "baseline",
                            "category": "auto",
                            "state": "stoppedDL",
                        }
                    },
                    "server_state": {},
                }
            if call == 2:
                slow_poll_started.set()
                assert release_slow_poll.wait(2)
                return {
                    "rid": 2,
                    "full_update": True,
                    "torrents": {
                        "old": {
                            "hash": "old",
                            "category": "auto",
                            "state": "stoppedDL",
                        }
                    },
                    "server_state": {},
                }
            second_poll_started.set()
            return {
                "rid": 3,
                "full_update": True,
                "torrents": {
                    "new": {
                        "hash": "new",
                        "category": "auto",
                        "state": "stoppedDL",
                    }
                },
                "server_state": {},
            }

    qbt = OrderedBarrierQbt()
    daemon = DaemonRuntime(
        state_db=tmp_path / "state.sqlite",
        qbt=qbt,
        executor=FakeExecutor(),
        free_bytes_provider=lambda: 6 * 1024**3,
        dry_run=True,
    )
    daemon.tick_safety()
    first = threading.Thread(target=daemon.tick_safety)
    second = threading.Thread(target=daemon.tick_safety)
    first.start()
    assert slow_poll_started.wait(1)
    second.start()

    captured = []
    capture_done = threading.Event()

    def capture_for_planner():
        captured.append(daemon._capture_safety_snapshot())
        capture_done.set()

    capture = threading.Thread(target=capture_for_planner)
    capture.start()
    assert capture_done.wait(0.5), "slow poll blocked planner snapshot capture"
    snapshots, healthy, sampled = captured[0]
    assert set(snapshots) == {"baseline"}
    assert healthy is True
    assert sampled is True
    assert not second_poll_started.wait(0.1), "second sync poll was not serialized"

    release_slow_poll.set()
    first.join(timeout=2)
    second.join(timeout=2)
    capture.join(timeout=2)
    assert not first.is_alive()
    assert not second.is_alive()
    final_snapshots, final_healthy, final_sampled = daemon._capture_safety_snapshot()
    assert qbt.rids == [0, 1, 2]
    assert daemon.monitor.sync.rid == 3
    assert set(final_snapshots) == {"new"}
    assert final_healthy is True
    assert final_sampled is True


def test_direct_planner_tick_recovery_precedes_emergency_qbt_post(
    tmp_path,
    monkeypatch,
):
    from qbt_orchestrator import service
    from qbt_orchestrator.capacity_reclaim import CapacityReclaimResult
    from qbt_orchestrator.planner import PlannerResult

    order = []

    class ActiveQbt(FakeQbt):
        def get_maindata(self, rid):
            self.rids.append(rid)
            return {
                "rid": rid + 1,
                "full_update": True,
                "torrents": {
                    "active": {
                        "hash": "active",
                        "name": "active",
                        "category": "auto",
                        "tags": "auto",
                        "state": "downloading",
                        "amount_left": 100,
                        "size": 200,
                    }
                },
                "server_state": {},
            }

    class OrderedExecutor(FakeExecutor):
        def qbt_post(self, path, payload):
            order.append(("qbt_post", path))
            super().qbt_post(path, payload)

    class Recovery:
        def run(
            self,
            snapshots,
            *,
            assessment=None,
            capacity_state,
            free_bytes,
            target_free_bytes,
        ):
            assert snapshots == {}
            assert assessment is None
            assert capacity_state == "recovery_preflight"
            assert free_bytes == 1024**3
            order.append(("recovery_preflight", target_free_bytes))
            con = sqlite3.connect(db)
            con.execute(
                "update capacity_reclaims set state='aborted_paused' "
                "where state='stopping'"
            )
            con.commit()
            con.close()
            return CapacityReclaimResult(dry_run=False)

    class NoopPlanner:
        def __init__(self, *_args, **_kwargs):
            pass

        def plan_and_apply(self, *_args, **_kwargs):
            return PlannerResult([], [], budget_bytes=0, mode="emergency")

    monkeypatch.setattr(service, "DownloadPlanner", NoopPlanner)
    db = tmp_path / "state.sqlite"
    executor = OrderedExecutor()
    daemon = service.DaemonRuntime(
        state_db=db,
        qbt=ActiveQbt(),
        executor=executor,
        free_bytes_provider=lambda: 1024**3,
        dry_run=False,
        planner_dry_run=False,
        capacity_recovery_reclaimer=Recovery(),
    )
    con = sqlite3.connect(db)
    con.execute(
        "insert into capacity_reclaims("
        "reclaim_key,hash,name,magnet_uri,host_path,content_path,state,"
        "capacity_generation,created_at,updated_at) "
        "values('h:1','h','H','magnet:?xt=h',?,?,'stopping',1,1,1)",
        (str((tmp_path / "incomplete" / "h").resolve()), "/downloads/incomplete/h"),
    )
    con.commit()
    con.close()

    payload = daemon.planner_tick()

    assert payload["capacity"]["assessment_generation"] == 1
    assert order[:2] == [
        ("recovery_preflight", daemon.drain_exit_bytes),
        ("qbt_post", "/api/v2/torrents/stop"),
    ]


def test_direct_planner_tick_recovery_failure_precedes_safety(tmp_path):
    from qbt_orchestrator.service import DaemonRuntime

    class FailingRecovery:
        def run(self, *_args, **_kwargs):
            raise RuntimeError("direct recovery failed")

    qbt = FakeQbt()
    executor = FakeExecutor()
    daemon = DaemonRuntime(
        state_db=tmp_path / "state.sqlite",
        qbt=qbt,
        executor=executor,
        free_bytes_provider=lambda: 1024**3,
        dry_run=False,
        planner_dry_run=False,
        capacity_recovery_reclaimer=FailingRecovery(),
    )
    con = sqlite3.connect(daemon.state_db)
    con.execute(
        "insert into capacity_reclaims("
        "reclaim_key,hash,name,magnet_uri,host_path,content_path,state,"
        "capacity_generation,created_at,updated_at) "
        "values('h:1','h','H','magnet:?xt=h',?,?,'stopping',1,1,1)",
        (str((tmp_path / "incomplete" / "h").resolve()), "/downloads/incomplete/h"),
    )
    con.commit()
    con.close()

    with pytest.raises(RuntimeError, match="direct recovery failed"):
        daemon.planner_tick()

    assert qbt.rids == []
    assert executor.posts == []
    con = sqlite3.connect(daemon.state_db)
    assert con.execute("select count(*) from capacity_assessment_state").fetchone()[0] == 0
    con.close()


@pytest.mark.parametrize("scheduler_engine_mode", ["legacy", "shadow", "live"])
def test_capacity_assessment_metric_is_once_per_generation_in_all_scheduler_modes(
    tmp_path,
    scheduler_engine_mode,
):
    from qbt_orchestrator.service import DaemonRuntime

    daemon = DaemonRuntime(
        state_db=tmp_path / f"{scheduler_engine_mode}.sqlite",
        qbt=FakeQbt(),
        executor=FakeExecutor(),
        free_bytes_provider=lambda: 6 * 1024**3,
        dry_run=True,
        scheduler_engine_mode=scheduler_engine_mode,
    )
    daemon.tick_safety()

    first = daemon.planner_tick()
    second = daemon.planner_tick()

    con = sqlite3.connect(daemon.state_db)
    rows = con.execute(
        "select metrics_json from metrics_snapshots "
        "where component='capacity_assessment' order by id"
    ).fetchall()
    con.close()
    metrics = [json.loads(row[0]) for row in rows]
    assert [item["generation"] for item in metrics] == [
        first["capacity"]["assessment_generation"],
        second["capacity"]["assessment_generation"],
    ]
    assert set(metrics[0]) == {
        "generation",
        "managed_incomplete",
        "viable_finish",
        "nonviable_finish",
        "feasible_full_finish",
        "free_bytes",
        "target_free_bytes",
        "actual_budget_bytes",
        "planned_selected_count",
        "mature_reclaimable_count",
        "sync_healthy",
    }
    assert metrics[0]["sync_healthy"] is True


def test_unhealthy_generation_breaks_reclaimable_continuity_after_long_outage(
    tmp_path,
    monkeypatch,
):
    from qbt_orchestrator import service

    clock = [10_000]
    torrent = {
        "hash": "stale",
        "name": "stale",
        "category": "auto",
        "tags": "auto",
        "state": "stoppedDL",
        "amount_left": 100,
        "size": 200,
        "progress": 0.5,
        "availability": 0.5,
        "num_seeds": 0,
        "dlspeed": 0,
    }

    class FlakyQbt(FakeQbt):
        def __init__(self):
            super().__init__()
            self.offline = False

        def get_maindata(self, rid):
            self.rids.append(rid)
            if self.offline:
                raise RuntimeError("offline")
            return {
                "rid": rid + 1,
                "full_update": True,
                "torrents": {"stale": torrent},
                "server_state": {},
            }

    monkeypatch.setattr(service.time, "time", lambda: clock[0])
    qbt = FlakyQbt()
    daemon = service.DaemonRuntime(
        state_db=tmp_path / "state.sqlite",
        qbt=qbt,
        executor=FakeExecutor(),
        free_bytes_provider=lambda: 2 * 1024**3,
        dry_run=True,
        capacity_reclaim_min_no_progress_sec=0,
        capacity_reclaim_min_reclaimable_sec=3_600,
    )
    con = sqlite3.connect(daemon.state_db)
    con.execute(
        "insert into torrent_health("
        "hash,sampled_at,completed_bytes,progress,no_progress_since,updated_at) "
        "values('stale',0,0,0.5,0,0)"
    )
    con.commit()
    con.close()

    daemon.tick_safety()
    assert daemon.planner_tick()["capacity"]["assessment_generation"] == 1
    con = sqlite3.connect(daemon.state_db)
    assert con.execute(
        "select reclaimable_since,capacity_generation from torrent_health where hash='stale'"
    ).fetchone() == (10_000, 1)
    con.close()

    clock[0] += 3_600
    qbt.offline = True
    daemon.tick_safety()
    outage = daemon.planner_tick()
    assert outage["capacity"]["state"] == "sync_unhealthy"
    assert outage["capacity"]["assessment_generation"] == 2

    clock[0] += 3_600
    qbt.offline = False
    daemon.tick_safety()
    recovered = daemon.planner_tick()
    con = sqlite3.connect(daemon.state_db)
    reclaimable_since, generation = con.execute(
        "select reclaimable_since,capacity_generation from torrent_health where hash='stale'"
    ).fetchone()
    metric_rows = con.execute(
        "select metrics_json from metrics_snapshots "
        "where component='capacity_assessment' order by id"
    ).fetchall()
    con.close()
    assert recovered["capacity"]["assessment_generation"] == 3
    assert generation == 3
    assert reclaimable_since != 10_000
    assert [json.loads(row[0])["mature_reclaimable_count"] for row in metric_rows] == [
        0,
        0,
        0,
    ]


def test_capacity_recovery_preflight_failure_retries_without_committing_or_planning(
    tmp_path,
):
    from qbt_orchestrator.capacity_reclaim import CapacityReclaimResult
    from qbt_orchestrator.service import DaemonRuntime

    class FailingReclaimer:
        dry_run = True

        def __init__(self):
            self.fail = True
            self.calls = []

        def run(
            self,
            snapshots,
            *,
            assessment=None,
            capacity_state,
            free_bytes,
            target_free_bytes,
        ):
            self.calls.append(assessment)
            if assessment is None and not self.fail:
                con = sqlite3.connect(db)
                con.execute(
                    "update capacity_reclaims set state='aborted_paused' "
                    "where state='stopping'"
                )
                con.commit()
                con.close()
            return CapacityReclaimResult(
                dry_run=True,
                assessment_generation=(
                    0 if assessment is None else assessment.generation
                ),
                errors=["recovery failed"] if assessment is None and self.fail else [],
            )

    db = tmp_path / "state.sqlite"
    reclaimer = FailingReclaimer()
    daemon = DaemonRuntime(
        state_db=db,
        qbt=FakeQbt(),
        executor=FakeExecutor(),
        free_bytes_provider=lambda: 6 * 1024**3,
        dry_run=False,
        planner_dry_run=False,
        capacity_reclaimer=reclaimer,
    )
    daemon.tick_safety()
    con = sqlite3.connect(db)
    con.execute(
        "insert into capacity_reclaims("
        "reclaim_key,hash,name,magnet_uri,host_path,content_path,state,"
        "capacity_generation,created_at,updated_at) "
        "values('h:1','h','H','magnet:?xt=h',?,?,'stopping',1,1,1)",
        (str((tmp_path / "incomplete" / "h").resolve()), "/downloads/incomplete/h"),
    )
    con.commit()
    con.close()

    with pytest.raises(RuntimeError, match="recovery preflight failed"):
        daemon.planner_tick()

    con = sqlite3.connect(db)
    assert (
        con.execute("select count(*) from capacity_assessment_state").fetchone()[0]
        == 0
    )
    assert con.execute("select count(*) from scheduler_allocations").fetchone()[0] == 0
    con.close()
    assert daemon._capacity_recovery_preflight_done is False

    reclaimer.fail = False
    first = daemon.planner_tick()
    second = daemon.planner_tick()

    assert first["capacity"]["assessment_generation"] == 1
    assert second["capacity"]["assessment_generation"] == 2
    assert reclaimer.calls[0:2] == [None, None]
    assert [item.generation for item in reclaimer.calls[2:]] == [1]
    assert daemon._capacity_recovery_preflight_done is True


def test_capacity_recovery_gate_rechecks_pending_rows_after_first_generation(tmp_path):
    from qbt_orchestrator.capacity_reclaim import CapacityReclaimResult
    from qbt_orchestrator.service import DaemonRuntime

    class Recovery:
        dry_run = False

        def __init__(self):
            self.fail_recovery = False
            self.assessments = []

        def run(
            self,
            snapshots,
            *,
            assessment=None,
            capacity_state,
            free_bytes,
            target_free_bytes,
        ):
            self.assessments.append(assessment)
            if assessment is None:
                if self.fail_recovery:
                    return CapacityReclaimResult(
                        dry_run=False,
                        errors=["retry later"],
                    )
                con = sqlite3.connect(db)
                con.execute(
                    "update capacity_reclaims set state='aborted_paused' "
                    "where state='stopping'"
                )
                con.commit()
                con.close()
            return CapacityReclaimResult(
                dry_run=False,
                assessment_generation=0 if assessment is None else assessment.generation,
            )

    db = tmp_path / "state.sqlite"
    recovery = Recovery()
    daemon = DaemonRuntime(
        state_db=db,
        qbt=FakeQbt(),
        executor=FakeExecutor(),
        free_bytes_provider=lambda: 6 * 1024**3,
        dry_run=False,
        planner_dry_run=False,
        capacity_reclaimer=recovery,
    )
    daemon.tick_safety()
    assert daemon.planner_tick()["capacity"]["assessment_generation"] == 1

    con = sqlite3.connect(db)
    con.execute(
        "insert into capacity_reclaims("
        "reclaim_key,hash,name,magnet_uri,host_path,content_path,state,"
        "capacity_generation,created_at,updated_at) "
        "values('h:1','h','H','magnet:?xt=h',?,?,'stopping',1,1,1)",
        (str((tmp_path / "incomplete" / "h").resolve()), "/downloads/incomplete/h"),
    )
    con.commit()
    con.close()

    recovery.fail_recovery = True
    with pytest.raises(RuntimeError, match="retry later"):
        daemon.planner_tick()
    assert daemon._capacity_recovery_preflight_done is False
    con = sqlite3.connect(db)
    assert con.execute(
        "select current_generation from capacity_assessment_state where id=1"
    ).fetchone()[0] == 1
    con.close()

    recovery.fail_recovery = False
    assert daemon.planner_tick()["capacity"]["assessment_generation"] == 2
    assert recovery.assessments[-2:] == [None, None]


def test_capacity_recovery_preflight_fails_closed_without_reclaimer(tmp_path):
    from qbt_orchestrator.service import DaemonRuntime

    class LockedQbt(FakeQbt):
        def get_maindata(self, rid):
            self.rids.append(rid)
            return {
                "rid": rid + 1,
                "full_update": True,
                "torrents": {
                    "h": {
                        "name": "locked",
                        "category": "auto",
                        "tags": "auto",
                        "state": "stoppedDL",
                        "amount_left": 1,
                        "size": 2,
                        "num_seeds": 1,
                    }
                },
                "server_state": {},
            }

    db = tmp_path / "state.sqlite"
    daemon = DaemonRuntime(
        state_db=db,
        qbt=LockedQbt(),
        executor=FakeExecutor(),
        free_bytes_provider=lambda: 6 * 1024**3,
        dry_run=False,
        planner_dry_run=False,
    )
    daemon.tick_safety()
    con = sqlite3.connect(db)
    con.execute(
        "insert into capacity_reclaims("
        "reclaim_key,hash,name,magnet_uri,host_path,content_path,state,"
        "capacity_generation,created_at,updated_at) "
        "values('h:1','h','H','magnet:?xt=h',?,?,'stopping',1,1,1)",
        (str((tmp_path / "incomplete" / "h").resolve()), "/downloads/incomplete/h"),
    )
    con.commit()
    con.close()

    with pytest.raises(RuntimeError, match="recovery reclaimer is unavailable"):
        daemon.planner_tick()

    con = sqlite3.connect(db)
    assert con.execute("select count(*) from capacity_assessment_state").fetchone()[0] == 0
    assert con.execute("select count(*) from scheduler_allocations").fetchone()[0] == 0
    con.execute(
        "update capacity_reclaims set state='aborted_paused' where id=1"
    )
    con.commit()
    con.close()
    assert daemon._capacity_recovery_preflight_done is False

    payload = daemon.planner_tick()

    assert payload["capacity"]["assessment_generation"] == 1
    assert payload["planner"]["selected_hashes"] == ["h"]
    assert daemon._capacity_recovery_preflight_done is True


@pytest.mark.parametrize(
    ("runtime_dry_run", "planner_dry_run"),
    [(True, False), (False, True)],
)
def test_capacity_recovery_preflight_dry_run_never_calls_live_reclaimer(
    tmp_path,
    runtime_dry_run,
    planner_dry_run,
):
    from qbt_orchestrator.service import DaemonRuntime

    class LiveRecovery:
        dry_run = False

        def __init__(self):
            self.called = False

        def run(
            self,
            snapshots,
            *,
            assessment=None,
            capacity_state,
            free_bytes,
            target_free_bytes,
        ):
            self.called = True
            raise AssertionError("dry-run runtime must not invoke live recovery")

    db = tmp_path / "state.sqlite"
    live_recovery = LiveRecovery()
    daemon = DaemonRuntime(
        state_db=db,
        qbt=FakeQbt(),
        executor=FakeExecutor(),
        free_bytes_provider=lambda: 6 * 1024**3,
        dry_run=runtime_dry_run,
        planner_dry_run=planner_dry_run,
        capacity_recovery_reclaimer=live_recovery,
    )
    daemon.tick_safety()
    con = sqlite3.connect(db)
    con.execute(
        "insert into capacity_reclaims("
        "reclaim_key,hash,name,magnet_uri,host_path,content_path,state,"
        "capacity_generation,created_at,updated_at) "
        "values('h:1','h','H','magnet:?xt=h',?,?,'stopping',1,1,1)",
        (str((tmp_path / "incomplete" / "h").resolve()), "/downloads/incomplete/h"),
    )
    con.commit()
    con.close()

    with pytest.raises(RuntimeError, match="dry-run requires live recovery"):
        daemon.planner_tick()

    assert live_recovery.called is False
    con = sqlite3.connect(db)
    assert con.execute("select count(*) from capacity_assessment_state").fetchone()[0] == 0
    con.close()
    assert daemon._capacity_recovery_preflight_done is False


def test_capacity_recovery_preflight_fails_on_no_durable_progress(tmp_path):
    from qbt_orchestrator.capacity_reclaim import CapacityReclaimResult
    from qbt_orchestrator.service import DaemonRuntime

    class StalledRecovery:
        dry_run = False

        def __init__(self):
            self.calls = 0

        def run(
            self,
            snapshots,
            *,
            assessment=None,
            capacity_state,
            free_bytes,
            target_free_bytes,
        ):
            self.calls += 1
            return CapacityReclaimResult(dry_run=False)

    db = tmp_path / "state.sqlite"
    recovery = StalledRecovery()
    daemon = DaemonRuntime(
        state_db=db,
        qbt=FakeQbt(),
        executor=FakeExecutor(),
        free_bytes_provider=lambda: 6 * 1024**3,
        dry_run=False,
        planner_dry_run=False,
        capacity_recovery_reclaimer=recovery,
    )
    daemon.tick_safety()
    con = sqlite3.connect(db)
    con.execute(
        "insert into capacity_reclaims("
        "reclaim_key,hash,name,magnet_uri,host_path,content_path,state,"
        "capacity_generation,created_at,updated_at) "
        "values('h:1','h','H','magnet:?xt=h',?,?,'stopping',1,1,1)",
        (str((tmp_path / "incomplete" / "h").resolve()), "/downloads/incomplete/h"),
    )
    con.commit()
    con.close()

    with pytest.raises(RuntimeError, match="no durable state progress"):
        daemon.planner_tick()

    con = sqlite3.connect(db)
    event = json.loads(
        con.execute(
            "select data_json from events_v2 where "
            "event_type='recovery_preflight_failed' order by id desc limit 1"
        ).fetchone()[0]
    )
    con.close()
    assert recovery.calls == 1
    assert event["rounds"] == 1
    assert event["pending"] == [{"id": 1, "state": "stopping"}]
    assert daemon._capacity_recovery_preflight_done is False


def test_capacity_recovery_preflight_dry_run_without_pending_rows_skips_live_recovery(
    tmp_path,
):
    from qbt_orchestrator.service import DaemonRuntime

    class LiveRecovery:
        dry_run = False
        called = False

        def run(self, *_args, **_kwargs):
            self.called = True
            raise AssertionError("no recovery call is needed without pending rows")

    recovery = LiveRecovery()
    daemon = DaemonRuntime(
        state_db=tmp_path / "state.sqlite",
        qbt=FakeQbt(),
        executor=FakeExecutor(),
        free_bytes_provider=lambda: 6 * 1024**3,
        dry_run=True,
        capacity_recovery_reclaimer=recovery,
    )
    daemon.tick_safety()

    payload = daemon.planner_tick()

    assert payload["capacity"]["assessment_generation"] == 1
    assert recovery.called is False
    assert daemon._capacity_recovery_preflight_done is True


def test_daemon_startup_recovery_failure_precedes_safety_and_workers(tmp_path):
    from qbt_orchestrator.service import DaemonRuntime

    class FailingRecovery:
        dry_run = True

        def run(
            self,
            snapshots,
            *,
            assessment=None,
            capacity_state,
            free_bytes,
            target_free_bytes,
        ):
            assert assessment is None
            raise RuntimeError("startup recovery failed")

    qbt = FakeQbt()
    executor = FakeExecutor()
    daemon = DaemonRuntime(
        state_db=tmp_path / "state.sqlite",
        qbt=qbt,
        executor=executor,
        free_bytes_provider=lambda: 1024**3,
        dry_run=False,
        planner_dry_run=False,
        capacity_reclaimer=FailingRecovery(),
        background_event_workers=True,
        background_periodic_workers=True,
    )
    con = sqlite3.connect(daemon.state_db)
    con.execute(
        "insert into capacity_reclaims("
        "reclaim_key,hash,name,magnet_uri,host_path,content_path,state,"
        "capacity_generation,created_at,updated_at) "
        "values('h:1','h','H','magnet:?xt=h',?,?,'stopping',1,1,1)",
        (str((tmp_path / "incomplete" / "h").resolve()), "/downloads/incomplete/h"),
    )
    con.commit()
    con.close()

    with pytest.raises(RuntimeError, match="startup recovery failed"):
        daemon.run(max_safety_ticks=1)

    assert qbt.rids == []
    assert executor.posts == []
    assert daemon._event_worker_threads == []
    assert daemon._periodic_workers == []
    con = sqlite3.connect(daemon.state_db)
    event = json.loads(
        con.execute(
            "select data_json from events_v2 where "
            "event_type='recovery_preflight_failed' order by id desc limit 1"
        ).fetchone()[0]
    )
    con.close()
    assert event["rounds"] == 1
    assert event["errors"] == ["startup recovery failed"]


def test_daemon_startup_recovery_precedes_emergency_qbt_post(tmp_path):
    from qbt_orchestrator.capacity_reclaim import CapacityReclaimResult
    from qbt_orchestrator.service import DaemonRuntime

    order = []

    class ActiveQbt(FakeQbt):
        def get_maindata(self, rid):
            self.rids.append(rid)
            return {
                "rid": rid + 1,
                "full_update": True,
                "torrents": {
                    "active": {
                        "hash": "active",
                        "name": "active",
                        "category": "auto",
                        "tags": "auto",
                        "state": "downloading",
                        "amount_left": 100,
                        "size": 200,
                    }
                },
                "server_state": {},
            }

    class OrderedExecutor(FakeExecutor):
        def qbt_post(self, path, payload):
            order.append(("qbt_post", path))
            super().qbt_post(path, payload)

    class SuccessfulRecovery:
        def run(
            self,
            snapshots,
            *,
            assessment=None,
            capacity_state,
            free_bytes,
            target_free_bytes,
        ):
            assert snapshots == {}
            assert assessment is None
            assert capacity_state == "recovery_preflight"
            assert free_bytes == 1024**3
            order.append(("recovery_preflight", target_free_bytes))
            con = sqlite3.connect(db)
            con.execute(
                "update capacity_reclaims set state='aborted_paused' "
                "where state='stopping'"
            )
            con.commit()
            con.close()
            return CapacityReclaimResult(dry_run=False)

    db = tmp_path / "state.sqlite"
    executor = OrderedExecutor()
    daemon = DaemonRuntime(
        state_db=db,
        qbt=ActiveQbt(),
        executor=executor,
        free_bytes_provider=lambda: 1024**3,
        dry_run=False,
        planner_dry_run=False,
        capacity_recovery_reclaimer=SuccessfulRecovery(),
        loop_tasks=[],
        safety_interval=0,
    )
    con = sqlite3.connect(db)
    con.execute(
        "insert into capacity_reclaims("
        "reclaim_key,hash,name,magnet_uri,host_path,content_path,state,"
        "capacity_generation,created_at,updated_at) "
        "values('h:1','h','H','magnet:?xt=h',?,?,'stopping',1,1,1)",
        (str((tmp_path / "incomplete" / "h").resolve()), "/downloads/incomplete/h"),
    )
    con.commit()
    con.close()

    assert daemon.run(max_safety_ticks=1) == 1

    assert order == [
        ("recovery_preflight", daemon.drain_exit_bytes),
        ("qbt_post", "/api/v2/torrents/stop"),
    ]
    assert executor.posts == [
        ("/api/v2/torrents/stop", {"hashes": "active"})
    ]


def test_daemon_startup_dry_run_pending_recovery_fails_before_safety(tmp_path):
    from qbt_orchestrator.service import DaemonRuntime

    class ForbiddenRecovery:
        called = False

        def run(self, *_args, **_kwargs):
            self.called = True
            raise AssertionError("dry-run startup must not invoke live recovery")

    qbt = FakeQbt()
    executor = FakeExecutor()
    recovery = ForbiddenRecovery()
    daemon = DaemonRuntime(
        state_db=tmp_path / "state.sqlite",
        qbt=qbt,
        executor=executor,
        free_bytes_provider=lambda: 1024**3,
        dry_run=True,
        capacity_recovery_reclaimer=recovery,
        background_event_workers=True,
        background_periodic_workers=True,
    )
    con = sqlite3.connect(daemon.state_db)
    con.execute(
        "insert into capacity_reclaims("
        "reclaim_key,hash,name,magnet_uri,host_path,content_path,state,"
        "capacity_generation,created_at,updated_at) "
        "values('h:1','h','H','magnet:?xt=h',?,?,'stopping',1,1,1)",
        (str((tmp_path / "incomplete" / "h").resolve()), "/downloads/incomplete/h"),
    )
    con.commit()
    con.close()

    with pytest.raises(RuntimeError, match="dry-run requires live recovery"):
        daemon.run(max_safety_ticks=1)

    assert recovery.called is False
    assert qbt.rids == []
    assert executor.posts == []
    assert daemon._event_worker_threads == []
    assert daemon._periodic_workers == []


def test_runtime_calls_real_dead_partial_reclaimer_with_committed_assessment(
    tmp_path,
):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.service import DaemonRuntime

    managed = tmp_path / "incomplete"
    managed.mkdir()
    db = tmp_path / "state.sqlite"
    executor = FakeExecutor()
    reclaimer = DeadPartialReclaimer(
        db,
        executor,
        host_downloads=tmp_path,
        container_downloads="/downloads",
        managed_root=managed,
        dry_run=True,
        max_per_tick=0,
    )
    daemon = DaemonRuntime(
        state_db=db,
        qbt=FakeQbt(),
        executor=executor,
        free_bytes_provider=lambda: 6 * 1024**3,
        dry_run=True,
        capacity_reclaimer=reclaimer,
    )
    daemon.tick_safety()

    payload = daemon.planner_tick()

    generation = payload["capacity"]["assessment_generation"]
    assert generation == 1
    assert payload["capacity_reclaim"]["assessment_generation"] == generation


@pytest.mark.parametrize("lease_state", ["stopping", "quarantined"])
def test_real_capacity_recovery_preflight_converges_before_new_generation(
    tmp_path,
    lease_state,
):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.executor import Executor
    from qbt_orchestrator.service import DaemonRuntime

    managed = tmp_path / "incomplete"
    managed.mkdir()
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)

    class RecoveryQbt(FakeQbt):
        def post(self, path, payload):
            raise AssertionError("recovery fixture should not need a qBT mutation")

        def torrent_info(self, torrent_hash, timeout=None):
            return {
                "hash": torrent_hash,
                "category": "auto",
                "tags": "auto",
                "state": "stoppedDL",
                "amount_left": 1,
                "availability": 0.5,
                "num_seeds": 0,
                "num_complete": 0,
                "content_path": f"/downloads/incomplete/{torrent_hash}",
            }

    quarantine_path = None
    filesystem_dev = None
    filesystem_ino = None
    host_path = managed / "h"
    if lease_state == "quarantined":
        quarantine_path = managed / ".qbt-orchestrator-reclaim" / "reclaim-1"
        quarantine_path.mkdir(parents=True)
        (quarantine_path / "part").write_bytes(b"payload")
        metadata = quarantine_path.lstat()
        filesystem_dev = int(metadata.st_dev)
        filesystem_ino = int(metadata.st_ino)
    con = sqlite3.connect(db)
    con.execute(
        "insert into capacity_reclaims("
        "reclaim_key,hash,name,magnet_uri,host_path,content_path,state,"
        "capacity_generation,quarantine_path,filesystem_dev,filesystem_ino,"
        "created_at,updated_at) values(?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            "h:1",
            "h",
            "H",
            "mag" + "net:?xt=urn:btih:h",
            str(host_path.resolve()),
            "/downloads/incomplete/h",
            lease_state,
            1,
            None if quarantine_path is None else str(quarantine_path.resolve()),
            filesystem_dev,
            filesystem_ino,
            1,
            1,
        ),
    )
    con.commit()
    con.close()

    qbt = RecoveryQbt()
    executor = Executor(qbt, dry_run=False, state_db=db)
    reclaimer = DeadPartialReclaimer(
        db,
        executor,
        host_downloads=tmp_path,
        container_downloads="/downloads",
        managed_root=managed,
        dry_run=False,
        max_per_tick=0,
        disk_free_bytes=lambda _path: 0,
    )
    daemon = DaemonRuntime(
        state_db=db,
        qbt=qbt,
        executor=executor,
        free_bytes_provider=lambda: 6 * 1024**3,
        dry_run=False,
        planner_dry_run=False,
        capacity_reclaimer=reclaimer,
    )
    try:
        daemon.tick_safety()
        payload = daemon.planner_tick()

        con = sqlite3.connect(db)
        recovered_state = con.execute(
            "select state from capacity_reclaims where id=1"
        ).fetchone()[0]
        generation = con.execute(
            "select current_generation from capacity_assessment_state where id=1"
        ).fetchone()[0]
        con.close()
        assert recovered_state == "cancelled"
        assert generation == payload["capacity"]["assessment_generation"] == 1
        if lease_state == "quarantined":
            assert host_path.exists()
            assert not quarantine_path.exists()
    finally:
        executor.close(timeout=1)


def test_real_capacity_recovery_preflight_runs_multiple_rounds_to_reclaimed(
    tmp_path,
):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.executor import Executor
    from qbt_orchestrator.service import DaemonRuntime

    managed = tmp_path / "incomplete"
    managed.mkdir()
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    con = sqlite3.connect(db)
    con.execute(
        "insert into capacity_assessment_state("
        "id,current_generation,observed_at,summary_json) values(1,1,1,'{}')"
    )
    con.execute(
        "insert into capacity_reclaims("
        "reclaim_key,hash,name,magnet_uri,host_path,content_path,state,"
        "capacity_generation,created_at,updated_at) "
        "values('h:1','h','H','magnet:?xt=h',?,?,'deleting',1,1,1)",
        (str((managed / "h").resolve()), "/downloads/incomplete/h"),
    )
    con.commit()
    con.close()

    class RecoveryQbt(FakeQbt):
        def __init__(self):
            super().__init__()
            self.posts = []
            self.tags = {"h": {"auto"}}

        def post(self, path, payload):
            self.posts.append((path, dict(payload)))
            if str(path).endswith("/addTags"):
                torrent_hash = str(payload.get("hashes") or "")
                self.tags.setdefault(torrent_hash, set()).update(
                    part.strip()
                    for part in str(payload.get("tags") or "").split(",")
                    if part.strip()
                )
            return True

        def torrent_info(self, torrent_hash, timeout=None):
            torrent_hash = str(torrent_hash)
            return {
                "hash": torrent_hash,
                "state": "stoppedDL",
                "tags": ", ".join(sorted(self.tags.get(torrent_hash, {"auto"}))),
            }

    qbt = RecoveryQbt()
    executor = Executor(qbt, dry_run=False, state_db=db)
    reclaimer = DeadPartialReclaimer(
        db,
        executor,
        host_downloads=tmp_path,
        container_downloads="/downloads",
        managed_root=managed,
        dry_run=False,
        max_per_tick=0,
        disk_free_bytes=lambda _path: 0,
    )
    real_run = reclaimer.run
    recovery_assessments = []

    def recording_run(
        snapshots,
        *,
        assessment=None,
        capacity_state,
        free_bytes,
        target_free_bytes,
    ):
        recovery_assessments.append(assessment)
        return real_run(
            snapshots,
            assessment=assessment,
            capacity_state=capacity_state,
            free_bytes=free_bytes,
            target_free_bytes=target_free_bytes,
        )

    reclaimer.run = recording_run
    daemon = DaemonRuntime(
        state_db=db,
        qbt=qbt,
        executor=executor,
        free_bytes_provider=lambda: 6 * 1024**3,
        dry_run=False,
        planner_dry_run=False,
        capacity_reclaimer=reclaimer,
    )
    try:
        daemon.tick_safety()
        payload = daemon.planner_tick()

        con = sqlite3.connect(db)
        state = con.execute(
            "select state from capacity_reclaims where id=1"
        ).fetchone()[0]
        event = con.execute(
            "select data_json from events_v2 where "
            "component='capacity_reclaim' and "
            "event_type='recovery_preflight_completed' "
            "order by id desc limit 1"
        ).fetchone()[0]
        con.close()
        assert state == "reclaimed"
        assert recovery_assessments[0:2] == [None, None]
        assert recovery_assessments[2].generation == payload["capacity"]["assessment_generation"]
        assert json.loads(event)["rounds"] == 2
        assert qbt.posts == [
            ("/api/v2/torrents/recheck", {"hashes": "h"}),
            (
                "/api/v2/torrents/addTags",
                {"hashes": "h", "tags": "capacity-reclaimed,hold"},
            ),
        ]
        assert {"auto", "capacity-reclaimed", "hold"} <= qbt.tags["h"]
    finally:
        executor.close(timeout=1)


def test_real_capacity_recovery_recheck_failure_retries_without_generation(
    tmp_path,
):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.executor import Executor
    from qbt_orchestrator.service import DaemonRuntime

    managed = tmp_path / "incomplete"
    managed.mkdir()
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    con = sqlite3.connect(db)
    con.execute(
        "insert into capacity_assessment_state("
        "id,current_generation,observed_at,summary_json) values(1,1,1,'{}')"
    )
    con.execute(
        "insert into capacity_reclaims("
        "reclaim_key,hash,name,magnet_uri,host_path,content_path,state,"
        "capacity_generation,created_at,updated_at) "
        "values('h:1','h','H','magnet:?xt=h',?,?,'deleted',1,1,1)",
        (str((managed / "h").resolve()), "/downloads/incomplete/h"),
    )
    con.commit()
    con.close()

    class FailingRecheckQbt(FakeQbt):
        def __init__(self):
            super().__init__()
            self.posts = []

        def post(self, path, payload):
            self.posts.append((path, payload))
            raise RuntimeError("recheck failed")

    qbt = FailingRecheckQbt()
    executor = Executor(qbt, dry_run=False, state_db=db)
    reclaimer = DeadPartialReclaimer(
        db,
        executor,
        host_downloads=tmp_path,
        container_downloads="/downloads",
        managed_root=managed,
        dry_run=False,
        max_per_tick=0,
        disk_free_bytes=lambda _path: 0,
    )
    daemon = DaemonRuntime(
        state_db=db,
        qbt=qbt,
        executor=executor,
        free_bytes_provider=lambda: 6 * 1024**3,
        dry_run=False,
        planner_dry_run=False,
        capacity_reclaimer=reclaimer,
    )
    try:
        daemon.tick_safety()
        with pytest.raises(RuntimeError, match="recovery preflight failed"):
            daemon.planner_tick()
        with pytest.raises(RuntimeError, match="recovery preflight failed"):
            daemon.planner_tick()

        con = sqlite3.connect(db)
        row = con.execute(
            "select state from capacity_reclaims where id=1"
        ).fetchone()
        generation = con.execute(
            "select current_generation from capacity_assessment_state where id=1"
        ).fetchone()[0]
        capacity_count = con.execute("select count(*) from capacity_state").fetchone()[0]
        con.close()
        assert row[0] == "recheck_pending"
        assert generation == 1
        assert capacity_count == 0
        assert len(qbt.posts) == 2
        assert daemon._capacity_recovery_preflight_done is False
    finally:
        executor.close(timeout=1)


def test_daemon_runtime_runs_safety_ticks_and_persists_disk_state():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.service import DaemonRuntime

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        qbt = FakeQbt()
        daemon = DaemonRuntime(
            state_db=db,
            qbt=qbt,
            executor=FakeExecutor(),
            free_bytes_provider=lambda: 6 * 1024**3,
            dry_run=True,
            safety_interval=0,
        )
        daemon.run(max_safety_ticks=2)

        assert qbt.rids == [0, 1]
        con = sqlite3.connect(db)
        row = con.execute("select pressure_state, free_bytes from disk_state where id=1").fetchone()
        event_count = con.execute("select count(*) from events_v2 where component='daemon' and event_type='safety_tick'").fetchone()[0]
        con.close()
        assert row == ("ok", 6 * 1024**3)
    assert event_count == 1


def test_daemon_safety_event_sampling_keeps_transitions_immediate():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.service import DaemonRuntime

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        clock = [0.0]
        free_bytes = [6 * 1024**3]
        daemon = DaemonRuntime(
            state_db=db,
            qbt=FakeQbt(),
            executor=FakeExecutor(),
            free_bytes_provider=lambda: free_bytes[0],
            dry_run=True,
            safety_interval=0,
            monotonic=lambda: clock[0],
            safety_event_sample_interval_sec=60,
        )

        daemon.tick_safety()
        clock[0] = 1
        daemon.tick_safety()
        free_bytes[0] = 1024**3
        clock[0] = 2
        daemon.tick_safety()

        con = sqlite3.connect(db)
        rows = con.execute(
            "select message,data_json from events_v2 "
            "where component='daemon' and event_type='safety_tick' order by id"
        ).fetchall()
        con.close()
        assert len(rows) == 2
        assert "disk=emergency" in rows[-1][0]
        assert json.loads(rows[-1][1])["state_changed"] is True


def test_daemon_emits_one_event_when_sync_session_becomes_degraded():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.service import DaemonRuntime

    class RepeatedFullQbt(FakeQbt):
        def get_maindata(self, rid):
            self.rids.append(rid)
            return {"rid": rid + 1, "full_update": True, "torrents": {}, "server_state": {}}

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        qbt = RepeatedFullQbt()
        daemon = DaemonRuntime(
            state_db=db,
            qbt=qbt,
            executor=FakeExecutor(),
            free_bytes_provider=lambda: 6 * 1024**3,
            dry_run=True,
            safety_interval=0,
            sync_repeated_full_limit=1,
            sync_degraded_interval_sec=60,
        )

        daemon.tick_safety()
        daemon.tick_safety()
        daemon.tick_safety()

        con = sqlite3.connect(db)
        events = con.execute(
            "select event_type,data_json from events_v2 where component='qbt' and event_type='sync_session_degraded'"
        ).fetchall()
        con.close()
        assert len(events) == 1
        assert json.loads(events[0][1])["repeated_full_updates"] == 1
        assert qbt.rids == [0, 1]


def test_daemon_runtime_emergency_tick_pauses_managed_downloads():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.service import DaemonRuntime

    class ManagedQbt(FakeQbt):
        def get_maindata(self, rid):
            self.rids.append(rid)
            return {"rid": rid + 1, "full_update": True, "torrents": {"h1": {"name": "a", "category": "auto", "state": "downloading"}}, "server_state": {}}

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        executor = FakeExecutor()
        daemon = DaemonRuntime(
            state_db=db,
            qbt=ManagedQbt(),
            executor=executor,
            free_bytes_provider=lambda: 1 * 1024**3,
            dry_run=False,
            safety_interval=0,
        )
        daemon.run(max_safety_ticks=1)

        assert executor.posts == [("/api/v2/torrents/stop", {"hashes": "h1"})]


def test_daemon_runtime_uses_configurable_one_point_five_gib_emergency_floor():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.service import DaemonRuntime

    gib = 1024**3

    class ManagedQbt(FakeQbt):
        def get_maindata(self, rid):
            self.rids.append(rid)
            return {"rid": rid + 1, "full_update": True, "torrents": {"h1": {"hash": "h1", "name": "a", "category": "auto", "state": "downloading"}}, "server_state": {}}

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        executor = FakeExecutor()
        daemon = DaemonRuntime(
            state_db=db,
            qbt=ManagedQbt(),
            executor=executor,
            free_bytes_provider=lambda: int(1.6 * gib),
            dry_run=False,
            safety_interval=0,
            emergency_floor_bytes=int(1.5 * gib),
        )
        daemon.run(max_safety_ticks=1)
        assert executor.posts == []

        daemon_low = DaemonRuntime(
            state_db=db,
            qbt=ManagedQbt(),
            executor=executor,
            free_bytes_provider=lambda: int(1.4 * gib),
            dry_run=False,
            safety_interval=0,
            emergency_floor_bytes=int(1.5 * gib),
        )
        daemon_low.run(max_safety_ticks=1)
        assert executor.posts == [("/api/v2/torrents/stop", {"hashes": "h1"})]


def test_daemon_runtime_enqueues_proactive_telegram_alerts_for_all_stopped_and_near_thresholds():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.service import DaemonRuntime

    gib = 1024**3
    mib = 1024**2

    class StoppedQbt(FakeQbt):
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
                        "amount_left": 10 * gib,
                        "size": 12 * gib,
                        "progress": 0.1,
                    }
                },
                "server_state": {},
            }

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        daemon = DaemonRuntime(
            state_db=db,
            qbt=StoppedQbt(),
            executor=FakeExecutor(),
            free_bytes_provider=lambda: 3 * gib + 64 * mib,
            dry_run=True,
            safety_interval=0,
            disk_floor_bytes=3 * gib,
            recovery_enter_bytes=3 * gib + 512 * mib,
            emergency_floor_bytes=int(1.5 * gib),
            scheduler_alert_chat_ids=["12345"],
            scheduler_alerts_enabled=True,
            scheduler_alert_interval_sec=60,
            disk_alert_margin_bytes=512 * mib,
        )

        daemon.monitor.tick()
        daemon.planner_tick()
        daemon.planner_tick()

        con = sqlite3.connect(db)
        con.row_factory = sqlite3.Row
        rows = [
            dict(r)
            for r in con.execute(
                "select chat_id,topic,level,message,state from bot_notifications order by id"
            )
        ]
        warn_rows = [
            dict(r)
            for r in con.execute(
                "select warning_key,topic from bot_warning_inbox "
                "where warning_key='daemon_task:scheduler:all_stopped' "
                "or topic='scheduler_all_stopped'"
            )
        ]
        con.close()
        assert [(r["chat_id"], r["topic"], r["level"], r["state"]) for r in rows] == [
            ("12345", "disk_threshold", "warning", "queued"),
        ]
        assert "free=" in rows[0]["message"]
        assert not any(r["topic"] == "scheduler_all_stopped" for r in rows)
        assert warn_rows == []


class FakeTelegramService:
    def __init__(self, fail_once=False):
        self.calls = 0
        self.fail_once = fail_once

    def poll_once(self):
        self.calls += 1
        if self.fail_once and self.calls == 1:
            raise RuntimeError("telegram boom")
        return 1


def test_telegram_supervisor_polls_and_survives_crash():
    from qbt_orchestrator.service import TelegramSupervisor

    service = FakeTelegramService(fail_once=True)
    supervisor = TelegramSupervisor(service, interval=0, max_backoff=0)

    assert supervisor.poll_once_supervised() == 0
    assert supervisor.poll_once_supervised() == 1
    assert service.calls == 2
    assert supervisor.consecutive_failures == 0


def test_telegram_supervisor_rate_limits_panel_refresh_error_logs(caplog):
    import logging

    from qbt_orchestrator.service import TelegramSupervisor

    class ServiceWithBrokenRefresh:
        def __init__(self):
            self.calls = 0
            self.router = self

        def poll_once(self):
            self.calls += 1
            return 1

        def refresh_panel_if_due(self, interval_sec=60):
            raise RuntimeError("panel refresh boom")

    service = ServiceWithBrokenRefresh()
    supervisor = TelegramSupervisor(service, interval=0, max_backoff=0)
    with caplog.at_level(logging.WARNING, logger="qbt_orchestrator.service"):
        assert supervisor.poll_once_supervised() == 1
        assert supervisor.poll_once_supervised() == 1
    messages = [
        record.getMessage()
        for record in caplog.records
        if "panel auto-refresh failed" in record.getMessage()
    ]
    assert len(messages) == 1
    assert "panel refresh boom" in messages[0]


def test_daemon_runtime_starts_optional_telegram_supervisor():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.service import DaemonRuntime, TelegramSupervisor

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        telegram_service = FakeTelegramService()
        supervisor = TelegramSupervisor(telegram_service, interval=0, max_backoff=0)
        daemon = DaemonRuntime(
            state_db=db,
            qbt=FakeQbt(),
            executor=FakeExecutor(),
            free_bytes_provider=lambda: 6 * 1024**3,
            dry_run=True,
            safety_interval=0,
            telegram_supervisor=supervisor,
        )

        daemon.run(max_safety_ticks=2)

        assert telegram_service.calls >= 1


def test_build_telegram_supervisor_from_env_requires_token_and_parses_roles():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.service import build_telegram_supervisor_from_env

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        assert build_telegram_supervisor_from_env(db, env={}) is None

        made = {}

        class FakeApi:
            def __init__(self, token):
                made["token"] = token
            def get_updates(self, offset, timeout):
                return []
            def send_message(self, chat_id, text, reply_markup=None):
                return {"ok": True}

        supervisor = build_telegram_supervisor_from_env(
            db,
            env={
                "QBT_ORCH_TELEGRAM_TOKEN": "123456:abc",
                "QBT_ORCH_TG_VIEWERS": "10",
                "QBT_ORCH_TG_OPERATORS": "20,21",
                "QBT_ORCH_TG_ADMINS": "30",
            },
            api_factory=FakeApi,
        )
        assert supervisor is not None
        assert made["token"] == "123456:abc"
        assert supervisor.service.authorizer.allowed(10, "status")
        assert supervisor.service.authorizer.allowed(20, "pause")
        assert supervisor.service.authorizer.allowed(30, "config")
        assert not supervisor.service.authorizer.allowed(30, "cleanup")
        assert not supervisor.service.authorizer.allowed(10, "cleanup")


def test_daemon_runtime_processes_queued_bot_commands_after_safety_tick():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.runtime import BotCommandRepository, CommandProcessor
    from qbt_orchestrator.service import DaemonRuntime

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        commands = BotCommandRepository(db)
        commands.insert_command("c1", 100, 30, "pause", {"args": ["h1"]})
        executor = FakeExecutor()
        daemon = DaemonRuntime(
            state_db=db,
            qbt=FakeQbt(),
            executor=executor,
            free_bytes_provider=lambda: 6 * 1024**3,
            dry_run=False,
            safety_interval=0,
            command_processor=CommandProcessor(commands, executor),
        )

        daemon.run(max_safety_ticks=1)

        assert executor.posts == [("/api/v2/torrents/stop", {"hashes": "h1"})]
        assert commands.get("c1")["state"] == "done"


def test_daemon_notification_sender_dry_run_does_not_claim_or_send():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.runtime import BotNotificationRepository
    from qbt_orchestrator.service import DaemonRuntime

    class Sender:
        def __init__(self):
            self.calls = 0
        def has_pending(self):
            return True
        def send_next(self):
            self.calls += 1

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        repo = BotNotificationRepository(db)
        note_id = repo.enqueue(100, "status", "hello")
        sender = Sender()
        daemon = DaemonRuntime(
            state_db=db,
            qbt=FakeQbt(),
            executor=FakeExecutor(),
            free_bytes_provider=lambda: 6 * 1024**3,
            dry_run=False,
            safety_interval=0,
            telegram_notification_sender=sender,
            notification_dry_run=True,
        )

        daemon.run(max_safety_ticks=1)

        assert sender.calls == 0
        assert repo.get(note_id)["state"] == "queued"
        con = sqlite3.connect(db)
        action = con.execute("select action_type,path,status,dry_run from action_log where action_type='telegram_notify'").fetchone()
        con.close()
        assert action == ("telegram_notify", "bot_notifications", "dry_run", 1)


def test_daemon_notification_sender_live_sends_one_message():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.runtime import BotNotificationRepository
    from qbt_orchestrator.service import DaemonRuntime

    class Sender:
        def __init__(self, repo):
            self.repo = repo
            self.calls = 0
        def has_pending(self):
            return self.repo.peek_next() is not None
        def send_next(self):
            self.calls += 1
            row = self.repo.claim_next()
            self.repo.mark_sent(row["id"])
            return row["id"]

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        repo = BotNotificationRepository(db)
        note_id = repo.enqueue(100, "status", "hello")
        sender = Sender(repo)
        daemon = DaemonRuntime(
            state_db=db,
            qbt=FakeQbt(),
            executor=FakeExecutor(),
            free_bytes_provider=lambda: 6 * 1024**3,
            dry_run=False,
            safety_interval=0,
            telegram_notification_sender=sender,
            notification_dry_run=False,
        )

        daemon.run(max_safety_ticks=1)

        assert sender.calls == 1
        assert repo.get(note_id)["state"] == "sent"


class FakeClock:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []
    def monotonic(self):
        return self.now
    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


class CountingLoop:
    def __init__(self, name):
        self.name = name
        self.calls = []
    def __call__(self):
        self.calls.append(self.name)
        return {"loop": self.name, "calls": len(self.calls)}


def test_daemon_runtime_runs_design_multirate_loops_and_records_events():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.service import DaemonRuntime, LoopTask

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        clock = FakeClock()
        planner = CountingLoop("planner")
        file_batch = CountingLoop("file_batch")
        maintenance = CountingLoop("maintenance")
        carousel = CountingLoop("carousel")
        daemon = DaemonRuntime(
            state_db=db,
            qbt=FakeQbt(),
            executor=FakeExecutor(),
            free_bytes_provider=lambda: 6 * 1024**3,
            dry_run=True,
            safety_interval=2,
            monotonic=clock.monotonic,
            sleeper=clock.sleep,
            loop_tasks=[
                LoopTask("planner", 15, planner),
                LoopTask("file_batch", 60, file_batch),
                LoopTask("maintenance", 300, maintenance),
                LoopTask("carousel", 1800, carousel),
            ],
        )

        daemon.run(max_safety_ticks=31)

        assert len(planner.calls) == 4  # t=0,16,32,48 with 2s ticks
        assert len(file_batch.calls) == 2  # t=0,60
        assert len(maintenance.calls) == 1
        assert len(carousel.calls) == 1
        con = sqlite3.connect(db)
        events = con.execute("select event_type,count(*) from events_v2 group by event_type").fetchall()
        con.close()
        assert ("loop_tick", 8) in events


class SequenceMonotonic:
    def __init__(self, values):
        self._values = iter(values)

    def __call__(self):
        return float(next(self._values))


def test_loop_task_records_duration_and_deadline_miss():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.service import DaemonRuntime, LoopTask

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        clock = SequenceMonotonic([0.0, 0.0, 7.5])
        daemon = DaemonRuntime(
            state_db=db,
            qbt=FakeQbt(),
            executor=FakeExecutor(),
            free_bytes_provider=lambda: 6 * 1024**3,
            dry_run=True,
            monotonic=clock,
            loop_tasks=[LoopTask("file_batch", 60, lambda: {"ok": True}, max_runtime_sec=5)],
        )

        assert daemon.run_due_loop_tasks() == 1

        con = sqlite3.connect(db)
        row = con.execute(
            "select component,metrics_json from metrics_snapshots where component='loop_runtime:file_batch'"
        ).fetchone()
        con.close()
        assert row is not None
        metrics = json.loads(row[1])
        assert metrics["duration_ms"] == 7500
        assert metrics["max_runtime_ms"] == 5000
        assert metrics["deadline_missed"] is True
        assert metrics["sample_count"] == 1
        assert metrics["deadline_miss_count"] == 1


def test_loop_runtime_metric_rolls_up_and_records_failed_callbacks():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.service import DaemonRuntime, LoopTask

    outcomes = iter([False, True])

    def callback():
        if next(outcomes):
            raise RuntimeError("planned failure")
        return {"ok": True}

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        clock = SequenceMonotonic([0.0, 0.0, 1.0, 61.0, 61.0, 68.5])
        task = LoopTask("file_batch", 60, callback, max_runtime_sec=5)
        daemon = DaemonRuntime(
            state_db=db,
            qbt=FakeQbt(),
            executor=FakeExecutor(),
            free_bytes_provider=lambda: 6 * 1024**3,
            dry_run=True,
            monotonic=clock,
            loop_tasks=[task],
        )

        assert daemon.run_due_loop_tasks() == 1
        assert daemon.run_due_loop_tasks() == 1

        con = sqlite3.connect(db)
        rows = con.execute(
            "select metrics_json from metrics_snapshots where component='loop_runtime:file_batch'"
        ).fetchall()
        failed_events = con.execute(
            "select count(*) from events_v2 where component='file_batch' and event_type='loop_failed'"
        ).fetchone()[0]
        con.close()
        assert len(rows) == 1
        metrics = json.loads(rows[0][0])
        assert metrics["sample_count"] == 2
        assert metrics["failure_count"] == 1
        assert metrics["deadline_miss_count"] == 1
        assert metrics["recent_duration_ms"] == [1000, 7500]
        assert metrics["p50_duration_ms"] == 1000
        assert metrics["p95_duration_ms"] == 7500
        assert failed_events == 1


def test_daemon_maintenance_records_qbt_path_drift_from_sync_cache():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.path_reconcile import QbtPathReconciler
    from qbt_orchestrator.service import DaemonRuntime

    class DriftQbt(FakeQbt):
        def get_maindata(self, rid):
            self.rids.append(rid)
            return {
                "rid": rid + 1,
                "full_update": True,
                "torrents": {
                    "h1": {
                        "name": "BBAN-582",
                        "category": "auto",
                        "tags": "auto",
                        "state": "stoppedDL",
                        "amount_left": 1,
                        "size": 2,
                        "progress": 0.06,
                        "save_path": "/downloads/active",
                        "content_path": "/downloads/BBAN-582",
                    }
                },
                "server_state": {},
            }

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        daemon = DaemonRuntime(
            state_db=db,
            qbt=DriftQbt(),
            executor=FakeExecutor(),
            free_bytes_provider=lambda: 6 * 1024**3,
            dry_run=True,
            safety_interval=0,
            path_reconciler=QbtPathReconciler(db),
        )

        daemon.run(max_safety_ticks=1)

        con = sqlite3.connect(db)
        event = con.execute("select component,event_type,hash from events_v2 where component='qbt_reconcile'").fetchone()
        loop = con.execute("select data_json from events_v2 where component='maintenance' and event_type='loop_tick' order by id desc limit 1").fetchone()[0]
        con.close()
        assert event == ("qbt_reconcile", "path_drift", "h1")
        assert "path_reconcile" in loop


def test_daemon_default_planner_loop_uses_sync_cache_and_records_allocations():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.service import DaemonRuntime

    class PlannerQbt(FakeQbt):
        def get_maindata(self, rid):
            self.rids.append(rid)
            return {
                "rid": rid + 1,
                "full_update": True,
                "torrents": {"h1": {"name": "small", "category": "auto", "state": "stoppedDL", "amount_left": 1, "size": 2, "progress": 0.1}},
                "server_state": {},
            }

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        daemon = DaemonRuntime(
            state_db=db,
            qbt=PlannerQbt(),
            executor=FakeExecutor(),
            free_bytes_provider=lambda: 6 * 1024**3,
            dry_run=True,
            safety_interval=0,
        )

        daemon.run(max_safety_ticks=1)

        con = sqlite3.connect(db)
        alloc = con.execute("select hash,desired_state,slot_kind from scheduler_allocations").fetchone()
        action = con.execute("select path,status,dry_run from action_log").fetchone()
        loop = con.execute("select data_json from events_v2 where component='planner' and event_type='loop_tick' order by id desc limit 1").fetchone()[0]
        con.close()
        assert alloc == ("h1", "active", "stable")
        assert action == ("/api/v2/torrents/start", "dry_run", 1)
        assert "not_configured" not in loop


def test_daemon_planner_loop_invokes_seeding_preemption_policy_for_waiting_hot_task():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.service import DaemonRuntime

    class PlannerQbt(FakeQbt):
        def get_maindata(self, rid):
            self.rids.append(rid)
            return {
                "rid": rid + 1,
                "full_update": True,
                "torrents": {
                    "newhot": {"name": "NEW-HOT", "category": "auto", "tags": "auto,hot", "state": "stoppedDL", "amount_left": 4 * 1024**3, "size": 5 * 1024**3, "progress": 0.5, "dlspeed": 5 * 1024**2},
                    "seed1": {"name": "OLD-SEED", "category": "auto", "tags": "auto", "state": "uploading", "amount_left": 0, "progress": 1.0, "size": 6 * 1024**3, "seeding_time": 7200, "ratio": 1.0},
                },
                "server_state": {},
            }

    class FakePreemption:
        def __init__(self):
            self.calls = []

        def evaluate_and_apply(self, snapshots, *, disk_state, trigger_reason, selected_hashes):
            self.calls.append((set(snapshots), disk_state, trigger_reason, set(selected_hashes)))
            return None

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        preemption = FakePreemption()
        daemon = DaemonRuntime(
            state_db=db,
            qbt=PlannerQbt(),
            executor=FakeExecutor(),
            free_bytes_provider=lambda: int(4.5 * 1024**3),
            dry_run=True,
            safety_interval=0,
            preemption_service=preemption,
        )

        daemon.run(max_safety_ticks=1)

        assert preemption.calls
        hashes, disk_state, trigger_reason, selected_hashes = preemption.calls[0]
        assert {"newhot", "seed1"}.issubset(hashes)
        assert disk_state == "watch"
        assert trigger_reason == "planner_pressure"
        assert "newhot" not in selected_hashes


def test_daemon_live_safety_keeps_planner_dry_run_until_explicitly_enabled():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.service import DaemonRuntime

    class PlannerQbt(FakeQbt):
        def get_maindata(self, rid):
            self.rids.append(rid)
            return {
                "rid": rid + 1,
                "full_update": True,
                "torrents": {"h1": {"name": "small", "category": "auto", "state": "stoppedDL", "amount_left": 1, "size": 2, "progress": 0.1}},
                "server_state": {},
            }

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        executor = FakeExecutor()
        daemon = DaemonRuntime(
            state_db=db,
            qbt=PlannerQbt(),
            executor=executor,
            free_bytes_provider=lambda: 6 * 1024**3,
            dry_run=False,
            safety_interval=0,
        )

        daemon.run(max_safety_ticks=1)

        assert executor.posts == []
        con = sqlite3.connect(db)
        action = con.execute("select path,status,dry_run from action_log").fetchone()
        con.close()
        assert action == ("/api/v2/torrents/start", "dry_run", 1)


def test_daemon_planner_can_be_explicitly_enabled_for_live_apply():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.service import DaemonRuntime

    class PlannerQbt(FakeQbt):
        def get_maindata(self, rid):
            self.rids.append(rid)
            return {
                "rid": rid + 1,
                "full_update": True,
                "torrents": {"h1": {"name": "small", "category": "auto", "state": "stoppedDL", "amount_left": 1, "size": 2, "progress": 0.1}},
                "server_state": {},
            }

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        executor = FakeExecutor()
        daemon = DaemonRuntime(
            state_db=db,
            qbt=PlannerQbt(),
            executor=executor,
            free_bytes_provider=lambda: 6 * 1024**3,
            dry_run=False,
            planner_dry_run=False,
            safety_interval=0,
        )

        daemon.run(max_safety_ticks=1)

        assert executor.posts == [("/api/v2/torrents/start", {"hashes": "h1"})]


def test_daemon_upload_worker_dry_run_does_not_claim_or_call_rclone():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.runtime import TorrentJobRepository, UploadJobRunner
    from qbt_orchestrator.service import DaemonRuntime
    from tests.fakes import FakeRclone

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        repo = TorrentJobRepository(db)
        job_id = repo.enqueue("h1", 1, "upload", {"local": "/tmp/a.mp4", "remote": "gcrypt:/A/a.mp4", "size": 100, "full_torrent": True}, priority=1)
        rclone = FakeRclone(copy_ok=True, remote_sizes={"gcrypt:/A/a.mp4": 100})
        daemon = DaemonRuntime(
            state_db=db,
            qbt=FakeQbt(),
            executor=FakeExecutor(),
            free_bytes_provider=lambda: 6 * 1024**3,
            dry_run=False,
            safety_interval=0,
            upload_runner=UploadJobRunner(repo, rclone, FakeExecutor()),
            upload_dry_run=True,
        )

        daemon.run(max_safety_ticks=1)

        assert rclone.copies == []
        assert repo.get(job_id)["state"] == "queued"
        con = sqlite3.connect(db)
        action = con.execute("select action_type,path,status,dry_run from action_log where action_type='upload_job'").fetchone()
        event = con.execute("select event_type from events_v2 where component='upload' order by id desc limit 1").fetchone()[0]
        con.close()
        assert action == ("upload_job", "torrent_jobs/upload", "dry_run", 1)
        assert event == "upload_dry_run"


def test_daemon_upload_worker_live_processes_job_and_verify_pending():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.runtime import TorrentJobRepository, UploadJobRunner
    from qbt_orchestrator.service import DaemonRuntime
    from tests.fakes import FakeRclone

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        repo = TorrentJobRepository(db)
        job_id = repo.enqueue("h2", 2, "upload", {"local": "/tmp/b.mp4", "remote": "gcrypt:/B/b.mp4", "size": 100, "full_torrent": True}, priority=1)
        rclone = FakeRclone(copy_ok=True, remote_sizes={"gcrypt:/B/b.mp4": 99})
        daemon = DaemonRuntime(
            state_db=db,
            qbt=FakeQbt(),
            executor=FakeExecutor(),
            free_bytes_provider=lambda: 6 * 1024**3,
            dry_run=False,
            safety_interval=0,
            upload_runner=UploadJobRunner(repo, rclone, FakeExecutor()),
            upload_dry_run=False,
        )

        daemon.run(max_safety_ticks=1)

        assert rclone.copies == [("/tmp/b.mp4", "gcrypt:/B/b.mp4")]
        assert repo.get(job_id)["state"] == "verify_pending"
        con = sqlite3.connect(db)
        event = con.execute("select event_type from events_v2 where component='upload' order by id desc limit 1").fetchone()[0]
        con.close()
        assert event == "upload_job_processed"


def test_daemon_cleanup_request_dry_run_does_not_claim_or_mutate_batch():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.runtime import CleanupRequestRunner, TorrentJobRepository
    from qbt_orchestrator.service import DaemonRuntime

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        con.execute(
            "insert into torrent_batches(id,hash,batch_no,state,mode,indices_json,total_bytes,downloaded_bytes,reserved_bytes,local_pinned_bytes,cleanup_deferred_at,created_at,updated_at) values(?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (1, "h1", 1, "cleanup_deferred", "pipeline", "[0]", 10, 10, 12, 10, 100, 100, 100),
        )
        con.commit(); con.close()
        repo = TorrentJobRepository(db, now=lambda: 100)
        job_id = repo.enqueue("h1", 1, "cleanup_request", {"target": "h1"}, priority=10)
        daemon = DaemonRuntime(
            state_db=db,
            qbt=FakeQbt(),
            executor=FakeExecutor(),
            free_bytes_provider=lambda: 6 * 1024**3,
            dry_run=False,
            safety_interval=0,
            cleanup_runner=CleanupRequestRunner(repo, FakeExecutor()),
            cleanup_dry_run=True,
        )

        assert daemon.process_cleanup_requests() == 1

        assert repo.get(job_id)["state"] == "queued"
        con = sqlite3.connect(db)
        batch_state = con.execute("select state from torrent_batches where id=1").fetchone()[0]
        action = con.execute("select action_type,path,status,dry_run from action_log where action_type='cleanup_request'").fetchone()
        con.close()
        assert batch_state == "cleanup_deferred"
        assert action == ("cleanup_request", "torrent_jobs/cleanup_request", "dry_run", 1)


def test_daemon_cleanup_request_live_processes_logical_cleanup():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.runtime import CleanupRequestRunner, TorrentJobRepository
    from qbt_orchestrator.service import DaemonRuntime

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        con.execute(
            "insert into torrent_batches(id,hash,batch_no,state,mode,indices_json,total_bytes,downloaded_bytes,reserved_bytes,local_pinned_bytes,cleanup_deferred_at,created_at,updated_at) values(?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (2, "h2", 1, "cleanup_deferred", "pipeline", "[0]", 10, 10, 12, 10, 100, 100, 100),
        )
        con.commit(); con.close()
        repo = TorrentJobRepository(db, now=lambda: 200)
        job_id = repo.enqueue("h2", 2, "cleanup_request", {"target": "h2"}, priority=10)
        daemon = DaemonRuntime(
            state_db=db,
            qbt=FakeQbt(),
            executor=FakeExecutor(),
            free_bytes_provider=lambda: 6 * 1024**3,
            dry_run=False,
            safety_interval=0,
            cleanup_runner=CleanupRequestRunner(repo, FakeExecutor()),
            cleanup_dry_run=False,
        )

        assert daemon.process_cleanup_requests() == 1

        assert repo.get(job_id)["state"] == "done"
        con = sqlite3.connect(db)
        batch_state = con.execute("select state from torrent_batches where id=2").fetchone()[0]
        con.close()
        assert batch_state == "cleanup_requested"


def test_daemon_background_event_workers_do_not_block_safety_loop():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.service import DaemonRuntime

    class SlowUploadRunner:
        def __init__(self):
            self.calls = 0

        def run_next(self):
            self.calls += 1
            time.sleep(1.0)
            return None

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        runner = SlowUploadRunner()
        qbt = FakeQbt()
        daemon = DaemonRuntime(
            state_db=db,
            qbt=qbt,
            executor=FakeExecutor(),
            free_bytes_provider=lambda: 6 * 1024**3,
            dry_run=False,
            safety_interval=0,
            upload_runner=runner,
            upload_dry_run=False,
            background_event_workers=True,
            event_worker_interval=0.01,
            event_worker_join_timeout=0.01,
        )

        started = time.monotonic()
        daemon.run(max_safety_ticks=2)
        elapsed = time.monotonic() - started

        assert qbt.rids == [0, 1]
        # The safety loop must not wait for two full upload-worker sleeps.
        # Small VPS runners can add scheduling overhead around thread shutdown,
        # so keep the assertion below the blocking-path duration instead of
        # assuming sub-second wall-clock timing.
        assert elapsed < 1.5
        assert runner.calls >= 1


def test_daemon_background_periodic_workers_do_not_block_safety_loop():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.service import DaemonRuntime, LoopTask

    class BlockingInventory:
        def __init__(self):
            self.started = threading.Event()
            self.release = threading.Event()

        def __call__(self):
            self.started.set()
            self.release.wait(timeout=2)
            return {"released": self.release.is_set()}

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        blocker = BlockingInventory()
        qbt = FakeQbt()
        daemon = DaemonRuntime(
            state_db=db,
            qbt=qbt,
            executor=FakeExecutor(),
            free_bytes_provider=lambda: 6 * 1024**3,
            dry_run=True,
            safety_interval=0.01,
            loop_tasks=[LoopTask("file_batch", 60, blocker, max_runtime_sec=0.05)],
            background_periodic_workers=True,
            periodic_worker_join_timeout=0.01,
            sync_repeated_full_limit=999,
        )

        started = time.monotonic()
        try:
            daemon.run(max_safety_ticks=5)
            elapsed = time.monotonic() - started
            assert blocker.started.wait(timeout=0.2)
            assert qbt.rids == [0, 1, 2, 3, 4]
            assert elapsed < 0.5
        finally:
            blocker.release.set()
            for worker in list(daemon._periodic_workers):
                worker.join(timeout=1)


class RecordingBackfill:
    def __init__(self, result=None):
        self.result = result or {"status": "not_found", "artifacts": []}
        self.calls = []

    def scrape_one(self, media_group_key, manifest_id):
        self.calls.append((media_group_key, manifest_id))
        return self.result


def test_daemon_media_and_emby_workers_dry_run_do_not_claim_or_call_emby():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.media import EmbyRefreshWorker, MediaPipelineJobRunner, MediaPipelineService
    from qbt_orchestrator.runtime import TorrentJobRepository
    from qbt_orchestrator.service import DaemonRuntime
    from tests.fakes import FakeEmby

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        repo = TorrentJobRepository(db, now=lambda: 1000)
        job_id = repo.enqueue(
            "h1",
            None,
            "media_pipeline",
            {"upload_manifest_id": "m1", "files": [{"remote_path": "gcrypt:/ABC-123/ABC-123.mp4", "size": 1024**3, "duration_sec": 120}]},
            priority=1,
        )
        con = sqlite3.connect(db)
        con.execute(
            "insert into emby_refresh_tasks(emby_media_dir,state,earliest_run_at,max_run_at,payload_json,created_at,updated_at) values(?,?,?,?,?,?,?)",
            ("/media/gcrypt/ABC-123", "queued", 900, 1200, "{}", 800, 800),
        )
        con.commit()
        con.close()
        emby = FakeEmby()
        daemon = DaemonRuntime(
            state_db=db,
            qbt=FakeQbt(),
            executor=FakeExecutor(),
            free_bytes_provider=lambda: 6 * 1024**3,
            dry_run=False,
            safety_interval=0,
            media_pipeline_runner=MediaPipelineJobRunner(repo, MediaPipelineService(db, RecordingBackfill(), now=lambda: 1000)),
            media_pipeline_dry_run=True,
            emby_refresh_worker=EmbyRefreshWorker(db, emby=emby, now=lambda: 1000),
            emby_refresh_dry_run=True,
        )

        daemon.run(max_safety_ticks=1)

        assert repo.get(job_id)["state"] == "queued"
        assert emby.refreshes == []
        con = sqlite3.connect(db)
        task_state = con.execute("select state from emby_refresh_tasks").fetchone()[0]
        actions = con.execute("select action_type,path,status,dry_run from action_log where action_type in ('media_pipeline_job','emby_refresh') order by id").fetchall()
        con.close()
        assert task_state == "queued"
        assert actions == [
            ("media_pipeline_job", "torrent_jobs/media_pipeline", "dry_run", 1),
            ("emby_refresh", "/media/gcrypt/ABC-123", "dry_run", 1),
        ]


def test_daemon_media_pipeline_then_emby_refresh_live_path():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.media import EmbyRefreshWorker, MediaPipelineJobRunner, MediaPipelineService
    from qbt_orchestrator.runtime import TorrentJobRepository
    from qbt_orchestrator.service import DaemonRuntime
    from tests.fakes import FakeEmby

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        repo = TorrentJobRepository(db, now=lambda: 1000)
        job_id = repo.enqueue(
            "h1",
            None,
            "media_pipeline",
            {"upload_manifest_id": "m1", "files": [{"remote_path": "gcrypt:/ABC-123/ABC-123.mp4", "size": 1024**3, "duration_sec": 120}]},
            priority=1,
        )
        emby = FakeEmby()
        daemon = DaemonRuntime(
            state_db=db,
            qbt=FakeQbt(),
            executor=FakeExecutor(),
            free_bytes_provider=lambda: 6 * 1024**3,
            dry_run=False,
            safety_interval=0,
            media_pipeline_runner=MediaPipelineJobRunner(repo, MediaPipelineService(db, RecordingBackfill(), now=lambda: 1000)),
            media_pipeline_dry_run=False,
            emby_refresh_worker=EmbyRefreshWorker(db, emby=emby, now=lambda: 1400),
            emby_refresh_dry_run=False,
        )

        daemon.run(max_safety_ticks=1)

        assert repo.get(job_id)["state"] == "done"
        assert emby.refreshes == [{"Updates": [{"Path": "/media/gcrypt/ABC-123", "UpdateType": "Created"}]}]
        con = sqlite3.connect(db)
        refresh_state = con.execute("select state from emby_refresh_tasks").fetchone()[0]
        events = con.execute("select component,event_type from events_v2 where component in ('media_pipeline','emby') order by id").fetchall()
        con.close()
        assert refresh_state == "done"
        assert ("media_pipeline", "media_pipeline_job_processed") in events
        assert ("emby", "emby_refresh_processed") in events


def test_daemon_background_workers_include_media_promotion_worker():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.promotion import MediaPromotionRepository, MediaPromotionRunner
    from qbt_orchestrator.service import DaemonRuntime
    from tests.fakes import FakeRclone

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        daemon = DaemonRuntime(
            state_db=db,
            qbt=FakeQbt(),
            executor=FakeExecutor(),
            free_bytes_provider=lambda: 6 * 1024**3,
            dry_run=False,
            safety_interval=0,
            media_promotion_runner=MediaPromotionRunner(
                MediaPromotionRepository(db), FakeRclone()
            ),
            media_promotion_dry_run=False,
        )

        worker_names = [name for name, _callback in daemon._background_event_worker_specs()]

        assert "promotion" in worker_names


def test_daemon_media_promotion_dry_run_does_not_claim_job():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.promotion import MediaPromotionRepository, MediaPromotionRunner
    from qbt_orchestrator.service import DaemonRuntime
    from tests.fakes import FakeRclone

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        repo = MediaPromotionRepository(db, now=lambda: 1000)
        promotion_id = repo.enqueue(
            upload_job_id=1,
            hash="h1",
            media_group_id=None,
            normalized_id="ABC-123",
            metadata_title="Title",
            display_title="ABC-123 Title",
            source_remote="gcrypt:/old.mp4",
            target_remote="gcrypt:/ABC-123/ABC-123 Title.mp4",
            expected_size=10,
        )
        daemon = DaemonRuntime(
            state_db=db,
            qbt=FakeQbt(),
            executor=FakeExecutor(),
            free_bytes_provider=lambda: 6 * 1024**3,
            dry_run=False,
            safety_interval=0,
            media_promotion_runner=MediaPromotionRunner(repo, FakeRclone()),
            media_promotion_dry_run=True,
        )
        daemon.tick_safety()

        assert daemon.process_media_promotion_jobs() == 1
        assert repo.get(promotion_id)["state"] == "planned"
        assert repo.get(promotion_id)["attempts"] == 0


def test_daemon_live_promotion_moves_media_and_opens_finalization_barrier():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.promotion import MediaPromotionRepository, MediaPromotionRunner
    from qbt_orchestrator.runtime import TorrentJobRepository
    from qbt_orchestrator.service import DaemonRuntime
    from tests.fakes import FakeRclone

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        jobs = TorrentJobRepository(db, now=lambda: 1000)
        upload_id = jobs.enqueue("h1", None, "upload", {"full_torrent": True}, priority=1)
        con = sqlite3.connect(db)
        con.execute(
            "update torrent_jobs set state='promotion_wait',phase='promotion_wait' where id=?",
            (upload_id,),
        )
        con.execute(
            "insert into media_groups(id,media_group_key,normalized_id,emby_media_dir,created_at,updated_at) values(?,?,?,?,?,?)",
            (1, "ABC-123", "ABC-123", "/media/gcrypt/ABC-123", 100, 100),
        )
        con.execute(
            "insert into media_pipeline_runs(upload_manifest_id,media_group_id,state,created_at,updated_at,canonical_remote_dir,canonical_basename,canonical_video_manifest_json) "
            "values(?,?,?,?,?,?,?,?)",
            (
                f"upload-job-{upload_id}",
                1,
                "SidecarVerified",
                100,
                100,
                "gcrypt:/ABC-123",
                "ABC-123 Title",
                '[{"remote_path":"gcrypt:/ABC-123/ABC-123 Title.mp4","size":10}]',
            ),
        )
        con.commit()
        con.close()
        promotions = MediaPromotionRepository(db, now=lambda: 1000)
        promotion_id = promotions.enqueue(
            upload_job_id=upload_id,
            hash="h1",
            media_group_id=1,
            normalized_id="ABC-123",
            metadata_title="Title",
            display_title="ABC-123 Title",
            source_remote="gcrypt:/hash/raw.mp4",
            target_remote="gcrypt:/ABC-123/ABC-123 Title.mp4",
            expected_size=10,
        )
        rclone = FakeRclone(remote_sizes={"gcrypt:/hash/raw.mp4": 10})
        daemon = DaemonRuntime(
            state_db=db,
            qbt=FakeQbt(),
            executor=FakeExecutor(),
            free_bytes_provider=lambda: 6 * 1024**3,
            dry_run=False,
            safety_interval=0,
            media_promotion_runner=MediaPromotionRunner(promotions, rclone),
            media_promotion_dry_run=False,
        )
        daemon.tick_safety()

        assert daemon.process_media_promotion_jobs() == 1

        assert rclone.movetos == [
            ("gcrypt:/hash/raw.mp4", "gcrypt:/ABC-123/ABC-123 Title.mp4")
        ]
        assert promotions.get(promotion_id)["state"] == "verified"
        assert jobs.get(upload_id)["state"] == "cleanup_wait"
        con = sqlite3.connect(db)
        assert con.execute(
            "select count(*) from torrent_jobs where job_type='cleanup_full_torrent'"
        ).fetchone()[0] == 1
        assert con.execute("select count(*) from emby_refresh_tasks").fetchone()[0] == 1
        con.close()


def test_daemon_default_file_batch_loop_uses_sync_cache_and_dry_run_records_upload():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.service import DaemonRuntime

    class CompletedQbt(FakeQbt):
        def get_maindata(self, rid):
            self.rids.append(rid)
            return {
                "rid": rid + 1,
                "full_update": True,
                "torrents": {"h1": {"name": "Done", "category": "auto", "tags": "auto", "state": "uploading", "amount_left": 0, "size": 100, "progress": 1.0, "content_path": "/downloads/active/Done"}},
                "server_state": {},
            }

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        daemon = DaemonRuntime(
            state_db=db,
            qbt=CompletedQbt(),
            executor=FakeExecutor(),
            free_bytes_provider=lambda: 6 * 1024**3,
            dry_run=False,
            safety_interval=0,
            file_batch_dry_run=True,
        )

        daemon.run(max_safety_ticks=1)

        con = sqlite3.connect(db)
        jobs = con.execute("select count(*) from torrent_jobs").fetchone()[0]
        action = con.execute("select action_type,status,dry_run from action_log where action_type='enqueue_upload'").fetchone()
        loop = con.execute("select data_json from events_v2 where component='file_batch' and event_type='loop_tick' order by id desc limit 1").fetchone()[0]
        con.close()
        assert jobs == 0
        assert action == ("enqueue_upload", "dry_run", 1)
        assert "not_configured" not in loop


def test_daemon_file_batch_can_enqueue_when_explicitly_enabled():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.service import DaemonRuntime

    class CompletedQbt(FakeQbt):
        def get_maindata(self, rid):
            self.rids.append(rid)
            return {
                "rid": rid + 1,
                "full_update": True,
                "torrents": {"h2": {"name": "Done2", "category": "auto", "tags": "auto", "state": "uploading", "amount_left": 0, "size": 100, "progress": 1.0, "content_path": "/downloads/active/Done2"}},
                "server_state": {},
            }

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        daemon = DaemonRuntime(
            state_db=db,
            qbt=CompletedQbt(),
            executor=FakeExecutor(),
            free_bytes_provider=lambda: 6 * 1024**3,
            dry_run=False,
            safety_interval=0,
            file_batch_dry_run=False,
        )

        daemon.run(max_safety_ticks=1)

        con = sqlite3.connect(db)
        job = con.execute("select hash,job_type,state from torrent_jobs").fetchone()
        con.close()
        assert job == ("h2", "upload", "queued")


def test_daemon_file_batch_loop_creates_pipeline_batch_from_qbt_file_list():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.service import DaemonRuntime

    gib = 1024**3

    class BatchQbt(FakeQbt):
        def __init__(self):
            super().__init__()
            self.posts = []

        def get_maindata(self, rid):
            self.rids.append(rid)
            return {
                "rid": rid + 1,
                "full_update": True,
                "torrents": {
                    "big": {
                        "name": "Big",
                        "category": "auto",
                        "tags": "auto",
                        "state": "stoppedDL",
                        "amount_left": 2 * gib,
                        "size": 2 * gib,
                        "progress": 0.0,
                    }
                },
                "server_state": {},
            }

        def torrent_files(self, hash):
            return [{"index": 0, "name": "A.mp4", "size": gib, "progress": 0, "priority": 0}]

        def torrent_properties(self, hash):
            return {"piece_size": 16 * 1024**2}

        def qbt_post(self, path, payload):
            self.posts.append((path, payload))

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        qbt = BatchQbt()
        daemon = DaemonRuntime(
            state_db=db,
            qbt=qbt,
            executor=qbt,
            free_bytes_provider=lambda: 6 * gib,
            dry_run=False,
            safety_interval=0,
            file_batch_dry_run=False,
            batch_pipeline_enabled=True,
        )

        daemon.run(max_safety_ticks=1)

        assert ("/api/v2/torrents/filePrio", {"hash": "big", "id": "0", "priority": "1"}) in qbt.posts
        con = sqlite3.connect(db)
        batch = con.execute("select hash,state,indices_json from torrent_batches").fetchone()
        reservation = con.execute("select hash,kind,state from resource_reservations where kind='batch'").fetchone()
        loop = con.execute("select data_json from events_v2 where component='file_batch' and event_type='loop_tick' order by id desc limit 1").fetchone()[0]
        con.close()
        assert batch == ("big", "downloading", "[0]")
        assert reservation == ("big", "batch", "active")
        assert '"batches_created": 1' in loop


def test_daemon_drain_file_batch_does_not_call_torrent_files():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.service import DaemonRuntime

    gib = 1024**3

    class DrainQbt(FakeQbt):
        def __init__(self):
            super().__init__()
            self.heavy_calls = []

        def get_maindata(self, rid):
            self.rids.append(rid)
            return {
                "rid": rid + 1,
                "full_update": True,
                "torrents": {
                    "big": {
                        "name": "Big",
                        "category": "auto",
                        "tags": "auto",
                        "state": "stoppedDL",
                        "amount_left": 5 * gib,
                        "size": 5 * gib,
                        "progress": 0.0,
                    }
                },
                "server_state": {},
            }

        def torrent_files(self, hash):
            self.heavy_calls.append(("torrent_files", hash))
            return [{"index": 0, "name": "A.mp4", "size": 5 * gib, "progress": 0.0, "priority": 0}]

        def torrent_properties(self, hash):
            self.heavy_calls.append(("torrent_properties", hash))
            return {"piece_size": 16 * 1024**2}

    class RecordingJunkJanitor:
        def __init__(self):
            self.file_lists = None

        def reconcile(self, snapshots, file_lists, sync_healthy):
            self.file_lists = file_lists
            return {"scanned": len(snapshots)}

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        qbt = DrainQbt()
        junk_janitor = RecordingJunkJanitor()
        daemon = DaemonRuntime(
            state_db=db,
            qbt=qbt,
            executor=FakeExecutor(),
            free_bytes_provider=lambda: int(2.2 * gib),
            dry_run=False,
            safety_interval=0,
            file_batch_dry_run=False,
            batch_pipeline_enabled=True,
            disk_floor_bytes=3 * gib,
            junk_janitor=junk_janitor,
        )
        daemon.tick_safety()

        result = daemon.file_batch_tick()

        assert qbt.heavy_calls == []
        assert result["batches_created"] == 0
        assert result["batches_blocked"] == 1
        assert result["blocked_reasons"] == {"mode_disallows_batch": 1}
        assert junk_janitor.file_lists == {}


def test_daemon_file_batch_live_bypasses_backpressure_for_disk_releasing_upload():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.io_governor import UploadBackpressurePolicy
    from qbt_orchestrator.maintenance import SQLiteMaintenanceService
    from qbt_orchestrator.runtime import TorrentJobRepository
    from qbt_orchestrator.service import DaemonRuntime

    class CompletedQbt(FakeQbt):
        def get_maindata(self, rid):
            self.rids.append(rid)
            return {
                "rid": rid + 1,
                "full_update": True,
                "torrents": {
                    "new": {
                        "name": "New",
                        "category": "auto",
                        "tags": "auto",
                        "state": "uploading",
                        "amount_left": 0,
                        "size": 100,
                        "progress": 1.0,
                        "content_path": "/downloads/active/New",
                    }
                },
                "server_state": {},
            }

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        TorrentJobRepository(db, now=lambda: 1000).enqueue(
            "old",
            None,
            "upload",
            {"local": "/tmp/old", "remote": "gcrypt:/old", "size": 21 * 1024**3, "full_torrent": True},
            priority=1,
        )
        daemon = DaemonRuntime(
            state_db=db,
            qbt=CompletedQbt(),
            executor=FakeExecutor(),
            free_bytes_provider=lambda: 6 * 1024**3,
            dry_run=False,
            safety_interval=0,
            file_batch_dry_run=False,
            upload_backpressure_policy=UploadBackpressurePolicy(max_backlog_bytes=20 * 1024**3, now=lambda: 5000),
            maintenance_service=SQLiteMaintenanceService(db, now=lambda: 5000, retention_days=5),
        )

        daemon.run(max_safety_ticks=1)

        con = sqlite3.connect(db)
        jobs = con.execute("select hash from torrent_jobs order by id").fetchall()
        event = con.execute("select component,event_type from events_v2 where component='upload_backpressure' order by id desc limit 1").fetchone()
        con.close()
        assert jobs == [("old",), ("new",)]
        assert event is None



def test_tag_pending_does_not_block_startup_or_unrelated_planner_selection():
    """tag_pending is nonblocking: other hashes still advance planner generation."""
    from qbt_orchestrator.service import (
        CAPACITY_RECOVERY_PENDING_STATES,
        DaemonRuntime,
    )
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.soak_queue import SoakQueueConfig

    class SnapshotQbt(FakeQbt):
        def get_maindata(self, rid):
            self.rids.append(rid)
            return {
                "rid": rid + 1,
                "full_update": True,
                "torrents": {
                    "stuck": {
                        "hash": "stuck",
                        "name": "Stuck",
                        "category": "auto",
                        "tags": "auto",
                        "state": "stoppedDL",
                        "amount_left": 900,
                        "size": 1000,
                        "progress": 0.1,
                        "num_seeds": 1,
                    },
                    "safe": {
                        "hash": "safe",
                        "name": "Safe",
                        "category": "auto",
                        "tags": "auto",
                        "state": "stoppedDL",
                        "amount_left": 9,
                        "size": 10,
                        "progress": 0.9,
                        "num_seeds": 1,
                    },
                },
                "server_state": {},
            }

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        con.execute(
            "insert into capacity_reclaims("
            "reclaim_key,hash,name,magnet_uri,host_path,content_path,state,"
            "capacity_generation,recheck_state,recheck_error,created_at,updated_at) "
            "values(?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "stuck:1",
                "stuck",
                "Stuck",
                "magnet:?xt=stuck",
                str(Path(td) / "stuck"),
                "/downloads/incomplete/stuck",
                "tag_pending",
                4,
                "requested",
                "post_reclaim_tag_failed:fixture",
                1,
                1,
            ),
        )
        con.commit()
        con.close()

        assert "tag_pending" not in CAPACITY_RECOVERY_PENDING_STATES

        daemon = DaemonRuntime(
            state_db=db,
            qbt=SnapshotQbt(),
            executor=FakeExecutor(),
            free_bytes_provider=lambda: 20 * 1024**3,
            dry_run=False,
            safety_interval=0,
            planner_dry_run=False,
            planner_active_slots=1,
            soak_enabled=True,
            soak_dry_run=False,
            soak_config=SoakQueueConfig(
                resident_slots=1,
                min_free_bytes=0,
                disk_floor_bytes=0,
                max_qbt_active_downloads=16,
            ),
        )
        daemon.tick_safety()
        result = daemon.planner_tick()

        assert result["planner"]["plan_generation"] == 1
        assert "safe" in result["planner"]["selected_hashes"]
        assert "stuck" not in result["planner"]["selected_hashes"]
        assert "stuck" not in result["soak_queue"]["started"]
        con = sqlite3.connect(db)
        state = con.execute(
            "select state from capacity_reclaims where hash='stuck'"
        ).fetchone()[0]
        con.close()
        assert state == "tag_pending"


def test_tag_pending_nonblocking_retry_failure_keeps_other_hashes_schedulable(
    tmp_path,
):
    """Continuous addTags failure stays fenced on stuck; planner still picks safe."""
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.executor import Executor
    from qbt_orchestrator.service import DaemonRuntime
    from qbt_orchestrator.soak_queue import SoakQueueConfig

    class FailAddTagsQbt(FakeQbt):
        def __init__(self):
            super().__init__()
            self.posts = []

        def get_maindata(self, rid):
            self.rids.append(rid)
            return {
                "rid": rid + 1,
                "full_update": True,
                "torrents": {
                    "stuck": {
                        "hash": "stuck",
                        "name": "Stuck",
                        "category": "auto",
                        "tags": "auto",
                        "state": "stoppedDL",
                        "amount_left": 900,
                        "size": 1000,
                        "progress": 0.1,
                        "num_seeds": 1,
                    },
                    "safe": {
                        "hash": "safe",
                        "name": "Safe",
                        "category": "auto",
                        "tags": "auto",
                        "state": "stoppedDL",
                        "amount_left": 9,
                        "size": 10,
                        "progress": 0.9,
                        "num_seeds": 1,
                    },
                },
                "server_state": {},
            }

        def post(self, path, payload):
            self.posts.append((path, dict(payload)))
            if str(path).endswith("/addTags"):
                raise RuntimeError("addTags unavailable")
            return True

        def torrent_info(self, torrent_hash, timeout=None):
            return {
                "hash": str(torrent_hash),
                "state": "stoppedDL",
                "tags": "auto",
            }

    db = tmp_path / "state.sqlite"
    managed = tmp_path / "incomplete"
    managed.mkdir()
    migrate(db, dry_run=False)
    con = sqlite3.connect(db)
    con.execute(
        "insert into capacity_assessment_state("
        "id,current_generation,observed_at,summary_json) values(1,4,1,'{}')"
    )
    con.execute(
        "insert into capacity_reclaims("
        "reclaim_key,hash,name,magnet_uri,host_path,content_path,state,"
        "capacity_generation,recheck_state,recheck_error,created_at,updated_at) "
        "values(?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            "stuck:1",
            "stuck",
            "Stuck",
            "magnet:?xt=stuck",
            str(managed / "stuck"),
            "/downloads/incomplete/stuck",
            "tag_pending",
            4,
            "requested",
            "post_reclaim_tag_failed:fixture",
            1,
            1,
        ),
    )
    con.commit()
    con.close()

    qbt = FailAddTagsQbt()
    executor = Executor(qbt, dry_run=False, state_db=db)
    recovery = DeadPartialReclaimer(
        db,
        executor,
        host_downloads=tmp_path,
        container_downloads="/downloads",
        managed_root=managed,
        dry_run=False,
        max_per_tick=0,
        disk_free_bytes=lambda _path: 0,
        notification_chat_ids=["100"],
        now=lambda: 5_000,
    )
    daemon = DaemonRuntime(
        state_db=db,
        qbt=qbt,
        executor=executor,
        free_bytes_provider=lambda: 20 * 1024**3,
        dry_run=False,
        safety_interval=0,
        planner_dry_run=False,
        planner_active_slots=1,
        soak_enabled=True,
        soak_dry_run=False,
        soak_config=SoakQueueConfig(
            resident_slots=1,
            min_free_bytes=0,
            disk_floor_bytes=0,
            max_qbt_active_downloads=16,
        ),
        capacity_recovery_reclaimer=recovery,
    )
    try:
        daemon.tick_safety()
        result = daemon.planner_tick()

        con = sqlite3.connect(db)
        stuck_state = con.execute(
            "select state from capacity_reclaims where hash='stuck'"
        ).fetchone()[0]
        con.close()
        assert stuck_state == "tag_pending"
        assert result["planner"]["plan_generation"] == 1
        assert "safe" in result["planner"]["selected_hashes"]
        assert "stuck" not in result["planner"]["selected_hashes"]
        assert "stuck" not in result["soak_queue"]["started"]
        assert any(path.endswith("/addTags") for path, _ in qbt.posts)
        assert not any(
            path.endswith("/start") and payload.get("hashes") == "stuck"
            for path, payload in qbt.posts
        )
        assert any(
            path.endswith("/start") and payload.get("hashes") == "safe"
            for path, payload in qbt.posts
        )
    finally:
        executor.close(timeout=1)


def test_path_conflict_reclaim_fence_does_not_abort_planner_tick_via_soak():
    """Durable partial_or_unknown must not kill planner_tick through SoakQueue.

    Reproduction: path_conflict_manual fence + stopped auto torrent used to reach
    SoakQueue reservation sync, raise CapacityReclaimLockedError, and abort the
    whole planner generation for unrelated torrents.
    """
    from qbt_orchestrator.capacity_reclaim import PATH_CONFLICT_MANUAL
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.service import DaemonRuntime
    from qbt_orchestrator.soak_queue import SoakQueueConfig

    class SnapshotQbt(FakeQbt):
        def get_maindata(self, rid):
            self.rids.append(rid)
            return {
                "rid": rid + 1,
                "full_update": True,
                "torrents": {
                    "h": {
                        "hash": "h",
                        "name": "Conflict",
                        "category": "auto",
                        "tags": "auto",
                        "state": "stoppedDL",
                        "amount_left": 900,
                        "size": 1000,
                        "progress": 0.1,
                        "num_seeds": 1,
                    },
                    "safe": {
                        "hash": "safe",
                        "name": "Safe",
                        "category": "auto",
                        "tags": "auto",
                        "state": "stoppedDL",
                        "amount_left": 9,
                        "size": 10,
                        "progress": 0.9,
                        "num_seeds": 1,
                    },
                },
                "server_state": {},
            }

    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        db = root / "state.sqlite"
        managed = root / "incomplete"
        conflict_payload = managed / "h"
        quarantine_payload = (
            managed / ".qbt-orchestrator-reclaim" / "reclaim-1"
        )
        conflict_payload.mkdir(parents=True)
        quarantine_payload.mkdir(parents=True)
        (conflict_payload / "part").write_bytes(b"x" * 4096)
        (quarantine_payload / "part").write_bytes(b"y" * 4096)
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        con.execute(
            "insert into capacity_reclaims("
            "reclaim_key,hash,name,magnet_uri,host_path,content_path,state,"
            "capacity_generation,recheck_error,created_at,updated_at) "
            "values(?,?,?,?,?,?,?,?,?,?,?)",
            (
                "h:1",
                "h",
                "Conflict",
                "magnet:?xt=h",
                str(conflict_payload.resolve()),
                "/downloads/incomplete/h",
                "partial_or_unknown",
                4,
                PATH_CONFLICT_MANUAL,
                1,
                1,
            ),
        )
        con.commit()
        con.close()

        executor = FakeExecutor()
        daemon = DaemonRuntime(
            state_db=db,
            qbt=SnapshotQbt(),
            executor=executor,
            free_bytes_provider=lambda: 20 * 1024**3,
            dry_run=False,
            safety_interval=0,
            planner_dry_run=False,
            planner_active_slots=1,
            soak_enabled=True,
            soak_dry_run=False,
            soak_config=SoakQueueConfig(
                resident_slots=1,
                min_free_bytes=0,
                disk_floor_bytes=0,
                max_qbt_active_downloads=16,
            ),
        )
        daemon.tick_safety()
        result = daemon.planner_tick()

        assert "planner" in result
        assert result["planner"]["plan_generation"] == 1
        assert "h" not in result["soak_queue"]["started"]
        assert "h" not in result["planner"]["selected_hashes"]
        assert result["soak_queue"]["started"] == ["safe"]
        assert result["planner"]["selected_hashes"] == ["safe"]
        assert ("/api/v2/torrents/start", {"hashes": "safe"}) in executor.posts
        assert ("/api/v2/torrents/start", {"hashes": "h"}) not in executor.posts
        con = sqlite3.connect(db)
        row = con.execute(
            "select state, recheck_error from capacity_reclaims where hash='h'"
        ).fetchone()
        con.close()
        assert row == ("partial_or_unknown", PATH_CONFLICT_MANUAL)


def test_runtime_planner_tick_runs_soak_queue_before_planner_and_protects_residents():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.service import DaemonRuntime
    from qbt_orchestrator.soak_queue import SoakQueueConfig

    class SeqExecutor(FakeExecutor):
        def __init__(self):
            super().__init__()
            self.seq = []
        def set_seq_dl(self, hash, desired):
            self.seq.append((hash, desired))
            return True

    class SnapshotQbt(FakeQbt):
        def get_maindata(self, rid):
            self.rids.append(rid)
            return {
                "rid": rid + 1,
                "full_update": True,
                "torrents": {
                    "active": {"hash": "active", "name": "Active", "category": "auto", "tags": "auto", "state": "pausedDL", "amount_left": 1, "size": 10, "progress": 0.1, "num_seeds": 1},
                    "soak": {"hash": "soak", "name": "Soak", "category": "auto", "tags": "auto", "state": "pausedDL", "amount_left": 9, "size": 10, "progress": 0.9, "num_seeds": 1},
                },
                "server_state": {},
            }

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        executor = SeqExecutor()
        daemon = DaemonRuntime(
            state_db=db,
            qbt=SnapshotQbt(),
            executor=executor,
            free_bytes_provider=lambda: 20 * 1024**3,
            dry_run=False,
            safety_interval=0,
            planner_dry_run=False,
            planner_active_slots=1,
            soak_enabled=True,
            soak_dry_run=False,
            soak_config=SoakQueueConfig(resident_slots=1, min_free_bytes=0, disk_floor_bytes=0, max_qbt_active_downloads=16),
        )
        daemon.tick_safety()
        result = daemon.planner_tick()

        assert result["soak_queue"]["started"] == ["soak"]
        assert result["planner"]["selected_hashes"] == ["soak"]
        assert result["planner"]["plan_generation"] == 1
        assert ("/api/v2/torrents/start", {"hashes": "soak"}) in executor.posts
        assert ("/api/v2/torrents/start", {"hashes": "active"}) not in executor.posts
        assert executor.seq == [("active", False), ("soak", False)]


def test_runtime_planner_tick_protects_active_pipeline_batch_from_pause():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.service import DaemonRuntime

    gib = 1024**3

    class BatchSnapshotQbt(FakeQbt):
        def get_maindata(self, rid):
            self.rids.append(rid)
            return {
                "rid": rid + 1,
                "full_update": True,
                "torrents": {
                    "batch": {"hash": "batch", "name": "Batch", "category": "auto", "tags": "auto", "state": "downloading", "amount_left": 3 * gib, "size": 5 * gib, "progress": 0.4},
                    "tiny": {"hash": "tiny", "name": "Tiny", "category": "auto", "tags": "auto", "state": "stoppedDL", "amount_left": 1, "size": 10, "progress": 0.9},
                },
                "server_state": {},
            }

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        con.execute(
            "insert into torrent_batches(id,hash,batch_no,state,mode,indices_json,total_bytes,reserved_bytes,created_at,updated_at) values(?,?,?,?,?,?,?,?,?,?)",
            (1, "batch", 1, "downloading", "pipeline", "[0]", 5 * gib, 3 * gib, 900, 900),
        )
        con.execute(
            "insert into resource_reservations(hash,batch_id,kind,bytes,state,created_at,expires_at,reason) values(?,?,?,?,?,?,?,?)",
            ("batch", 1, "batch", 3 * gib, "active", 900, None, "batch_pipeline_reserved"),
        )
        con.execute(
            "insert into scheduler_intents(component,hash,intent,priority,expires_at,data_json) values(?,?,?,?,?,?)",
            ("batch", "batch", "protect_batch", 20, None, '{"batch_id":1}'),
        )
        con.commit(); con.close()
        executor = FakeExecutor()
        daemon = DaemonRuntime(
            state_db=db,
            qbt=BatchSnapshotQbt(),
            executor=executor,
            free_bytes_provider=lambda: 6 * gib,
            dry_run=False,
            safety_interval=0,
            planner_dry_run=False,
            planner_active_slots=1,
            disk_floor_bytes=2 * gib,
        )
        daemon.tick_safety()

        result = daemon.planner_tick()

        assert result["planner"]["selected_hashes"] == ["batch"]
        assert ("/api/v2/torrents/start", {"hashes": "tiny"}) not in executor.posts
        assert not any(path == "/api/v2/torrents/stop" and payload == {"hashes": "batch"} for path, payload in executor.posts)


def test_cli_builds_soak_queue_from_env_with_live_defaults():
    import argparse
    import os
    from qbt_orchestrator.cli import _build_runtime
    from qbt_orchestrator.db import migrate

    keys = [
        "QBT_ORCH_STATE_DB", "QBT_ORCH_DRY_RUN", "QBT_ORCH_PLANNER_DRY_RUN", "QBT_ORCH_SOAK_ENABLED",
        "QBT_ORCH_SOAK_DRY_RUN", "QBT_ORCH_SOAK_RESIDENT_SLOTS", "QBT_ORCH_SOAK_MIN_FREE_GB",
        "QBT_ORCH_SOAK_ALLOWED_MODES", "QBT_ORCH_SOAK_REQUIRE_SWARM", "QBT_ORCH_MAX_COLD_PARTIAL_GB",
        "QBT_ORCH_MAX_COLD_PARTIAL_TORRENTS", "QBT_ORCH_SOAK_MAX_NEW_PER_HOUR",
        "QBT_ORCH_SOAK_MAX_EXPOSURE_GB", "QBT_ORCH_SOAK_MAX_PER_TORRENT_EXPOSURE_MB",
        "QBT_ORCH_DISK_FLOOR_GB", "QBT_ORCH_SOAK_LOW_CAPACITY_THROTTLE_MARGIN_GB",
        "QBT_ORCH_SOAK_LOW_CAPACITY_LIMIT_BPS",
        "QBT_ORCH_DISK_PATH", "QBT_ORCH_ORPHAN_JANITOR", "QBT_ORCH_JUNK_JANITOR", "QBT_ORCH_CAROUSEL",
        "QBT_ORCH_QBT_PREFERENCES_GUARD", "QBT_ORCH_PATH_RECONCILE",
        "QBT_ORCH_DRAIN_EXIT_GB", "QBT_ORCH_EXPLORE_ENTER_GB", "QBT_ORCH_CAPACITY_DEADLOCK_ALERTS",
        "QBT_ORCH_SCHEDULER_ENGINE",
        "QBT_ORCH_FINISH_RESIDENT_MAX_STALL_SEC", "QBT_ORCH_CAPACITY_VIABILITY_STALE_SEC",
    ]
    old = {k: os.environ.get(k) for k in keys}
    try:
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / "state.sqlite"
            migrate(db, dry_run=False)
            os.environ.update({
                "QBT_ORCH_STATE_DB": str(db),
                "QBT_ORCH_DRY_RUN": "0",
                "QBT_ORCH_PLANNER_DRY_RUN": "0",
                "QBT_ORCH_SOAK_ENABLED": "1",
                "QBT_ORCH_SOAK_DRY_RUN": "0",
                "QBT_ORCH_SOAK_RESIDENT_SLOTS": "8",
                "QBT_ORCH_SOAK_ALLOWED_MODES": "normal,explore",
                "QBT_ORCH_SOAK_REQUIRE_SWARM": "1",
                "QBT_ORCH_MAX_COLD_PARTIAL_GB": "4",
                "QBT_ORCH_MAX_COLD_PARTIAL_TORRENTS": "8",
                "QBT_ORCH_SOAK_MAX_NEW_PER_HOUR": "4",
                "QBT_ORCH_SOAK_MIN_FREE_GB": "8",
                "QBT_ORCH_DISK_FLOOR_GB": "3",
                "QBT_ORCH_SOAK_MAX_EXPOSURE_GB": "4",
                "QBT_ORCH_SOAK_MAX_PER_TORRENT_EXPOSURE_MB": "512",
                "QBT_ORCH_SOAK_LOW_CAPACITY_THROTTLE_MARGIN_GB": "1",
                "QBT_ORCH_SOAK_LOW_CAPACITY_LIMIT_BPS": "262144",
                "QBT_ORCH_DISK_PATH": td,
                "QBT_ORCH_ORPHAN_JANITOR": "0",
                "QBT_ORCH_JUNK_JANITOR": "0",
                "QBT_ORCH_CAROUSEL": "0",
                "QBT_ORCH_QBT_PREFERENCES_GUARD": "0",
                "QBT_ORCH_PATH_RECONCILE": "0",
                "QBT_ORCH_DRAIN_EXIT_GB": "5.5",
                "QBT_ORCH_EXPLORE_ENTER_GB": "9",
                "QBT_ORCH_CAPACITY_DEADLOCK_ALERTS": "0",
                "QBT_ORCH_SCHEDULER_ENGINE": "shadow",
                "QBT_ORCH_FINISH_RESIDENT_MAX_STALL_SEC": "2400",
                "QBT_ORCH_CAPACITY_VIABILITY_STALE_SEC": "2100",
            })
            ns = argparse.Namespace(cmd="daemon", dry_run=False, config=None, safety_interval=0, max_safety_ticks=1)
            runtime, dry_run = _build_runtime(ns, db)

            assert dry_run is False
            assert runtime.planner_dry_run is False
            assert runtime.soak_queue_service is not None
            assert runtime.soak_queue_service.dry_run is False
            assert runtime.soak_queue_service.config.resident_slots == 8
            assert runtime.soak_queue_service.config.allowed_modes == ("normal", "explore")
            assert runtime.soak_queue_service.config.require_swarm is True
            assert runtime.soak_queue_service.config.max_cold_partial_bytes == 4 * 1024**3
            assert runtime.soak_queue_service.config.max_cold_partial_torrents == 8
            assert runtime.soak_queue_service.config.max_new_per_hour == 4
            assert runtime.soak_queue_service.config.min_free_bytes == 8 * 1024**3
            assert runtime.soak_queue_service.config.disk_floor_bytes == 3 * 1024**3
            assert runtime.soak_queue_service.config.low_capacity_throttle_margin_bytes == 1024**3
            assert runtime.soak_queue_service.config.low_capacity_soak_limit_bps == 262144
            assert runtime.soak_queue_service.config.max_total_exposure_bytes == 4 * 1024**3
            assert runtime.soak_queue_service.config.max_per_torrent_exposure_bytes == 512 * 1024**2
            assert runtime.disk_floor_bytes == 3 * 1024**3
            assert runtime.drain_exit_bytes == int(5.5 * 1024**3)
            assert runtime.explore_enter_bytes == 9 * 1024**3
            assert runtime.capacity_deadlock_alerts_enabled is False
            assert runtime.scheduler_engine_mode == "shadow"
            assert runtime.finish_resident_max_stall_sec == 2400
            assert runtime.capacity_viability_stale_sec == 2100
    finally:
        for k, v in old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def test_cli_builds_qbt_host_http_client_from_env():
    import argparse
    import os
    from qbt_orchestrator.cli import _build_runtime
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.integrations.qbt import QbtHttpClient

    keys = [
        "QBT_ORCH_STATE_DB", "QBT_ORCH_DRY_RUN", "QBT_ORCH_PLANNER_DRY_RUN",
        "QBT_ORCH_QBT_API_MODE", "QBT_ORCH_QBT_API_BASE", "QBT_ORCH_QBT_USERNAME",
        "QBT_ORCH_QBT_PASSWORD", "QBT_ORCH_QBT_API_TIMEOUT_SEC", "QBT_ORCH_QBT_API_MAX_RPS",
        "QBT_ORCH_DISK_PATH", "QBT_ORCH_ORPHAN_JANITOR", "QBT_ORCH_JUNK_JANITOR", "QBT_ORCH_CAROUSEL",
        "QBT_ORCH_QBT_PREFERENCES_GUARD", "QBT_ORCH_PATH_RECONCILE", "QBT_ORCH_SOAK_ENABLED",
    ]
    old = {k: os.environ.get(k) for k in keys}
    try:
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / "state.sqlite"
            migrate(db, dry_run=False)
            os.environ.update({
                "QBT_ORCH_STATE_DB": str(db),
                "QBT_ORCH_DRY_RUN": "0",
                "QBT_ORCH_PLANNER_DRY_RUN": "1",
                "QBT_ORCH_QBT_API_MODE": "host",
                "QBT_ORCH_QBT_API_BASE": "http://127.0.0.1:8081",
                "QBT_ORCH_QBT_USERNAME": "admin",
                "QBT_ORCH_QBT_PASSWORD": "secret",
                "QBT_ORCH_QBT_API_TIMEOUT_SEC": "7",
                "QBT_ORCH_QBT_API_MAX_RPS": "2",
                "QBT_ORCH_DISK_PATH": td,
                "QBT_ORCH_ORPHAN_JANITOR": "0",
                "QBT_ORCH_JUNK_JANITOR": "0",
                "QBT_ORCH_CAROUSEL": "0",
                "QBT_ORCH_QBT_PREFERENCES_GUARD": "0",
                "QBT_ORCH_PATH_RECONCILE": "0",
                "QBT_ORCH_SOAK_ENABLED": "0",
            })

            runtime, dry_run = _build_runtime(argparse.Namespace(cmd="daemon", dry_run=False, config=None, safety_interval=0, max_safety_ticks=1), db)

            assert dry_run is False
            assert isinstance(runtime.qbt, QbtHttpClient)
            assert runtime.qbt.api_base == "http://127.0.0.1:8081"
            assert runtime.qbt.username == "admin"
            assert runtime.qbt.password == "secret"
            assert runtime.qbt.timeout == 7
            assert runtime.qbt.rate_limiter.rate_per_sec == 2
    finally:
        for k, v in old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def test_cli_builds_qbt_host_proxy_client_without_auth_from_env():
    import argparse
    import os
    from qbt_orchestrator.cli import _build_runtime
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.integrations.qbt import QbtHttpClient

    keys = [
        "QBT_ORCH_STATE_DB", "QBT_ORCH_DRY_RUN", "QBT_ORCH_PLANNER_DRY_RUN",
        "QBT_ORCH_QBT_API_MODE", "QBT_ORCH_QBT_API_BASE", "QBT_ORCH_QBT_USERNAME",
        "QBT_ORCH_QBT_PASSWORD", "QBT_ORCH_QBT_API_TIMEOUT_SEC", "QBT_ORCH_QBT_API_MAX_RPS",
        "QBT_ORCH_DISK_PATH", "QBT_ORCH_ORPHAN_JANITOR", "QBT_ORCH_JUNK_JANITOR", "QBT_ORCH_CAROUSEL",
        "QBT_ORCH_QBT_PREFERENCES_GUARD", "QBT_ORCH_PATH_RECONCILE", "QBT_ORCH_SOAK_ENABLED",
    ]
    old = {k: os.environ.get(k) for k in keys}
    try:
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / "state.sqlite"
            migrate(db, dry_run=False)
            os.environ.update({
                "QBT_ORCH_STATE_DB": str(db),
                "QBT_ORCH_DRY_RUN": "0",
                "QBT_ORCH_PLANNER_DRY_RUN": "1",
                "QBT_ORCH_QBT_API_MODE": "host-proxy",
                "QBT_ORCH_QBT_API_BASE": "http://127.0.0.1:18081",
                "QBT_ORCH_QBT_USERNAME": "admin",
                "QBT_ORCH_QBT_PASSWORD": "secret",
                "QBT_ORCH_QBT_API_TIMEOUT_SEC": "7",
                "QBT_ORCH_QBT_API_MAX_RPS": "2",
                "QBT_ORCH_DISK_PATH": td,
                "QBT_ORCH_ORPHAN_JANITOR": "0",
                "QBT_ORCH_JUNK_JANITOR": "0",
                "QBT_ORCH_CAROUSEL": "0",
                "QBT_ORCH_QBT_PREFERENCES_GUARD": "0",
                "QBT_ORCH_PATH_RECONCILE": "0",
                "QBT_ORCH_SOAK_ENABLED": "0",
            })

            runtime, dry_run = _build_runtime(argparse.Namespace(cmd="daemon", dry_run=False, config=None, safety_interval=0, max_safety_ticks=1), db)

            assert dry_run is False
            assert isinstance(runtime.qbt, QbtHttpClient)
            assert runtime.qbt.api_base == "http://127.0.0.1:18081"
            assert runtime.qbt.auth_mode == "none"
            assert runtime.qbt.auth_enabled is False
            assert runtime.qbt.username == ""
            assert runtime.qbt.password == ""
            assert runtime.qbt.default_headers == {"Host": "127.0.0.1:8080"}
            assert runtime.qbt.timeout == 7
            assert runtime.qbt.rate_limiter.rate_per_sec == 2
    finally:
        for k, v in old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


if __name__ == "__main__":
    inspect = __import__("inspect")
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn) and not inspect.signature(fn).parameters:
            fn()
    print("ok")


def test_qbt_tag_janitor_loop_defaults_and_shared_gateway(tmp_path, monkeypatch):
    from qbt_orchestrator.cli import _build_runtime

    for name, value in {
        "QBT_ORCH_STATE_DB": str(tmp_path / "cli.sqlite"),
        "QBT_ORCH_DRY_RUN": "0",
        "QBT_ORCH_DISK_PATH": str(tmp_path),
        "QBT_ORCH_ORPHAN_JANITOR": "0",
        "QBT_ORCH_JUNK_JANITOR": "0",
        "QBT_ORCH_CAROUSEL": "0",
        "QBT_ORCH_QBT_PREFERENCES_GUARD": "0",
        "QBT_ORCH_PATH_RECONCILE": "0",
        "QBT_ORCH_SOAK_ENABLED": "0",
        "QBT_ORCH_METADATA_PROBE_ENABLED": "1",
        "QBT_ORCH_CHECKED_ADD_ENABLED": "1",
        "QBT_ORCH_ADD_TAG_GC_ENABLED": "1",
        "QBT_ORCH_ADD_TAG_GC_DRY_RUN": "1",
        "QBT_ORCH_ADD_TAG_GC_INTERVAL_SEC": "300",
        "QBT_ORCH_ADD_TAG_GC_BATCH_LIMIT": "25",
        "QBT_ORCH_FILENAME_NORMALIZE": "0",
        "QBT_ORCH_CAPACITY_RECLAIM": "0",
    }.items():
        monkeypatch.setenv(name, value)

    class Ns:
        config = None
        cmd = "daemon"
        safety_interval = 0
        max_safety_ticks = 1
        dry_run = False

    runtime, _ = _build_runtime(Ns(), tmp_path / "fallback.sqlite")
    assert runtime.checked_add_service is not None
    assert runtime.metadata_probe_coordinator is not None
    assert runtime.qbt_tag_janitor is not None
    assert runtime.qbt_tag_janitor.dry_run is True
    assert runtime.qbt_tag_janitor.batch_limit == 25
    assert runtime.checked_add_service.gateway is runtime.metadata_probe_coordinator.gateway
    assert runtime.qbt_tag_janitor.gateway is runtime.checked_add_service.gateway
    tasks = {task.name: task for task in runtime.loop_tasks}
    assert tasks["checked_add"].interval_sec == 5
    assert tasks["qbt_tag_janitor"].interval_sec == 300


def test_qbt_tag_janitor_tick_suspends_when_sync_unhealthy(tmp_path):
    from qbt_orchestrator.service import DaemonRuntime

    class Qbt:
        def get_maindata(self, rid):
            return {"rid": rid + 1, "full_update": True, "torrents": {}, "server_state": {}}

    class Executor:
        def qbt_post(self, _path, _payload):
            return True

    class Janitor:
        def __init__(self):
            self.calls = []

        def tick(self, snapshots=None, *, sync_healthy=True):
            self.calls.append((sync_healthy, dict(snapshots or {})))
            return {"status": "suspended" if not sync_healthy else "ok"}

    janitor = Janitor()
    daemon = DaemonRuntime(
        state_db=tmp_path / "runtime.sqlite",
        qbt=Qbt(),
        executor=Executor(),
        free_bytes_provider=lambda: 10 * 1024**3,
        dry_run=False,
        carousel_enabled=False,
        qbt_tag_janitor=janitor,
        qbt_tag_janitor_interval_sec=300,
    )
    daemon.monitor.sync.high_risk_actions_allowed = False
    assert daemon.qbt_tag_janitor_tick()["status"] == "suspended"
    assert janitor.calls[-1][0] is False
    assert "qbt_tag_janitor" in {task.name for task in daemon.loop_tasks}
