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


def _seed_dead_allocations(db: Path, hashes: list[str]) -> None:
    con = sqlite3.connect(db)
    for h in hashes:
        con.execute(
            "insert into scheduler_allocations(hash,desired_state,applied_state,slot_kind,allocated_at,reason) values(?,?,?,?,?,?)",
            (h, "dead", "dead", "dead", 100, "test_dead"),
        )
    con.commit()
    con.close()


class RecordingExecutor:
    def __init__(self):
        self.posts = []
        self.seq = []

    def qbt_post(self, path, payload):
        self.posts.append((path, payload))

    def set_seq_dl(self, hash, desired):
        self.seq.append((hash, desired))
        return True


def test_carousel_service_starts_at_most_three_dead_probes_and_disables_seq_dl():
    from qbt_orchestrator.carousel import CarouselService
    from qbt_orchestrator.db import migrate

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        _seed_dead_allocations(db, ["h1", "h2", "h3", "h4"])
        executor = RecordingExecutor()
        svc = CarouselService(db, executor, dry_run=False, concurrency=3, now=lambda: 1000)
        snapshots = {h: {"hash": h, "category": "auto", "amount_left": 1, "num_seeds": 0, "num_peers": 0} for h in ["h1", "h2", "h3", "h4"]}

        result = svc.run_once(snapshots, sync_healthy=True)

        assert result["started"] == ["h1", "h2", "h3"]
        assert result["active_probes"] == 3
        assert executor.posts == [("/api/v2/torrents/start", {"hashes": "h1|h2|h3"})]
        assert executor.seq == [("h1", False), ("h2", False), ("h3", False)]
        states = _rows(db, "select hash,state,probe_started_at from carousel_state order by hash")
        assert states == [
            {"hash": "h1", "state": "probing", "probe_started_at": 1000},
            {"hash": "h2", "state": "probing", "probe_started_at": 1000},
            {"hash": "h3", "state": "probing", "probe_started_at": 1000},
        ]


def test_carousel_service_promotes_probe_with_swarm_and_stops_expired_dead_probe():
    from qbt_orchestrator.carousel import CarouselService
    from qbt_orchestrator.db import migrate

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        _seed_dead_allocations(db, ["hot", "cold"])
        con = sqlite3.connect(db)
        con.execute("insert into carousel_state(hash,state,probe_started_at,last_probe_at,backoff_level,updated_at) values('hot','probing',900,900,0,900)")
        con.execute("insert into carousel_state(hash,state,probe_started_at,last_probe_at,backoff_level,updated_at) values('cold','probing',0,0,0,0)")
        con.commit(); con.close()
        executor = RecordingExecutor()
        svc = CarouselService(db, executor, dry_run=False, concurrency=3, probe_duration_sec=1800, now=lambda: 2000)
        snapshots = {
            "hot": {"hash": "hot", "category": "auto", "amount_left": 1, "num_seeds": 0, "num_peers": 2},
            "cold": {"hash": "cold", "category": "auto", "amount_left": 1, "num_seeds": 0, "num_peers": 0},
        }

        result = svc.run_once(snapshots, sync_healthy=True)

        assert result["promoted"] == ["hot"]
        assert result["stopped"] == ["cold"]
        assert ("/api/v2/torrents/stop", {"hashes": "cold"}) in executor.posts
        alloc = _rows(db, "select hash,desired_state,slot_kind,desired_seq_dl,reason from scheduler_allocations order by hash")
        assert {r["hash"]: (r["desired_state"], r["slot_kind"], r["desired_seq_dl"], r["reason"]) for r in alloc} == {
            "cold": ("dead", "dead", 0, "carousel_no_swarm"),
            "hot": ("soak", "soak", 0, "carousel_swarm_seen"),
        }
        states = {r["hash"]: r for r in _rows(db, "select hash,state,backoff_level,backoff_until from carousel_state order by hash")}
        assert states["hot"]["state"] == "soak"
        assert states["cold"]["state"] == "dead"
        assert states["cold"]["backoff_level"] == 1
        assert states["cold"]["backoff_until"] == 2000 + 30 * 60


def test_carousel_can_probe_dead_allocation_created_by_planner_health_policy():
    from qbt_orchestrator.carousel import CarouselService
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.planner import DownloadPlanner
    from tests.fakes import FakeExecutor

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        con.execute("insert into scheduler_allocations(hash,desired_state,applied_state,slot_kind,desired_seq_dl,allocated_at,reason) values('deadish','soak','soak','soak',0,100,'budget_or_slot_exhausted')")
        con.execute("insert into torrent_health(hash,sampled_at,dlspeed_bps,completed_bytes,last_completed_bytes,progress,num_seeds,num_peers,last_swarm_seen_at,no_progress_since,soak_since,updated_at) values('deadish',1000,0,100,100,0.2,0,0,1000,1000,1000,1000)")
        con.commit(); con.close()
        snapshots = {"deadish": {"hash": "deadish", "category": "auto", "tags": "auto", "state": "stoppedDL", "amount_left": 10, "size": 20, "progress": 0.2, "dlspeed": 0, "completed": 100, "num_seeds": 0, "num_peers": 0}}
        DownloadPlanner(db, FakeExecutor(), dry_run=False, active_slots=0, disk_floor_bytes=0, now=lambda: 4601).plan_and_apply(snapshots, free_bytes=100, sync_healthy=True)
        executor = RecordingExecutor()
        carousel = CarouselService(db, executor, dry_run=False, concurrency=3, now=lambda: 4700)

        result = carousel.run_once(snapshots, sync_healthy=True)

        assert result["started"] == ["deadish"]
        assert executor.seq == [("deadish", False)]
        assert executor.posts == [("/api/v2/torrents/start", {"hashes": "deadish"})]


def test_carousel_service_suspends_when_sync_unhealthy_without_qbt_writes():
    from qbt_orchestrator.carousel import CarouselService
    from qbt_orchestrator.db import migrate

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        _seed_dead_allocations(db, ["h1"])
        executor = RecordingExecutor()
        svc = CarouselService(db, executor, dry_run=False, concurrency=3, now=lambda: 1000)

        result = svc.run_once({"h1": {"hash": "h1", "category": "auto", "amount_left": 1}}, sync_healthy=False)

        assert result["suspended"] is True
        assert executor.posts == []
        assert _rows(db, "select * from carousel_state") == []
        event = _rows(db, "select component,event_type from events_v2 order by id desc limit 1")[0]
        assert event == {"component": "carousel", "event_type": "suspended_unhealthy_sync"}


def test_daemon_default_carousel_loop_uses_sync_cache_not_not_configured():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.service import DaemonRuntime
    from tests.test_daemon_runtime import FakeExecutor

    class Qbt:
        def __init__(self):
            self.rids = []
        def get_maindata(self, rid):
            self.rids.append(rid)
            return {
                "rid": rid + 1,
                "full_update": True,
                "torrents": {"h1": {"name": "dead", "category": "auto", "amount_left": 1, "num_seeds": 0, "num_peers": 0}},
                "server_state": {},
            }

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        _seed_dead_allocations(db, ["h1"])
        daemon = DaemonRuntime(
            state_db=db,
            qbt=Qbt(),
            executor=FakeExecutor(),
            free_bytes_provider=lambda: 6 * 1024**3,
            dry_run=True,
            safety_interval=0,
        )

        daemon.run(max_safety_ticks=1)

        con = sqlite3.connect(db)
        loop_json = con.execute(
            "select data_json from events_v2 where component='carousel' and event_type='loop_tick' order by id desc limit 1"
        ).fetchone()[0]
        con.close()
        result = json.loads(loop_json)["result"]
        assert "not_configured" not in json.dumps(result)
        assert result["started"] == ["h1"]
        assert result["dry_run"] is True


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
    print("ok")
