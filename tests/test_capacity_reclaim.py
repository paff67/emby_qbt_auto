#!/usr/bin/env python3
from __future__ import annotations

import sqlite3
import sys
import tempfile
import json
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))


class RecordingExecutor:
    def __init__(self, info=None, *, stop_updates_state=True):
        self.posts = []
        self.info = {str(key): dict(value) for key, value in (info or {}).items()}
        self.qbt = self
        self.stop_updates_state = bool(stop_updates_state)

    def qbt_post(self, path, payload):
        self.posts.append((path, payload))
        if path.endswith("/stop") and self.stop_updates_state:
            torrent_hash = str(payload["hashes"])
            self.info.setdefault(torrent_hash, {})["state"] = "stoppedDL"

    def torrent_info(self, torrent_hash):
        torrent_hash = str(torrent_hash)
        current = {
            "category": "auto",
            "tags": "auto",
            "state": "stoppedDL",
            "amount_left": 1,
            "availability": 0.5,
            "num_seeds": 0,
            "num_complete": 0,
            "content_path": f"/downloads/incomplete/{torrent_hash}",
        }
        current.update(self.info.get(torrent_hash, {}))
        return current

    def get_maindata(self, rid):
        assert rid == 0
        return {
            "rid": 1,
            "full_update": True,
            "torrents": {key: dict(value) for key, value in self.info.items()},
        }


def _assessment(
    torrent_hash: str = "h",
    *,
    generation: int = 4,
    observed_at: int = 5_000,
    availability: float | None = 0.5,
    viable: bool = False,
    managed: bool = True,
    incomplete: bool = True,
    complete_sources: int = 0,
    no_progress_since: int | None = 100,
):
    from qbt_orchestrator.capacity_assessment import (
        CapacityAssessment,
        TorrentCapacityEvidence,
    )

    evidence = TorrentCapacityEvidence(
        hash=torrent_hash,
        managed=managed,
        incomplete=incomplete,
        amount_left=900 if incomplete else 0,
        completed_bytes=100,
        availability=availability,
        complete_sources=complete_sources,
        no_progress_since=no_progress_since,
        viable=viable,
        viability_reason=(
            "complete_source" if viable and complete_sources else "stale_without_complete_source"
        ),
    )
    return CapacityAssessment(
        generation=generation,
        observed_at=observed_at,
        scheduler_mode="drain",
        free_bytes=0,
        target_free_bytes=10_000,
        available_growth_bytes=0,
        selected_hashes=frozenset(),
        disk_releasing_jobs=0,
        torrents={torrent_hash: evidence},
    )


def _assessment_for_hashes(
    torrent_hashes,
    *,
    generation: int = 1,
    observed_at: int = 20_000,
):
    from qbt_orchestrator.capacity_assessment import CapacityAssessment

    evidence = {
        torrent_hash: _assessment(
            torrent_hash,
            generation=generation,
            observed_at=observed_at,
        ).torrents[torrent_hash]
        for torrent_hash in torrent_hashes
    }
    return CapacityAssessment(
        generation=generation,
        observed_at=observed_at,
        scheduler_mode="drain",
        free_bytes=0,
        target_free_bytes=10_000,
        available_growth_bytes=0,
        selected_hashes=frozenset(),
        disk_releasing_jobs=0,
        torrents=evidence,
    )


def _capacity_health(
    db: Path,
    torrent_hash: str,
    *,
    generation: int = 4,
    current_generation: int | None = None,
    reclaimable_since: int | None = 1_000,
    no_progress_since: int | None = 100,
    capacity_viable: int = 0,
    desired_state: str = "soak",
) -> None:
    con = sqlite3.connect(db)
    con.execute(
        "insert or replace into scheduler_allocations(hash,desired_state,applied_state,slot_kind,allocated_at,reason) "
        "values(?,?,?,?,0,'test')",
        (torrent_hash, desired_state, desired_state, desired_state),
    )
    con.execute(
        "insert or replace into torrent_health("
        "hash,sampled_at,no_progress_since,reclaimable_since,capacity_viable,capacity_reason,"
        "capacity_assessed_at,capacity_generation,updated_at) values(?,?,?,?,?,?,?,?,?)",
        (
            torrent_hash,
            100,
            no_progress_since,
            reclaimable_since,
            capacity_viable,
            "stale_without_complete_source",
            5_000,
            generation,
            5_000,
        ),
    )
    con.execute(
        "insert or replace into capacity_assessment_state("
        "id,current_generation,observed_at,summary_json) values(1,?,?, '{}')",
        (generation if current_generation is None else current_generation, 5_000),
    )
    con.commit()
    con.close()


def _snapshot(
    torrent_hash: str,
    *,
    content_path: str,
    tags: str = "auto",
    peers: int = 0,
) -> dict[str, dict[str, object]]:
    return {
        torrent_hash: {
            "hash": torrent_hash,
            "name": torrent_hash,
            "category": "auto",
            "tags": tags,
            "state": "downloading",
            "amount_left": 900,
            "completed_bytes": 100,
            "progress": 0.0,
            "availability": 0.5,
            "num_seeds": 0,
            "num_complete": 0,
            "num_peers": peers,
            "content_path": content_path,
        }
    }


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
    con.execute(
        "update torrent_health set reclaimable_since=?,no_progress_since=100,capacity_viable=0,"
        "capacity_reason='stale_without_complete_source',capacity_assessed_at=?,"
        "capacity_generation=1 where hash=?",
        (now - 10_000, now, torrent_hash),
    )
    con.execute(
        "insert or replace into capacity_assessment_state("
        "id,current_generation,observed_at,summary_json) values(1,1,?,'{}')",
        (now,),
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
            assessment=_assessment("dead-one", generation=1, observed_at=now),
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
        executor = RecordingExecutor(
            {
                "dead-one": _snapshot(
                    "dead-one", content_path="/downloads/incomplete/dead-one"
                )["dead-one"]
            }
        )
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
            assessment=_assessment("dead-one", generation=1, observed_at=now),
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
            assessment=_assessment_for_hashes(
                ("held", "running", "overlap"), observed_at=now
            ),
            capacity_state="capacity_deadlock",
            free_bytes=0,
            target_free_bytes=10_000,
        )

        assert result.planned == 0
        reasons = result.rejection_counts
        assert reasons["protected_tag"] == 1
        assert reasons["path_missing"] == 1
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
            "update torrent_health set reclaimable_since=? where hash='dead-one'",
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
            assessment=_assessment("dead-one", generation=1, observed_at=now),
            capacity_state="capacity_deadlock",
            free_bytes=0,
            target_free_bytes=10_000,
        )

        assert result.planned == 0
        assert result.rejection_counts["reclaimable_age"] == 1


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
            assessment=_assessment(
                "dead-one", generation=1, observed_at=now, availability=0.8
            ),
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
            assessment=_assessment(
                "dead-one", generation=1, observed_at=now,
                availability=1.0, viable=True, complete_sources=1,
            ),
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
            assessment=_assessment("dead-one", generation=1, observed_at=now),
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
            db,
            RecheckFailExecutor(
                {
                    "dead-one": _snapshot(
                        "dead-one", content_path="/downloads/incomplete/dead-one"
                    )["dead-one"]
                }
            ),
            host_downloads=root,
            container_downloads="/downloads", managed_root=managed,
            dry_run=False, min_dead_age_sec=3_600, min_reclaim_bytes=1,
            now=lambda: now,
        )

        result = reclaimer.run(
            _snapshot("dead-one", content_path="/downloads/incomplete/dead-one"),
            assessment=_assessment("dead-one", generation=1, observed_at=now),
            capacity_state="capacity_deadlock", free_bytes=0, target_free_bytes=10_000,
        )

        assert not payload.exists()
        assert result.reclaimed == 0
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
            RecordingExecutor(
                {
                    "dead-one": {
                        **_snapshot(
                            "dead-one", content_path="/downloads/incomplete/dead-one"
                        )["dead-one"],
                        "progress": 0.1,
                    }
                }
            ),
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
            assessment=_assessment("dead-one", generation=1, observed_at=now),
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
        assert reclaim["reclaim_key"] == f"dead-one:{now - 10_000}"
        assert reclaim["reclaimable_since"] == now - 10_000
        assert reclaim["capacity_generation"] == 1
        assert reclaim["capacity_reason"] == "stale_without_complete_source"
        assessment_json = json.loads(reclaim["assessment_json"])
        assert assessment_json["generation"] == 1
        assert assessment_json["torrent"]["hash"] == "dead-one"
        assert "content_path" not in reclaim["assessment_json"]
        assert "magnet" not in reclaim["assessment_json"]
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
            assessment=_assessment("dead-one", generation=1, observed_at=now),
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
            RecheckFailExecutor(
                {
                    "dead-one": _snapshot(
                        "dead-one", content_path="/downloads/incomplete/dead-one"
                    )["dead-one"]
                }
            ),
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
            assessment=_assessment("dead-one", generation=1, observed_at=now),
            capacity_state="capacity_deadlock",
            free_bytes=0,
            target_free_bytes=10_000,
        )

        assert result.reclaimed == 0
        con = sqlite3.connect(db)
        con.row_factory = sqlite3.Row
        reclaim = dict(con.execute("select * from capacity_reclaims").fetchone())
        notice = dict(
            con.execute(
                "select * from bot_notifications where topic='capacity_reclaim'"
            ).fetchone()
        )
        con.close()
        assert reclaim["state"] == "recheck_pending"
        assert reclaim["recheck_state"] == "failed"
        assert "recheck unavailable" in reclaim["recheck_error"]
        assert "重新校验请求失败" in notice["message"]


def test_nonviable_soak_allocation_matures_and_leech_peers_do_not_block(tmp_path):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    payload.mkdir(parents=True)
    (payload / "part").write_bytes(b"x" * 4096)
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h", desired_state="soak")
    reclaimer = DeadPartialReclaimer(
        db,
        RecordingExecutor(),
        host_downloads=tmp_path,
        container_downloads="/downloads",
        managed_root=managed,
        dry_run=True,
        min_reclaimable_age_sec=3_600,
        min_reclaim_bytes=1,
        now=lambda: 5_000,
    )

    result = reclaimer.run(
        _snapshot("h", content_path="/downloads/incomplete/h", peers=3),
        assessment=_assessment(),
        capacity_state="capacity_deadlock",
        free_bytes=0,
        target_free_bytes=10_000,
    )

    assert result.planned == 1
    assert result.assessment_generation == 4


def test_scheduler_dead_soak_switch_does_not_affect_reclaim_candidate(tmp_path):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    payload.mkdir(parents=True)
    (payload / "part").write_bytes(b"x" * 4096)
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h", desired_state="dead")
    reclaimer = DeadPartialReclaimer(
        db, RecordingExecutor(), host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=True, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        now=lambda: 5_000,
    )
    kwargs = dict(
        assessment=_assessment(), capacity_state="capacity_deadlock",
        free_bytes=0, target_free_bytes=10_000,
    )

    dead = reclaimer.run(_snapshot("h", content_path="/downloads/incomplete/h"), **kwargs)
    con = sqlite3.connect(db)
    con.execute("update scheduler_allocations set desired_state='soak' where hash='h'")
    con.commit()
    con.close()
    soak = reclaimer.run(_snapshot("h", content_path="/downloads/incomplete/h"), **kwargs)

    assert dead.planned == soak.planned == 1


def test_live_reclaim_fences_stale_assessment_generation(tmp_path):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    payload.mkdir(parents=True)
    (payload / "part").write_bytes(b"x" * 4096)
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h", generation=4, current_generation=5)
    info = _snapshot("h", content_path="/downloads/incomplete/h")["h"]
    executor = RecordingExecutor({"h": info})
    reclaimer = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        now=lambda: 5_000,
    )

    result = reclaimer.run(
        {"h": info}, assessment=_assessment(generation=4),
        capacity_state="capacity_deadlock", free_bytes=0, target_free_bytes=10_000,
    )

    assert result.reclaimed == 0
    assert result.rejection_counts["stale_assessment"] == 1
    assert payload.exists()
    assert executor.posts == []


@pytest.mark.parametrize(
    ("availability", "reason"),
    [(1.0, "complete_source"), (None, "availability_unknown"), (-1.0, "availability_unknown")],
)
def test_reclaim_rejects_unusable_assessment_availability(tmp_path, availability, reason):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    managed.mkdir()
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    reclaimer = DeadPartialReclaimer(
        db, RecordingExecutor(), host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=True, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        now=lambda: 5_000,
    )

    result = reclaimer.run(
        _snapshot("h", content_path="/downloads/incomplete/h"),
        assessment=_assessment(availability=availability),
        capacity_state="capacity_deadlock", free_bytes=0, target_free_bytes=10_000,
    )

    assert result.planned == 0
    assert result.rejection_counts[reason] == 1


@pytest.mark.parametrize(
    ("protection", "reason"),
    [
        ("job", "open_job"),
        ("reservation", "active_reservation"),
        ("cooldown", "active_cooldown"),
    ],
)
def test_reclaim_rejects_active_database_protections(tmp_path, protection, reason):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    payload.mkdir(parents=True)
    (payload / "part").write_bytes(b"x" * 4096)
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    con = sqlite3.connect(db)
    if protection == "job":
        con.execute(
            "insert into torrent_jobs(hash,job_type,state,created_at,updated_at) "
            "values('h','upload','running',0,0)"
        )
    elif protection == "reservation":
        con.execute(
            "insert into resource_reservations(hash,kind,bytes,state,created_at,expires_at) "
            "values('h','batch',1,'active',0,6000)"
        )
    else:
        con.execute(
            "insert into soak_state(hash,state,cooldown_until,updated_at) "
            "values('h','soak_cooldown',6000,0)"
        )
    con.commit()
    con.close()
    reclaimer = DeadPartialReclaimer(
        db, RecordingExecutor(), host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=True, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        now=lambda: 5_000,
    )

    result = reclaimer.run(
        _snapshot("h", content_path="/downloads/incomplete/h"), assessment=_assessment(),
        capacity_state="capacity_deadlock", free_bytes=0, target_free_bytes=10_000,
    )

    assert result.planned == 0
    assert result.rejection_counts[reason] == 1


def test_reclaim_rejects_protected_tag(tmp_path):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    managed.mkdir()
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    reclaimer = DeadPartialReclaimer(
        db, RecordingExecutor(), host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=True, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        now=lambda: 5_000,
    )

    result = reclaimer.run(
        _snapshot("h", content_path="/downloads/incomplete/h", tags="auto,hold"),
        assessment=_assessment(), capacity_state="capacity_deadlock",
        free_bytes=0, target_free_bytes=10_000,
    )

    assert result.planned == 0
    assert result.rejection_counts["protected_tag"] == 1


def test_reclaim_rejects_overlapping_path(tmp_path):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    shared = managed / "shared"
    shared.mkdir(parents=True)
    (shared / "part").write_bytes(b"x" * 4096)
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    snapshots = _snapshot("h", content_path="/downloads/incomplete/shared")
    snapshots["other"] = {
        **snapshots["h"], "hash": "other",
        "content_path": "/downloads/incomplete/shared/part",
    }
    reclaimer = DeadPartialReclaimer(
        db, RecordingExecutor(), host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=True, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        now=lambda: 5_000,
    )

    result = reclaimer.run(
        snapshots, assessment=_assessment(), capacity_state="capacity_deadlock",
        free_bytes=0, target_free_bytes=10_000,
    )

    assert result.planned == 0
    assert result.rejection_counts["path_overlap"] == 1


def test_reclaim_default_threshold_rejects_less_than_64_mib(tmp_path):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    payload.mkdir(parents=True)
    (payload / "part").write_bytes(b"x" * 4096)
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    reclaimer = DeadPartialReclaimer(
        db, RecordingExecutor(), host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=True, min_reclaimable_age_sec=3_600, now=lambda: 5_000,
    )

    result = reclaimer.run(
        _snapshot("h", content_path="/downloads/incomplete/h"), assessment=_assessment(),
        capacity_state="capacity_deadlock", free_bytes=0, target_free_bytes=10_000,
    )

    assert result.planned == 0
    assert result.rejection_counts["below_min_reclaim"] == 1


def test_uncommitted_assessment_is_fenced_before_selection(tmp_path):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    managed.mkdir()
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    reclaimer = DeadPartialReclaimer(
        db, RecordingExecutor(), host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
    )

    result = reclaimer.run(
        {}, assessment=_assessment(generation=0),
        capacity_state="capacity_deadlock", free_bytes=0, target_free_bytes=1,
    )

    assert result.assessment_generation == 0
    assert result.rejection_counts == {"uncommitted_assessment": 1}


def test_missing_assessment_is_fenced_without_touching_qbt(tmp_path):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    managed.mkdir()
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    executor = RecordingExecutor()
    reclaimer = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False,
    )

    result = reclaimer.run(
        {}, capacity_state="capacity_deadlock", free_bytes=0, target_free_bytes=1,
    )

    assert result.assessment_generation == 0
    assert result.rejection_counts == {"uncommitted_assessment": 1}
    assert executor.posts == []


def test_live_reclaim_waits_for_stopped_state_with_bounded_timeout(tmp_path):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    payload.mkdir(parents=True)
    (payload / "part").write_bytes(b"x" * 4096)
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    info = _snapshot("h", content_path="/downloads/incomplete/h")["h"]
    executor = RecordingExecutor({"h": info}, stop_updates_state=False)
    reclaimer = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        stop_timeout_sec=0, sleep=lambda _seconds: pytest.fail("unexpected sleep"),
        now=lambda: 5_000,
    )

    result = reclaimer.run(
        {"h": info}, assessment=_assessment(), capacity_state="capacity_deadlock",
        free_bytes=0, target_free_bytes=10_000,
    )

    assert result.reclaimed == 0
    assert result.rejection_counts["stop_timeout"] == 1
    assert payload.exists()
    _assert_capacity_reclaim_aborted_paused(db, "stop_timeout")


def test_live_reclaim_fences_generation_change_during_stop_window(tmp_path):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    payload.mkdir(parents=True)
    (payload / "part").write_bytes(b"x" * 4096)
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    info = _snapshot("h", content_path="/downloads/incomplete/h")["h"]

    class GenerationRaceExecutor(RecordingExecutor):
        def qbt_post(self, path, payload):
            super().qbt_post(path, payload)
            if path.endswith("/stop"):
                con = sqlite3.connect(db)
                con.execute(
                    "update capacity_assessment_state set current_generation=5 where id=1"
                )
                con.commit()
                con.close()

    executor = GenerationRaceExecutor({"h": info})
    reclaimer = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        now=lambda: 5_000,
    )

    result = reclaimer.run(
        {"h": info}, assessment=_assessment(), capacity_state="capacity_deadlock",
        free_bytes=0, target_free_bytes=10_000,
    )

    assert result.planned == 1
    assert result.reclaimed == 0
    assert result.rejection_counts["stale_assessment"] == 1
    assert payload.exists()
    _assert_capacity_reclaim_aborted_paused(db, "stale_assessment")


def test_live_reclaim_rechecks_path_after_stop_before_audit(tmp_path):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    original = managed / "h"
    changed = managed / "changed"
    original.mkdir(parents=True)
    changed.mkdir(parents=True)
    (original / "part").write_bytes(b"x" * 4096)
    (changed / "part").write_bytes(b"y" * 4096)
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    info = _snapshot("h", content_path="/downloads/incomplete/h")["h"]

    class PathRaceExecutor(RecordingExecutor):
        def qbt_post(self, path, payload):
            super().qbt_post(path, payload)
            if path.endswith("/stop"):
                self.info["h"]["content_path"] = "/downloads/incomplete/changed"

    executor = PathRaceExecutor({"h": info})
    reclaimer = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        now=lambda: 5_000,
    )

    result = reclaimer.run(
        {"h": info}, assessment=_assessment(), capacity_state="capacity_deadlock",
        free_bytes=0, target_free_bytes=10_000,
    )

    assert result.reclaimed == 0
    assert result.rejection_counts["path_changed"] == 1
    assert original.exists() and changed.exists()
    _assert_capacity_reclaim_aborted_paused(db, "path_changed")


def test_live_reclaim_rechecks_active_protection_after_stop(tmp_path):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    payload.mkdir(parents=True)
    (payload / "part").write_bytes(b"x" * 4096)
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    info = _snapshot("h", content_path="/downloads/incomplete/h")["h"]

    class ProtectionRaceExecutor(RecordingExecutor):
        def qbt_post(self, path, payload):
            super().qbt_post(path, payload)
            if path.endswith("/stop"):
                con = sqlite3.connect(db)
                con.execute(
                    "insert into torrent_jobs(hash,job_type,state,created_at,updated_at) "
                    "values('h','upload','running',0,0)"
                )
                con.commit()
                con.close()

    executor = ProtectionRaceExecutor({"h": info})
    reclaimer = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        now=lambda: 5_000,
    )

    result = reclaimer.run(
        {"h": info}, assessment=_assessment(), capacity_state="capacity_deadlock",
        free_bytes=0, target_free_bytes=10_000,
    )

    assert result.reclaimed == 0
    assert result.rejection_counts["active_protection"] == 1
    assert payload.exists()
    _assert_capacity_reclaim_aborted_paused(db, "active_protection")


def test_live_revalidation_rejects_torrent_that_became_manually_managed(tmp_path):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    payload.mkdir(parents=True)
    (payload / "part").write_bytes(b"x" * 4096)
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    snapshot = _snapshot("h", content_path="/downloads/incomplete/h")["h"]
    current = {**snapshot, "category": "", "tags": ""}
    executor = RecordingExecutor({"h": current})
    reclaimer = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        now=lambda: 5_000,
    )

    result = reclaimer.run(
        {"h": snapshot}, assessment=_assessment(), capacity_state="capacity_deadlock",
        free_bytes=0, target_free_bytes=10_000,
    )

    assert result.reclaimed == 0
    assert result.rejection_counts["not_managed"] == 1
    assert executor.posts == []
    assert payload.exists()


def _assert_no_capacity_reclaim_audit(db: Path) -> None:
    con = sqlite3.connect(db)
    try:
        assert con.execute("select count(*) from capacity_reclaims").fetchone()[0] == 0
    finally:
        con.close()


def _assert_capacity_reclaim_aborted_paused(db: Path, reason: str | None = None) -> None:
    rows = _capacity_reclaim_rows(db)
    assert len(rows) == 1
    assert rows[0]["state"] == "aborted_paused"
    assert rows[0]["recheck_state"] == "not_requested"
    if reason is not None:
        assert rows[0]["recheck_error"] == reason


def _capacity_reclaim_rows(db: Path) -> list[dict]:
    con = sqlite3.connect(db)
    con.row_factory = sqlite3.Row
    try:
        return [dict(row) for row in con.execute("select * from capacity_reclaims order by id")]
    finally:
        con.close()


def _capacity_reclaim_notifications(db: Path) -> list[dict]:
    con = sqlite3.connect(db)
    con.row_factory = sqlite3.Row
    try:
        return [
            dict(row)
            for row in con.execute(
                "select * from bot_notifications where topic='capacity_reclaim' order by id"
            )
        ]
    finally:
        con.close()


@pytest.mark.parametrize("availability", [float("nan"), float("inf"), float("-inf")])
def test_live_revalidation_rejects_nonfinite_availability(tmp_path, availability):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    payload.mkdir(parents=True)
    (payload / "part").write_bytes(b"x" * 4096)
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    snapshot = _snapshot("h", content_path="/downloads/incomplete/h")["h"]
    current = {**snapshot, "availability": availability}
    executor = RecordingExecutor({"h": current})
    reclaimer = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        now=lambda: 5_000,
    )

    result = reclaimer.run(
        {"h": snapshot}, assessment=_assessment(), capacity_state="capacity_deadlock",
        free_bytes=0, target_free_bytes=10_000,
    )

    assert result.reclaimed == 0
    assert result.rejection_counts["availability_unknown"] == 1
    assert payload.exists()
    _assert_no_capacity_reclaim_audit(db)


def test_live_revalidation_rejects_mismatched_torrent_identity(tmp_path):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    payload.mkdir(parents=True)
    (payload / "part").write_bytes(b"x" * 4096)
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    snapshot = _snapshot("h", content_path="/downloads/incomplete/h")["h"]
    current = {**snapshot, "hash": "OTHER"}
    executor = RecordingExecutor({"h": current})
    reclaimer = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        now=lambda: 5_000,
    )

    result = reclaimer.run(
        {"h": snapshot}, assessment=_assessment(), capacity_state="capacity_deadlock",
        free_bytes=0, target_free_bytes=10_000,
    )

    assert result.reclaimed == 0
    assert result.rejection_counts["torrent_identity_changed"] == 1
    assert payload.exists()
    _assert_no_capacity_reclaim_audit(db)


@pytest.mark.parametrize("speed_field", ["dlspeed", "dlspeed_bps"])
def test_live_revalidation_rejects_resumed_download_speed(tmp_path, speed_field):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    payload.mkdir(parents=True)
    (payload / "part").write_bytes(b"x" * 4096)
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    snapshot = _snapshot("h", content_path="/downloads/incomplete/h")["h"]
    current = {**snapshot, speed_field: 1}
    executor = RecordingExecutor({"h": current})
    reclaimer = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        now=lambda: 5_000,
    )

    result = reclaimer.run(
        {"h": snapshot}, assessment=_assessment(), capacity_state="capacity_deadlock",
        free_bytes=0, target_free_bytes=10_000,
    )

    assert result.reclaimed == 0
    assert result.rejection_counts["progress_resumed"] == 1
    assert payload.exists()
    _assert_no_capacity_reclaim_audit(db)


@pytest.mark.parametrize("completed_field", ["completed_bytes", "completed", "downloaded"])
def test_live_revalidation_rejects_completed_byte_growth(tmp_path, completed_field):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    payload.mkdir(parents=True)
    (payload / "part").write_bytes(b"x" * 4096)
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    snapshot = _snapshot("h", content_path="/downloads/incomplete/h")["h"]
    current = dict(snapshot)
    current.pop("completed_bytes", None)
    current[completed_field] = 101
    executor = RecordingExecutor({"h": current})
    reclaimer = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        now=lambda: 5_000,
    )

    result = reclaimer.run(
        {"h": snapshot}, assessment=_assessment(), capacity_state="capacity_deadlock",
        free_bytes=0, target_free_bytes=10_000,
    )

    assert result.reclaimed == 0
    assert result.rejection_counts["progress_evidence_changed"] == 1
    assert payload.exists()
    _assert_no_capacity_reclaim_audit(db)


def test_stop_window_completed_growth_is_revalidated_before_audit(tmp_path):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    payload.mkdir(parents=True)
    (payload / "part").write_bytes(b"x" * 4096)
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    snapshot = _snapshot("h", content_path="/downloads/incomplete/h")["h"]
    before_stop = {**snapshot, "state": "downloading", "completed_bytes": 100}
    after_stop = {**snapshot, "state": "stoppedDL", "completed_bytes": 101}

    class StopWindowExecutor(RecordingExecutor):
        def __init__(self):
            super().__init__({"h": before_stop})
            self.responses = [before_stop, after_stop]

        def torrent_info(self, torrent_hash):
            return dict(self.responses.pop(0))

    executor = StopWindowExecutor()
    reclaimer = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        now=lambda: 5_000,
    )

    result = reclaimer.run(
        {"h": snapshot}, assessment=_assessment(), capacity_state="capacity_deadlock",
        free_bytes=0, target_free_bytes=10_000,
    )

    assert result.planned == 1
    assert result.reclaimed == 0
    assert result.rejection_counts["progress_evidence_changed"] == 1
    assert executor.posts == [("/api/v2/torrents/stop", {"hashes": "h"})]
    assert payload.exists()
    _assert_capacity_reclaim_aborted_paused(db, "progress_evidence_changed")


@pytest.mark.parametrize("live_hash", [pytest.param("missing", id="missing-key"), None, "", "   "])
def test_live_revalidation_fails_closed_when_torrent_hash_is_unknown(
    tmp_path, live_hash
):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    payload.mkdir(parents=True)
    (payload / "part").write_bytes(b"x" * 4096)
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    snapshot = _snapshot("h", content_path="/downloads/incomplete/h")["h"]
    current = {**snapshot, "state": "stoppedDL"}
    if live_hash == "missing":
        current.pop("hash")
    else:
        current["hash"] = live_hash

    class RawInfoExecutor(RecordingExecutor):
        def torrent_info(self, torrent_hash):
            return dict(current)

    executor = RawInfoExecutor({"h": current})
    reclaimer = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        now=lambda: 5_000,
    )

    result = reclaimer.run(
        {"h": snapshot}, assessment=_assessment(), capacity_state="capacity_deadlock",
        free_bytes=0, target_free_bytes=10_000,
    )

    assert result.reclaimed == 0
    assert result.rejection_counts["torrent_identity_unknown"] == 1
    assert payload.exists()
    _assert_no_capacity_reclaim_audit(db)


def test_stop_window_missing_torrent_hash_is_revalidated_before_audit(tmp_path):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    payload.mkdir(parents=True)
    (payload / "part").write_bytes(b"x" * 4096)
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    snapshot = _snapshot("h", content_path="/downloads/incomplete/h")["h"]
    before_stop = {**snapshot, "state": "downloading"}
    after_stop = {**snapshot, "state": "stoppedDL", "hash": "   "}

    class StopWindowExecutor(RecordingExecutor):
        def __init__(self):
            super().__init__({"h": before_stop})
            self.responses = [before_stop, after_stop]

        def torrent_info(self, torrent_hash):
            return dict(self.responses.pop(0))

    executor = StopWindowExecutor()
    reclaimer = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        now=lambda: 5_000,
    )

    result = reclaimer.run(
        {"h": snapshot}, assessment=_assessment(), capacity_state="capacity_deadlock",
        free_bytes=0, target_free_bytes=10_000,
    )

    assert result.planned == 1
    assert result.reclaimed == 0
    assert result.rejection_counts["torrent_identity_unknown"] == 1
    assert executor.posts == [("/api/v2/torrents/stop", {"hashes": "h"})]
    assert payload.exists()
    _assert_capacity_reclaim_aborted_paused(db, "torrent_identity_unknown")


def test_stop_window_no_progress_evidence_change_is_fenced_before_audit(tmp_path):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    payload.mkdir(parents=True)
    (payload / "part").write_bytes(b"x" * 4096)
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    snapshot = _snapshot("h", content_path="/downloads/incomplete/h")["h"]

    class ProgressRaceExecutor(RecordingExecutor):
        def qbt_post(self, path, payload):
            super().qbt_post(path, payload)
            if path.endswith("/stop"):
                con = sqlite3.connect(db)
                con.execute(
                    "update torrent_health set no_progress_since=5000 where hash='h'"
                )
                con.commit()
                con.close()

    executor = ProgressRaceExecutor({"h": snapshot})
    reclaimer = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        now=lambda: 5_000,
    )

    result = reclaimer.run(
        {"h": snapshot}, assessment=_assessment(), capacity_state="capacity_deadlock",
        free_bytes=0, target_free_bytes=10_000,
    )

    assert result.reclaimed == 0
    assert result.rejection_counts["progress_evidence_changed"] == 1
    assert payload.exists()
    _assert_capacity_reclaim_aborted_paused(db, "progress_evidence_changed")


def test_live_revalidation_rejects_progress_fraction_growth(tmp_path):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    payload.mkdir(parents=True)
    (payload / "part").write_bytes(b"x" * 4096)
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    snapshot = {**_snapshot("h", content_path="/downloads/incomplete/h")["h"], "progress": 0.2}
    current = {**snapshot, "progress": 0.3}
    executor = RecordingExecutor({"h": current})
    reclaimer = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        now=lambda: 5_000,
    )

    result = reclaimer.run(
        {"h": snapshot}, assessment=_assessment(), capacity_state="capacity_deadlock",
        free_bytes=0, target_free_bytes=10_000,
    )

    assert result.reclaimed == 0
    assert result.rejection_counts["progress_evidence_changed"] == 1
    assert payload.exists()
    _assert_no_capacity_reclaim_audit(db)


def test_live_revalidation_rejects_amount_left_decrease(tmp_path):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    payload.mkdir(parents=True)
    (payload / "part").write_bytes(b"x" * 4096)
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    snapshot = _snapshot("h", content_path="/downloads/incomplete/h")["h"]
    current = {**snapshot, "amount_left": 899}
    executor = RecordingExecutor({"h": current})
    reclaimer = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        now=lambda: 5_000,
    )

    result = reclaimer.run(
        {"h": snapshot}, assessment=_assessment(), capacity_state="capacity_deadlock",
        free_bytes=0, target_free_bytes=10_000,
    )

    assert result.reclaimed == 0
    assert result.rejection_counts["content_selection_changed"] == 1
    assert payload.exists()
    _assert_no_capacity_reclaim_audit(db)


def test_stop_window_progress_fraction_growth_is_revalidated_before_audit(tmp_path):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    payload.mkdir(parents=True)
    (payload / "part").write_bytes(b"x" * 4096)
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    snapshot = {**_snapshot("h", content_path="/downloads/incomplete/h")["h"], "progress": 0.2}
    before_stop = {**snapshot, "state": "downloading"}
    after_stop = {**snapshot, "state": "stoppedDL", "progress": 0.3}

    class StopWindowExecutor(RecordingExecutor):
        def __init__(self):
            super().__init__({"h": before_stop})
            self.responses = [before_stop, after_stop]

        def torrent_info(self, torrent_hash):
            return dict(self.responses.pop(0))

    executor = StopWindowExecutor()
    reclaimer = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        now=lambda: 5_000,
    )

    result = reclaimer.run(
        {"h": snapshot}, assessment=_assessment(), capacity_state="capacity_deadlock",
        free_bytes=0, target_free_bytes=10_000,
    )

    assert result.reclaimed == 0
    assert result.rejection_counts["progress_evidence_changed"] == 1
    assert executor.posts == [("/api/v2/torrents/stop", {"hashes": "h"})]
    assert payload.exists()
    _assert_capacity_reclaim_aborted_paused(db, "progress_evidence_changed")


def test_stop_window_fresh_inventory_detects_new_path_overlap(tmp_path):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    old_other = managed / "other"
    payload.mkdir(parents=True)
    old_other.mkdir(parents=True)
    (payload / "part").write_bytes(b"x" * 4096)
    (old_other / "part").write_bytes(b"y" * 4096)
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    candidate = _snapshot("h", content_path="/downloads/incomplete/h")["h"]
    other = _snapshot("other", content_path="/downloads/incomplete/other")["other"]

    class InventoryRaceExecutor(RecordingExecutor):
        def get_maindata(self, rid):
            assert rid == 0
            return {
                "full_update": True,
                "torrents": {
                    "h": dict(self.info["h"]),
                    "other": {**other, "content_path": "/downloads/incomplete/h"},
                },
            }

    executor = InventoryRaceExecutor({"h": candidate, "other": other})
    reclaimer = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        now=lambda: 5_000,
    )

    result = reclaimer.run(
        {"h": candidate, "other": other}, assessment=_assessment(),
        capacity_state="capacity_deadlock", free_bytes=0, target_free_bytes=10_000,
    )

    assert result.reclaimed == 0
    assert result.rejection_counts["path_overlap"] == 1
    assert payload.exists()
    _assert_capacity_reclaim_aborted_paused(db, "path_overlap")


@pytest.mark.parametrize("inventory_mode", ["api-error", "missing-torrents", "not-full"])
def test_live_reclaim_fails_closed_when_fresh_inventory_is_unavailable(
    tmp_path, inventory_mode
):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    payload.mkdir(parents=True)
    (payload / "part").write_bytes(b"x" * 4096)
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    candidate = _snapshot("h", content_path="/downloads/incomplete/h")["h"]

    class BrokenInventoryExecutor(RecordingExecutor):
        def get_maindata(self, rid):
            if inventory_mode == "api-error":
                raise RuntimeError("sync unavailable")
            if inventory_mode == "missing-torrents":
                return {"full_update": True}
            return {"full_update": False, "torrents": {"h": dict(candidate)}}

    executor = BrokenInventoryExecutor({"h": candidate})
    reclaimer = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        now=lambda: 5_000,
    )

    result = reclaimer.run(
        {"h": candidate}, assessment=_assessment(), capacity_state="capacity_deadlock",
        free_bytes=0, target_free_bytes=10_000,
    )

    assert result.reclaimed == 0
    assert result.rejection_counts["path_inventory_failed"] == 1
    assert payload.exists()
    _assert_capacity_reclaim_aborted_paused(db, "path_inventory_failed")


def test_selection_rejects_no_progress_evidence_mismatch(tmp_path):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    payload.mkdir(parents=True)
    (payload / "part").write_bytes(b"x" * 4096)
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h", no_progress_since=101)
    snapshot = _snapshot("h", content_path="/downloads/incomplete/h")
    reclaimer = DeadPartialReclaimer(
        db, RecordingExecutor(), host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=True, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        now=lambda: 5_000,
    )

    result = reclaimer.run(
        snapshot, assessment=_assessment(no_progress_since=100),
        capacity_state="capacity_deadlock", free_bytes=0, target_free_bytes=10_000,
    )

    assert result.planned == 0
    assert result.rejection_counts["progress_evidence_changed"] == 1


@pytest.mark.parametrize(
    "current_progress",
    [pytest.param("missing", id="missing"), "bad", float("nan"), -0.1],
)
def test_live_revalidation_fails_closed_on_unknown_progress(
    tmp_path, current_progress
):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    payload.mkdir(parents=True)
    (payload / "part").write_bytes(b"x" * 4096)
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    snapshot = _snapshot("h", content_path="/downloads/incomplete/h")["h"]
    current = dict(snapshot)
    if current_progress == "missing":
        current.pop("progress")
    else:
        current["progress"] = current_progress

    class RawInfoExecutor(RecordingExecutor):
        def torrent_info(self, torrent_hash):
            return dict(current)

    executor = RawInfoExecutor({"h": current})
    reclaimer = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        now=lambda: 5_000,
    )

    result = reclaimer.run(
        {"h": snapshot}, assessment=_assessment(), capacity_state="capacity_deadlock",
        free_bytes=0, target_free_bytes=10_000,
    )

    assert result.reclaimed == 0
    assert result.rejection_counts["progress_evidence_unknown"] == 1
    assert payload.exists()
    _assert_no_capacity_reclaim_audit(db)


def test_fresh_inventory_candidate_path_must_match_live_info(tmp_path):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    moved = managed / "moved"
    payload.mkdir(parents=True)
    moved.mkdir(parents=True)
    (payload / "part").write_bytes(b"x" * 4096)
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    candidate = _snapshot("h", content_path="/downloads/incomplete/h")["h"]

    class InventoryPathRaceExecutor(RecordingExecutor):
        def get_maindata(self, rid):
            return {
                "full_update": True,
                "torrents": {
                    "h": {
                        **candidate,
                        "state": "stoppedDL",
                        "content_path": "/downloads/incomplete/moved",
                    }
                },
            }

    executor = InventoryPathRaceExecutor({"h": candidate})
    reclaimer = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        now=lambda: 5_000,
    )

    result = reclaimer.run(
        {"h": candidate}, assessment=_assessment(), capacity_state="capacity_deadlock",
        free_bytes=0, target_free_bytes=10_000,
    )

    assert result.reclaimed == 0
    assert result.rejection_counts["path_changed"] == 1
    assert payload.exists() and moved.exists()
    _assert_capacity_reclaim_aborted_paused(db, "path_changed")


@pytest.mark.parametrize(
    ("inventory_change", "reason"),
    [
        ({"tags": "auto,hold"}, "protected_tag"),
        ({"category": "", "tags": ""}, "not_managed"),
        ({"availability": 1.0}, "complete_source"),
        ({"num_seeds": 1}, "complete_source"),
        ({"num_complete": 1}, "complete_source"),
        ({"completed_bytes": 101}, "progress_evidence_changed"),
        ({"progress": 0.1}, "progress_evidence_changed"),
        ({"amount_left": 899}, "content_selection_changed"),
    ],
)
def test_fresh_inventory_candidate_evidence_is_revalidated_before_audit(
    tmp_path, inventory_change, reason
):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    payload.mkdir(parents=True)
    (payload / "part").write_bytes(b"x" * 4096)
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    candidate = _snapshot("h", content_path="/downloads/incomplete/h")["h"]

    class InventoryEvidenceRaceExecutor(RecordingExecutor):
        def get_maindata(self, rid):
            assert rid == 0
            return {
                "full_update": True,
                "torrents": {
                    "h": {**self.info["h"], **inventory_change},
                },
            }

    executor = InventoryEvidenceRaceExecutor({"h": candidate})
    reclaimer = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        now=lambda: 5_000,
    )

    result = reclaimer.run(
        {"h": candidate}, assessment=_assessment(), capacity_state="capacity_deadlock",
        free_bytes=0, target_free_bytes=10_000,
    )

    assert result.reclaimed == 0
    assert result.rejection_counts[reason] == 1
    assert payload.exists()
    _assert_capacity_reclaim_aborted_paused(db, reason)


@pytest.mark.parametrize(
    ("inventory_state", "expected_reclaimed"),
    [
        ("downloading", 0),
        ("forcedDL", 0),
        ("metaDL", 0),
        ("", 0),
        (None, 0),
        ("stoppedDL", 1),
        ("pausedDL", 1),
    ],
)
def test_fresh_inventory_candidate_must_still_be_stopped_before_audit(
    tmp_path, inventory_state, expected_reclaimed
):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    payload.mkdir(parents=True)
    (payload / "part").write_bytes(b"x" * 4096)
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    candidate = _snapshot("h", content_path="/downloads/incomplete/h")["h"]

    class InventoryStateExecutor(RecordingExecutor):
        def get_maindata(self, rid):
            assert rid == 0
            return {
                "full_update": True,
                "torrents": {
                    "h": {**self.info["h"], "state": inventory_state},
                },
            }

    executor = InventoryStateExecutor({"h": candidate})
    reclaimer = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        now=lambda: 5_000,
    )

    result = reclaimer.run(
        {"h": candidate}, assessment=_assessment(), capacity_state="capacity_deadlock",
        free_bytes=0, target_free_bytes=10_000,
    )

    assert result.reclaimed == expected_reclaimed
    if expected_reclaimed:
        assert not payload.exists()
    else:
        assert result.rejection_counts["torrent_not_stopped"] == 1
        assert payload.exists()
        _assert_capacity_reclaim_aborted_paused(db, "torrent_not_stopped")


@pytest.mark.parametrize(
    ("phase", "baseline_change", "live_change", "reason"),
    [
        (
            "initial",
            {"progress": 0.2},
            {"amount_left": 1900, "progress": 0.1},
            "content_selection_changed",
        ),
        (
            "final",
            {"progress": 0.2},
            {"amount_left": 1900, "progress": 0.1},
            "content_selection_changed",
        ),
        ("initial", {"progress": 0.2}, {"progress": 0.1}, "progress_evidence_changed"),
        ("final", {"progress": 0.2}, {"progress": 0.1}, "progress_evidence_changed"),
        ("initial", {}, {"completed_bytes": 99}, "progress_evidence_changed"),
        ("final", {}, {"completed_bytes": 99}, "progress_evidence_changed"),
        ("initial", {"size": 2000}, {"size": 3000}, "content_selection_changed"),
        ("final", {"size": 2000}, {"size": 3000}, "content_selection_changed"),
        (
            "initial",
            {"total_size": 2000},
            {"total_size": 3000},
            "content_selection_changed",
        ),
        (
            "final",
            {"total_size": 2000},
            {"total_size": 3000},
            "content_selection_changed",
        ),
    ],
)
def test_reclaim_fences_exact_content_baseline_before_audit(
    tmp_path, phase, baseline_change, live_change, reason
):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    payload.mkdir(parents=True)
    (payload / "part").write_bytes(b"x" * 4096)
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    candidate = {
        **_snapshot("h", content_path="/downloads/incomplete/h")["h"],
        **baseline_change,
    }

    class ContentRaceExecutor(RecordingExecutor):
        def torrent_info(self, torrent_hash):
            current = super().torrent_info(torrent_hash)
            if phase == "initial":
                current.update(live_change)
            return current

        def get_maindata(self, rid):
            assert rid == 0
            current = dict(self.info["h"])
            if phase == "final":
                current.update(live_change)
            return {"full_update": True, "torrents": {"h": current}}

    executor = ContentRaceExecutor({"h": candidate})
    reclaimer = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        now=lambda: 5_000,
    )

    result = reclaimer.run(
        {"h": candidate}, assessment=_assessment(), capacity_state="capacity_deadlock",
        free_bytes=0, target_free_bytes=10_000,
    )

    assert result.reclaimed == 0
    assert result.rejection_counts[reason] == 1
    assert payload.exists()
    if phase == "initial":
        _assert_no_capacity_reclaim_audit(db)
    else:
        _assert_capacity_reclaim_aborted_paused(db, reason)


def test_reclaim_candidate_records_missing_optional_size_fields_as_none(tmp_path):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    payload.mkdir(parents=True)
    (payload / "part").write_bytes(b"x" * 4096)
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    candidate = _snapshot("h", content_path="/downloads/incomplete/h")["h"]
    reclaimer = DeadPartialReclaimer(
        db, RecordingExecutor(), host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=True, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        now=lambda: 5_000,
    )

    result = reclaimer.run(
        {"h": candidate}, assessment=_assessment(), capacity_state="capacity_deadlock",
        free_bytes=0, target_free_bytes=10_000,
    )

    assert result.planned == 1
    assert result.candidates[0]["size"] is None
    assert result.candidates[0]["total_size"] is None
    assert result.candidates[0]["wanted_size"] is None


@pytest.mark.parametrize("size_field", ["size", "total_size", "wanted_size"])
def test_reclaim_allows_optional_size_first_appearing_in_final_inventory(
    tmp_path, size_field
):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    payload.mkdir(parents=True)
    (payload / "part").write_bytes(b"x" * 4096)
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    candidate = _snapshot("h", content_path="/downloads/incomplete/h")["h"]

    class FinalSizeExecutor(RecordingExecutor):
        def get_maindata(self, rid):
            assert rid == 0
            return {
                "full_update": True,
                "torrents": {"h": {**self.info["h"], size_field: 2000}},
            }

    reclaimer = DeadPartialReclaimer(
        db, FinalSizeExecutor({"h": candidate}), host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        now=lambda: 5_000,
    )

    result = reclaimer.run(
        {"h": candidate}, assessment=_assessment(), capacity_state="capacity_deadlock",
        free_bytes=0, target_free_bytes=10_000,
    )

    assert result.reclaimed == 1
    assert not payload.exists()


def test_stop_confirmation_uses_real_deadline_and_max_poll_count(tmp_path):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    payload.mkdir(parents=True)
    (payload / "part").write_bytes(b"x" * 4096)
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    candidate = _snapshot("h", content_path="/downloads/incomplete/h")["h"]

    class SlowStopExecutor(RecordingExecutor):
        def __init__(self):
            super().__init__({"h": candidate}, stop_updates_state=False)
            self.stop_started = None
            self.stop_polls = 0
            self.poll_timeouts = []

        def qbt_post(self, path, body):
            super().qbt_post(path, body)
            if path.endswith("/stop"):
                self.stop_started = time.monotonic()

        def torrent_info(self, torrent_hash, timeout=None):
            if self.stop_started is not None:
                self.stop_polls += 1
                self.poll_timeouts.append(timeout)
                time.sleep(min(0.02, timeout if timeout is not None else 0.02))
            return dict(candidate)

    executor = SlowStopExecutor()
    reclaimer = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        stop_timeout_sec=0.05, stop_poll_interval_sec=0.001, stop_max_polls=10,
        monotonic=time.monotonic, now=lambda: 5_000,
    )

    result = reclaimer.run(
        {"h": candidate}, assessment=_assessment(), capacity_state="capacity_deadlock",
        free_bytes=0, target_free_bytes=10_000,
    )
    elapsed = time.monotonic() - executor.stop_started

    assert result.reclaimed == 0
    assert result.rejection_counts["stop_timeout"] == 1
    assert elapsed <= 0.08
    assert 1 <= executor.stop_polls <= 10
    assert all(0 < timeout <= 0.05 for timeout in executor.poll_timeouts)
    assert payload.exists()
    _assert_capacity_reclaim_aborted_paused(db, "stop_timeout")


def test_final_inventory_receives_configured_request_timeout(tmp_path):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    payload.mkdir(parents=True)
    (payload / "part").write_bytes(b"x" * 4096)
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    candidate = _snapshot("h", content_path="/downloads/incomplete/h")["h"]

    class TimedInventoryExecutor(RecordingExecutor):
        def __init__(self):
            super().__init__({"h": candidate})
            self.inventory_timeouts = []

        def get_maindata(self, rid, timeout=None):
            self.inventory_timeouts.append(timeout)
            return super().get_maindata(rid)

    executor = TimedInventoryExecutor()
    reclaimer = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        inventory_timeout_sec=0.03, now=lambda: 5_000,
    )

    result = reclaimer.run(
        {"h": candidate}, assessment=_assessment(), capacity_state="capacity_deadlock",
        free_bytes=0, target_free_bytes=10_000,
    )

    assert result.reclaimed == 1
    assert executor.inventory_timeouts == [0.03]


def test_post_stop_progress_abort_is_persisted_and_idempotent(tmp_path):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    payload.mkdir(parents=True)
    (payload / "part").write_bytes(b"x" * 4096)
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    candidate = _snapshot("h", content_path="/downloads/incomplete/h")["h"]
    after_stop = {**candidate, "state": "stoppedDL", "completed_bytes": 101}

    class StopProgressExecutor(RecordingExecutor):
        def __init__(self):
            super().__init__({"h": candidate})
            self.info_calls = 0

        def torrent_info(self, torrent_hash):
            self.info_calls += 1
            if self.info_calls == 2:
                return dict(after_stop)
            return dict(candidate)

    executor = StopProgressExecutor()
    reclaimer = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        notification_chat_ids=["100"], now=lambda: 5_000,
    )

    first = reclaimer.run(
        {"h": candidate}, assessment=_assessment(), capacity_state="capacity_deadlock",
        free_bytes=0, target_free_bytes=10_000,
    )
    second = reclaimer.run(
        {"h": candidate}, assessment=_assessment(), capacity_state="capacity_deadlock",
        free_bytes=0, target_free_bytes=10_000,
    )

    rows = _capacity_reclaim_rows(db)
    notifications = _capacity_reclaim_notifications(db)
    assert first.reclaimed == 0
    assert first.rejection_counts["progress_evidence_changed"] == 1
    assert second.rejection_counts["reclaim_already_recorded"] == 1
    assert rows[0]["state"] == "aborted_paused"
    assert rows[0]["recheck_state"] == "not_requested"
    assert len(notifications) == 1
    warning_payload = json.loads(notifications[0]["payload_json"])
    assert warning_payload["reason"] == "progress_evidence_changed"
    assert warning_payload["requires_confirmation"] is True
    assert warning_payload["allowed_actions"] == ["resume", "keep_paused"]
    assert "保持暂停" in notifications[0]["message"]
    assert [post for post in executor.posts if post[0].endswith("/stop")] == [
        ("/api/v2/torrents/stop", {"hashes": "h"})
    ]
    assert payload.exists()


def test_stop_post_failure_is_persisted_as_unknown_once(tmp_path):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    payload.mkdir(parents=True)
    (payload / "part").write_bytes(b"x" * 4096)
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    candidate = _snapshot("h", content_path="/downloads/incomplete/h")["h"]

    class StopFailureExecutor(RecordingExecutor):
        def qbt_post(self, path, body):
            self.posts.append((path, body))
            if path.endswith("/stop"):
                raise RuntimeError("123456:secret stop unavailable")

    executor = StopFailureExecutor({"h": candidate})
    reclaimer = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        notification_chat_ids=["100"], now=lambda: 5_000,
    )

    reclaimer.run(
        {"h": candidate}, assessment=_assessment(), capacity_state="capacity_deadlock",
        free_bytes=0, target_free_bytes=10_000,
    )
    reclaimer.run(
        {"h": candidate}, assessment=_assessment(), capacity_state="capacity_deadlock",
        free_bytes=0, target_free_bytes=10_000,
    )

    row = _capacity_reclaim_rows(db)[0]
    notifications = _capacity_reclaim_notifications(db)
    assert row["state"] == "stop_unknown"
    assert "123456:secret" not in row["recheck_error"]
    assert len(notifications) == 1
    assert len([post for post in executor.posts if post[0].endswith("/stop")]) == 1
    assert payload.exists()


def test_partial_delete_exception_is_persisted_without_claiming_bytes(tmp_path):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    payload.mkdir(parents=True)
    first_part = payload / "first.part"
    second_part = payload / "second.part"
    first_part.write_bytes(b"x" * 4096)
    second_part.write_bytes(b"y" * 4096)
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    candidate = _snapshot("h", content_path="/downloads/incomplete/h")["h"]
    executor = RecordingExecutor({"h": candidate})
    reclaimer = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        notification_chat_ids=["100"], now=lambda: 5_000,
    )
    delete_calls = []

    def partial_delete(_path):
        delete_calls.append(_path)
        first_part.unlink()
        raise OSError("partial deletion")

    reclaimer._delete_path = partial_delete

    first = reclaimer.run(
        {"h": candidate}, assessment=_assessment(), capacity_state="capacity_deadlock",
        free_bytes=0, target_free_bytes=10_000,
    )
    second = reclaimer.run(
        {"h": candidate}, assessment=_assessment(), capacity_state="capacity_deadlock",
        free_bytes=0, target_free_bytes=10_000,
    )

    row = _capacity_reclaim_rows(db)[0]
    assert first.reclaimed == 0
    assert first.reclaimed_bytes == 0
    assert second.rejection_counts["reclaim_already_recorded"] == 1
    assert row["state"] == "partial_or_unknown"
    assert row["recheck_state"] == "not_requested"
    assert len(_capacity_reclaim_notifications(db)) == 1
    assert delete_calls == [payload]
    assert not first_part.exists() and second_part.exists()


def test_deleted_state_survives_completion_failure_and_reconciles(tmp_path):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    payload.mkdir(parents=True)
    (payload / "part").write_bytes(b"x" * 4096)
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    candidate = _snapshot("h", content_path="/downloads/incomplete/h")["h"]
    executor = RecordingExecutor({"h": candidate})
    first_reclaimer = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        notification_chat_ids=["100"], now=lambda: 5_000,
    )

    def fail_complete(*_args, **_kwargs):
        raise RuntimeError("notification transaction unavailable")

    first_reclaimer.audit.complete = fail_complete
    first = first_reclaimer.run(
        {"h": candidate}, assessment=_assessment(), capacity_state="capacity_deadlock",
        free_bytes=0, target_free_bytes=10_000,
    )

    assert _capacity_reclaim_rows(db)[0]["state"] == "deleted"
    assert first.reclaimed == 0
    assert first.reclaimed_bytes >= 4096
    assert not payload.exists()

    recovery = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        notification_chat_ids=["100"], now=lambda: 5_001,
    )
    recovery.run(
        {"h": candidate}, assessment=_assessment(), capacity_state="capacity_deadlock",
        free_bytes=0, target_free_bytes=10_000,
    )

    assert _capacity_reclaim_rows(db)[0]["state"] == "reclaimed"
    assert len(_capacity_reclaim_notifications(db)) == 1
    assert len([post for post in executor.posts if post[0].endswith("/recheck")]) == 2


def test_recheck_failure_persists_pending_and_next_run_completes_once(tmp_path):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    payload.mkdir(parents=True)
    (payload / "part").write_bytes(b"x" * 4096)
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    candidate = _snapshot("h", content_path="/downloads/incomplete/h")["h"]

    class RetryRecheckExecutor(RecordingExecutor):
        def __init__(self):
            super().__init__({"h": candidate})
            self.fail_recheck = True

        def qbt_post(self, path, body):
            super().qbt_post(path, body)
            if path.endswith("/recheck") and self.fail_recheck:
                self.fail_recheck = False
                raise RuntimeError("recheck unavailable")

    executor = RetryRecheckExecutor()
    reclaimer = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        notification_chat_ids=["100"], now=lambda: 5_000,
    )

    first = reclaimer.run(
        {"h": candidate}, assessment=_assessment(), capacity_state="capacity_deadlock",
        free_bytes=0, target_free_bytes=10_000,
    )
    assert first.reclaimed == 0
    assert first.reclaimed_bytes >= 4096
    assert _capacity_reclaim_rows(db)[0]["state"] == "recheck_pending"
    assert len(_capacity_reclaim_notifications(db)) == 1

    reclaimer.run(
        {"h": candidate}, assessment=_assessment(), capacity_state="capacity_deadlock",
        free_bytes=0, target_free_bytes=10_000,
    )
    reclaimer.run(
        {"h": candidate}, assessment=_assessment(), capacity_state="capacity_deadlock",
        free_bytes=0, target_free_bytes=10_000,
    )

    assert _capacity_reclaim_rows(db)[0]["state"] == "reclaimed"
    assert len(_capacity_reclaim_notifications(db)) == 2
    assert len([post for post in executor.posts if post[0].endswith("/recheck")]) == 2


@pytest.mark.parametrize(
    ("seed_state", "path_exists", "expected_state", "warning_count"),
    [
        ("stopping", True, "aborted_paused", 1),
        ("stopping", False, "aborted_paused", 1),
        ("deleting", True, "partial_or_unknown", 1),
        ("deleting", False, "deleted", 0),
    ],
)
def test_restart_reconciliation_converges_seeded_states(
    tmp_path, seed_state, path_exists, expected_state, warning_count
):
    from qbt_orchestrator.capacity_reclaim import (
        CapacityReclaimAuditStore,
        DeadPartialReclaimer,
    )
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    managed.mkdir()
    payload = managed / "h"
    if path_exists:
        payload.mkdir()
        (payload / "part").write_bytes(b"x" * 4096)
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    candidate = {
        "hash": "h",
        "name": "Recovery Name",
        "magnet_uri": "mag" + "net:?xt=urn:btih:h",
        "host_path": str(payload),
        "content_path": "/downloads/incomplete/h",
        "allocated_bytes": 4096,
        "completed_bytes": 100,
        "progress": 0.0,
        "reclaimable_since": 1_000,
        "capacity_generation": 4,
        "capacity_reason": "stale_without_complete_source",
        "assessment_json": "{}",
    }
    audit = CapacityReclaimAuditStore(
        db, notification_chat_ids=["100"], now=lambda: 4_999,
    )
    reservation = audit.reserve(candidate)
    if seed_state != "stopping":
        con = sqlite3.connect(db)
        con.execute(
            "update capacity_reclaims set state=? where id=?",
            (seed_state, reservation["reclaim_id"]),
        )
        con.commit()
        con.close()

    stopped = {
        **_snapshot("h", content_path="/downloads/incomplete/h")["h"],
        "state": "stoppedDL",
    }
    reclaimer = DeadPartialReclaimer(
        db, RecordingExecutor({"h": stopped}), host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        notification_chat_ids=["100"], now=lambda: 5_000,
    )
    reclaimer.run(
        {}, assessment=_assessment(), capacity_state="capacity_deadlock",
        free_bytes=0, target_free_bytes=10_000,
    )

    assert _capacity_reclaim_rows(db)[0]["state"] == expected_state
    assert len(_capacity_reclaim_notifications(db)) == warning_count
