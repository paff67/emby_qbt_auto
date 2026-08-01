#!/usr/bin/env python3
from __future__ import annotations

import os
import sqlite3
import stat as statmod
import sys
import tempfile
import json
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))


@pytest.fixture(autouse=True)
def _normalize_overlay_st_dev(monkeypatch, request):
    """Normalize overlay file/parent st_dev skew for same-device success paths.

    Opt out with ``@pytest.mark.real_st_dev`` when a test intentionally asserts
    cross-device or mount-boundary rejection.
    """
    if request.node.get_closest_marker("real_st_dev") is not None:
        return
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer

    original = DeadPartialReclaimer._lstat

    def _lstat(path):
        path = Path(path)
        metadata = original(path)
        st_dev = int(metadata.st_dev)
        if statmod.S_ISREG(int(metadata.st_mode)):
            try:
                st_dev = int(original(path.parent).st_dev)
            except OSError:
                pass
        return SimpleNamespace(
            st_mode=int(metadata.st_mode),
            st_ino=int(metadata.st_ino),
            st_dev=st_dev,
            st_nlink=int(getattr(metadata, "st_nlink", 1) or 1),
            st_uid=int(getattr(metadata, "st_uid", 0) or 0),
            st_gid=int(getattr(metadata, "st_gid", 0) or 0),
            st_size=int(getattr(metadata, "st_size", 0) or 0),
            st_blocks=int(getattr(metadata, "st_blocks", 0) or 0),
            st_file_attributes=int(
                getattr(metadata, "st_file_attributes", 0) or 0
            ),
        )

    monkeypatch.setattr(DeadPartialReclaimer, "_lstat", staticmethod(_lstat))
    original_ismount = os.path.ismount

    def _ismount(value):
        path = Path(value)
        try:
            mode = path.lstat().st_mode
        except OSError:
            return original_ismount(value)
        if not statmod.S_ISDIR(mode):
            return False
        return original_ismount(value)

    monkeypatch.setattr(os.path, "ismount", _ismount)


class RecordingExecutor:
    def __init__(self, info=None, *, files=None, stop_updates_state=True):
        self.posts = []
        self.post_lease_tokens = []
        self.blocked_posts = []
        self.hash_mutation_leases = {}
        self.info = {str(key): dict(value) for key, value in (info or {}).items()}
        self.files = {
            str(key): [dict(row) for row in rows]
            for key, rows in (files or {}).items()
        }
        self.qbt = self
        self.stop_updates_state = bool(stop_updates_state)

    @staticmethod
    def _payload_hashes(payload):
        hashes = set()
        for key in ("hash", "hashes"):
            raw = str(payload.get(key) or "")
            hashes.update(
                value.strip().lower()
                for value in raw.split("|")
                if value.strip()
            )
        return hashes

    def acquire_hash_mutation_lease(self, torrent_hash, token):
        torrent_hash = str(torrent_hash).strip().lower()
        owner = self.hash_mutation_leases.get(torrent_hash)
        if owner not in {None, str(token)}:
            return False
        self.hash_mutation_leases[torrent_hash] = str(token)
        return True

    def hydrate_hash_mutation_lease(self, torrent_hash, token):
        return self.acquire_hash_mutation_lease(torrent_hash, token)

    def release_hash_mutation_lease(self, torrent_hash, token):
        torrent_hash = str(torrent_hash).strip().lower()
        if self.hash_mutation_leases.get(torrent_hash) != str(token):
            return False
        del self.hash_mutation_leases[torrent_hash]
        return True

    def qbt_post(self, path, payload, *, lease_token=None):
        hashes = self._payload_hashes(payload)
        if any(
            self.hash_mutation_leases.get(torrent_hash) not in {None, lease_token}
            for torrent_hash in hashes
        ):
            self.blocked_posts.append((path, payload))
            return False
        self.posts.append((path, payload))
        self.post_lease_tokens.append(lease_token)
        if path.endswith("/stop") and self.stop_updates_state:
            torrent_hash = str(payload["hashes"])
            self.info.setdefault(torrent_hash, {})["state"] = "stoppedDL"
        return True

    def torrent_info(self, torrent_hash, timeout=None):
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

    def torrent_files(self, torrent_hash, timeout=None):
        return [
            dict(row)
            for row in self.files.get(
                str(torrent_hash),
                [{"index": 0, "size": 1, "priority": 1}],
            )
        ]

    def get_maindata(self, rid, timeout=None):
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
    con.execute(
        "insert or replace into capacity_state("
        "id,scheduler_mode,state,entered_at,last_evaluated_at,reason,details_json,"
        "assessment_generation) values(1,'drain','capacity_deadlock',1,1,'test','{}',?)",
        (generation if current_generation is None else current_generation,),
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
    con.execute(
        "insert or replace into capacity_state("
        "id,scheduler_mode,state,entered_at,last_evaluated_at,reason,details_json,"
        "assessment_generation) values(1,'drain','capacity_deadlock',1,1,'test','{}',1)"
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
            dry_run=False, disk_free_bytes=lambda _path: 0,
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
        def qbt_post(self, path, payload, *, lease_token=None):
            super().qbt_post(path, payload, lease_token=lease_token)
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
            dry_run=False, disk_free_bytes=lambda _path: 0, min_dead_age_sec=3_600, min_reclaim_bytes=1,
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
            dry_run=False, disk_free_bytes=lambda _path: 0,
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
        def qbt_post(self, path, payload, *, lease_token=None):
            super().qbt_post(path, payload, lease_token=lease_token)
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
            dry_run=False, disk_free_bytes=lambda _path: 0,
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
        dry_run=False, disk_free_bytes=lambda _path: 0, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
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
        dry_run=False, disk_free_bytes=lambda _path: 0,
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
        dry_run=False, disk_free_bytes=lambda _path: 0, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
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
        def qbt_post(self, path, payload, *, lease_token=None):
            super().qbt_post(path, payload, lease_token=lease_token)
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
        dry_run=False, disk_free_bytes=lambda _path: 0, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        now=lambda: 5_000,
    )

    result = reclaimer.run(
        {"h": info}, assessment=_assessment(), capacity_state="capacity_deadlock",
        free_bytes=0, target_free_bytes=10_000,
    )

    assert result.planned == 0
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
        def qbt_post(self, path, payload, *, lease_token=None):
            super().qbt_post(path, payload, lease_token=lease_token)
            if path.endswith("/stop"):
                self.info["h"]["content_path"] = "/downloads/incomplete/changed"

    executor = PathRaceExecutor({"h": info})
    reclaimer = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, disk_free_bytes=lambda _path: 0, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
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


def test_live_reclaim_blocks_active_protection_after_stop(tmp_path):
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
    protection_errors = []

    class ProtectionRaceExecutor(RecordingExecutor):
        def qbt_post(self, path, payload, *, lease_token=None):
            super().qbt_post(path, payload, lease_token=lease_token)
            if path.endswith("/stop"):
                con = sqlite3.connect(db)
                try:
                    con.execute(
                        "insert into torrent_jobs(hash,job_type,state,created_at,updated_at) "
                        "values('h','upload','running',0,0)"
                    )
                    con.commit()
                except sqlite3.IntegrityError as exc:
                    protection_errors.append(str(exc))
                finally:
                    con.close()

    executor = ProtectionRaceExecutor({"h": info})
    reclaimer = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, disk_free_bytes=lambda _path: 0, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        now=lambda: 5_000,
    )

    result = reclaimer.run(
        {"h": info}, assessment=_assessment(), capacity_state="capacity_deadlock",
        free_bytes=0, target_free_bytes=10_000,
    )

    # SQLite triggers are gone; post-stop Python fence catches open jobs.
    assert result.reclaimed == 0
    assert result.planned == 0
    assert result.rejection_counts["active_protection"] == 1
    assert protection_errors == []
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
        dry_run=False, disk_free_bytes=lambda _path: 0, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
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
    assert rows[0]["state"] == "cancelled"
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


def _direct_reclaim_candidate(tmp_path: Path, torrent_hash: str = "h") -> dict:
    return {
        "hash": torrent_hash,
        "name": f"Reclaim {torrent_hash}",
        "magnet_uri": "mag" + f"net:?xt=urn:btih:{torrent_hash}",
        "host_path": str((tmp_path / "incomplete" / torrent_hash).resolve()),
        "content_path": f"/downloads/incomplete/{torrent_hash}",
        "allocated_bytes": 4096,
        "completed_bytes": 100,
        "progress": 0.0,
        "no_progress_since": 100,
        "reclaimable_since": 1_000,
        "capacity_generation": 4,
        "capacity_reason": "stale_without_complete_source",
        "assessment_json": "{}",
        "file_selection_fingerprint": "a" * 64,
        "target_free_bytes": 10_000,
    }


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
        dry_run=False, disk_free_bytes=lambda _path: 0, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
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
        dry_run=False, disk_free_bytes=lambda _path: 0, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
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
        dry_run=False, disk_free_bytes=lambda _path: 0, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
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
        dry_run=False, disk_free_bytes=lambda _path: 0, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
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

        def torrent_info(self, torrent_hash, timeout=None):
            current = dict(self.responses.pop(0))
            # Keep post-stop inventory (get_maindata) aligned with the live view.
            self.info[str(torrent_hash)] = dict(current)
            return current

    executor = StopWindowExecutor()
    reclaimer = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, disk_free_bytes=lambda _path: 0, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        now=lambda: 5_000,
    )

    result = reclaimer.run(
        {"h": snapshot}, assessment=_assessment(), capacity_state="capacity_deadlock",
        free_bytes=0, target_free_bytes=10_000,
    )

    assert result.planned == 0
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
        def torrent_info(self, torrent_hash, timeout=None):
            return dict(current)

    executor = RawInfoExecutor({"h": current})
    reclaimer = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, disk_free_bytes=lambda _path: 0, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
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
    # Conflicting map-key vs row hash must fail closed at post-stop inventory.
    after_stop = {**snapshot, "state": "stoppedDL", "hash": "other"}

    class StopWindowExecutor(RecordingExecutor):
        def __init__(self):
            super().__init__({"h": before_stop})
            self.responses = [before_stop, after_stop]

        def torrent_info(self, torrent_hash, timeout=None):
            current = dict(self.responses.pop(0))
            self.info[str(torrent_hash)] = dict(current)
            return current

    executor = StopWindowExecutor()
    reclaimer = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, disk_free_bytes=lambda _path: 0, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        now=lambda: 5_000,
    )

    result = reclaimer.run(
        {"h": snapshot}, assessment=_assessment(), capacity_state="capacity_deadlock",
        free_bytes=0, target_free_bytes=10_000,
    )

    assert result.planned == 0
    assert result.reclaimed == 0
    assert result.rejection_counts["path_inventory_failed"] == 1
    assert executor.posts == [("/api/v2/torrents/stop", {"hashes": "h"})]
    assert payload.exists()
    _assert_capacity_reclaim_aborted_paused(db, "path_inventory_failed")


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
        def qbt_post(self, path, payload, *, lease_token=None):
            super().qbt_post(path, payload, lease_token=lease_token)
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
        dry_run=False, disk_free_bytes=lambda _path: 0, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
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
        dry_run=False, disk_free_bytes=lambda _path: 0, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
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
        dry_run=False, disk_free_bytes=lambda _path: 0, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
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

        def torrent_info(self, torrent_hash, timeout=None):
            current = dict(self.responses.pop(0))
            self.info[str(torrent_hash)] = dict(current)
            return current

    executor = StopWindowExecutor()
    reclaimer = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, disk_free_bytes=lambda _path: 0, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        now=lambda: 5_000,
    )

    result = reclaimer.run(
        {"h": snapshot}, assessment=_assessment(), capacity_state="capacity_deadlock",
        free_bytes=0, target_free_bytes=10_000,
    )

    assert result.planned == 0
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
        def get_maindata(self, rid, timeout=None):
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
        dry_run=False, disk_free_bytes=lambda _path: 0, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
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
        def get_maindata(self, rid, timeout=None):
            if inventory_mode == "api-error":
                raise RuntimeError("sync unavailable")
            if inventory_mode == "missing-torrents":
                return {"full_update": True}
            return {"full_update": False, "torrents": {"h": dict(candidate)}}

    executor = BrokenInventoryExecutor({"h": candidate})
    reclaimer = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, disk_free_bytes=lambda _path: 0, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
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
        def torrent_info(self, torrent_hash, timeout=None):
            return dict(current)

    executor = RawInfoExecutor({"h": current})
    reclaimer = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, disk_free_bytes=lambda _path: 0, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
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
        def get_maindata(self, rid, timeout=None):
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
        dry_run=False, disk_free_bytes=lambda _path: 0, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
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
        def get_maindata(self, rid, timeout=None):
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
        dry_run=False, disk_free_bytes=lambda _path: 0, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
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
        def get_maindata(self, rid, timeout=None):
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
        dry_run=False, disk_free_bytes=lambda _path: 0, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
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
        def torrent_info(self, torrent_hash, timeout=None):
            current = super().torrent_info(torrent_hash)
            if phase == "initial":
                current.update(live_change)
            return current

        def get_maindata(self, rid, timeout=None):
            assert rid == 0
            current = dict(self.info["h"])
            if phase == "final":
                current.update(live_change)
            return {"full_update": True, "torrents": {"h": current}}

    executor = ContentRaceExecutor({"h": candidate})
    reclaimer = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, disk_free_bytes=lambda _path: 0, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
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
        def get_maindata(self, rid, timeout=None):
            assert rid == 0
            return {
                "full_update": True,
                "torrents": {"h": {**self.info["h"], size_field: 2000}},
            }

    reclaimer = DeadPartialReclaimer(
        db, FinalSizeExecutor({"h": candidate}), host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, disk_free_bytes=lambda _path: 0, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
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

        def qbt_post(self, path, body, *, lease_token=None):
            super().qbt_post(path, body, lease_token=lease_token)
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
        dry_run=False, disk_free_bytes=lambda _path: 0, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
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


def test_pre_stop_and_post_quarantine_inventories_receive_request_timeout(tmp_path):
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
            self.info_timeouts = []

        def torrent_info(self, torrent_hash, timeout=None):
            self.info_timeouts.append(timeout)
            return super().torrent_info(torrent_hash, timeout=timeout)

        def get_maindata(self, rid, timeout=None):
            self.inventory_timeouts.append(timeout)
            return super().get_maindata(rid, timeout=timeout)

    executor = TimedInventoryExecutor()
    reclaimer = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, disk_free_bytes=lambda _path: 0, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        inventory_timeout_sec=0.03, now=lambda: 5_000,
    )

    result = reclaimer.run(
        {"h": candidate}, assessment=_assessment(), capacity_state="capacity_deadlock",
        free_bytes=0, target_free_bytes=10_000,
    )

    assert result.reclaimed == 1
    # Precheck uses torrent_info; only one post-stop inventory (no post-quarantine qBT auth).
    assert 0.03 in executor.info_timeouts
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

        def torrent_info(self, torrent_hash, timeout=None):
            self.info_calls += 1
            if self.info_calls == 2:
                self.info[str(torrent_hash)] = dict(after_stop)
                return dict(after_stop)
            return dict(candidate)

    executor = StopProgressExecutor()
    reclaimer = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, disk_free_bytes=lambda _path: 0, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
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
    assert first.planned == 0
    assert first.reclaimed == 0
    assert first.rejection_counts["progress_evidence_changed"] == 1
    assert second.rejection_counts["reclaim_already_recorded"] == 1
    assert rows[0]["state"] == "cancelled"
    assert rows[0]["recheck_state"] == "not_requested"
    assert len(notifications) == 1
    warning_payload = json.loads(notifications[0]["payload_json"])
    assert warning_payload["reason"] == "progress_evidence_changed"
    assert warning_payload["automatic"] is True
    assert "requires_confirmation" not in warning_payload
    assert "自动" in notifications[0]["message"]
    assert "交还调度器" in notifications[0]["message"]
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
        def qbt_post(self, path, body, *, lease_token=None):
            super().qbt_post(path, body, lease_token=lease_token)
            if path.endswith("/stop"):
                raise RuntimeError("123456:secret stop unavailable")

    executor = StopFailureExecutor({"h": candidate})
    reclaimer = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, disk_free_bytes=lambda _path: 0, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
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
    assert row["state"] == "cancelled"
    assert "123456:secret" not in row["recheck_error"]
    assert len(notifications) == 2
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
        dry_run=False, disk_free_bytes=lambda _path: 0, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        notification_chat_ids=["100"], now=lambda: 5_000,
    )
    delete_calls = []

    def partial_delete(_path, _identity):
        delete_calls.append(_path)
        (_path / "first.part").unlink()
        raise OSError("partial deletion")

    reclaimer._delete_quarantine_path = partial_delete

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
    assert row["state"] == "cancelled"
    assert row["recheck_state"] == "not_requested"
    assert len(_capacity_reclaim_notifications(db)) == 2
    quarantine_path = Path(row["quarantine_path"])
    assert delete_calls == [quarantine_path]
    assert payload.exists()
    assert not (payload / "first.part").exists()
    assert (payload / "second.part").exists()
    assert not quarantine_path.exists()


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
        dry_run=False, disk_free_bytes=lambda _path: 0, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
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
        dry_run=False, disk_free_bytes=lambda _path: 0, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
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

        def qbt_post(self, path, body, *, lease_token=None):
            super().qbt_post(path, body, lease_token=lease_token)
            if path.endswith("/recheck") and self.fail_recheck:
                self.fail_recheck = False
                raise RuntimeError("recheck unavailable")

    executor = RetryRecheckExecutor()
    reclaimer = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, disk_free_bytes=lambda _path: 0, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
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
        ("stopping", True, "cancelled", 1),
        ("stopping", False, "cancelled", 1),
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
        "no_progress_since": 100,
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
        dry_run=False, disk_free_bytes=lambda _path: 0, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        notification_chat_ids=["100"], now=lambda: 5_000,
    )
    reclaimer.run(
        {}, assessment=_assessment(), capacity_state="capacity_deadlock",
        free_bytes=0, target_free_bytes=10_000,
    )

    assert _capacity_reclaim_rows(db)[0]["state"] == expected_state
    assert len(_capacity_reclaim_notifications(db)) == warning_count


def test_capacity_reclaim_lock_helper_keeps_all_nonreleased_states_locked(tmp_path):
    from qbt_orchestrator.capacity_reclaim import (
        RECLAIM_LOCKED_STATES,
        capacity_reclaim_locked_hashes,
    )
    from qbt_orchestrator.db import migrate

    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    states = sorted(RECLAIM_LOCKED_STATES | {"released", "cancelled"})
    con = sqlite3.connect(db)
    for index, state in enumerate(states):
        con.execute(
            "insert into capacity_reclaims("
            "reclaim_key,hash,name,magnet_uri,host_path,content_path,state,created_at,updated_at"
            ") values(?,?,?,?,?,?,?,?,?)",
            (
                f"key-{index}",
                state,
                state,
                "mag" + f"net:?xt=urn:btih:{state}",
                str(tmp_path / state),
                f"/downloads/incomplete/{state}",
                state,
                1,
                1,
            ),
        )
    con.commit()
    con.close()

    assert capacity_reclaim_locked_hashes(db) == set(RECLAIM_LOCKED_STATES)
    assert {"aborted_paused", "reclaimed", "failed"}.isdisjoint(
        RECLAIM_LOCKED_STATES
    )


def _seed_post_delete_reclaim_row(db: Path, tmp_path: Path, *, state: str = "deleted"):
    from qbt_orchestrator.capacity_reclaim import CapacityReclaimAuditStore

    managed = tmp_path / "incomplete"
    managed.mkdir(parents=True, exist_ok=True)
    (managed / "h").mkdir(exist_ok=True)
    _capacity_health(db, "h")
    audit = CapacityReclaimAuditStore(
        db, notification_chat_ids=["100"], now=lambda: 5_000
    )
    reservation = audit.reserve(_direct_reclaim_candidate(tmp_path, "h"))
    assert reservation["reserved"] is True
    reclaim_id = int(reservation["reclaim_id"])
    con = sqlite3.connect(db)
    con.execute(
        "update capacity_reclaims set state=?, recheck_state='requested',"
        "recheck_error=null where id=?",
        (state, reclaim_id),
    )
    con.commit()
    con.close()
    return audit, reclaim_id, _direct_reclaim_candidate(tmp_path, "h")


def test_mark_tag_pending_audit_redacts_error_and_queues_warning(tmp_path):
    from qbt_orchestrator.db import migrate

    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    audit, reclaim_id, _candidate = _seed_post_delete_reclaim_row(
        db, tmp_path, state="deleted"
    )

    result = audit.mark_tag_pending(
        reclaim_id,
        4,
        "HTTP 503 bot 123456:secret-token unavailable",
    )

    assert result["state"] == "tag_pending"
    row = _capacity_reclaim_rows(db)[0]
    assert row["state"] == "tag_pending"
    assert row["recheck_state"] == "requested"
    assert str(row["recheck_error"]).startswith("post_reclaim_tag_failed:")
    assert "123456:secret-token" not in str(row["recheck_error"])
    notices = _capacity_reclaim_notifications(db)
    assert len(notices) == 1
    assert notices[0]["level"] == "warning"
    payload = json.loads(notices[0]["payload_json"])
    assert payload["hash"] == "h"
    assert payload["name"] == "Reclaim h"
    assert payload["reclaim_id"] == reclaim_id
    assert payload["reason"] == "post_reclaim_tag_failed"
    assert payload["archive_tags"] == ["capacity-reclaimed", "hold"]
    assert "123456:secret-token" not in notices[0]["message"]
    assert "capacity-reclaimed, hold" in notices[0]["message"]

    # Idempotent refresh in the same hour must not create a second notice row.
    again = audit.mark_tag_pending(
        reclaim_id, 4, "HTTP 503 bot 123456:secret-token unavailable"
    )
    assert again["state"] == "tag_pending"
    assert len(_capacity_reclaim_notifications(db)) == 1
    assert len(_capacity_reclaim_rows(db)) == 1


def test_complete_tag_outcome_applied_clears_error_from_tag_pending(tmp_path):
    from qbt_orchestrator.db import migrate

    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    audit, reclaim_id, candidate = _seed_post_delete_reclaim_row(
        db, tmp_path, state="deleted"
    )
    audit.mark_tag_pending(reclaim_id, 4, "temporary tag failure")

    completed = audit.complete(
        reclaim_id,
        candidate,
        recheck_error=None,
        tag_outcome="applied",
    )

    assert completed["state"] == "reclaimed"
    assert completed["tag_outcome"] == "applied"
    row = _capacity_reclaim_rows(db)[0]
    assert row["state"] == "reclaimed"
    assert row["recheck_state"] == "requested"
    assert row["recheck_error"] is None
    notices = [
        notice
        for notice in _capacity_reclaim_notifications(db)
        if notice["level"] == "info"
    ]
    assert len(notices) == 1
    assert "标签：capacity-reclaimed, hold" in notices[0]["message"]
    payload = json.loads(notices[0]["payload_json"])
    assert payload["archive_tags"] == ["capacity-reclaimed", "hold"]
    assert payload["tag_outcome"] == "applied"


def test_complete_tag_outcome_torrent_absent_records_terminal_warning(tmp_path):
    from qbt_orchestrator.db import migrate

    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    audit, reclaim_id, candidate = _seed_post_delete_reclaim_row(
        db, tmp_path, state="recheck_pending"
    )

    completed = audit.complete(
        reclaim_id,
        candidate,
        recheck_error=None,
        tag_outcome="torrent_absent",
    )

    assert completed["state"] == "reclaimed"
    assert completed["tag_outcome"] == "torrent_absent"
    row = _capacity_reclaim_rows(db)[0]
    assert row["state"] == "reclaimed"
    assert row["recheck_error"] == "torrent_absent_after_reclaim"
    notices = _capacity_reclaim_notifications(db)
    assert len(notices) == 1
    assert notices[0]["level"] == "warning"
    assert "无法添加归档标签" in notices[0]["message"]
    payload = json.loads(notices[0]["payload_json"])
    assert payload["tag_outcome"] == "torrent_absent"
    assert payload["recheck_error"] == "torrent_absent_after_reclaim"
    assert payload["archive_tags"] == ["capacity-reclaimed", "hold"]


CAPACITY_RECLAIM_TRIGGER_NAMES = (
    "trg_capacity_reclaim_lock_job_insert",
    "trg_capacity_reclaim_lock_job_update",
    "trg_capacity_reclaim_lock_reservation_insert",
    "trg_capacity_reclaim_lock_reservation_update",
    "trg_capacity_reclaim_lock_soak_insert",
    "trg_capacity_reclaim_lock_soak_update",
    "trg_capacity_reclaim_fence_assessment_insert",
    "trg_capacity_reclaim_fence_assessment_update",
    "trg_capacity_reclaim_fence_assessment_delete",
    "trg_capacity_reclaim_fence_health_insert",
    "trg_capacity_reclaim_fence_health_update",
    "trg_capacity_reclaim_fence_health_delete",
    "trg_capacity_reclaim_fence_capacity_state_insert",
    "trg_capacity_reclaim_fence_capacity_state_update",
    "trg_capacity_reclaim_fence_capacity_state_delete",
)


def test_capacity_reclaim_migration_drops_all_capacity_triggers_idempotently(tmp_path):
    from qbt_orchestrator.db import migrate, migration_sql

    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    migrate(db, dry_run=False)

    con = sqlite3.connect(db)
    reclaim_columns = {
        row[1] for row in con.execute("pragma table_info(capacity_reclaims)")
    }
    triggers = {
        row[0]
        for row in con.execute(
            "select name from sqlite_master where type='trigger'"
        ).fetchall()
    }
    versions = {
        int(row[0])
        for row in con.execute("select version from schema_migrations").fetchall()
    }
    con.close()

    assert len(CAPACITY_RECLAIM_TRIGGER_NAMES) == 15
    assert set(CAPACITY_RECLAIM_TRIGGER_NAMES).isdisjoint(triggers)
    assert 19 in versions
    assert {"file_selection_fingerprint", "target_free_bytes"} <= reclaim_columns
    sql = "\n".join(migration_sql())
    for name in CAPACITY_RECLAIM_TRIGGER_NAMES:
        assert f"drop trigger if exists {name}" in sql.lower()
        assert f"create trigger {name}" not in sql.lower()
        assert f"create trigger if not exists {name}" not in sql.lower()



def test_mixed_case_open_job_blocks_reclaim_and_preserves_payload(tmp_path):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    payload.mkdir(parents=True)
    (payload / "part").write_bytes(b"keep" * 1024)
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    con = sqlite3.connect(db)
    con.execute(
        "insert into torrent_jobs(hash,job_type,state) "
        "values(' H ','upload','running')"
    )
    con.commit()
    con.close()
    candidate = _snapshot(
        "h", content_path="/downloads/incomplete/h"
    )["h"]
    reclaimer = DeadPartialReclaimer(
        db,
        RecordingExecutor({"h": candidate}),
        host_downloads=tmp_path,
        container_downloads="/downloads",
        managed_root=managed,
        dry_run=False,
        disk_free_bytes=lambda _path: 0,
        min_reclaimable_age_sec=3_600,
        min_reclaim_bytes=1,
        now=lambda: 5_000,
    )

    result = reclaimer.run(
        {" H ": {**candidate, "hash": " H "}},
        assessment=_assessment(),
        capacity_state="capacity_deadlock",
        free_bytes=0,
        target_free_bytes=10_000,
    )

    assert result.reclaimed == 0
    assert result.rejection_counts["open_job"] == 1
    assert payload.exists()
    assert _capacity_reclaim_rows(db) == []








@pytest.mark.parametrize(
    ("capacity_state", "capacity_generation"),
    [("progress_possible", 4), ("capacity_deadlock", 5)],
)
def test_reserve_requires_matching_persistent_capacity_episode(
    tmp_path, capacity_state, capacity_generation
):
    from qbt_orchestrator.capacity_reclaim import CapacityReclaimAuditStore
    from qbt_orchestrator.db import migrate

    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    con = sqlite3.connect(db)
    con.execute(
        "update capacity_state set state=?,assessment_generation=? where id=1",
        (capacity_state, capacity_generation),
    )
    con.commit()
    con.close()

    result = CapacityReclaimAuditStore(db, now=lambda: 5_000).reserve(
        _direct_reclaim_candidate(tmp_path)
    )

    assert result["reserved"] is False
    assert result["reason"] == "capacity_episode_changed"


def test_mark_deleting_rechecks_persistent_capacity_episode(tmp_path):
    from qbt_orchestrator.capacity_reclaim import CapacityReclaimAuditStore
    from qbt_orchestrator.db import migrate

    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    metadata = tmp_path.lstat()
    candidate = {
        **_direct_reclaim_candidate(tmp_path),
        "filesystem_dev": int(metadata.st_dev),
        "filesystem_ino": int(metadata.st_ino),
    }
    audit = CapacityReclaimAuditStore(db, now=lambda: 5_000)
    reservation = audit.reserve(candidate)
    con = sqlite3.connect(db)
    con.execute("update capacity_state set state='progress_possible' where id=1")
    con.commit()
    con.close()

    assert audit.mark_deleting(reservation["reclaim_id"], candidate) is False
    assert _capacity_reclaim_rows(db)[0]["state"] == "stopping"


@pytest.mark.parametrize(
    ("protection", "expected_reason"),
    [
        ("job", "open_job"),
        ("reservation", "active_reservation"),
        ("cooldown", "active_cooldown"),
    ],
)
def test_reserve_atomically_rejects_existing_protection_without_creating_lease(
    tmp_path, protection, expected_reason
):
    from qbt_orchestrator.capacity_reclaim import CapacityReclaimAuditStore
    from qbt_orchestrator.db import migrate

    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    con = sqlite3.connect(db)
    if protection == "job":
        con.execute(
            "insert into torrent_jobs(hash,job_type,state) values('h','upload','queued')"
        )
    elif protection == "reservation":
        con.execute(
            "insert into resource_reservations(hash,kind,bytes,state,expires_at) "
            "values('h','batch',1,'active',6000)"
        )
    else:
        con.execute(
            "insert into soak_state(hash,state,cooldown_until,updated_at) "
            "values('h','soak_cooldown',6000,1)"
        )
    con.commit()
    con.close()

    result = CapacityReclaimAuditStore(db, now=lambda: 5_000).reserve(
        _direct_reclaim_candidate(tmp_path)
    )

    assert result == {
        "reclaim_id": None,
        "state": None,
        "capacity_generation": 4,
        "reserved": False,
        "reason": expected_reason,
    }
    assert _capacity_reclaim_rows(db) == []


@pytest.mark.parametrize(
    ("mutation", "expected_reason"),
    [
        ("assessment_generation", "stale_assessment"),
        ("health_generation", "stale_health_generation"),
        ("capacity_viable", "capacity_viable"),
        ("reclaimable_since", "reclaimable_changed"),
        ("no_progress_since", "progress_evidence_changed"),
    ],
)
def test_reserve_atomically_revalidates_capacity_evidence(
    tmp_path, mutation, expected_reason
):
    from qbt_orchestrator.capacity_reclaim import CapacityReclaimAuditStore
    from qbt_orchestrator.db import migrate

    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    con = sqlite3.connect(db)
    if mutation == "assessment_generation":
        con.execute(
            "update capacity_assessment_state set current_generation=5 where id=1"
        )
    elif mutation == "health_generation":
        con.execute("update torrent_health set capacity_generation=5 where hash='h'")
    elif mutation == "capacity_viable":
        con.execute("update torrent_health set capacity_viable=1 where hash='h'")
    elif mutation == "reclaimable_since":
        con.execute("update torrent_health set reclaimable_since=999 where hash='h'")
    else:
        con.execute("update torrent_health set no_progress_since=101 where hash='h'")
    con.commit()
    con.close()

    result = CapacityReclaimAuditStore(db, now=lambda: 5_000).reserve(
        _direct_reclaim_candidate(tmp_path)
    )

    assert result["reserved"] is False
    assert result["reason"] == expected_reason
    assert _capacity_reclaim_rows(db) == []


def test_mark_deleting_revalidates_protection_inside_final_transaction(tmp_path):
    from qbt_orchestrator.capacity_reclaim import CapacityReclaimAuditStore
    from qbt_orchestrator.db import migrate

    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    metadata = tmp_path.lstat()
    candidate = {
        **_direct_reclaim_candidate(tmp_path),
        "filesystem_dev": int(metadata.st_dev),
        "filesystem_ino": int(metadata.st_ino),
    }
    audit = CapacityReclaimAuditStore(db, now=lambda: 5_000)
    reservation = audit.reserve(candidate)
    con = sqlite3.connect(db)
    con.execute(
        "insert into torrent_jobs(hash,job_type,state) values('h','upload','running')"
    )
    con.commit()
    con.close()

    assert audit.mark_deleting(reservation["reclaim_id"], candidate) is False
    assert _capacity_reclaim_rows(db)[0]["state"] == "stopping"



@pytest.mark.parametrize("payload_kind", ["file", "directory"])
def test_live_reclaim_atomically_quarantines_identity_before_deletion(
    tmp_path, payload_kind
):
    from qbt_orchestrator.capacity_reclaim import (
        QUARANTINE_DIRNAME,
        DeadPartialReclaimer,
    )
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    managed.mkdir()
    payload = managed / "h"
    if payload_kind == "directory":
        payload.mkdir()
        (payload / "part").write_bytes(b"x" * 4096)
    else:
        payload.write_bytes(b"x" * 4096)
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    candidate = _snapshot("h", content_path="/downloads/incomplete/h")["h"]
    executor = RecordingExecutor({"h": candidate})
    reclaimer = DeadPartialReclaimer(
        db,
        executor,
        host_downloads=tmp_path,
        container_downloads="/downloads",
        managed_root=managed,
        dry_run=False, disk_free_bytes=lambda _path: 0,
        min_reclaimable_age_sec=3_600,
        min_reclaim_bytes=1,
        now=lambda: 5_000,
    )
    before = reclaimer._lstat(payload)
    operations = []
    original_rename = reclaimer._rename_to_quarantine
    original_delete = reclaimer._delete_quarantine_path

    def traced_rename(source, destination, identity):
        assert source == payload.resolve()
        assert source.exists() and not destination.exists()
        original_rename(source, destination, identity)
        assert not source.exists() and destination.exists()
        operations.append(("renamed", destination))

    def traced_delete(destination, identity):
        assert operations == [("renamed", destination)]
        assert destination.exists() and not payload.exists()
        original_delete(destination, identity)
        operations.append(("deleted", destination))

    reclaimer._rename_to_quarantine = traced_rename
    reclaimer._delete_quarantine_path = traced_delete

    result = reclaimer.run(
        {"h": candidate},
        assessment=_assessment(),
        capacity_state="capacity_deadlock",
        free_bytes=0,
        target_free_bytes=10_000,
    )

    row = _capacity_reclaim_rows(db)[0]
    expected_quarantine = managed / QUARANTINE_DIRNAME / f"reclaim-{row['id']}"
    assert result.reclaimed == 1
    assert [item[0] for item in operations] == ["renamed", "deleted"]
    assert not payload.exists() and not expected_quarantine.exists()
    assert row["state"] == "reclaimed"
    assert row["quarantine_path"] == str(expected_quarantine)
    assert row["filesystem_dev"] == int(before.st_dev)
    assert row["filesystem_ino"] == int(before.st_ino)
    if os.name != "nt":
        assert statmod.S_IMODE((managed / QUARANTINE_DIRNAME).stat().st_mode) == 0o700


def test_identity_swap_between_capture_and_rename_is_restored_without_delete(tmp_path):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    payload.mkdir(parents=True)
    original_part = payload / "original.part"
    original_part.write_bytes(b"original")
    displaced = tmp_path / "captured-original"
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    candidate = _snapshot("h", content_path="/downloads/incomplete/h")["h"]
    reclaimer = DeadPartialReclaimer(
        db,
        RecordingExecutor({"h": candidate}),
        host_downloads=tmp_path,
        container_downloads="/downloads",
        managed_root=managed,
        dry_run=False, disk_free_bytes=lambda _path: 0,
        min_reclaimable_age_sec=3_600,
        min_reclaim_bytes=1,
        now=lambda: 5_000,
    )
    original_rename = reclaimer._rename_to_quarantine
    delete_calls = []

    def swap_then_rename(source, destination, identity):
        source.rename(displaced)
        source.mkdir()
        (source / "replacement.part").write_bytes(b"replacement")
        original_rename(source, destination, identity)

    reclaimer._rename_to_quarantine = swap_then_rename
    reclaimer._delete_quarantine_path = lambda *_args: delete_calls.append(_args)

    result = reclaimer.run(
        {"h": candidate},
        assessment=_assessment(),
        capacity_state="capacity_deadlock",
        free_bytes=0,
        target_free_bytes=10_000,
    )

    assert result.reclaimed == 0
    assert _capacity_reclaim_rows(db)[0]["state"] == "partial_or_unknown"
    assert (payload / "replacement.part").read_bytes() == b"replacement"
    assert (displaced / "original.part").read_bytes() == b"original"
    assert delete_calls == []


@pytest.mark.skipif(os.name == "nt", reason="real symlink coverage runs on POSIX CI")
def test_symlink_swap_between_capture_and_rename_never_deletes_external_target(tmp_path):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    payload.mkdir(parents=True)
    (payload / "original.part").write_bytes(b"original")
    displaced = tmp_path / "captured-original"
    external = tmp_path / "external"
    external.mkdir()
    external_marker = external / "keep"
    external_marker.write_bytes(b"keep")
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    candidate = _snapshot("h", content_path="/downloads/incomplete/h")["h"]
    reclaimer = DeadPartialReclaimer(
        db,
        RecordingExecutor({"h": candidate}),
        host_downloads=tmp_path,
        container_downloads="/downloads",
        managed_root=managed,
        dry_run=False, disk_free_bytes=lambda _path: 0,
        min_reclaimable_age_sec=3_600,
        min_reclaim_bytes=1,
        now=lambda: 5_000,
    )
    original_rename = reclaimer._rename_to_quarantine

    def symlink_swap(source, destination, identity):
        source.rename(displaced)
        source.symlink_to(external, target_is_directory=True)
        original_rename(source, destination, identity)

    reclaimer._rename_to_quarantine = symlink_swap

    result = reclaimer.run(
        {"h": candidate},
        assessment=_assessment(),
        capacity_state="capacity_deadlock",
        free_bytes=0,
        target_free_bytes=10_000,
    )

    assert result.reclaimed == 0
    assert _capacity_reclaim_rows(db)[0]["state"] == "partial_or_unknown"
    assert payload.is_symlink()
    assert external_marker.read_bytes() == b"keep"


def test_payload_tree_fence_rejects_candidate_mount_before_rename(tmp_path, monkeypatch):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    payload.mkdir(parents=True)
    child = payload / "part"
    child.write_bytes(b"x" * 4096)
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    candidate = _snapshot("h", content_path="/downloads/incomplete/h")["h"]
    reclaimer = DeadPartialReclaimer(
        db,
        RecordingExecutor({"h": candidate}),
        host_downloads=tmp_path,
        container_downloads="/downloads",
        managed_root=managed,
        dry_run=False, disk_free_bytes=lambda _path: 0,
        min_reclaimable_age_sec=3_600,
        min_reclaim_bytes=1,
        now=lambda: 5_000,
    )
    original_ismount = os.path.ismount
    monkeypatch.setattr(
        os.path,
        "ismount",
        lambda value: Path(value) == payload.resolve() or original_ismount(value),
    )
    rename_calls = []
    reclaimer._rename_to_quarantine = lambda *_args: rename_calls.append(_args)

    result = reclaimer.run(
        {"h": candidate},
        assessment=_assessment(),
        capacity_state="capacity_deadlock",
        free_bytes=0,
        target_free_bytes=10_000,
    )

    assert result.reclaimed == 0
    assert result.rejection_counts["unsafe_filesystem_object"] == 1
    assert _capacity_reclaim_rows(db)[0]["state"] == "cancelled"
    assert payload.exists() and child.exists()
    assert rename_calls == []


def _reclaimer_for_fs_unit(tmp_path):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    managed.mkdir()
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    return DeadPartialReclaimer(
        db,
        RecordingExecutor(),
        host_downloads=tmp_path,
        container_downloads="/downloads",
        managed_root=managed,
        dry_run=True,
        min_reclaim_bytes=1,
        now=lambda: 5_000,
    )


def test_remove_node_deletes_same_device_directory_and_file(tmp_path):
    from qbt_orchestrator.capacity_reclaim import FilesystemIdentity

    reclaimer = _reclaimer_for_fs_unit(tmp_path)
    payload = reclaimer.managed_root / "victim"
    payload.mkdir()
    child = payload / "part"
    child.write_bytes(b"x" * 4096)
    identity = reclaimer._capture_payload_identity(payload)

    reclaimer._remove_node_no_follow(
        payload,
        identity.dev,
        expected_identity=identity,
    )

    assert not payload.exists()
    assert not child.exists()


@pytest.mark.real_st_dev
def test_remove_node_rejects_child_cross_device_without_mutating_tree(
    tmp_path, monkeypatch
):
    from qbt_orchestrator.capacity_reclaim import (
        FilesystemIdentity,
        UnsafeFilesystemObject,
    )

    reclaimer = _reclaimer_for_fs_unit(tmp_path)
    payload = reclaimer.managed_root / "victim"
    payload.mkdir()
    child = payload / "part"
    child.write_bytes(b"keep-child")
    marker = child.read_bytes()
    identity = reclaimer._capture_payload_identity(payload)
    original = reclaimer._lstat

    def _lstat(path):
        metadata = original(path)
        if Path(path) == child:
            return SimpleNamespace(
                st_mode=int(metadata.st_mode),
                st_ino=int(metadata.st_ino),
                st_dev=int(identity.dev) + 99,
                st_nlink=1,
                st_size=int(metadata.st_size),
                st_blocks=int(getattr(metadata, "st_blocks", 0) or 0),
            )
        return metadata

    monkeypatch.setattr(type(reclaimer), "_lstat", staticmethod(_lstat))

    with pytest.raises(UnsafeFilesystemObject, match="cross-device"):
        reclaimer._remove_node_no_follow(
            payload,
            identity.dev,
            expected_identity=identity,
        )

    assert payload.exists()
    assert child.exists()
    assert child.read_bytes() == marker


@pytest.mark.real_st_dev
def test_remove_node_rejects_child_mount_before_scandir(tmp_path, monkeypatch):
    from qbt_orchestrator.capacity_reclaim import UnsafeFilesystemObject

    reclaimer = _reclaimer_for_fs_unit(tmp_path)
    payload = reclaimer.managed_root / "victim"
    payload.mkdir()
    nested = payload / "mnt"
    nested.mkdir()
    secret = nested / "secret.bin"
    secret.write_bytes(b"do-not-touch")
    identity = reclaimer._capture_payload_identity(payload)
    original_ismount = os.path.ismount
    scandir_calls = []
    original_scandir = os.scandir

    def _ismount(value):
        return Path(value) == nested or original_ismount(value)

    def _scandir(value):
        scandir_calls.append(Path(value))
        return original_scandir(value)

    monkeypatch.setattr(os.path, "ismount", _ismount)
    monkeypatch.setattr(os, "scandir", _scandir)

    with pytest.raises(UnsafeFilesystemObject, match="mount point rejected"):
        reclaimer._remove_node_no_follow(
            payload,
            identity.dev,
            expected_identity=identity,
        )

    assert nested not in scandir_calls
    assert secret.exists()
    assert secret.read_bytes() == b"do-not-touch"
    assert payload.exists()


@pytest.mark.real_st_dev
def test_remove_node_rejects_child_symlink_without_following(tmp_path):
    from qbt_orchestrator.capacity_reclaim import UnsafeFilesystemObject

    reclaimer = _reclaimer_for_fs_unit(tmp_path)
    external = tmp_path / "outside"
    external.write_bytes(b"external")
    payload = reclaimer.managed_root / "victim"
    payload.mkdir()
    link = payload / "link"
    link.symlink_to(external)
    identity = reclaimer._capture_payload_identity(payload)

    with pytest.raises(UnsafeFilesystemObject, match="symlink rejected"):
        reclaimer._remove_node_no_follow(
            payload,
            identity.dev,
            expected_identity=identity,
        )

    assert link.is_symlink()
    assert external.read_bytes() == b"external"
    assert payload.exists()


def test_mark_quarantined_failure_restores_original_and_never_deletes(tmp_path):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    payload.mkdir(parents=True)
    marker = payload / "part"
    marker.write_bytes(b"x" * 4096)
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    candidate = _snapshot("h", content_path="/downloads/incomplete/h")["h"]
    reclaimer = DeadPartialReclaimer(
        db,
        RecordingExecutor({"h": candidate}),
        host_downloads=tmp_path,
        container_downloads="/downloads",
        managed_root=managed,
        dry_run=False, disk_free_bytes=lambda _path: 0,
        min_reclaimable_age_sec=3_600,
        min_reclaim_bytes=1,
        now=lambda: 5_000,
    )
    reclaimer.audit.mark_quarantined = lambda *_args, **_kwargs: (_ for _ in ()).throw(
        RuntimeError("database unavailable")
    )
    delete_calls = []
    reclaimer._delete_quarantine_path = lambda *_args: delete_calls.append(_args)

    result = reclaimer.run(
        {"h": candidate},
        assessment=_assessment(),
        capacity_state="capacity_deadlock",
        free_bytes=0,
        target_free_bytes=10_000,
    )

    row = _capacity_reclaim_rows(db)[0]
    assert result.reclaimed == 0
    assert row["state"] == "partial_or_unknown"
    assert marker.read_bytes() == b"x" * 4096
    assert not Path(row["quarantine_path"] or managed / "missing").exists()
    assert delete_calls == []


def test_mark_quarantined_and_restore_failures_are_both_persisted(tmp_path):
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
        db, RecordingExecutor({"h": candidate}), host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, disk_free_bytes=lambda _path: 0,
        min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        now=lambda: 5_000,
    )
    reclaimer.audit.mark_quarantined = lambda *_args, **_kwargs: (
        (_ for _ in ()).throw(RuntimeError("database unavailable"))
    )
    reclaimer._restore_from_quarantine = lambda *_args: False

    reclaimer.run(
        {"h": candidate}, assessment=_assessment(),
        capacity_state="capacity_deadlock", free_bytes=0,
        target_free_bytes=10_000,
    )

    row = _capacity_reclaim_rows(db)[0]
    assert row["state"] == "partial_or_unknown"
    assert "quarantine restore failed" in row["recheck_error"]
    assert "database unavailable" in row["recheck_error"]


def test_post_rename_validation_and_restore_failures_are_both_persisted(
    tmp_path,
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
    reclaimer = DeadPartialReclaimer(
        db, RecordingExecutor({"h": candidate}), host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, disk_free_bytes=lambda _path: 0,
        min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        now=lambda: 5_000,
    )
    original_lstat = reclaimer._lstat

    def changed_destination_lstat(path):
        metadata = original_lstat(path)
        if Path(path).name.startswith("reclaim-"):
            return SimpleNamespace(
                st_dev=metadata.st_dev,
                st_ino=int(metadata.st_ino) + 1,
                st_mode=metadata.st_mode,
                st_file_attributes=getattr(metadata, "st_file_attributes", 0),
            )
        return metadata

    reclaimer._lstat = changed_destination_lstat
    reclaimer._restore_from_quarantine = lambda *_args: False

    reclaimer.run(
        {"h": candidate}, assessment=_assessment(),
        capacity_state="capacity_deadlock", free_bytes=0,
        target_free_bytes=10_000,
    )

    row = _capacity_reclaim_rows(db)[0]
    assert row["state"] == "partial_or_unknown"
    assert "quarantine restore failed" in row["recheck_error"]
    assert "moved payload identity changed" in row["recheck_error"]


def test_restart_recovers_only_valid_seeded_quarantine(tmp_path):
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
    crashing = DeadPartialReclaimer(
        db,
        executor,
        host_downloads=tmp_path,
        container_downloads="/downloads",
        managed_root=managed,
        dry_run=False, disk_free_bytes=lambda _path: 0,
        min_reclaimable_age_sec=3_600,
        min_reclaim_bytes=1,
        now=lambda: 5_000,
    )
    crashing._delete_quarantine_path = lambda *_args: (_ for _ in ()).throw(
        KeyboardInterrupt("simulated crash")
    )

    with pytest.raises(KeyboardInterrupt, match="simulated crash"):
        crashing.run(
            {"h": candidate},
            assessment=_assessment(),
            capacity_state="capacity_deadlock",
            free_bytes=0,
            target_free_bytes=10_000,
        )

    crashed_row = _capacity_reclaim_rows(db)[0]
    quarantine_path = Path(crashed_row["quarantine_path"])
    assert crashed_row["state"] == "quarantined"
    assert not payload.exists() and quarantine_path.exists()

    recovery = DeadPartialReclaimer(
        db,
        executor,
        host_downloads=tmp_path,
        container_downloads="/downloads",
        managed_root=managed,
        dry_run=False, disk_free_bytes=lambda _path: 0,
        min_reclaimable_age_sec=3_600,
        min_reclaim_bytes=1,
        now=lambda: 5_001,
    )
    recovery.run(
        {},
        assessment=_assessment(),
        capacity_state="capacity_deadlock",
        free_bytes=0,
        target_free_bytes=10_000,
    )

    assert _capacity_reclaim_rows(db)[0]["state"] == "deleted"
    assert not quarantine_path.exists()
    recovery.run(
        {},
        assessment=_assessment(),
        capacity_state="capacity_deadlock",
        free_bytes=0,
        target_free_bytes=10_000,
    )
    assert _capacity_reclaim_rows(db)[0]["state"] == "reclaimed"


def test_restart_rejects_forged_quarantine_path_escape_without_delete(tmp_path):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    managed.mkdir()
    external = tmp_path / "external-payload"
    external.mkdir()
    marker = external / "keep"
    marker.write_bytes(b"keep")
    metadata = external.lstat()
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    candidate = _direct_reclaim_candidate(tmp_path)
    audit_row = {
        **candidate,
        "filesystem_dev": int(metadata.st_dev),
        "filesystem_ino": int(metadata.st_ino),
    }
    from qbt_orchestrator.capacity_reclaim import CapacityReclaimAuditStore

    audit = CapacityReclaimAuditStore(db, now=lambda: 5_000)
    reservation = audit.reserve(candidate)
    assert audit.mark_deleting(reservation["reclaim_id"], audit_row) is True
    con = sqlite3.connect(db)
    con.execute(
        "update capacity_reclaims set state='quarantined',quarantine_path=? where id=?",
        (str(external), reservation["reclaim_id"]),
    )
    con.commit()
    con.close()
    executor = RecordingExecutor({})
    recovery = DeadPartialReclaimer(
        db,
        executor,
        host_downloads=tmp_path,
        container_downloads="/downloads",
        managed_root=managed,
        dry_run=False, disk_free_bytes=lambda _path: 0,
        min_reclaimable_age_sec=3_600,
        min_reclaim_bytes=1,
        notification_chat_ids=["100"],
        now=lambda: 5_001,
    )

    recovery.run(
        {},
        assessment=_assessment(),
        capacity_state="capacity_deadlock",
        free_bytes=0,
        target_free_bytes=10_000,
    )

    assert marker.read_bytes() == b"keep"
    assert _capacity_reclaim_rows(db)[0]["state"] == "partial_or_unknown"
    assert len(_capacity_reclaim_notifications(db)) == 1


def _advance_capacity_generation(db: Path, generation: int) -> None:
    con = sqlite3.connect(db)
    con.execute(
        "update capacity_assessment_state set current_generation=? where id=1",
        (int(generation),),
    )
    con.commit()
    con.close()


def test_missing_assessment_reconciles_prior_generation_stopping_lease_once(tmp_path):
    from qbt_orchestrator.capacity_reclaim import (
        CapacityReclaimAuditStore,
        DeadPartialReclaimer,
    )
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    managed.mkdir()
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h", generation=4)
    audit = CapacityReclaimAuditStore(
        db, notification_chat_ids=["100"], now=lambda: 4_999,
    )
    assert audit.reserve(_direct_reclaim_candidate(tmp_path))["reserved"] is True
    _advance_capacity_generation(db, 5)
    stopped = {
        **_snapshot("h", content_path="/downloads/incomplete/h")["h"],
        "state": "stoppedDL",
    }
    executor = RecordingExecutor({"h": stopped})
    reclaimer = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, disk_free_bytes=lambda _path: 0, notification_chat_ids=["100"], now=lambda: 5_000,
    )

    first = reclaimer.run(
        {}, capacity_state="capacity_deadlock", free_bytes=0, target_free_bytes=1,
    )
    second = reclaimer.run(
        {}, capacity_state="capacity_deadlock", free_bytes=0, target_free_bytes=1,
    )

    row = _capacity_reclaim_rows(db)[0]
    assert first.rejection_counts == {"uncommitted_assessment": 1}
    assert first.errors == []
    assert second.rejection_counts == {"uncommitted_assessment": 1}
    assert row["capacity_generation"] == 4
    assert row["state"] == "cancelled"
    assert len(_capacity_reclaim_notifications(db)) == 1
    assert executor.posts == []


def test_missing_assessment_returns_recovery_errors_with_uncommitted_result(tmp_path):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    managed.mkdir()
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    reclaimer = DeadPartialReclaimer(
        db, RecordingExecutor(), host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, disk_free_bytes=lambda _path: 0,
    )
    reclaimer.audit.recovery_rows = lambda: (_ for _ in ()).throw(
        RuntimeError("recovery unavailable")
    )

    result = reclaimer.run(
        {}, capacity_state="capacity_deadlock", free_bytes=0, target_free_bytes=1,
    )

    assert result.rejection_counts == {"uncommitted_assessment": 1}
    assert result.errors == [
        "capacity reclaim recovery scan failed: recovery unavailable"
    ]


@pytest.mark.parametrize(
    ("seed_state", "path_exists", "expected_state", "recheck_count"),
    [
        ("deleting", True, "cancelled", 0),
        ("deleted", False, "reclaimed", 1),
        ("recheck_pending", False, "reclaimed", 1),
        ("partial_or_unknown", True, "cancelled", 0),
    ],
)
def test_prior_generation_recovery_uses_row_generation_and_dedupes_notification(
    tmp_path, seed_state, path_exists, expected_state, recheck_count
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
        (payload / "part").write_bytes(b"keep")
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h", generation=4)
    audit = CapacityReclaimAuditStore(
        db, notification_chat_ids=["100"], now=lambda: 4_999,
    )
    reservation = audit.reserve(_direct_reclaim_candidate(tmp_path))
    assert reservation["reserved"] is True
    _advance_capacity_generation(db, 5)
    con = sqlite3.connect(db)
    con.execute(
        "update capacity_reclaims set state=? where id=?",
        (seed_state, int(reservation["reclaim_id"])),
    )
    con.commit()
    con.close()
    executor = RecordingExecutor()
    reclaimer = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, disk_free_bytes=lambda _path: 0, notification_chat_ids=["100"], now=lambda: 5_000,
    )

    reclaimer.run(
        {}, assessment=_assessment(generation=5),
        capacity_state="capacity_deadlock", free_bytes=0, target_free_bytes=1,
    )
    reclaimer.run(
        {}, assessment=_assessment(generation=5),
        capacity_state="capacity_deadlock", free_bytes=0, target_free_bytes=1,
    )

    row = _capacity_reclaim_rows(db)[0]
    assert row["capacity_generation"] == 4
    assert row["state"] == expected_state
    assert payload.exists() is path_exists
    expected_notices = 2 if seed_state == "deleting" and path_exists else 1
    assert len(_capacity_reclaim_notifications(db)) == expected_notices
    assert len([post for post in executor.posts if post[0].endswith("/recheck")]) == recheck_count


def test_prior_generation_quarantine_is_restored_without_delete(tmp_path):
    from qbt_orchestrator.capacity_reclaim import (
        CapacityReclaimAuditStore,
        DeadPartialReclaimer,
    )
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    payload.mkdir(parents=True)
    marker = payload / "part"
    marker.write_bytes(b"keep")
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h", generation=4)
    executor = RecordingExecutor()
    reclaimer = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, disk_free_bytes=lambda _path: 0, notification_chat_ids=["100"], now=lambda: 5_000,
    )
    identity = reclaimer._capture_payload_identity(payload)
    candidate = {
        **_direct_reclaim_candidate(tmp_path),
        "filesystem_dev": identity.dev,
        "filesystem_ino": identity.ino,
    }
    audit = CapacityReclaimAuditStore(
        db, notification_chat_ids=["100"], now=lambda: 4_999,
    )
    reservation = audit.reserve(candidate)
    reclaim_id = int(reservation["reclaim_id"])
    assert audit.mark_deleting(reclaim_id, candidate) is True
    quarantine_path = reclaimer._quarantine_destination(reclaim_id)
    reclaimer._rename_to_quarantine(payload, quarantine_path, identity)
    assert audit.mark_quarantined(
        reclaim_id, 4, quarantine_path, identity.dev, identity.ino
    ) is True
    con = sqlite3.connect(db)
    con.execute(
        "update capacity_assessment_state set current_generation=5 where id=1"
    )
    con.commit()
    con.close()
    delete_calls = []
    reclaimer._delete_quarantine_path = lambda *_args: delete_calls.append(_args)

    reclaimer.run(
        {}, assessment=_assessment(generation=5),
        capacity_state="capacity_deadlock", free_bytes=0, target_free_bytes=1,
    )
    reclaimer.run(
        {}, assessment=_assessment(generation=5),
        capacity_state="capacity_deadlock", free_bytes=0, target_free_bytes=1,
    )

    row = _capacity_reclaim_rows(db)[0]
    assert row["capacity_generation"] == 4
    assert row["state"] == "cancelled"
    assert marker.read_bytes() == b"keep"
    assert not quarantine_path.exists()
    assert delete_calls == []
    assert len(_capacity_reclaim_notifications(db)) == 1


def test_prior_generation_quarantine_missing_from_both_paths_stays_partial(tmp_path):
    from qbt_orchestrator.capacity_reclaim import (
        CapacityReclaimAuditStore,
        DeadPartialReclaimer,
    )
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    managed.mkdir()
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h", generation=4)
    audit = CapacityReclaimAuditStore(
        db, notification_chat_ids=["100"], now=lambda: 4_999,
    )
    reservation = audit.reserve(_direct_reclaim_candidate(tmp_path))
    reclaim_id = int(reservation["reclaim_id"])
    _advance_capacity_generation(db, 5)
    recorded_quarantine = managed / ".qbt-orchestrator-reclaim" / f"reclaim-{reclaim_id}"
    con = sqlite3.connect(db)
    con.execute(
        "update capacity_reclaims set state='quarantined',quarantine_path=?,"
        "filesystem_dev=1,filesystem_ino=1 where id=?",
        (str(recorded_quarantine), reclaim_id),
    )
    con.commit()
    con.close()
    executor = RecordingExecutor()
    reclaimer = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, disk_free_bytes=lambda _path: 0, notification_chat_ids=["100"], now=lambda: 5_000,
    )

    reclaimer.run(
        {}, assessment=_assessment(generation=5),
        capacity_state="capacity_deadlock", free_bytes=0, target_free_bytes=1,
    )

    assert _capacity_reclaim_rows(db)[0]["state"] == "partial_or_unknown"
    assert len(_capacity_reclaim_notifications(db)) == 1
    assert executor.posts == []


def test_file_selection_fingerprint_is_strict_canonical_and_order_independent():
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer

    first = [
        {"index": 2, "size": 100, "priority": 0, "wanted": False},
        {"index": 1, "size": 100, "priority": 7, "wanted": True},
    ]
    second = list(reversed(first))
    assert DeadPartialReclaimer._file_selection_fingerprint(first) == (
        DeadPartialReclaimer._file_selection_fingerprint(second)
    )
    assert DeadPartialReclaimer._file_selection_fingerprint(first) != (
        DeadPartialReclaimer._file_selection_fingerprint(
            [
                {"index": 1, "size": 100, "priority": 0, "wanted": False},
                {"index": 2, "size": 100, "priority": 7, "wanted": True},
            ]
        )
    )
    for invalid in (
        [{"size": 1, "priority": 1}],
        [{"index": 0, "priority": 1}],
        [{"index": 0, "size": 1}],
        [
            {"index": 0, "size": 1, "priority": 1},
            {"index": 0, "size": 2, "priority": 0},
        ],
        [{"index": "bad", "size": 1, "priority": 1}],
        [{"index": 0, "size": -1, "priority": 1}],
        [{"index": 0, "size": 1, "priority": "bad"}],
        [{"index": 0, "size": 1, "priority": 1, "skip": "maybe"}],
    ):
        with pytest.raises(ValueError):
            DeadPartialReclaimer._file_selection_fingerprint(invalid)


@pytest.mark.parametrize(
    ("swap_at", "expect_stop"),
    [
        pytest.param(2, False, id="before-stop"),
        pytest.param(3, True, id="post-stop-inventory"),
    ],
)
def test_equal_size_priority_swap_is_fenced_before_stop_or_final_delete(
    tmp_path, swap_at, expect_stop
):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    payload.mkdir(parents=True)
    (payload / "part").write_bytes(b"keep" * 1024)
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    candidate = _snapshot("h", content_path="/downloads/incomplete/h")["h"]

    class PrioritySwapExecutor(RecordingExecutor):
        def __init__(self):
            super().__init__({"h": candidate})
            self.file_calls = 0

        def torrent_files(self, torrent_hash, timeout=None):
            self.file_calls += 1
            if self.file_calls < swap_at:
                priorities = (7, 0)
            else:
                priorities = (0, 7)
            return [
                {"index": 0, "size": 100, "priority": priorities[0]},
                {"index": 1, "size": 100, "priority": priorities[1]},
            ]

    executor = PrioritySwapExecutor()
    reclaimer = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, disk_free_bytes=lambda _path: 0,
        min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        now=lambda: 5_000,
    )
    delete_calls = []
    reclaimer._delete_quarantine_path = lambda *_args: delete_calls.append(_args)

    result = reclaimer.run(
        {"h": candidate}, assessment=_assessment(),
        capacity_state="capacity_deadlock", free_bytes=0,
        target_free_bytes=10_000,
    )

    assert result.reclaimed == 0
    assert result.rejection_counts["content_selection_changed"] == 1
    assert bool([post for post in executor.posts if post[0].endswith("/stop")]) is expect_stop
    assert payload.exists()
    assert delete_calls == []
    row = _capacity_reclaim_rows(db)[0]
    assert row["state"] == "cancelled"
    assert len(row["file_selection_fingerprint"]) == 64



@pytest.mark.parametrize("resolved_stage", ["before_quarantine", "before_delete"])
def test_live_disk_free_recheck_aborts_and_restores_without_delete(
    tmp_path, resolved_stage
):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    payload.mkdir(parents=True)
    marker = payload / "part"
    marker.write_bytes(b"keep" * 1024)
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    candidate = _snapshot("h", content_path="/downloads/incomplete/h")["h"]
    free_values = iter(
        [10_000] if resolved_stage == "before_quarantine" else [0, 10_000]
    )
    reclaimer = DeadPartialReclaimer(
        db, RecordingExecutor({"h": candidate}), host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, disk_free_bytes=lambda _path: next(free_values),
        min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        now=lambda: 5_000,
    )
    delete_calls = []
    reclaimer._delete_quarantine_path = lambda *_args: delete_calls.append(_args)

    result = reclaimer.run(
        {"h": candidate}, assessment=_assessment(),
        capacity_state="capacity_deadlock", free_bytes=0,
        target_free_bytes=10_000,
    )

    assert result.reclaimed == 0
    assert result.rejection_counts["capacity_pressure_resolved"] == 1
    assert marker.read_bytes() == b"keep" * 1024
    assert delete_calls == []
    row = _capacity_reclaim_rows(db)[0]
    assert row["state"] == "cancelled"
    assert row["target_free_bytes"] == 10_000


@pytest.mark.parametrize(
    ("mutation", "expected_reason"),
    [
        ("assessment", "stale_assessment"),
        ("health", "progress_evidence_changed"),
        ("capacity_state", "capacity_episode_changed"),
    ],
)
def test_final_atomic_authorization_detects_committed_capacity_mutation_and_restores(
    tmp_path, mutation, expected_reason
):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    payload.mkdir(parents=True)
    marker = payload / "part"
    marker.write_bytes(b"keep" * 1024)
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    candidate = _snapshot("h", content_path="/downloads/incomplete/h")["h"]
    reclaimer = DeadPartialReclaimer(
        db, RecordingExecutor({"h": candidate}), host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, disk_free_bytes=lambda _path: 0,
        min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        now=lambda: 5_000,
    )
    original_authorize = reclaimer.audit.authorize_delete
    mutation_committed = []

    def mutate_then_authorize(reclaim_id, frozen):
        con = sqlite3.connect(db)
        if mutation == "assessment":
            con.execute(
                "update capacity_assessment_state set current_generation=5 where id=1"
            )
        elif mutation == "health":
            con.execute(
                "update torrent_health set no_progress_since=101 where hash='h'"
            )
        else:
            con.execute(
                "update capacity_state set state='progress_possible' where id=1"
            )
        con.commit()
        con.close()
        mutation_committed.append(True)
        return original_authorize(reclaim_id, frozen)

    reclaimer.audit.authorize_delete = mutate_then_authorize
    delete_calls = []
    reclaimer._delete_quarantine_path = lambda *_args: delete_calls.append(_args)

    result = reclaimer.run(
        {"h": candidate}, assessment=_assessment(),
        capacity_state="capacity_deadlock", free_bytes=0,
        target_free_bytes=10_000,
    )

    assert mutation_committed == [True]
    assert result.reclaimed == 0
    assert result.rejection_counts[expected_reason] == 1
    assert marker.read_bytes() == b"keep" * 1024
    assert delete_calls == []
    assert _capacity_reclaim_rows(db)[0]["state"] == "cancelled"


@pytest.mark.parametrize("resolved_by", ["capacity_state", "disk_free"])
def test_same_generation_quarantine_recovery_restores_when_pressure_resolved(
    tmp_path, resolved_by
):
    from qbt_orchestrator.capacity_reclaim import (
        CapacityReclaimAuditStore,
        DeadPartialReclaimer,
    )
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    payload.mkdir(parents=True)
    marker = payload / "part"
    marker.write_bytes(b"keep")
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    reclaimer = DeadPartialReclaimer(
        db, RecordingExecutor(), host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False,
        disk_free_bytes=(lambda _path: 10_000 if resolved_by == "disk_free" else 0),
        notification_chat_ids=["100"], now=lambda: 5_000,
    )
    identity = reclaimer._capture_payload_identity(payload)
    candidate = {
        **_direct_reclaim_candidate(tmp_path),
        "filesystem_dev": identity.dev,
        "filesystem_ino": identity.ino,
    }
    audit = CapacityReclaimAuditStore(
        db, notification_chat_ids=["100"], now=lambda: 4_999,
    )
    reservation = audit.reserve(candidate)
    reclaim_id = int(reservation["reclaim_id"])
    assert audit.mark_deleting(reclaim_id, candidate) is True
    quarantine_path = reclaimer._quarantine_destination(reclaim_id)
    reclaimer._rename_to_quarantine(payload, quarantine_path, identity)
    assert audit.mark_quarantined(
        reclaim_id, 4, quarantine_path, identity.dev, identity.ino
    ) is True
    if resolved_by == "capacity_state":
        con = sqlite3.connect(db)
        con.execute("update capacity_state set state='progress_possible' where id=1")
        con.commit()
        con.close()
    delete_calls = []
    reclaimer._delete_quarantine_path = lambda *_args: delete_calls.append(_args)

    reclaimer.run(
        {}, assessment=_assessment(), capacity_state="capacity_deadlock",
        free_bytes=0, target_free_bytes=10_000,
    )

    assert marker.read_bytes() == b"keep"
    assert not quarantine_path.exists()
    assert delete_calls == []
    row = _capacity_reclaim_rows(db)[0]
    assert row["state"] == "cancelled"
    assert row["recheck_error"] == "live_revalidation_unavailable"
    assert len(_capacity_reclaim_notifications(db)) == 1


def test_live_reclaim_caps_to_one_and_stops_after_first_delete_error(tmp_path):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    snapshots = {}
    for torrent_hash in ("h1", "h2"):
        payload = managed / torrent_hash
        payload.mkdir(parents=True)
        (payload / "part").write_bytes(b"x" * 4096)
        _snapshot_row = _snapshot(
            torrent_hash,
            content_path=f"/downloads/incomplete/{torrent_hash}",
        )[torrent_hash]
        snapshots[torrent_hash] = _snapshot_row
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h1")
    _capacity_health(db, "h2")
    executor = RecordingExecutor(snapshots)
    reclaimer = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, disk_free_bytes=lambda _path: 0,
        min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        max_per_tick=2, now=lambda: 5_000,
    )
    delete_calls = []

    def fail_first_delete(*args):
        delete_calls.append(args)
        raise OSError("first delete failed")

    reclaimer._delete_quarantine_path = fail_first_delete

    result = reclaimer.run(
        snapshots, assessment=_assessment_for_hashes(("h1", "h2"), generation=4),
        capacity_state="capacity_deadlock", free_bytes=0,
        target_free_bytes=100_000,
    )

    assert result.planned == 1
    assert len(delete_calls) == 1
    assert [
        body["hashes"]
        for path, body in executor.posts
        if path.endswith("/stop")
    ] == ["h1"]
    assert (managed / "h2" / "part").exists()


def test_dry_run_reports_multiple_candidates_and_live_execution_cap(tmp_path):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    snapshots = {}
    for torrent_hash in ("h1", "h2"):
        payload = managed / torrent_hash
        payload.mkdir(parents=True)
        (payload / "part").write_bytes(b"x" * 4096)
        snapshots.update(
            _snapshot(
                torrent_hash,
                content_path=f"/downloads/incomplete/{torrent_hash}",
            )
        )
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h1")
    _capacity_health(db, "h2")
    reclaimer = DeadPartialReclaimer(
        db, RecordingExecutor(snapshots), host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=True, min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        max_per_tick=2, now=lambda: 5_000,
    )

    result = reclaimer.run(
        snapshots, assessment=_assessment_for_hashes(("h1", "h2"), generation=4),
        capacity_state="capacity_deadlock", free_bytes=0,
        target_free_bytes=100_000,
    )

    assert result.planned == 2
    assert result.live_execution_cap == 1
    assert result.as_dict()["live_execution_cap"] == 1


@pytest.mark.skipif(os.name == "nt", reason="real symlink coverage runs on POSIX CI")
def test_fresh_inventory_detects_other_torrent_symlink_alias_to_candidate(tmp_path):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    payload.mkdir(parents=True)
    (payload / "part").write_bytes(b"keep")
    alias = tmp_path / "complete" / "alias"
    alias.parent.mkdir()
    alias.symlink_to(payload, target_is_directory=True)
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    candidate = _snapshot("h", content_path="/downloads/incomplete/h")["h"]
    other = {
        **_snapshot("other", content_path="/downloads/complete/alias")["other"],
        "hash": "other",
    }
    executor = RecordingExecutor({"h": candidate, "other": other})
    reclaimer = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, disk_free_bytes=lambda _path: 0,
        min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        now=lambda: 5_000,
    )

    result = reclaimer.run(
        {"h": candidate}, assessment=_assessment(),
        capacity_state="capacity_deadlock", free_bytes=0,
        target_free_bytes=10_000,
    )

    assert result.rejection_counts["path_overlap"] == 1
    assert (payload / "part").exists()


def test_fresh_inventory_fails_closed_for_unresolved_managed_other_path(tmp_path):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    payload.mkdir(parents=True)
    (payload / "part").write_bytes(b"keep")
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    candidate = _snapshot("h", content_path="/downloads/incomplete/h")["h"]
    other = {
        **_snapshot(
            "other", content_path="/downloads/incomplete/missing-other"
        )["other"],
        "hash": "other",
    }
    reclaimer = DeadPartialReclaimer(
        db, RecordingExecutor({"h": candidate, "other": other}),
        host_downloads=tmp_path, container_downloads="/downloads",
        managed_root=managed, dry_run=False, disk_free_bytes=lambda _path: 0,
        min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        now=lambda: 5_000,
    )

    result = reclaimer.run(
        {"h": candidate}, assessment=_assessment(),
        capacity_state="capacity_deadlock", free_bytes=0,
        target_free_bytes=10_000,
    )

    assert result.reclaimed == 1
    assert not payload.exists()




def _seed_same_generation_quarantined_reclaim(tmp_path, executor):
    from qbt_orchestrator.capacity_reclaim import (
        CapacityReclaimAuditStore,
        DeadPartialReclaimer,
    )
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    payload.mkdir(parents=True)
    marker = payload / "part"
    marker.write_bytes(b"keep")
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    reclaimer = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, disk_free_bytes=lambda _path: 0,
        notification_chat_ids=["100"], now=lambda: 5_000,
    )
    identity = reclaimer._capture_payload_identity(payload)
    selection = [{"index": 0, "size": 1, "priority": 1}]
    candidate = {
        **_direct_reclaim_candidate(tmp_path),
        "filesystem_dev": identity.dev,
        "filesystem_ino": identity.ino,
        "file_selection_fingerprint": reclaimer._file_selection_fingerprint(
            selection
        ),
        "assessment_json": json.dumps(
            {
                "torrent": {"no_progress_since": 100},
                "live_baseline": {
                    "amount_left": 900,
                    "completed_bytes": 100,
                    "progress": 0.0,
                },
            },
            sort_keys=True,
        ),
    }
    audit = CapacityReclaimAuditStore(
        db, notification_chat_ids=["100"], now=lambda: 4_999,
    )
    reservation = audit.reserve(candidate)
    reclaim_id = int(reservation["reclaim_id"])
    assert audit.mark_deleting(reclaim_id, candidate) is True
    quarantine_path = reclaimer._quarantine_destination(reclaim_id)
    reclaimer._rename_to_quarantine(payload, quarantine_path, identity)
    assert audit.mark_quarantined(
        reclaim_id, 4, quarantine_path, identity.dev, identity.ino
    ) is True
    return reclaimer, db, marker, quarantine_path



def test_missing_assessment_restores_same_generation_quarantine_for_confirmation(
    tmp_path,
):
    stopped = {
        **_snapshot("h", content_path="/downloads/incomplete/h")["h"],
        "state": "stoppedDL",
    }
    executor = RecordingExecutor({"h": stopped})
    reclaimer, db, marker, quarantine_path = (
        _seed_same_generation_quarantined_reclaim(tmp_path, executor)
    )
    delete_calls = []
    reclaimer._delete_quarantine_path = lambda *_args: delete_calls.append(_args)

    result = reclaimer.run(
        {}, capacity_state="capacity_deadlock",
        free_bytes=0, target_free_bytes=10_000,
    )

    row = _capacity_reclaim_rows(db)[0]
    assert result.rejection_counts == {"uncommitted_assessment": 1}
    assert row["state"] == "cancelled"
    assert row["recheck_error"] == "live_revalidation_unavailable"
    assert marker.read_bytes() == b"keep"
    assert not quarantine_path.exists()
    assert delete_calls == []
    assert executor.posts == []


def test_reclaim_lease_blocks_internal_mutators_after_delete_authorization(
    tmp_path,
):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    payload.mkdir(parents=True)
    (payload / "part").write_bytes(b"keep" * 1024)
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    candidate = _snapshot("h", content_path="/downloads/incomplete/h")["h"]
    executor = RecordingExecutor({"h": candidate})
    reclaimer = DeadPartialReclaimer(
        db, executor, host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, disk_free_bytes=lambda _path: 0,
        min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        now=lambda: 5_000,
    )
    original_authorize = reclaimer.audit.authorize_delete
    competing_results = []

    def authorize_then_compete(reclaim_id, frozen):
        reason = original_authorize(reclaim_id, frozen)
        assert reason is None
        competing_results.append(
            executor.qbt_post(
                "/api/v2/torrents/filePrio",
                {"hash": "h", "id": "0", "priority": "0"},
            )
        )
        competing_results.append(
            executor.qbt_post(
                "/api/v2/torrents/start", {"hashes": "h"}
            )
        )
        return reason

    reclaimer.audit.authorize_delete = authorize_then_compete

    result = reclaimer.run(
        {"h": candidate}, assessment=_assessment(),
        capacity_state="capacity_deadlock", free_bytes=0,
        target_free_bytes=10_000,
    )

    row = _capacity_reclaim_rows(db)[0]
    token = f"reclaim:{row['id']}:4"
    assert result.reclaimed == 1
    assert competing_results == [False, False]
    assert [path for path, _payload in executor.blocked_posts] == [
        "/api/v2/torrents/filePrio",
        "/api/v2/torrents/start",
    ]
    assert executor.post_lease_tokens == [token, token]
    assert executor.hash_mutation_leases == {}





def test_recovery_hydrates_all_locked_hash_leases_before_first_qbt_call(tmp_path):
    from qbt_orchestrator.capacity_reclaim import CapacityReclaimAuditStore

    stopped = {
        **_snapshot("h", content_path="/downloads/incomplete/h")["h"],
        "state": "stoppedDL",
    }

    class HydrationOrderExecutor(RecordingExecutor):
        expected_leases = {}

        def torrent_info(self, torrent_hash, timeout=None):
            assert self.hash_mutation_leases == self.expected_leases
            return super().torrent_info(torrent_hash, timeout=timeout)

    executor = HydrationOrderExecutor({"h": stopped})
    reclaimer, db, _marker, _quarantine_path = (
        _seed_same_generation_quarantined_reclaim(tmp_path, executor)
    )
    con = sqlite3.connect(db)
    con.execute(
        "insert into scheduler_allocations(hash,desired_state,applied_state,"
        "slot_kind,allocated_at,reason) values('x','soak','soak','soak',0,'test')"
    )
    con.execute(
        "insert into torrent_health(hash,sampled_at,no_progress_since,"
        "reclaimable_since,capacity_viable,capacity_reason,capacity_assessed_at,"
        "capacity_generation,updated_at) values('x',100,100,1000,0,"
        "'stale_without_complete_source',5000,4,5000)"
    )
    con.commit()
    con.close()
    audit = CapacityReclaimAuditStore(db, now=lambda: 5_000)
    x_reservation = audit.reserve(_direct_reclaim_candidate(tmp_path, "x"))
    assert x_reservation["reserved"] is True
    con = sqlite3.connect(db)
    # Terminal cancelled rows must not receive reclaim mutation leases.
    con.execute(
        "update capacity_reclaims set state='cancelled',"
        "recheck_error='terminal fixture' where id=?",
        (x_reservation["reclaim_id"],),
    )
    con.commit()
    con.close()
    rows = _capacity_reclaim_rows(db)
    executor.expected_leases = {
        str(row["hash"]): f"reclaim:{row['id']}:{row['capacity_generation']}"
        for row in rows
        if row["state"] == "quarantined"
    }

    reclaimer.run(
        {}, assessment=_assessment(), capacity_state="capacity_deadlock",
        free_bytes=0, target_free_bytes=10_000,
    )

    rows = {row["hash"]: row for row in _capacity_reclaim_rows(db)}
    assert rows["h"]["state"] == "deleted"
    assert rows["x"]["state"] == "cancelled"
    assert executor.hash_mutation_leases == executor.expected_leases
    reclaimer.run(
        {}, assessment=_assessment(), capacity_state="capacity_deadlock",
        free_bytes=0, target_free_bytes=10_000,
    )
    rows = {row["hash"]: row for row in _capacity_reclaim_rows(db)}
    assert rows["h"]["state"] == "reclaimed"
    assert executor.post_lease_tokens == [executor.expected_leases["h"]]
    assert executor.hash_mutation_leases == {}


def test_reclaimer_rehydration_matches_startup_highest_id_for_duplicate_hash(tmp_path):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.executor import Executor

    class Qbt:
        def post(self, path, payload):
            raise AssertionError("lease hydration must not post to qBT")

    db = tmp_path / "state.sqlite"
    managed = tmp_path / "incomplete"
    managed.mkdir()
    migrate(db, dry_run=False)
    con = sqlite3.connect(db)
    for reclaim_key, torrent_hash, state, generation in (
        ("older", " H ", "reclaimed", 3),
        ("newer", "h", "quarantined", 7),
    ):
        con.execute(
            "insert into capacity_reclaims("
            "reclaim_key,hash,name,magnet_uri,host_path,content_path,state,"
            "capacity_generation,created_at,updated_at) "
            "values(?,?,?,?,?,?,?,?,?,?)",
            (
                reclaim_key,
                torrent_hash,
                torrent_hash,
                "magnet:?xt=test",
                str(managed / "h"),
                "/downloads/incomplete/h",
                state,
                generation,
                1,
                1,
            ),
        )
    con.commit()
    con.close()

    executor = Executor(Qbt(), dry_run=True, state_db=db)
    reclaimer = DeadPartialReclaimer(
        db,
        executor,
        host_downloads=tmp_path,
        container_downloads="/downloads",
        managed_root=managed,
        dry_run=True,
    )

    assert reclaimer._hydrate_reclaim_mutation_leases() == []


def test_restore_failure_persists_restore_error_with_original_failure(tmp_path):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    payload.mkdir(parents=True)
    (payload / "part").write_bytes(b"keep" * 1024)
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    candidate = _snapshot("h", content_path="/downloads/incomplete/h")["h"]
    disk_calls = 0

    def disk_free(_path):
        nonlocal disk_calls
        disk_calls += 1
        if disk_calls >= 2:
            raise RuntimeError("original disk probe failed")
        return 0

    reclaimer = DeadPartialReclaimer(
        db, RecordingExecutor({"h": candidate}), host_downloads=tmp_path,
        container_downloads="/downloads", managed_root=managed,
        dry_run=False, disk_free_bytes=disk_free,
        min_reclaimable_age_sec=3_600, min_reclaim_bytes=1,
        now=lambda: 5_000,
    )
    reclaimer._restore_from_quarantine = lambda *_args: False

    reclaimer.run(
        {"h": candidate}, assessment=_assessment(),
        capacity_state="capacity_deadlock", free_bytes=0,
        target_free_bytes=10_000,
    )

    row = _capacity_reclaim_rows(db)[0]
    assert row["state"] == "partial_or_unknown"
    assert "quarantine restore failed" in row["recheck_error"]
    assert "original disk probe failed" in row["recheck_error"]


def test_live_inventory_ignores_unrelated_empty_precheck_and_missing_paths(tmp_path):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    payload.mkdir(parents=True)
    (payload / "part").write_bytes(b"keep" * 1024)
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    candidate = _snapshot("h", content_path="/downloads/incomplete/h")["h"]
    executor = RecordingExecutor(
        {
            "h": candidate,
            "metadata": {
                "hash": "metadata",
                "category": "precheck",
                "tags": "metadata-probe,hold",
                "content_path": "",
            },
            "missing": {
                **_snapshot(
                    "missing",
                    content_path="/downloads/incomplete/missing-payload",
                )["missing"],
                "hash": "missing",
            },
        }
    )
    reclaimer = DeadPartialReclaimer(
        db,
        executor,
        host_downloads=tmp_path,
        container_downloads="/downloads",
        managed_root=managed,
        dry_run=False,
        disk_free_bytes=lambda _path: 0,
        min_reclaimable_age_sec=3_600,
        min_reclaim_bytes=1,
        now=lambda: 5_000,
    )

    result = reclaimer.run(
        {"h": candidate},
        assessment=_assessment(),
        capacity_state="capacity_deadlock",
        free_bytes=0,
        target_free_bytes=10_000,
    )

    assert result.reclaimed == 1
    assert "path_inventory_failed" not in result.rejection_counts
    assert not payload.exists()


def test_live_selection_skips_recorded_head_and_processes_next_candidate(tmp_path):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    snapshots = {}
    for torrent_hash, size in (("h1", 8192), ("h2", 4096)):
        payload = managed / torrent_hash
        payload.mkdir(parents=True)
        (payload / "part").write_bytes(b"x" * size)
        snapshots.update(
            _snapshot(
                torrent_hash,
                content_path=f"/downloads/incomplete/{torrent_hash}",
            )
        )
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h1")
    _capacity_health(db, "h2")
    con = sqlite3.connect(db)
    con.execute(
        "insert into capacity_reclaims("
        "reclaim_key,hash,name,magnet_uri,host_path,content_path,state,"
        "capacity_generation,created_at,updated_at) values(?,?,?,?,?,?,?,?,?,?)",
        (
            "h1:1000",
            "h1",
            "h1",
            "magnet:?xt=test",
            str(managed / "h1"),
            "/downloads/incomplete/h1",
            "reclaimed",
            4,
            1,
            1,
        ),
    )
    con.commit()
    con.close()
    executor = RecordingExecutor(snapshots)
    reclaimer = DeadPartialReclaimer(
        db,
        executor,
        host_downloads=tmp_path,
        container_downloads="/downloads",
        managed_root=managed,
        dry_run=False,
        disk_free_bytes=lambda _path: 0,
        min_reclaimable_age_sec=3_600,
        min_reclaim_bytes=1,
        max_per_tick=1,
        now=lambda: 5_000,
    )

    result = reclaimer.run(
        snapshots,
        assessment=_assessment_for_hashes(("h1", "h2"), generation=4),
        capacity_state="capacity_deadlock",
        free_bytes=0,
        target_free_bytes=100_000,
    )

    assert result.planned == 1
    assert result.reclaimed == 1
    assert (managed / "h1").exists()
    assert not (managed / "h2").exists()
    assert [
        body["hashes"]
        for path, body in executor.posts
        if path.endswith("/stop")
    ] == ["h2"]


def test_predelete_abort_is_terminal_without_confirmation_and_releases_lease(tmp_path):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    payload.mkdir(parents=True)
    (payload / "part").write_bytes(b"keep" * 1024)
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    candidate = _snapshot("h", content_path="/downloads/incomplete/h")["h"]

    class BrokenInventory(RecordingExecutor):
        def get_maindata(self, rid, timeout=None):
            return {"full_update": False, "torrents": {"h": candidate}}

    executor = BrokenInventory({"h": candidate})
    reclaimer = DeadPartialReclaimer(
        db,
        executor,
        host_downloads=tmp_path,
        container_downloads="/downloads",
        managed_root=managed,
        dry_run=False,
        disk_free_bytes=lambda _path: 0,
        min_reclaimable_age_sec=3_600,
        min_reclaim_bytes=1,
        notification_chat_ids=["100"],
        now=lambda: 5_000,
    )

    result = reclaimer.run(
        {"h": candidate},
        assessment=_assessment(),
        capacity_state="capacity_deadlock",
        free_bytes=0,
        target_free_bytes=10_000,
    )

    row = _capacity_reclaim_rows(db)[0]
    notice = json.loads(_capacity_reclaim_notifications(db)[0]["payload_json"])
    assert result.reclaimed == 0
    assert row["state"] == "cancelled"
    assert executor.hash_mutation_leases == {}
    assert notice["automatic"] is True
    assert "requires_confirmation" not in notice
    assert "allowed_actions" not in notice
    assert payload.exists()


def test_legacy_aborted_paused_is_retired_and_does_not_hydrate_a_lease(tmp_path):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.executor import Executor

    class RecordingQbt:
        def __init__(self):
            self.posts = []

        def post(self, path, payload):
            self.posts.append((path, payload))
            return {"ok": True}

    managed = tmp_path / "incomplete"
    managed.mkdir()
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    con = sqlite3.connect(db)
    reclaim_id = con.execute(
        "insert into capacity_reclaims("
        "reclaim_key,hash,name,magnet_uri,host_path,content_path,state,"
        "capacity_generation,created_at,updated_at) values(?,?,?,?,?,?,?,?,?,?)",
        (
            "h:1000",
            "h",
            "h",
            "magnet:?xt=test",
            str(managed / "h"),
            "/downloads/incomplete/h",
            "aborted_paused",
            4,
            1,
            1,
        ),
    ).lastrowid
    con.commit()
    con.close()
    assert _capacity_reclaim_rows(db)[0]["state"] == "aborted_paused"

    # Startup order: migrate() retires aborted_paused before Executor hydration.
    migrate(db, dry_run=False)
    assert _capacity_reclaim_rows(db)[0]["state"] == "cancelled"
    assert _capacity_reclaim_rows(db)[0]["recheck_error"] == "legacy_confirmation_removed"

    qbt = RecordingQbt()
    executor = Executor(qbt, dry_run=False, state_db=db)
    try:
        assert executor.qbt_post("/api/v2/torrents/start", {"hashes": "h"}) is True
        assert qbt.posts == [("/api/v2/torrents/start", {"hashes": "h"})]
    finally:
        executor.close(timeout=1)

    # Reclaim hot path must not retire or release leases for already-cancelled rows.
    recording = RecordingExecutor()
    recording.hydrate_hash_mutation_lease("h", f"reclaim:{reclaim_id}:4")
    reclaimer = DeadPartialReclaimer(
        db,
        recording,
        host_downloads=tmp_path,
        container_downloads="/downloads",
        managed_root=managed,
        dry_run=False,
        max_per_tick=0,
        now=lambda: 5_000,
    )
    reclaimer.run(
        {},
        assessment=_assessment(),
        capacity_state="normal",
        free_bytes=10_000,
        target_free_bytes=10_000,
    )
    assert _capacity_reclaim_rows(db)[0]["state"] == "cancelled"
    assert recording.hash_mutation_leases == {"h": f"reclaim:{reclaim_id}:4"}


def test_unrelated_dotdot_alias_still_blocks_candidate_reclaim(tmp_path):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    payload.mkdir(parents=True)
    (payload / "part").write_bytes(b"keep" * 1024)
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    candidate = _snapshot("h", content_path="/downloads/incomplete/h")["h"]
    alias = {
        **_snapshot(
            "other",
            content_path="/downloads/incomplete/other/../h",
        )["other"],
        "hash": "other",
    }
    executor = RecordingExecutor({"h": candidate, "other": alias})
    reclaimer = DeadPartialReclaimer(
        db,
        executor,
        host_downloads=tmp_path,
        container_downloads="/downloads",
        managed_root=managed,
        dry_run=False,
        disk_free_bytes=lambda _path: 0,
        min_reclaimable_age_sec=3_600,
        min_reclaim_bytes=1,
        now=lambda: 5_000,
    )

    result = reclaimer.run(
        {"h": candidate},
        assessment=_assessment(),
        capacity_state="capacity_deadlock",
        free_bytes=0,
        target_free_bytes=10_000,
    )

    assert result.rejection_counts["path_overlap"] == 1
    assert payload.exists()


def test_unresolved_stop_unknown_does_not_block_other_safe_candidate(tmp_path):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    safe = managed / "safe"
    safe.mkdir(parents=True)
    (safe / "part").write_bytes(b"x" * 4096)
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "safe")
    con = sqlite3.connect(db)
    con.execute(
        "insert into capacity_reclaims("
        "reclaim_key,hash,name,magnet_uri,host_path,content_path,state,"
        "capacity_generation,created_at,updated_at) values(?,?,?,?,?,?,?,?,?,?)",
        (
            "stuck:1000",
            "stuck",
            "stuck",
            "magnet:?xt=test",
            str(managed / "stuck"),
            "/downloads/incomplete/stuck",
            "stop_unknown",
            4,
            1,
            1,
        ),
    )
    con.commit()
    con.close()
    safe_row = _snapshot(
        "safe", content_path="/downloads/incomplete/safe"
    )["safe"]

    class OneHashUnavailable(RecordingExecutor):
        def torrent_info(self, torrent_hash, timeout=None):
            if str(torrent_hash) == "stuck":
                raise TimeoutError("stuck unavailable")
            return super().torrent_info(torrent_hash, timeout=timeout)

    executor = OneHashUnavailable({"safe": safe_row})
    reclaimer = DeadPartialReclaimer(
        db,
        executor,
        host_downloads=tmp_path,
        container_downloads="/downloads",
        managed_root=managed,
        dry_run=False,
        disk_free_bytes=lambda _path: 0,
        min_reclaimable_age_sec=3_600,
        min_reclaim_bytes=1,
        now=lambda: 5_000,
    )

    result = reclaimer.run(
        {"safe": safe_row},
        assessment=_assessment("safe"),
        capacity_state="capacity_deadlock",
        free_bytes=0,
        target_free_bytes=10_000,
    )

    rows = {row["hash"]: row for row in _capacity_reclaim_rows(db)}
    assert rows["stuck"]["state"] == "stop_unknown"
    assert rows["safe"]["state"] == "reclaimed"
    assert result.reclaimed == 1
    assert not safe.exists()


@pytest.mark.parametrize("terminal_state", ["cancelled", "aborted_paused", "reclaimed", "failed"])
def test_terminal_reclaim_rows_do_not_block_sqlite_work_state(tmp_path, terminal_state):
    from qbt_orchestrator.db import migrate

    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    con = sqlite3.connect(db)
    con.execute(
        "insert into capacity_reclaims("
        "reclaim_key,hash,name,magnet_uri,host_path,content_path,state,"
        "capacity_generation,created_at,updated_at) values(?,?,?,?,?,?,?,?,?,?)",
        (
            f"h:{terminal_state}",
            "h",
            "h",
            "magnet:?xt=test",
            str(tmp_path / "h"),
            "/downloads/incomplete/h",
            terminal_state,
            4,
            1,
            1,
        ),
    )
    con.execute(
        "insert into torrent_jobs(hash,job_type,state,created_at,updated_at) "
        "values('h','upload','queued',1,1)"
    )
    con.execute(
        "insert into resource_reservations(hash,kind,bytes,state,created_at) "
        "values('h','batch',1,'active',1)"
    )
    con.execute(
        "insert into soak_state(hash,cooldown_until,updated_at) values('h',10,1)"
    )
    con.commit()
    con.close()


def test_cancelled_reclaim_reuses_same_row_on_a_later_tick(tmp_path):
    from qbt_orchestrator.capacity_reclaim import CapacityReclaimAuditStore
    from qbt_orchestrator.db import migrate

    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    clock = [5_000]
    audit = CapacityReclaimAuditStore(db, now=lambda: clock[0])
    candidate = _direct_reclaim_candidate(tmp_path)
    first = audit.reserve(candidate)
    assert first["reserved"] is True
    assert audit.mark_cancelled(
        first["reclaim_id"], 4, "automatic retry test"
    ) is True
    same_generation = audit.reserve(candidate)
    assert same_generation["reserved"] is False
    assert same_generation["reason"] == "reclaim_already_recorded"

    # Wall-clock alone must not unlock reuse.
    clock[0] += 86_400
    still_blocked = audit.reserve(candidate)
    assert still_blocked["reserved"] is False
    assert still_blocked["reason"] == "reclaim_already_recorded"

    _capacity_health(db, "h", generation=5)
    next_generation = {**candidate, "capacity_generation": 5}
    retried = audit.reserve(next_generation)

    assert retried["reserved"] is True
    assert retried["reclaim_id"] == first["reclaim_id"]
    assert retried["capacity_generation"] == 5
    assert len(_capacity_reclaim_rows(db)) == 1


def test_dry_run_does_not_retire_legacy_confirmation_rows(tmp_path):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    managed.mkdir()
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    con = sqlite3.connect(db)
    con.execute(
        "insert into capacity_reclaims("
        "reclaim_key,hash,name,magnet_uri,host_path,content_path,state,"
        "capacity_generation,created_at,updated_at) values(?,?,?,?,?,?,?,?,?,?)",
        (
            "h:1000",
            "h",
            "h",
            "magnet:?xt=test",
            str(managed / "h"),
            "/downloads/incomplete/h",
            "aborted_paused",
            4,
            1,
            1,
        ),
    )
    con.commit()
    con.close()
    reclaimer = DeadPartialReclaimer(
        db,
        RecordingExecutor(),
        host_downloads=tmp_path,
        container_downloads="/downloads",
        managed_root=managed,
        dry_run=True,
        max_per_tick=0,
        now=lambda: 5_000,
    )

    reclaimer.run(
        {},
        assessment=_assessment(),
        capacity_state="normal",
        free_bytes=10_000,
        target_free_bytes=10_000,
    )

    assert _capacity_reclaim_rows(db)[0]["state"] == "aborted_paused"


def _prepare_multi_candidates(tmp_path, hashes):
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    snapshots = {}
    for torrent_hash in hashes:
        payload = managed / torrent_hash
        payload.mkdir(parents=True)
        (payload / "part").write_bytes(b"x" * 4096)
        snapshots[torrent_hash] = _snapshot(
            torrent_hash,
            content_path=f"/downloads/incomplete/{torrent_hash}",
        )[torrent_hash]
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    for torrent_hash in hashes:
        _capacity_health(db, torrent_hash)
    return managed, db, snapshots


def test_tick_caps_reserve_stop_attempts_at_three(tmp_path):
    from qbt_orchestrator.capacity_reclaim import (
        MAX_CANDIDATE_ATTEMPTS_PER_TICK,
        DeadPartialReclaimer,
    )

    hashes = ("a", "b", "c", "d")
    managed, db, snapshots = _prepare_multi_candidates(tmp_path, hashes)

    class AlwaysTimeoutStop(RecordingExecutor):
        def __init__(self):
            super().__init__(snapshots, stop_updates_state=False)

        def torrent_info(self, torrent_hash, timeout=None):
            current = super().torrent_info(torrent_hash, timeout=timeout)
            current["state"] = "downloading"
            return current

    executor = AlwaysTimeoutStop()
    reclaimer = DeadPartialReclaimer(
        db,
        executor,
        host_downloads=tmp_path,
        container_downloads="/downloads",
        managed_root=managed,
        dry_run=False,
        disk_free_bytes=lambda _path: 0,
        min_reclaimable_age_sec=3_600,
        min_reclaim_bytes=1,
        max_per_tick=4,
        stop_timeout_sec=0,
        sleep=lambda _seconds: None,
        now=lambda: 5_000,
    )

    result = reclaimer.run(
        snapshots,
        assessment=_assessment_for_hashes(hashes, generation=4),
        capacity_state="capacity_deadlock",
        free_bytes=0,
        target_free_bytes=100_000,
    )

    stops = [body["hashes"] for path, body in executor.posts if path.endswith("/stop")]
    assert MAX_CANDIDATE_ATTEMPTS_PER_TICK == 3
    assert len(stops) == 3
    assert result.planned == 0
    assert result.rejection_counts["stop_timeout"] == 3
    rows = _capacity_reclaim_rows(db)
    assert len(rows) == 3
    assert {row["state"] for row in rows} == {"cancelled"}
    assert (managed / "d" / "part").exists()


def test_tick_allows_only_one_quarantine_destructive_transaction(tmp_path):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer

    hashes = ("h1", "h2")
    managed, db, snapshots = _prepare_multi_candidates(tmp_path, hashes)
    executor = RecordingExecutor(snapshots)
    reclaimer = DeadPartialReclaimer(
        db,
        executor,
        host_downloads=tmp_path,
        container_downloads="/downloads",
        managed_root=managed,
        dry_run=False,
        disk_free_bytes=lambda _path: 0,
        min_reclaimable_age_sec=3_600,
        min_reclaim_bytes=1,
        max_per_tick=2,
        now=lambda: 5_000,
    )
    rename_calls = []
    original_rename = reclaimer._rename_to_quarantine

    def track_rename(*args):
        rename_calls.append(args[0])
        return original_rename(*args)

    reclaimer._rename_to_quarantine = track_rename

    result = reclaimer.run(
        snapshots,
        assessment=_assessment_for_hashes(hashes, generation=4),
        capacity_state="capacity_deadlock",
        free_bytes=0,
        target_free_bytes=100_000,
    )

    stops = [body["hashes"] for path, body in executor.posts if path.endswith("/stop")]
    assert len(rename_calls) == 1
    assert result.planned == 1
    assert result.reclaimed == 1
    assert len(stops) == 1
    assert (managed / stops[0]).exists() is False
    other = "h2" if stops[0] == "h1" else "h1"
    assert (managed / other / "part").exists()


def test_stop_timeout_continues_to_second_candidate_same_tick(tmp_path):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer

    hashes = ("first", "second")
    managed, db, snapshots = _prepare_multi_candidates(tmp_path, hashes)
    # Prefer "first" by larger allocated footprint.
    (managed / "first" / "part").write_bytes(b"x" * 8192)
    (managed / "second" / "part").write_bytes(b"x" * 4096)

    class FirstNeverStops(RecordingExecutor):
        """Local stop_timeout: first stays non-stopped; second stops normally.

        Transport TimeoutError during confirmation is a different path
        (stop_unknown + end tick) and is covered separately.
        """

        def __init__(self):
            super().__init__(snapshots, stop_updates_state=False)
            self._clock = 0.0

        def qbt_post(self, path, payload, *, lease_token=None):
            result = super().qbt_post(path, payload, lease_token=lease_token)
            if path.endswith("/stop") and str(payload.get("hashes")) == "second":
                self.info.setdefault("second", {})["state"] = "stoppedDL"
            return result

        def torrent_info(self, torrent_hash, timeout=None):
            current = super().torrent_info(torrent_hash, timeout=timeout)
            if str(torrent_hash) == "first":
                current["state"] = "downloading"
                # Advance fake monotonic so the local stop deadline expires.
                self._clock += 1.0
            return current

        def monotonic(self):
            return self._clock

    executor = FirstNeverStops()
    reclaimer = DeadPartialReclaimer(
        db,
        executor,
        host_downloads=tmp_path,
        container_downloads="/downloads",
        managed_root=managed,
        dry_run=False,
        disk_free_bytes=lambda _path: 0,
        min_reclaimable_age_sec=3_600,
        min_reclaim_bytes=1,
        max_per_tick=2,
        stop_timeout_sec=0.5,
        stop_poll_interval_sec=0,
        sleep=lambda _seconds: None,
        monotonic=executor.monotonic,
        now=lambda: 5_000,
    )

    result = reclaimer.run(
        snapshots,
        assessment=_assessment_for_hashes(hashes, generation=4),
        capacity_state="capacity_deadlock",
        free_bytes=0,
        target_free_bytes=100_000,
    )

    stops = [body["hashes"] for path, body in executor.posts if path.endswith("/stop")]
    assert stops == ["first", "second"]
    assert result.rejection_counts["stop_timeout"] == 1
    assert result.planned == 1
    assert result.reclaimed == 1
    rows = {row["hash"]: row for row in _capacity_reclaim_rows(db)}
    assert rows["first"]["state"] == "cancelled"
    assert rows["second"]["state"] == "reclaimed"
    assert (managed / "first" / "part").exists()
    assert not (managed / "second").exists()


def test_qbt_system_failure_ends_tick_immediately(tmp_path):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer

    hashes = ("first", "second")
    managed, db, snapshots = _prepare_multi_candidates(tmp_path, hashes)
    (managed / "first" / "part").write_bytes(b"x" * 8192)
    (managed / "second" / "part").write_bytes(b"x" * 4096)

    class SystemFailureOnStop(RecordingExecutor):
        def qbt_post(self, path, payload, *, lease_token=None):
            if path.endswith("/stop"):
                raise RuntimeError("connection refused by qBT")
            return super().qbt_post(path, payload, lease_token=lease_token)

    executor = SystemFailureOnStop(snapshots)
    reclaimer = DeadPartialReclaimer(
        db,
        executor,
        host_downloads=tmp_path,
        container_downloads="/downloads",
        managed_root=managed,
        dry_run=False,
        disk_free_bytes=lambda _path: 0,
        min_reclaimable_age_sec=3_600,
        min_reclaim_bytes=1,
        max_per_tick=2,
        now=lambda: 5_000,
    )

    result = reclaimer.run(
        snapshots,
        assessment=_assessment_for_hashes(hashes, generation=4),
        capacity_state="capacity_deadlock",
        free_bytes=0,
        target_free_bytes=100_000,
    )

    rows = _capacity_reclaim_rows(db)
    assert len(rows) == 1
    assert rows[0]["hash"] == "first"
    assert rows[0]["state"] == "stop_unknown"
    assert result.planned == 0
    assert result.reclaimed == 0
    assert result.rejection_counts["stop_failed"] == 1
    assert (managed / "second" / "part").exists()


def test_stop_confirmation_connection_failure_fences_stop_unknown_and_ends_tick(
    tmp_path,
):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer

    hashes = ("first", "second")
    managed, db, snapshots = _prepare_multi_candidates(tmp_path, hashes)

    class ConfirmFailsAfterStop(RecordingExecutor):
        def torrent_info(self, torrent_hash, timeout=None):
            stopped = any(
                path.endswith("/stop") and body.get("hashes") == torrent_hash
                for path, body in self.posts
            )
            if stopped:
                raise ConnectionError("qBT confirmation connection reset")
            return super().torrent_info(torrent_hash, timeout=timeout)

    executor = ConfirmFailsAfterStop(snapshots)
    reclaimer = DeadPartialReclaimer(
        db,
        executor,
        host_downloads=tmp_path,
        container_downloads="/downloads",
        managed_root=managed,
        dry_run=False,
        disk_free_bytes=lambda _path: 0,
        min_reclaimable_age_sec=3_600,
        min_reclaim_bytes=1,
        max_per_tick=2,
        now=lambda: 5_000,
    )

    result = reclaimer.run(
        snapshots,
        assessment=_assessment_for_hashes(hashes, generation=4),
        capacity_state="capacity_deadlock",
        free_bytes=0,
        target_free_bytes=100_000,
    )

    stops = [body["hashes"] for path, body in executor.posts if path.endswith("/stop")]
    rows = _capacity_reclaim_rows(db)
    assert stops == ["first"]
    assert len(rows) == 1
    assert rows[0]["state"] == "stop_unknown"
    assert str(rows[0]["recheck_error"]).startswith("qbt_unreachable:")
    assert "first" in executor.hash_mutation_leases
    assert result.rejection_counts["qbt_unreachable"] == 1
    assert (managed / "second" / "part").exists()


def test_tick_caps_consecutive_reserve_rejects_at_three(tmp_path):
    from qbt_orchestrator.capacity_reclaim import (
        CapacityReclaimAuditStore,
        DeadPartialReclaimer,
        MAX_CANDIDATE_ATTEMPTS_PER_TICK,
    )

    hashes = ("a", "b", "c", "d")
    managed, db, snapshots = _prepare_multi_candidates(tmp_path, hashes)
    reserve_calls = []
    original_reserve = CapacityReclaimAuditStore.reserve

    def counting_reserve(self, candidate):
        reserve_calls.append(str(candidate["hash"]))
        result = original_reserve(self, candidate)
        if result.get("reserved"):
            return {
                **result,
                "reserved": False,
                "reason": "capacity_episode_changed",
            }
        return result

    CapacityReclaimAuditStore.reserve = counting_reserve
    try:
        executor = RecordingExecutor(snapshots)
        reclaimer = DeadPartialReclaimer(
            db,
            executor,
            host_downloads=tmp_path,
            container_downloads="/downloads",
            managed_root=managed,
            dry_run=False,
            disk_free_bytes=lambda _path: 0,
            min_reclaimable_age_sec=3_600,
            min_reclaim_bytes=1,
            max_per_tick=4,
            now=lambda: 5_000,
        )
        result = reclaimer.run(
            snapshots,
            assessment=_assessment_for_hashes(hashes, generation=4),
            capacity_state="capacity_deadlock",
            free_bytes=0,
            target_free_bytes=100_000,
        )
    finally:
        CapacityReclaimAuditStore.reserve = original_reserve

    assert MAX_CANDIDATE_ATTEMPTS_PER_TICK == 3
    assert len(reserve_calls) == 3
    assert result.planned == 0
    assert result.rejection_counts["capacity_episode_changed"] == 3
    assert not any(path.endswith("/stop") for path, _ in executor.posts)


@pytest.mark.parametrize(
    ("presence", "expected_state", "expected_error"),
    [
        pytest.param("both", "partial_or_unknown", "path_conflict_manual", id="both-remain"),
        pytest.param(
            "original_only", "cancelled", "path_conflict_original_only", id="original-only"
        ),
        pytest.param(
            "quarantine_only",
            "cancelled",
            "partial_state_restored",
            id="quarantine-only-unfence",
        ),
        pytest.param(
            "neither", "partial_or_unknown", "payload_missing_manual", id="neither"
        ),
    ],
)
def test_path_conflict_manual_cheap_recheck_and_auto_unfence(
    tmp_path, presence, expected_state, expected_error
):
    from qbt_orchestrator.capacity_reclaim import (
        PATH_CONFLICT_MANUAL,
        CapacityReclaimAuditStore,
        DeadPartialReclaimer,
        QUARANTINE_DIRNAME,
    )
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    payload = managed / "h"
    payload.mkdir(parents=True)
    marker = payload / "part"
    marker.write_bytes(b"keep")
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    _capacity_health(db, "h")
    stopped = {
        **_snapshot("h", content_path="/downloads/incomplete/h")["h"],
        "state": "stoppedDL",
    }
    executor = RecordingExecutor({"h": stopped})
    reclaimer = DeadPartialReclaimer(
        db,
        executor,
        host_downloads=tmp_path,
        container_downloads="/downloads",
        managed_root=managed,
        dry_run=False,
        disk_free_bytes=lambda _path: 0,
        notification_chat_ids=["100"],
        now=lambda: 5_000,
    )
    identity = reclaimer._capture_payload_identity(payload)
    selection = [{"index": 0, "size": 1, "priority": 1}]
    candidate = {
        **_direct_reclaim_candidate(tmp_path),
        "filesystem_dev": identity.dev,
        "filesystem_ino": identity.ino,
        "file_selection_fingerprint": reclaimer._file_selection_fingerprint(selection),
        "assessment_json": json.dumps(
            {
                "torrent": {"no_progress_since": 100},
                "live_baseline": {
                    "amount_left": 900,
                    "completed_bytes": 100,
                    "progress": 0.0,
                },
            },
            sort_keys=True,
        ),
    }
    audit = CapacityReclaimAuditStore(
        db, notification_chat_ids=["100"], now=lambda: 4_999
    )
    reservation = audit.reserve(candidate)
    reclaim_id = int(reservation["reclaim_id"])
    assert audit.mark_deleting(reclaim_id, candidate) is True
    quarantine_path = reclaimer._quarantine_destination(reclaim_id)
    reclaimer._rename_to_quarantine(payload, quarantine_path, identity)
    assert audit.mark_quarantined(
        reclaim_id, 4, quarantine_path, identity.dev, identity.ino
    ) is True
    # Recreate original alongside quarantine to enter dual-path conflict.
    payload.mkdir(parents=True)
    (payload / "part").write_bytes(b"keep")
    assert audit.mark_manual_path_conflict(
        reclaim_id,
        4,
        host_path=str(payload),
        quarantine_path=str(quarantine_path),
        allocated_bytes=1,
    ) is True
    row = _capacity_reclaim_rows(db)[0]
    assert row["state"] == "partial_or_unknown"
    assert row["recheck_error"] == PATH_CONFLICT_MANUAL

    if presence == "original_only":
        if quarantine_path.exists():
            if quarantine_path.is_dir():
                for child in quarantine_path.iterdir():
                    child.unlink()
                quarantine_path.rmdir()
            else:
                quarantine_path.unlink()
    elif presence == "quarantine_only":
        if payload.exists():
            for child in payload.iterdir():
                child.unlink()
            payload.rmdir()
    elif presence == "neither":
        if payload.exists():
            for child in payload.iterdir():
                child.unlink()
            payload.rmdir()
        if quarantine_path.exists():
            for child in quarantine_path.iterdir():
                child.unlink()
            quarantine_path.rmdir()
    # presence == "both": leave both in place

    notices_before = len(_capacity_reclaim_notifications(db))
    reclaimer.run(
        {},
        assessment=_assessment(),
        capacity_state="capacity_deadlock",
        free_bytes=0,
        target_free_bytes=10_000,
    )

    row = _capacity_reclaim_rows(db)[0]
    assert row["state"] == expected_state
    assert row["recheck_error"] == expected_error
    if presence == "both":
        assert payload.exists() and quarantine_path.exists()
        assert len(_capacity_reclaim_notifications(db)) >= notices_before
    if presence == "quarantine_only":
        # Cheap recheck clears the manual fence, restores, and cancels.
        assert not quarantine_path.exists()
        assert payload.exists()
        assert marker.read_bytes() == b"keep"
    assert QUARANTINE_DIRNAME in str(reclaimer.quarantine_root)


@pytest.mark.parametrize(
    ("info_payload", "expected_reason"),
    [
        pytest.param({"hash": "stuck"}, "torrent_absent_after_stop_unknown", id="missing-state"),
        pytest.param(
            {"hash": "stuck", "state": "downloading"},
            "stop_state_observed:downloading",
            id="observed-state",
        ),
    ],
)
def test_stop_unknown_reconcile_reasons(tmp_path, info_payload, expected_reason):
    from qbt_orchestrator.capacity_reclaim import DeadPartialReclaimer
    from qbt_orchestrator.db import migrate

    managed = tmp_path / "incomplete"
    managed.mkdir()
    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    con = sqlite3.connect(db)
    con.execute(
        "insert into capacity_reclaims("
        "reclaim_key,hash,name,magnet_uri,host_path,content_path,state,"
        "capacity_generation,created_at,updated_at) values(?,?,?,?,?,?,?,?,?,?)",
        (
            "stuck:1000",
            "stuck",
            "stuck",
            "magnet:?xt=test",
            str(managed / "stuck"),
            "/downloads/incomplete/stuck",
            "stop_unknown",
            4,
            1,
            1,
        ),
    )
    con.commit()
    con.close()

    class InfoExecutor(RecordingExecutor):
        def torrent_info(self, torrent_hash, timeout=None):
            assert str(torrent_hash) == "stuck"
            return dict(info_payload)

    executor = InfoExecutor({"stuck": info_payload})
    reclaimer = DeadPartialReclaimer(
        db,
        executor,
        host_downloads=tmp_path,
        container_downloads="/downloads",
        managed_root=managed,
        dry_run=False,
        max_per_tick=0,
        now=lambda: 5_000,
    )

    reclaimer.run(
        {},
        assessment=_assessment("stuck"),
        capacity_state="normal",
        free_bytes=10_000,
        target_free_bytes=10_000,
    )

    row = _capacity_reclaim_rows(db)[0]
    assert row["state"] == "cancelled"
    assert row["recheck_error"] == expected_reason
    assert "stop_state_reconciled" not in str(row["recheck_error"])
