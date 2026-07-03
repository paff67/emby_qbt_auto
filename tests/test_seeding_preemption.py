#!/usr/bin/env python3
from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))


class RecordingExecutor:
    def __init__(self):
        self.posts = []

    def qbt_post(self, path, payload):
        self.posts.append((path, payload))


def _row(db: Path, sql: str):
    con = sqlite3.connect(db)
    con.row_factory = sqlite3.Row
    try:
        r = con.execute(sql).fetchone()
        return dict(r) if r else None
    finally:
        con.close()


def test_migration_adds_seeding_preemptions_audit_table():
    from qbt_orchestrator.db import migrate

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db)
        con = sqlite3.connect(db)
        tables = {r[0] for r in con.execute("select name from sqlite_master where type='table'")}
        indexes = {r[0] for r in con.execute("select name from sqlite_master where type='index'")}
        con.close()

        assert "seeding_preemptions" in tables
        assert "idx_seeding_preemptions_ts" in indexes
        assert "idx_seeding_preemptions_hash" in indexes


def test_preemption_scores_high_value_task_and_enqueues_upload_without_delete():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.seeding_preemption import SeedingPreemptionService

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db)
        executor = RecordingExecutor()
        svc = SeedingPreemptionService(
            db,
            executor,
            dry_run=False,
            now=lambda: 10_000,
            host_downloads="/data/downloads",
            container_downloads="/downloads",
        )
        snapshots = {
            "newhot": {
                "hash": "newhot",
                "name": "NEW-HOT",
                "category": "auto",
                "tags": "auto,hot",
                "progress": 0.82,
                "amount_left": 2 * 1024**3,
                "dlspeed": 6 * 1024**2,
                "num_seeds": 8,
                "num_peers": 12,
                "state": "stoppedDL",
                "added_on": 9_900,
            },
            "seed1": {
                "hash": "seed1",
                "name": "OLD-SEED",
                "category": "auto",
                "tags": "auto",
                "progress": 1.0,
                "amount_left": 0,
                "size": 6 * 1024**3,
                "ratio": 1.2,
                "seeding_time": 7200,
                "upspeed": 0,
                "state": "uploading",
                "content_path": "/downloads/active/OLD-SEED",
            },
        }

        decision = svc.evaluate_and_apply(snapshots, disk_state="guard", trigger_reason="high_value_waiting")

        assert decision is not None
        assert decision.accepted is True
        assert decision.seeding_hash == "seed1"
        assert decision.target_hash == "newhot"
        assert decision.new_task_score >= 75
        assert decision.preemptability_score >= 65
        assert executor.posts == [("/api/v2/torrents/stop", {"hashes": "seed1"})]
        assert not any(path == "/api/v2/torrents/delete" for path, _ in executor.posts)

        job = _row(db, "select * from torrent_jobs where job_type='upload'")
        assert job is not None
        payload = json.loads(job["payload_json"])
        assert payload["hash"] == "seed1"
        assert payload["local"] == "/data/downloads/active/OLD-SEED"
        assert payload["remote"] == "gcrypt:/OLD-SEED"
        assert payload["full_torrent"] is True

        audit = _row(db, "select * from seeding_preemptions")
        assert audit is not None
        assert audit["seeding_hash"] == "seed1"
        assert audit["target_hash"] == "newhot"
        assert audit["upload_job_id"] == job["id"]
        guard = json.loads(audit["guard_json"])
        assert guard["guards_passed"]
        assert guard["guards_blocked"] == []

        event = _row(db, "select component,event_type from events_v2 where component='seeding_preemption'")
        assert event == {"component": "seeding_preemption", "event_type": "preempted"}


def test_preemption_hard_guards_protect_seed_long_and_active_upload():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.seeding_preemption import SeedingPreemptionService

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db)
        executor = RecordingExecutor()
        svc = SeedingPreemptionService(db, executor, dry_run=False, now=lambda: 10_000)
        base_new = {
            "hash": "newhot",
            "name": "NEW-HOT",
            "category": "auto",
            "tags": "auto,priority-hot",
            "progress": 0.9,
            "amount_left": 1024**3,
            "dlspeed": 8 * 1024**2,
            "num_seeds": 20,
            "num_peers": 20,
        }
        snapshots = {
            "newhot": base_new,
            "seedlong": {
                "hash": "seedlong",
                "name": "KEEP-LONG",
                "category": "auto",
                "tags": "auto,seed-long",
                "progress": 1.0,
                "amount_left": 0,
                "size": 20 * 1024**3,
                "ratio": 2.0,
                "seeding_time": 86400,
                "upspeed": 0,
                "content_path": "/downloads/active/KEEP-LONG",
            },
            "uploading": {
                "hash": "uploading",
                "name": "REAL-UPLOAD",
                "category": "auto",
                "tags": "auto",
                "progress": 1.0,
                "amount_left": 0,
                "size": 20 * 1024**3,
                "ratio": 2.0,
                "seeding_time": 86400,
                "upspeed": 128 * 1024,
                "content_path": "/downloads/active/REAL-UPLOAD",
            },
        }

        decision = svc.evaluate_and_apply(snapshots, disk_state="critical", trigger_reason="disk_critical")

        assert decision is None
        assert executor.posts == []
        assert _row(db, "select * from torrent_jobs") is None
        blocked = _row(db, "select * from decision_log where component='seeding_preemption' order by id desc limit 1")
        assert blocked is not None
        assert blocked["decision"] == "hold"
        data = json.loads(blocked["data_json"])
        assert "seed_long" in json.dumps(data) or "active_upload" in json.dumps(data)


def test_preemption_dry_run_records_decision_without_qbt_or_upload_job():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.seeding_preemption import SeedingPreemptionService

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db)
        executor = RecordingExecutor()
        svc = SeedingPreemptionService(db, executor, dry_run=True, now=lambda: 10_000)
        snapshots = {
            "newhot": {"hash": "newhot", "name": "NEW-HOT", "category": "auto", "tags": "auto,hot", "progress": 0.9, "amount_left": 1024**3, "dlspeed": 8 * 1024**2, "num_seeds": 10, "num_peers": 10},
            "seed1": {"hash": "seed1", "name": "OLD-SEED", "category": "auto", "tags": "auto", "progress": 1.0, "amount_left": 0, "size": 8 * 1024**3, "ratio": 1.0, "seeding_time": 3600, "upspeed": 0, "content_path": "/downloads/active/OLD-SEED"},
        }

        decision = svc.evaluate_and_apply(snapshots, disk_state="watch", trigger_reason="watch")

        assert decision and decision.accepted is True
        assert executor.posts == []
        assert _row(db, "select * from torrent_jobs") is None
        action = _row(db, "select action_type,status,dry_run from action_log where action_type='seeding_preempt'")
        assert action == {"action_type": "seeding_preempt", "status": "dry_run", "dry_run": 1}


def test_cli_builds_preemption_service_from_env(monkeypatch):
    from qbt_orchestrator.cli import _build_preemption_from_env
    from qbt_orchestrator.seeding_preemption import SeedingPreemptionService

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        executor = RecordingExecutor()
        monkeypatch.setenv("QBT_ORCH_PREEMPTION", "1")
        monkeypatch.setenv("QBT_ORCH_PREEMPTION_DRY_RUN", "0")
        monkeypatch.setenv("QBT_ORCH_PREEMPTION_MIN_NEW_SCORE", "80")
        monkeypatch.setenv("QBT_ORCH_PREEMPTION_MIN_SEED_SCORE", "70")

        service = _build_preemption_from_env(db, executor, env=dict(**__import__("os").environ), global_dry_run=False)

        assert isinstance(service, SeedingPreemptionService)
        assert service.dry_run is False
        assert service.config.min_new_task_score == 80
        assert service.config.min_preemptability_score == 70

        monkeypatch.setenv("QBT_ORCH_PREEMPTION", "0")
        assert _build_preemption_from_env(db, executor, env=dict(**__import__("os").environ), global_dry_run=False) is None


def test_cli_keeps_preemption_disabled_unless_explicitly_enabled(monkeypatch):
    from qbt_orchestrator.cli import _build_preemption_from_env

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        monkeypatch.delenv("QBT_ORCH_PREEMPTION", raising=False)

        assert _build_preemption_from_env(db, RecordingExecutor(), env=dict(**__import__("os").environ), global_dry_run=False) is None


def test_preemption_respects_hourly_rate_and_per_torrent_cooldown():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.seeding_preemption import PreemptionConfig, SeedingPreemptionService

    now = 10_000
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db)
        executor = RecordingExecutor()
        svc = SeedingPreemptionService(
            db,
            executor,
            dry_run=False,
            now=lambda: now,
            config=PreemptionConfig(max_preemptions_per_hour=1, cooldown_after_preemption_sec=7200),
        )
        snapshots = {
            "newhot": {"hash": "newhot", "name": "NEW-HOT", "category": "auto", "tags": "auto,hot", "progress": 0.9, "amount_left": 1024**3, "dlspeed": 8 * 1024**2, "num_seeds": 10, "num_peers": 10},
            "seed1": {"hash": "seed1", "name": "OLD-SEED", "category": "auto", "tags": "auto", "progress": 1.0, "amount_left": 0, "size": 8 * 1024**3, "ratio": 1.0, "seeding_time": 3600, "upspeed": 0, "content_path": "/downloads/active/OLD-SEED"},
        }

        first = svc.evaluate_and_apply(snapshots, disk_state="watch", trigger_reason="watch")
        second = svc.evaluate_and_apply(snapshots, disk_state="watch", trigger_reason="watch")

        assert first and first.accepted
        assert second is None
        assert executor.posts == [("/api/v2/torrents/stop", {"hashes": "seed1"})]
        blocked = _row(db, "select reason_code,data_json from decision_log where component='seeding_preemption' order by id desc limit 1")
        assert blocked is not None
        assert blocked["reason_code"] in {"hourly_rate_limit", "all_seed_candidates_guarded"}
        assert "cooldown" in blocked["data_json"] or blocked["reason_code"] == "hourly_rate_limit"
