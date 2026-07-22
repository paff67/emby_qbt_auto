#!/usr/bin/env python3
from __future__ import annotations

import sys
import argparse
import json
import tempfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))

GIB = 1024**3


def test_config_capacity_policy_is_authoritative_when_env_is_absent():
    from qbt_orchestrator.cli import _resolve_capacity_policy
    from qbt_orchestrator.config import load_config_from_dict

    cfg = load_config_from_dict(
        {
            "disk": {
                "target_min_free_gb": 3,
                "pause_new_free_below_gb": 3,
                "pause_all_downloads_free_below_gb": 2,
                "ok_free_gb": 5,
                "explore_free_gb": 8,
            }
        }
    )

    policy = _resolve_capacity_policy(cfg.disk, {})

    assert policy.disk_floor_bytes == 3 * GIB
    assert policy.emergency_floor_bytes == 2 * GIB
    assert policy.recovery_enter_bytes == 3 * GIB
    assert policy.drain_exit_bytes == 5 * GIB
    assert policy.explore_enter_bytes == 8 * GIB


def test_config_capacity_policy_rejects_silent_environment_conflict():
    from qbt_orchestrator.cli import _resolve_capacity_policy
    from qbt_orchestrator.config import load_config_from_dict

    cfg = load_config_from_dict({"disk": {"target_min_free_gb": 3}})

    with pytest.raises(ValueError, match="QBT_ORCH_DISK_FLOOR_GB"):
        _resolve_capacity_policy(cfg.disk, {"QBT_ORCH_DISK_FLOOR_GB": "2"})


def test_matching_environment_capacity_value_is_accepted_but_not_authoritative():
    from qbt_orchestrator.cli import _resolve_capacity_policy
    from qbt_orchestrator.config import load_config_from_dict

    cfg = load_config_from_dict({"disk": {"target_min_free_gb": 3}})

    policy = _resolve_capacity_policy(cfg.disk, {"QBT_ORCH_DISK_FLOOR_GB": "3"})

    assert policy.disk_floor_bytes == 3 * GIB


def test_runtime_uses_validated_config_capacity_policy(monkeypatch):
    from qbt_orchestrator.cli import _build_runtime
    from qbt_orchestrator.db import migrate

    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        db = root / "state.sqlite"
        config = root / "config.json"
        config.write_text(
            json.dumps(
                {
                    "dry_run": True,
                    "paths": {"state_db": str(db)},
                    "disk": {
                        "target_min_free_gb": 3,
                        "pause_new_free_below_gb": 3,
                        "pause_all_downloads_free_below_gb": 2,
                        "ok_free_gb": 5,
                        "explore_free_gb": 8,
                    },
                }
            ),
            encoding="utf-8",
        )
        migrate(db, dry_run=False)
        for key, value in {
            "QBT_ORCH_STATE_DB": str(db),
            "QBT_ORCH_DISK_PATH": td,
            "QBT_ORCH_HOST_DOWNLOADS": td,
            "QBT_ORCH_CONTAINER_DOWNLOADS": "/downloads",
            "QBT_ORCH_ORPHAN_JANITOR": "0",
            "QBT_ORCH_JUNK_JANITOR": "0",
            "QBT_ORCH_CAROUSEL": "0",
            "QBT_ORCH_QBT_PREFERENCES_GUARD": "0",
            "QBT_ORCH_PATH_RECONCILE": "0",
            "QBT_ORCH_SOAK_ENABLED": "0",
            "QBT_ORCH_CAPACITY_RECLAIM": "1",
            "QBT_ORCH_CAPACITY_RECLAIM_DRY_RUN": "1",
            "QBT_ORCH_CAPACITY_RECLAIM_ROOT": str(root / "incomplete"),
        }.items():
            monkeypatch.setenv(key, value)
        for key in (
            "QBT_ORCH_DISK_FLOOR_GB",
            "QBT_ORCH_EMERGENCY_FLOOR_GB",
            "QBT_ORCH_RECOVERY_ENTER_GB",
            "QBT_ORCH_DRAIN_EXIT_GB",
            "QBT_ORCH_EXPLORE_ENTER_GB",
        ):
            monkeypatch.delenv(key, raising=False)
        ns = argparse.Namespace(
            cmd="daemon",
            dry_run=True,
            config=str(config),
            safety_interval=0,
            max_safety_ticks=1,
        )

        runtime, _dry_run = _build_runtime(ns, db)

        assert runtime.disk_floor_bytes == 3 * GIB
        assert runtime.emergency_floor_bytes == 2 * GIB
        assert runtime.recovery_enter_bytes == 3 * GIB
        assert runtime.drain_exit_bytes == 5 * GIB
        assert runtime.explore_enter_bytes == 8 * GIB
        assert runtime.capacity_reclaimer is not None
        assert runtime.capacity_reclaimer.dry_run is True
