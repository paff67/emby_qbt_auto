#!/usr/bin/env python3
from __future__ import annotations

import json
import sqlite3
import tempfile
from pathlib import Path
from urllib.parse import parse_qs
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))


class FakeQbtPreferencesClient:
    def __init__(self, prefs):
        self.prefs = dict(prefs)
        self.set_calls = []

    def get_preferences(self):
        return dict(self.prefs)

    def set_preferences(self, prefs):
        self.set_calls.append(dict(prefs))
        self.prefs.update(prefs)
        return "Ok."


def test_qbt_docker_client_reads_and_sets_preferences_with_json_payload():
    from qbt_orchestrator.integrations.qbt import QbtDockerClient

    calls = []

    def runner(argv, input_text=None, timeout=None):
        calls.append((list(argv), input_text, timeout))
        if "/api/v2/app/preferences" in argv[-1]:
            return 0, json.dumps({"preallocate_all": True, "incomplete_files_ext": False}), ""
        return 0, "Ok.", ""

    client = QbtDockerClient(runner=runner)

    assert client.get_preferences()["preallocate_all"] is True
    assert client.set_preferences({"preallocate_all": False}) == "Ok."
    assert "/api/v2/app/setPreferences" in calls[-1][0][-1]
    payload = parse_qs(calls[-1][1])
    assert json.loads(payload["json"][0]) == {"preallocate_all": False}


def test_qbt_preferences_guard_records_drift_and_dry_run_without_forcing_incomplete_ext():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.preferences import QbtPreferencesGuard

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        qbt = FakeQbtPreferencesClient({"preallocate_all": True, "incomplete_files_ext": False})
        guard = QbtPreferencesGuard(
            state_db=db,
            qbt=qbt,
            desired_preallocate_all=False,
            desired_incomplete_files_ext=None,
            dry_run=True,
            now=lambda: 100,
        )

        result = guard.reconcile()

        assert result["drift"] == {"preallocate_all": {"actual": True, "desired": False}}
        assert result["would_set"] == {"preallocate_all": False}
        assert qbt.set_calls == []
        con = sqlite3.connect(db)
        action = con.execute("select action_type,path,status,dry_run,payload_json from action_log order by id desc limit 1").fetchone()
        event = con.execute("select component,event_type,data_json from events_v2 order by id desc limit 1").fetchone()
        con.close()
        assert action[:4] == ("qbt_preferences", "/api/v2/app/setPreferences", "dry_run", 1)
        assert json.loads(action[4])["set"] == {"preallocate_all": False}
        assert event[:2] == ("qbt_preferences", "preferences_drift")
        event_data = json.loads(event[2])
        assert "incomplete_files_ext" not in event_data["drift"]
        assert result["observed"] == {"incomplete_files_ext": False}


def test_qbt_preferences_guard_live_sets_only_configured_preferences():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.preferences import QbtPreferencesGuard

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        qbt = FakeQbtPreferencesClient({"preallocate_all": True, "incomplete_files_ext": False})
        guard = QbtPreferencesGuard(
            state_db=db,
            qbt=qbt,
            desired_preallocate_all=False,
            desired_incomplete_files_ext=True,
            dry_run=False,
            now=lambda: 100,
        )

        result = guard.reconcile()

        assert qbt.set_calls == [{"preallocate_all": False, "incomplete_files_ext": True}]
        assert result["applied"] == {"preallocate_all": False, "incomplete_files_ext": True}
        con = sqlite3.connect(db)
        status = con.execute("select status,dry_run from action_log where action_type='qbt_preferences'").fetchone()
        con.close()
        assert status == ("succeeded", 0)


def test_sqlite_maintenance_runs_qbt_preferences_guard_when_configured():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.maintenance import SQLiteMaintenanceService
    from qbt_orchestrator.preferences import QbtPreferencesGuard

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        qbt = FakeQbtPreferencesClient({"preallocate_all": True, "incomplete_files_ext": False})
        guard = QbtPreferencesGuard(db, qbt, desired_preallocate_all=False, dry_run=True, now=lambda: 100)
        service = SQLiteMaintenanceService(db, now=lambda: 100, preferences_guard=guard)

        result = service.run_once()

        assert result["qbt_preferences"]["would_set"] == {"preallocate_all": False}


if __name__ == "__main__":
    inspect = __import__("inspect")
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn) and not inspect.signature(fn).parameters:
            fn()
    print("ok")
