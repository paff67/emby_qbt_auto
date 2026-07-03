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


def _events(db: Path):
    con = sqlite3.connect(db)
    con.row_factory = sqlite3.Row
    rows = [dict(r) for r in con.execute("select component,event_type,hash,message,data_json from events_v2 order by id")]
    con.close()
    return rows


def test_path_reconciler_records_managed_content_path_outside_active_or_temp_roots():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.path_reconcile import QbtPathReconciler

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        reconciler = QbtPathReconciler(db, expected_save_path="/downloads/active", allowed_temp_path="/downloads/incomplete")

        result = reconciler.reconcile({
            "h1": {"hash": "h1", "name": "BBAN-582", "category": "auto", "tags": "auto", "save_path": "/downloads/active", "content_path": "/downloads/BBAN-582", "progress": 0.06},
            "h2": {"hash": "h2", "name": "nana", "category": "auto", "tags": "auto", "save_path": "/downloads/active", "content_path": "/downloads/incomplete/nana", "progress": 0.91},
            "h3": {"hash": "h3", "name": "hold", "category": "auto", "tags": "hold", "save_path": "/downloads/active", "content_path": "/downloads/legacy/hold", "progress": 0.1},
        })

        assert result["scanned"] == 2
        assert result["drift_count"] == 1
        assert result["drifts"][0]["hash"] == "h1"
        assert result["drifts"][0]["reason"] == "content_path_outside_managed_roots"
        rows = _events(db)
        assert [(r["component"], r["event_type"], r["hash"]) for r in rows] == [("qbt_reconcile", "path_drift", "h1")]
        payload = json.loads(rows[0]["data_json"])
        assert payload["content_path"] == "/downloads/BBAN-582"
        assert payload["allowed_roots"] == ["/downloads/active", "/downloads/incomplete"]


def test_path_reconciler_records_save_path_mismatch_but_does_not_duplicate_unchanged_drift():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.path_reconcile import QbtPathReconciler

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        reconciler = QbtPathReconciler(db, expected_save_path="/downloads/active", allowed_temp_path="/downloads/incomplete")
        snapshots = {
            "h1": {"hash": "h1", "name": "Old", "category": "auto", "tags": "auto", "save_path": "/downloads", "content_path": "/downloads/active/Old", "progress": 1.0},
        }

        first = reconciler.reconcile(snapshots)
        second = reconciler.reconcile(snapshots)

        assert first["drift_count"] == 1
        assert second["drift_count"] == 1
        rows = _events(db)
        assert len(rows) == 1
        assert json.loads(rows[0]["data_json"])["save_path"] == "/downloads"


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
    print("ok")