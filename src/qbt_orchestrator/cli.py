from __future__ import annotations
import argparse, json, sqlite3
from pathlib import Path
from typing import Sequence
from .db import migrate, readonly_counts, recover_jobs

def _print_json(obj) -> None: print(json.dumps(obj, ensure_ascii=False, indent=2))

def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="qbt-orchestrator"); sub = parser.add_subparsers(dest="cmd", required=True)
    for name in ["status", "events", "trace", "once", "daemon", "reconcile", "migrate"]:
        p = sub.add_parser(name); p.add_argument("target", nargs="?"); p.add_argument("--state-db", default="/var/lib/qbt-orchestrator/state.sqlite"); p.add_argument("--config", default=None); p.add_argument("--json", action="store_true"); p.add_argument("--dry-run", action="store_true"); p.add_argument("--apply", action="store_true")
    ns = parser.parse_args(list(argv) if argv is not None else None); db = Path(ns.state_db)
    if ns.cmd == "migrate":
        sql = migrate(db, dry_run=not ns.apply); print((json.dumps({"dry_run": not ns.apply, "statements": len(sql)}) if ns.json else f"migration {'dry-run' if not ns.apply else 'applied'}: {len(sql)} statements")); return 0
    if not db.exists(): migrate(db, False)
    if ns.cmd == "status":
        payload = {"counts": readonly_counts(db), "recoverable_jobs": len(recover_jobs(db))}; _print_json(payload) if ns.json else print(payload); return 0
    if ns.cmd == "events":
        con = sqlite3.connect(db); rows = con.execute("select ts,level,component,event_type,message from events_v2 order by id desc limit 50").fetchall(); con.close(); _print_json([tuple(r) for r in rows]) if ns.json else print(rows); return 0
    if ns.cmd == "trace": _print_json({"target": ns.target, "events": [], "actions": [], "decisions": []}) if ns.json else print(f"trace {ns.target}: no records"); return 0
    if ns.cmd in {"once", "daemon", "reconcile"}: print(f"{ns.cmd} {'dry-run' if ns.dry_run else 'live'} completed without external actions"); return 0
    return 2
if __name__ == "__main__": raise SystemExit(main())
