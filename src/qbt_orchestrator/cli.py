from __future__ import annotations
import argparse, json, os, sqlite3
from pathlib import Path
from typing import Sequence
from .config import load_config
from .db import migrate, readonly_counts, recover_jobs
from .executor import Executor
from .integrations.qbt import QbtDockerClient
from .service import DaemonRuntime, build_telegram_supervisor_from_env

def _print_json(obj) -> None: print(json.dumps(obj, ensure_ascii=False, indent=2))

def _truthy(value: str | None) -> bool | None:
    if value is None:
        return None
    return value.strip().lower() in {"1", "true", "yes", "on"}

def _free_bytes_for(path: str):
    def sample() -> int:
        st = os.statvfs(path)
        return int(st.f_bavail * st.f_frsize)
    return sample

def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="qbt-orchestrator"); sub = parser.add_subparsers(dest="cmd", required=True)
    for name in ["status", "events", "trace", "once", "daemon", "reconcile", "migrate"]:
        p = sub.add_parser(name); p.add_argument("target", nargs="?"); p.add_argument("--state-db", default="/var/lib/qbt-orchestrator/state.sqlite"); p.add_argument("--config", default=None); p.add_argument("--json", action="store_true"); p.add_argument("--dry-run", action="store_true"); p.add_argument("--apply", action="store_true")
        if name == "daemon":
            p.add_argument("--max-safety-ticks", type=int, default=None)
            p.add_argument("--safety-interval", type=float, default=2.0)
    ns = parser.parse_args(list(argv) if argv is not None else None); db = Path(ns.state_db)
    if ns.cmd == "migrate":
        sql = migrate(db, dry_run=not ns.apply); print((json.dumps({"dry_run": not ns.apply, "statements": len(sql)}) if ns.json else f"migration {'dry-run' if not ns.apply else 'applied'}: {len(sql)} statements")); return 0
    if not db.exists(): migrate(db, False)
    if ns.cmd == "status":
        payload = {"counts": readonly_counts(db), "recoverable_jobs": len(recover_jobs(db))}; _print_json(payload) if ns.json else print(payload); return 0
    if ns.cmd == "events":
        con = sqlite3.connect(db); rows = con.execute("select ts,level,component,event_type,message from events_v2 order by id desc limit 50").fetchall(); con.close(); _print_json([tuple(r) for r in rows]) if ns.json else print(rows); return 0
    if ns.cmd == "trace": _print_json({"target": ns.target, "events": [], "actions": [], "decisions": []}) if ns.json else print(f"trace {ns.target}: no records"); return 0
    if ns.cmd == "daemon":
        cfg = load_config(ns.config) if ns.config else None
        env_dry_run = _truthy(os.environ.get("QBT_ORCH_DRY_RUN"))
        dry_run = bool(ns.dry_run or (env_dry_run if env_dry_run is not None else (cfg.dry_run if cfg else True)))
        state_db = Path(os.environ.get("QBT_ORCH_STATE_DB") or (cfg.state_db if cfg else str(db)))
        qbt_cfg = cfg.qbt if cfg else None
        qbt = QbtDockerClient(container=qbt_cfg.container if qbt_cfg else "qbittorrent", api_base=qbt_cfg.api_base if qbt_cfg else "http://127.0.0.1:8080")
        executor = Executor(qbt, dry_run=dry_run)
        disk_path = os.environ.get("QBT_ORCH_DISK_PATH", "/data/downloads")
        telegram_supervisor = build_telegram_supervisor_from_env(state_db, os.environ)
        runtime = DaemonRuntime(state_db=state_db, qbt=qbt, executor=executor, free_bytes_provider=_free_bytes_for(disk_path), dry_run=dry_run, safety_interval=ns.safety_interval, telegram_supervisor=telegram_supervisor)
        runtime.install_signal_handlers()
        ticks = runtime.run(max_safety_ticks=ns.max_safety_ticks)
        print(f"daemon {'dry-run' if dry_run else 'live'} stopped after {ticks} safety ticks")
        return 0
    if ns.cmd in {"once", "reconcile"}: print(f"{ns.cmd} {'dry-run' if ns.dry_run else 'live'} completed without external actions"); return 0
    return 2
if __name__ == "__main__": raise SystemExit(main())
