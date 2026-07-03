from __future__ import annotations
import argparse, json, os, shutil, sqlite3
from pathlib import Path
from typing import Sequence
from .config import load_config
from .carousel import CarouselService
from .db import migrate, readonly_counts, recover_jobs
from .executor import Executor
from .integrations.qbt import QbtDockerClient
from .integrations.rclone import RcloneClient
from .integrations.emby import EmbyClient
from .integrations.telegram import TelegramHttpApi, TelegramNotificationSender
from .io_governor import IoGovernor, UploadBackpressurePolicy
from .junk_janitor import JunkJanitorService
from .maintenance import SQLiteMaintenanceService
from .media import EmbyRefreshWorker, MediaPipelineJobRunner, MediaPipelineService
from .orphan_janitor import OrphanJanitorService
from .path_reconcile import QbtPathReconciler
from .preferences import QbtPreferencesGuard
from .runtime import BotCommandRepository, BotNotificationRepository, CommandProcessor, TorrentJobRepository, UploadJobRunner, reconcile_jobs
from .runtime import ObservabilityStore
from .service import DaemonRuntime, build_telegram_supervisor_from_env


class PassthroughBackfill:
    def scrape_one(self, media_group_key, manifest_id):
        return {"status": "not_found", "artifacts": [], "media_group_key": media_group_key, "manifest_id": manifest_id}

def _print_json(obj) -> None: print(json.dumps(obj, ensure_ascii=False, indent=2))

def _truthy(value: str | None) -> bool | None:
    if value is None:
        return None
    return value.strip().lower() in {"1", "true", "yes", "on"}

def _bool_or_none(value: str | None) -> bool | None:
    if value is None or value.strip() == "":
        return None
    return _truthy(value)

def _free_bytes_for(path: str):
    def sample() -> int:
        if hasattr(os, "statvfs"):
            st = os.statvfs(path)
            return int(st.f_bavail * st.f_frsize)
        return int(shutil.disk_usage(path).free)
    return sample

def _iowait_provider_from_env():
    raw = os.environ.get("QBT_ORCH_IOWAIT_PERCENT")
    fixed = float(raw) if raw not in (None, "") else 0.0
    def sample() -> float:
        return fixed
    return sample

def _connect_readonly(db: Path) -> sqlite3.Connection:
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    return con

def _status_payload(db: Path, view: str | None) -> dict:
    if view in {None, "all"}:
        return {"counts": readonly_counts(db), "recoverable_jobs": len(recover_jobs(db))}
    con = _connect_readonly(db)
    try:
        if view == "disk":
            row = con.execute("select * from disk_state where id=1").fetchone()
            return dict(row) if row else {}
        if view == "queue":
            rows = con.execute("select state,count(*) as count from torrent_jobs group by state").fetchall()
            by_state = {str(r["state"]): int(r["count"]) for r in rows}
            return {"by_state": by_state, "recoverable_jobs": len(recover_jobs(db))}
        if view == "db":
            return {"counts": readonly_counts(db), "recoverable_jobs": len(recover_jobs(db))}
        if view == "perf":
            recent_events = con.execute("select count(*) from events_v2").fetchone()[0]
            latest_metrics = [dict(r) for r in con.execute("select * from metrics_snapshots order by id desc limit 10")]
            return {"recent_events": int(recent_events), "latest_metrics": latest_metrics}
        if view == "io":
            rows = [dict(r) for r in con.execute("select * from metrics_snapshots where component in ('io','rclone','upload') order by id desc limit 10")]
            return {"metrics": rows}
        if view == "api":
            rows = [dict(r) for r in con.execute("select ts,level,component,event_type,message from events_v2 where component in ('qbt','telegram','emby','rclone') order by id desc limit 20")]
            return {"events": rows}
    finally:
        con.close()
    raise SystemExit(f"unknown status view: {view}")

def _build_runtime(ns, db: Path, force_dry_run: bool | None = None) -> tuple[DaemonRuntime, bool]:
    cfg = load_config(ns.config) if ns.config else None
    env_dry_run = _truthy(os.environ.get("QBT_ORCH_DRY_RUN"))
    dry_run = bool(ns.dry_run or (force_dry_run if force_dry_run is not None else (env_dry_run if env_dry_run is not None else (cfg.dry_run if cfg else True))))
    state_db = Path(os.environ.get("QBT_ORCH_STATE_DB") or (cfg.state_db if cfg else str(db)))
    qbt_cfg = cfg.qbt if cfg else None
    qbt = QbtDockerClient(
        container=qbt_cfg.container if qbt_cfg else "qbittorrent",
        api_base=qbt_cfg.api_base if qbt_cfg else "http://127.0.0.1:8080",
        timeout=int(os.environ.get("QBT_ORCH_QBT_API_TIMEOUT_SEC", "10")),
        api_max_requests_per_sec=float(os.environ.get("QBT_ORCH_QBT_API_MAX_RPS", "4")),
    )
    executor = Executor(qbt, dry_run=dry_run)
    disk_path = os.environ.get("QBT_ORCH_DISK_PATH", "/data/downloads")
    telegram_supervisor = build_telegram_supervisor_from_env(state_db, os.environ)
    notification_repo = BotNotificationRepository(state_db)
    command_processor = CommandProcessor(BotCommandRepository(state_db), executor, notifications=notification_repo)
    telegram_token = os.environ.get("QBT_ORCH_TELEGRAM_TOKEN") or os.environ.get("TELEGRAM_BOT_TOKEN")
    telegram_notification_sender = TelegramNotificationSender(notification_repo, TelegramHttpApi(telegram_token)) if telegram_token else None
    notification_env = _truthy(os.environ.get("QBT_ORCH_NOTIFICATION_DRY_RUN"))
    notification_dry_run = True if dry_run else (notification_env if notification_env is not None else True)
    planner_env = _truthy(os.environ.get("QBT_ORCH_PLANNER_DRY_RUN"))
    planner_dry_run = True if dry_run else (planner_env if planner_env is not None else True)
    rclone_cfg = cfg.rclone if cfg else None
    free_bytes_provider = _free_bytes_for(disk_path)
    io_governor = IoGovernor(
        iowait_provider=_iowait_provider_from_env(),
        free_bytes_provider=free_bytes_provider,
    )
    rclone = RcloneClient(
        config_path=rclone_cfg.config if rclone_cfg else "/root/.config/rclone/rclone.conf",
        transfers=rclone_cfg.transfers if rclone_cfg else 1,
        checkers=rclone_cfg.checkers if rclone_cfg else 2,
        limits_provider=io_governor.rclone_limits,
    )
    upload_env = _truthy(os.environ.get("QBT_ORCH_UPLOAD_DRY_RUN"))
    upload_dry_run = True if dry_run else (upload_env if upload_env is not None else True)
    upload_runner = UploadJobRunner(TorrentJobRepository(state_db), rclone, executor)
    file_batch_env = _truthy(os.environ.get("QBT_ORCH_FILE_BATCH_DRY_RUN"))
    file_batch_dry_run = True if dry_run else (file_batch_env if file_batch_env is not None else True)
    upload_backpressure_policy = UploadBackpressurePolicy(
        max_backlog_bytes=int(float(os.environ.get("QBT_ORCH_UPLOAD_BACKPRESSURE_MAX_BACKLOG_GB", "20")) * 1024**3),
        max_oldest_pending_sec=int(os.environ.get("QBT_ORCH_UPLOAD_BACKPRESSURE_MAX_OLDEST_PENDING_SEC", "3600")),
    )
    media_env = _truthy(os.environ.get("QBT_ORCH_MEDIA_PIPELINE_DRY_RUN"))
    media_pipeline_dry_run = True if dry_run else (media_env if media_env is not None else True)
    media_runner = MediaPipelineJobRunner(
        TorrentJobRepository(state_db),
        MediaPipelineService(state_db, PassthroughBackfill(), emby_prefix=cfg.emby.container_media_prefix if cfg else "/media/gcrypt"),
    )
    emby_env = _truthy(os.environ.get("QBT_ORCH_EMBY_REFRESH_DRY_RUN"))
    emby_refresh_dry_run = True if dry_run else (emby_env if emby_env is not None else True)
    emby_client = EmbyClient(
        base_url=os.environ.get("EMBY_BASE_URL", "http://127.0.0.1:8096"),
        api_key=os.environ.get("EMBY_API_KEY", ""),
        media_prefix=cfg.emby.container_media_prefix if cfg else "/media/gcrypt",
    )
    emby_worker = EmbyRefreshWorker(state_db, emby_client, media_prefix=cfg.emby.container_media_prefix if cfg else "/media/gcrypt")
    preferences_guard = None
    prefs_guard_enabled = _truthy(os.environ.get("QBT_ORCH_QBT_PREFERENCES_GUARD"))
    if prefs_guard_enabled is None:
        prefs_guard_enabled = True
    if prefs_guard_enabled:
        prefs_dry_env = _truthy(os.environ.get("QBT_ORCH_QBT_PREFERENCES_DRY_RUN"))
        preferences_guard = QbtPreferencesGuard(
            state_db,
            qbt,
            desired_preallocate_all=_truthy(os.environ.get("QBT_ORCH_QBT_PREALLOCATE_ALL")) if os.environ.get("QBT_ORCH_QBT_PREALLOCATE_ALL") is not None else (cfg.qbt_preferences.preallocate_all if cfg else False),
            desired_incomplete_files_ext=_bool_or_none(os.environ.get("QBT_ORCH_QBT_INCOMPLETE_FILES_EXT")),
            dry_run=True if dry_run else (prefs_dry_env if prefs_dry_env is not None else True),
        )
    maintenance_service = SQLiteMaintenanceService(
        state_db,
        retention_days=int(os.environ.get("QBT_ORCH_RETENTION_DAYS", "5")),
        retention_delete_batch_size=int(os.environ.get("QBT_ORCH_RETENTION_DELETE_BATCH_SIZE", "1000")),
        journal_size_limit_bytes=int(os.environ.get("QBT_ORCH_SQLITE_JOURNAL_SIZE_LIMIT_BYTES", str(64 * 1024 * 1024))),
        preferences_guard=preferences_guard,
    )
    path_reconciler = None
    path_reconcile_enabled = _truthy(os.environ.get("QBT_ORCH_PATH_RECONCILE"))
    if path_reconcile_enabled is None:
        path_reconcile_enabled = True
    if path_reconcile_enabled:
        path_reconciler = QbtPathReconciler(
            state_db,
            expected_save_path=qbt_cfg.save_path if qbt_cfg else "/downloads/active",
            allowed_temp_path=qbt_cfg.temp_path if qbt_cfg else "/downloads/incomplete",
        )
    orphan_janitor = None
    orphan_enabled = _truthy(os.environ.get("QBT_ORCH_ORPHAN_JANITOR"))
    if orphan_enabled is None:
        orphan_enabled = True
    if orphan_enabled:
        orphan_dry_env = _truthy(os.environ.get("QBT_ORCH_ORPHAN_JANITOR_DRY_RUN"))
        orphan_janitor = OrphanJanitorService(
            state_db=state_db,
            managed_root=os.environ.get("QBT_ORCH_ORPHAN_ROOT", "/data/downloads/active"),
            trash_dir=os.environ.get("QBT_ORCH_ORPHAN_TRASH_DIR", "/data/downloads/.orchestrator-trash"),
            dry_run=True if dry_run else (orphan_dry_env if orphan_dry_env is not None else True),
            min_age_sec=int(os.environ.get("QBT_ORCH_ORPHAN_MIN_AGE_SEC", "86400")),
            min_confirmations=int(os.environ.get("QBT_ORCH_ORPHAN_CONFIRMATIONS", "2")),
            host_downloads=os.environ.get("QBT_ORCH_HOST_DOWNLOADS", "/data/downloads"),
            container_downloads=os.environ.get("QBT_ORCH_CONTAINER_DOWNLOADS", "/downloads"),
        )
    junk_janitor = None
    junk_enabled = _truthy(os.environ.get("QBT_ORCH_JUNK_JANITOR"))
    if junk_enabled is None:
        junk_enabled = True
    if junk_enabled:
        junk_dry_env = _truthy(os.environ.get("QBT_ORCH_JUNK_JANITOR_DRY_RUN"))
        junk_janitor = JunkJanitorService(
            state_db,
            executor,
            managed_root=os.environ.get("QBT_ORCH_JUNK_ROOT", "/data/downloads/active"),
            trash_dir=os.environ.get("QBT_ORCH_JUNK_TRASH_DIR", "/data/downloads/.orchestrator-trash"),
            dry_run=True if dry_run else (junk_dry_env if junk_dry_env is not None else True),
            stable_mtime_sec=int(os.environ.get("QBT_ORCH_JUNK_STABLE_MTIME_SEC", "60")),
            max_auto_quarantine_bytes=int(os.environ.get("QBT_ORCH_JUNK_MAX_AUTO_QUARANTINE_BYTES", str(10 * 1024 * 1024))),
            active_fast_download_bps=int(os.environ.get("QBT_ORCH_JUNK_ACTIVE_FAST_BPS", str(2 * 1024 * 1024))),
            host_downloads=os.environ.get("QBT_ORCH_HOST_DOWNLOADS", "/data/downloads"),
            container_downloads=os.environ.get("QBT_ORCH_CONTAINER_DOWNLOADS", "/downloads"),
        )
    carousel_enabled = _truthy(os.environ.get("QBT_ORCH_CAROUSEL"))
    if carousel_enabled is None:
        carousel_enabled = True
    carousel_dry_env = _truthy(os.environ.get("QBT_ORCH_CAROUSEL_DRY_RUN"))
    carousel_dry_run = True if dry_run else (carousel_dry_env if carousel_dry_env is not None else True)
    carousel_service = None
    if carousel_enabled:
        carousel_service = CarouselService(
            state_db,
            executor,
            dry_run=carousel_dry_run,
            concurrency=int(os.environ.get("QBT_ORCH_CAROUSEL_CONCURRENCY", "3")),
            probe_duration_sec=int(os.environ.get("QBT_ORCH_CAROUSEL_PROBE_DURATION_SEC", "1800")),
        )
    runtime = DaemonRuntime(
        state_db=state_db,
        qbt=qbt,
        executor=executor,
        free_bytes_provider=free_bytes_provider,
        dry_run=dry_run,
        safety_interval=getattr(ns, "safety_interval", 2.0),
        telegram_supervisor=telegram_supervisor,
        command_processor=command_processor,
        planner_dry_run=planner_dry_run,
        upload_runner=upload_runner,
        upload_dry_run=upload_dry_run,
        file_batch_dry_run=file_batch_dry_run,
        upload_backpressure_policy=upload_backpressure_policy,
        host_downloads=os.environ.get("QBT_ORCH_HOST_DOWNLOADS", "/data/downloads"),
        container_downloads=os.environ.get("QBT_ORCH_CONTAINER_DOWNLOADS", "/downloads"),
        rclone_remote=rclone_cfg.remote if rclone_cfg else "gcrypt:",
        media_pipeline_runner=media_runner,
        media_pipeline_dry_run=media_pipeline_dry_run,
        emby_refresh_worker=emby_worker,
        emby_refresh_dry_run=emby_refresh_dry_run,
        telegram_notification_sender=telegram_notification_sender,
        notification_dry_run=notification_dry_run,
        maintenance_service=maintenance_service,
        orphan_janitor=orphan_janitor,
        junk_janitor=junk_janitor,
        junk_file_refresh_limit=int(os.environ.get("QBT_ORCH_JUNK_FILE_REFRESH_LIMIT", "3")),
        carousel_service=carousel_service,
        carousel_enabled=carousel_enabled,
        carousel_dry_run=carousel_dry_run,
        path_reconciler=path_reconciler,
    )
    return runtime, dry_run

def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="qbt-orchestrator"); sub = parser.add_subparsers(dest="cmd", required=True)
    for name in ["status", "events", "trace", "once", "daemon", "reconcile", "migrate"]:
        p = sub.add_parser(name); p.add_argument("target", nargs="?"); p.add_argument("--state-db", default="/var/lib/qbt-orchestrator/state.sqlite"); p.add_argument("--config", default=None); p.add_argument("--json", action="store_true"); p.add_argument("--dry-run", action="store_true"); p.add_argument("--apply", action="store_true")
        if name == "daemon":
            p.add_argument("--max-safety-ticks", type=int, default=None)
            p.add_argument("--safety-interval", type=float, default=2.0)
        if name == "reconcile":
            p.add_argument("--now", type=int, default=None)
    ns = parser.parse_args(list(argv) if argv is not None else None); db = Path(ns.state_db)
    if ns.cmd == "migrate":
        sql = migrate(db, dry_run=not ns.apply); print((json.dumps({"dry_run": not ns.apply, "statements": len(sql)}) if ns.json else f"migration {'dry-run' if not ns.apply else 'applied'}: {len(sql)} statements")); return 0
    if not db.exists(): migrate(db, False)
    if ns.cmd == "status":
        payload = _status_payload(db, ns.target); _print_json(payload) if ns.json else print(payload); return 0
    if ns.cmd == "events":
        con = sqlite3.connect(db); rows = con.execute("select ts,level,component,event_type,message from events_v2 order by id desc limit 50").fetchall(); con.close(); _print_json([tuple(r) for r in rows]) if ns.json else print(rows); return 0
    if ns.cmd == "trace":
        trace = ObservabilityStore(db).trace(str(ns.target or ""))
        payload = {"target": ns.target, **trace}
        _print_json(payload) if ns.json else print(payload)
        return 0
    if ns.cmd == "once":
        runtime, dry_run = _build_runtime(ns, db)
        ticks = runtime.run(max_safety_ticks=1)
        print(f"once {'dry-run' if dry_run else 'live'} completed after {ticks} safety tick")
        return 0
    if ns.cmd == "daemon":
        runtime, dry_run = _build_runtime(ns, db)
        runtime.install_signal_handlers()
        ticks = runtime.run(max_safety_ticks=ns.max_safety_ticks)
        print(f"daemon {'dry-run' if dry_run else 'live'} stopped after {ticks} safety ticks")
        return 0
    if ns.cmd in {"reconcile"}:
        payload = reconcile_jobs(db, now=ns.now, dry_run=not ns.apply)
        _print_json(payload) if ns.json else print(payload)
        return 0
    return 2
if __name__ == "__main__": raise SystemExit(main())
