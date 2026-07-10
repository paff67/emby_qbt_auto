from __future__ import annotations

from dataclasses import dataclass
import json
import signal
import sqlite3
import threading
import time
from pathlib import Path
from typing import Callable, Mapping

from .alerts import SchedulerAlertConfig, SchedulerAlertService
from .budget import calculate_growth_budget, resource_claims_from_rows
from .capacity_state import (
    CapacityStateStore,
    ModeController,
    build_capacity_observation,
    detect_capacity_state,
)
from .carousel import CarouselService
from .daemon import SafetyMonitor
from .db import migrate, readonly_connect, start_persistent_write_actor, stop_write_actor, write_transaction
from .file_batch import FileBatchService, active_pipeline_batch_hashes
from .integrations.telegram import TelegramHttpApi, TelegramPollingService
from .junk_janitor import JunkJanitorService
from .maintenance import SQLiteMaintenanceService
from .observe_promotion import ObservePromotionService
from .observability import redact
from .planner import DownloadPlanner
from .policies.disk import classify_disk
from .periodic import PeriodicTask, PeriodicWorker
from .runtime import BotCommandRepository, BotNotificationRepository, ObservabilityStore
from .scheduler_engine import SchedulerEngine
from .soak_queue import SoakQueueConfig, SoakQueueResult, SoakQueueService
from .telegram_control import TelegramAuthorizer
from .work_items import build_full_finish_work_items


@dataclass
class LoopTask:
    name: str
    interval_sec: float
    callback: Callable[[], object]
    next_due: float = 0.0
    max_runtime_sec: float = 1.0

    def due(self, now_monotonic: float) -> bool:
        return now_monotonic >= self.next_due

    def mark_ran(self, now_monotonic: float) -> None:
        self.next_due = now_monotonic + self.interval_sec


class TelegramSupervisor:
    """Supervise Telegram polling outside the 2s safety loop."""

    def __init__(self, service, interval: float = 1.0, max_backoff: float = 60.0):
        self.service = service
        self.interval = interval
        self.max_backoff = max_backoff
        self.consecutive_failures = 0
        self._stopping = threading.Event()
        self._thread: threading.Thread | None = None

    def poll_once_supervised(self) -> int:
        try:
            count = int(self.service.poll_once())
            self.consecutive_failures = 0
            return count
        except Exception:
            self.consecutive_failures += 1
            return 0

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stopping.clear()
        self._thread = threading.Thread(target=self._run, name="telegram-supervisor", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stopping.set()
        if self._thread:
            self._thread.join(timeout=5)

    def _run(self) -> None:
        while not self._stopping.is_set():
            self.poll_once_supervised()
            if self.consecutive_failures:
                sleep_for = min(self.max_backoff, max(self.interval, 2 ** min(self.consecutive_failures, 6)))
            else:
                sleep_for = self.interval
            self._stopping.wait(sleep_for)


def _parse_id_set(raw: str | None) -> set[int]:
    if not raw:
        return set()
    out: set[int] = set()
    for item in raw.replace(";", ",").split(","):
        item = item.strip()
        if item:
            out.add(int(item))
    return out


def build_telegram_supervisor_from_env(
    state_db: str | Path,
    env: Mapping[str, str] | None = None,
    api_factory=TelegramHttpApi,
) -> TelegramSupervisor | None:
    env = env or {}
    token = env.get("QBT_ORCH_TELEGRAM_TOKEN") or env.get("TELEGRAM_BOT_TOKEN")
    if not token:
        return None
    authorizer = TelegramAuthorizer(
        viewers=_parse_id_set(env.get("QBT_ORCH_TG_VIEWERS")),
        operators=_parse_id_set(env.get("QBT_ORCH_TG_OPERATORS")),
        admins=_parse_id_set(env.get("QBT_ORCH_TG_ADMINS")),
    )
    command_store = BotCommandRepository(state_db)
    api = api_factory(token)
    poll_timeout = int(env.get("QBT_ORCH_TG_POLL_TIMEOUT", "30"))
    interval = float(env.get("QBT_ORCH_TG_SUPERVISOR_INTERVAL", "1"))
    max_backoff = float(env.get("QBT_ORCH_TG_MAX_BACKOFF", "60"))
    return TelegramSupervisor(TelegramPollingService(api, authorizer, command_store, poll_timeout=poll_timeout), interval=interval, max_backoff=max_backoff)


class DaemonRuntime:
    """Small, continuously running daemon harness for the safety fast-path.

    The full scheduler/upload/media workers are deliberately backed by SQLite
    queues in other modules.  This runtime provides the systemd-friendly process
    shell and the 2s safety loop that must stay alive even when other workers
    fail or are disabled.
    """

    def __init__(
        self,
        state_db: str | Path,
        qbt,
        executor,
        free_bytes_provider: Callable[[], int],
        dry_run: bool,
        safety_interval: float = 2.0,
        managed_count_provider: Callable[[], int] | None = None,
        telegram_supervisor: TelegramSupervisor | None = None,
        command_processor=None,
        loop_tasks: list[LoopTask] | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
        planner_dry_run: bool = True,
        planner_active_slots: int = 5,
        planner_slow_active_demote_sec: int = 180,
        scheduler_engine_mode: str = "legacy",
        scheduler_engine=None,
        scheduler_unit_bytes: int = 64 * 1024**2,
        disk_floor_bytes: int = 3 * 1024**3,
        emergency_floor_bytes: int = int(1.5 * 1024**3),
        recovery_enabled: bool = True,
        recovery_enter_bytes: int | None = None,
        drain_exit_bytes: int | None = None,
        explore_enter_bytes: int = 8 * 1024**3,
        recovery_margin_bytes: int = 256 * 1024**2,
        recovery_active_slots: int = 4,
        recovery_max_remaining_bytes: int = int(1.5 * 1024**3),
        upload_runner=None,
        upload_dry_run: bool = True,
        cleanup_runner=None,
        cleanup_dry_run: bool = True,
        file_batch_dry_run: bool = True,
        upload_backpressure_policy=None,
        host_downloads: str = "/data/downloads",
        container_downloads: str = "/downloads",
        rclone_remote: str = "gcrypt:",
        media_pipeline_runner=None,
        media_pipeline_dry_run: bool = True,
        emby_refresh_worker=None,
        emby_refresh_dry_run: bool = True,
        telegram_notification_sender=None,
        notification_dry_run: bool = True,
        maintenance_service=None,
        orphan_janitor=None,
        junk_janitor=None,
        observe_promotion_service: ObservePromotionService | None = None,
        junk_file_refresh_limit: int = 3,
        carousel_service=None,
        carousel_enabled: bool = True,
        carousel_dry_run: bool = True,
        path_reconciler=None,
        preemption_service=None,
        soak_queue_service=None,
        soak_enabled: bool = False,
        soak_dry_run: bool = True,
        soak_config: SoakQueueConfig | None = None,
        batch_pipeline_enabled: bool = False,
        batch_live_verify: bool = False,
        batch_allow_hashes: set[str] | None = None,
        batch_allow_tag: str = "",
        batch_max_live_batch_bytes: int = 0,
        batch_max_new_per_tick: int = 1_000_000,
        background_event_workers: bool = False,
        event_worker_interval: float = 1.0,
        event_worker_join_timeout: float = 0.2,
        scheduler_alerts_enabled: bool = False,
        scheduler_alert_chat_ids: list[str] | None = None,
        scheduler_alert_interval_sec: int = 1800,
        disk_alert_margin_bytes: int = 512 * 1024**2,
        capacity_deadlock_alerts_enabled: bool = True,
        scheduler_alert_service=None,
        sync_repeated_full_limit: int = 3,
        sync_degraded_interval_sec: float = 10.0,
        background_periodic_workers: bool = False,
        periodic_worker_join_timeout: float = 5.0,
    ):
        self.state_db = Path(state_db)
        migrate(self.state_db, dry_run=False)
        self.qbt = qbt
        self.executor = executor
        self.free_bytes_provider = free_bytes_provider
        self.dry_run = dry_run
        self.safety_interval = safety_interval
        self.telegram_supervisor = telegram_supervisor
        self.command_processor = command_processor
        self.planner_dry_run = planner_dry_run or dry_run
        self.planner_active_slots = int(planner_active_slots)
        self.planner_slow_active_demote_sec = int(planner_slow_active_demote_sec)
        self.scheduler_engine_mode = str(scheduler_engine_mode or "legacy").strip().lower()
        if self.scheduler_engine_mode not in {"legacy", "shadow", "live"}:
            raise ValueError("scheduler_engine_mode must be legacy, shadow, or live")
        self.scheduler_engine = scheduler_engine or SchedulerEngine(unit_bytes=int(scheduler_unit_bytes))
        self.disk_floor_bytes = int(disk_floor_bytes)
        self.emergency_floor_bytes = int(emergency_floor_bytes)
        self.recovery_enabled = bool(recovery_enabled)
        self.recovery_enter_bytes = int(recovery_enter_bytes if recovery_enter_bytes is not None else self.disk_floor_bytes)
        self.drain_exit_bytes = int(
            drain_exit_bytes
            if drain_exit_bytes is not None
            else max(self.disk_floor_bytes, self.recovery_enter_bytes) + 512 * 1024**2
        )
        self.explore_enter_bytes = int(explore_enter_bytes)
        self.recovery_margin_bytes = int(recovery_margin_bytes)
        self.recovery_active_slots = int(recovery_active_slots)
        self.recovery_max_remaining_bytes = int(recovery_max_remaining_bytes)
        self.upload_runner = upload_runner
        self.upload_dry_run = upload_dry_run or dry_run
        self.cleanup_runner = cleanup_runner
        self.cleanup_dry_run = cleanup_dry_run or dry_run
        self.file_batch_dry_run = file_batch_dry_run or dry_run
        self.upload_backpressure_policy = upload_backpressure_policy
        self.host_downloads = host_downloads
        self.container_downloads = container_downloads
        self.rclone_remote = rclone_remote
        self.media_pipeline_runner = media_pipeline_runner
        self.media_pipeline_dry_run = media_pipeline_dry_run or dry_run
        self.emby_refresh_worker = emby_refresh_worker
        self.emby_refresh_dry_run = emby_refresh_dry_run or dry_run
        self.telegram_notification_sender = telegram_notification_sender
        self.notification_dry_run = notification_dry_run or dry_run
        self.maintenance_service = maintenance_service or SQLiteMaintenanceService(self.state_db)
        self.orphan_janitor = orphan_janitor
        self.junk_janitor = junk_janitor
        self.observe_promotion_service = observe_promotion_service
        self.path_reconciler = path_reconciler
        self.preemption_service = preemption_service
        self.soak_dry_run = soak_dry_run or dry_run
        if soak_queue_service is not None:
            self.soak_queue_service = soak_queue_service
        elif soak_enabled:
            self.soak_queue_service = SoakQueueService(
                self.state_db,
                self.executor,
                dry_run=self.soak_dry_run,
                config=soak_config or SoakQueueConfig(),
            )
        else:
            self.soak_queue_service = None
        self.batch_pipeline_enabled = bool(batch_pipeline_enabled)
        self.batch_live_verify = bool(batch_live_verify)
        self.batch_allow_hashes = {str(item).strip().lower() for item in (batch_allow_hashes or set()) if str(item).strip()}
        self.batch_allow_tag = str(batch_allow_tag or "").strip()
        self.batch_max_live_batch_bytes = int(batch_max_live_batch_bytes or 0)
        self.batch_max_new_per_tick = int(batch_max_new_per_tick)
        self.background_event_workers = bool(background_event_workers)
        self.event_worker_interval = max(0.01, float(event_worker_interval))
        self.event_worker_join_timeout = max(0.0, float(event_worker_join_timeout))
        self._event_worker_stop = threading.Event()
        self._event_worker_threads: list[threading.Thread] = []
        self.background_periodic_workers = bool(background_periodic_workers)
        self.periodic_worker_join_timeout = max(0.0, float(periodic_worker_join_timeout))
        self._periodic_workers: list[PeriodicWorker] = []
        self.junk_file_refresh_limit = int(junk_file_refresh_limit)
        self.carousel_dry_run = carousel_dry_run or dry_run
        if carousel_service is not None:
            self.carousel_service = carousel_service
        elif carousel_enabled:
            self.carousel_service = CarouselService(self.state_db, self.executor, dry_run=self.carousel_dry_run)
        else:
            self.carousel_service = None
        self.loop_tasks = loop_tasks if loop_tasks is not None else self._default_loop_tasks()
        self.monotonic = monotonic
        self.sleeper = sleeper
        self.monitor = SafetyMonitor(
            qbt,
            executor,
            free_bytes_provider,
            managed_count_provider=managed_count_provider,
            emergency_floor_bytes=self.emergency_floor_bytes,
            sync_repeated_full_limit=sync_repeated_full_limit,
            sync_degraded_interval_sec=sync_degraded_interval_sec,
            monotonic=monotonic,
        )
        self.obs = ObservabilityStore(self.state_db)
        self.mode_controller = ModeController(
            emergency_enter=self.emergency_floor_bytes,
            drain_enter=self.recovery_enter_bytes,
            drain_exit=self.drain_exit_bytes,
            explore_enter=self.explore_enter_bytes,
        )
        self.capacity_state_store = CapacityStateStore(self.state_db)
        self._mode_lock = threading.Lock()
        self._scheduler_mode = self.capacity_state_store.current_mode("normal")
        self.capacity_deadlock_alerts_enabled = bool(capacity_deadlock_alerts_enabled)
        self._sync_session_degraded_reported = False
        if scheduler_alert_service is not None:
            self.scheduler_alert_service = scheduler_alert_service
        else:
            chat_ids = [str(x).strip() for x in (scheduler_alert_chat_ids or []) if str(x).strip()]
            self.scheduler_alert_service = SchedulerAlertService(
                BotNotificationRepository(self.state_db),
                SchedulerAlertConfig(
                    enabled=bool(scheduler_alerts_enabled and chat_ids),
                    chat_ids=chat_ids,
                    interval_sec=int(scheduler_alert_interval_sec),
                    disk_alert_margin_bytes=int(disk_alert_margin_bytes),
                    capacity_deadlock_enabled=self.capacity_deadlock_alerts_enabled,
                ),
            )
        self._stopping = False

    def stop(self, *_args) -> None:
        self._stopping = True

    def install_signal_handlers(self) -> None:
        signal.signal(signal.SIGTERM, self.stop)
        signal.signal(signal.SIGINT, self.stop)

    def _default_loop_tasks(self) -> list[LoopTask]:
        return [
            LoopTask("planner", 15, self.planner_tick, max_runtime_sec=2),
            LoopTask("file_batch", 60, self.file_batch_tick, max_runtime_sec=5),
            LoopTask("maintenance", 300, self.maintenance_tick, max_runtime_sec=5),
            LoopTask("carousel", 1800, self.carousel_tick, max_runtime_sec=2),
        ]

    def maintenance_tick(self) -> dict:
        result = self.maintenance_service.run_once()
        snapshots = {h: vars(snapshot) for h, snapshot in self.monitor.sync.snapshots.items()}
        if self.path_reconciler is not None:
            result["path_reconcile"] = self.path_reconciler.reconcile(snapshots)
        if self.orphan_janitor is not None:
            result["orphan_janitor"] = self.orphan_janitor.reconcile(
                snapshots,
                sync_healthy=bool(self.monitor.sync.high_risk_actions_allowed),
            )
        return result

    def planner_tick(self) -> dict:
        snapshots = {h: vars(snapshot) for h, snapshot in self.monitor.sync.snapshots.items()}
        free_bytes = int(self.free_bytes_provider())
        scheduler_mode = self._next_scheduler_mode(free_bytes)
        sync_healthy = bool(self.monitor.sync.high_risk_actions_allowed)
        if self.soak_queue_service is not None:
            soak_result = self.soak_queue_service.run_once(
                snapshots,
                free_bytes=free_bytes,
                sync_healthy=sync_healthy,
            )
        else:
            soak_result = SoakQueueResult(dry_run=self.dry_run)
        batch_protected_hashes = active_pipeline_batch_hashes(self.state_db)
        engine_plan = None
        engine_budget = None
        if self.scheduler_engine_mode != "legacy":
            engine_items = build_full_finish_work_items(snapshots)
            engine_budget = self._scheduler_growth_budget(
                free_bytes,
                reallocatable_hashes={item.hash for item in engine_items if not item.hold},
            )
            engine_slots = self.recovery_active_slots if scheduler_mode == "drain" else self.planner_active_slots
            if scheduler_mode == "emergency" or not sync_healthy:
                engine_slots = 0
            engine_plan = self.scheduler_engine.select(
                engine_items,
                scheduler_mode,
                engine_budget.available_growth_bytes,
                engine_slots,
            )
        allowed_active_hashes = (
            {item.hash for item in engine_plan.selected}
            if self.scheduler_engine_mode == "live" and engine_plan is not None
            else None
        )
        planner = DownloadPlanner(
            self.state_db,
            self.executor,
            dry_run=self.planner_dry_run,
            active_slots=self.planner_active_slots,
            slow_active_demote_sec=self.planner_slow_active_demote_sec,
            disk_floor_bytes=self.disk_floor_bytes,
            recovery_enabled=self.recovery_enabled,
            recovery_enter_bytes=self.recovery_enter_bytes,
            emergency_floor_bytes=self.emergency_floor_bytes,
            recovery_margin_bytes=self.recovery_margin_bytes,
            recovery_active_slots=self.recovery_active_slots,
            recovery_max_remaining_bytes=self.recovery_max_remaining_bytes,
        )
        result = planner.plan_and_apply(
            snapshots,
            free_bytes=free_bytes,
            sync_healthy=sync_healthy,
            protected_running_hashes=soak_result.protected_hashes | batch_protected_hashes,
            forced_active_hashes=set(soak_result.hot_hashes),
            cooldown_hashes=set(soak_result.cooldown_hashes),
            external_reserved_bytes=int(soak_result.reserved_bytes),
            allowed_active_hashes=allowed_active_hashes,
        )
        scheduler_payload = self._scheduler_engine_payload(
            engine_plan,
            engine_budget,
            legacy_selected_hashes=result.selected_hashes,
            legacy_budget_bytes=result.budget_bytes,
        )
        preemption_result = None
        if self.preemption_service is not None and sync_healthy:
            preemption_result = self.preemption_service.evaluate_and_apply(
                snapshots,
                disk_state=classify_disk(free_bytes, emergency_free_bytes=self.emergency_floor_bytes).state.value,
                trigger_reason="planner_pressure",
                selected_hashes=set(result.selected_hashes),
            )
        capacity_observation = build_capacity_observation(
            snapshots,
            available_growth_bytes=int(result.budget_bytes),
            selected_hashes={str(item) for item in result.selected_hashes},
            disk_releasing_jobs=self._disk_releasing_job_count(),
            free_bytes=free_bytes,
        )
        capacity_details = capacity_observation.as_details()
        capacity_result = detect_capacity_state(
            mode=scheduler_mode,
            managed_incomplete=capacity_observation.managed_incomplete,
            feasible_full_finish=capacity_observation.feasible_full_finish,
            disk_releasing_jobs=capacity_observation.disk_releasing_jobs,
        )
        capacity_transition = self.capacity_state_store.persist(
            scheduler_mode,
            capacity_result,
            capacity_details,
        )
        alert_ids = self.scheduler_alert_service.evaluate_and_enqueue(
            snapshots=snapshots,
            free_bytes=free_bytes,
            disk_floor_bytes=self.disk_floor_bytes,
            recovery_enter_bytes=self.recovery_enter_bytes,
            emergency_floor_bytes=self.emergency_floor_bytes,
            planner_result=result,
            sync_healthy=sync_healthy,
        )
        if hasattr(self.scheduler_alert_service, "enqueue_capacity_deadlock"):
            alert_ids.extend(
                self.scheduler_alert_service.enqueue_capacity_deadlock(
                    capacity_transition,
                    required_minimum_growth_bytes=capacity_observation.required_minimum_growth_bytes,
                    top_manual_candidates=list(capacity_observation.top_manual_candidates),
                )
            )
        capacity_payload = {
            "scheduler_mode": capacity_transition.scheduler_mode,
            "state": capacity_transition.state,
            "reason": capacity_transition.reason,
            "entered_at": capacity_transition.entered_at,
            "last_evaluated_at": capacity_transition.last_evaluated_at,
            "transitioned": capacity_transition.transitioned,
            "previous_state": capacity_transition.previous_state,
            "details": capacity_transition.details,
            "actions": list(capacity_result.actions),
        }
        return {
            "selected": result.selected_hashes,
            "paused": result.paused_hashes,
            "conservative": result.conservative,
            "budget_bytes": result.budget_bytes,
            "mode": result.mode,
            "scheduler_mode": scheduler_mode,
            "planner_dry_run": self.planner_dry_run,
            "planner": {
                "selected_hashes": result.selected_hashes,
                "paused_hashes": result.paused_hashes,
                "conservative": result.conservative,
                "budget_bytes": result.budget_bytes,
                "mode": result.mode,
                "planner_dry_run": self.planner_dry_run,
            },
            "soak_queue": soak_result.as_dict(),
            "preemption": None if preemption_result is None else getattr(preemption_result, "__dict__", preemption_result),
            "scheduler_engine": scheduler_payload,
            "capacity": capacity_payload,
            "alerts_enqueued": alert_ids,
        }

    def file_batch_tick(self) -> dict:
        snapshots = {h: vars(snapshot) for h, snapshot in self.monitor.sync.snapshots.items()}
        free_bytes = int(self.free_bytes_provider())
        scheduler_mode = self._batch_scheduler_mode(free_bytes)
        observe_result = None
        if self.observe_promotion_service is not None:
            observe_result = self.observe_promotion_service.promote_ready(
                snapshots,
                sync_healthy=bool(self.monitor.sync.high_risk_actions_allowed),
            )
            for promoted_hash in observe_result.get("promoted", []):
                torrent = snapshots.get(str(promoted_hash))
                if not torrent:
                    continue
                tags = {p.strip() for p in str(torrent.get("tags") or "").split(",") if p.strip()}
                tags.difference_update({"metadata-timeout", "observe", "precheck"})
                tags.update({"auto", "checked"})
                torrent["tags"] = ",".join(sorted(tags))
                torrent["category"] = "auto"
        service = FileBatchService(
            state_db=self.state_db,
            dry_run=self.file_batch_dry_run,
            host_downloads=self.host_downloads,
            container_downloads=self.container_downloads,
            remote=self.rclone_remote,
            backpressure_policy=self.upload_backpressure_policy,
            qbt=self.qbt,
            executor=self.executor,
            batch_pipeline_enabled=self.batch_pipeline_enabled,
            batch_live_verify=self.batch_live_verify,
            batch_allow_hashes=self.batch_allow_hashes,
            batch_allow_tag=self.batch_allow_tag,
            batch_max_live_batch_bytes=self.batch_max_live_batch_bytes,
            batch_max_new_per_tick=self.batch_max_new_per_tick,
            disk_floor_bytes=self.disk_floor_bytes,
        )
        result = service.sync_completed(
            snapshots,
            free_bytes=free_bytes,
            sync_healthy=bool(self.monitor.sync.high_risk_actions_allowed),
            scheduler_mode=scheduler_mode,
        )
        payload = {
            "scanned": result.scanned,
            "eligible": result.eligible,
            "enqueued": result.enqueued,
            "skipped_existing": result.skipped_existing,
            "file_batch_dry_run": bool(result.dry_run),
            "batches_created": result.batches_created,
            "batches_blocked": result.batches_blocked,
            "blocked_reasons": result.blocked_reasons,
        }
        if observe_result is not None:
            payload["observe_promotion"] = observe_result
        if self.junk_janitor is not None:
            payload["junk_janitor"] = self.junk_janitor.reconcile(
                snapshots,
                self._junk_file_lists(snapshots) if scheduler_mode in {"normal", "explore"} else {},
                sync_healthy=bool(self.monitor.sync.high_risk_actions_allowed),
            )
        return payload

    def _batch_scheduler_mode(self, free_bytes: int) -> str:
        """Return the shared hysteretic mode used by batch admission."""
        return self._next_scheduler_mode(free_bytes)

    def _next_scheduler_mode(self, free_bytes: int) -> str:
        with self._mode_lock:
            if not self.recovery_enabled and int(free_bytes) >= self.emergency_floor_bytes:
                self._scheduler_mode = "explore" if int(free_bytes) >= self.explore_enter_bytes else "normal"
            else:
                self._scheduler_mode = self.mode_controller.next_mode(self._scheduler_mode, int(free_bytes))
            return self._scheduler_mode

    def _disk_releasing_job_count(self) -> int:
        con = readonly_connect(self.state_db)
        try:
            rows = con.execute(
                "select job_type,payload_json from torrent_jobs "
                "where state in ('queued','running','verify_pending','retry_wait','cleanup_wait')"
            ).fetchall()
        finally:
            con.close()
        count = 0
        for row in rows:
            job_type = str(row["job_type"] or "")
            if job_type == "cleanup_full_torrent":
                count += 1
                continue
            if job_type != "upload":
                continue
            try:
                payload = json.loads(row["payload_json"] or "{}")
            except (TypeError, ValueError, json.JSONDecodeError):
                payload = {}
            if bool(payload.get("full_torrent")):
                count += 1
        return count

    def _scheduler_growth_budget(
        self,
        free_bytes: int,
        *,
        reallocatable_hashes: set[str] | None = None,
    ):
        con = readonly_connect(self.state_db)
        try:
            rows = con.execute(
                "select hash,kind,accounting_class,bytes from resource_reservations "
                "where state='active' and (expires_at is null or expires_at>?)",
                (int(time.time()),),
            ).fetchall()
        finally:
            con.close()
        # Only active-download claims represented by a non-hold WorkItem are
        # replaced this tick. Every other future claim remains external.
        reallocatable = {str(item) for item in (reallocatable_hashes or set())}
        external_claims = [
            claim
            for claim in resource_claims_from_rows(rows)
            if not (str(claim.kind) == "active_download" and str(claim.hash) in reallocatable)
        ]
        return calculate_growth_budget(
            free_bytes=int(free_bytes),
            emergency_floor_bytes=self.emergency_floor_bytes,
            dynamic_guard_bytes=max(0, self.disk_floor_bytes - self.emergency_floor_bytes),
            claims=external_claims,
        )

    def _scheduler_engine_payload(
        self,
        engine_plan,
        engine_budget,
        *,
        legacy_selected_hashes,
        legacy_budget_bytes: int,
    ) -> dict:
        legacy_selected = sorted(str(item) for item in legacy_selected_hashes)
        if engine_plan is None or engine_budget is None:
            return {
                "mode": "legacy",
                "applied_plan": "legacy",
                "selected_hashes": legacy_selected,
            }
        engine_selected = sorted(item.hash for item in engine_plan.selected)
        engine_set = set(engine_selected)
        legacy_set = set(legacy_selected)
        unsafe_rejections = sum(
            int(engine_plan.rejection_counts.get(reason) or 0)
            for reason in ("hold", "mode_disallowed", "budget_exceeded")
        )
        payload = {
            "mode": self.scheduler_engine_mode,
            "applied_plan": "engine" if self.scheduler_engine_mode == "live" else "legacy",
            "selected_hashes": engine_selected,
            "engine_selected_hashes": engine_selected,
            "legacy_selected_hashes": legacy_selected,
            "only_engine_hashes": sorted(engine_set - legacy_set),
            "only_legacy_hashes": sorted(legacy_set - engine_set),
            "engine_budget_bytes": int(engine_plan.available_growth_bytes),
            "legacy_budget_bytes": int(legacy_budget_bytes),
            "budget_difference_bytes": int(engine_plan.available_growth_bytes) - int(legacy_budget_bytes),
            "future_growth_reserved_bytes": int(engine_budget.future_growth_reserved_bytes),
            "current_pinned_bytes": int(engine_budget.current_pinned_bytes),
            "unsafe_plan_rejection_count": unsafe_rejections,
            "rejection_counts": dict(engine_plan.rejection_counts),
        }
        # Persist one comparison sample; only shadow guarantees the Planner side
        # is an unconstrained legacy counterfactual.
        self.obs.metric_snapshot(f"scheduler_engine_{self.scheduler_engine_mode}", payload)
        return payload

    def _effective_config_snapshot(self) -> dict:
        return {
            "thresholds": {
                "emergency_enter_bytes": self.emergency_floor_bytes,
                "drain_enter_bytes": self.recovery_enter_bytes,
                "drain_exit_bytes": self.drain_exit_bytes,
                "explore_enter_bytes": self.explore_enter_bytes,
            },
            "limits": {
                "planner_active_slots": self.planner_active_slots,
                "recovery_active_slots": self.recovery_active_slots,
                "recovery_max_remaining_bytes": self.recovery_max_remaining_bytes,
            },
            "feature_flags": {
                "dry_run": bool(self.dry_run),
                "planner_dry_run": bool(self.planner_dry_run),
                "scheduler_engine": self.scheduler_engine_mode,
                "file_batch_dry_run": bool(self.file_batch_dry_run),
                "recovery_enabled": bool(self.recovery_enabled),
                "soak_queue": self.soak_queue_service is not None,
                "batch_pipeline": bool(self.batch_pipeline_enabled),
                "batch_live_verify": bool(self.batch_live_verify),
                "background_event_workers": bool(self.background_event_workers),
                "background_periodic_workers": bool(self.background_periodic_workers),
                "scheduler_alerts": bool(self.scheduler_alert_service.config.enabled)
                if hasattr(self.scheduler_alert_service, "config")
                else False,
                "capacity_deadlock_alerts": bool(self.capacity_deadlock_alerts_enabled),
            },
        }

    def _junk_file_lists(self, snapshots: Mapping[str, Mapping[str, object]]) -> dict[str, list[dict]]:
        if not hasattr(self.qbt, "torrent_files"):
            return {}
        out: dict[str, list[dict]] = {}
        for h, torrent in snapshots.items():
            if len(out) >= self.junk_file_refresh_limit:
                break
            tags = {p.strip() for p in str(torrent.get("tags") or "").split(",") if p.strip()}
            managed = (str(torrent.get("category") or "") == "auto" or "auto" in tags) and "hold" not in tags
            if not managed:
                continue
            try:
                out[h] = list(self.qbt.torrent_files(h))
            except Exception as exc:
                self.obs.event("error", "junk_janitor", "file_list_failed", str(redact(str(exc))), {"hash": h, "dry_run": self.dry_run}, hash=h)
        return out

    def carousel_tick(self) -> dict:
        if self.carousel_service is None:
            return {"status": "disabled"}
        snapshots = {h: vars(snapshot) for h, snapshot in self.monitor.sync.snapshots.items()}
        return self.carousel_service.run_once(
            snapshots,
            sync_healthy=bool(self.monitor.sync.high_risk_actions_allowed),
            free_bytes=int(self.free_bytes_provider()),
        )

    def tick_safety(self) -> None:
        result = self.monitor.tick()
        free_bytes = int(self.free_bytes_provider())
        self._persist_disk_state(free_bytes, result.disk_state)
        sync_stats = self.monitor.sync.session_stats.as_dict()
        if self.monitor.sync.session_stats.degraded and not self._sync_session_degraded_reported:
            self.obs.event(
                "warning",
                "qbt",
                "sync_session_degraded",
                "qBT sync repeatedly returned full snapshots; degraded polling enabled",
                sync_stats,
            )
            self._sync_session_degraded_reported = True
        elif not self.monitor.sync.session_stats.degraded and self._sync_session_degraded_reported:
            self.obs.event(
                "info",
                "qbt",
                "sync_session_recovered",
                "qBT sync resumed delta snapshots",
                sync_stats,
            )
            self._sync_session_degraded_reported = False
        self.obs.event(
            "info",
            "daemon",
            "safety_tick",
            f"disk={result.disk_state} sync={result.sync_health}",
            {
                "free_bytes": free_bytes,
                "sync_health": result.sync_health,
                "sync_skipped": result.sync_skipped,
                "sync_session": sync_stats,
                "dry_run": self.dry_run,
            },
        )

    def process_bot_commands(self, max_commands: int = 20) -> int:
        if self.command_processor is None:
            return 0
        processed = 0
        for _ in range(max_commands):
            command_id = self.command_processor.run_next()
            if command_id is None:
                break
            processed += 1
        if processed:
            self.obs.event("info", "telegram", "commands_processed", f"processed={processed}", {"count": processed})
        return processed

    def process_bot_notifications(self, max_notifications: int = 5) -> int:
        if self.telegram_notification_sender is None:
            return 0
        if self.notification_dry_run:
            if not self.telegram_notification_sender.has_pending():
                return 0
            self.obs.action(
                hash=None,
                job_id=None,
                action_type="telegram_notify",
                path="bot_notifications",
                payload={"state": "queued"},
                status="dry_run",
                dry_run=True,
            )
            self.obs.event("info", "telegram", "notification_dry_run", "telegram notification pending", {"dry_run": True})
            return 1
        processed = 0
        for _ in range(max_notifications):
            if not self.telegram_notification_sender.has_pending():
                break
            notification_id = self.telegram_notification_sender.send_next()
            if notification_id is None:
                break
            processed += 1
            self.obs.event("info", "telegram", "notification_processed", f"notification {notification_id} processed", {"notification_id": notification_id})
        return processed

    def process_upload_jobs(self, max_jobs: int = 1) -> int:
        if self.upload_runner is None:
            return 0
        processed = 0
        if self.upload_dry_run:
            row = self.upload_runner.repo.peek_next("upload")
            if not row:
                return 0
            self.obs.action(
                hash=row.get("hash"),
                job_id=int(row["id"]),
                action_type="upload_job",
                path="torrent_jobs/upload",
                payload={"job_id": row["id"], "state": row.get("state"), "job_type": row.get("job_type")},
                status="dry_run",
                dry_run=True,
            )
            self.obs.event("info", "upload", "upload_dry_run", f"upload job {row['id']} pending", {"job_id": row["id"]}, hash=row.get("hash"), job_id=int(row["id"]))
            return 1
        for _ in range(max_jobs):
            job_id = self.upload_runner.run_next()
            if job_id is None:
                break
            processed += 1
            self.obs.event("info", "upload", "upload_job_processed", f"upload job {job_id} processed", {"job_id": job_id}, job_id=int(job_id))
        return processed

    def process_cleanup_requests(self, max_jobs: int = 1) -> int:
        if self.cleanup_runner is None:
            return 0
        processed = 0
        if self.cleanup_dry_run:
            row = self.cleanup_runner.repo.peek_next("cleanup_request")
            if not row:
                return 0
            self.obs.action(
                hash=row.get("hash"),
                job_id=int(row["id"]),
                action_type="cleanup_request",
                path="torrent_jobs/cleanup_request",
                payload={"job_id": row["id"], "state": row.get("state"), "job_type": row.get("job_type")},
                status="dry_run",
                dry_run=True,
            )
            self.obs.event("info", "cleanup", "cleanup_request_dry_run", f"cleanup request {row['id']} pending", {"job_id": row["id"]}, hash=row.get("hash"), job_id=int(row["id"]))
            return 1
        for _ in range(max_jobs):
            job_id = self.cleanup_runner.run_next()
            if job_id is None:
                break
            processed += 1
            self.obs.event("info", "cleanup", "cleanup_request_processed", f"cleanup request {job_id} processed", {"job_id": job_id}, job_id=int(job_id))
        return processed

    def process_media_pipeline_jobs(self, max_jobs: int = 1) -> int:
        if self.media_pipeline_runner is None:
            return 0
        processed = 0
        if self.media_pipeline_dry_run:
            row = self.media_pipeline_runner.repo.peek_next("media_pipeline")
            if not row:
                return 0
            self.obs.action(
                hash=row.get("hash"),
                job_id=int(row["id"]),
                action_type="media_pipeline_job",
                path="torrent_jobs/media_pipeline",
                payload={"job_id": row["id"], "state": row.get("state"), "job_type": row.get("job_type")},
                status="dry_run",
                dry_run=True,
            )
            self.obs.event("info", "media_pipeline", "media_pipeline_dry_run", f"media pipeline job {row['id']} pending", {"job_id": row["id"]}, hash=row.get("hash"), job_id=int(row["id"]))
            return 1
        for _ in range(max_jobs):
            job_id = self.media_pipeline_runner.run_next()
            if job_id is None:
                break
            processed += 1
            self.obs.event("info", "media_pipeline", "media_pipeline_job_processed", f"media pipeline job {job_id} processed", {"job_id": job_id}, job_id=int(job_id))
        return processed

    def process_emby_refresh_tasks(self, max_tasks: int = 1) -> int:
        if self.emby_refresh_worker is None:
            return 0
        processed = 0
        if self.emby_refresh_dry_run:
            row = self.emby_refresh_worker.peek_next()
            if not row:
                return 0
            self.obs.action(
                hash=None,
                job_id=None,
                action_type="emby_refresh",
                path=str(row.get("emby_media_dir") or ""),
                payload={"task_id": row["id"], "state": row.get("state")},
                status="dry_run",
                dry_run=True,
            )
            self.obs.event("info", "emby", "emby_refresh_dry_run", f"emby refresh task {row['id']} pending", {"task_id": row["id"], "path": row.get("emby_media_dir")})
            return 1
        for _ in range(max_tasks):
            task_id = self.emby_refresh_worker.run_next()
            if task_id is None:
                break
            processed += 1
            self.obs.event("info", "emby", "emby_refresh_processed", f"emby refresh task {task_id} processed", {"task_id": task_id})
        return processed

    def _background_event_worker_specs(self) -> list[tuple[str, Callable[[], int]]]:
        return [
            ("telegram", self.process_bot_notifications),
            ("upload", self.process_upload_jobs),
            ("cleanup", self.process_cleanup_requests),
            ("media_pipeline", self.process_media_pipeline_jobs),
            ("emby", self.process_emby_refresh_tasks),
        ]

    def _start_background_event_workers(self) -> None:
        if not self.background_event_workers:
            return
        if any(thread.is_alive() for thread in self._event_worker_threads):
            return
        self._event_worker_stop.clear()
        self._event_worker_threads = []
        worker_names: list[str] = []
        for name, callback in self._background_event_worker_specs():
            thread = threading.Thread(
                target=self._run_background_event_worker,
                name=f"qbt-event-{name}",
                args=(name, callback),
                daemon=True,
            )
            thread.start()
            self._event_worker_threads.append(thread)
            worker_names.append(name)
        self.obs.event("info", "daemon", "event_workers_started", "background event workers started", {"workers": worker_names})

    def _stop_background_event_workers(self) -> None:
        if not self._event_worker_threads:
            return
        self._event_worker_stop.set()
        for thread in list(self._event_worker_threads):
            thread.join(timeout=self.event_worker_join_timeout)
        alive = [thread.name for thread in self._event_worker_threads if thread.is_alive()]
        self.obs.event("info", "daemon", "event_workers_stopped", "background event workers stop requested", {"alive": alive})
        self._event_worker_threads = [thread for thread in self._event_worker_threads if thread.is_alive()]

    def _run_background_event_worker(self, name: str, callback: Callable[[], int]) -> None:
        while not self._event_worker_stop.is_set():
            try:
                callback()
            except Exception as exc:
                self.obs.event(
                    "error",
                    name,
                    "event_worker_failed",
                    str(redact(str(exc))),
                    {"background_event_workers": True, "dry_run": self.dry_run},
                )
            self._event_worker_stop.wait(self.event_worker_interval)

    def _start_periodic_workers(self) -> None:
        if not self.background_periodic_workers:
            return
        if any(worker.is_alive() for worker in self._periodic_workers):
            return
        self._periodic_workers = []
        for loop_task in self.loop_tasks:
            task = PeriodicTask(
                loop_task.name,
                loop_task.interval_sec,
                lambda current=loop_task: self._execute_loop_task(current),
            )
            worker = PeriodicWorker(task, monotonic=self.monotonic, on_error=self._periodic_worker_error)
            self._periodic_workers.append(worker)
            worker.start()
        self.obs.event(
            "info",
            "daemon",
            "periodic_workers_started",
            "background periodic workers started",
            {"workers": [worker.task.name for worker in self._periodic_workers]},
        )

    def _stop_periodic_workers(self) -> list[str]:
        if not self._periodic_workers:
            return []
        for worker in self._periodic_workers:
            worker.stop()
        for worker in self._periodic_workers:
            worker.join(timeout=self.periodic_worker_join_timeout)
        alive = [worker.task.name for worker in self._periodic_workers if worker.is_alive()]
        self.obs.event(
            "info",
            "daemon",
            "periodic_workers_stopped",
            "background periodic workers stop requested",
            {"alive": alive},
        )
        self._periodic_workers = [worker for worker in self._periodic_workers if worker.is_alive()]
        return alive

    def _periodic_worker_error(self, name: str, exc: Exception) -> None:
        self.obs.event(
            "error",
            name,
            "periodic_worker_failed",
            str(redact(str(exc))),
            {"background_periodic_workers": True, "dry_run": self.dry_run},
        )

    def run_due_loop_tasks(self) -> int:
        ran = 0
        now_monotonic = self.monotonic()
        for task in self.loop_tasks:
            if not task.due(now_monotonic):
                continue
            self._execute_loop_task(task)
            task.mark_ran(now_monotonic)
            ran += 1
        return ran

    def _execute_loop_task(self, task: LoopTask) -> None:
        started = self.monotonic()
        result = None
        callback_error = None
        try:
            result = task.callback()
        except Exception as exc:
            callback_error = exc
        duration = max(0.0, self.monotonic() - started)
        try:
            if callback_error is None:
                self.obs.event("info", task.name, "loop_tick", f"{task.name} loop completed", {"result": result, "dry_run": self.dry_run})
            else:
                self.obs.event("error", task.name, "loop_failed", str(redact(str(callback_error))), {"dry_run": self.dry_run})
        finally:
            self._loop_metric(task, duration, succeeded=callback_error is None)

    def _loop_metric(self, task: LoopTask, duration: float, *, succeeded: bool) -> None:
        self.obs.rolling_timing_metric(
            f"loop_runtime:{task.name}",
            duration_ms=int(duration * 1000),
            max_runtime_ms=int(task.max_runtime_sec * 1000),
            succeeded=succeeded,
        )

    def _persist_disk_state(self, free_bytes: int, state: str) -> None:
        now = int(time.time())
        def txn(con: sqlite3.Connection) -> None:
            prev = con.execute("select pressure_state, state_since from disk_state where id=1").fetchone()
            previous_state = prev[0] if prev else None
            state_since = prev[1] if prev and prev[0] == state else now
            con.execute(
                "insert into disk_state(id,sampled_at,free_bytes,pressure_state,previous_state,state_since,resume_allowed) "
                "values(1,?,?,?,?,?,?) "
                "on conflict(id) do update set sampled_at=excluded.sampled_at, free_bytes=excluded.free_bytes, "
                "pressure_state=excluded.pressure_state, previous_state=excluded.previous_state, "
                "state_since=excluded.state_since, resume_allowed=excluded.resume_allowed",
                (now, free_bytes, state, previous_state, state_since, 0 if state == "emergency" else 1),
            )

        write_transaction(self.state_db, txn)

    def run(self, max_safety_ticks: int | None = None) -> int:
        start_persistent_write_actor(self.state_db)
        ticks = 0
        try:
            self.obs.event(
                "info",
                "daemon",
                "effective_config",
                "resolved scheduler thresholds and feature flags",
                self._effective_config_snapshot(),
            )
            self.obs.event("info", "daemon", "started", "qbt orchestrator daemon started", {"dry_run": self.dry_run})
            if self.telegram_supervisor is not None:
                self.telegram_supervisor.start()
            self._start_background_event_workers()
            self._start_periodic_workers()
            while not self._stopping:
                started = self.monotonic()
                try:
                    self.tick_safety()
                except Exception as exc:  # keep safety process supervised and observable
                    self.obs.event("error", "daemon", "safety_tick_failed", str(redact(str(exc))), {"dry_run": self.dry_run})
                if not self.background_periodic_workers:
                    self.run_due_loop_tasks()
                try:
                    self.process_bot_commands()
                except Exception as exc:
                    self.obs.event("error", "telegram", "command_processing_failed", str(redact(str(exc))), {"dry_run": self.dry_run})
                if not self.background_event_workers:
                    try:
                        self.process_bot_notifications()
                    except Exception as exc:
                        self.obs.event("error", "telegram", "notification_processing_failed", str(redact(str(exc))), {"dry_run": self.dry_run})
                    try:
                        self.process_upload_jobs()
                    except Exception as exc:
                        self.obs.event("error", "upload", "upload_processing_failed", str(redact(str(exc))), {"dry_run": self.dry_run})
                    try:
                        self.process_cleanup_requests()
                    except Exception as exc:
                        self.obs.event("error", "cleanup", "cleanup_processing_failed", str(redact(str(exc))), {"dry_run": self.dry_run})
                    try:
                        self.process_media_pipeline_jobs()
                    except Exception as exc:
                        self.obs.event("error", "media_pipeline", "media_pipeline_processing_failed", str(redact(str(exc))), {"dry_run": self.dry_run})
                    try:
                        self.process_emby_refresh_tasks()
                    except Exception as exc:
                        self.obs.event("error", "emby", "emby_refresh_processing_failed", str(redact(str(exc))), {"dry_run": self.dry_run})
                ticks += 1
                if max_safety_ticks is not None and ticks >= max_safety_ticks:
                    break
                sleep_for = self.safety_interval - (self.monotonic() - started)
                if sleep_for > 0:
                    self.sleeper(sleep_for)
        finally:
            self._stop_periodic_workers()
            self._stop_background_event_workers()
            if self.telegram_supervisor is not None:
                self.telegram_supervisor.stop()
            try:
                self.obs.event("info", "daemon", "stopped", "qbt orchestrator daemon stopped", {"ticks": ticks})
            finally:
                stop_write_actor(self.state_db)
        return ticks
