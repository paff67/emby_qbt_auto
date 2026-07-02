from __future__ import annotations

from dataclasses import dataclass
import signal
import sqlite3
import threading
import time
from pathlib import Path
from typing import Callable, Mapping

from .daemon import SafetyMonitor
from .db import migrate
from .integrations.telegram import TelegramHttpApi, TelegramPollingService
from .observability import redact
from .runtime import BotCommandRepository, ObservabilityStore
from .telegram_control import TelegramAuthorizer


@dataclass
class LoopTask:
    name: str
    interval_sec: float
    callback: Callable[[], object]
    next_due: float = 0.0

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
        self.loop_tasks = loop_tasks if loop_tasks is not None else self._default_loop_tasks()
        self.monotonic = monotonic
        self.sleeper = sleeper
        self.monitor = SafetyMonitor(qbt, executor, free_bytes_provider, managed_count_provider=managed_count_provider)
        self.obs = ObservabilityStore(self.state_db)
        self._stopping = False

    def stop(self, *_args) -> None:
        self._stopping = True

    def install_signal_handlers(self) -> None:
        signal.signal(signal.SIGTERM, self.stop)
        signal.signal(signal.SIGINT, self.stop)

    def _default_loop_tasks(self) -> list[LoopTask]:
        return [
            LoopTask("planner", 15, lambda: {"status": "not_configured"}),
            LoopTask("file_batch", 60, lambda: {"status": "not_configured"}),
            LoopTask("maintenance", 300, lambda: {"status": "not_configured"}),
            LoopTask("carousel", 1800, lambda: {"status": "not_configured"}),
        ]

    def tick_safety(self) -> None:
        result = self.monitor.tick()
        free_bytes = int(self.free_bytes_provider())
        self._persist_disk_state(free_bytes, result.disk_state)
        self.obs.event(
            "info",
            "daemon",
            "safety_tick",
            f"disk={result.disk_state} sync={result.sync_health}",
            {"free_bytes": free_bytes, "sync_health": result.sync_health, "dry_run": self.dry_run},
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

    def run_due_loop_tasks(self) -> int:
        ran = 0
        now_monotonic = self.monotonic()
        for task in self.loop_tasks:
            if not task.due(now_monotonic):
                continue
            try:
                result = task.callback()
                self.obs.event("info", task.name, "loop_tick", f"{task.name} loop completed", {"result": result, "dry_run": self.dry_run})
            except Exception as exc:
                self.obs.event("error", task.name, "loop_failed", str(redact(str(exc))), {"dry_run": self.dry_run})
            finally:
                task.mark_ran(now_monotonic)
                ran += 1
        return ran

    def _persist_disk_state(self, free_bytes: int, state: str) -> None:
        now = int(time.time())
        con = sqlite3.connect(self.state_db)
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
        con.commit()
        con.close()

    def run(self, max_safety_ticks: int | None = None) -> int:
        self.obs.event("info", "daemon", "started", "qbt orchestrator daemon started", {"dry_run": self.dry_run})
        if self.telegram_supervisor is not None:
            self.telegram_supervisor.start()
        ticks = 0
        try:
            while not self._stopping:
                started = self.monotonic()
                try:
                    self.tick_safety()
                except Exception as exc:  # keep safety process supervised and observable
                    self.obs.event("error", "daemon", "safety_tick_failed", str(redact(str(exc))), {"dry_run": self.dry_run})
                self.run_due_loop_tasks()
                try:
                    self.process_bot_commands()
                except Exception as exc:
                    self.obs.event("error", "telegram", "command_processing_failed", str(redact(str(exc))), {"dry_run": self.dry_run})
                ticks += 1
                if max_safety_ticks is not None and ticks >= max_safety_ticks:
                    break
                sleep_for = self.safety_interval - (self.monotonic() - started)
                if sleep_for > 0:
                    self.sleeper(sleep_for)
        finally:
            if self.telegram_supervisor is not None:
                self.telegram_supervisor.stop()
            self.obs.event("info", "daemon", "stopped", "qbt orchestrator daemon stopped", {"ticks": ticks})
        return ticks
