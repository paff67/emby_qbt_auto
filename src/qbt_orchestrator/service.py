from __future__ import annotations

import signal
import sqlite3
import time
from pathlib import Path
from typing import Callable

from .daemon import SafetyMonitor
from .db import migrate
from .observability import redact
from .runtime import ObservabilityStore


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
    ):
        self.state_db = Path(state_db)
        migrate(self.state_db, dry_run=False)
        self.qbt = qbt
        self.executor = executor
        self.free_bytes_provider = free_bytes_provider
        self.dry_run = dry_run
        self.safety_interval = safety_interval
        self.monitor = SafetyMonitor(qbt, executor, free_bytes_provider, managed_count_provider=managed_count_provider)
        self.obs = ObservabilityStore(self.state_db)
        self._stopping = False

    def stop(self, *_args) -> None:
        self._stopping = True

    def install_signal_handlers(self) -> None:
        signal.signal(signal.SIGTERM, self.stop)
        signal.signal(signal.SIGINT, self.stop)

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
        ticks = 0
        while not self._stopping:
            started = time.monotonic()
            try:
                self.tick_safety()
            except Exception as exc:  # keep safety process supervised and observable
                self.obs.event("error", "daemon", "safety_tick_failed", str(redact(str(exc))), {"dry_run": self.dry_run})
            ticks += 1
            if max_safety_ticks is not None and ticks >= max_safety_ticks:
                break
            sleep_for = self.safety_interval - (time.monotonic() - started)
            if sleep_for > 0:
                time.sleep(sleep_for)
        self.obs.event("info", "daemon", "stopped", "qbt orchestrator daemon stopped", {"ticks": ticks})
        return ticks
