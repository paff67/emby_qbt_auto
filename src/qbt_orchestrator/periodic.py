from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Callable


@dataclass(frozen=True)
class PeriodicTask:
    name: str
    interval_sec: float
    callback: Callable[[], object]

    def __post_init__(self) -> None:
        if float(self.interval_sec) <= 0:
            raise ValueError("periodic task interval must be positive")


class PeriodicWorker:
    """Run one fixed-rate task serially and skip periods missed while busy."""

    def __init__(
        self,
        task: PeriodicTask,
        *,
        monotonic: Callable[[], float] = time.monotonic,
        on_error: Callable[[str, Exception], None] | None = None,
    ):
        self.task = task
        self.monotonic = monotonic
        self.on_error = on_error
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self.run, name=f"qbt-periodic-{self.task.name}", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()

    def join(self, timeout: float | None = None) -> None:
        if self._thread:
            self._thread.join(timeout=timeout)

    def is_alive(self) -> bool:
        return bool(self._thread and self._thread.is_alive())

    def run(self) -> None:
        interval = float(self.task.interval_sec)
        next_due = float(self.monotonic())
        while not self._stop_event.is_set():
            now = float(self.monotonic())
            if now < next_due:
                self._stop_event.wait(next_due - now)
                continue
            try:
                self.task.callback()
            except Exception as exc:  # worker supervision must outlive a task failure
                if self.on_error is not None:
                    try:
                        self.on_error(self.task.name, exc)
                    except Exception:
                        # The error reporter may depend on the same failed DB or
                        # network.  Do not turn that secondary failure into an
                        # unhandled thread exception.
                        pass
            finished = float(self.monotonic())
            missed = max(0, int((finished - next_due) // interval))
            next_due += (missed + 1) * interval
