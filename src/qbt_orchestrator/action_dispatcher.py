from __future__ import annotations

import copy
import itertools
import queue
import threading
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any, Callable


class ActionPriority(IntEnum):
    EMERGENCY = 0
    CONTROL = 10
    MAINTENANCE = 20


class ActionFuture:
    def __init__(self) -> None:
        self._done = threading.Event()
        self._value: Any = None
        self._error: BaseException | None = None

    def set_result(self, value: Any) -> None:
        self._value = value
        self._done.set()

    def set_exception(self, error: BaseException) -> None:
        self._error = error
        self._done.set()

    def result(self, timeout: float | None = None) -> Any:
        if not self._done.wait(timeout=timeout):
            raise TimeoutError("qBT action did not finish before timeout")
        if self._error is not None:
            raise self._error
        return self._value


@dataclass(order=True)
class DispatchedAction:
    priority: int
    sequence: int
    path: str = field(compare=False)
    payload: dict[str, Any] = field(compare=False)
    future: ActionFuture = field(compare=False)
    stop: bool = field(default=False, compare=False)


class ActionDispatcher:
    """Serialize qBT writes while allowing emergencies to bypass queue backlog."""

    _STOP_PRIORITY = 1_000_000

    def __init__(self, handler: Callable[[str, dict[str, Any]], Any]):
        self.handler = handler
        self._queue: queue.PriorityQueue[DispatchedAction] = queue.PriorityQueue()
        self._sequence = itertools.count()
        self._state_lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._closed = False

    def submit(
        self,
        path: str,
        payload: dict[str, Any],
        *,
        priority: ActionPriority | int = ActionPriority.CONTROL,
        wait: bool = True,
    ) -> Any:
        with self._state_lock:
            if self._closed:
                raise RuntimeError("qBT action dispatcher is closed")
            self._start_locked()
            sequence = next(self._sequence)
        future = ActionFuture()
        self._queue.put(
            DispatchedAction(
                int(priority),
                sequence,
                str(path),
                copy.deepcopy(dict(payload)),
                future,
            )
        )
        return future.result() if wait else future

    def close(self) -> None:
        with self._state_lock:
            if self._closed:
                return
            self._closed = True
            if self._thread is None:
                return
            sequence = next(self._sequence)
        self._queue.put(
            DispatchedAction(
                self._STOP_PRIORITY,
                sequence,
                "",
                {},
                ActionFuture(),
                stop=True,
            )
        )

    def join(self, timeout: float | None = None) -> None:
        if self._thread:
            self._thread.join(timeout=timeout)

    def _start_locked(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run, name="qbt-action-dispatcher", daemon=True)
        self._thread.start()

    def _run(self) -> None:
        while True:
            action = self._queue.get()
            try:
                if action.stop:
                    return
                try:
                    action.future.set_result(self.handler(action.path, action.payload))
                except BaseException as exc:
                    action.future.set_exception(exc)
            finally:
                self._queue.task_done()
