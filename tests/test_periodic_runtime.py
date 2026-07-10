from __future__ import annotations

import threading
import time


def test_periodic_worker_skips_missed_intervals_without_overlap():
    from qbt_orchestrator.periodic import PeriodicTask, PeriodicWorker

    first_started = threading.Event()
    release_first = threading.Event()
    lock = threading.Lock()
    calls = 0
    active = 0
    max_active = 0

    def callback():
        nonlocal calls, active, max_active
        with lock:
            calls += 1
            active += 1
            max_active = max(max_active, active)
            call_no = calls
        if call_no == 1:
            first_started.set()
            release_first.wait(timeout=1)
        with lock:
            active -= 1

    worker = PeriodicWorker(PeriodicTask("inventory", 0.01, callback))
    worker.start()
    try:
        assert first_started.wait(timeout=0.5)
        time.sleep(0.05)
        with lock:
            assert calls == 1
        release_first.set()
        time.sleep(0.035)
    finally:
        release_first.set()
        worker.stop()
        worker.join(timeout=1)

    assert max_active == 1
    assert calls <= 4


def test_action_dispatcher_prioritizes_emergency_over_queued_actions():
    from qbt_orchestrator.action_dispatcher import ActionDispatcher, ActionPriority

    calls = []
    first_started = threading.Event()
    release_first = threading.Event()

    def handler(path, payload):
        calls.append((path, payload))
        if path == "/first":
            first_started.set()
            release_first.wait(timeout=1)
        return "Ok."

    dispatcher = ActionDispatcher(handler)
    first = dispatcher.submit("/first", {"id": 1}, priority=ActionPriority.CONTROL, wait=False)
    assert first_started.wait(timeout=0.5)
    maintenance = dispatcher.submit("/maintenance", {"id": 2}, priority=ActionPriority.MAINTENANCE, wait=False)
    emergency = dispatcher.submit("/emergency", {"id": 3}, priority=ActionPriority.EMERGENCY, wait=False)
    release_first.set()

    assert first.result(timeout=1) == "Ok."
    assert emergency.result(timeout=1) == "Ok."
    assert maintenance.result(timeout=1) == "Ok."
    dispatcher.close()
    dispatcher.join(timeout=1)

    assert [path for path, _payload in calls] == ["/first", "/emergency", "/maintenance"]
