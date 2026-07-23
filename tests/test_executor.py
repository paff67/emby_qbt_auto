from __future__ import annotations

import threading
import time


class RecordingQbt:
    def __init__(self):
        self.posts = []

    def post(self, path, payload):
        self.posts.append((path, dict(payload)))
        return "Ok."


def test_hash_mutation_lease_blocks_non_owner_mutators_and_allows_owner():
    from qbt_orchestrator.executor import Executor

    qbt = RecordingQbt()
    executor = Executor(qbt, dry_run=False)
    token = "reclaim:7:4"
    try:
        assert executor.acquire_hash_mutation_lease("H", token) is True
        assert executor.qbt_post(
            "/api/v2/torrents/filePrio",
            {"hash": "h", "id": "0", "priority": "0"},
        ) is False
        assert executor.qbt_post(
            "/api/v2/torrents/start", {"hashes": "other|H"}
        ) is False
        assert executor.qbt_post(
            "/api/v2/torrents/setLocation",
            {"hashes": "h", "location": "/downloads/other"},
        ) is False
        assert executor.qbt_post(
            "/api/v2/torrents/stop",
            {"hashes": "h"},
            lease_token=token,
        ) is True
        assert executor.qbt_post(
            "/api/v2/torrents/recheck",
            {"hashes": "h"},
            lease_token=token,
        ) is True
    finally:
        executor.close(timeout=1)

    assert [path for path, _payload in qbt.posts] == [
        "/api/v2/torrents/stop",
        "/api/v2/torrents/recheck",
    ]
    assert [entry.status for entry in executor.action_log] == [
        "skipped_hash_lease",
        "skipped_hash_lease",
        "skipped_hash_lease",
        "succeeded",
        "succeeded",
    ]


def test_queued_before_lease_action_is_checked_when_dispatcher_executes_it():
    from qbt_orchestrator.executor import Executor

    first_started = threading.Event()
    release_first = threading.Event()

    class BlockingQbt(RecordingQbt):
        def post(self, path, payload):
            self.posts.append((path, dict(payload)))
            if path == "/block":
                first_started.set()
                release_first.wait(timeout=1)
            return "Ok."

    qbt = BlockingQbt()
    executor = Executor(qbt, dry_run=False)
    results = {}
    first = threading.Thread(
        target=lambda: results.setdefault(
            "first", executor.qbt_post("/block", {"hashes": "other"})
        )
    )
    queued = threading.Thread(
        target=lambda: results.setdefault(
            "queued",
            executor.qbt_post(
                "/api/v2/torrents/start", {"hashes": "h"}
            ),
        )
    )
    first.start()
    assert first_started.wait(timeout=0.5)
    queued.start()
    deadline = time.monotonic() + 0.5
    while executor.dispatcher._queue.qsize() < 1 and time.monotonic() < deadline:
        time.sleep(0.001)
    assert executor.dispatcher._queue.qsize() == 1
    assert executor.acquire_hash_mutation_lease("h", "reclaim:9:4") is True
    release_first.set()
    first.join(timeout=1)
    queued.join(timeout=1)
    executor.close(timeout=1)

    assert results == {"first": True, "queued": False}
    assert [path for path, _payload in qbt.posts] == ["/block"]
    queued_log = next(
        entry
        for entry in executor.action_log
        if entry.path == "/api/v2/torrents/start"
    )
    assert queued_log.status == "skipped_hash_lease"


def test_acquiring_hash_lease_waits_for_inflight_dispatch_to_finish():
    from qbt_orchestrator.executor import Executor

    request_started = threading.Event()
    release_request = threading.Event()
    lease_acquired = threading.Event()

    class BlockingQbt(RecordingQbt):
        def post(self, path, payload):
            request_started.set()
            release_request.wait(timeout=1)
            return super().post(path, payload)

    executor = Executor(BlockingQbt(), dry_run=False)
    action = threading.Thread(
        target=lambda: executor.qbt_post(
            "/api/v2/torrents/start", {"hashes": "h"}
        )
    )

    def acquire():
        assert executor.acquire_hash_mutation_lease(
            "h", "reclaim:10:4"
        ) is True
        lease_acquired.set()

    acquisition = threading.Thread(target=acquire)
    action.start()
    assert request_started.wait(timeout=0.5)
    acquisition.start()
    assert not lease_acquired.wait(timeout=0.03)
    release_request.set()
    action.join(timeout=1)
    acquisition.join(timeout=1)
    executor.close(timeout=1)
    assert lease_acquired.is_set()


def test_acquiring_hash_lease_does_not_wait_for_unrelated_inflight_hash():
    from qbt_orchestrator.executor import Executor

    request_started = threading.Event()
    release_request = threading.Event()
    lease_acquired = threading.Event()

    class BlockingQbt(RecordingQbt):
        def post(self, path, payload):
            request_started.set()
            release_request.wait(timeout=1)
            return super().post(path, payload)

    executor = Executor(BlockingQbt(), dry_run=False)
    action = threading.Thread(
        target=lambda: executor.qbt_post(
            "/api/v2/torrents/start", {"hashes": "other"}
        )
    )
    acquisition = threading.Thread(
        target=lambda: (
            executor.acquire_hash_mutation_lease("h", "reclaim:11:4"),
            lease_acquired.set(),
        )
    )
    action.start()
    assert request_started.wait(timeout=0.5)
    acquisition.start()
    acquired_without_waiting = lease_acquired.wait(timeout=0.03)
    release_request.set()
    action.join(timeout=1)
    acquisition.join(timeout=1)
    executor.close(timeout=1)
    assert acquired_without_waiting is True


def test_hash_mutation_lease_release_requires_owner_and_hydration_is_idempotent():
    from qbt_orchestrator.executor import Executor

    executor = Executor(RecordingQbt(), dry_run=True)
    assert executor.hydrate_hash_mutation_lease(" H ", "reclaim:2:4") is True
    assert executor.hydrate_hash_mutation_lease("h", "reclaim:2:4") is True
    assert executor.acquire_hash_mutation_lease("h", "other") is False
    assert executor.release_hash_mutation_lease("h", "other") is False
    assert executor.qbt_post(
        "/api/v2/torrents/start", {"hashes": "h"}
    ) is False
    assert executor.action_log[-1].status == "skipped_hash_lease"
    assert executor.action_log[-1].dry_run is True
    assert executor.release_hash_mutation_lease("H", "reclaim:2:4") is True
    assert executor.qbt_post(
        "/api/v2/torrents/start", {"hashes": "h"}
    ) is True
    assert executor.action_log[-1].status == "dry_run"


def test_hash_mutation_lease_blocks_all_hashes_selector_while_any_lease_exists():
    from qbt_orchestrator.executor import Executor

    qbt = RecordingQbt()
    executor = Executor(qbt, dry_run=False)
    try:
        assert executor.acquire_hash_mutation_lease("h", "reclaim:1:4") is True
        assert executor.qbt_post(
            "/api/v2/torrents/stop", {"hashes": "all"}
        ) is False
    finally:
        executor.close(timeout=1)
    assert qbt.posts == []
    assert executor.action_log[-1].status == "skipped_hash_lease"


def test_injected_dispatcher_handler_is_preserved_behind_lease_guard():
    from qbt_orchestrator.action_dispatcher import ActionDispatcher
    from qbt_orchestrator.executor import Executor

    qbt = RecordingQbt()
    dispatched = []

    def handler(path, payload):
        dispatched.append((path, dict(payload)))
        return "custom"

    dispatcher = ActionDispatcher(handler)
    executor = Executor(qbt, dry_run=False, dispatcher=dispatcher)
    try:
        assert executor.qbt_post(
            "/api/v2/torrents/start", {"hashes": "h"}
        ) is True
    finally:
        executor.close(timeout=1)

    assert dispatched == [
        ("/api/v2/torrents/start", {"hashes": "h"})
    ]
    assert qbt.posts == []
