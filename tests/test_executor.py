from __future__ import annotations

import sqlite3
import threading
import time

import pytest


class RecordingQbt:
    def __init__(self):
        self.posts = []

    def post(self, path, payload):
        self.posts.append((path, dict(payload)))
        return "Ok."


def _seed_reclaim(db, torrent_hash, state, generation):
    con = sqlite3.connect(db)
    cur = con.execute(
        "insert into capacity_reclaims("
        "reclaim_key,hash,name,magnet_uri,host_path,content_path,state,"
        "capacity_generation,created_at,updated_at) values(?,?,?,?,?,?,?,?,?,?)",
        (
            f"{torrent_hash}:{state}:{generation}",
            torrent_hash,
            torrent_hash,
            "magnet:?xt=test",
            f"/data/{torrent_hash}",
            f"/downloads/{torrent_hash}",
            state,
            generation,
            1,
            1,
        ),
    )
    reclaim_id = int(cur.lastrowid)
    con.commit()
    con.close()
    return reclaim_id


def test_executor_startup_hydrates_all_durable_reclaim_states_before_first_mutation(tmp_path):
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.executor import Executor, QbtMutationLeaseBlocked

    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    for torrent_hash, state in (
        ("reclaimed", "reclaimed"),
        ("aborted", "aborted_paused"),
        ("quarantined", "quarantined"),
        ("released", "released"),
        ("cancelled", "cancelled"),
    ):
        _seed_reclaim(db, torrent_hash, state, 4)

    qbt = RecordingQbt()
    executor = Executor(qbt, dry_run=False, state_db=db)
    try:
        with pytest.raises(QbtMutationLeaseBlocked):
            executor.qbt_post(
                "/api/v2/torrents/start", {"hashes": " RECLAIMED "}
            )
        with pytest.raises(QbtMutationLeaseBlocked):
            executor.qbt_post(
                "/api/v2/torrents/filePrio",
                {"hash": "ABORTED", "id": "0", "priority": "1"},
            )
        with pytest.raises(QbtMutationLeaseBlocked):
            executor.qbt_post(
                "/api/v2/torrents/start", {"hashes": "Quarantined"}
            )
        assert qbt.posts == []

        assert executor.qbt_post(
            "/api/v2/torrents/start", {"hashes": "released"}
        ) is True
        assert executor.qbt_post(
            "/api/v2/torrents/start", {"hashes": "cancelled"}
        ) is True
    finally:
        executor.close(timeout=1)

    assert [entry.status for entry in executor.action_log[:3]] == [
        "skipped_hash_lease",
        "skipped_hash_lease",
        "skipped_hash_lease",
    ]
    assert [payload["hashes"] for _path, payload in qbt.posts] == [
        "released",
        "cancelled",
    ]


def test_executor_startup_duplicate_hash_uses_highest_reclaim_id_and_records_warning(tmp_path, caplog):
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.executor import Executor, QbtMutationLeaseBlocked

    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    low_id = _seed_reclaim(db, " H ", "reclaimed", 3)
    high_id = _seed_reclaim(db, "h", "quarantined", 7)

    qbt = RecordingQbt()
    executor = Executor(qbt, dry_run=False, state_db=db)
    try:
        with pytest.raises(QbtMutationLeaseBlocked):
            executor.qbt_post(
                "/api/v2/torrents/start",
                {"hashes": " H "},
                lease_token=f"reclaim:{low_id}:3",
            )
        assert executor.qbt_post(
            "/api/v2/torrents/start",
            {"hashes": "h"},
            lease_token=f"reclaim:{high_id}:7",
        ) is True
    finally:
        executor.close(timeout=1)

    assert qbt.posts == [("/api/v2/torrents/start", {"hashes": "h"})]
    assert executor.startup_reclaim_lease_warnings
    assert f"keeping id={high_id}" in executor.startup_reclaim_lease_warnings[0]
    assert f"ignoring id={low_id}" in executor.startup_reclaim_lease_warnings[0]
    assert "multiple durable capacity reclaim leases" in caplog.text


def test_executor_startup_hydration_fails_closed_when_reclaim_table_is_missing(tmp_path):
    from qbt_orchestrator.executor import Executor

    db = tmp_path / "unmigrated.sqlite"
    db.touch()

    with pytest.raises(
        RuntimeError,
        match="durable capacity reclaim lease startup hydration failed",
    ):
        Executor(RecordingQbt(), dry_run=False, state_db=db)


def test_hash_mutation_lease_blocks_non_owner_mutators_and_allows_owner():
    from qbt_orchestrator.executor import Executor, QbtMutationLeaseBlocked

    qbt = RecordingQbt()
    executor = Executor(qbt, dry_run=False)
    token = "reclaim:7:4"
    try:
        assert executor.acquire_hash_mutation_lease("H", token) is True
        blocked_payloads = (
            (
                "/api/v2/torrents/filePrio",
                {"hash": "h", "id": "0", "priority": "0"},
            ),
            ("/api/v2/torrents/start", {"hashes": "other|H"}),
            (
                "/api/v2/torrents/setLocation",
                {"hashes": "h", "location": "/downloads/other"},
            ),
        )
        for path, payload in blocked_payloads:
            with pytest.raises(QbtMutationLeaseBlocked) as caught:
                executor.qbt_post(path, payload)
            assert caught.value.path == path
            assert "h" in caught.value.hashes
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
    from qbt_orchestrator.executor import Executor, QbtMutationLeaseBlocked

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
    def queued_mutation():
        try:
            executor.qbt_post(
                "/api/v2/torrents/start", {"hashes": "h"}
            )
        except QbtMutationLeaseBlocked as exc:
            results["queued"] = exc

    queued = threading.Thread(target=queued_mutation)
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

    assert results["first"] is True
    assert isinstance(results["queued"], QbtMutationLeaseBlocked)
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
    from qbt_orchestrator.executor import Executor, QbtMutationLeaseBlocked

    executor = Executor(RecordingQbt(), dry_run=True)
    assert executor.hydrate_hash_mutation_lease(" H ", "reclaim:2:4") is True
    assert executor.hydrate_hash_mutation_lease("h", "reclaim:2:4") is True
    assert executor.acquire_hash_mutation_lease("h", "other") is False
    assert executor.release_hash_mutation_lease("h", "other") is False
    with pytest.raises(QbtMutationLeaseBlocked):
        executor.qbt_post(
            "/api/v2/torrents/start", {"hashes": " H "}
        )
    assert executor.action_log[-1].status == "skipped_hash_lease"
    assert executor.action_log[-1].dry_run is True
    assert executor.release_hash_mutation_lease("H", "reclaim:2:4") is True
    assert executor.qbt_post(
        "/api/v2/torrents/start", {"hashes": "h"}
    ) is True
    assert executor.action_log[-1].status == "dry_run"


def test_hash_mutation_lease_blocks_all_hashes_selector_while_any_lease_exists():
    from qbt_orchestrator.executor import Executor, QbtMutationLeaseBlocked

    qbt = RecordingQbt()
    executor = Executor(qbt, dry_run=False)
    try:
        assert executor.acquire_hash_mutation_lease("h", "reclaim:1:4") is True
        with pytest.raises(QbtMutationLeaseBlocked):
            executor.qbt_post(
                "/api/v2/torrents/stop", {"hashes": "all"}
            )
    finally:
        executor.close(timeout=1)
    assert qbt.posts == []
    assert executor.action_log[-1].status == "skipped_hash_lease"


def test_guarded_stale_returns_false_but_lease_conflict_raises():
    from qbt_orchestrator.executor import Executor, QbtMutationLeaseBlocked

    qbt = RecordingQbt()
    executor = Executor(qbt, dry_run=False)
    try:
        assert executor.qbt_post_guarded(
            "/api/v2/torrents/start",
            {"hashes": "other"},
            guard=lambda: False,
        ) is False
        assert executor.action_log[-1].status == "skipped_stale_generation"

        assert executor.acquire_hash_mutation_lease(
            "h", "reclaim:1:1"
        ) is True
        with pytest.raises(QbtMutationLeaseBlocked):
            executor.qbt_post_guarded(
                "/api/v2/torrents/start",
                {"hashes": " H "},
                guard=lambda: False,
            )
        assert executor.action_log[-1].status == "skipped_hash_lease"
    finally:
        executor.close(timeout=1)

    assert qbt.posts == []


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
