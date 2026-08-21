from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import parse_qs, urlsplit

import pytest

from qbt_orchestrator.db import migrate, readonly_connect


class Clock:
    def __init__(self, value: int = 1_900_000_000):
        self.value = value

    def __call__(self) -> int:
        return self.value

    def advance(self, seconds: int) -> None:
        self.value += seconds


class RecordingExecutor:
    def __init__(self, *, result: bool = True):
        self.result = result
        self.posts: list[tuple[str, dict]] = []

    def qbt_post(self, path, payload):
        self.posts.append((path, dict(payload)))
        return self.result

    def qbt_post_guarded(self, path, payload, *, guard):
        if not guard():
            return False
        return self.qbt_post(path, payload)


class SnapshotQbt:
    def __init__(
        self, files=None, *, state="stoppedDL", tags="", has_metadata=None
    ):
        self.files = list(files or [])
        self.file_reads: list[str] = []
        self.state = state
        self.tags = tags
        self.has_metadata = has_metadata
        self.info_reads: list[str] = []

    def torrent_files(self, torrent_hash):
        self.file_reads.append(torrent_hash)
        return [dict(row) for row in self.files]

    def torrent_info(self, torrent_hash):
        self.info_reads.append(torrent_hash)
        return {
            "hash": torrent_hash,
            "state": self.state,
            "tags": self.tags,
            "has_metadata": self.has_metadata,
        }


def test_precheck_gateway_uses_existing_executor_and_fixed_safe_payload():
    from qbt_orchestrator.qbt_precheck import QbtPrecheckGateway

    executor = RecordingExecutor()
    gateway = QbtPrecheckGateway(SnapshotQbt(), executor)
    magnet = "magnet:?" + "xt=urn:btih:" + "a" * 40
    tag = "add-item-" + "b" * 32

    gateway.add_magnet(magnet, tag)
    gateway.stop("c" * 40)
    gateway.zero_file_priorities(
        "c" * 40,
        [{"index": 2}, {"index": 0}, {"index": 2}],
    )
    gateway.remove_registration("c" * 40)

    assert executor.posts == [
        (
            "/api/v2/torrents/add",
            {
                "urls": magnet,
                "category": "precheck",
                "tags": f"precheck,metadata-probe,{tag},hold",
                "stopped": "false",
                "dlLimit": "1024",
            },
        ),
        ("/api/v2/torrents/stop", {"hashes": "c" * 40}),
        (
            "/api/v2/torrents/filePrio",
            {"hash": "c" * 40, "id": "0|2", "priority": "0"},
        ),
        (
            "/api/v2/torrents/delete",
            {"hashes": "c" * 40, "deleteFiles": "false"},
        ),
    ]


def test_precheck_gateway_starts_owned_metadata_probe_with_payload_limit():
    from qbt_orchestrator.qbt_precheck import QbtPrecheckGateway

    executor = RecordingExecutor()
    gateway = QbtPrecheckGateway(SnapshotQbt(), executor)

    assert gateway.start_metadata_probe(
        "c" * 40, payload_limit_bps=1024, guard=lambda: True
    ) is True
    assert executor.posts == [
        (
            "/api/v2/torrents/setDownloadLimit",
            {"hashes": "c" * 40, "limit": "1024"},
        ),
        ("/api/v2/torrents/start", {"hashes": "c" * 40}),
    ]


def test_precheck_gateway_reconciles_an_exact_opaque_tag_and_rejects_ambiguity():
    from qbt_orchestrator.qbt_precheck import QbtPrecheckGateway

    tag = "add-item-" + "d" * 32
    gateway = QbtPrecheckGateway(SnapshotQbt(), RecordingExecutor())
    gateway.set_snapshots(
        {
            "a" * 40: {"hash": "a" * 40, "tags": f"hold,{tag}", "state": "metaDL"},
            "b" * 40: {"hash": "b" * 40, "tags": "hold,unrelated"},
        }
    )
    assert gateway.find_by_tag(tag)["hash"] == "a" * 40

    gateway.set_snapshots(
        {
            "a" * 40: {"hash": "a" * 40, "tags": tag},
            "b" * 40: {"hash": "b" * 40, "tags": tag},
        }
    )
    with pytest.raises(ValueError, match="^qbt_precheck_tag_ambiguous$"):
        gateway.find_by_tag(tag)


def test_precheck_gateway_reads_current_torrent_state_through_existing_client():
    from qbt_orchestrator.qbt_precheck import QbtPrecheckGateway

    qbt = SnapshotQbt(state="pausedDL")
    gateway = QbtPrecheckGateway(qbt, RecordingExecutor())
    assert gateway.torrent_info("a" * 40)["state"] == "pausedDL"
    assert qbt.info_reads == ["a" * 40]


def test_precheck_gateway_validation_never_calls_executor_for_unsafe_inputs():
    from qbt_orchestrator.qbt_precheck import QbtPrecheckGateway

    executor = RecordingExecutor()
    gateway = QbtPrecheckGateway(SnapshotQbt(), executor)
    with pytest.raises(ValueError, match="^magnet_uri$"):
        gateway.add_magnet("https://example.test/file", "add-item-" + "a" * 32)
    with pytest.raises(ValueError, match="^qbt_precheck_tag$"):
        gateway.add_magnet(
            "magnet:?" + "xt=urn:btih:" + "a" * 40,
            "hold,precheck",
        )
    with pytest.raises(ValueError, match="^torrent_hash$"):
        gateway.stop("not-a-hash")
    with pytest.raises(ValueError, match="^file_index$"):
        gateway.zero_file_priorities("a" * 40, [{"index": -1}])
    assert executor.posts == []


def test_precheck_gateway_uses_executor_guard_to_fence_stale_metadata_lease():
    from qbt_orchestrator.qbt_precheck import QbtPrecheckGateway

    executor = RecordingExecutor()
    gateway = QbtPrecheckGateway(SnapshotQbt(), executor)

    assert gateway.stop("a" * 40, guard=lambda: False) is False
    assert executor.posts == []
    assert gateway.stop("a" * 40, guard=lambda: True) is True
    assert executor.posts == [
        ("/api/v2/torrents/stop", {"hashes": "a" * 40})
    ]


def test_global_dry_run_never_calls_real_qbt_precheck_write():
    from qbt_orchestrator.executor import Executor
    from qbt_orchestrator.qbt_precheck import QbtPrecheckGateway

    class Qbt(SnapshotQbt):
        def post(self, _path, _payload):
            raise AssertionError("dry-run must not call qBT")

    executor = Executor(Qbt(), dry_run=True)
    gateway = QbtPrecheckGateway(executor.qbt, executor)
    assert gateway.add_magnet(
        "magnet:?" + "xt=urn:btih:" + "a" * 40,
        "add-item-" + "b" * 32,
    ) is True
    assert executor.action_log[-1].status == "dry_run"


def test_metadata_probe_config_keeps_only_fixed_personal_instance_controls():
    from qbt_orchestrator.metadata_probe import MetadataProbeConfig

    config = MetadataProbeConfig()
    assert config.slots == 3
    assert config.poll_interval_sec == 5
    assert config.windows_sec == (300, 600, 900)
    assert config.backoffs_sec == (1800, 21600)
    assert config.payload_limit_bps == 1024
    assert config.lease_sec == 30
    assert config.visibility_grace_sec == 30
    with pytest.raises(ValueError, match="slots"):
        MetadataProbeConfig(slots=0)


class FakeProbeGateway:
    def __init__(self):
        self.by_tag: dict[str, dict] = {}
        self.files_by_hash: dict[str, list[dict]] = {}
        self.added: list[tuple[str, str]] = []
        self.stopped: list[str] = []
        self.zeroed: list[str] = []
        self.removed: list[str] = []
        self.find_calls: list[str] = []
        self.info_state: str | None = None
        self.info_tags: str | None = None
        self.info_reads: list[str] = []
        self.remove_tag_after_stop = False
        self.fail_add = False
        self.lose_add_response = False

    def set_snapshots(self, _snapshots):
        return None

    @staticmethod
    def expected_hash(magnet: str):
        query = parse_qs(urlsplit(magnet).query)
        return query["xt"][0].split(":")[-1].lower()

    def add_magnet(self, magnet: str, tag: str, *, guard=None):
        if guard is not None and not guard():
            return False
        self.added.append((magnet, tag))
        if self.fail_add:
            raise RuntimeError("transport failed; secret input must not be copied")
        query = parse_qs(urlsplit(magnet).query)
        torrent_hash = query["xt"][0].split(":")[-1].lower()
        self.by_tag.setdefault(
            tag,
            {"hash": torrent_hash, "tags": f"precheck,{tag},hold", "state": "metaDL"},
        )
        self.files_by_hash.setdefault(
            torrent_hash,
            [{"index": 0, "name": "video.mkv", "priority": 1}],
        )
        if self.lose_add_response:
            raise RuntimeError("response lost")
        return True

    def find_by_tag(self, tag: str):
        self.find_calls.append(tag)
        row = self.by_tag.get(tag)
        return None if row is None else dict(row)

    def find_by_hash(self, torrent_hash: str):
        for row in self.by_tag.values():
            if str(row.get("hash") or "").lower() == torrent_hash.lower():
                return dict(row)
        return None

    @staticmethod
    def metadata_ready(snapshot):
        return "meta" not in str(snapshot.get("state") or "").lower()

    def stop(self, torrent_hash: str, *, guard=None):
        if guard is not None and not guard():
            return False
        self.stopped.append(torrent_hash)
        if self.remove_tag_after_stop:
            for row in self.by_tag.values():
                if str(row.get("hash") or "").lower() == torrent_hash.lower():
                    row["tags"] = "hold"
            self.info_tags = "hold"
        return True

    def torrent_files(self, torrent_hash: str):
        return [dict(row) for row in self.files_by_hash.get(torrent_hash, [])]

    def torrent_info(self, torrent_hash: str):
        self.info_reads.append(torrent_hash)
        matching = next(
            (
                row
                for row in self.by_tag.values()
                if str(row.get("hash") or "").lower() == torrent_hash.lower()
            ),
            {},
        )
        tags = self.info_tags
        if tags is None:
            tags = str(matching.get("tags") or "")
        state = self.info_state
        if state is None:
            state = str(matching.get("state") or "")
        return {"hash": torrent_hash, "state": state, "tags": tags}

    def zero_file_priorities(self, torrent_hash: str, files, *, guard=None):
        if guard is not None and not guard():
            return False
        self.zeroed.append(torrent_hash)
        for row in self.files_by_hash[torrent_hash]:
            row["priority"] = 0
        return True

    @staticmethod
    def all_priorities_zero(files):
        return bool(files) and all(int(row.get("priority") or 0) == 0 for row in files)

    def remove_registration(self, torrent_hash: str, *, guard=None):
        if guard is not None and not guard():
            return False
        self.removed.append(torrent_hash)
        return True


def _probe_fixture(tmp_path, *, batches: list[int], clock=None, owner="worker-a"):
    from qbt_orchestrator.bot_add_queue import BotAddQueueRepository
    from qbt_orchestrator.metadata_probe import MetadataProbeCoordinator

    db = tmp_path / "state.sqlite"
    migrate(db)
    clock = clock or Clock()
    queue = BotAddQueueRepository(db, now=clock)
    item_ids: list[int] = []
    next_hash = 1
    for count in batches:
        batch = queue.open_draft("chat", f"user-{len(item_ids)}")
        links = [
            "magnet:?" + "xt=urn:btih:" + f"{value:040x}"
            for value in range(next_hash, next_hash + count)
        ]
        next_hash += count
        queue.append_message(batch["id"], 100 + batch["id"], links)
        queue.submit(batch["id"])
        for item in queue.list_items(batch["id"]):
            queue.transition_item(item["id"], {"received"}, "resolving", "resolved")
            queue.transition_item(
                item["id"], {"resolving"}, "waiting_probe_slot", "probe_required"
            )
            item_ids.append(item["id"])
    gateway = FakeProbeGateway()
    coordinator = MetadataProbeCoordinator(queue, gateway, owner=owner, now=clock)
    return queue, gateway, coordinator, clock, item_ids, db


def test_coordinator_uses_three_slots_and_round_robins_across_batches(tmp_path):
    queue, gateway, coordinator, clock, item_ids, _db = _probe_fixture(
        tmp_path, batches=[2, 1, 1]
    )

    result = coordinator.tick(sync_healthy=True)

    assert len(result["started"]) == 3
    started = [queue.get_item(item_id) for item_id in result["started"]]
    assert len({item["batch_id"] for item in started}) == 3
    assert all(item["state"] == "metadata_wait" for item in started)
    assert all(item["metadata_probe_attempt"] == 1 for item in started)
    assert all(item["metadata_probe_deadline"] == clock.value + 300 for item in started)
    assert len(gateway.added) == 3
    waiting = [queue.get_item(item_id) for item_id in item_ids if item_id not in result["started"]]
    assert [item["state"] for item in waiting] == ["waiting_probe_slot"]


def test_single_batch_can_fill_all_three_probe_slots(tmp_path):
    queue, _gateway, coordinator, _clock, _item_ids, _db = _probe_fixture(
        tmp_path, batches=[4]
    )
    result = coordinator.tick()
    assert len(result["started"]) == 3
    assert sum(
        item["state"] == "metadata_wait"
        for item in queue.list_items(queue.get_item(result["started"][0])["batch_id"])
    ) == 3


def test_three_batches_reserve_new_slots_for_batches_without_an_active_probe(tmp_path):
    from qbt_orchestrator.metadata_probe import (
        MetadataProbeConfig,
        MetadataProbeCoordinator,
    )

    queue, gateway, _coordinator, clock, item_ids, _db = _probe_fixture(
        tmp_path, batches=[2, 1, 1]
    )
    one_slot = MetadataProbeCoordinator(
        queue,
        gateway,
        owner="worker-a",
        now=clock,
        config=MetadataProbeConfig(slots=1),
    )
    first = one_slot.tick()
    active = queue.get_item(first["started"][0])
    assert active["batch_id"] == queue.get_item(item_ids[0])["batch_id"]

    three_slots = MetadataProbeCoordinator(
        queue,
        gateway,
        owner="worker-a",
        now=clock,
        config=MetadataProbeConfig(slots=3),
    )
    filled = three_slots.tick()
    filled_batches = {
        queue.get_item(item_id)["batch_id"] for item_id in filled["started"]
    }
    all_batches = {queue.get_item(item_id)["batch_id"] for item_id in item_ids}

    assert len(filled["started"]) == 2
    assert filled_batches == all_batches - {active["batch_id"]}


def test_polling_is_due_only_and_tick_never_sleeps(tmp_path, monkeypatch):
    _queue, gateway, coordinator, clock, _item_ids, _db = _probe_fixture(
        tmp_path, batches=[1]
    )
    monkeypatch.setattr(
        "time.sleep", lambda *_args: pytest.fail("coordinator tick must not sleep")
    )
    coordinator.tick()
    calls_after_start = len(gateway.find_calls)
    clock.advance(4)
    coordinator.tick()
    assert len(gateway.find_calls) == calls_after_start
    clock.advance(1)
    coordinator.tick()
    assert len(gateway.find_calls) == calls_after_start + 1


def test_ready_probe_is_stopped_zeroed_verified_and_left_for_prechecking(tmp_path):
    queue, gateway, coordinator, clock, item_ids, _db = _probe_fixture(
        tmp_path, batches=[1]
    )
    coordinator.tick()
    item = queue.get_item(item_ids[0])
    gateway.by_tag[item["qbt_precheck_tag"]]["state"] = "stoppedDL"
    clock.advance(5)

    result = coordinator.tick()

    ready = queue.get_item(item_ids[0])
    assert result["ready"] == [item_ids[0]]
    assert ready["state"] == "prechecking"
    assert ready["metadata_lease_owner"] is None
    assert gateway.stopped == [ready["qbt_hash"]]
    assert gateway.zeroed == [ready["qbt_hash"]]
    assert gateway.files_by_hash[ready["qbt_hash"]][0]["priority"] == 0
    assert len(gateway.info_reads) >= 3
    assert set(gateway.info_reads) == {ready["qbt_hash"]}


@pytest.mark.parametrize(
    "state", ["downloading", "uploading", "checkingDL", "forcedDL"]
)
def test_ready_probe_does_not_advance_until_qbt_confirms_it_is_stopped(
    tmp_path, state
):
    queue, gateway, coordinator, clock, item_ids, _db = _probe_fixture(
        tmp_path, batches=[1]
    )
    coordinator.tick()
    item = queue.get_item(item_ids[0])
    gateway.by_tag[item["qbt_precheck_tag"]]["state"] = "stoppedDL"
    gateway.info_state = state
    clock.advance(5)

    result = coordinator.tick()

    stored = queue.get_item(item_ids[0])
    assert result["ready"] == []
    assert stored["state"] == "metadata_wait"
    assert stored["last_error"] == "qbt_precheck_failed"


def test_existing_same_hash_without_item_tag_finishes_as_duplicate_without_qbt_writes(
    tmp_path,
):
    from qbt_orchestrator.metadata_probe import MetadataProbeCoordinator
    from qbt_orchestrator.qbt_precheck import QbtPrecheckGateway

    qbt = SnapshotQbt(state="downloading", tags="auto")
    queue, _gateway, _coordinator, clock, item_ids, _db = _probe_fixture(
        tmp_path, batches=[1]
    )
    executor = RecordingExecutor()
    result = MetadataProbeCoordinator(
        queue,
        QbtPrecheckGateway(qbt, executor),
        owner="worker",
        now=clock,
    ).tick(snapshots={})

    item = queue.get_item(item_ids[0])
    batch = queue.get_batch(item["batch_id"])
    assert result["duplicates"] == [item_ids[0]]
    assert item["state"] == "duplicate_local"
    assert item["decision_reason"] == "existing_torrent_without_precheck_tag"
    assert item["raw_input"] is None
    assert item["metadata_lease_owner"] is None
    assert batch["duplicate_count"] == 1
    assert batch["state"] == "complete"
    assert executor.posts == []


def test_owned_existing_stopped_magnet_is_started_to_fetch_metadata(tmp_path):
    from qbt_orchestrator.metadata_probe import MetadataProbeCoordinator
    from qbt_orchestrator.qbt_precheck import QbtPrecheckGateway

    queue, _gateway, _coordinator, clock, item_ids, _db = _probe_fixture(
        tmp_path, batches=[1]
    )
    item = queue.get_item(item_ids[0])
    tag = MetadataProbeCoordinator._tag_for(item)
    qbt = SnapshotQbt(
        state="stoppedDL", tags=f"auto,checked,{tag}", has_metadata=False
    )
    executor = RecordingExecutor()

    result = MetadataProbeCoordinator(
        queue,
        QbtPrecheckGateway(qbt, executor),
        owner="worker",
        now=clock,
    ).tick(snapshots={})

    stored = queue.get_item(item_ids[0])
    assert result["started"] == [item_ids[0]]
    assert stored["state"] == "metadata_wait"
    assert executor.posts == [
        (
            "/api/v2/torrents/setDownloadLimit",
            {"hashes": stored["qbt_hash"], "limit": "1024"},
        ),
        ("/api/v2/torrents/start", {"hashes": stored["qbt_hash"]}),
    ]


def test_realtime_tag_removal_before_ready_write_never_stops_or_zeroes_foreign_torrent(
    tmp_path,
):
    queue, gateway, coordinator, clock, item_ids, _db = _probe_fixture(
        tmp_path, batches=[1]
    )
    coordinator.tick()
    item = queue.get_item(item_ids[0])
    gateway.by_tag[item["qbt_precheck_tag"]]["state"] = "stoppedDL"
    gateway.info_tags = "auto"
    clock.advance(5)

    result = coordinator.tick()

    assert result["duplicates"] == [item_ids[0]]
    assert queue.get_item(item_ids[0])["state"] == "duplicate_local"
    assert gateway.stopped == []
    assert gateway.zeroed == []


def test_each_ready_write_rechecks_tag_and_skips_zero_if_tag_removed_after_stop(
    tmp_path,
):
    queue, gateway, coordinator, clock, item_ids, _db = _probe_fixture(
        tmp_path, batches=[1]
    )
    coordinator.tick()
    item = queue.get_item(item_ids[0])
    gateway.by_tag[item["qbt_precheck_tag"]]["state"] = "stoppedDL"
    gateway.remove_tag_after_stop = True
    clock.advance(5)

    coordinator.tick()

    assert gateway.stopped == [item["qbt_hash"]]
    assert gateway.zeroed == []
    assert queue.get_item(item_ids[0])["state"] == "metadata_wait"


def test_probe_windows_and_backoffs_end_in_metadata_unavailable(tmp_path):
    queue, gateway, coordinator, clock, item_ids, _db = _probe_fixture(
        tmp_path, batches=[1]
    )
    coordinator.tick()
    registered_hash = queue.get_item(item_ids[0])["qbt_hash"]

    clock.advance(300)
    first = coordinator.tick()
    item = queue.get_item(item_ids[0])
    assert first["timed_out"] == [item_ids[0]]
    assert item["state"] == "metadata_retry_wait"
    assert item["metadata_retry_at"] == clock.value + 1800
    assert gateway.removed == [registered_hash]

    clock.advance(1800)
    coordinator.tick()
    item = queue.get_item(item_ids[0])
    assert item["metadata_probe_attempt"] == 2
    assert item["metadata_probe_deadline"] == clock.value + 600
    clock.advance(600)
    coordinator.tick()
    item = queue.get_item(item_ids[0])
    assert item["state"] == "metadata_retry_wait"
    assert item["metadata_retry_at"] == clock.value + 21600

    clock.advance(21600)
    coordinator.tick()
    item = queue.get_item(item_ids[0])
    assert item["metadata_probe_attempt"] == 3
    assert item["metadata_probe_deadline"] == clock.value + 900
    clock.advance(900)
    coordinator.tick()
    item = queue.get_item(item_ids[0])
    assert item["state"] == "metadata_unavailable"
    assert item["approval_generation"] == 1
    assert item["metadata_lease_owner"] is None


def test_metadata_unavailable_reports_warning_inbox_with_action_buttons(tmp_path):
    import json

    from qbt_orchestrator.metadata_probe import MetadataProbeCoordinator
    from qbt_orchestrator.runtime import BotNotificationRepository
    from qbt_orchestrator.warning_inbox import WarningService

    queue, gateway, _coordinator, clock, item_ids, db = _probe_fixture(
        tmp_path, batches=[1]
    )
    warnings = WarningService(
        db,
        admin_chat_id="1001",
        notifications=BotNotificationRepository(db, now=clock),
        now=clock,
    )
    coordinator = MetadataProbeCoordinator(
        queue, gateway, owner="worker-a", now=clock, warning_service=warnings
    )
    coordinator.tick()
    clock.advance(300)
    coordinator.tick()
    clock.advance(1800)
    coordinator.tick()
    clock.advance(600)
    coordinator.tick()
    clock.advance(21600)
    coordinator.tick()
    clock.advance(900)
    coordinator.tick()
    item = queue.get_item(item_ids[0])
    assert item["state"] == "metadata_unavailable"
    batch_id = int(item["batch_id"])
    item_id = int(item["id"])
    generation = int(item["approval_generation"])

    con = readonly_connect(db)
    try:
        warning = con.execute(
            "select id,warning_key,related_batch_id,related_item_id,occurrence_count "
            "from bot_warning_inbox where warning_key=?",
            (f"checked_add:metadata_unavailable:{item_id}",),
        ).fetchone()
        notes = con.execute(
            "select count(*) from bot_notifications where dedupe_key like 'warn:%'"
        ).fetchone()[0]
    finally:
        con.close()
    assert warning is not None
    assert int(warning["related_batch_id"]) == batch_id
    assert int(warning["related_item_id"]) == item_id
    # Warnings stay in the center; retry/cancel live on the queue detail panel.
    assert int(notes) == 0
    del generation  # retained for readability of the scenario above

    # Same item/generation must not create a second inbox occurrence.
    second = coordinator.tick()
    assert int(second.get("warning_backfilled") or 0) == 0
    con = readonly_connect(db)
    try:
        notes = con.execute(
            "select count(*) from bot_notifications where topic='metadata_probe'"
        ).fetchone()[0]
        occurrences = con.execute(
            "select occurrence_count from bot_warning_inbox where id=?",
            (int(warning["id"]),),
        ).fetchone()[0]
    finally:
        con.close()
    assert int(notes) == 0
    assert int(occurrences) == 1


def test_metadata_tick_backfills_missing_warning_inbox(tmp_path):
    from qbt_orchestrator.db import write_execute
    from qbt_orchestrator.metadata_probe import MetadataProbeCoordinator
    from qbt_orchestrator.runtime import BotNotificationRepository
    from qbt_orchestrator.warning_inbox import WarningService

    queue, gateway, _coordinator, clock, item_ids, db = _probe_fixture(
        tmp_path, batches=[1]
    )
    item_id = int(item_ids[0])
    write_execute(
        db,
        "update bot_add_items set state='metadata_unavailable',approval_generation=1 "
        "where id=?",
        (item_id,),
    )
    warnings = WarningService(
        db,
        admin_chat_id="1001",
        notifications=BotNotificationRepository(db, now=clock),
        now=clock,
    )
    coordinator = MetadataProbeCoordinator(
        queue, gateway, owner="worker-a", now=clock, warning_service=warnings
    )
    result = coordinator.tick(sync_healthy=False)
    assert int(result["warning_backfilled"]) == 1
    con = readonly_connect(db)
    try:
        warning = con.execute(
            "select related_item_id from bot_warning_inbox where warning_key=?",
            (f"checked_add:metadata_unavailable:{item_id}",),
        ).fetchone()
        notes = con.execute(
            "select count(*) from bot_notifications where topic='metadata_probe'"
        ).fetchone()[0]
    finally:
        con.close()
    assert warning is not None
    assert int(warning["related_item_id"]) == item_id
    assert int(notes) == 0
    # Already present: no second backfill.
    again = coordinator.tick(sync_healthy=False)
    assert int(again["warning_backfilled"]) == 0


def test_metadata_tick_enriches_legacy_warning_without_new_occurrence(tmp_path):
    from qbt_orchestrator.db import write_execute
    from qbt_orchestrator.metadata_probe import MetadataProbeCoordinator
    from qbt_orchestrator.warning_inbox import WarningService

    queue, gateway, _coordinator, clock, item_ids, db = _probe_fixture(
        tmp_path, batches=[1]
    )
    item_id = int(item_ids[0])
    item = queue.get_item(item_id)
    write_execute(
        db,
        "update bot_add_items set state='metadata_unavailable',approval_generation=1,"
        "display_name='BBAN-523' where id=?",
        (item_id,),
    )
    write_execute(
        db,
        "insert into bot_warning_inbox("
        "warning_key,severity,topic,safe_message,related_batch_id,related_item_id,"
        "occurrence_count,first_occurred_at,last_occurred_at,updated_at,resolved) "
        "values(?,?,?,?,?,?,1,1,1,1,0)",
        (
            f"checked_add:metadata_unavailable:{item_id}",
            "warning",
            "metadata_probe",
            "暂时无法获取元数据，请从批次详情选择重试或取消。",
            int(item["batch_id"]),
            item_id,
        ),
    )
    coordinator = MetadataProbeCoordinator(
        queue,
        gateway,
        owner="worker-a",
        now=clock,
        warning_service=WarningService(db, now=clock),
    )
    result = coordinator.tick(sync_healthy=False)
    assert int(result["warning_backfilled"]) == 1
    con = readonly_connect(db)
    try:
        warning = con.execute(
            "select safe_message,occurrence_count,occurrence_fingerprint "
            "from bot_warning_inbox where warning_key=?",
            (f"checked_add:metadata_unavailable:{item_id}",),
        ).fetchone()
    finally:
        con.close()
    assert "BBAN-523" in str(warning["safe_message"])
    assert int(warning["occurrence_count"]) == 1
    assert str(warning["occurrence_fingerprint"]).startswith("metadata:")


def test_timeout_never_touches_same_hash_without_item_tag_and_marks_duplicate(tmp_path):
    from qbt_orchestrator.metadata_probe import MetadataProbeCoordinator
    from qbt_orchestrator.qbt_precheck import QbtPrecheckGateway

    qbt = SnapshotQbt(state="metaDL")
    queue, _gateway, _coordinator, clock, _item_ids, _db = _probe_fixture(
        tmp_path, batches=[1]
    )
    executor = RecordingExecutor()
    coordinator = MetadataProbeCoordinator(
        queue,
        QbtPrecheckGateway(qbt, executor),
        owner="worker",
        now=clock,
    )
    coordinator.tick(snapshots={})
    assert queue.get_item(_item_ids[0])["state"] == "duplicate_local"
    assert executor.posts == []


def test_timeout_can_reconcile_owned_registration_by_hash_when_tagged(tmp_path):
    from qbt_orchestrator.metadata_probe import MetadataProbeCoordinator
    from qbt_orchestrator.qbt_precheck import QbtPrecheckGateway

    queue, _gateway, _coordinator, clock, item_ids, _db = _probe_fixture(
        tmp_path, batches=[1]
    )
    item = queue.get_item(item_ids[0])
    tag = MetadataProbeCoordinator._tag_for(item)
    qbt = SnapshotQbt(state="metaDL", tags=f"precheck,{tag},hold")
    executor = RecordingExecutor()
    coordinator = MetadataProbeCoordinator(
        queue,
        QbtPrecheckGateway(qbt, executor),
        owner="worker",
        now=clock,
    )
    coordinator.tick(snapshots={})
    clock.advance(300)
    coordinator.tick(snapshots={})

    assert (
        "/api/v2/torrents/delete",
        {"hashes": "0" * 39 + "1", "deleteFiles": "false"},
    ) in executor.posts


def test_active_lease_blocks_second_worker_and_expired_lease_recovers_by_tag(tmp_path):
    from qbt_orchestrator.metadata_probe import MetadataProbeCoordinator

    queue, gateway, first, clock, item_ids, _db = _probe_fixture(
        tmp_path, batches=[1], owner="worker-a"
    )
    first.tick()
    first_lease = queue.get_item(item_ids[0])
    second = MetadataProbeCoordinator(queue, gateway, owner="worker-b", now=clock)
    assert second.tick()["started"] == []
    assert len(gateway.added) == 1

    clock.advance(31)
    recovered = second.tick()
    assert recovered["recovered"] == [item_ids[0]]
    assert len(gateway.added) == 1
    assert queue.get_item(item_ids[0])["metadata_lease_owner"] == "worker-b"
    with pytest.raises(ValueError, match="^metadata_lease_conflict$"):
        queue.update_metadata_probe(
            item_ids[0],
            "worker-a",
            first_lease["metadata_lease_generation"],
            {"last_error": "stale"},
        )


def test_qbt_failure_keeps_item_recoverable_without_persisting_raw_error(tmp_path):
    queue, gateway, coordinator, _clock, item_ids, _db = _probe_fixture(
        tmp_path, batches=[1]
    )
    gateway.fail_add = True

    result = coordinator.tick()

    item = queue.get_item(item_ids[0])
    assert result["errors"] == 1
    assert item["state"] == "metadata_wait"
    assert item["metadata_lease_owner"] == "worker-a"
    assert item["last_error"] == "qbt_precheck_failed"
    assert "magnet" not in item["last_error"]


def test_qbt_failure_stops_new_claims_and_deduplicates_repeated_error_counts(tmp_path):
    queue, gateway, coordinator, clock, item_ids, _db = _probe_fixture(
        tmp_path, batches=[3]
    )
    gateway.fail_add = True

    first = coordinator.tick()
    assert first["errors"] == 1
    assert sum(queue.get_item(item_id)["state"] == "metadata_wait" for item_id in item_ids) == 1

    clock.advance(5)
    repeated = coordinator.tick()
    assert repeated["errors"] == 0
    assert sum(queue.get_item(item_id)["state"] == "metadata_wait" for item_id in item_ids) == 1


def test_lost_add_response_is_reconciled_by_tag_without_duplicate_add(tmp_path):
    queue, gateway, coordinator, clock, item_ids, _db = _probe_fixture(
        tmp_path, batches=[1]
    )
    gateway.lose_add_response = True
    first = coordinator.tick()
    assert first["errors"] == 1
    assert len(gateway.added) == 1

    gateway.lose_add_response = False
    clock.advance(5)
    recovered = coordinator.tick()
    assert recovered["errors"] == 0
    assert len(gateway.added) == 1
    assert queue.get_item(item_ids[0])["qbt_hash"] is not None
    assert queue.get_item(item_ids[0])["last_error"] is None


def test_real_gateway_does_not_repeat_add_while_safe_snapshots_stay_empty(tmp_path):
    from qbt_orchestrator.metadata_probe import MetadataProbeCoordinator
    from qbt_orchestrator.qbt_precheck import QbtPrecheckGateway

    class NotVisibleQbt(SnapshotQbt):
        def torrent_info(self, torrent_hash):
            self.info_reads.append(torrent_hash)
            return {"hash": torrent_hash}

    queue, _gateway, _coordinator, clock, item_ids, _db = _probe_fixture(
        tmp_path, batches=[1]
    )
    executor = RecordingExecutor()
    gateway = QbtPrecheckGateway(NotVisibleQbt(), executor)
    coordinator = MetadataProbeCoordinator(
        queue, gateway, owner="worker", now=clock
    )

    coordinator.tick(snapshots={})
    clock.advance(5)
    coordinator.tick(snapshots={})

    add_posts = [
        post for post in executor.posts if post[0] == "/api/v2/torrents/add"
    ]
    assert len(add_posts) == 1
    stored = queue.get_item(item_ids[0])
    assert stored["state"] == "metadata_wait"
    assert stored["qbt_hash"] == "0" * 39 + "1"


def test_marker_before_add_waits_for_grace_then_requeues_without_consuming_attempt(
    tmp_path,
):
    from qbt_orchestrator.metadata_probe import MetadataProbeCoordinator
    from qbt_orchestrator.qbt_precheck import QbtPrecheckGateway

    class NotVisibleQbt(SnapshotQbt):
        def torrent_info(self, torrent_hash):
            return {"hash": torrent_hash}

    queue, _gateway, _coordinator, clock, item_ids, _db = _probe_fixture(
        tmp_path, batches=[1]
    )
    item_id = item_ids[0]
    lease = queue.claim_metadata_lease(item_id, "crashed", clock.value + 30)
    queue.transition_item(
        item_id,
        {"waiting_probe_slot"},
        "metadata_wait",
        "metadata_probe_started",
        {
            "metadata_probe_attempt": 1,
            "metadata_probe_started_at": clock.value,
            "metadata_probe_deadline": clock.value + 300,
            "metadata_next_poll_at": clock.value + 5,
            "qbt_precheck_tag": "add-item-" + "c" * 32,
            "qbt_hash": "0" * 39 + "1",
        },
        metadata_lease_owner="crashed",
        metadata_lease_generation=lease["metadata_lease_generation"],
    )
    executor = RecordingExecutor()
    coordinator = MetadataProbeCoordinator(
        queue,
        QbtPrecheckGateway(NotVisibleQbt(), executor),
        owner="restarted",
        now=clock,
    )

    clock.advance(5)
    coordinator.tick(snapshots={})
    assert executor.posts == []

    clock.advance(26)
    result = coordinator.tick(snapshots={})
    add_posts = [
        post for post in executor.posts if post[0] == "/api/v2/torrents/add"
    ]
    assert result["requeued"] == [item_id]
    assert len(add_posts) == 1
    assert queue.get_item(item_id)["metadata_probe_attempt"] == 1


def test_draft_and_not_due_retry_items_do_not_take_probe_slots(tmp_path):
    from qbt_orchestrator.bot_add_queue import BotAddQueueRepository
    from qbt_orchestrator.metadata_probe import MetadataProbeCoordinator

    db = tmp_path / "state.sqlite"
    migrate(db)
    clock = Clock()
    queue = BotAddQueueRepository(db, now=clock)
    draft = queue.open_draft("chat", "user")
    queue.append_message(
        draft["id"], 1, ["magnet:?" + "xt=urn:btih:" + "a" * 40]
    )
    gateway = FakeProbeGateway()
    coordinator = MetadataProbeCoordinator(queue, gateway, owner="worker", now=clock)

    assert coordinator.tick()["started"] == []
    assert gateway.added == []

    queue.submit(draft["id"])
    item = queue.list_items(draft["id"])[0]
    queue.transition_item(item["id"], {"received"}, "resolving", "resolved")
    queue.transition_item(item["id"], {"resolving"}, "metadata_unavailable", "failed")
    unavailable = queue.get_item(item["id"])
    queue.transition_item(
        item["id"],
        {"metadata_unavailable"},
        "metadata_retry_wait",
        "retry_later",
        approval_generation=unavailable["approval_generation"],
        metadata_action="retry_24h",
    )
    assert coordinator.tick()["started"] == []
    assert gateway.added == []


def test_probe_does_not_add_when_raw_input_expires_before_probe_window(tmp_path):
    from qbt_orchestrator.bot_add_queue import AddQueueLimits, BotAddQueueRepository
    from qbt_orchestrator.metadata_probe import MetadataProbeCoordinator

    db = tmp_path / "state.sqlite"
    migrate(db)
    clock = Clock()
    queue = BotAddQueueRepository(
        db,
        now=clock,
        limits=AddQueueLimits(raw_input_ttl_sec=100),
    )
    batch = queue.open_draft("chat", "user")
    queue.append_message(
        batch["id"], 1, ["magnet:?" + "xt=urn:btih:" + "a" * 40]
    )
    queue.submit(batch["id"])
    item = queue.list_items(batch["id"])[0]
    queue.transition_item(item["id"], {"received"}, "resolving", "resolved")
    queue.transition_item(
        item["id"], {"resolving"}, "waiting_probe_slot", "probe_required"
    )
    gateway = FakeProbeGateway()

    MetadataProbeCoordinator(queue, gateway, owner="worker", now=clock).tick()

    stored = queue.get_item(item["id"])
    assert stored["state"] == "metadata_unavailable"
    assert stored["metadata_lease_owner"] is None
    assert gateway.added == []

def test_unhealthy_sync_suspends_without_claiming_or_writing(tmp_path):
    queue, gateway, coordinator, _clock, item_ids, _db = _probe_fixture(
        tmp_path, batches=[1]
    )
    result = coordinator.tick(sync_healthy=False)
    assert result["suspended"] is True
    assert queue.get_item(item_ids[0])["state"] == "waiting_probe_slot"
    assert gateway.added == []


def test_daemon_runtime_only_schedules_optional_metadata_probe_and_passes_sync_snapshot(
    tmp_path,
):
    from qbt_orchestrator.service import DaemonRuntime

    class Qbt:
        def get_maindata(self, rid):
            return {
                "rid": rid + 1,
                "full_update": True,
                "torrents": {
                    "a" * 40: {
                        "hash": "a" * 40,
                        "tags": "precheck,add-item-" + "b" * 32,
                        "state": "metaDL",
                    }
                },
                "server_state": {},
            }

    class Coordinator:
        def __init__(self):
            self.calls = []

        def tick(self, *, sync_healthy, snapshots):
            self.calls.append((sync_healthy, snapshots))
            return {"started": []}

    coordinator = Coordinator()
    runtime = DaemonRuntime(
        state_db=tmp_path / "state.sqlite",
        qbt=Qbt(),
        executor=RecordingExecutor(),
        free_bytes_provider=lambda: 10 * 1024**3,
        dry_run=True,
        carousel_enabled=False,
        metadata_probe_coordinator=coordinator,
    )
    runtime.tick_safety()

    assert "metadata_probe" in [task.name for task in runtime.loop_tasks]
    runtime.metadata_probe_tick()
    assert coordinator.calls[0][0] is True
    assert "a" * 40 in coordinator.calls[0][1]

    disabled = DaemonRuntime(
        state_db=tmp_path / "disabled.sqlite",
        qbt=Qbt(),
        executor=RecordingExecutor(),
        free_bytes_provider=lambda: 10 * 1024**3,
        dry_run=True,
        carousel_enabled=False,
    )
    assert "metadata_probe" not in [task.name for task in disabled.loop_tasks]
    assert disabled.metadata_probe_tick() == {"status": "disabled"}


def test_cli_metadata_probe_flag_defaults_off_skips_global_dry_run_and_enables_live(
    tmp_path, monkeypatch
):
    import argparse
    from qbt_orchestrator import cli

    class Qbt:
        def post(self, _path, _payload):
            return "Ok."

        def torrent_files(self, _torrent_hash):
            return []

    monkeypatch.setattr(cli, "_build_qbt_client_from_env", lambda *_args: Qbt())
    monkeypatch.setenv("QBT_ORCH_STATE_DB", str(tmp_path / "state.sqlite"))
    monkeypatch.setenv("QBT_ORCH_DRY_RUN", "1")
    monkeypatch.setenv("QBT_ORCH_ORPHAN_JANITOR", "0")
    monkeypatch.setenv("QBT_ORCH_JUNK_JANITOR", "0")
    monkeypatch.setenv("QBT_ORCH_CAROUSEL", "0")
    monkeypatch.setenv("QBT_ORCH_QBT_PREFERENCES_GUARD", "0")
    monkeypatch.setenv("QBT_ORCH_PATH_RECONCILE", "0")
    monkeypatch.delenv("QBT_ORCH_METADATA_PROBE_ENABLED", raising=False)
    ns = argparse.Namespace(
        cmd="daemon",
        dry_run=True,
        config=None,
        safety_interval=0,
        max_safety_ticks=1,
    )

    disabled, _ = cli._build_runtime(ns, tmp_path / "state.sqlite")
    assert disabled.metadata_probe_coordinator is None

    monkeypatch.setenv("QBT_ORCH_METADATA_PROBE_ENABLED", "1")
    dry_enabled, _ = cli._build_runtime(ns, tmp_path / "state.sqlite")
    assert dry_enabled.metadata_probe_coordinator is None
    assert dry_enabled.metadata_probe_tick() == {"status": "disabled"}

    from qbt_orchestrator.bot_add_queue import BotAddQueueRepository

    queue = BotAddQueueRepository(tmp_path / "state.sqlite")
    batch = queue.open_draft("chat", "user")
    queue.append_message(
        batch["id"], 1, ["magnet:?" + "xt=urn:btih:" + "a" * 40]
    )
    queue.submit(batch["id"])
    item = queue.list_items(batch["id"])[0]
    queue.transition_item(item["id"], {"received"}, "resolving", "resolved")
    queue.transition_item(
        item["id"], {"resolving"}, "waiting_probe_slot", "probe_required"
    )
    dry_enabled.metadata_probe_tick()
    assert queue.get_item(item["id"])["state"] == "waiting_probe_slot"

    ns.dry_run = False
    monkeypatch.setenv("QBT_ORCH_DRY_RUN", "0")
    enabled, _ = cli._build_runtime(ns, tmp_path / "state.sqlite")
    assert enabled.metadata_probe_coordinator is not None
    assert [task.name for task in enabled.loop_tasks].count("metadata_probe") == 1
    enabled.executor.close(timeout=1)
