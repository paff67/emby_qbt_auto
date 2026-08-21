from __future__ import annotations

from urllib.parse import parse_qs, urlsplit

import pytest

from qbt_orchestrator.db import migrate


class Clock:
    def __init__(self, value: int = 2_000_000_000):
        self.value = value

    def __call__(self) -> int:
        return self.value


class Normalizer:
    def normalize(self, raw_filename):
        stem = str(raw_filename).rsplit("/", 1)[-1].split(".", 1)[0]
        return {"normalized_id": stem, "confidence": 1.0, "reason": "test"}


class FakeGateway:
    def __init__(self, torrent_hash: str, tag: str, name: str, size: int):
        self.torrent_hash = torrent_hash
        self.snapshot = {
            "hash": torrent_hash,
            "state": "stoppedDL",
            "tags": f"precheck,metadata-probe,hold,{tag}",
            "category": "precheck",
            "force_start": False,
            "dl_limit": 1024,
        }
        self.files = [{"index": 0, "name": name, "size": size, "priority": 0}]
        self.posts: list[tuple[str, dict]] = []
        self.removed = False
        self.fail_next_priority_write = False
        self.fail_remove_tag: str | None = None

    def torrent_info(self, torrent_hash):
        if self.removed:
            return {"hash": torrent_hash, "state": "", "tags": "", "category": ""}
        return dict(self.snapshot)

    def torrent_files(self, _torrent_hash):
        return [dict(row) for row in self.files]

    def remove_registration(self, torrent_hash, *, guard=None):
        if guard is not None and not guard():
            return False
        self.posts.append(("delete", {"hashes": torrent_hash, "deleteFiles": "false"}))
        self.removed = True
        return True

    def stop(self, torrent_hash, *, guard=None):
        ok = self._write("stop", {"hashes": torrent_hash}, guard)
        if ok:
            self.snapshot["state"] = "stoppedDL"
            self.snapshot["force_start"] = False
        return ok

    def set_category(self, torrent_hash, category, *, guard=None):
        ok = self._write("category", {"hashes": torrent_hash, "category": category}, guard)
        if ok:
            self.snapshot["category"] = category
        return ok

    def add_tags(self, torrent_hash, tags, *, guard=None):
        ok = self._write("add_tags", {"hashes": torrent_hash, "tags": tags}, guard)
        if ok:
            current = self._tags()
            current.update(str(tags).split(","))
            self.snapshot["tags"] = ",".join(sorted(current))
        return ok

    def remove_tags(self, torrent_hash, tags, *, guard=None):
        if self.fail_remove_tag and self.fail_remove_tag in str(tags).split(","):
            self.fail_remove_tag = None
            return False
        ok = self._write("remove_tags", {"hashes": torrent_hash, "tags": tags}, guard)
        if ok:
            current = self._tags() - set(str(tags).split(","))
            self.snapshot["tags"] = ",".join(sorted(current))
        return ok

    def set_force_start(self, torrent_hash, value, *, guard=None):
        ok = self._write("force", {"hashes": torrent_hash, "value": str(value).lower()}, guard)
        if ok:
            self.snapshot["force_start"] = bool(value)
        return ok

    def set_download_limit(self, torrent_hash, limit_bps, *, guard=None):
        ok = self._write(
            "download_limit",
            {"hashes": torrent_hash, "limit": str(limit_bps)},
            guard,
        )
        if ok:
            self.snapshot["dl_limit"] = int(limit_bps)
        return ok

    def set_file_priorities(self, torrent_hash, indices, priority, *, guard=None):
        if self.fail_next_priority_write:
            self.fail_next_priority_write = False
            return False
        ok = self._write(
            "file_priority",
            {"hash": torrent_hash, "id": "|".join(str(i) for i in indices), "priority": str(priority)},
            guard,
        )
        if ok:
            selected = set(indices)
            for row in self.files:
                if row["index"] in selected:
                    row["priority"] = priority
        return ok

    def _tags(self):
        return {part for part in str(self.snapshot["tags"]).split(",") if part}

    def _write(self, name, payload, guard):
        if guard is not None and not guard():
            return False
        self.posts.append((name, dict(payload)))
        return True


def _fixture(
    tmp_path,
    *,
    name="SONE-792.mkv",
    size=2_000 * 1024 * 1024,
    remote_size=None,
):
    from qbt_orchestrator.bot_add_queue import BotAddQueueRepository
    from qbt_orchestrator.checked_add import CheckedAddService, DuplicateMatcher, RemoteMediaIndex

    db = tmp_path / "state.sqlite"
    migrate(db)
    clock = Clock()
    queue = BotAddQueueRepository(db, now=clock)
    batch = queue.open_draft("123", "123")
    magnet = "magnet:?" + "xt=urn:btih:" + "a" * 40
    queue.append_message(batch["id"], 1, [magnet])
    queue.submit(batch["id"])
    item = queue.list_items(batch["id"])[0]
    queue.transition_item(item["id"], {"received"}, "resolving", "resolved")
    queue.transition_item(item["id"], {"resolving"}, "waiting_probe_slot", "probe")
    lease = queue.claim_metadata_lease(item["id"], "probe", clock.value + 30)
    tag = "add-item-" + "b" * 32
    torrent_hash = parse_qs(urlsplit(magnet).query)["xt"][0].split(":")[-1]
    queue.transition_item(
        item["id"], {"waiting_probe_slot"}, "metadata_wait", "started",
        {"qbt_hash": torrent_hash, "qbt_precheck_tag": tag},
        metadata_lease_owner="probe",
        metadata_lease_generation=lease["metadata_lease_generation"],
    )
    queue.transition_item(
        item["id"],
        {"metadata_wait"},
        "prechecking",
        "ready",
        metadata_lease_owner="probe",
        metadata_lease_generation=lease["metadata_lease_generation"],
    )
    queue.release_metadata_lease(
        item["id"], "probe", lease["metadata_lease_generation"]
    )
    if remote_size is not None:
        RemoteMediaIndex(db, backfill_db=None, now=clock).replace_rows(
            [{"video_path": "gcrypt:/SONE-792.mkv", "normalized_id": "SONE-792", "size": remote_size}]
        )
    gateway = FakeGateway(torrent_hash, tag, name, size)
    service = CheckedAddService(
        queue,
        gateway,
        DuplicateMatcher(db, normalizer=Normalizer()),
        owner="checked-worker",
        now=clock,
    )
    return queue, gateway, service, item["id"]


def test_unique_prechecked_item_is_enrolled_stopped_without_direct_start(tmp_path):
    queue, gateway, service, item_id = _fixture(tmp_path)

    result = service.tick()

    item = queue.get_item(item_id)
    assert result["enrolled"] == [item_id]
    assert item["state"] == "enrolled"
    assert gateway.snapshot["category"] == "auto"
    assert "checked" in gateway._tags()
    assert not {"precheck", "metadata-probe", "hold"} & gateway._tags()
    assert gateway.files[0]["priority"] == 1
    assert gateway.snapshot["dl_limit"] == 0
    assert any(name == "download_limit" for name, _ in gateway.posts)
    assert not any(name == "start" for name, _ in gateway.posts)


def test_unique_enrollment_keeps_hold_until_database_is_enrolled(tmp_path, monkeypatch):
    queue, gateway, service, item_id = _fixture(tmp_path)
    original = queue.transition_item

    def observe_commit(item, expected, new_state, *args, **kwargs):
        if new_state == "enrolled":
            assert "hold" in gateway._tags()
        return original(item, expected, new_state, *args, **kwargs)

    monkeypatch.setattr(queue, "transition_item", observe_commit)

    assert service.tick()["enrolled"] == [item_id]
    assert "hold" not in gateway._tags()


def test_enrollment_zeroes_every_file_then_selects_only_primary_and_verifies(tmp_path):
    queue, gateway, service, item_id = _fixture(tmp_path)
    gateway.files = [
        {"index": 0, "name": "sample.mkv", "size": 200 * 1024**2, "priority": 7},
        {"index": 1, "name": "SONE-792.mkv", "size": 2_000 * 1024**2, "priority": 6},
        {"index": 2, "name": "notes.txt", "size": 1024, "priority": 1},
    ]

    assert service.tick()["enrolled"] == [item_id]

    assert [row["priority"] for row in gateway.files] == [0, 1, 0]
    priority_posts = [payload for name, payload in gateway.posts if name == "file_priority"]
    assert priority_posts[:2] == [
        {"hash": "a" * 40, "id": "0|1|2", "priority": "0"},
        {"hash": "a" * 40, "id": "1", "priority": "1"},
    ]


def test_priority_write_failure_leaves_enrolling_held_and_retry_recovers(tmp_path):
    queue, gateway, service, item_id = _fixture(tmp_path)
    gateway.fail_next_priority_write = True

    first = service.tick()
    assert first["errors"] == 0
    assert first["enrolled"] == []
    item = queue.get_item(item_id)
    assert item["state"] == "enrolling"
    assert item["attempts"] == 1
    assert item["last_error"] == "qbt_write_fenced"
    assert "hold" in gateway._tags()

    service.now.value = int(item["next_run_at"])
    assert service.tick()["enrolled"] == [item_id]
    assert gateway.files[0]["priority"] == 1


def test_batch_cancel_rejects_active_enrollment_without_mutating_lease_or_marker(tmp_path):
    queue, gateway, service, item_id = _fixture(tmp_path)
    gateway.fail_next_priority_write = True
    assert service.tick()["enrolled"] == []
    before = queue.get_item(item_id)

    with pytest.raises(ValueError, match="^batch_requires_guarded_cancel$"):
        queue.cancel_batch(before["batch_id"], "123")

    after = queue.get_item(item_id)
    assert after == before
    service.now.value = int(after["next_run_at"])
    assert service.tick()["enrolled"] == [item_id]
    assert gateway.removed is False


def test_unique_finalization_failure_leaves_enrolled_held_then_tick_reconciles(tmp_path):
    queue, gateway, service, item_id = _fixture(tmp_path)
    gateway.fail_remove_tag = "hold"

    assert service.tick()["enrolled"] == [item_id]
    stored = queue.get_item(item_id)
    assert stored["state"] == "enrolled"
    assert stored["qbt_precheck_tag"]
    assert "hold" in gateway._tags()

    reconciled = service.tick()

    assert reconciled["finalized"] == [item_id]
    assert queue.get_item(item_id)["qbt_precheck_tag"] is None
    assert "hold" not in gateway._tags()


def test_different_remote_size_requires_confirmation_then_approval_stays_held(tmp_path):
    queue, gateway, service, item_id = _fixture(
        tmp_path, size=2_000 * 1024**2, remote_size=1_000 * 1024**2
    )
    service.tick()
    pending = queue.get_item(item_id)
    assert pending["state"] == "needs_confirmation"

    approved = service.approve_hold(item_id, "123", pending["approval_generation"])

    assert approved["state"] == "enrolled_hold"
    assert approved["approved_by"] == "123"
    assert gateway.snapshot["category"] == "auto"
    assert {"hold", "checked", "maybe-duplicate"} <= gateway._tags()
    assert "precheck" not in gateway._tags()
    assert not any(name == "start" for name, _ in gateway.posts)


def test_cancel_needs_confirmation_removes_only_registration_and_replay_is_idempotent(tmp_path):
    queue, gateway, service, item_id = _fixture(
        tmp_path, size=2_000 * 1024**2, remote_size=1_000 * 1024**2
    )
    service.tick()
    pending = queue.get_item(item_id)

    first = service.cancel(item_id, "123", pending["approval_generation"])
    second = service.cancel(item_id, "123", pending["approval_generation"])

    assert first["state"] == second["state"] == "cancelled"
    assert gateway.posts[-1] == ("delete", {"hashes": "a" * 40, "deleteFiles": "false"})
    assert sum(name == "delete" for name, _ in gateway.posts) == 1


def test_batch_cancel_rejects_owned_confirmation_but_individual_cancel_cleans_qbt(tmp_path):
    queue, gateway, service, item_id = _fixture(
        tmp_path, size=2_000 * 1024**2, remote_size=1_000 * 1024**2
    )
    service.tick()
    pending = queue.get_item(item_id)

    with pytest.raises(ValueError, match="^batch_requires_guarded_cancel$"):
        queue.cancel_batch(pending["batch_id"], "123")
    assert queue.get_item(item_id)["state"] == "needs_confirmation"

    cancelled = service.cancel(
        item_id, "123", pending["approval_generation"]
    )
    assert cancelled["state"] == "cancelled"
    assert gateway.posts[-1] == (
        "delete",
        {"hashes": "a" * 40, "deleteFiles": "false"},
    )


def test_batch_cancel_still_accepts_queued_items_without_qbt_registration(tmp_path):
    from qbt_orchestrator.bot_add_queue import BotAddQueueRepository

    db = tmp_path / "queued.sqlite"
    migrate(db)
    queue = BotAddQueueRepository(db)
    batch = queue.open_draft("123", "123")
    queue.append_message(
        batch["id"], 1, ["magnet:?" + "xt=urn:btih:" + "c" * 40]
    )
    queue.submit(batch["id"])

    cancelled = queue.cancel_batch(batch["id"], "123")

    assert cancelled["state"] == "cancelled"
    assert queue.list_items(batch["id"])[0]["state"] == "cancelled"


def test_allow_scheduling_only_removes_hold_and_is_idempotent(tmp_path):
    queue, gateway, service, item_id = _fixture(
        tmp_path, size=2_000 * 1024**2, remote_size=1_000 * 1024**2
    )
    service.tick()
    pending = queue.get_item(item_id)
    held = service.approve_hold(item_id, "123", pending["approval_generation"])
    before = len(gateway.posts)

    first = service.allow_scheduling(item_id, "123", held["approval_generation"])
    second = service.allow_scheduling(item_id, "123", held["approval_generation"])

    assert first["state"] == second["state"] == "enrolled"
    assert "hold" not in gateway._tags()
    assert gateway.posts[before:] == [("remove_tags", {"hashes": "a" * 40, "tags": "hold"})]


def test_allow_scheduling_commits_enrolled_before_removing_hold(tmp_path, monkeypatch):
    queue, gateway, service, item_id = _fixture(
        tmp_path, size=2_000 * 1024**2, remote_size=1_000 * 1024**2
    )
    service.tick()
    pending = queue.get_item(item_id)
    held = service.approve_hold(item_id, "123", pending["approval_generation"])
    original = queue.transition_item

    def observe_commit(item, expected, new_state, *args, **kwargs):
        if expected == {"enrolled_hold"} and new_state == "enrolled":
            assert "hold" in gateway._tags()
        return original(item, expected, new_state, *args, **kwargs)

    monkeypatch.setattr(queue, "transition_item", observe_commit)

    result = service.allow_scheduling(
        item_id, "123", held["approval_generation"]
    )
    assert result["state"] == "enrolled"
    assert "hold" not in gateway._tags()


def test_allow_scheduling_remove_failure_is_safe_and_same_callback_retries(tmp_path):
    queue, gateway, service, item_id = _fixture(
        tmp_path, size=2_000 * 1024**2, remote_size=1_000 * 1024**2
    )
    service.tick()
    pending = queue.get_item(item_id)
    held = service.approve_hold(item_id, "123", pending["approval_generation"])
    gateway.fail_remove_tag = "hold"

    with pytest.raises(ValueError, match="qbt_write_fenced"):
        service.allow_scheduling(item_id, "123", held["approval_generation"])
    assert queue.get_item(item_id)["state"] == "enrolled"
    assert "hold" in gateway._tags()

    retried = service.allow_scheduling(
        item_id, "123", held["approval_generation"]
    )
    assert retried["state"] == "enrolled"
    assert "hold" not in gateway._tags()


def test_stale_approval_token_has_no_qbt_side_effect(tmp_path):
    queue, gateway, service, item_id = _fixture(
        tmp_path, size=2_000 * 1024**2, remote_size=1_000 * 1024**2
    )
    service.tick()
    pending = queue.get_item(item_id)
    before = list(gateway.posts)

    with pytest.raises(ValueError, match="approval_generation_conflict"):
        service.approve_hold(item_id, "123", pending["approval_generation"] + 1)

    assert gateway.posts == before


def test_foreign_or_missing_opaque_tag_fails_closed_without_writes(tmp_path):
    queue, gateway, service, item_id = _fixture(tmp_path)
    gateway.snapshot["tags"] = "precheck,hold"

    result = service.tick()

    assert result["errors"] == 1
    assert queue.get_item(item_id)["state"] == "prechecking"
    assert gateway.posts == []


def test_exact_remote_duplicate_cleanup_is_delete_files_false(tmp_path):
    queue, gateway, service, item_id = _fixture(
        tmp_path,
        size=1_000 * 1024**2,
        remote_size=1_050 * 1024**2,
    )

    result = service.tick()

    assert result["duplicates"] == [item_id]
    assert queue.get_item(item_id)["state"] == "duplicate_remote"
    assert gateway.posts == [
        ("delete", {"hashes": "a" * 40, "deleteFiles": "false"})
    ]


def test_duplicate_cleanup_response_then_db_failure_recovers_without_second_delete(
    tmp_path, monkeypatch
):
    queue, gateway, service, item_id = _fixture(
        tmp_path,
        size=1_000 * 1024**2,
        remote_size=1_050 * 1024**2,
    )
    original = queue.transition_item
    failed = False

    def fail_once(item, expected, new_state, *args, **kwargs):
        nonlocal failed
        if new_state == "duplicate_remote" and not failed:
            failed = True
            raise RuntimeError("simulated db interruption")
        return original(item, expected, new_state, *args, **kwargs)

    monkeypatch.setattr(queue, "transition_item", fail_once)
    assert service.tick()["errors"] == 1
    assert queue.get_item(item_id)["state"] == "prechecking"
    assert gateway.removed is True

    recovered = service.tick()

    assert recovered["duplicates"] == [item_id]
    assert queue.get_item(item_id)["state"] == "duplicate_remote"
    assert sum(name == "delete" for name, _ in gateway.posts) == 1


def test_enrollment_qbt_writes_then_db_failure_reconciles_without_start(
    tmp_path, monkeypatch
):
    queue, gateway, service, item_id = _fixture(tmp_path)
    original = queue.transition_item
    failed = False

    def fail_once(item, expected, new_state, *args, **kwargs):
        nonlocal failed
        if new_state == "enrolled" and not failed:
            failed = True
            raise RuntimeError("simulated final commit interruption")
        return original(item, expected, new_state, *args, **kwargs)

    monkeypatch.setattr(queue, "transition_item", fail_once)
    first = service.tick()
    assert first["errors"] == 1
    assert queue.get_item(item_id)["state"] == "enrolling"
    assert "add-item-" in gateway.snapshot["tags"]

    # Enrollment writes already converged; next tick only needs DB commit.
    recovered = service.tick()

    assert recovered["enrolled"] == [item_id]
    assert queue.get_item(item_id)["state"] == "enrolled"
    assert not any(name == "start" for name, _ in gateway.posts)


def test_expired_raw_input_does_not_block_owned_precheck_validation(tmp_path):
    from qbt_orchestrator.db import write_transaction

    queue, _gateway, service, item_id = _fixture(tmp_path)
    write_transaction(
        queue.state_db,
        lambda con: con.execute(
            "update bot_add_items set raw_input=null,raw_input_expires_at=null where id=?",
            (item_id,),
        ),
    )

    assert service.tick()["enrolled"] == [item_id]


def test_confirmation_notification_is_deduplicated_by_item_generation(tmp_path):
    import json

    from qbt_orchestrator.runtime import BotNotificationRepository

    queue, _gateway, service, item_id = _fixture(
        tmp_path,
        size=2_000 * 1024**2,
        remote_size=1_000 * 1024**2,
    )
    notifications = BotNotificationRepository(queue.state_db, now=service.now)
    service.notifications = notifications

    assert service.tick()["confirmations"] == [item_id]
    assert service.tick()["confirmations"] == []
    rows = notifications.list_all()
    assert len(rows) == 1
    assert rows[0]["dedupe_key"] == "checked-add-confirm:1:1"
    payload = json.loads(rows[0]["payload_json"])
    buttons = [
        btn["callback_data"]
        for row in payload["reply_markup"]["inline_keyboard"]
        for btn in row
    ]
    assert f"i:y:{item_id}:1" in buttons
    assert f"i:x:{item_id}:1" in buttons


def test_daemon_checked_add_hook_uses_current_sync_health(tmp_path):
    from qbt_orchestrator.service import DaemonRuntime

    class Qbt:
        def get_maindata(self, rid):
            return {"rid": rid + 1, "full_update": True, "torrents": {}, "server_state": {}}

    class Executor:
        def qbt_post(self, _path, _payload):
            return True

    class Checked:
        def __init__(self):
            self.calls = []

        def tick(self, *, sync_healthy):
            self.calls.append(sync_healthy)
            return {"suspended": not sync_healthy}

    checked = Checked()
    daemon = DaemonRuntime(
        state_db=tmp_path / "runtime.sqlite",
        qbt=Qbt(),
        executor=Executor(),
        free_bytes_provider=lambda: 10 * 1024**3,
        dry_run=False,
        carousel_enabled=False,
        checked_add_service=checked,
    )
    daemon.tick_safety()

    assert daemon.checked_add_tick() == {"suspended": False}
    assert checked.calls == [True]
    assert "checked_add" in {task.name for task in daemon.loop_tasks}


@pytest.mark.parametrize(
    ("flag", "dry_run", "expected"),
    [(None, False, False), ("1", False, True), ("1", True, False)],
)
def test_cli_checked_add_flag_defaults_off_and_never_builds_in_dry_run(
    tmp_path, monkeypatch, flag, dry_run, expected
):
    from qbt_orchestrator.cli import _build_runtime

    for name, value in {
        "QBT_ORCH_STATE_DB": str(tmp_path / "cli.sqlite"),
        "QBT_ORCH_DRY_RUN": "1" if dry_run else "0",
        "QBT_ORCH_DISK_PATH": str(tmp_path),
        "QBT_ORCH_ORPHAN_JANITOR": "0",
        "QBT_ORCH_JUNK_JANITOR": "0",
        "QBT_ORCH_CAROUSEL": "0",
        "QBT_ORCH_QBT_PREFERENCES_GUARD": "0",
        "QBT_ORCH_PATH_RECONCILE": "0",
        "QBT_ORCH_SOAK_ENABLED": "0",
        "QBT_ORCH_METADATA_PROBE_ENABLED": "0",
        "QBT_ORCH_CHECKED_ADD_ENABLED": flag,
        "QBT_ORCH_FILENAME_NORMALIZE": "0",
    }.items():
        if value is None:
            monkeypatch.delenv(name, raising=False)
        else:
            monkeypatch.setenv(name, value)

    class Ns:
        config = None
        cmd = "daemon"
        safety_interval = 0
        max_safety_ticks = 1

        def __init__(self):
            self.dry_run = dry_run

    runtime, _ = _build_runtime(Ns(), tmp_path / "fallback.sqlite")
    assert (runtime.checked_add_service is not None) is expected


def test_real_precheck_gateway_enrollment_writes_use_existing_executor():
    from qbt_orchestrator.qbt_precheck import QbtPrecheckGateway

    class Qbt:
        pass

    class Executor:
        def __init__(self):
            self.posts = []

        def qbt_post(self, path, payload):
            self.posts.append((path, payload))
            return True

    executor = Executor()
    gateway = QbtPrecheckGateway(Qbt(), executor)
    h = "a" * 40
    gateway.set_category(h, "auto")
    gateway.add_tags(h, "checked,maybe-duplicate")
    gateway.remove_tags(h, "precheck,metadata-probe")
    gateway.set_force_start(h, False)

    assert executor.posts == [
        ("/api/v2/torrents/setCategory", {"hashes": h, "category": "auto"}),
        ("/api/v2/torrents/addTags", {"hashes": h, "tags": "checked,maybe-duplicate"}),
        ("/api/v2/torrents/removeTags", {"hashes": h, "tags": "precheck,metadata-probe"}),
        ("/api/v2/torrents/setForceStart", {"hashes": h, "value": "false"}),
    ]


def test_transient_add_tag_is_a_hard_ownership_fence():
    from qbt_orchestrator.torrent_ownership import has_transient_add_fence

    assert has_transient_add_fence({"tags": "checked,add-item-" + "a" * 32})
    assert not has_transient_add_fence({"tags": "checked,add-item-user-label"})


def test_only_current_32_hex_tags_are_gc_eligible():
    from qbt_orchestrator.torrent_ownership import is_gc_eligible_add_tag

    assert is_gc_eligible_add_tag("add-item-" + "a" * 32)
    assert not is_gc_eligible_add_tag("add-item-" + "a" * 16)
    assert not is_gc_eligible_add_tag("add-item-" + "a" * 64)


def test_precheck_gateway_delete_tags_is_guarded_and_gc_eligible_only():
    from qbt_orchestrator.qbt_precheck import QbtPrecheckGateway

    class Qbt:
        def torrent_tags(self):
            return ["add-item-" + "a" * 32, "checked", "add-item-" + "b" * 16]

        def torrents_by_tag(self, tag):
            return []

    class Executor:
        def __init__(self):
            self.posts = []

        def qbt_post_guarded(self, path, payload, *, guard):
            assert guard() is True
            self.posts.append((path, payload))
            return True

    tag = "add-item-" + "a" * 32
    executor = Executor()
    gateway = QbtPrecheckGateway(Qbt(), executor)
    assert gateway.list_tags() == {tag, "checked", "add-item-" + "b" * 16}
    assert gateway.torrents_by_tag(tag) == []
    assert gateway.delete_tags([tag, tag], guard=lambda: True) is True
    assert executor.posts == [
        ("/api/v2/torrents/deleteTags", {"tags": tag}),
    ]
    try:
        gateway.delete_tags(["checked"])
        raise AssertionError("expected ValueError")
    except ValueError as exc:
        assert str(exc) == "qbt_precheck_tag"
    try:
        gateway.delete_tags(["add-item-" + "b" * 16])
        raise AssertionError("expected ValueError")
    except ValueError as exc:
        assert str(exc) == "qbt_precheck_tag"


def test_enrolling_running_auto_recovers_without_start(tmp_path):
    queue, gateway, service, item_id = _fixture(tmp_path)
    opaque_tag = "add-item-" + "b" * 32
    queue.transition_item(
        item_id,
        {"prechecking"},
        "ready",
        "manual",
        {
            "decision": "unique",
            "decision_reason": "unique",
            "normalized_media_id": "SONE-792",
            "primary_video_size": gateway.files[0]["size"],
            "display_name": "SONE-792.mkv",
        },
    )
    queue.transition_item(item_id, {"ready"}, "enrolling", "automatic_enrollment")
    gateway.snapshot.update(
        {
            "state": "downloading",
            "category": "auto",
            "tags": f"checked,hold,{opaque_tag}",
            "force_start": False,
        }
    )
    gateway.files[0]["priority"] = 1

    result = service.tick()

    item = queue.get_item(item_id)
    assert result["enrolled"] == [item_id]
    assert item["state"] == "enrolled"
    assert item["qbt_precheck_tag"] is None
    assert gateway.snapshot["state"] == "stoppedDL"
    assert gateway.snapshot["force_start"] is False
    assert gateway.snapshot["category"] == "auto"
    assert gateway.files[0]["priority"] == 1
    assert opaque_tag not in gateway._tags()
    assert "hold" not in gateway._tags()
    assert "checked" in gateway._tags()
    assert not any(name == "start" for name, _ in gateway.posts)


def test_enrollment_replay_after_each_injected_failure(tmp_path):
    queue, gateway, service, item_id = _fixture(tmp_path)
    torrent_hash = gateway.torrent_hash
    fail_names = [
        "stop",
        "file_priority",
        "file_priority",
        "add_tags",
        "remove_tags",
        "force",
        "category",
    ]
    original_write = gateway._write
    calls = {"n": 0}

    def flaky_write(name, payload, guard):
        # Fail once on the first occurrence of each target write name in order.
        if calls["n"] < len(fail_names) and name == fail_names[calls["n"]]:
            calls["n"] += 1
            if guard is not None and not guard():
                return False
            gateway.posts.append((f"fail_{name}", dict(payload)))
            return False
        return original_write(name, payload, guard)

    gateway._write = flaky_write

    # Drive through each injected failure with backoff advances.
    for _ in range(len(fail_names)):
        result = service.tick()
        item = queue.get_item(item_id)
        assert item["state"] == "enrolling"
        assert result["enrolled"] == []
        assert item["next_run_at"] is not None
        service.now.value = int(item["next_run_at"])

    gateway._write = original_write
    final = service.tick()
    item = queue.get_item(item_id)
    assert final["enrolled"] == [item_id]
    assert item["state"] == "enrolled"
    assert item["qbt_hash"] == torrent_hash
    assert item["qbt_precheck_tag"] is None
    assert sum(name == "delete" for name, _ in gateway.posts) == 0
    assert not any(name == "start" for name, _ in gateway.posts)


def test_enrollment_stuck_warning_after_three_failures(tmp_path):
    from qbt_orchestrator.warning_inbox import WarningInboxRepository, WarningService

    queue, gateway, service, item_id = _fixture(tmp_path)
    warnings = WarningService(queue.state_db, now=service.now)
    service.warning_service = warnings
    gateway.fail_next_priority_write = True

    for _ in range(3):
        gateway.fail_next_priority_write = True
        service.tick()
        item = queue.get_item(item_id)
        service.now.value = int(item["next_run_at"])

    inbox = WarningInboxRepository(queue.state_db, now=service.now)
    rows = inbox.list_unread(limit=20)
    keys = {row["warning_key"] for row in rows}
    assert f"checked_add:enrollment_stuck:{item_id}" in keys

    gateway.fail_next_priority_write = False
    service.tick()
    rows = inbox.list_unread(limit=20)
    keys = {row["warning_key"] for row in rows}
    assert f"checked_add:enrollment_stuck:{item_id}" not in keys
