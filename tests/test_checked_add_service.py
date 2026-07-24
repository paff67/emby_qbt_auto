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
        }
        self.files = [{"index": 0, "name": name, "size": size, "priority": 0}]
        self.posts: list[tuple[str, dict]] = []
        self.removed = False

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
        return self._write("stop", {"hashes": torrent_hash}, guard)

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

    def set_file_priorities(self, torrent_hash, indices, priority, *, guard=None):
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
    assert not any(name == "start" for name, _ in gateway.posts)


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
    assert service.tick()["errors"] == 1
    assert queue.get_item(item_id)["state"] == "enrolling"
    assert "add-item-" in gateway.snapshot["tags"]

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
