from __future__ import annotations

import json

import pytest

from qbt_orchestrator.db import migrate, readonly_connect
from qbt_orchestrator.manual_media_delete import (
    ManualMediaDeleteService,
    RcloneDriveTrashAdapter,
    _exact_remote_dir_for_id,
)
from qbt_orchestrator.processed_media import ProcessedMediaRepository


class FakeDrive:
    def __init__(self, trash_remote: str = "trash:"):
        self.trash_remote = trash_remote.rstrip(":") + ":"
        self.calls: list[tuple[str, str]] = []
        self.sources: set[str] = set()
        self.targets: set[str] = set()
        self.fail_move = False

    def planned_trash_path(self, remote_dir: str) -> str:
        basename = remote_dir.rstrip("/").split(":")[-1].rstrip("/").split("/")[-1]
        return f"{self.trash_remote}{basename}"

    def trash_exact_directory(self, remote_dir: str, expected_trash_path: str) -> str:
        source = remote_dir.rstrip("/")
        target = str(expected_trash_path).strip()
        self.calls.append((source, target))
        source_exists = source in self.sources
        target_exists = target in self.targets
        if self.fail_move:
            raise RuntimeError("drive_unavailable")
        if source_exists and not target_exists:
            self.sources.discard(source)
            self.targets.add(target)
        elif (not source_exists) and target_exists:
            pass
        else:
            raise RuntimeError("remote_trash_conflict")
        return target


class FakeMount:
    def __init__(self, present: bool = False):
        self.present = present

    def path_exists(self, host_or_mount_path: str) -> bool:
        return self.present


class FakeEmby:
    def __init__(self):
        self.calls: list[tuple[str, str]] = []

    def refresh_normalized_id(self, normalized_id: str, media_path: str) -> None:
        self.calls.append((normalized_id, media_path))


def test_exact_remote_dir_requires_normalized_id_basename():
    assert _exact_remote_dir_for_id("BBAN-574", "gcrypt:/BBAN-574/a.mp4") == "gcrypt:/BBAN-574"
    with pytest.raises(ValueError):
        _exact_remote_dir_for_id("BBAN-574", "gcrypt:/other/BBAN-574-extra")


def test_manual_delete_saga_keeps_permanent_block(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    repo = ProcessedMediaRepository(db, now=lambda: 10, enforce=True)
    repo.observe("BBAN-574", origin="test")
    drive = FakeDrive()
    drive.sources.add("gcrypt:/BBAN-574")
    emby = FakeEmby()
    service = ManualMediaDeleteService(
        db,
        drive_trasher=drive,
        mount_checker=FakeMount(present=False),
        emby_cleaner=emby,
        processed_media=repo,
        now=lambda: 20,
    )
    result = service.delete(
        "BBAN-574",
        actor_id="ops",
        reason="audit",
        remote_path="gcrypt:/BBAN-574/a.mp4",
    )
    assert result.ok
    assert result.stage == "manual_deleted"
    assert result.download_policy == "block_permanent"
    assert drive.calls == [("gcrypt:/BBAN-574", "trash:BBAN-574")]
    assert emby.calls
    assert repo.is_permanently_blocked("BBAN-574") is not None
    history = repo.history("BBAN-574", limit=20)
    assert any(item["event_type"] == "manual_deleted" for item in history)
    assert any(
        item["event_type"] == "manual_delete_stage"
        and json.loads(item["payload_json"] or "{}").get("stage") == "manual_deleted"
        for item in history
    )


def test_manual_delete_requires_adapters():
    with pytest.raises(TypeError):
        ManualMediaDeleteService("/tmp/x.sqlite")  # type: ignore[call-arg]


def test_manual_delete_failure_keeps_block(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    repo = ProcessedMediaRepository(db, now=lambda: 10, enforce=True)
    repo.mark_ingested("BBAN-576", origin="test")

    drive = FakeDrive()
    drive.sources.add("gcrypt:/BBAN-576")
    drive.fail_move = True
    service = ManualMediaDeleteService(
        db,
        drive_trasher=drive,
        mount_checker=FakeMount(present=False),
        emby_cleaner=FakeEmby(),
        processed_media=repo,
        now=lambda: 20,
    )
    result = service.delete(
        "BBAN-576",
        actor_id="ops",
        reason="audit",
        remote_path="gcrypt:/BBAN-576",
    )
    assert not result.ok
    assert result.download_policy == "block_permanent"
    assert result.lifecycle_state == "manual_delete_failed"


def test_register_tombstone_honors_deleted_at(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    repo = ProcessedMediaRepository(db, now=lambda: 9999999999, enforce=True)
    row = repo.register_tombstone(
        "BBAN-582",
        deleted_at=123,
        actor_id="ops",
        reason="audit",
        create_from_audit=True,
    )
    assert int(row["manually_deleted_at"]) == 123
    history = repo.history("BBAN-582", limit=10)
    deleted_events = [item for item in history if item["event_type"] == "manual_deleted"]
    assert deleted_events
    assert int(deleted_events[0]["event_at"]) == 123


def test_manual_delete_persists_trash_path_before_move_and_resumes(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    repo = ProcessedMediaRepository(db, now=lambda: 10, enforce=True)
    repo.observe("BBAN-590", origin="test")
    drive = FakeDrive()
    drive.sources.add("gcrypt:/BBAN-590")

    class CrashAfterMove(FakeDrive):
        def __init__(self, inner: FakeDrive):
            self.inner = inner
            self.calls = inner.calls
            self.sources = inner.sources
            self.targets = inner.targets

        def planned_trash_path(self, remote_dir: str) -> str:
            return self.inner.planned_trash_path(remote_dir)

        def trash_exact_directory(self, remote_dir: str, expected_trash_path: str) -> str:
            target = self.inner.trash_exact_directory(remote_dir, expected_trash_path)
            raise RuntimeError("crash_after_move")

    crashing = CrashAfterMove(drive)
    service = ManualMediaDeleteService(
        db,
        drive_trasher=crashing,
        mount_checker=FakeMount(present=False),
        emby_cleaner=FakeEmby(),
        processed_media=repo,
        now=lambda: 20,
    )
    failed = service.delete(
        "BBAN-590",
        actor_id="ops",
        reason="audit",
        remote_path="gcrypt:/BBAN-590",
    )
    assert not failed.ok
    con = readonly_connect(db)
    try:
        row = con.execute(
            "select payload_json from processed_media_events "
            "where event_type='manual_delete_stage' order by id desc limit 1"
        ).fetchone()
    finally:
        con.close()
    payload = json.loads(row["payload_json"])
    assert payload["stage"] == "pending"
    assert payload["trash_path"] == "trash:BBAN-590"
    assert "trash:BBAN-590" in drive.targets
    assert "gcrypt:/BBAN-590" not in drive.sources

    service2 = ManualMediaDeleteService(
        db,
        drive_trasher=drive,
        mount_checker=FakeMount(present=False),
        emby_cleaner=FakeEmby(),
        processed_media=repo,
        now=lambda: 30,
    )
    result = service2.delete(
        "BBAN-590",
        actor_id="ops",
        reason="audit",
        remote_path="gcrypt:/BBAN-590",
    )
    assert result.ok
    assert result.stage == "manual_deleted"
    con = readonly_connect(db)
    try:
        final = con.execute(
            "select payload_json from processed_media_events "
            "where event_type='manual_delete_stage' order by id desc limit 1"
        ).fetchone()
    finally:
        con.close()
    assert json.loads(final["payload_json"])["trash_path"] == "trash:BBAN-590"


def test_resume_uses_persisted_trash_path_not_current_config(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    repo = ProcessedMediaRepository(db, now=lambda: 10, enforce=True)
    repo.observe("ABCD-1", origin="test")
    old = FakeDrive(trash_remote="old-trash:")
    old.sources.add("gcrypt:/ABCD-1")

    class CrashAfterMove(FakeDrive):
        def __init__(self, inner: FakeDrive):
            self.inner = inner
            self.calls = inner.calls
            self.sources = inner.sources
            self.targets = inner.targets
            self.trash_remote = inner.trash_remote

        def planned_trash_path(self, remote_dir: str) -> str:
            return self.inner.planned_trash_path(remote_dir)

        def trash_exact_directory(self, remote_dir: str, expected_trash_path: str) -> str:
            target = self.inner.trash_exact_directory(remote_dir, expected_trash_path)
            raise RuntimeError("crash_after_move")

    service = ManualMediaDeleteService(
        db,
        drive_trasher=CrashAfterMove(old),
        mount_checker=FakeMount(present=False),
        emby_cleaner=FakeEmby(),
        processed_media=repo,
        now=lambda: 20,
    )
    assert not service.delete(
        "ABCD-1",
        actor_id="ops",
        reason="audit",
        remote_path="gcrypt:/ABCD-1",
    ).ok
    assert old.calls == [("gcrypt:/ABCD-1", "old-trash:ABCD-1")]

    # Restart with a different trash_remote config; resume must keep old target.
    new_drive = FakeDrive(trash_remote="new-trash:")
    new_drive.sources = old.sources
    new_drive.targets = old.targets
    resumed = ManualMediaDeleteService(
        db,
        drive_trasher=new_drive,
        mount_checker=FakeMount(present=False),
        emby_cleaner=FakeEmby(),
        processed_media=repo,
        now=lambda: 30,
    )
    result = resumed.delete(
        "ABCD-1",
        actor_id="ops",
        reason="audit",
        remote_path="gcrypt:/ABCD-1",
    )
    assert result.ok
    assert new_drive.calls == [("gcrypt:/ABCD-1", "old-trash:ABCD-1")]
    assert all(target != "new-trash:ABCD-1" for _, target in new_drive.calls)
    assert "new-trash:ABCD-1" not in new_drive.targets


def test_rclone_adapter_honors_expected_trash_path():
    class FakeRclone:
        def __init__(self):
            self.paths = {"old-trash:BBAN-591": {"Name": "BBAN-591"}}
            self.moves = []
            self.stats = []

        def stat(self, remote: str):
            self.stats.append(remote)
            return self.paths.get(remote)

        def moveto(self, source, target):
            self.moves.append((source, target))
            self.paths.pop(source, None)
            self.paths[target] = {"Name": "x"}

    rclone = FakeRclone()
    adapter = RcloneDriveTrashAdapter(rclone, trash_remote="new-trash:")
    assert adapter.planned_trash_path("gcrypt:/BBAN-591") == "new-trash:BBAN-591"
    assert (
        adapter.trash_exact_directory("gcrypt:/BBAN-591", "old-trash:BBAN-591")
        == "old-trash:BBAN-591"
    )
    assert rclone.moves == []
    assert "old-trash:BBAN-591" in rclone.stats
    assert "new-trash:BBAN-591" not in rclone.stats


def test_post_finalize_exception_does_not_downgrade_summary(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    repo = ProcessedMediaRepository(db, now=lambda: 10, enforce=True)
    repo.observe("BBAN-601", origin="test")
    drive = FakeDrive()
    drive.sources.add("gcrypt:/BBAN-601")

    class FinalizeThenBoom(ProcessedMediaRepository):
        def finalize_manual_delete(self, *args, **kwargs):
            result = super().finalize_manual_delete(*args, **kwargs)
            raise RuntimeError("after_finalize_side_effect")

    boom_repo = FinalizeThenBoom(db, now=lambda: 20, enforce=True)
    service = ManualMediaDeleteService(
        db,
        drive_trasher=drive,
        mount_checker=FakeMount(present=False),
        emby_cleaner=FakeEmby(),
        processed_media=boom_repo,
        now=lambda: 20,
    )
    result = service.delete(
        "BBAN-601",
        actor_id="ops",
        reason="audit",
        remote_path="gcrypt:/BBAN-601",
    )
    assert result.ok
    assert result.lifecycle_state == "manual_deleted"
    row = repo.get_by_normalized_id("BBAN-601")
    assert row["lifecycle_state"] == "manual_deleted"
    assert row["manually_deleted_at"] is not None


def test_manual_media_delete_cli_accepts_config(monkeypatch, tmp_path):
    from qbt_orchestrator.cli import main

    db = tmp_path / "state.sqlite"
    migrate(db)
    monkeypatch.setenv("QBT_ORCH_MANUAL_DELETE_ENABLED", "0")
    code = main(
        [
            "manual-media-delete",
            "--state-db",
            str(db),
            "--config",
            "/nonexistent-but-parsed.conf",
            "--id",
            "BBAN-1",
            "--actor",
            "ops",
            "--remote-path",
            "gcrypt:/BBAN-1",
            "--json",
        ]
    )
    assert code == 2
