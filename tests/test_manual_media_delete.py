from __future__ import annotations

import pytest

from qbt_orchestrator.db import migrate
from qbt_orchestrator.manual_media_delete import (
    ManualMediaDeleteService,
    _exact_remote_dir_for_id,
)
from qbt_orchestrator.processed_media import ProcessedMediaRepository


class FakeDrive:
    def __init__(self):
        self.calls: list[str] = []

    def trash_exact_directory(self, remote_dir: str) -> str:
        self.calls.append(remote_dir)
        return f"trash:{remote_dir.split(':', 1)[-1]}"


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
    assert drive.calls == ["gcrypt:/BBAN-574"]
    assert emby.calls
    assert repo.is_permanently_blocked("BBAN-574") is not None


def test_manual_delete_requires_adapters():
    with pytest.raises(TypeError):
        ManualMediaDeleteService("/tmp/x.sqlite")  # type: ignore[call-arg]


def test_manual_delete_failure_keeps_block(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    repo = ProcessedMediaRepository(db, now=lambda: 10, enforce=True)
    repo.mark_ingested("BBAN-576", origin="test")

    class BoomDrive(FakeDrive):
        def trash_exact_directory(self, remote_dir: str) -> str:
            raise RuntimeError("drive_unavailable")

    service = ManualMediaDeleteService(
        db,
        drive_trasher=BoomDrive(),
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
