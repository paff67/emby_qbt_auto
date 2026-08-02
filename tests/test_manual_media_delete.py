from __future__ import annotations

from qbt_orchestrator.db import migrate
from qbt_orchestrator.manual_media_delete import ManualMediaDeleteService
from qbt_orchestrator.processed_media import ProcessedMediaRepository


def test_manual_delete_saga_keeps_permanent_block(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    repo = ProcessedMediaRepository(db, now=lambda: 10, enforce=True)
    repo.observe("BBAN-574", origin="test")
    seen = {"trashed": False, "present": True}

    def trash(_path: str) -> None:
        seen["trashed"] = True
        seen["present"] = False

    service = ManualMediaDeleteService(
        db,
        processed_media=repo,
        drive_trasher=trash,
        mount_checker=lambda _path: seen["present"],
        emby_cleaner=lambda _nid: None,
        now=lambda: 20,
    )
    result = service.delete("BBAN-574", actor_id="ops", reason="audit", remote_path="gcrypt:/BBAN-574")
    assert result.ok
    assert result.download_policy == "block_permanent"
    assert result.lifecycle_state == "manual_deleted"
    assert repo.is_permanently_blocked("BBAN-574") is not None


def test_manual_delete_failure_keeps_block(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    repo = ProcessedMediaRepository(db, now=lambda: 10, enforce=True)
    repo.mark_ingested("BBAN-576", origin="test")

    def boom(_path: str) -> None:
        raise RuntimeError("drive_unavailable")

    service = ManualMediaDeleteService(
        db,
        processed_media=repo,
        drive_trasher=boom,
        now=lambda: 20,
    )
    result = service.delete("BBAN-576", actor_id="ops", reason="audit", remote_path="gcrypt:/x")
    assert not result.ok
    assert result.download_policy == "block_permanent"
    assert result.lifecycle_state == "manual_delete_failed"
