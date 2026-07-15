from __future__ import annotations

import tempfile
from pathlib import Path


def _enqueue(repo):
    return repo.enqueue(
        upload_job_id=7,
        hash="abc",
        media_group_id=3,
        normalized_id="BBAN-582",
        metadata_title="影片名称",
        display_title="BBAN-582 影片名称",
        source_remote="gcrypt:/ingest/raw.mp4",
        target_remote="gcrypt:/BBAN-582/BBAN-582 影片名称.mp4",
        expected_size=123,
    )


def test_media_promotion_schema_and_idempotent_enqueue():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.promotion import MediaPromotionRepository

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        repo = MediaPromotionRepository(db, now=lambda: 1_000)

        first = _enqueue(repo)
        second = _enqueue(repo)

        assert first == second
        row = repo.get(first)
        assert row["state"] == "planned"
        assert row["expected_hashes_json"] == "{}"
        assert row["created_at"] == 1_000


def test_media_promotion_claim_retry_and_verification_are_durable():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.promotion import MediaPromotionRepository

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        clock = [1_000]
        repo = MediaPromotionRepository(db, now=lambda: clock[0])
        promotion_id = _enqueue(repo)

        claimed = repo.claim_next(owner="worker-a", lease_sec=30)
        assert claimed["id"] == promotion_id
        assert claimed["state"] == "moving"
        assert claimed["attempts"] == 1
        assert claimed["lease_owner"] == "worker-a"

        repo.schedule_retry(promotion_id, "temporary", delay_sec=60)
        assert repo.claim_next(owner="worker-a") is None
        clock[0] = 1_060
        assert repo.claim_next(owner="worker-a")["id"] == promotion_id

        repo.record_verified(
            promotion_id,
            method="path_size",
            details={"verified": True, "mismatches": []},
        )
        final = repo.get(promotion_id)
        assert final["state"] == "verified"
        assert final["verified_at"] == 1_060
        assert final["lease_owner"] is None
        assert repo.pending_for_upload(7) == []


def test_promotion_moves_and_verifies_exact_destination():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.promotion import MediaPromotionRepository, MediaPromotionRunner
    from tests.fakes import FakeRclone

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        repo = MediaPromotionRepository(db, now=lambda: 1_000)
        promotion_id = _enqueue(repo)
        rclone = FakeRclone(remote_sizes={"gcrypt:/ingest/raw.mp4": 123})
        runner = MediaPromotionRunner(repo, rclone)

        assert runner.run_next() == promotion_id
        assert rclone.movetos == [
            (
                "gcrypt:/ingest/raw.mp4",
                "gcrypt:/BBAN-582/BBAN-582 影片名称.mp4",
            )
        ]
        assert repo.get(promotion_id)["state"] == "verified"


def test_promotion_reverses_move_when_destination_verification_fails():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.promotion import MediaPromotionRepository, MediaPromotionRunner
    from tests.fakes import FakeRclone

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        repo = MediaPromotionRepository(db, now=lambda: 1_000)
        promotion_id = _enqueue(repo)
        source = "gcrypt:/ingest/raw.mp4"
        target = "gcrypt:/BBAN-582/BBAN-582 影片名称.mp4"
        rclone = FakeRclone(remote_sizes={source: 123}, moved_size=122)
        runner = MediaPromotionRunner(repo, rclone)

        assert runner.run_next() == promotion_id
        assert rclone.movetos == [(source, target), (target, source)]
        row = repo.get(promotion_id)
        assert row["state"] == "retry_wait"
        assert "destination size mismatch" in row["last_error"]


def test_promotion_never_overwrites_conflicting_destination():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.promotion import MediaPromotionRepository, MediaPromotionRunner
    from tests.fakes import FakeRclone

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        repo = MediaPromotionRepository(db, now=lambda: 1_000)
        promotion_id = _enqueue(repo)
        source = "gcrypt:/ingest/raw.mp4"
        target = "gcrypt:/BBAN-582/BBAN-582 影片名称.mp4"
        rclone = FakeRclone(remote_sizes={source: 123, target: 999})
        runner = MediaPromotionRunner(repo, rclone)

        assert runner.run_next() == promotion_id
        assert rclone.movetos == []
        row = repo.get(promotion_id)
        assert row["state"] == "failed"
        assert row["last_error"] == "target_conflict"
