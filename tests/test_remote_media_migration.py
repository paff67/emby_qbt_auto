from __future__ import annotations

import json
from pathlib import Path


class FakeRemote:
    def __init__(self, objects: dict[str, int], *, corrupt_first_move: bool = False):
        self.objects = dict(objects)
        self.movetos: list[tuple[str, str]] = []
        self.corrupt_first_move = corrupt_first_move

    def inventory(self):
        return [
            {"Path": path.split(":/", 1)[1], "Size": size, "Hashes": {}}
            for path, size in sorted(self.objects.items())
        ]

    def stat(self, remote: str):
        size = self.objects.get(remote)
        return None if size is None else {"Path": remote, "Size": size, "Hashes": {}}

    def moveto(self, source: str, target: str):
        self.movetos.append((source, target))
        size = self.objects.pop(source)
        if self.corrupt_first_move and len(self.movetos) == 1:
            size += 1
        self.objects[target] = size


def _inventory():
    return [
        {
            "Path": "BBAN-582.torrent-238df97834d4/489155.com@BBAN-582.mp4",
            "Size": 6_334_240_229,
            "Hashes": {},
        },
        {"Path": "BBAN-582/poster.jpg", "Size": 200, "Hashes": {}},
        {"Path": "misc/unknown.mp4", "Size": 500, "Hashes": {}},
    ]


def test_migration_merges_hash_wrapper_into_existing_id_directory():
    from qbt_orchestrator.remote_migration import build_migration_plan

    plan = build_migration_plan(
        _inventory(), {"BBAN-582": {"title": "影片名称", "confidence": 0.99}}
    )

    move = next(action for action in plan.actions if action.kind == "video")
    assert move.source == (
        "gcrypt:/BBAN-582.torrent-238df97834d4/489155.com@BBAN-582.mp4"
    )
    assert move.target == "gcrypt:/BBAN-582/BBAN-582 影片名称.mp4"
    assert move.expected_size == 6_334_240_229
    assert move.normalized_id == "BBAN-582"


def test_migration_preserves_unmatched_and_conflicting_objects():
    from qbt_orchestrator.remote_migration import build_migration_plan

    inventory = _inventory() + [
        {"Path": "WAAA-614/raw-WAAA-614.mp4", "Size": 100, "Hashes": {}},
        {"Path": "WAAA-614/WAAA-614 Existing.mp4", "Size": 99, "Hashes": {}},
    ]
    plan = build_migration_plan(
        inventory,
        {
            "BBAN-582": {"title": "影片名称", "confidence": 0.99},
            "WAAA-614": {"title": "Existing", "confidence": 0.99},
        },
    )

    reasons = {item.reason for item in plan.review}
    assert "missing_title" in reasons
    assert "target_conflict" in reasons
    assert not any(action.source.endswith("unknown.mp4") for action in plan.actions)
    assert not any(action.normalized_id == "WAAA-614" for action in plan.actions)


def test_second_plan_is_empty_after_apply_and_verify(tmp_path: Path):
    from qbt_orchestrator.remote_migration import apply_migration, build_migration_plan

    source = "gcrypt:/BBAN-582.torrent-238df97834d4/489155.com@BBAN-582.mp4"
    remote = FakeRemote({source: 6_334_240_229})
    titles = {"BBAN-582": {"title": "影片名称", "confidence": 0.99}}
    first = build_migration_plan(remote.inventory(), titles)

    result = apply_migration(first, remote, journal_path=tmp_path / "journal.jsonl")
    second = build_migration_plan(remote.inventory(), titles)

    assert result.verified == 1
    assert result.failed == 0
    assert second.actions == []
    states = [json.loads(line)["state"] for line in (tmp_path / "journal.jsonl").read_text(encoding="utf-8").splitlines()]
    assert states == ["moving", "verified"]


def test_failed_verification_rolls_move_back(tmp_path: Path):
    from qbt_orchestrator.remote_migration import apply_migration, build_migration_plan

    source = "gcrypt:/BBAN-582-hash/BBAN-582.mp4"
    remote = FakeRemote({source: 100}, corrupt_first_move=True)
    plan = build_migration_plan(
        remote.inventory(), {"BBAN-582": {"title": "Title", "confidence": 1.0}}
    )

    result = apply_migration(plan, remote, journal_path=tmp_path / "journal.jsonl")

    assert result.failed == 1
    assert source in remote.objects
    assert remote.objects[source] == 101
    states = [json.loads(line)["state"] for line in (tmp_path / "journal.jsonl").read_text(encoding="utf-8").splitlines()]
    assert states == ["moving", "rollback_wait", "rolled_back", "failed"]


def test_render_canonical_nfo_updates_emby_identity_fields():
    from qbt_orchestrator.remote_migration import render_canonical_nfo

    rendered = render_canonical_nfo(
        b"<movie><title>Old</title><plot>Keep me</plot></movie>",
        "BBAN-582",
        "影片名称",
    ).decode("utf-8")

    assert "<title>BBAN-582 影片名称</title>" in rendered
    assert "<originaltitle>影片名称</originaltitle>" in rendered
    assert "<id>BBAN-582</id>" in rendered
    assert "<sorttitle>BBAN-582</sorttitle>" in rendered
    assert "<plot>Keep me</plot>" in rendered


def test_reconcile_verified_migration_updates_only_matching_upload(tmp_path: Path):
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.remote_migration import (
        build_migration_plan,
        reconcile_verified_migrations,
    )
    from qbt_orchestrator.runtime import TorrentJobRepository

    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    jobs = TorrentJobRepository(db, now=lambda: 1000)
    matching = jobs.enqueue(
        "238df97834d4abcdef",
        None,
        "upload",
        {"remote": "gcrypt:/old", "full_torrent": True},
        priority=10,
    )
    unrelated = jobs.enqueue(
        "deadbeefdead0000",
        None,
        "upload",
        {"remote": "gcrypt:/unrelated", "full_torrent": True},
        priority=10,
    )
    jobs.update_state(matching, "promotion_wait")
    jobs.update_state(unrelated, "promotion_wait")
    source = "gcrypt:/BBAN-582.torrent-238df97834d4/BBAN-582.mp4"
    target = "gcrypt:/BBAN-582/BBAN-582 Title.mp4"
    plan = build_migration_plan(
        [{"Path": source.split(":/", 1)[1], "Size": 100, "Hashes": {}}],
        {"BBAN-582": {"title": "Title", "confidence": 1.0}},
    )
    remote = FakeRemote({target: 100})

    changed = reconcile_verified_migrations(
        db,
        plan,
        remote,
        nfo_verified_ids={"BBAN-582"},
        now=2000,
    )

    assert changed == 1
    assert jobs.get(matching)["state"] == "cleanup_wait"
    assert jobs.get(unrelated)["state"] == "promotion_wait"
    cleanup = [
        row
        for row in __import__("sqlite3").connect(db).execute(
            "select payload_json from torrent_jobs where parent_job_id=?",
            (matching,),
        )
    ]
    assert json.loads(cleanup[0][0])["canonical_remote_verified"] is True


def test_audit_accepts_separately_verified_rewritten_nfo(tmp_path: Path):
    from qbt_orchestrator.remote_migration import (
        apply_migration,
        audit_migration,
        build_migration_plan,
    )

    video = "gcrypt:/BBAN-582-hash/BBAN-582.mp4"
    nfo = "gcrypt:/BBAN-582-hash/BBAN-582.nfo"
    remote = FakeRemote({video: 100, nfo: 20})
    plan = build_migration_plan(
        remote.inventory(), {"BBAN-582": {"title": "Title", "confidence": 1.0}}
    )
    result = apply_migration(plan, remote, journal_path=tmp_path / "journal.jsonl")
    nfo_target = next(action.target for action in plan.actions if action.kind == "nfo")
    remote.objects[nfo_target] = 28  # canonical title prefix changed serialized XML size

    audit = audit_migration(plan, remote)

    assert result.verified == 2
    assert audit == {"verified": 2, "pending": 0, "conflict": 0}
