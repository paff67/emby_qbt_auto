from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
import threading
import time


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


def test_migration_prefers_largest_video_when_sources_share_target():
    from qbt_orchestrator.remote_migration import build_migration_plan

    inventory = [
        {"Path": "ABC-123-old/a-ad.mp4", "Size": 15_000_000, "Hashes": {}},
        {"Path": "ABC-123-old/z-main.mp4", "Size": 5_000_000_000, "Hashes": {}},
    ]

    plan = build_migration_plan(
        inventory,
        {"ABC-123": {"title": "Title", "confidence": 1.0}},
    )

    video = next(action for action in plan.actions if action.kind == "video")
    assert video.source == "gcrypt:/ABC-123-old/z-main.mp4"
    assert video.expected_size == 5_000_000_000
    assert any(
        item.source == "gcrypt:/ABC-123-old/a-ad.mp4"
        and item.reason == "target_conflict"
        for item in plan.review
    )


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
    assert states == ["moving", "rollback_wait", "failed"]


def test_bounded_parallel_apply_verifies_independent_actions(tmp_path: Path):
    from qbt_orchestrator.remote_migration import apply_migration, build_migration_plan

    remote = FakeRemote(
        {
            "gcrypt:/ABC-123-old/ABC-123.mp4": 100,
            "gcrypt:/DEF-456-old/DEF-456.mp4": 200,
        }
    )
    plan = build_migration_plan(
        remote.inventory(),
        {
            "ABC-123": {"title": "One", "confidence": 1.0},
            "DEF-456": {"title": "Two", "confidence": 1.0},
        },
    )

    result = apply_migration(
        plan,
        remote,
        journal_path=tmp_path / "journal.jsonl",
        workers=2,
    )

    assert result.verified == 2
    assert result.failed == 0
    assert len((tmp_path / "journal.jsonl").read_text().splitlines()) == 4


def test_parallel_apply_serializes_actions_within_same_id_directory(tmp_path: Path):
    from qbt_orchestrator.remote_migration import apply_migration, build_migration_plan

    class DetectingRemote(FakeRemote):
        def __init__(self, objects):
            super().__init__(objects)
            self.active = set()
            self.overlap = False
            self.lock = threading.Lock()

        def moveto(self, source, target):
            target_id = target.split(":/", 1)[1].split("/", 1)[0]
            with self.lock:
                if target_id in self.active:
                    self.overlap = True
                self.active.add(target_id)
            time.sleep(0.01)
            try:
                super().moveto(source, target)
            finally:
                with self.lock:
                    self.active.remove(target_id)

    remote = DetectingRemote(
        {
            "gcrypt:/ABC-123-old/ABC-123.mp4": 100,
            "gcrypt:/ABC-123-old/ABC-123.nfo": 20,
            "gcrypt:/DEF-456-old/DEF-456.mp4": 200,
        }
    )
    plan = build_migration_plan(
        remote.inventory(),
        {
            "ABC-123": {"title": "One", "confidence": 1.0},
            "DEF-456": {"title": "Two", "confidence": 1.0},
        },
    )

    result = apply_migration(
        plan,
        remote,
        journal_path=tmp_path / "journal.jsonl",
        workers=3,
    )

    assert result.verified == 3
    assert remote.overlap is False


def test_apply_retries_eventually_consistent_target_before_rollback(tmp_path: Path):
    from qbt_orchestrator.remote_migration import apply_migration, build_migration_plan

    class EventuallyConsistentRemote(FakeRemote):
        def __init__(self, objects):
            super().__init__(objects)
            self.hidden = {}

        def moveto(self, source, target):
            super().moveto(source, target)
            self.hidden[target] = 2

        def stat(self, remote):
            if self.hidden.get(remote, 0) > 0:
                self.hidden[remote] -= 1
                return None
            return super().stat(remote)

    source = "gcrypt:/ABC-123-old/ABC-123.mp4"
    remote = EventuallyConsistentRemote({source: 100})
    plan = build_migration_plan(
        remote.inventory(), {"ABC-123": {"title": "One", "confidence": 1.0}}
    )

    result = apply_migration(
        plan,
        remote,
        journal_path=tmp_path / "journal.jsonl",
        verify_attempts=4,
        verify_delay_sec=0,
    )

    assert result.verified == 1
    assert result.failed == 0
    assert len(remote.movetos) == 1
    states = [
        json.loads(line)["state"]
        for line in (tmp_path / "journal.jsonl").read_text().splitlines()
    ]
    assert states == ["moving", "verified"]


def test_apply_resumes_verified_rewritten_nfo_from_journal(tmp_path: Path):
    from qbt_orchestrator.remote_migration import apply_migration, build_migration_plan

    class CountingRemote(FakeRemote):
        def __init__(self, objects):
            super().__init__(objects)
            self.stats = []

        def stat(self, remote):
            self.stats.append(remote)
            return super().stat(remote)

    source = "gcrypt:/ABC-123-old/ABC-123.nfo"
    plan = build_migration_plan(
        [{"Path": source.split(":/", 1)[1], "Size": 20, "Hashes": {}}],
        {"ABC-123": {"title": "One", "confidence": 1.0}},
    )
    action = plan.actions[0]
    journal = tmp_path / "journal.jsonl"
    journal.write_text(
        json.dumps({**asdict(action), "state": "verified"}) + "\n",
        encoding="utf-8",
    )
    remote = CountingRemote({action.target: 28})

    result = apply_migration(
        plan,
        remote,
        journal_path=journal,
        verify_attempts=1,
    )

    assert result.verified == 1
    assert result.failed == 0
    assert remote.stats == [action.target]
    assert len(journal.read_text(encoding="utf-8").splitlines()) == 1


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


def test_reconcile_historical_migration_queues_emby_without_pending_upload(tmp_path: Path):
    import sqlite3

    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.remote_migration import (
        build_migration_plan,
        reconcile_verified_migrations,
    )

    db = tmp_path / "state.sqlite"
    migrate(db, dry_run=False)
    source = "gcrypt:/BBAN-582-old/BBAN-582.mp4"
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

    con = sqlite3.connect(db)
    refresh = con.execute(
        "select emby_media_dir,state from emby_refresh_tasks"
    ).fetchall()
    con.close()
    assert changed == 0
    assert refresh == [("/media/gcrypt/BBAN-582", "queued")]
