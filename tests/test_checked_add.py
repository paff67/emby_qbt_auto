from __future__ import annotations

import sqlite3
import threading
from dataclasses import FrozenInstanceError

import pytest

from qbt_orchestrator.checked_add import (
    DuplicateMatcher,
    FilenameNormalizerAdapter,
    RemoteMediaIndex,
    select_primary_video,
)
from qbt_orchestrator.db import migrate, readonly_connect, write_transaction


class Clock:
    def __init__(self, value: int = 1_800_000_000):
        self.value = value

    def __call__(self) -> int:
        return self.value


class RecordingNormalizer:
    def __init__(self, result=None, error: Exception | None = None):
        self.result = result or {"normalized_id": "BBAN-582", "confidence": 0.95}
        self.error = error
        self.calls: list[str] = []

    def normalize(self, name: str):
        self.calls.append(name)
        if self.error:
            raise self.error
        return dict(self.result)


@pytest.fixture
def state_db(tmp_path):
    path = tmp_path / "state.sqlite"
    migrate(path)
    return path


def _create_backfill(path, rows):
    con = sqlite3.connect(path)
    con.execute(
        "create table items(video_path text,normalized_id text,size integer,"
        "raw_basename text,status text)"
    )
    con.executemany("insert into items values(?,?,?,?,?)", rows)
    con.commit()
    con.close()


def _remote_rows(path):
    con = readonly_connect(path)
    try:
        return [dict(row) for row in con.execute(
            "select video_path,normalized_id,size,raw_basename,status,source,updated_at "
            "from remote_media_index order by video_path"
        )]
    finally:
        con.close()


def test_remote_index_refresh_uses_filtered_backfill_rows_and_atomic_replace(state_db, tmp_path):
    source = tmp_path / "backfill.sqlite"
    _create_backfill(
        source,
        [
            ("gcrypt:/BBAN-582/a.mp4", "BBAN-582", 1000, "a.mp4", "done"),
            ("gcrypt:/missing.mp4", "MISS-001", 50, "missing.mp4", "missing_remote"),
            ("", "EMPTY-001", 50, "empty.mp4", "done"),
            ("gcrypt:/no-id.mp4", "", 50, "no-id.mp4", "done"),
        ],
    )
    clock = Clock(100)
    index = RemoteMediaIndex(state_db, backfill_db=source, now=clock)

    first = index.refresh()
    assert first.status == "refreshed"
    assert first.row_count == 1
    assert [row["normalized_id"] for row in _remote_rows(state_db)] == ["BBAN-582"]
    assert _remote_rows(state_db)[0]["updated_at"] == 100

    con = sqlite3.connect(source)
    con.execute("delete from items")
    con.execute(
        "insert into items values(?,?,?,?,?)",
        ("gcrypt:/SONE-792/b.mkv", "SONE-792", 2000, "b.mkv", "verified"),
    )
    con.commit()
    con.close()
    clock.value += 6 * 3600 + 1

    second = index.refresh()
    assert second.status == "refreshed"
    assert [row["normalized_id"] for row in _remote_rows(state_db)] == ["SONE-792"]


def test_remote_index_fresh_ttl_uses_durable_metadata_even_when_empty(state_db, tmp_path):
    source = tmp_path / "backfill.sqlite"
    _create_backfill(source, [])
    clock = Clock(200)
    first = RemoteMediaIndex(state_db, backfill_db=source, now=clock)
    assert first.refresh().status == "refreshed"

    source.unlink()
    second_worker = RemoteMediaIndex(state_db, backfill_db=source, now=clock)
    fresh = second_worker.refresh()
    assert fresh.status == "fresh"
    assert fresh.row_count == 0


@pytest.mark.parametrize("failure", ["missing", "schema", "corrupt"])
def test_remote_index_source_failure_preserves_previous_snapshot_without_path_leak(
    state_db, tmp_path, failure
):
    index = RemoteMediaIndex(state_db, backfill_db=None, now=lambda: 100)
    index.replace_rows(
        [{"video_path": "gcrypt:/BBAN-582/a.mp4", "normalized_id": "BBAN-582", "size": 1000}]
    )
    source = tmp_path / ("private-secret-source.sqlite")
    if failure == "schema":
        sqlite3.connect(source).close()
    elif failure == "corrupt":
        source.write_bytes(b"not-a-sqlite-database")
    refresh = RemoteMediaIndex(state_db, backfill_db=source, now=lambda: 100 + 6 * 3600 + 1)

    result = refresh.refresh()

    assert result.status == "preserved"
    assert result.error_code in {"source_unavailable", "source_schema_error"}
    assert "private-secret" not in repr(result)
    assert [row["normalized_id"] for row in _remote_rows(state_db)] == ["BBAN-582"]
    if failure == "missing":
        assert not source.exists()


def test_remote_index_successful_empty_snapshot_clears_old_backfill_rows(state_db, tmp_path):
    index = RemoteMediaIndex(state_db, backfill_db=None, now=lambda: 100)
    index.replace_rows(
        [{"video_path": "gcrypt:/BBAN-582/a.mp4", "normalized_id": "BBAN-582", "size": 1000}]
    )
    source = tmp_path / "backfill.sqlite"
    _create_backfill(source, [])

    result = RemoteMediaIndex(
        state_db, backfill_db=source, now=lambda: 100 + 6 * 3600 + 1
    ).refresh(force=True)

    assert result.status == "refreshed"
    assert result.row_count == 0
    assert _remote_rows(state_db) == []


def test_remote_index_locked_source_preserves_previous_snapshot(state_db, tmp_path):
    RemoteMediaIndex(state_db, backfill_db=None, now=lambda: 100).replace_rows(
        [{"video_path": "gcrypt:/old.mp4", "normalized_id": "OLD-001", "size": 1}]
    )
    source = tmp_path / "backfill.sqlite"
    _create_backfill(
        source,
        [("gcrypt:/new.mp4", "NEW-001", 2, "new.mp4", "done")],
    )
    lock = sqlite3.connect(source, timeout=0)
    lock.execute("pragma journal_mode=DELETE")
    lock.execute("begin exclusive")
    try:
        result = RemoteMediaIndex(
            state_db, backfill_db=source, now=lambda: 100 + 6 * 3600 + 1
        ).refresh(force=True)
    finally:
        lock.rollback()
        lock.close()
    assert result.status == "preserved"
    assert result.error_code == "source_unavailable"
    assert [row["normalized_id"] for row in _remote_rows(state_db)] == ["OLD-001"]


def test_remote_index_rejects_overlong_keys_instead_of_truncating_them(state_db):
    path = "gcrypt:/" + "x" * 1100 + ".mp4"
    result = RemoteMediaIndex(state_db, backfill_db=None, now=lambda: 100).replace_rows(
        [{"video_path": path, "normalized_id": "BBAN-582", "size": 1000}]
    )
    assert result.row_count == 0
    assert _remote_rows(state_db) == []


def test_remote_index_older_concurrent_snapshot_cannot_overwrite_newer(state_db):
    old_read_started = threading.Event()
    allow_old_read = threading.Event()

    class ControlledIndex(RemoteMediaIndex):
        def __init__(self, *args, rows, block=False, **kwargs):
            super().__init__(*args, **kwargs)
            self.rows = rows
            self.block = block

        def _read_source_rows(self):
            if self.block:
                old_read_started.set()
                assert allow_old_read.wait(timeout=5)
            return self.rows

    old = ControlledIndex(
        state_db,
        backfill_db="ignored",
        now=lambda: 100,
        rows=[{"video_path": "gcrypt:/old.mp4", "normalized_id": "OLD-001", "size": 1}],
        block=True,
    )
    new = ControlledIndex(
        state_db,
        backfill_db="ignored",
        now=lambda: 101,
        rows=[{"video_path": "gcrypt:/new.mp4", "normalized_id": "NEW-001", "size": 2}],
    )
    results = []
    thread = threading.Thread(target=lambda: results.append(old.refresh(force=True)))
    thread.start()
    assert old_read_started.wait(timeout=5)
    newer = new.refresh(force=True)
    allow_old_read.set()
    thread.join(timeout=5)

    assert newer.status == "refreshed"
    assert results[0].status == "superseded"
    assert [row["normalized_id"] for row in _remote_rows(state_db)] == ["NEW-001"]


def test_select_primary_video_uses_media_extensions_minimum_and_stable_tie_break():
    files = [
        {"index": 7, "name": "movie-b.MKV", "size": 200 * 1024**2, "progress": 0.2},
        {"index": 3, "name": "movie-a.mp4", "size": 200 * 1024**2, "progress": 0.1},
        {"index": 1, "name": "notes.txt", "size": 900 * 1024**2},
    ]

    selected = select_primary_video(files)

    assert selected is not None
    assert (selected.index, selected.name, selected.size) == (3, "movie-a.mp4", 200 * 1024**2)


def test_select_primary_video_accepts_documented_positional_minimum():
    selected = select_primary_video([{"name": "movie.mp4", "size": 10}], 10)
    assert selected is not None


@pytest.mark.parametrize(
    "files",
    [
        [{"name": "sample.mp4", "size": 500 * 1024**2}],
        [{"name": "trailer.mkv", "size": 500 * 1024**2}],
        [{"name": "movie.mp4", "size": 99 * 1024**2}],
        [{"name": "movie.mp4", "size": True}],
        [{"name": "movie.mp4", "size": -1}],
        [{"name": "bad\nname.mp4", "size": 500 * 1024**2}],
        [{"size": 500 * 1024**2}],
    ],
)
def test_select_primary_video_rejects_non_primary_or_malformed_inventory(files):
    assert select_primary_video(files) is None


def test_filename_normalizer_adapter_reuses_injected_normalizer_and_canonicalizes_id():
    normalizer = RecordingNormalizer({"normalized_id": "bban_582", "confidence": 0.9})
    adapter = FilenameNormalizerAdapter(normalizer)

    result = adapter.normalize("BBAN-582-remaster.mp4")

    assert result.normalized_id == "BBAN-582"
    assert normalizer.calls == ["BBAN-582-remaster.mp4"]


@pytest.mark.parametrize(
    "raw_id",
    ["FC2-PPV-1234567", "123ABC-456", "123456-789", "A1-234", "HEYZO-1234"],
)
def test_filename_normalizer_adapter_preserves_real_normalizer_id_families(raw_id):
    adapter = FilenameNormalizerAdapter(
        RecordingNormalizer({"normalized_id": raw_id, "confidence": 0.95})
    )
    assert adapter.normalize(f"{raw_id}.mp4").normalized_id == raw_id


def test_filename_normalizer_adapter_returns_unrecognized_on_exception_or_unsafe_id():
    failed = FilenameNormalizerAdapter(RecordingNormalizer(error=RuntimeError("secret")))
    unsafe = FilenameNormalizerAdapter(
        RecordingNormalizer({"normalized_id": "../../BAD-1\n", "confidence": 1})
    )

    assert failed.normalize("movie.mp4").normalized_id is None
    assert failed.normalize("movie.mp4").reason == "normalizer_failed"
    assert unsafe.normalize("movie.mp4").normalized_id is None


def test_duplicate_matcher_classifies_exact_size_and_variant(state_db):
    RemoteMediaIndex(state_db, backfill_db=None, now=lambda: 100).replace_rows(
        [{
            "video_path": "gcrypt:/BBAN-582/a.mp4",
            "normalized_id": "BBAN-582",
            "size": 1000,
            "raw_basename": "a.mp4",
            "status": "done",
        }]
    )
    matcher = DuplicateMatcher(
        state_db, normalizer=RecordingNormalizer(), size_tolerance_ratio=0.15
    )

    assert matcher.decide("BBAN-582.mp4", 1050).decision == "duplicate_remote"
    assert matcher.decide("BBAN-582.mp4", 2000).decision == "needs_confirmation"


def test_fuzzy_name_is_warning_only(state_db):
    matcher = DuplicateMatcher(
        state_db, normalizer=RecordingNormalizer(), size_tolerance_ratio=0.15
    )

    result = matcher.decide(
        "BBAN-582-remaster.mp4", 2000, fuzzy_matches=["BBAN-583"]
    )

    assert result.decision == "ready"
    assert result.warnings == ("fuzzy_name_only",)
    assert result.evidence == ("fuzzy:BBAN-583",)


def test_prior_successful_canonical_identity_is_local_duplicate_but_transient_rows_are_not(state_db):
    def insert(state, identity, suffix):
        def txn(con):
            batch = con.execute(
                "insert into bot_add_batches(batch_key,chat_id,user_id,state,created_at,updated_at) "
                "values(?,?,?,?,?,?)",
                (f"batch-{suffix}", "chat", "user", "complete", 1, 1),
            ).lastrowid
            con.execute(
                "insert into bot_add_items(batch_id,source_message_id,source_index,input_kind,"
                "redacted_input,input_sha256,canonical_identity,state,created_at,updated_at) "
                "values(?,?,?,?,?,?,?,?,?,?)",
                (batch, suffix, 0, "magnet", "redacted", f"sha-{suffix}", identity, state, 1, 1),
            )
        write_transaction(state_db, txn)

    insert("failed", "btih:failed", 1)
    insert("enrolled_hold", "btih:success", 2)
    matcher = DuplicateMatcher(state_db, normalizer=RecordingNormalizer())

    assert matcher.decide("x.mp4", 1000, canonical_identity="btih:success").decision == "duplicate_local"
    assert matcher.decide("x.mp4", 1000, canonical_identity="btih:failed").decision == "ready"


def test_remote_multiple_rows_any_trusted_size_match_wins_deterministically(state_db):
    RemoteMediaIndex(state_db, backfill_db=None, now=lambda: 100).replace_rows(
        [
            {"video_path": "gcrypt:/z.mp4", "normalized_id": "BBAN-582", "size": 2000},
            {"video_path": "gcrypt:/a.mp4", "normalized_id": "BBAN-582", "size": 1000},
            {"video_path": "gcrypt:/unknown.mp4", "normalized_id": "BBAN-582", "size": None},
        ]
    )
    matcher = DuplicateMatcher(state_db, normalizer=RecordingNormalizer())

    result = matcher.decide("BBAN-582.mp4", 1050)

    assert result.decision == "duplicate_remote"
    assert [match.video_path for match in result.matches] == sorted(
        match.video_path for match in result.matches
    )
    assert len(result.evidence) <= 20


@pytest.mark.parametrize("candidate,remote", [(100, 85), (85, 100)])
def test_size_tolerance_uses_max_denominator_and_includes_exact_boundary(
    state_db, candidate, remote
):
    RemoteMediaIndex(state_db, backfill_db=None, now=lambda: 100).replace_rows(
        [{"video_path": "gcrypt:/a.mp4", "normalized_id": "BBAN-582", "size": remote}]
    )
    result = DuplicateMatcher(
        state_db, normalizer=RecordingNormalizer(), size_tolerance_ratio=0.15
    ).decide("BBAN-582.mp4", candidate)
    assert result.decision == "duplicate_remote"
    assert result.matches[0].size_diff_ratio == pytest.approx(abs(candidate - remote) / max(candidate, remote))


@pytest.mark.parametrize("candidate,remote", [(None, 100), (0, 100), (100, None), (100, 0), (True, 100)])
def test_unknown_or_invalid_size_requires_confirmation_for_exact_id(
    state_db, candidate, remote
):
    RemoteMediaIndex(state_db, backfill_db=None, now=lambda: 100).replace_rows(
        [{"video_path": "gcrypt:/a.mp4", "normalized_id": "BBAN-582", "size": remote}]
    )
    result = DuplicateMatcher(state_db, normalizer=RecordingNormalizer()).decide(
        "BBAN-582.mp4", candidate
    )
    assert result.decision == "needs_confirmation"


@pytest.mark.parametrize("value", [-0.01, 1.01, True, "0.15"])
def test_duplicate_matcher_validates_tolerance(state_db, value):
    with pytest.raises(ValueError, match="size_tolerance_ratio"):
        DuplicateMatcher(state_db, normalizer=RecordingNormalizer(), size_tolerance_ratio=value)


def test_duplicate_decision_is_frozen_and_evidence_is_bounded(state_db):
    result = DuplicateMatcher(state_db, normalizer=RecordingNormalizer()).decide(
        "BBAN-582.mp4", 1000, fuzzy_matches=["X" * 1000] * 100
    )
    with pytest.raises(FrozenInstanceError):
        result.decision = "other"
    assert len(result.evidence) <= 20
    assert all(len(value) <= 256 and "\n" not in value for value in result.evidence)


def test_duplicate_queries_use_identity_and_remote_indexes(state_db):
    con = readonly_connect(state_db)
    try:
        local_plan = " ".join(str(row[3]) for row in con.execute(
            "explain query plan select id from bot_add_items "
            "where canonical_identity=? and state in ('enrolled','enrolled_hold') limit 1",
            ("btih:test",),
        ))
        remote_plan = " ".join(str(row[3]) for row in con.execute(
            "explain query plan select video_path from remote_media_index "
            "where normalized_id=? order by video_path limit 20",
            ("BBAN-582",),
        ))
    finally:
        con.close()
    assert "idx_bot_add_items_canonical_identity" in local_plan
    assert "idx_remote_media_normalized_id" in remote_plan
