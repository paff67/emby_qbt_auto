from __future__ import annotations

import sqlite3
import threading
from dataclasses import FrozenInstanceError, asdict

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
    path = "gcrypt:/" + "x" * 5000 + ".mp4"
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
    assert results[0].generation == newer.generation
    assert results[0].attempted_generation < results[0].generation
    assert results[0].row_count == newer.row_count
    assert results[0].refreshed_at == newer.refreshed_at
    assert [row["normalized_id"] for row in _remote_rows(state_db)] == ["NEW-001"]


def test_remote_index_delayed_success_applies_after_newer_attempt_fails(state_db):
    RemoteMediaIndex(state_db, backfill_db=None, now=lambda: 99).replace_rows(
        [{"video_path": "gcrypt:/base.mp4", "normalized_id": "BASE-001", "size": 1}]
    )
    delayed_started = threading.Event()
    allow_delayed = threading.Event()

    class DelayedSuccess(RemoteMediaIndex):
        def _read_source_rows(self):
            delayed_started.set()
            assert allow_delayed.wait(timeout=5)
            return [{"video_path": "gcrypt:/gen2.mp4", "normalized_id": "GEN-002", "size": 2}]

    class NewerFailure(RemoteMediaIndex):
        def _read_source_rows(self):
            raise sqlite3.OperationalError("database is locked")

    results = []
    delayed = DelayedSuccess(state_db, backfill_db="ignored", now=lambda: 100)
    thread = threading.Thread(target=lambda: results.append(delayed.refresh(force=True)))
    thread.start()
    assert delayed_started.wait(timeout=5)
    failed = NewerFailure(state_db, backfill_db="ignored", now=lambda: 101).refresh(force=True)
    allow_delayed.set()
    thread.join(timeout=5)

    assert failed.status == "preserved"
    assert failed.generation == 1
    assert failed.attempted_generation == 3
    assert results[0].status == "refreshed"
    assert results[0].generation == 2
    assert results[0].attempted_generation == 2
    assert [row["normalized_id"] for row in _remote_rows(state_db)] == ["GEN-002"]


def test_remote_index_delayed_older_success_loses_to_newer_success(state_db):
    RemoteMediaIndex(state_db, backfill_db=None, now=lambda: 99).replace_rows(
        [{"video_path": "gcrypt:/base.mp4", "normalized_id": "BASE-001", "size": 1}]
    )
    delayed_started = threading.Event()
    allow_delayed = threading.Event()

    class DelayedSuccess(RemoteMediaIndex):
        def _read_source_rows(self):
            delayed_started.set()
            assert allow_delayed.wait(timeout=5)
            return [{"video_path": "gcrypt:/gen2.mp4", "normalized_id": "GEN-002", "size": 2}]

    class NewerSuccess(RemoteMediaIndex):
        def _read_source_rows(self):
            return [{"video_path": "gcrypt:/gen3.mp4", "normalized_id": "GEN-003", "size": 3}]

    results = []
    thread = threading.Thread(
        target=lambda: results.append(
            DelayedSuccess(state_db, backfill_db="ignored", now=lambda: 100).refresh(force=True)
        )
    )
    thread.start()
    assert delayed_started.wait(timeout=5)
    newer = NewerSuccess(state_db, backfill_db="ignored", now=lambda: 101).refresh(force=True)
    allow_delayed.set()
    thread.join(timeout=5)

    assert newer.status == "refreshed"
    assert results[0].status == "superseded"
    assert results[0].generation == 3
    assert results[0].attempted_generation == 2
    assert [row["normalized_id"] for row in _remote_rows(state_db)] == ["GEN-003"]


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
        [{"name": "movie.mp4", "size": 99 * 1024**2}],
        [{"name": "movie.mp4", "size": True}],
        [{"name": "movie.mp4", "size": -1}],
        [{"name": "bad\nname.mp4", "size": 500 * 1024**2}],
        [{"size": 500 * 1024**2}],
    ],
)
def test_select_primary_video_rejects_non_primary_or_malformed_inventory(files):
    assert select_primary_video(files) is None


@pytest.mark.parametrize(
    "name",
    ["sample.mp4", "trailer.mkv", "preview/movie.mp4", "A-TEASER-123.mp4"],
)
def test_select_primary_video_does_not_drop_legal_media_because_of_name(name):
    selected = select_primary_video([{"name": name, "size": 500 * 1024**2}])
    assert selected is not None
    assert selected.name == name


@pytest.mark.parametrize("bad_index", [1.0, 1.5, True, " 1", "+1", "01", "1.0"])
def test_select_primary_video_rejects_non_strict_file_indexes(bad_index):
    assert select_primary_video(
        [{"index": bad_index, "name": "movie.mp4", "size": 500 * 1024**2}]
    ) is None


@pytest.mark.parametrize("valid_index,expected", [(3, 3), ("3", 3), ("0", 0)])
def test_select_primary_video_accepts_integer_or_strict_decimal_index(valid_index, expected):
    selected = select_primary_video(
        [{"index": valid_index, "name": "movie.mp4", "size": 500 * 1024**2}]
    )
    assert selected is not None
    assert selected.index == expected


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


@pytest.mark.parametrize("confidence", [None, float("nan"), float("inf"), True])
def test_filename_normalizer_adapter_treats_missing_nonfinite_or_bool_confidence_as_zero(
    confidence,
):
    result = FilenameNormalizerAdapter(
        RecordingNormalizer({"normalized_id": "BBAN-582", "confidence": confidence})
    ).normalize("BBAN-582.mp4")
    assert result.confidence == 0.0


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
    assert result.evidence[0].startswith("fuzzy_sha256:")


@pytest.mark.parametrize(
    "confidence,expected,warning",
    [
        (0.0, "ready", "low_confidence_media_id"),
        (0.799, "ready", "low_confidence_media_id"),
        (0.8, "duplicate_remote", None),
    ],
)
def test_duplicate_matcher_requires_configured_normalizer_confidence(
    state_db, confidence, expected, warning
):
    RemoteMediaIndex(state_db, backfill_db=None, now=lambda: 100).replace_rows(
        [{"video_path": "gcrypt:/a.mp4", "normalized_id": "BBAN-582", "size": 1000}]
    )
    matcher = DuplicateMatcher(
        state_db,
        normalizer=RecordingNormalizer(
            {"normalized_id": "BBAN-582", "confidence": confidence}
        ),
        min_normalizer_confidence=0.8,
    )
    result = matcher.decide("BBAN-582.mp4", 1000)
    assert result.decision == expected
    assert (warning in result.warnings) if warning else not result.warnings


@pytest.mark.parametrize("threshold", [-0.01, 1.01, True, "0.8", float("nan")])
def test_duplicate_matcher_validates_min_normalizer_confidence(state_db, threshold):
    with pytest.raises(ValueError, match="min_normalizer_confidence"):
        DuplicateMatcher(
            state_db,
            normalizer=RecordingNormalizer(),
            min_normalizer_confidence=threshold,
        )


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


def test_local_identity_evidence_is_fingerprinted_not_echoed(state_db):
    secret_identity = "https://user:pass@example.test/private?token=QUERYSECRET"
    def txn(con):
        batch = con.execute(
            "insert into bot_add_batches(batch_key,chat_id,user_id,state,created_at,updated_at) "
            "values('secret-batch','chat','user','complete',1,1)"
        ).lastrowid
        con.execute(
            "insert into bot_add_items(batch_id,source_message_id,source_index,input_kind,"
            "redacted_input,input_sha256,canonical_identity,state,created_at,updated_at) "
            "values(?,1,0,'magnet','redacted','secret-sha',?,'enrolled',1,1)",
            (batch, secret_identity),
        )
    write_transaction(state_db, txn)
    result = DuplicateMatcher(state_db, normalizer=RecordingNormalizer()).decide(
        "movie.mp4", 1000, canonical_identity=secret_identity
    )
    rendered = repr(asdict(result))
    assert result.decision == "duplicate_local"
    assert "identity_sha256:" in rendered
    assert "user:pass" not in rendered
    assert "QUERYSECRET" not in rendered


def test_exact_local_identity_precedes_low_confidence_filename(state_db):
    identity = "btih:" + "ab" * 20
    def txn(con):
        batch = con.execute(
            "insert into bot_add_batches(batch_key,chat_id,user_id,state,created_at,updated_at) "
            "values('low-confidence-local','chat','user','complete',1,1)"
        ).lastrowid
        con.execute(
            "insert into bot_add_items(batch_id,source_message_id,source_index,input_kind,"
            "redacted_input,input_sha256,canonical_identity,state,created_at,updated_at) "
            "values(?,2,0,'magnet','redacted','local-low-sha',?,'enrolled_hold',1,1)",
            (batch, identity),
        )
    write_transaction(state_db, txn)
    matcher = DuplicateMatcher(
        state_db,
        normalizer=RecordingNormalizer(
            {"normalized_id": "BBAN-582", "confidence": 0.0}
        ),
    )
    assert matcher.decide(
        "BBAN-582.mp4", 1000, canonical_identity=identity
    ).decision == "duplicate_local"


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
    assert [match.path_sha256 for match in result.matches] == sorted(
        match.path_sha256 for match in result.matches
    )
    assert len(result.evidence) <= 20


def test_remote_match_after_evidence_limit_still_decides_duplicate_and_is_included(state_db):
    rows = [
        {
            "video_path": f"gcrypt:/a-{index:02d}.mp4",
            "normalized_id": "BBAN-582",
            "size": 2000,
        }
        for index in range(20)
    ]
    rows.append(
        {"video_path": "gcrypt:/z-decisive.mp4", "normalized_id": "BBAN-582", "size": 1000}
    )
    RemoteMediaIndex(state_db, backfill_db=None, now=lambda: 100).replace_rows(rows)

    result = DuplicateMatcher(state_db, normalizer=RecordingNormalizer()).decide(
        "BBAN-582.mp4", 1000
    )

    assert result.decision == "duplicate_remote"
    decisive = next(match for match in result.matches if match.size_close)
    assert len(decisive.path_sha256) == 64
    assert len(result.matches) <= 20
    assert len(result.evidence) <= 20
    assert any("count=21" in item and "truncated=true" in item for item in result.evidence)


def test_duplicate_decision_never_projects_opaque_remote_path_or_credentials(state_db):
    path = "https://user:pass@example.test/private/TOKEN123/movie.mp4?auth=QUERYSECRET"
    RemoteMediaIndex(state_db, backfill_db=None, now=lambda: 100).replace_rows(
        [{"video_path": path, "normalized_id": "BBAN-582", "size": 1000, "status": "done?token=STATUSSECRET"}]
    )
    result = DuplicateMatcher(state_db, normalizer=RecordingNormalizer()).decide(
        "BBAN-582.mp4", 1000
    )
    rendered = repr(asdict(result)) + repr(result.evidence)
    assert result.decision == "duplicate_remote"
    for secret in ("user:pass", "TOKEN123", "QUERYSECRET", "STATUSSECRET", path):
        assert secret not in rendered
    assert result.matches[0].status == "unknown"
    assert len(result.matches[0].path_sha256) == 64


def test_remote_index_preserves_opaque_unicode_paths_as_distinct_keys(state_db):
    fullwidth = "gcrypt:/Ａ/movie.mp4"
    ascii_path = "gcrypt:/A/movie.mp4"
    result = RemoteMediaIndex(state_db, backfill_db=None, now=lambda: 100).replace_rows(
        [
            {"video_path": fullwidth, "normalized_id": "BBAN-582", "size": 1000},
            {"video_path": ascii_path, "normalized_id": "BBAN-582", "size": 1000},
        ]
    )
    assert result.row_count == 2
    assert [row["video_path"] for row in _remote_rows(state_db)] == sorted(
        [fullwidth, ascii_path]
    )


@pytest.mark.parametrize(
    "candidate,remote,expected",
    [
        (100, 85, "needs_confirmation"),
        (85, 100, "duplicate_remote"),
        (115, 100, "duplicate_remote"),
        (115000000000000001, 100000000000000000, "needs_confirmation"),
    ],
)
def test_size_tolerance_uses_remote_denominator_with_stable_boundary(
    state_db, candidate, remote, expected
):
    RemoteMediaIndex(state_db, backfill_db=None, now=lambda: 100).replace_rows(
        [{"video_path": "gcrypt:/a.mp4", "normalized_id": "BBAN-582", "size": remote}]
    )
    result = DuplicateMatcher(
        state_db, normalizer=RecordingNormalizer(), size_tolerance_ratio=0.15
    ).decide("BBAN-582.mp4", candidate)
    assert result.decision == expected
    assert result.matches[0].size_diff_ratio == pytest.approx(abs(candidate - remote) / remote)


@pytest.mark.parametrize("value", ["1000", 1000.0, True, -1, 2**63])
def test_remote_index_non_sqlite_integer_sizes_are_persisted_as_unknown(state_db, value):
    RemoteMediaIndex(state_db, backfill_db=None, now=lambda: 100).replace_rows(
        [{"video_path": "gcrypt:/a.mp4", "normalized_id": "BBAN-582", "size": value}]
    )
    assert _remote_rows(state_db)[0]["size"] is None


def test_remote_index_normalization_exception_preserves_old_snapshot(state_db):
    RemoteMediaIndex(state_db, backfill_db=None, now=lambda: 99).replace_rows(
        [{"video_path": "gcrypt:/old.mp4", "normalized_id": "OLD-001", "size": 1}]
    )

    class ExplodingMapping(dict):
        def get(self, key, default=None):
            raise RuntimeError("private/source/path")

    class BrokenSource(RemoteMediaIndex):
        def _read_source_rows(self):
            return [ExplodingMapping()]

    result = BrokenSource(
        state_db, backfill_db="private-source", now=lambda: 100
    ).refresh(force=True)

    assert result.status == "preserved"
    assert result.error_code == "source_snapshot_invalid"
    assert "private" not in repr(result)
    assert [row["normalized_id"] for row in _remote_rows(state_db)] == ["OLD-001"]


def test_remote_index_apply_failure_rolls_back_replace_and_returns_safe_result(state_db):
    RemoteMediaIndex(state_db, backfill_db=None, now=lambda: 99).replace_rows(
        [{"video_path": "gcrypt:/old.mp4", "normalized_id": "OLD-001", "size": 1}]
    )
    write_transaction(
        state_db,
        lambda con: con.execute(
            "create trigger reject_remote_snapshot before insert on remote_media_index "
            "begin select raise(abort,'private/source/path'); end"
        ),
    )
    result = RemoteMediaIndex(state_db, backfill_db=None, now=lambda: 100).replace_rows(
        [{"video_path": "gcrypt:/new.mp4", "normalized_id": "NEW-001", "size": 2}]
    )

    assert result.status == "preserved"
    assert result.error_code == "snapshot_apply_failed"
    assert "private" not in repr(result)
    assert [row["normalized_id"] for row in _remote_rows(state_db)] == ["OLD-001"]


def test_remote_index_fails_closed_on_partial_invalid_source_snapshot(state_db):
    RemoteMediaIndex(state_db, backfill_db=None, now=lambda: 99).replace_rows(
        [{"video_path": "gcrypt:/old.mp4", "normalized_id": "OLD-001", "size": 1}]
    )
    result = RemoteMediaIndex(state_db, backfill_db=None, now=lambda: 100).replace_rows(
        [
            {"video_path": "gcrypt:/new.mp4", "normalized_id": "NEW-001", "size": 2},
            {"video_path": "bad\npath.mp4", "normalized_id": "BAD-001", "size": 3},
        ]
    )
    assert result.status == "preserved"
    assert result.error_code == "source_snapshot_invalid"
    assert result.source_row_count == 2
    assert result.invalid_row_count == 1
    assert [row["normalized_id"] for row in _remote_rows(state_db)] == ["OLD-001"]


def test_remote_index_preserves_old_snapshot_when_source_budget_is_exceeded(state_db):
    RemoteMediaIndex(state_db, backfill_db=None, now=lambda: 99).replace_rows(
        [{"video_path": "gcrypt:/old.mp4", "normalized_id": "OLD-001", "size": 1}]
    )
    result = RemoteMediaIndex(
        state_db,
        backfill_db=None,
        now=lambda: 100,
        max_source_rows=1,
        max_source_bytes=1024,
    ).replace_rows(
        [
            {"video_path": "gcrypt:/a.mp4", "normalized_id": "AAA-001", "size": 1},
            {"video_path": "gcrypt:/b.mp4", "normalized_id": "BBB-001", "size": 1},
        ]
    )
    assert result.status == "preserved"
    assert result.error_code == "source_snapshot_limit"
    assert result.source_row_count == 2
    assert [row["normalized_id"] for row in _remote_rows(state_db)] == ["OLD-001"]


def test_remote_index_source_reader_is_streamed_and_closable(state_db, tmp_path):
    source = tmp_path / "stream-source.sqlite"
    _create_backfill(
        source,
        [("gcrypt:/a.mp4", "AAA-001", 1, "a.mp4", "done")],
    )
    rows = RemoteMediaIndex(state_db, backfill_db=source)._read_source_rows()
    assert not isinstance(rows, (list, tuple))
    assert dict(next(iter(rows)))["normalized_id"] == "AAA-001"
    rows.close()


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_source_rows": 0},
        {"max_source_rows": True},
        {"max_source_bytes": 0},
        {"max_source_bytes": True},
    ],
)
def test_remote_index_validates_source_budgets(state_db, kwargs):
    with pytest.raises(ValueError):
        RemoteMediaIndex(state_db, backfill_db=None, **kwargs)


def test_remote_index_enforces_source_byte_budget_without_replacing(state_db):
    RemoteMediaIndex(state_db, backfill_db=None, now=lambda: 99).replace_rows(
        [{"video_path": "gcrypt:/old.mp4", "normalized_id": "OLD-001", "size": 1}]
    )
    result = RemoteMediaIndex(
        state_db,
        backfill_db=None,
        now=lambda: 100,
        max_source_bytes=16,
    ).replace_rows(
        [{"video_path": "gcrypt:/new.mp4", "normalized_id": "NEW-001", "size": 2}]
    )
    assert result.status == "preserved"
    assert result.error_code == "source_snapshot_limit"
    assert [row["normalized_id"] for row in _remote_rows(state_db)] == ["OLD-001"]


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
    assert "idx_remote_media_normalized_path" in remote_plan
    assert "TEMP B-TREE" not in remote_plan.upper()


def test_tombstone_blocks_before_remote_duplicate(state_db):
    from qbt_orchestrator.processed_media import ProcessedMediaRepository

    RemoteMediaIndex(state_db, backfill_db=None, now=lambda: 100).replace_rows(
        [{
            "video_path": "gcrypt:/BBAN-582/a.mp4",
            "normalized_id": "BBAN-582",
            "size": 1000,
            "raw_basename": "a.mp4",
            "status": "done",
        }]
    )
    repo = ProcessedMediaRepository(state_db, now=lambda: 100, enforce=True)
    repo.register_tombstone(
        "BBAN-582",
        deleted_at=100,
        actor_id="ops",
        reason="manual",
        create_from_audit=True,
    )
    matcher = DuplicateMatcher(
        state_db,
        normalizer=RecordingNormalizer(),
        size_tolerance_ratio=0.15,
        processed_media=repo,
        enforce_processed_media=True,
    )
    decision = matcher.decide("BBAN-582.mp4", 1050)
    assert decision.decision == "blocked_manual_deleted"
    assert decision.reason == "previously_ingested_then_manually_deleted"
    # Different size / restored remote / alias-style names remain blocked.
    assert matcher.decide("BBAN-582-HD.mp4", 999999).decision == "blocked_manual_deleted"
    assert matcher.decide("prefix-BBAN-582-suffix.mp4", 1000).decision == "blocked_manual_deleted"
