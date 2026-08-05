from __future__ import annotations

import hashlib
import json
import math
import re
import sqlite3
import time
import unicodedata
from dataclasses import asdict, dataclass, field
from decimal import Decimal
from pathlib import Path, PurePosixPath
from typing import Any, Iterable, Mapping, Protocol

from .db import readonly_connect, write_transaction
from .media import FallbackFilenameNormalizer, _MEDIA_EXTS
from .processed_media import ProcessedMediaRepository


_REMOTE_SOURCE = "backfill"
_DEFAULT_TTL_SEC = 6 * 3600
_MAX_REMOTE_MATCHES = 20
_MAX_EVIDENCE = 20
_MAX_PATH_BYTES = 4096
_MAX_NAME_CHARS = 512
_SQLITE_MAX_INTEGER = (1 << 63) - 1
_DEFAULT_MAX_SOURCE_ROWS = 1_000_000
_DEFAULT_MAX_SOURCE_BYTES = 256 * 1024 * 1024
_SAFE_REMOTE_STATUSES = frozenset(
    {"done", "verified", "complete", "uploaded", "remote_probe"}
)
_MEDIA_ID = re.compile(r"(?:[A-Z0-9]{1,16}-){1,2}\d{2,9}")


class FilenameNormalizer(Protocol):
    def normalize(self, raw_filename: str) -> Mapping[str, Any]: ...


@dataclass(frozen=True)
class RemoteIndexRefreshResult:
    status: str
    row_count: int
    generation: int
    refreshed_at: int | None
    error_code: str | None = None
    attempted_generation: int | None = None
    source_row_count: int = 0
    invalid_row_count: int = 0


@dataclass(frozen=True)
class PrimaryVideo:
    index: int
    name: str
    size: int
    progress: float | None = None


@dataclass(frozen=True)
class NormalizationResult:
    normalized_id: str | None
    confidence: float
    reason: str


@dataclass(frozen=True)
class RemoteMediaMatch:
    normalized_id: str
    size: int | None
    status: str
    path_sha256: str
    size_close: bool
    size_diff_ratio: float | None


@dataclass(frozen=True)
class DuplicateDecision:
    decision: str
    reason: str
    normalized_id: str | None
    matches: tuple[RemoteMediaMatch, ...] = ()
    evidence: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()


@dataclass(frozen=True)
class _RemoteMatchScan:
    total_count: int
    matches: tuple[RemoteMediaMatch, ...]
    decisive_match: RemoteMediaMatch | None


@dataclass(frozen=True)
class _IndexedRemoteMatch:
    video_path: str = field(repr=False)
    projected: RemoteMediaMatch


@dataclass(frozen=True)
class _PreparedSnapshot:
    rows: tuple[tuple[Any, ...], ...]
    source_row_count: int
    invalid_row_count: int
    source_bytes: int


class _SnapshotRejected(ValueError):
    def __init__(self, code: str, *, source_row_count: int, invalid_row_count: int):
        self.code = code
        self.source_row_count = source_row_count
        self.invalid_row_count = invalid_row_count
        super().__init__(code)


def _safe_text(
    value: Any, *, limit: int, allow_empty: bool = False, truncate: bool = False
) -> str | None:
    if value is None:
        return "" if allow_empty else None
    text = unicodedata.normalize("NFKC", str(value)).strip()
    if (not text and not allow_empty) or any(ord(char) < 32 or ord(char) == 127 for char in text):
        return None
    if len(text) > limit:
        return text[:limit] if truncate else None
    return text


def _canonical_media_id(value: Any) -> str | None:
    text = _safe_text(value, limit=64)
    if text is None:
        return None
    text = re.sub(r"[-_ ]+", "-", text.upper()).strip("-")
    if not _MEDIA_ID.fullmatch(text):
        return None
    return text


def _opaque_video_path(value: Any) -> str | None:
    if type(value) is not str or not value:
        return None
    if any(unicodedata.category(char) == "Cc" for char in value):
        return None
    try:
        encoded = value.encode("utf-8", "strict")
    except UnicodeError:
        return None
    if len(encoded) > _MAX_PATH_BYTES:
        return None
    return value


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8", "strict")).hexdigest()


def _strict_file_index(value: Any) -> int | None:
    if type(value) is int:
        return value if 0 <= value <= _SQLITE_MAX_INTEGER else None
    if (
        type(value) is str
        and len(value) <= 19
        and re.fullmatch(r"(?:0|[1-9]\d*)", value)
    ):
        parsed = int(value)
        return parsed if parsed <= _SQLITE_MAX_INTEGER else None
    return None


def _known_positive_size(value: Any) -> int | None:
    if type(value) is not int:
        return None
    if value <= 0 or value > _SQLITE_MAX_INTEGER:
        return None
    return value


def _mapping_value(item: Any, *names: str) -> Any:
    if isinstance(item, Mapping):
        for name in names:
            if name in item:
                return item[name]
        return None
    for name in names:
        if hasattr(item, name):
            return getattr(item, name)
    return None


def select_primary_video(
    files: Iterable[Any], min_bytes: int = 100 * 1024 * 1024
) -> PrimaryVideo | None:
    """Select the largest trustworthy qBT video, deterministically.

    A sub-threshold video is not useful for the remote size decision and is
    deliberately not used as a torrent-name fallback.
    """
    if isinstance(min_bytes, bool) or not isinstance(min_bytes, int) or min_bytes <= 0:
        raise ValueError("min_bytes")
    candidates: list[PrimaryVideo] = []
    for ordinal, item in enumerate(files):
        raw_name = _mapping_value(item, "name", "path")
        name = _safe_text(raw_name, limit=_MAX_NAME_CHARS)
        size = _known_positive_size(_mapping_value(item, "size"))
        if name is None or size is None or size < min_bytes:
            continue
        normalized_path = name.replace("\\", "/")
        if PurePosixPath(normalized_path).suffix.lower() not in _MEDIA_EXTS:
            continue
        raw_index = _mapping_value(item, "index")
        if raw_index is None:
            index = ordinal
        else:
            index = _strict_file_index(raw_index)
            if index is None:
                continue
        progress_raw = _mapping_value(item, "progress")
        progress: float | None = None
        if not isinstance(progress_raw, bool):
            try:
                candidate_progress = float(progress_raw)
                if math.isfinite(candidate_progress) and 0 <= candidate_progress <= 1:
                    progress = candidate_progress
            except (TypeError, ValueError, OverflowError):
                pass
        candidates.append(PrimaryVideo(index=index, name=name, size=size, progress=progress))
    if not candidates:
        return None
    candidates.sort(key=lambda row: (-row.size, row.name.casefold(), row.name, row.index))
    return candidates[0]


class FilenameNormalizerAdapter:
    """Defensive adapter around the configured filename-normalizer contract."""

    def __init__(self, normalizer: FilenameNormalizer | None = None):
        self.normalizer = normalizer or FallbackFilenameNormalizer()

    def normalize(self, raw_filename: str) -> NormalizationResult:
        name = _safe_text(raw_filename, limit=_MAX_NAME_CHARS)
        if name is None:
            return NormalizationResult(None, 0.0, "invalid_filename")
        try:
            payload = self.normalizer.normalize(name)
            if not isinstance(payload, Mapping):
                return NormalizationResult(None, 0.0, "normalizer_invalid_result")
        except Exception:
            return NormalizationResult(None, 0.0, "normalizer_failed")
        normalized_id = _canonical_media_id(payload.get("normalized_id"))
        raw_confidence = payload.get("confidence", 0.0)
        if isinstance(raw_confidence, bool):
            confidence = 0.0
        else:
            try:
                confidence = float(raw_confidence)
            except (TypeError, ValueError, OverflowError):
                confidence = 0.0
        if not math.isfinite(confidence):
            confidence = 0.0
        confidence = min(1.0, max(0.0, confidence))
        reason = _safe_text(payload.get("reason"), limit=128, truncate=True) or (
            "normalized" if normalized_id else "unrecognized"
        )
        if normalized_id is None and payload.get("normalized_id"):
            reason = "normalizer_invalid_id"
        return NormalizationResult(normalized_id, confidence, reason)


class RemoteMediaIndex:
    SOURCE_QUERY = (
        "select video_path,normalized_id,size,raw_basename,status "
        "from items "
        "where normalized_id is not null and normalized_id!='' "
        "and video_path is not null and video_path!='' "
        "and coalesce(status,'') not in "
        "('missing_remote','duplicate_alias','normalize_failed')"
    )

    def __init__(
        self,
        state_db: str | Path,
        *,
        backfill_db: str | Path | None,
        now=None,
        ttl_sec: int = _DEFAULT_TTL_SEC,
        max_source_rows: int = _DEFAULT_MAX_SOURCE_ROWS,
        max_source_bytes: int = _DEFAULT_MAX_SOURCE_BYTES,
    ):
        for name, value in (
            ("ttl_sec", ttl_sec),
            ("max_source_rows", max_source_rows),
            ("max_source_bytes", max_source_bytes),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(name)
        self.state_db = Path(state_db)
        self.backfill_db = Path(backfill_db) if backfill_db is not None else None
        self.now = now or (lambda: int(time.time()))
        self.ttl_sec = ttl_sec
        self.max_source_rows = max_source_rows
        self.max_source_bytes = max_source_bytes

    def replace_rows(self, rows: Iterable[Mapping[str, Any]]) -> RemoteIndexRefreshResult:
        now = int(self.now())
        generation, _ = self._begin_refresh(now, force=True)
        try:
            prepared = self._prepare_rows(rows, updated_at=now)
        except _SnapshotRejected as exc:
            return self._preserve(
                generation,
                now,
                exc.code,
                source_row_count=exc.source_row_count,
                invalid_row_count=exc.invalid_row_count,
            )
        except Exception:
            return self._preserve(generation, now, "source_snapshot_invalid")
        try:
            return self._apply_snapshot(generation, now, prepared)
        except Exception:
            return self._preserve(
                generation,
                now,
                "snapshot_apply_failed",
                source_row_count=prepared.source_row_count,
                invalid_row_count=prepared.invalid_row_count,
            )

    def refresh(self, *, force: bool = False) -> RemoteIndexRefreshResult:
        now = int(self.now())
        generation, fresh = self._begin_refresh(now, force=force)
        if fresh is not None:
            return fresh
        if self.backfill_db is None:
            return self._preserve(generation, now, "source_unconfigured")
        source_rows = None
        try:
            source_rows = self._read_source_rows()
            rows = self._prepare_rows(source_rows, updated_at=now)
        except _SnapshotRejected as exc:
            return self._preserve(
                generation,
                now,
                exc.code,
                source_row_count=exc.source_row_count,
                invalid_row_count=exc.invalid_row_count,
            )
        except sqlite3.DatabaseError as exc:
            message = str(exc).lower()
            code = "source_schema_error" if "no such table" in message or "no such column" in message else "source_unavailable"
            return self._preserve(generation, now, code)
        except (OSError, ValueError, TypeError):
            return self._preserve(generation, now, "source_unavailable")
        except Exception:
            return self._preserve(generation, now, "source_snapshot_invalid")
        finally:
            close = getattr(source_rows, "close", None)
            if callable(close):
                try:
                    close()
                except Exception:
                    pass
        try:
            return self._apply_snapshot(generation, now, rows)
        except Exception:
            return self._preserve(
                generation,
                now,
                "snapshot_apply_failed",
                source_row_count=rows.source_row_count,
                invalid_row_count=rows.invalid_row_count,
            )

    def _read_source_rows(self) -> Iterable[sqlite3.Row]:
        assert self.backfill_db is not None
        def stream():
            con = readonly_connect(self.backfill_db)
            try:
                yield from con.execute(self.SOURCE_QUERY)
            finally:
                con.close()
        return stream()

    def _begin_refresh(
        self, now: int, *, force: bool
    ) -> tuple[int, RemoteIndexRefreshResult | None]:
        def txn(con: sqlite3.Connection):
            con.execute(
                "insert or ignore into remote_media_index_refresh_state("
                "source,requested_generation,applied_generation,row_count,last_result) "
                "values(?,0,0,0,'never')",
                (_REMOTE_SOURCE,),
            )
            row = con.execute(
                "select * from remote_media_index_refresh_state where source=?",
                (_REMOTE_SOURCE,),
            ).fetchone()
            refreshed_at = int(row["refreshed_at"]) if row["refreshed_at"] is not None else None
            if (
                not force
                and refreshed_at is not None
                and now >= refreshed_at
                and now - refreshed_at < self.ttl_sec
            ):
                return 0, RemoteIndexRefreshResult(
                    status="fresh",
                    row_count=int(row["row_count"]),
                    generation=int(row["applied_generation"]),
                    refreshed_at=refreshed_at,
                )
            generation = int(row["requested_generation"]) + 1
            con.execute(
                "update remote_media_index_refresh_state set requested_generation=?,"
                "last_attempt_at=?,last_result='reading' where source=?",
                (generation, now, _REMOTE_SOURCE),
            )
            return generation, None

        return write_transaction(self.state_db, txn)

    def _apply_snapshot(
        self, generation: int, now: int, snapshot: _PreparedSnapshot
    ) -> RemoteIndexRefreshResult:
        def txn(con: sqlite3.Connection):
            state = con.execute(
                "select requested_generation,refreshed_at,row_count,applied_generation "
                "from remote_media_index_refresh_state where source=?",
                (_REMOTE_SOURCE,),
            ).fetchone()
            if state is None or generation <= int(state["applied_generation"]):
                return RemoteIndexRefreshResult(
                    status="superseded",
                    row_count=int(state["row_count"]) if state else 0,
                    generation=int(state["applied_generation"]) if state else 0,
                    refreshed_at=(
                        int(state["refreshed_at"])
                        if state and state["refreshed_at"] is not None
                        else None
                    ),
                    attempted_generation=generation,
                    source_row_count=snapshot.source_row_count,
                    invalid_row_count=snapshot.invalid_row_count,
                )
            con.execute("delete from remote_media_index where source=?", (_REMOTE_SOURCE,))
            con.executemany(
                "insert into remote_media_index("
                "video_path,normalized_id,size,raw_basename,status,source,updated_at) "
                "values(?,?,?,?,?,?,?)",
                snapshot.rows,
            )
            con.execute(
                "update remote_media_index_refresh_state set applied_generation=?,"
                "refreshed_at=?,row_count=?,last_attempt_at=?,last_result='refreshed' "
                "where source=? and applied_generation<?",
                (
                    generation,
                    now,
                    len(snapshot.rows),
                    now,
                    _REMOTE_SOURCE,
                    generation,
                ),
            )
            return RemoteIndexRefreshResult(
                status="refreshed",
                row_count=len(snapshot.rows),
                generation=generation,
                refreshed_at=now,
                attempted_generation=generation,
                source_row_count=snapshot.source_row_count,
                invalid_row_count=snapshot.invalid_row_count,
            )

        return write_transaction(self.state_db, txn)

    def _preserve(
        self,
        generation: int,
        now: int,
        error_code: str,
        *,
        source_row_count: int = 0,
        invalid_row_count: int = 0,
    ) -> RemoteIndexRefreshResult:
        def txn(con: sqlite3.Connection):
            state = con.execute(
                "select requested_generation,applied_generation,refreshed_at,row_count "
                "from remote_media_index_refresh_state where source=?",
                (_REMOTE_SOURCE,),
            ).fetchone()
            if state is None:
                return RemoteIndexRefreshResult(
                    status="preserved",
                    row_count=0,
                    generation=0,
                    refreshed_at=None,
                    error_code=error_code,
                    attempted_generation=generation,
                    source_row_count=source_row_count,
                    invalid_row_count=invalid_row_count,
                )
            if int(state["requested_generation"]) == generation:
                con.execute(
                    "update remote_media_index_refresh_state set last_attempt_at=?,last_result=? "
                    "where source=? and requested_generation=?",
                    (now, error_code, _REMOTE_SOURCE, generation),
                )
            return RemoteIndexRefreshResult(
                status="preserved",
                row_count=int(state["row_count"]),
                generation=int(state["applied_generation"]),
                refreshed_at=(
                    int(state["refreshed_at"])
                    if state["refreshed_at"] is not None
                    else None
                ),
                error_code=error_code,
                attempted_generation=generation,
                source_row_count=source_row_count,
                invalid_row_count=invalid_row_count,
            )

        return write_transaction(self.state_db, txn)

    def _prepare_rows(
        self, rows: Iterable[Mapping[str, Any]], *, updated_at: int
    ) -> _PreparedSnapshot:
        by_path: dict[str, tuple[Any, ...]] = {}
        source_row_count = 0
        invalid_row_count = 0
        source_bytes = 0
        for raw in rows:
            source_row_count += 1
            if source_row_count > self.max_source_rows:
                raise _SnapshotRejected(
                    "source_snapshot_limit",
                    source_row_count=source_row_count,
                    invalid_row_count=invalid_row_count,
                )
            if not hasattr(raw, "get"):
                raw = dict(raw)
            values = tuple(
                raw.get(name)
                for name in ("video_path", "normalized_id", "size", "raw_basename", "status")
            )
            try:
                source_bytes += sum(self._source_value_bytes(value) for value in values)
            except (TypeError, UnicodeError):
                invalid_row_count += 1
                continue
            if source_bytes > self.max_source_bytes:
                raise _SnapshotRejected(
                    "source_snapshot_limit",
                    source_row_count=source_row_count,
                    invalid_row_count=invalid_row_count,
                )
            video_path = _opaque_video_path(values[0])
            normalized_id = _canonical_media_id(values[1])
            if video_path is None or normalized_id is None or video_path in by_path:
                invalid_row_count += 1
                continue
            raw_size = values[2]
            size = _known_positive_size(raw_size)
            if raw_size in (0, "0"):
                size = None
            raw_basename = _safe_text(
                values[3], limit=255, allow_empty=True, truncate=True
            ) or ""
            status = _safe_text(
                values[4], limit=64, allow_empty=True, truncate=True
            ) or ""
            row = (
                video_path,
                normalized_id,
                size,
                raw_basename,
                status,
                _REMOTE_SOURCE,
                updated_at,
            )
            by_path[video_path] = row
        if invalid_row_count:
            raise _SnapshotRejected(
                "source_snapshot_invalid",
                source_row_count=source_row_count,
                invalid_row_count=invalid_row_count,
            )
        return _PreparedSnapshot(
            rows=tuple(by_path[path] for path in sorted(by_path)),
            source_row_count=source_row_count,
            invalid_row_count=0,
            source_bytes=source_bytes,
        )

    @staticmethod
    def _source_value_bytes(value: Any) -> int:
        if value is None:
            return 0
        if type(value) is str:
            return len(value.encode("utf-8", "strict"))
        if type(value) is bytes:
            return len(value)
        if type(value) in {bool, int, float}:
            return 8
        raise TypeError("unsupported source value")


class DuplicateMatcher:
    def __init__(
        self,
        state_db: str | Path,
        *,
        normalizer: FilenameNormalizer | FilenameNormalizerAdapter | None = None,
        backfill_db: str | Path | None = None,
        now=None,
        size_tolerance_ratio: float = 0.15,
        min_normalizer_confidence: float = 0.8,
        processed_media: ProcessedMediaRepository | None = None,
        enforce_processed_media: bool | None = None,
    ):
        if isinstance(size_tolerance_ratio, bool) or not isinstance(
            size_tolerance_ratio, (int, float)
        ):
            raise ValueError("size_tolerance_ratio")
        tolerance = float(size_tolerance_ratio)
        if not math.isfinite(tolerance) or not 0 <= tolerance <= 1:
            raise ValueError("size_tolerance_ratio")
        if isinstance(min_normalizer_confidence, bool) or not isinstance(
            min_normalizer_confidence, (int, float)
        ):
            raise ValueError("min_normalizer_confidence")
        confidence_threshold = float(min_normalizer_confidence)
        if not math.isfinite(confidence_threshold) or not 0 <= confidence_threshold <= 1:
            raise ValueError("min_normalizer_confidence")
        self.state_db = Path(state_db)
        self.normalizer = (
            normalizer
            if isinstance(normalizer, FilenameNormalizerAdapter)
            else FilenameNormalizerAdapter(normalizer)
        )
        self.size_tolerance_ratio = tolerance
        self._tolerance_decimal = Decimal(str(tolerance))
        self.min_normalizer_confidence = confidence_threshold
        if processed_media is not None:
            self.processed_media = processed_media
        else:
            enforce = bool(enforce_processed_media) if enforce_processed_media is not None else False
            self.processed_media = ProcessedMediaRepository(
                self.state_db, now=now or (lambda: int(time.time())), enforce=enforce
            )
        if enforce_processed_media is not None:
            self.processed_media.enforce = bool(enforce_processed_media)
        self.remote_index = RemoteMediaIndex(
            self.state_db,
            backfill_db=backfill_db,
            now=now or (lambda: int(time.time())),
        )

    def decide(
        self,
        primary_name: str,
        primary_size: Any,
        *,
        canonical_identity: str | None = None,
        fuzzy_matches: Iterable[Any] = (),
    ) -> DuplicateDecision:
        identity = _safe_text(canonical_identity, limit=256) if canonical_identity else None
        if identity and self._successful_identity_exists(identity):
            return DuplicateDecision(
                "duplicate_local",
                "canonical_identity_already_enrolled",
                None,
                evidence=(f"identity_sha256:{_sha256_text(identity)}",),
            )

        normalized = self.normalizer.normalize(primary_name)
        warnings: list[str] = []
        evidence: list[str] = []
        fuzzy = self._safe_fuzzy_matches(fuzzy_matches)
        if fuzzy:
            warnings.append("fuzzy_name_only")
            evidence.extend(f"fuzzy_sha256:{value}" for value in fuzzy)
        if normalized.normalized_id is None:
            if normalized.reason in {"normalizer_failed", "normalizer_invalid_result", "normalizer_invalid_id"}:
                warnings.append("normalization_unavailable")
            return DuplicateDecision(
                "ready",
                "no_exact_remote_media_id",
                None,
                evidence=tuple(evidence[:_MAX_EVIDENCE]),
                warnings=tuple(dict.fromkeys(warnings)),
            )
        if normalized.confidence < self.min_normalizer_confidence:
            warnings.append("low_confidence_media_id")
            return DuplicateDecision(
                "ready",
                "normalized_media_id_below_confidence_threshold",
                normalized.normalized_id,
                evidence=tuple(evidence[:_MAX_EVIDENCE]),
                warnings=tuple(dict.fromkeys(warnings)),
            )

        tombstone = self.processed_media.is_permanently_blocked(normalized.normalized_id)
        if tombstone is not None:
            evidence.insert(
                0,
                f"tombstone:policy=block_permanent:lifecycle={tombstone.get('lifecycle_state')}",
            )
            return DuplicateDecision(
                "blocked_manual_deleted",
                "previously_ingested_then_manually_deleted",
                normalized.normalized_id,
                evidence=tuple(evidence[:_MAX_EVIDENCE]),
                warnings=tuple(dict.fromkeys(warnings)),
            )

        candidate_size = _known_positive_size(primary_size)
        scan = self._scan_remote_matches(normalized.normalized_id, candidate_size)
        if scan.total_count == 0:
            return DuplicateDecision(
                "ready",
                "no_exact_remote_media_id",
                normalized.normalized_id,
                evidence=tuple(evidence[:_MAX_EVIDENCE]),
                warnings=tuple(dict.fromkeys(warnings)),
            )

        remote_evidence: list[str] = []
        if scan.total_count > len(scan.matches):
            remote_evidence.append(
                f"remote_matches:count={scan.total_count}:truncated=true"
            )
        evidence_matches = list(scan.matches)
        if scan.decisive_match is not None:
            evidence_matches.sort(
                key=lambda item: (
                    item != scan.decisive_match,
                    item.path_sha256,
                )
            )
        for match in evidence_matches:
            ratio_text = (
                "unknown"
                if match.size_diff_ratio is None
                else f"{match.size_diff_ratio:.6f}"
            )
            remote_evidence.append(
                f"remote:path_sha256={match.path_sha256}:size={match.size}:diff={ratio_text}"
            )
        evidence = remote_evidence + evidence
        return DuplicateDecision(
            "duplicate_remote" if scan.decisive_match is not None else "needs_confirmation",
            "exact_media_id_size_within_tolerance" if scan.decisive_match is not None else "exact_media_id_size_differs_or_unknown",
            normalized.normalized_id,
            matches=scan.matches,
            evidence=tuple(evidence[:_MAX_EVIDENCE]),
            warnings=tuple(dict.fromkeys(warnings)),
        )

    def _scan_remote_matches(
        self, normalized_id: str, candidate_size: int | None
    ) -> _RemoteMatchScan:
        con = readonly_connect(self.state_db)
        try:
            rows = con.execute(
                "select video_path,normalized_id,size,raw_basename,status,source,updated_at "
                "from remote_media_index where normalized_id=? order by video_path",
                (normalized_id,),
            )
            total_count = 0
            samples: list[_IndexedRemoteMatch] = []
            decisive: _IndexedRemoteMatch | None = None
            for raw_row in rows:
                total_count += 1
                row = dict(raw_row)
                opaque_path = _opaque_video_path(row.get("video_path"))
                remote_size = (
                    _known_positive_size(row.get("size"))
                    if opaque_path is not None
                    else None
                )
                ratio: float | None = None
                close = False
                if candidate_size is not None and remote_size is not None:
                    difference = abs(candidate_size - remote_size)
                    ratio = float(Decimal(difference) / Decimal(remote_size))
                    close = Decimal(difference) <= self._tolerance_decimal * Decimal(
                        remote_size
                    )
                raw_status = row.get("status")
                status = (
                    raw_status
                    if type(raw_status) is str and raw_status in _SAFE_REMOTE_STATUSES
                    else "unknown"
                )
                path_sha256 = _sha256_text(opaque_path or "[invalid-opaque-path]")
                match = _IndexedRemoteMatch(
                    video_path=opaque_path or "",
                    projected=RemoteMediaMatch(
                        normalized_id=normalized_id,
                        size=remote_size,
                        status=status,
                        path_sha256=path_sha256,
                        size_close=close,
                        size_diff_ratio=ratio,
                    ),
                )
                if len(samples) < _MAX_REMOTE_MATCHES:
                    samples.append(match)
                if close and decisive is None:
                    decisive = match
            if decisive is not None and decisive not in samples:
                if samples:
                    samples[-1] = decisive
                else:
                    samples.append(decisive)
            projected = sorted(
                (item.projected for item in samples),
                key=lambda item: item.path_sha256,
            )
            return _RemoteMatchScan(
                total_count,
                tuple(projected),
                decisive.projected if decisive is not None else None,
            )
        finally:
            con.close()

    def _successful_identity_exists(self, identity: str) -> bool:
        con = readonly_connect(self.state_db)
        try:
            row = con.execute(
                "select id from bot_add_items where canonical_identity=? "
                "and state in ('enrolled','enrolled_hold') limit 1",
                (identity,),
            ).fetchone()
            return row is not None
        finally:
            con.close()

    @staticmethod
    def _safe_fuzzy_matches(values: Iterable[Any]) -> tuple[str, ...]:
        result: list[str] = []
        seen: set[str] = set()
        for value in values:
            safe = _safe_text(value, limit=240, truncate=True)
            if safe is None:
                continue
            fingerprint = _sha256_text(safe)
            if fingerprint in seen:
                continue
            seen.add(fingerprint)
            result.append(fingerprint)
            if len(result) >= _MAX_EVIDENCE:
                break
        return tuple(result)


class CheckedAddService:
    """Validate prechecked bot items and enroll them without starting qBT.

    This deliberately stays a thin coordinator over the existing queue,
    duplicate matcher and qBT executor.  SQLite generations are the durable
    fence; the opaque qBT tag proves that a temporary registration belongs to
    the item before any external mutation.
    """

    _STOPPED_STATES = frozenset({"stoppeddl", "stoppedup", "pauseddl", "pausedup"})

    def __init__(
        self,
        repository,
        gateway,
        matcher: DuplicateMatcher,
        *,
        notifications=None,
        warning_service=None,
        owner: str = "checked-add",
        now=None,
        lease_sec: int = 30,
    ) -> None:
        if isinstance(lease_sec, bool) or not isinstance(lease_sec, int) or lease_sec <= 0:
            raise ValueError("lease_sec")
        self.repository = repository
        self.gateway = gateway
        self.matcher = matcher
        self.notifications = notifications
        self.warning_service = warning_service
        self.owner = str(owner or "checked-add")
        self.now = now or (lambda: int(time.time()))
        self.lease_sec = lease_sec

    def tick(self, sync_healthy: bool = True, max_items: int = 20) -> dict[str, Any]:
        if isinstance(max_items, bool) or not isinstance(max_items, int) or max_items <= 0:
            raise ValueError("max_items")
        result: dict[str, Any] = {
            "suspended": not bool(sync_healthy),
            "checked": [],
            "enrolled": [],
            "duplicates": [],
            "confirmations": [],
            "recovered": [],
            "finalized": [],
            "errors": 0,
        }
        if not sync_healthy:
            return result

        for item in self._items_needing_finalization(max_items):
            try:
                self._finalize_enrollment(dict(item))
                result["finalized"].append(int(item["id"]))
            except (ValueError, RuntimeError):
                result["errors"] += 1

        for item in self._items_in_states({"enrolling"}, max_items):
            try:
                finished = self._finish_enrollment(dict(item))
                if str(finished.get("state") or "") in {"enrolled", "enrolled_hold"}:
                    result["recovered"].append(int(item["id"]))
                    result["enrolled"].append(int(finished["id"]))
            except (ValueError, RuntimeError):
                result["errors"] += 1

        remaining = max(
            0, max_items - len(result["recovered"]) - int(result["errors"])
        )
        prechecking = self._items_in_states({"prechecking"}, remaining)
        if prechecking and self.matcher.remote_index.backfill_db is not None:
            self.matcher.remote_index.refresh()
        for item in prechecking:
            item_id = int(item["id"])
            try:
                outcome = self._validate_one(item_id)
                result["checked"].append(item_id)
                if outcome["state"] == "enrolled":
                    result["enrolled"].append(item_id)
                elif outcome["state"] in {"duplicate_local", "duplicate_remote"}:
                    result["duplicates"].append(item_id)
                elif outcome["state"] == "needs_confirmation":
                    result["confirmations"].append(item_id)
            except (ValueError, RuntimeError, sqlite3.Error):
                result["errors"] += 1
        return result

    def approve_hold(
        self, item_id: int, actor: str, approval_generation: int
    ) -> dict[str, Any]:
        current = self.repository.get_item(item_id)
        if current["state"] == "enrolled_hold":
            if int(approval_generation) != int(current["approval_generation"]) - 1:
                raise ValueError("approval_generation_conflict")
            return current
        if current["state"] == "enrolling" and current.get("approved_by") == str(actor):
            if int(approval_generation) != int(current["approval_generation"]) - 1:
                raise ValueError("approval_generation_conflict")
            return self._finish_enrollment(current)
        self._approval_token(current, approval_generation, "needs_confirmation")
        if self._owned_snapshot(current) is None:
            raise ValueError("qbt_precheck_ownership")
        enrolling = self.repository.transition_item(
            item_id,
            {"needs_confirmation"},
            "enrolling",
            "duplicate_override_approved",
            {"approved_by": str(actor), "approved_at": int(self.now())},
            approval_generation=approval_generation,
        )
        return self._finish_enrollment(enrolling)

    def cancel(
        self, item_id: int, actor: str, approval_generation: int
    ) -> dict[str, Any]:
        del actor  # Actor is recorded by the Telegram callback event layer.
        current = self.repository.get_item(item_id)
        if current["state"] == "cancelled":
            if int(approval_generation) != int(current["approval_generation"]) - 1:
                raise ValueError("approval_generation_conflict")
            return current
        self._approval_token(current, approval_generation, "needs_confirmation")
        snapshot = self._owned_snapshot(current, missing_ok=True)
        if snapshot is not None:
            if not self.gateway.remove_registration(
                str(current["qbt_hash"]),
                guard=lambda: self._approval_owned_guard(
                    item_id, approval_generation, "needs_confirmation"
                ),
            ):
                raise ValueError("qbt_write_fenced")
        return self.repository.transition_item(
            item_id,
            {"needs_confirmation"},
            "cancelled",
            "cancelled_by_operator",
            approval_generation=approval_generation,
        )

    def allow_scheduling(
        self, item_id: int, actor: str, approval_generation: int
    ) -> dict[str, Any]:
        del actor
        current = self.repository.get_item(item_id)
        if current["state"] == "enrolled":
            if not current.get("approved_by"):
                raise ValueError("state_conflict")
            if int(approval_generation) != int(current["approval_generation"]):
                raise ValueError("approval_generation_conflict")
            return self._release_enrolled_hold(current, approval_generation)
        self._approval_token(current, approval_generation, "enrolled_hold")
        self._managed_snapshot(current)
        enrolled = self.repository.transition_item(
            item_id,
            {"enrolled_hold"},
            "enrolled",
            "scheduling_allowed",
            approval_generation=approval_generation,
        )
        return self._release_enrolled_hold(enrolled, approval_generation)

    def _validate_one(self, item_id: int) -> dict[str, Any]:
        now = int(self.now())
        lease = self._claim_or_renew(item_id, now)
        generation = int(lease["metadata_lease_generation"])
        token = (self.owner, generation)
        current = self.repository.get_item(item_id)
        snapshot = self._owned_snapshot(current, missing_ok=True)

        # Recovery after qBT accepted duplicate cleanup but SQLite did not.
        if snapshot is None and current.get("decision") in {
            "duplicate_local",
            "duplicate_remote",
        }:
            return self.repository.transition_item(
                item_id,
                {"prechecking"},
                str(current["decision"]),
                str(current.get("decision_reason") or "duplicate_cleanup_recovered"),
                metadata_lease_owner=token[0],
                metadata_lease_generation=token[1],
            )
        if snapshot is None:
            self.repository.release_metadata_lease(item_id, *token)
            raise ValueError("qbt_precheck_ownership")

        files = self.gateway.torrent_files(str(current["qbt_hash"]))
        primary = select_primary_video(files)
        if primary is None:
            self.repository.update_metadata_probe(
                item_id,
                token[0],
                token[1],
                {"last_error": "primary_video_not_found"},
            )
            self.repository.release_metadata_lease(item_id, *token)
            raise ValueError("primary_video_not_found")
        total_size = sum(
            row["size"]
            for row in files
            if type(row.get("size")) is int and row["size"] > 0
        )
        decision = self.matcher.decide(
            primary.name,
            primary.size,
            canonical_identity=current.get("canonical_identity"),
        )
        evidence = {
            "matches": [asdict(match) for match in decision.matches],
            "evidence": list(decision.evidence),
            "warnings": list(decision.warnings),
        }
        self.repository.update_metadata_probe(
            item_id,
            token[0],
            token[1],
            {
                "display_name": primary.name,
                "normalized_media_id": decision.normalized_id,
                "total_size": total_size,
                "primary_video_size": primary.size,
                "decision": decision.decision,
                "decision_reason": decision.reason,
                "remote_match_json": json.dumps(evidence, ensure_ascii=False, sort_keys=True),
                "last_error": None,
            },
        )

        if decision.decision in {"duplicate_local", "duplicate_remote"}:
            if not self.gateway.remove_registration(
                str(current["qbt_hash"]),
                guard=lambda: self._lease_owned_guard(item_id, token),
            ):
                raise ValueError("qbt_write_fenced")
            return self.repository.transition_item(
                item_id,
                {"prechecking"},
                decision.decision,
                decision.reason,
                metadata_lease_owner=token[0],
                metadata_lease_generation=token[1],
            )
        if decision.decision == "blocked_manual_deleted":
            # Metadata-only recognition: stop precheck, zero priorities, remove
            # temporary registration without deleting payload files.
            guard = lambda: self._lease_owned_guard(item_id, token)
            try:
                self.gateway.zero_file_priorities(str(current["qbt_hash"]), guard=guard)
            except Exception:
                pass
            try:
                self.gateway.stop(str(current["qbt_hash"]), guard=guard)
            except Exception:
                pass
            if not self.gateway.remove_registration(
                str(current["qbt_hash"]),
                guard=guard,
            ):
                raise ValueError("qbt_write_fenced")
            if self.warning_service is not None:
                try:
                    self.warning_service.report(
                        warning_key=f"checked_add:blocked_manual_deleted:{item_id}",
                        severity="warning",
                        topic="checked_add",
                        safe_message="之前入库但后续被手动删除，已永久禁止重新下载",
                        related_item_id=item_id,
                        related_batch_id=int(current.get("batch_id") or 0) or None,
                    )
                except Exception:
                    pass
            return self.repository.transition_item(
                item_id,
                {"prechecking"},
                "cancelled",
                decision.reason,
                {
                    "decision": "blocked_manual_deleted",
                    "decision_reason": decision.reason,
                },
                metadata_lease_owner=token[0],
                metadata_lease_generation=token[1],
            )
        if decision.decision == "needs_confirmation":
            pending = self.repository.transition_item(
                item_id,
                {"prechecking"},
                "needs_confirmation",
                decision.reason,
                metadata_lease_owner=token[0],
                metadata_lease_generation=token[1],
            )
            self._notify_confirmation(pending)
            return pending
        ready = self.repository.transition_item(
            item_id,
            {"prechecking"},
            "ready",
            decision.reason,
            metadata_lease_owner=token[0],
            metadata_lease_generation=token[1],
        )
        enrolling = self.repository.transition_item(
            item_id, {"ready"}, "enrolling", "automatic_enrollment"
        )
        return self._finish_enrollment(enrolling)

    def _finish_enrollment(self, item: Mapping[str, Any]) -> dict[str, Any]:
        item_id = int(item["id"])
        current = self.repository.get_item(item_id)
        if current["state"] in {"enrolled", "enrolled_hold"}:
            return current
        if current["state"] != "enrolling":
            raise ValueError("state_conflict")
        now = int(self.now())
        lease = self._claim_or_renew(item_id, now)
        token = (self.owner, int(lease["metadata_lease_generation"]))
        current = self.repository.get_item(item_id)
        opaque_tag = str(current.get("qbt_precheck_tag") or "")
        self._opaque_owned_snapshot(current)
        held = bool(current.get("approved_by")) or current.get("decision") == "needs_confirmation"
        added_tags = "checked,maybe-duplicate,hold" if held else "checked,hold"
        removed_tags = "precheck,metadata-probe"
        torrent_hash = str(current["qbt_hash"])
        guard = lambda: self._enrollment_guard(item_id, token, torrent_hash, opaque_tag)

        def fail(code: str) -> dict[str, Any]:
            return self._record_enrollment_failure(item_id, token, code)

        # 3) stop first so later writes can re-enter after crashes.
        if not self.gateway.stop(torrent_hash, guard=guard):
            return fail("qbt_write_fenced")
        try:
            self._require_stopped(current)
        except ValueError as exc:
            return fail(str(exc) or "qbt_stop_not_observed")

        files = self.gateway.torrent_files(torrent_hash)
        primary = select_primary_video(files)
        if primary is None:
            return fail("primary_video_not_found")
        all_indices = [
            int(row["index"])
            for row in files
            if type(row.get("index")) is int and row["index"] >= 0
        ]
        if len(all_indices) != len(files) or not all_indices:
            return fail("file_index")

        # 4-5) converge file priorities while opaque tag + hold remain.
        if any(int(row.get("priority") or 0) != 0 for row in files):
            if not self.gateway.set_file_priorities(
                torrent_hash, all_indices, 0, guard=guard
            ):
                return fail("qbt_write_fenced")
        if not self.gateway.set_file_priorities(
            torrent_hash, [primary.index], 1, guard=guard
        ):
            return fail("qbt_write_fenced")

        # 6-8) tags and force_start before category=auto.
        if not self.gateway.add_tags(torrent_hash, added_tags, guard=guard):
            return fail("qbt_write_fenced")
        if not self.gateway.remove_tags(torrent_hash, removed_tags, guard=guard):
            return fail("qbt_write_fenced")
        if not self.gateway.set_force_start(torrent_hash, False, guard=guard):
            return fail("qbt_write_fenced")

        # 9) absorb filePrio auto-resume.
        if not self.gateway.stop(torrent_hash, guard=guard):
            return fail("qbt_write_fenced")
        try:
            self._require_stopped(current)
        except ValueError as exc:
            return fail(str(exc) or "qbt_stop_not_observed")

        # 10) set category=auto only after the torrent is owned+stopped.
        if not self.gateway.set_category(torrent_hash, "auto", guard=guard):
            return fail("qbt_write_fenced")

        # 11) final stop + verify category/tags/force_start/priorities/stopped.
        if not self.gateway.stop(torrent_hash, guard=guard):
            return fail("qbt_write_fenced")
        snapshot = self._opaque_owned_snapshot(current)
        if str(snapshot.get("state") or "").strip().lower() not in self._STOPPED_STATES:
            return fail("qbt_stop_not_observed")
        if str(snapshot.get("category") or "") != "auto":
            return fail("qbt_category_verification_failed")
        tags = self._tags(snapshot)
        required = {"checked", "hold", opaque_tag}
        if held:
            required.add("maybe-duplicate")
        if not required <= tags:
            return fail("qbt_tag_verification_failed")
        if {"precheck", "metadata-probe"} & tags:
            return fail("qbt_tag_verification_failed")
        if bool(snapshot.get("force_start")):
            return fail("qbt_force_start_verification_failed")
        verified_files = self.gateway.torrent_files(torrent_hash)
        if not verified_files or any(
            int(row.get("priority") or 0)
            != (1 if int(row.get("index", -1)) == primary.index else 0)
            for row in verified_files
        ):
            return fail("file_priority_verification_failed")

        # 12) commit SQLite while opaque fence remains.
        approval_generation = int(current["approval_generation"])
        finished = self.repository.transition_item(
            item_id,
            {"enrolling"},
            "enrolled_hold" if held else "enrolled",
            "approved_enrollment" if held else "automatic_enrollment_complete",
            metadata_lease_owner=token[0],
            metadata_lease_generation=token[1],
            approval_generation=approval_generation,
        )
        self._resolve_enrollment_warning(item_id)
        try:
            return self._finalize_enrollment(finished, opaque_tag=opaque_tag)
        except Exception:
            return finished

    def _record_enrollment_failure(
        self, item_id: int, token: tuple[str, int], error_code: str
    ) -> dict[str, Any]:
        code = str(error_code or "enrollment_failed").strip() or "enrollment_failed"
        now = int(self.now())
        attempts = int(self.repository.get_item(item_id).get("attempts") or 0) + 1
        delay = min(300, 5 * (2 ** min(attempts, 6)))
        try:
            updated = self.repository.record_enrollment_retry(
                item_id,
                metadata_lease_owner=token[0],
                metadata_lease_generation=token[1],
                error_code=code,
                next_run_at=now + delay,
            )
        except ValueError:
            return self.repository.get_item(item_id)
        if int(updated.get("attempts") or 0) >= 3 and self.warning_service is not None:
            self.warning_service.report_once(
                warning_key=f"checked_add:enrollment_stuck:{item_id}",
                severity="warning",
                topic="checked_add",
                safe_message=f"enrollment stuck for item {item_id}: {code}",
                related_item_id=item_id,
            )
        return updated

    def _resolve_enrollment_warning(self, item_id: int) -> None:
        if self.warning_service is None:
            return
        key = f"checked_add:enrollment_stuck:{item_id}"
        now = int(self.now())

        def txn(con: sqlite3.Connection) -> None:
            con.execute(
                "update bot_warning_inbox set resolved=1, resolved_at=?, "
                "resolved_by=?, updated_at=? where warning_key=? and resolved=0",
                (now, "checked-add", now, key),
            )

        try:
            write_transaction(self.repository.state_db, txn)
        except Exception:
            return

    def _notify_confirmation(self, item: Mapping[str, Any]) -> None:
        if self.notifications is None:
            return
        batch = self.repository.get_batch(int(item["batch_id"]))
        generation = int(item["approval_generation"])
        item_id = int(item["id"])
        media = str(item.get("normalized_media_id") or item.get("display_name") or item_id)
        self.notifications.enqueue_with_status(
            batch["chat_id"],
            "download_confirmation",
            f"任务 {media} 需要确认，当前保持暂停。",
            level="warning",
            payload={
                "item_id": item_id,
                "approval_generation": generation,
                "normalized_media_id": item.get("normalized_media_id"),
                "primary_video_size": item.get("primary_video_size"),
                "reply_markup": {
                    "inline_keyboard": [
                        [
                            {
                                "text": "确认并暂缓",
                                "callback_data": f"i:y:{item_id}:{generation}",
                            },
                            {
                                "text": "取消",
                                "callback_data": f"i:x:{item_id}:{generation}",
                            },
                        ]
                    ]
                },
            },
            dedupe_key=f"checked-add-confirm:{item_id}:{generation}",
        )

    def _items_in_states(self, states: set[str], limit: int) -> list[dict[str, Any]]:
        if limit <= 0:
            return []
        values = sorted(states)
        placeholders = ",".join("?" for _ in values)
        now = int(self.now())
        con = readonly_connect(self.repository.state_db)
        try:
            return [
                dict(row)
                for row in con.execute(
                    f"select * from bot_add_items where state in ({placeholders}) "
                    "and (next_run_at is null or next_run_at<=?) "
                    "order by updated_at,id limit ?",
                    (*values, now, int(limit)),
                )
            ]
        finally:
            con.close()

    def _items_needing_finalization(self, limit: int) -> list[dict[str, Any]]:
        con = readonly_connect(self.repository.state_db)
        try:
            return [
                dict(row)
                for row in con.execute(
                    "select * from bot_add_items where state in ('enrolled','enrolled_hold') "
                    "and qbt_precheck_tag is not null order by updated_at,id limit ?",
                    (int(limit),),
                )
            ]
        finally:
            con.close()

    def _finalize_enrollment(
        self, item: Mapping[str, Any], *, opaque_tag: str | None = None
    ) -> dict[str, Any]:
        current = self.repository.get_item(int(item["id"]))
        state = str(current["state"])
        if state not in {"enrolled", "enrolled_hold"}:
            raise ValueError("state_conflict")
        tag = str(opaque_tag or current.get("qbt_precheck_tag") or "")
        if not tag:
            return current
        snapshot = self._managed_snapshot(current)
        tags = self._tags(snapshot)
        guard = lambda: self._finalization_guard(int(current["id"]), state, tag)
        if state == "enrolled_hold" and "hold" not in tags:
            if not self.gateway.add_tags(
                str(current["qbt_hash"]), "hold", guard=guard
            ):
                raise ValueError("qbt_write_fenced")
        if state == "enrolled" and "hold" in tags:
            if not self.gateway.remove_tags(
                str(current["qbt_hash"]), "hold", guard=guard
            ):
                raise ValueError("qbt_write_fenced")
        if tag in self._tags(self._managed_snapshot(current)):
            if not self.gateway.remove_tags(
                str(current["qbt_hash"]), tag, guard=guard
            ):
                raise ValueError("qbt_write_fenced")
        return self.repository.finalize_enrollment_marker(
            int(current["id"]), state, tag
        )

    def _release_enrolled_hold(
        self, item: Mapping[str, Any], approval_generation: int
    ) -> dict[str, Any]:
        current = self.repository.get_item(int(item["id"]))
        snapshot = self._managed_snapshot(current)
        if "hold" in self._tags(snapshot):
            if not self.gateway.remove_tags(
                str(current["qbt_hash"]),
                "hold",
                guard=lambda: self._managed_release_guard(
                    int(current["id"]), approval_generation
                ),
            ):
                raise ValueError("qbt_write_fenced")
        return self.repository.get_item(int(current["id"]))

    def _claim_or_renew(self, item_id: int, now: int) -> dict[str, Any]:
        current = self.repository.get_item(item_id)
        if (
            str(current.get("metadata_lease_owner") or "") == self.owner
            and int(current.get("metadata_lease_until") or 0) > now
            and int(current.get("metadata_lease_generation") or 0) > 0
        ):
            return self.repository.renew_metadata_lease(
                item_id,
                self.owner,
                int(current["metadata_lease_generation"]),
                now + self.lease_sec,
            )
        return self.repository.claim_metadata_lease(
            item_id, self.owner, now + self.lease_sec
        )

    def _opaque_owned_snapshot(
        self, item: Mapping[str, Any], *, missing_ok: bool = False
    ) -> dict[str, Any] | None:
        torrent_hash = str(item.get("qbt_hash") or "").lower()
        tag = str(item.get("qbt_precheck_tag") or "")
        snapshot = self.gateway.torrent_info(torrent_hash)
        if not str(snapshot.get("state") or "").strip():
            if missing_ok:
                return None
            raise ValueError("qbt_precheck_missing")
        if str(snapshot.get("hash") or "").lower() != torrent_hash:
            raise ValueError("qbt_precheck_hash_mismatch")
        if not tag or tag not in self._tags(snapshot):
            raise ValueError("qbt_precheck_tag_mismatch")
        return dict(snapshot)

    def _require_stopped(self, item: Mapping[str, Any]) -> dict[str, Any]:
        snapshot = self._opaque_owned_snapshot(item)
        if str(snapshot.get("state") or "").strip().lower() not in self._STOPPED_STATES:
            raise ValueError("qbt_stop_not_observed")
        return snapshot

    def _owned_snapshot(
        self, item: Mapping[str, Any], *, missing_ok: bool = False
    ) -> dict[str, Any] | None:
        snapshot = self._opaque_owned_snapshot(item, missing_ok=missing_ok)
        if snapshot is None:
            return None
        if str(snapshot.get("state") or "").strip().lower() not in self._STOPPED_STATES:
            raise ValueError("qbt_precheck_not_stopped")
        return snapshot

    def _managed_snapshot(self, item: Mapping[str, Any]) -> dict[str, Any]:
        torrent_hash = str(item.get("qbt_hash") or "").lower()
        snapshot = self.gateway.torrent_info(torrent_hash)
        if str(snapshot.get("hash") or "").lower() != torrent_hash:
            raise ValueError("qbt_enrolled_hash_mismatch")
        if str(snapshot.get("category") or "") != "auto" or "checked" not in self._tags(snapshot):
            raise ValueError("qbt_enrolled_ownership")
        return dict(snapshot)

    def _enrollment_guard(
        self, item_id: int, token: tuple[str, int], torrent_hash: str, tag: str
    ) -> bool:
        try:
            current = self.repository.get_item(item_id)
            if (
                current.get("state") != "enrolling"
                or str(current.get("metadata_lease_owner") or "") != token[0]
                or int(current.get("metadata_lease_generation") or 0) != token[1]
                or int(current.get("metadata_lease_until") or 0) <= int(self.now())
                or str(current.get("qbt_hash") or "").lower() != str(torrent_hash).lower()
                or str(current.get("qbt_precheck_tag") or "") != tag
            ):
                return False
            return self._opaque_owned_snapshot(current) is not None
        except Exception:
            return False

    def _lease_owned_guard(self, item_id: int, token: tuple[str, int]) -> bool:
        try:
            current = self.repository.get_item(item_id)
            if (
                current.get("state") not in {"prechecking", "enrolling"}
                or str(current.get("metadata_lease_owner") or "") != token[0]
                or int(current.get("metadata_lease_generation") or 0) != token[1]
                or int(current.get("metadata_lease_until") or 0) <= int(self.now())
            ):
                return False
            return self._opaque_owned_snapshot(current) is not None
        except Exception:
            return False

    def _approval_owned_guard(self, item_id: int, generation: int, state: str) -> bool:
        try:
            current = self.repository.get_item(item_id)
            return (
                current["state"] == state
                and int(current["approval_generation"]) == int(generation)
                and self._owned_snapshot(current) is not None
            )
        except Exception:
            return False

    def _managed_release_guard(self, item_id: int, generation: int) -> bool:
        try:
            current = self.repository.get_item(item_id)
            return (
                current["state"] == "enrolled"
                and bool(current.get("approved_by"))
                and int(current["approval_generation"]) == int(generation)
                and "hold" in self._tags(self._managed_snapshot(current))
            )
        except Exception:
            return False

    def _finalization_guard(self, item_id: int, state: str, tag: str) -> bool:
        try:
            current = self.repository.get_item(item_id)
            return (
                current["state"] == state
                and str(current.get("qbt_precheck_tag") or "") == tag
                and self._managed_snapshot(current) is not None
            )
        except Exception:
            return False

    @staticmethod
    def _approval_token(
        item: Mapping[str, Any], generation: int, expected_state: str
    ) -> None:
        if item.get("state") != expected_state:
            raise ValueError("state_conflict")
        if int(item.get("approval_generation") or 0) != int(generation):
            raise ValueError("approval_generation_conflict")

    @staticmethod
    def _tags(snapshot: Mapping[str, Any]) -> set[str]:
        return {
            part.strip()
            for part in str(snapshot.get("tags") or "").split(",")
            if part.strip()
        }
