from __future__ import annotations

import math
import re
import sqlite3
import time
import unicodedata
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Iterable, Mapping, Protocol, Sequence

from .db import readonly_connect, write_transaction
from .media import FallbackFilenameNormalizer, _MEDIA_EXTS


_REMOTE_SOURCE = "backfill"
_DEFAULT_TTL_SEC = 6 * 3600
_MAX_REMOTE_MATCHES = 20
_MAX_EVIDENCE = 20
_MAX_PATH_CHARS = 1024
_MAX_NAME_CHARS = 512
_MEDIA_ID = re.compile(r"(?:[A-Z0-9]{1,16}-){1,2}\d{2,9}")
_NON_PRIMARY_VIDEO = re.compile(
    r"(?i)(?:^|[\\/._ -])(?:sample|trailer|preview|teaser)(?:$|[\\/._ -])"
)


class FilenameNormalizer(Protocol):
    def normalize(self, raw_filename: str) -> Mapping[str, Any]: ...


@dataclass(frozen=True)
class RemoteIndexRefreshResult:
    status: str
    row_count: int
    generation: int
    refreshed_at: int | None
    error_code: str | None = None


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
    video_path: str
    normalized_id: str
    size: int | None
    status: str
    source: str
    updated_at: int
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


def _known_positive_size(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        size = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return size if size > 0 else None


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
        if _NON_PRIMARY_VIDEO.search(normalized_path):
            continue
        raw_index = _mapping_value(item, "index")
        if raw_index is None:
            index = ordinal
        elif isinstance(raw_index, bool):
            continue
        else:
            try:
                index = int(raw_index)
            except (TypeError, ValueError, OverflowError):
                continue
            if index < 0:
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
    ):
        if isinstance(ttl_sec, bool) or not isinstance(ttl_sec, int) or ttl_sec <= 0:
            raise ValueError("ttl_sec")
        self.state_db = Path(state_db)
        self.backfill_db = Path(backfill_db) if backfill_db is not None else None
        self.now = now or (lambda: int(time.time()))
        self.ttl_sec = ttl_sec

    def replace_rows(self, rows: Iterable[Mapping[str, Any]]) -> RemoteIndexRefreshResult:
        now = int(self.now())
        generation, _ = self._begin_refresh(now, force=True)
        return self._apply_snapshot(generation, now, self._prepare_rows(rows, updated_at=now))

    def refresh(self, *, force: bool = False) -> RemoteIndexRefreshResult:
        now = int(self.now())
        generation, fresh = self._begin_refresh(now, force=force)
        if fresh is not None:
            return fresh
        if self.backfill_db is None:
            return self._preserve(generation, now, "source_unconfigured")
        try:
            rows = self._prepare_rows(self._read_source_rows(), updated_at=now)
        except sqlite3.DatabaseError as exc:
            message = str(exc).lower()
            code = "source_schema_error" if "no such table" in message or "no such column" in message else "source_unavailable"
            return self._preserve(generation, now, code)
        except (OSError, ValueError, TypeError):
            return self._preserve(generation, now, "source_unavailable")
        return self._apply_snapshot(generation, now, rows)

    def matches(self, normalized_id: str) -> tuple[dict[str, Any], ...]:
        media_id = _canonical_media_id(normalized_id)
        if media_id is None:
            return ()
        con = readonly_connect(self.state_db)
        try:
            rows = con.execute(
                "select video_path,normalized_id,size,raw_basename,status,source,updated_at "
                "from remote_media_index where normalized_id=? "
                "order by video_path limit ?",
                (media_id, _MAX_REMOTE_MATCHES),
            ).fetchall()
            return tuple(dict(row) for row in rows)
        finally:
            con.close()

    def _read_source_rows(self) -> Sequence[sqlite3.Row]:
        assert self.backfill_db is not None
        con = readonly_connect(self.backfill_db)
        try:
            return list(con.execute(self.SOURCE_QUERY))
        finally:
            con.close()

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
                    "fresh",
                    int(row["row_count"]),
                    int(row["applied_generation"]),
                    refreshed_at,
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
        self, generation: int, now: int, rows: tuple[tuple[Any, ...], ...]
    ) -> RemoteIndexRefreshResult:
        def txn(con: sqlite3.Connection):
            state = con.execute(
                "select requested_generation,refreshed_at,row_count,applied_generation "
                "from remote_media_index_refresh_state where source=?",
                (_REMOTE_SOURCE,),
            ).fetchone()
            if state is None or int(state["requested_generation"]) != generation:
                return RemoteIndexRefreshResult(
                    "superseded",
                    int(state["row_count"]) if state else 0,
                    generation,
                    int(state["refreshed_at"]) if state and state["refreshed_at"] is not None else None,
                )
            con.execute("delete from remote_media_index where source=?", (_REMOTE_SOURCE,))
            con.executemany(
                "insert into remote_media_index("
                "video_path,normalized_id,size,raw_basename,status,source,updated_at) "
                "values(?,?,?,?,?,?,?)",
                rows,
            )
            con.execute(
                "update remote_media_index_refresh_state set applied_generation=?,"
                "refreshed_at=?,row_count=?,last_attempt_at=?,last_result='refreshed' "
                "where source=? and requested_generation=?",
                (generation, now, len(rows), now, _REMOTE_SOURCE, generation),
            )
            return RemoteIndexRefreshResult("refreshed", len(rows), generation, now)

        return write_transaction(self.state_db, txn)

    def _preserve(
        self, generation: int, now: int, error_code: str
    ) -> RemoteIndexRefreshResult:
        def txn(con: sqlite3.Connection):
            state = con.execute(
                "select requested_generation,applied_generation,refreshed_at,row_count "
                "from remote_media_index_refresh_state where source=?",
                (_REMOTE_SOURCE,),
            ).fetchone()
            if state is None or int(state["requested_generation"]) != generation:
                return RemoteIndexRefreshResult(
                    "superseded",
                    int(state["row_count"]) if state else 0,
                    generation,
                    int(state["refreshed_at"]) if state and state["refreshed_at"] is not None else None,
                )
            con.execute(
                "update remote_media_index_refresh_state set last_attempt_at=?,last_result=? "
                "where source=? and requested_generation=?",
                (now, error_code, _REMOTE_SOURCE, generation),
            )
            return RemoteIndexRefreshResult(
                "preserved",
                int(state["row_count"]),
                int(state["applied_generation"]),
                int(state["refreshed_at"]) if state["refreshed_at"] is not None else None,
                error_code,
            )

        return write_transaction(self.state_db, txn)

    @staticmethod
    def _prepare_rows(
        rows: Iterable[Mapping[str, Any]], *, updated_at: int
    ) -> tuple[tuple[Any, ...], ...]:
        by_path: dict[str, tuple[Any, ...]] = {}
        for raw in rows:
            if not hasattr(raw, "get"):
                raw = dict(raw)
            video_path = _safe_text(raw.get("video_path"), limit=_MAX_PATH_CHARS)
            normalized_id = _canonical_media_id(raw.get("normalized_id"))
            if video_path is None or normalized_id is None:
                continue
            raw_size = raw.get("size")
            size = _known_positive_size(raw_size)
            if raw_size in (0, "0"):
                size = None
            raw_basename = _safe_text(
                raw.get("raw_basename"), limit=255, allow_empty=True, truncate=True
            ) or ""
            status = _safe_text(
                raw.get("status"), limit=64, allow_empty=True, truncate=True
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
        return tuple(by_path[path] for path in sorted(by_path))


class DuplicateMatcher:
    def __init__(
        self,
        state_db: str | Path,
        *,
        normalizer: FilenameNormalizer | FilenameNormalizerAdapter | None = None,
        size_tolerance_ratio: float = 0.15,
    ):
        if isinstance(size_tolerance_ratio, bool) or not isinstance(
            size_tolerance_ratio, (int, float)
        ):
            raise ValueError("size_tolerance_ratio")
        tolerance = float(size_tolerance_ratio)
        if not math.isfinite(tolerance) or not 0 <= tolerance <= 1:
            raise ValueError("size_tolerance_ratio")
        self.state_db = Path(state_db)
        self.normalizer = (
            normalizer
            if isinstance(normalizer, FilenameNormalizerAdapter)
            else FilenameNormalizerAdapter(normalizer)
        )
        self.size_tolerance_ratio = tolerance
        self.remote_index = RemoteMediaIndex(
            self.state_db, backfill_db=None, now=lambda: int(time.time())
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
                evidence=(f"identity:{identity[:128]}",),
            )

        normalized = self.normalizer.normalize(primary_name)
        warnings: list[str] = []
        evidence: list[str] = []
        fuzzy = self._safe_fuzzy_matches(fuzzy_matches)
        if fuzzy:
            warnings.append("fuzzy_name_only")
            evidence.extend(f"fuzzy:{value}" for value in fuzzy)
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

        rows = self.remote_index.matches(normalized.normalized_id)
        if not rows:
            return DuplicateDecision(
                "ready",
                "no_exact_remote_media_id",
                normalized.normalized_id,
                evidence=tuple(evidence[:_MAX_EVIDENCE]),
                warnings=tuple(dict.fromkeys(warnings)),
            )

        candidate_size = _known_positive_size(primary_size)
        matches: list[RemoteMediaMatch] = []
        any_close = False
        for row in rows:
            remote_size = _known_positive_size(row.get("size"))
            ratio: float | None = None
            close = False
            if candidate_size is not None and remote_size is not None:
                ratio = abs(candidate_size - remote_size) / max(candidate_size, remote_size)
                close = ratio <= self.size_tolerance_ratio
            any_close = any_close or close
            path = _safe_text(row.get("video_path"), limit=_MAX_PATH_CHARS) or "[invalid-path]"
            match = RemoteMediaMatch(
                video_path=path,
                normalized_id=normalized.normalized_id,
                size=remote_size,
                status=_safe_text(
                    row.get("status"), limit=64, allow_empty=True, truncate=True
                ) or "",
                source=_safe_text(
                    row.get("source"), limit=64, allow_empty=True, truncate=True
                ) or "",
                updated_at=int(row.get("updated_at") or 0),
                size_close=close,
                size_diff_ratio=ratio,
            )
            matches.append(match)
            ratio_text = "unknown" if ratio is None else f"{ratio:.6f}"
            evidence.append(f"remote:{path[:160]}:size={remote_size}:diff={ratio_text}")
        matches.sort(key=lambda row: row.video_path)
        return DuplicateDecision(
            "duplicate_remote" if any_close else "needs_confirmation",
            "exact_media_id_size_within_tolerance" if any_close else "exact_media_id_size_differs_or_unknown",
            normalized.normalized_id,
            matches=tuple(matches),
            evidence=tuple(evidence[:_MAX_EVIDENCE]),
            warnings=tuple(dict.fromkeys(warnings)),
        )

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
            if safe is None or safe in seen:
                continue
            seen.add(safe)
            result.append(safe)
            if len(result) >= _MAX_EVIDENCE:
                break
        return tuple(result)
