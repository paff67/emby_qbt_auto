from __future__ import annotations
import json
import re
import sqlite3
import time
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Iterable, Protocol

from .db import readonly_connect, write_transaction
from .io_governor import JobPriority
from .runtime import TorrentJobRepository
from .observability import redact
@dataclass(frozen=True)
class UploadedFile:
    remote_path: str; size: int; duration_sec: int | None = None
@dataclass(frozen=True)
class PipelineRun:
    media_group_key: str; state: str


class FilenameNormalizerProtocol(Protocol):
    def normalize(self, raw_filename: str) -> dict: ...


class FallbackFilenameNormalizer:
    _JAV_ID = re.compile(r"(?i)([A-Z]{2,10})[-_ ]?(\d{2,6})")

    def normalize(self, raw_filename: str) -> dict:
        stem = PurePosixPath(raw_filename).stem
        match = self._JAV_ID.search(stem)
        if match:
            return {
                "normalized_id": f"{match.group(1).upper()}-{match.group(2)}",
                "confidence": 0.85,
                "raw_basename": stem,
                "reason": "fallback_jav_id_matched",
            }
        return {"normalized_id": "", "confidence": 0.0, "raw_basename": stem, "reason": "normalizer_not_configured"}


_MULTI_PART = re.compile(r"(?i)(?:[._ -]?(?:cd|disc|disk|part|pt)[._ -]?\d{1,2}|[上下]|前編|後編)$")
def media_group_key_from_remote(remote_path: str) -> str:
    path = remote_path.split(":", 1)[1] if ":" in remote_path else remote_path
    parts = [p for p in PurePosixPath(path).parts if p not in {"/", ""}]
    if len(parts) >= 2 and parts[-2]: return parts[-2]
    stem = PurePosixPath(path).stem
    return _MULTI_PART.sub("", stem).strip(" ._-") or stem
class MediaPipeline:
    def __init__(self, backfill, upload_queue, emby, emby_prefix: str = "/media/gcrypt"):
        self.backfill = backfill; self.upload_queue = upload_queue; self.emby = emby; self.emby_prefix = emby_prefix.rstrip("/")
    def handle_upload_verified(self, manifest_id: str, files: Iterable[UploadedFile]) -> PipelineRun:
        valid = [f for f in files if f.size >= 50 * 1024**2 and (f.duration_sec is None or f.duration_sec >= 60)]
        if not valid: return PipelineRun("", "content_gate_failed")
        key = media_group_key_from_remote(valid[0].remote_path); scrape = self.backfill.scrape_one(key, manifest_id)
        if scrape.get("status") == "sidecar_verified":
            self.upload_queue.enqueue("sidecar_upload", {"media_group_key": key, "manifest_id": manifest_id, "artifacts": scrape.get("artifacts", [])}); state = "SidecarVerified"
        else: state = "PassthroughAllowed"
        self.emby.media_updated(f"{self.emby_prefix}/{key}"); return PipelineRun(key, state)


_MEDIA_EXTS = {".mp4", ".mkv", ".avi", ".mov", ".wmv", ".m4v", ".ts", ".webm", ".flv", ".mpg", ".mpeg", ".iso"}
_JUNK_NAME = re.compile(r"(?i)(最新地址|收藏不迷路|官方指定|博彩|赌场|直播|telegram|996gg\.cc)")


def _connect(path) -> sqlite3.Connection:
    return readonly_connect(path)


def _remote_path_without_remote(remote_path: str) -> str:
    return remote_path.split(":", 1)[1] if ":" in remote_path else remote_path


def _emby_dir_from_remote(remote_path: str, emby_prefix: str) -> str:
    path = PurePosixPath(_remote_path_without_remote(remote_path))
    parent = str(path.parent).strip("/")
    if not parent or parent == ".":
        raise ValueError("media refresh path must be a single group directory, not library root")
    return f"{emby_prefix.rstrip('/')}/{parent}".rstrip("/")


def _remote_parent(remote_path: str) -> str:
    remote, path = remote_path.split(":", 1) if ":" in remote_path else ("gcrypt", remote_path)
    parent = str(PurePosixPath(path).parent).strip("/")
    return f"{remote}:/{parent}" if parent else f"{remote}:"


class MediaPipelineService:
    """Persistent UploadVerified -> media group -> sidecar/passthrough -> Emby refresh queue.

    This service is intentionally orchestration-only: it records durable state and
    creates UploadWorker jobs.  It does not call rclone or Emby directly.
    """

    def __init__(
        self,
        state_db,
        backfill,
        *,
        emby_prefix: str = "/media/gcrypt",
        debounce_sec: int = 300,
        max_debounce_wait_sec: int = 900,
        normalizer: FilenameNormalizerProtocol | None = None,
        min_normalize_confidence: float = 0.8,
        allow_unrecognized_passthrough: bool = True,
        required_sidecar_outputs: tuple[str, ...] = ("nfo", "poster", "fanart"),
        now=None,
    ):
        self.state_db = state_db
        self.backfill = backfill
        self.emby_prefix = emby_prefix.rstrip("/")
        self.debounce_sec = int(debounce_sec)
        self.max_debounce_wait_sec = int(max_debounce_wait_sec)
        self.normalizer = normalizer or FallbackFilenameNormalizer()
        self.min_normalize_confidence = float(min_normalize_confidence)
        self.allow_unrecognized_passthrough = bool(allow_unrecognized_passthrough)
        self.required_sidecar_outputs = tuple(required_sidecar_outputs)
        self.now = now or (lambda: int(time.time()))
        self.jobs = TorrentJobRepository(state_db, now=self.now)

    def handle_upload_verified(self, manifest_id: str, files: Iterable[UploadedFile]) -> PipelineRun:
        valid = [f for f in files if self._content_gate_allows(f)]
        if not valid:
            return PipelineRun("", "content_gate_failed")

        primary = valid[0]
        raw_filename = PurePosixPath(_remote_path_without_remote(primary.remote_path)).name
        normalize_result = self._normalize_filename(raw_filename)
        normalize_confidence = self._normalize_confidence(normalize_result)
        normalized_id = str(normalize_result.get("normalized_id") or "").strip()
        normalize_high_confidence = bool(normalized_id) and normalize_confidence >= self.min_normalize_confidence
        fallback_key = media_group_key_from_remote(primary.remote_path)
        key = normalized_id if normalize_high_confidence else fallback_key
        emby_dir = f"{self.emby_prefix}/{key}".rstrip("/") if normalize_high_confidence else _emby_dir_from_remote(primary.remote_path, self.emby_prefix)
        group_id = self._ensure_media_group(key, emby_dir, normalized_id=normalized_id if normalize_high_confidence else fallback_key)
        run_id = self._ensure_pipeline_run(str(manifest_id), group_id)

        existing_sidecar_state = self._sidecar_manifest_state(group_id)
        queue_refresh = False
        if existing_sidecar_state == "sidecar_verified":
            state = "SidecarVerified"
            metadata_policy = "sidecar"
            metadata_quality = "normalized" if normalize_high_confidence else "cached"
            passthrough_reason = None
            missing_outputs: list[str] = []
            queue_refresh = True
        elif existing_sidecar_state in {"local_sidecar_validated", "sidecar_uploading"}:
            state = "SidecarUploadQueued"
            metadata_policy = "sidecar"
            metadata_quality = "normalized" if normalize_high_confidence else "cached"
            passthrough_reason = None
            missing_outputs = []
        elif not normalize_high_confidence:
            if not self.allow_unrecognized_passthrough:
                state = "ManualReview"
                metadata_policy = "manual_review"
                metadata_quality = "raw"
                passthrough_reason = "normalize_low_confidence"
            else:
                state = "PassthroughAllowed"
                metadata_policy = "passthrough"
                metadata_quality = "raw"
                passthrough_reason = "normalize_low_confidence"
                queue_refresh = True
            missing_outputs = []
        else:
            scrape = self.backfill.scrape_one(key, str(manifest_id))
            valid_artifacts, missing_outputs = self._validate_sidecar_artifacts(scrape.get("artifacts") or [])
            if scrape.get("status") == "sidecar_verified" and valid_artifacts:
                manifest_row_id = self._record_sidecar_manifest(group_id, scrape, state="local_sidecar_validated", missing_outputs=[])
                self._enqueue_sidecar_uploads(key, manifest_row_id, scrape.get("artifacts") or [])
                state = "SidecarUploadQueued"
                metadata_policy = "sidecar"
                metadata_quality = "normalized"
                passthrough_reason = None
            else:
                self._record_sidecar_manifest(group_id, scrape, state="sidecar_verify_failed", missing_outputs=missing_outputs)
                state = "PassthroughAllowed"
                metadata_policy = "passthrough"
                metadata_quality = "raw"
                passthrough_reason = "sidecar_verify_failed"
                queue_refresh = True

        self._set_pipeline_state(
            run_id,
            state,
            metadata_policy=metadata_policy,
            metadata_quality=metadata_quality,
            passthrough_reason=passthrough_reason,
            normalize_confidence=normalize_confidence,
            normalize_result=normalize_result,
            missing_outputs=missing_outputs,
        )
        if queue_refresh:
            self._queue_emby_refresh(emby_dir, key, manifest_id, state)
        return PipelineRun(key, state)

    def _normalize_filename(self, raw_filename: str) -> dict:
        try:
            result = self.normalizer.normalize(raw_filename)
            return result if isinstance(result, dict) else {"normalized_id": "", "confidence": 0.0, "reason": "normalizer_invalid_result"}
        except Exception as exc:
            return {"normalized_id": "", "confidence": 0.0, "raw_filename": raw_filename, "reason": "normalizer_failed", "error": redact(str(exc))[:500]}

    @staticmethod
    def _normalize_confidence(result: dict) -> float:
        raw = result.get("confidence", 0.0)
        if isinstance(raw, str):
            lowered = raw.strip().lower()
            if lowered in {"high", "trusted"}:
                return 1.0
            if lowered in {"medium", "med"}:
                return 0.7
            if lowered in {"low", "unknown"}:
                return 0.2
        try:
            return float(raw)
        except (TypeError, ValueError):
            return 0.0

    def _content_gate_allows(self, file: UploadedFile) -> bool:
        path = PurePosixPath(_remote_path_without_remote(file.remote_path))
        name = path.name
        if _JUNK_NAME.search(name):
            return False
        if path.suffix.lower() not in _MEDIA_EXTS:
            return False
        if file.size < 50 * 1024**2:
            return False
        if file.duration_sec is not None and file.duration_sec < 60:
            return False
        return True

    def _ensure_media_group(self, key: str, emby_dir: str, *, normalized_id: str | None = None) -> int:
        now = int(self.now())
        def txn(con: sqlite3.Connection) -> int:
            con.execute(
                "insert or ignore into media_groups(media_group_key,normalized_id,emby_media_dir,created_at,updated_at) values(?,?,?,?,?)",
                (key, normalized_id or key, emby_dir, now, now),
            )
            con.execute("update media_groups set normalized_id=?, emby_media_dir=?, updated_at=? where media_group_key=?", (normalized_id or key, emby_dir, now, key))
            row = con.execute("select id from media_groups where media_group_key=?", (key,)).fetchone()
            return int(row["id"])

        return int(write_transaction(self.state_db, txn))

    def _ensure_pipeline_run(self, manifest_id: str, group_id: int) -> int:
        now = int(self.now())
        def txn(con: sqlite3.Connection) -> int:
            con.execute(
                "insert or ignore into media_pipeline_runs(upload_manifest_id,media_group_id,state,created_at,updated_at) values(?,?,?,?,?)",
                (manifest_id, group_id, "created", now, now),
            )
            row = con.execute(
                "select id from media_pipeline_runs where upload_manifest_id=? and media_group_id=?",
                (manifest_id, group_id),
            ).fetchone()
            return int(row["id"])

        return int(write_transaction(self.state_db, txn))

    def _set_pipeline_state(
        self,
        run_id: int,
        state: str,
        *,
        metadata_policy: str | None = None,
        metadata_quality: str | None = None,
        passthrough_reason: str | None = None,
        normalize_confidence: float | None = None,
        normalize_result: dict | None = None,
        missing_outputs: list[str] | None = None,
    ) -> None:
        write_transaction(
            self.state_db,
            lambda con: con.execute(
                "update media_pipeline_runs set state=?, metadata_policy=?, metadata_quality=?, passthrough_reason=?, "
                "normalize_confidence=?, normalize_result_json=?, missing_outputs_json=?, updated_at=? where id=?",
                (
                    state,
                    metadata_policy,
                    metadata_quality,
                    passthrough_reason,
                    normalize_confidence,
                    json.dumps(normalize_result or {}, ensure_ascii=False),
                    json.dumps(missing_outputs or [], ensure_ascii=False),
                    int(self.now()),
                    run_id,
                ),
            ),
        )

    def _sidecar_already_verified(self, group_id: int) -> bool:
        return self._sidecar_manifest_state(group_id) == "sidecar_verified"

    def _sidecar_manifest_state(self, group_id: int) -> str | None:
        con = _connect(self.state_db)
        row = con.execute(
            "select state from sidecar_manifests where media_group_id=? and state in ('sidecar_verified','local_sidecar_validated','sidecar_uploading') "
            "order by case state when 'sidecar_verified' then 0 when 'local_sidecar_validated' then 1 else 2 end, id limit 1",
            (group_id,),
        ).fetchone()
        con.close()
        return str(row["state"]) if row else None

    def _record_sidecar_manifest(self, group_id: int, scrape: dict, *, state: str, missing_outputs: list[str]) -> int:
        now = int(self.now())
        artifacts = scrape.get("artifacts") or []
        artifact_manifest = {
            "artifact_manifest": scrape.get("artifact_manifest"),
            "artifacts": artifacts,
            "returncode": scrape.get("returncode"),
            "scraper_log_tail": scrape.get("scraper_log_tail"),
            "required_outputs": list(self.required_sidecar_outputs),
            "missing_outputs": list(missing_outputs),
        }
        artifact_total_bytes = sum(int(a.get("size") or 0) for a in artifacts if isinstance(a, dict))
        def txn(con: sqlite3.Connection) -> int:
            cur = con.execute(
                "insert into sidecar_manifests("
                "media_group_id,staging_dir,artifacts_json,state,created_at,updated_at,"
                "local_artifact_dir,artifact_manifest_json,artifact_total_bytes,scraper_exit_code,scraper_log_tail,media_group_key_snapshot"
                ") values(?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    group_id,
                    str(scrape.get("staging_dir") or ""),
                    json.dumps(artifacts, ensure_ascii=False),
                    state,
                    now,
                    now,
                    str(scrape.get("staging_dir") or ""),
                    json.dumps(artifact_manifest, ensure_ascii=False),
                    int(artifact_total_bytes),
                    scrape.get("returncode"),
                    scrape.get("scraper_log_tail"),
                    str(scrape.get("media_group_key") or ""),
                ),
            )
            return int(cur.lastrowid)

        return int(write_transaction(self.state_db, txn))

    def _validate_sidecar_artifacts(self, artifacts: list) -> tuple[bool, list[str]]:
        if not artifacts:
            return False, list(self.required_sidecar_outputs)
        seen: set[str] = set()
        for artifact in artifacts:
            if not isinstance(artifact, dict):
                return False, list(self.required_sidecar_outputs)
            local = str(artifact.get("local") or "")
            remote = str(artifact.get("remote") or "")
            if not local or local.startswith("gcrypt:"):
                return False, list(self.required_sidecar_outputs)
            if not remote.startswith("gcrypt:/"):
                return False, list(self.required_sidecar_outputs)
            if int(artifact.get("size") or 0) <= 0:
                return False, list(self.required_sidecar_outputs)
            name = PurePosixPath(remote).name.lower()
            suffix = PurePosixPath(name).suffix.lower()
            stem = PurePosixPath(name).stem.lower()
            if suffix == ".nfo":
                seen.add("nfo")
            if suffix in {".jpg", ".jpeg", ".png", ".webp"}:
                if stem in {"poster", "folder"} or stem.endswith("-poster"):
                    seen.add("poster")
                if stem in {"fanart", "backdrop"} or stem.endswith("-fanart"):
                    seen.add("fanart")
                if stem in {"thumb", "thumbnail"} or stem.endswith("-thumb"):
                    seen.add("thumb")
        missing = [item for item in self.required_sidecar_outputs if item not in seen]
        return not missing, missing

    def _enqueue_sidecar_uploads(self, key: str, sidecar_manifest_id: int, artifacts: list[dict]) -> None:
        for artifact in artifacts:
            payload = {
                "local": str(artifact["local"]),
                "remote": str(artifact["remote"]),
                "size": int(artifact.get("size") or 0),
                "full_torrent": False,
                "media_group_key": key,
                "sidecar_manifest_id": sidecar_manifest_id,
                "allow_unrecognized_passthrough": self.allow_unrecognized_passthrough,
            }
            self.jobs.enqueue(None, None, "sidecar_upload", payload, priority=int(JobPriority.SIDECAR_UPLOAD))

    def _queue_emby_refresh(self, emby_dir: str, key: str, manifest_id: str, state: str) -> None:
        if not emby_dir.startswith(self.emby_prefix + "/"):
            raise ValueError("emby refresh path outside media prefix")
        now = int(self.now())
        earliest = now + self.debounce_sec
        max_run = now + self.max_debounce_wait_sec
        payload = {"media_group_key": key, "upload_manifest_id": str(manifest_id), "trigger_state": state}
        def txn(con: sqlite3.Connection) -> None:
            row = con.execute(
                "select * from emby_refresh_tasks where emby_media_dir=? and state='queued' order by id limit 1",
                (emby_dir,),
            ).fetchone()
            if row:
                con.execute(
                    "update emby_refresh_tasks set earliest_run_at=?, max_run_at=?, payload_json=?, updated_at=? where id=?",
                    (min(int(row["max_run_at"]), earliest), int(row["max_run_at"]), json.dumps(payload, ensure_ascii=False), now, row["id"]),
                )
            else:
                con.execute(
                    "insert into emby_refresh_tasks(emby_media_dir,state,earliest_run_at,max_run_at,payload_json,created_at,updated_at) values(?,?,?,?,?,?,?)",
                    (emby_dir, "queued", earliest, max_run, json.dumps(payload, ensure_ascii=False), now, now),
                )

        write_transaction(self.state_db, txn)


class MediaPipelineJobRunner:
    def __init__(self, repo: TorrentJobRepository, service: MediaPipelineService, retry_delay_sec: int = 300):
        self.repo = repo
        self.service = service
        self.retry_delay_sec = int(retry_delay_sec)

    def run_next(self) -> int | None:
        row = self.repo.claim_next("media_pipeline")
        if not row:
            return None
        try:
            payload = json.loads(row["payload_json"] or "{}")
            files = [
                UploadedFile(
                    remote_path=str(item["remote_path"]),
                    size=int(item.get("size") or 0),
                    duration_sec=item.get("duration_sec"),
                )
                for item in payload.get("files", [])
            ]
            self.service.handle_upload_verified(str(payload.get("upload_manifest_id") or f"media-job-{row['id']}"), files)
            self.repo.update_state(int(row["id"]), "done", exit_code=0)
        except Exception as exc:
            self.repo.schedule_retry(int(row["id"]), redact(str(exc))[:500], exit_code=1, delay_sec=self.retry_delay_sec)
        return int(row["id"])


class EmbyRefreshWorker:
    def __init__(self, state_db, emby, *, now=None, media_prefix: str = "/media/gcrypt", retry_delay_sec: int = 60):
        self.state_db = state_db
        self.emby = emby
        self.now = now or (lambda: int(time.time()))
        self.media_prefix = media_prefix.rstrip("/")
        self.retry_delay_sec = max(1, int(retry_delay_sec))

    def run_next(self) -> int | None:
        row = self._claim_next()
        if not row:
            return None
        task_id = int(row["id"])
        path = str(row["emby_media_dir"] or "").rstrip("/")
        try:
            self._validate_path(path)
        except ValueError as exc:
            self._finish(task_id, "blocked", redact(str(exc))[:500])
            return task_id
        try:
            self.emby.media_updated(path)
            self._finish(task_id, "done", None)
        except Exception as exc:
            error = redact(str(exc))[:500]
            if self._is_transient(exc):
                self._retry_or_fail(row, error)
            else:
                self._finish(task_id, "blocked", error)
        return task_id

    def _claim_next(self) -> dict | None:
        now = int(self.now())
        def txn(con: sqlite3.Connection) -> dict | None:
            row = con.execute(
                "select * from emby_refresh_tasks where attempts<max_attempts and state in ('queued','retry_wait') and "
                "((state='retry_wait' and (next_run_at is null or next_run_at<=?)) or "
                "(state='queued' and ((earliest_run_at is not null and earliest_run_at<=?) or (max_run_at is not null and max_run_at<=?)))) "
                "order by coalesce(max_run_at, earliest_run_at, created_at), id limit 1",
                (now, now, now),
            ).fetchone()
            if not row:
                return None
            con.execute(
                "update emby_refresh_tasks set state='running',attempts=attempts+1,updated_at=? "
                "where id=? and state in ('queued','retry_wait')",
                (now, row["id"]),
            )
            return dict(con.execute("select * from emby_refresh_tasks where id=?", (row["id"],)).fetchone())

        return write_transaction(self.state_db, txn)

    def peek_next(self) -> dict | None:
        now = int(self.now())
        con = _connect(self.state_db)
        row = con.execute(
            "select * from emby_refresh_tasks where attempts<max_attempts and state in ('queued','retry_wait') and "
            "((state='retry_wait' and (next_run_at is null or next_run_at<=?)) or "
            "(state='queued' and ((earliest_run_at is not null and earliest_run_at<=?) or (max_run_at is not null and max_run_at<=?)))) "
            "order by coalesce(max_run_at, earliest_run_at, created_at), id limit 1",
            (now, now, now),
        ).fetchone()
        out = dict(row) if row else None
        con.close()
        return out

    def _finish(self, task_id: int, state: str, error: str | None) -> None:
        write_transaction(
            self.state_db,
            lambda con: con.execute(
                "update emby_refresh_tasks set state=?, last_error=?, updated_at=? where id=?",
                (state, error, int(self.now()), task_id),
            ),
        )

    def _validate_path(self, path: str) -> None:
        if path == self.media_prefix or not path.startswith(self.media_prefix + "/"):
            raise ValueError("refresh path too broad or outside media prefix")

    def _retry_or_fail(self, row: dict, error: str) -> None:
        attempts = int(row.get("attempts") or 0)
        max_attempts = int(row.get("max_attempts") or 1)
        now = int(self.now())
        if attempts >= max_attempts:
            self._finish(int(row["id"]), "failed", error)
            return
        delay = min(3600, self.retry_delay_sec * (2 ** max(0, attempts - 1)))
        write_transaction(
            self.state_db,
            lambda con: con.execute(
                "update emby_refresh_tasks set state='retry_wait',next_run_at=?,last_error=?,updated_at=? where id=?",
                (now + delay, error, now, int(row["id"])),
            ),
        )

    @staticmethod
    def _is_transient(exc: Exception) -> bool:
        if isinstance(exc, (TimeoutError, ConnectionError)):
            return True
        status = getattr(exc, "status_code", None)
        if status is not None:
            try:
                return int(status) >= 500
            except (TypeError, ValueError):
                pass
        return bool(re.search(r"\b5\d\d\b", str(exc)))
