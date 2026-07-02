from __future__ import annotations
import json
import re
import sqlite3
import time
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Iterable

from .runtime import TorrentJobRepository
from .observability import redact
@dataclass(frozen=True)
class UploadedFile:
    remote_path: str; size: int; duration_sec: int | None = None
@dataclass(frozen=True)
class PipelineRun:
    media_group_key: str; state: str
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
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    return con


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
        now=None,
    ):
        self.state_db = state_db
        self.backfill = backfill
        self.emby_prefix = emby_prefix.rstrip("/")
        self.debounce_sec = int(debounce_sec)
        self.max_debounce_wait_sec = int(max_debounce_wait_sec)
        self.now = now or (lambda: int(time.time()))
        self.jobs = TorrentJobRepository(state_db, now=self.now)

    def handle_upload_verified(self, manifest_id: str, files: Iterable[UploadedFile]) -> PipelineRun:
        valid = [f for f in files if self._content_gate_allows(f)]
        if not valid:
            return PipelineRun("", "content_gate_failed")

        key = media_group_key_from_remote(valid[0].remote_path)
        emby_dir = _emby_dir_from_remote(valid[0].remote_path, self.emby_prefix)
        group_id = self._ensure_media_group(key, emby_dir)
        run_id = self._ensure_pipeline_run(str(manifest_id), group_id)

        if self._sidecar_already_verified(group_id):
            state = "SidecarVerified"
        else:
            scrape = self.backfill.scrape_one(key, str(manifest_id))
            if scrape.get("status") == "sidecar_verified" and self._valid_artifacts(scrape.get("artifacts") or []):
                manifest_row_id = self._record_sidecar_manifest(group_id, scrape)
                self._enqueue_sidecar_uploads(key, manifest_row_id, scrape.get("artifacts") or [])
                state = "SidecarVerified"
            else:
                state = "PassthroughAllowed"

        self._set_pipeline_state(run_id, state)
        self._queue_emby_refresh(emby_dir, key, manifest_id, state)
        return PipelineRun(key, state)

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

    def _ensure_media_group(self, key: str, emby_dir: str) -> int:
        now = int(self.now())
        con = _connect(self.state_db)
        con.execute(
            "insert or ignore into media_groups(media_group_key,normalized_id,emby_media_dir,created_at,updated_at) values(?,?,?,?,?)",
            (key, key, emby_dir, now, now),
        )
        con.execute("update media_groups set emby_media_dir=?, updated_at=? where media_group_key=?", (emby_dir, now, key))
        row = con.execute("select id from media_groups where media_group_key=?", (key,)).fetchone()
        con.commit()
        con.close()
        return int(row["id"])

    def _ensure_pipeline_run(self, manifest_id: str, group_id: int) -> int:
        now = int(self.now())
        con = _connect(self.state_db)
        con.execute(
            "insert or ignore into media_pipeline_runs(upload_manifest_id,media_group_id,state,created_at,updated_at) values(?,?,?,?,?)",
            (manifest_id, group_id, "created", now, now),
        )
        row = con.execute(
            "select id from media_pipeline_runs where upload_manifest_id=? and media_group_id=?",
            (manifest_id, group_id),
        ).fetchone()
        con.commit()
        con.close()
        return int(row["id"])

    def _set_pipeline_state(self, run_id: int, state: str) -> None:
        con = _connect(self.state_db)
        con.execute("update media_pipeline_runs set state=?, updated_at=? where id=?", (state, int(self.now()), run_id))
        con.commit()
        con.close()

    def _sidecar_already_verified(self, group_id: int) -> bool:
        con = _connect(self.state_db)
        row = con.execute(
            "select id from sidecar_manifests where media_group_id=? and state='sidecar_verified' order by id limit 1",
            (group_id,),
        ).fetchone()
        con.close()
        return row is not None

    def _record_sidecar_manifest(self, group_id: int, scrape: dict) -> int:
        now = int(self.now())
        con = _connect(self.state_db)
        cur = con.execute(
            "insert into sidecar_manifests(media_group_id,staging_dir,artifacts_json,state,created_at,updated_at) values(?,?,?,?,?,?)",
            (group_id, str(scrape.get("staging_dir") or ""), json.dumps(scrape.get("artifacts") or [], ensure_ascii=False), "sidecar_verified", now, now),
        )
        con.commit()
        con.close()
        return int(cur.lastrowid)

    def _valid_artifacts(self, artifacts: list) -> bool:
        if not artifacts:
            return False
        for artifact in artifacts:
            if not isinstance(artifact, dict):
                return False
            local = str(artifact.get("local") or "")
            remote = str(artifact.get("remote") or "")
            if not local or local.startswith("gcrypt:"):
                return False
            if not remote.startswith("gcrypt:/"):
                return False
        return True

    def _enqueue_sidecar_uploads(self, key: str, sidecar_manifest_id: int, artifacts: list[dict]) -> None:
        for artifact in artifacts:
            payload = {
                "local": str(artifact["local"]),
                "remote": str(artifact["remote"]),
                "size": int(artifact.get("size") or 0),
                "full_torrent": False,
                "media_group_key": key,
                "sidecar_manifest_id": sidecar_manifest_id,
            }
            self.jobs.enqueue(None, None, "sidecar_upload", payload, priority=20)

    def _queue_emby_refresh(self, emby_dir: str, key: str, manifest_id: str, state: str) -> None:
        if not emby_dir.startswith(self.emby_prefix + "/"):
            raise ValueError("emby refresh path outside media prefix")
        now = int(self.now())
        earliest = now + self.debounce_sec
        max_run = now + self.max_debounce_wait_sec
        payload = {"media_group_key": key, "upload_manifest_id": str(manifest_id), "trigger_state": state}
        con = _connect(self.state_db)
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
        con.commit()
        con.close()


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
    def __init__(self, state_db, emby, *, now=None, media_prefix: str = "/media/gcrypt"):
        self.state_db = state_db
        self.emby = emby
        self.now = now or (lambda: int(time.time()))
        self.media_prefix = media_prefix.rstrip("/")

    def run_next(self) -> int | None:
        row = self._claim_next()
        if not row:
            return None
        task_id = int(row["id"])
        path = str(row["emby_media_dir"] or "").rstrip("/")
        try:
            self._validate_path(path)
            self.emby.media_updated(path)
            self._finish(task_id, "done", None)
        except Exception as exc:
            self._finish(task_id, "blocked", redact(str(exc))[:500])
        return task_id

    def _claim_next(self) -> dict | None:
        now = int(self.now())
        con = _connect(self.state_db)
        row = con.execute(
            "select * from emby_refresh_tasks where state='queued' and "
            "((earliest_run_at is not null and earliest_run_at<=?) or (max_run_at is not null and max_run_at<=?)) "
            "order by coalesce(max_run_at, earliest_run_at, created_at), id limit 1",
            (now, now),
        ).fetchone()
        if not row:
            con.close()
            return None
        con.execute("update emby_refresh_tasks set state='running', updated_at=? where id=? and state='queued'", (now, row["id"]))
        con.commit()
        out = dict(con.execute("select * from emby_refresh_tasks where id=?", (row["id"],)).fetchone())
        con.close()
        return out

    def peek_next(self) -> dict | None:
        now = int(self.now())
        con = _connect(self.state_db)
        row = con.execute(
            "select * from emby_refresh_tasks where state='queued' and "
            "((earliest_run_at is not null and earliest_run_at<=?) or (max_run_at is not null and max_run_at<=?)) "
            "order by coalesce(max_run_at, earliest_run_at, created_at), id limit 1",
            (now, now),
        ).fetchone()
        out = dict(row) if row else None
        con.close()
        return out

    def _finish(self, task_id: int, state: str, error: str | None) -> None:
        con = _connect(self.state_db)
        con.execute(
            "update emby_refresh_tasks set state=?, last_error=?, updated_at=? where id=?",
            (state, error, int(self.now()), task_id),
        )
        con.commit()
        con.close()

    def _validate_path(self, path: str) -> None:
        if path == self.media_prefix or not path.startswith(self.media_prefix + "/"):
            raise ValueError("refresh path too broad or outside media prefix")
