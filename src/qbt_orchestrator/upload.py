from __future__ import annotations
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any


def _rel(path: str) -> str:
    return str(path or "").replace("\\", "/").lstrip("/")


@dataclass(frozen=True)
class UploadJob:
    hash: str
    batch_id: int | None
    local: str
    remote: str
    size: int
    full_torrent: bool = False
    files: list[dict[str, Any]] | None = None
    copy_mode: str = "copy"


@dataclass(frozen=True)
class UploadResult:
    state: str; remote_verified: bool; cleanup_allowed: bool


class RcloneUploadWorker:
    def __init__(self, rclone, executor): self.rclone = rclone; self.executor = executor

    def run_once(self, job: UploadJob) -> UploadResult:
        if job.files:
            if job.copy_mode == "copy_files":
                copied = self._copy_each_file(job)
            elif job.copy_mode == "copyto":
                copied = self.rclone.copyto(job.local, job.remote)
            else:
                copied = self.rclone.copy(job.local, job.remote)
            if not copied:
                return UploadResult("retry_wait", False, False)
            if not self._verify_manifest(job):
                return UploadResult("verify_pending", False, False)
        else:
            if not self.rclone.copyto(job.local, job.remote): return UploadResult("retry_wait", False, False)
            if self.rclone.lsjson_size(job.remote) != job.size: return UploadResult("verify_pending", False, False)
        if job.full_torrent:
            self.executor.qbt_post("/api/v2/torrents/delete", {"hashes": job.hash, "deleteFiles": "true"}); return UploadResult("done", True, True)
        return UploadResult("cleanup_deferred", True, False)

    def _copy_each_file(self, job: UploadJob) -> bool:
        for item in job.files or []:
            rel = _rel(str(item.get("relative_path") or item.get("path") or item.get("name") or ""))
            local = str(item.get("local_path") or item.get("local") or "")
            remote = str(item.get("remote_path") or item.get("remote") or "")
            if not local and rel:
                local = str(PurePosixPath(str(job.local).replace("\\", "/")) / rel)
            if not remote and rel:
                remote = f"{str(job.remote).rstrip('/')}/{rel}"
            if not local or not remote:
                return False
            if not self.rclone.copyto(local, remote):
                return False
        return True

    def _verify_manifest(self, job: UploadJob) -> bool:
        rows = self.rclone.lsjson(job.remote, recursive=(job.copy_mode != "copyto"))
        actual: dict[str, int] = {}
        for row in rows or []:
            if row.get("IsDir"):
                continue
            rel = _rel(str(row.get("Path") or row.get("Name") or ""))
            if rel:
                actual[rel] = int(row.get("Size") or 0)
        expected_total = 0
        for item in job.files or []:
            rel = _rel(str(item.get("relative_path") or item.get("path") or item.get("name") or ""))
            size = int(item.get("size") or 0)
            expected_total += size
            if actual.get(rel) != size:
                return False
        return expected_total == int(job.size)
