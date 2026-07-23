from __future__ import annotations
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any

from .integrations.rclone import VerifyResult, verify_manifest_listing


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
    state: str
    remote_verified: bool
    cleanup_allowed: bool
    verification_method: str | None = None
    mismatches: tuple[str, ...] = ()


class RcloneUploadWorker:
    def __init__(self, rclone, executor): self.rclone = rclone; self.executor = executor

    def run_once(self, job: UploadJob) -> UploadResult:
        if not self.copy(job):
            return UploadResult("retry_wait", False, False)
        verification = self.verify(job)
        if not verification.verified:
            return UploadResult(
                "verify_pending",
                False,
                False,
                verification.method,
                tuple(verification.mismatches),
            )
        state = "cleanup_wait" if job.full_torrent else "cleanup_deferred"
        return UploadResult(state, True, job.full_torrent, verification.method, ())

    def copy(self, job: UploadJob) -> bool:
        if job.files:
            if job.copy_mode == "copy_files":
                return self._copy_each_file(job)
            elif job.copy_mode == "copyto":
                return bool(self.rclone.copyto(job.local, job.remote))
            return bool(self.rclone.copy(job.local, job.remote))
        return bool(self.rclone.copyto(job.local, job.remote))

    def verify(self, job: UploadJob) -> VerifyResult:
        if not job.files:
            actual_size = self.rclone.lsjson_size(job.remote)
            mismatches = [] if actual_size == job.size else ["size:remote"]
            return VerifyResult(not mismatches, "single_size", mismatches)
        if hasattr(self.rclone, "verify_manifest"):
            result = self.rclone.verify_manifest(
                list(job.files),
                job.remote,
                recursive=(job.copy_mode != "copyto"),
            )
        else:
            rows = self.rclone.lsjson(job.remote, recursive=(job.copy_mode != "copyto"))
            result = verify_manifest_listing(list(job.files), list(rows or []))
        expected_total = sum(int(item.get("size") or 0) for item in job.files)
        if result.verified and expected_total != int(job.size):
            return VerifyResult(False, result.method, ["size:manifest_total"])
        return result

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
