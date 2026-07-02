from __future__ import annotations
from dataclasses import dataclass
@dataclass(frozen=True)
class UploadJob:
    hash: str; batch_id: int | None; local: str; remote: str; size: int; full_torrent: bool = False
@dataclass(frozen=True)
class UploadResult:
    state: str; remote_verified: bool; cleanup_allowed: bool
class RcloneUploadWorker:
    def __init__(self, rclone, executor): self.rclone = rclone; self.executor = executor
    def run_once(self, job: UploadJob) -> UploadResult:
        if not self.rclone.copyto(job.local, job.remote): return UploadResult("retry_wait", False, False)
        if self.rclone.lsjson_size(job.remote) != job.size: return UploadResult("verify_pending", False, False)
        if job.full_torrent:
            self.executor.qbt_post("/api/v2/torrents/delete", {"hashes": job.hash, "deleteFiles": "true"}); return UploadResult("done", True, True)
        return UploadResult("cleanup_deferred", True, False)
