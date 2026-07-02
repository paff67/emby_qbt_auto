from __future__ import annotations
import re
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Iterable
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
