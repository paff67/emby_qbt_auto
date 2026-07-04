from __future__ import annotations
from typing import Any, Dict
from .models import ActionLogEntry
class Executor:
    def __init__(self, qbt, dry_run: bool = True):
        self.qbt = qbt; self.dry_run = dry_run; self.action_log: list[ActionLogEntry] = []
    def qbt_post(self, path: str, payload: Dict[str, Any]) -> None:
        if self.dry_run:
            self.action_log.append(ActionLogEntry(path, payload, "dry_run", True)); return
        try:
            self.qbt.post(path, payload); self.action_log.append(ActionLogEntry(path, payload, "succeeded", False))
        except Exception as e:
            self.action_log.append(ActionLogEntry(path, payload, "failed", False, str(e))); raise
    def set_seq_dl(self, hash: str, desired: bool) -> bool:
        current = bool(self.qbt.torrent_info(hash).get("seq_dl"))
        if current == bool(desired): return False
        self.qbt_post("/api/v2/torrents/toggleSequentialDownload", {"hashes": hash}); return True

    def set_download_limit(self, hash: str, limit_bps: int) -> None:
        self.qbt_post("/api/v2/torrents/setDownloadLimit", {"hashes": hash, "limit": str(max(0, int(limit_bps)))})
