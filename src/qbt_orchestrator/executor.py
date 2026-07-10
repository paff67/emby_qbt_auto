from __future__ import annotations

import copy
from typing import Any, Dict

from .action_dispatcher import ActionDispatcher, ActionPriority
from .models import ActionLogEntry


class Executor:
    """Apply qBT writes through one ordered dispatcher and retain an audit log."""

    def __init__(self, qbt, dry_run: bool = True, dispatcher: ActionDispatcher | None = None):
        self.qbt = qbt
        self.dry_run = dry_run
        self.action_log: list[ActionLogEntry] = []
        self.dispatcher = None if dry_run else (dispatcher or ActionDispatcher(self.qbt.post))

    def qbt_post(
        self,
        path: str,
        payload: Dict[str, Any],
        *,
        priority: ActionPriority | int = ActionPriority.CONTROL,
    ) -> None:
        safe_payload = copy.deepcopy(dict(payload))
        if self.dry_run:
            self.action_log.append(ActionLogEntry(path, safe_payload, "dry_run", True))
            return
        try:
            assert self.dispatcher is not None
            self.dispatcher.submit(path, safe_payload, priority=priority)
            self.action_log.append(ActionLogEntry(path, safe_payload, "succeeded", False))
        except Exception as exc:
            self.action_log.append(ActionLogEntry(path, safe_payload, "failed", False, str(exc)))
            raise

    def emergency_qbt_post(self, path: str, payload: Dict[str, Any]) -> None:
        self.qbt_post(path, payload, priority=ActionPriority.EMERGENCY)

    def maintenance_qbt_post(self, path: str, payload: Dict[str, Any]) -> None:
        self.qbt_post(path, payload, priority=ActionPriority.MAINTENANCE)

    def set_seq_dl(self, hash: str, desired: bool) -> bool:
        current = bool(self.qbt.torrent_info(hash).get("seq_dl"))
        if current == bool(desired):
            return False
        self.qbt_post("/api/v2/torrents/toggleSequentialDownload", {"hashes": hash})
        return True

    def set_download_limit(self, hash: str, limit_bps: int) -> None:
        self.qbt_post(
            "/api/v2/torrents/setDownloadLimit",
            {"hashes": hash, "limit": str(max(0, int(limit_bps)))},
        )

    def close(self, timeout: float | None = None) -> None:
        if self.dispatcher is None:
            return
        self.dispatcher.close()
        self.dispatcher.join(timeout=timeout)
