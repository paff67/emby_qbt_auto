from __future__ import annotations

import copy
from typing import Any, Callable, Dict

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
        self._dispatch_qbt_post(path, payload, priority=priority, guard=None)

    def qbt_post_guarded(
        self,
        path: str,
        payload: Dict[str, Any],
        *,
        guard: Callable[[], bool],
        priority: ActionPriority | int = ActionPriority.CONTROL,
    ) -> bool:
        """Apply a write only while its planner generation remains current."""
        return self._dispatch_qbt_post(path, payload, priority=priority, guard=guard)

    def _dispatch_qbt_post(
        self,
        path: str,
        payload: Dict[str, Any],
        *,
        priority: ActionPriority | int,
        guard: Callable[[], bool] | None,
    ) -> bool:
        safe_payload = copy.deepcopy(dict(payload))
        if self.dry_run:
            self.action_log.append(ActionLogEntry(path, safe_payload, "dry_run", True))
            return True
        try:
            assert self.dispatcher is not None
            result = self.dispatcher.submit(
                path,
                safe_payload,
                priority=priority,
                guard=guard,
            )
            if result is False:
                self.action_log.append(
                    ActionLogEntry(path, safe_payload, "skipped_stale_generation", False)
                )
                return False
            self.action_log.append(ActionLogEntry(path, safe_payload, "succeeded", False))
            return True
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

    def set_seq_dl_guarded(self, hash: str, desired: bool, *, guard: Callable[[], bool]) -> bool:
        current = bool(self.qbt.torrent_info(hash).get("seq_dl"))
        if current == bool(desired):
            return False
        return self.qbt_post_guarded(
            "/api/v2/torrents/toggleSequentialDownload",
            {"hashes": hash},
            guard=guard,
        )

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
