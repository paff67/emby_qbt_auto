from __future__ import annotations

import copy
import threading
from typing import Any, Callable, Dict

from .action_dispatcher import ActionDispatcher, ActionPriority
from .models import ActionLogEntry


class Executor:
    """Apply qBT writes through one ordered dispatcher and retain an audit log."""

    def __init__(self, qbt, dry_run: bool = True, dispatcher: ActionDispatcher | None = None):
        self.qbt = qbt
        self.dry_run = dry_run
        self.action_log: list[ActionLogEntry] = []
        self._hash_mutation_leases: dict[str, str] = {}
        self._hash_mutation_lease_lock = threading.RLock()
        self._hash_mutation_condition = threading.Condition(
            self._hash_mutation_lease_lock
        )
        self._hash_mutations_inflight: dict[str, int] = {}
        self._dispatch_context = threading.local()
        self._qbt_post_handler = (
            self.qbt.post if dispatcher is None else dispatcher.handler
        )
        self.dispatcher = None if dry_run else (
            dispatcher or ActionDispatcher(self._execute_qbt_post)
        )
        if dispatcher is not None and not dry_run:
            dispatcher.handler = self._execute_qbt_post

    def _execute_qbt_post(
        self,
        path: str,
        payload: Dict[str, Any],
    ) -> Any:
        try:
            return self._qbt_post_handler(path, payload)
        finally:
            targets = tuple(
                getattr(
                    self._dispatch_context,
                    "hash_mutation_targets",
                    (),
                )
            )
            self._dispatch_context.hash_mutation_targets = ()
            if targets:
                with self._hash_mutation_condition:
                    for torrent_hash in targets:
                        remaining = (
                            self._hash_mutations_inflight.get(torrent_hash, 0)
                            - 1
                        )
                        if remaining > 0:
                            self._hash_mutations_inflight[torrent_hash] = remaining
                        else:
                            self._hash_mutations_inflight.pop(torrent_hash, None)
                    self._hash_mutation_condition.notify_all()

    @staticmethod
    def _normalized_hash(value: Any) -> str:
        return str(value or "").strip().lower()

    @classmethod
    def _payload_hashes(cls, payload: Dict[str, Any]) -> set[str]:
        hashes: set[str] = set()
        for key in ("hash", "hashes"):
            raw = str(payload.get(key) or "")
            hashes.update(
                normalized
                for item in raw.split("|")
                if (normalized := cls._normalized_hash(item))
            )
        return hashes

    def acquire_hash_mutation_lease(self, hash: str, token: str) -> bool:
        torrent_hash = self._normalized_hash(hash)
        owner_token = str(token or "").strip()
        if not torrent_hash or not owner_token:
            return False
        with self._hash_mutation_condition:
            while (
                self._hash_mutations_inflight.get(torrent_hash, 0) > 0
                or self._hash_mutations_inflight.get("all", 0) > 0
            ):
                self._hash_mutation_condition.wait()
            current = self._hash_mutation_leases.get(torrent_hash)
            if current is not None and current != owner_token:
                return False
            self._hash_mutation_leases[torrent_hash] = owner_token
            return True

    def hydrate_hash_mutation_lease(self, hash: str, token: str) -> bool:
        return self.acquire_hash_mutation_lease(hash, token)

    def release_hash_mutation_lease(self, hash: str, token: str) -> bool:
        torrent_hash = self._normalized_hash(hash)
        owner_token = str(token or "").strip()
        with self._hash_mutation_condition:
            if self._hash_mutation_leases.get(torrent_hash) != owner_token:
                return False
            while (
                self._hash_mutations_inflight.get(torrent_hash, 0) > 0
                or self._hash_mutations_inflight.get("all", 0) > 0
            ):
                self._hash_mutation_condition.wait()
            if self._hash_mutation_leases.get(torrent_hash) != owner_token:
                return False
            del self._hash_mutation_leases[torrent_hash]
            return True

    def _hash_mutation_allowed(
        self,
        payload: Dict[str, Any],
        lease_token: str | None,
    ) -> bool:
        targets = self._payload_hashes(payload)
        owner_token = None if lease_token is None else str(lease_token)
        with self._hash_mutation_lease_lock:
            if "all" in targets:
                return all(
                    owner == owner_token
                    for owner in self._hash_mutation_leases.values()
                )
            return all(
                self._hash_mutation_leases.get(torrent_hash)
                in {None, owner_token}
                for torrent_hash in targets
            )

    def qbt_post(
        self,
        path: str,
        payload: Dict[str, Any],
        *,
        priority: ActionPriority | int = ActionPriority.CONTROL,
        lease_token: str | None = None,
    ) -> bool:
        return self._dispatch_qbt_post(
            path,
            payload,
            priority=priority,
            guard=None,
            lease_token=lease_token,
        )

    def qbt_post_guarded(
        self,
        path: str,
        payload: Dict[str, Any],
        *,
        guard: Callable[[], bool],
        priority: ActionPriority | int = ActionPriority.CONTROL,
        lease_token: str | None = None,
    ) -> bool:
        """Apply a write only while its planner generation remains current."""
        return self._dispatch_qbt_post(
            path,
            payload,
            priority=priority,
            guard=guard,
            lease_token=lease_token,
        )

    def _dispatch_qbt_post(
        self,
        path: str,
        payload: Dict[str, Any],
        *,
        priority: ActionPriority | int,
        guard: Callable[[], bool] | None,
        lease_token: str | None,
    ) -> bool:
        safe_payload = copy.deepcopy(dict(payload))
        if self.dry_run:
            if not self._hash_mutation_allowed(safe_payload, lease_token):
                self.action_log.append(
                    ActionLogEntry(
                        path,
                        safe_payload,
                        "skipped_hash_lease",
                        True,
                    )
                )
                return False
            self.action_log.append(ActionLogEntry(path, safe_payload, "dry_run", True))
            return True
        try:
            assert self.dispatcher is not None
            skip_status: list[str] = []

            def execution_guard() -> bool:
                with self._hash_mutation_condition:
                    if not self._hash_mutation_allowed(
                        safe_payload,
                        lease_token,
                    ):
                        skip_status.append("skipped_hash_lease")
                        return False
                    if guard is not None and not bool(guard()):
                        skip_status.append("skipped_stale_generation")
                        return False
                    targets = self._payload_hashes(safe_payload)
                    tracked_targets = (
                        {"all"} if "all" in targets else targets
                    )
                    for torrent_hash in tracked_targets:
                        self._hash_mutations_inflight[torrent_hash] = (
                            self._hash_mutations_inflight.get(torrent_hash, 0)
                            + 1
                        )
                    self._dispatch_context.hash_mutation_targets = tuple(
                        tracked_targets
                    )
                    return True

            result = self.dispatcher.submit(
                path,
                safe_payload,
                priority=priority,
                guard=execution_guard,
            )
            if result is False:
                self.action_log.append(
                    ActionLogEntry(
                        path,
                        safe_payload,
                        skip_status[-1]
                        if skip_status
                        else "skipped_stale_generation",
                        False,
                    )
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
