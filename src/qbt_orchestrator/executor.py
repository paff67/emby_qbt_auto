from __future__ import annotations

import copy
import logging
import threading
from pathlib import Path
from typing import Any, Callable, Dict

from .action_dispatcher import ActionDispatcher, ActionPriority
from .db import readonly_connect
from .hash_identity import canonical_torrent_hash
from .models import ActionLogEntry
from .observability import redact


LOGGER = logging.getLogger(__name__)


class QbtMutationLeaseBlocked(RuntimeError):
    """A qBT mutation was rejected by a canonical per-hash reclaim lease."""

    def __init__(self, path: str, hashes: set[str] | tuple[str, ...]):
        self.path = str(redact(str(path)))
        self.hashes = tuple(
            sorted(
                {
                    canonical
                    for value in hashes
                    if (canonical := canonical_torrent_hash(value))
                }
            )
        )
        message = (
            "qBT mutation blocked by reclaim lease: "
            f"path={self.path} hashes={','.join(self.hashes)}"
        )
        super().__init__(str(redact(message)))


class Executor:
    """Apply qBT writes through one ordered dispatcher and retain an audit log."""

    def __init__(
        self,
        qbt,
        dry_run: bool = True,
        dispatcher: ActionDispatcher | None = None,
        state_db: str | Path | None = None,
    ):
        self.qbt = qbt
        self.dry_run = dry_run
        self.state_db = None if state_db is None else Path(state_db)
        self.action_log: list[ActionLogEntry] = []
        self.startup_reclaim_lease_warnings: list[str] = []
        self._hash_mutation_leases: dict[str, str] = {}
        self._hash_mutation_lease_lock = threading.RLock()
        self._hash_mutation_condition = threading.Condition(
            self._hash_mutation_lease_lock
        )
        self._hash_mutations_inflight: dict[str, int] = {}
        self._dispatch_context = threading.local()
        if self.state_db is not None:
            self._hydrate_durable_reclaim_leases_at_startup()
        self._qbt_post_handler = (
            self.qbt.post if dispatcher is None else dispatcher.handler
        )
        self.dispatcher = None if dry_run else (
            dispatcher or ActionDispatcher(self._execute_qbt_post)
        )
        if dispatcher is not None and not dry_run:
            dispatcher.handler = self._execute_qbt_post

    def _hydrate_durable_reclaim_leases_at_startup(self) -> None:
        """Load durable reclaim fences before a dispatcher can accept work."""
        assert self.state_db is not None
        try:
            con = readonly_connect(self.state_db)
            try:
                rows = con.execute(
                    "select id,hash,capacity_generation,state "
                    "from capacity_reclaims "
                    "where state is null or state not in ('released','cancelled') "
                    "order by id desc"
                ).fetchall()
            finally:
                con.close()

            rows_by_hash: dict[str, list[tuple[int, int]]] = {}
            for row in rows:
                torrent_hash = canonical_torrent_hash(row["hash"])
                if not torrent_hash:
                    raise ValueError(
                        f"locked capacity_reclaims row id={row['id']} has empty hash"
                    )
                reclaim_id = int(row["id"])
                generation = int(row["capacity_generation"] or 0)
                rows_by_hash.setdefault(torrent_hash, []).append(
                    (reclaim_id, generation)
                )

            for torrent_hash in sorted(rows_by_hash):
                candidates = sorted(
                    rows_by_hash[torrent_hash],
                    key=lambda item: item[0],
                    reverse=True,
                )
                reclaim_id, generation = candidates[0]
                if len(candidates) > 1:
                    ignored = ",".join(str(item[0]) for item in candidates[1:])
                    warning = (
                        "multiple durable capacity reclaim leases for hash "
                        f"{torrent_hash}; keeping id={reclaim_id}; "
                        f"ignoring id={ignored}"
                    )
                    self.startup_reclaim_lease_warnings.append(warning)
                    LOGGER.warning(warning)
                token = f"reclaim:{reclaim_id}:{generation}"
                if not self.hydrate_hash_mutation_lease(torrent_hash, token):
                    raise RuntimeError(
                        "could not install durable capacity reclaim lease "
                        f"for hash {torrent_hash}"
                    )
        except Exception as exc:
            raise RuntimeError(
                "durable capacity reclaim lease startup hydration failed "
                f"for {self.state_db}: {exc}"
            ) from exc

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
    def _payload_hashes(payload: Dict[str, Any]) -> set[str]:
        hashes: set[str] = set()
        for key in ("hash", "hashes"):
            raw = str(payload.get(key) or "")
            hashes.update(
                normalized
                for item in raw.split("|")
                if (normalized := canonical_torrent_hash(item))
            )
        return hashes

    def acquire_hash_mutation_lease(self, hash: str, token: str) -> bool:
        torrent_hash = canonical_torrent_hash(hash)
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
        torrent_hash = canonical_torrent_hash(hash)
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
                blocked = QbtMutationLeaseBlocked(
                    path,
                    self._payload_hashes(safe_payload),
                )
                self.action_log.append(
                    ActionLogEntry(
                        path,
                        safe_payload,
                        "skipped_hash_lease",
                        True,
                    )
                )
                raise blocked
            self.action_log.append(ActionLogEntry(path, safe_payload, "dry_run", True))
            return True
        try:
            assert self.dispatcher is not None

            def execution_guard() -> bool:
                with self._hash_mutation_condition:
                    if not self._hash_mutation_allowed(
                        safe_payload,
                        lease_token,
                    ):
                        raise QbtMutationLeaseBlocked(
                            path,
                            self._payload_hashes(safe_payload),
                        )
                    if guard is not None and not bool(guard()):
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
                        "skipped_stale_generation",
                        False,
                    )
                )
                return False
            self.action_log.append(ActionLogEntry(path, safe_payload, "succeeded", False))
            return True
        except QbtMutationLeaseBlocked:
            self.action_log.append(
                ActionLogEntry(
                    path,
                    safe_payload,
                    "skipped_hash_lease",
                    False,
                )
            )
            raise
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
