from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import time
from typing import Callable, Mapping

from .models import TorrentSnapshot
from .snapshot_store import TorrentRawSnapshotStore


class SyncHealth(str, Enum):
    HEALTHY_FULL = "healthy_full"
    HEALTHY_DELTA = "healthy_delta"
    UNHEALTHY = "unhealthy"
    AUTH_FAILED = "auth_failed"
    BROKEN_RESPONSE = "broken_response"
    SUSPECT_EMPTY_FULL = "suspect_empty_full"
    SUSPECT_DROP = "suspect_drop"


@dataclass(frozen=True)
class SyncResult:
    health: SyncHealth
    rid: int
    full_update: bool = False
    error: str | None = None
    skipped: bool = False


@dataclass
class SyncSessionStats:
    full_updates: int = 0
    delta_updates: int = 0
    repeated_full_updates: int = 0
    consecutive_repeated_full_updates: int = 0
    skipped_polls: int = 0
    degraded: bool = False

    def observe(self, *, full_update: bool, previous_rid: int, repeated_full_limit: int) -> None:
        if full_update:
            self.full_updates += 1
            if previous_rid > 0:
                self.repeated_full_updates += 1
                self.consecutive_repeated_full_updates += 1
            else:
                self.consecutive_repeated_full_updates = 0
        else:
            self.delta_updates += 1
            self.consecutive_repeated_full_updates = 0
        self.degraded = self.consecutive_repeated_full_updates >= max(1, int(repeated_full_limit))

    def as_dict(self) -> dict[str, int | bool]:
        return {
            "full_updates": self.full_updates,
            "delta_updates": self.delta_updates,
            "repeated_full_updates": self.repeated_full_updates,
            "consecutive_repeated_full_updates": self.consecutive_repeated_full_updates,
            "skipped_polls": self.skipped_polls,
            "degraded": self.degraded,
        }


class QbtSyncCache:
    def __init__(
        self,
        client,
        managed_count_provider: Callable[[], int] | None = None,
        max_snapshot_count_drop_ratio: float = 0.5,
        repeated_full_limit: int = 3,
        degraded_interval_sec: float = 10.0,
        monotonic: Callable[[], float] = time.monotonic,
    ):
        self.client = client
        self.managed_count_provider = managed_count_provider or (lambda: 0)
        self.max_snapshot_count_drop_ratio = max_snapshot_count_drop_ratio
        self.repeated_full_limit = max(1, int(repeated_full_limit))
        self.degraded_interval_sec = max(0.0, float(degraded_interval_sec))
        self.monotonic = monotonic
        self.rid = 0
        self.snapshots: dict[str, TorrentSnapshot] = {}
        self.server_state: dict = {}
        self.health = SyncHealth.UNHEALTHY
        self.high_risk_actions_allowed = False
        self._store = TorrentRawSnapshotStore()
        self.session_stats = SyncSessionStats()
        self._next_poll_at = 0.0

    def poll_once(self) -> SyncResult:
        now_monotonic = float(self.monotonic())
        if self.session_stats.degraded and now_monotonic < self._next_poll_at:
            self.session_stats.skipped_polls += 1
            return SyncResult(self.health, self.rid, skipped=True)

        previous_rid = self.rid
        try:
            payload = self.client.get_maindata(self.rid)
            if not isinstance(payload, Mapping):
                raise ValueError("maindata response is not a mapping")
        except PermissionError as exc:
            return self._reject(SyncHealth.AUTH_FAILED, str(exc))
        except Exception as exc:
            return self._reject(SyncHealth.UNHEALTHY, str(exc))

        full_update = bool(payload.get("full_update"))
        new_rid = int(payload.get("rid", self.rid))
        torrents = payload.get("torrents") or {}
        if not isinstance(torrents, Mapping):
            return self._reject(SyncHealth.BROKEN_RESPONSE, "bad torrents payload", full_update=full_update)

        if full_update:
            suspect = self._validate_full_update(torrents)
            if suspect is not None:
                return suspect
            self._store.replace_full(torrents)
            self.server_state = dict(payload.get("server_state") or {})
            health = SyncHealth.HEALTHY_FULL
        else:
            self._store.apply_delta(torrents, removed=payload.get("torrents_removed") or [])
            self.server_state.update(dict(payload.get("server_state") or {}))
            health = SyncHealth.HEALTHY_DELTA

        self.snapshots = self._store.snapshots()
        self.rid = new_rid
        self.health = health
        self.high_risk_actions_allowed = True
        self.session_stats.observe(
            full_update=full_update,
            previous_rid=previous_rid,
            repeated_full_limit=self.repeated_full_limit,
        )
        self._next_poll_at = now_monotonic + self.degraded_interval_sec if self.session_stats.degraded else 0.0
        return SyncResult(self.health, self.rid, full_update)

    def _validate_full_update(self, torrents: Mapping) -> SyncResult | None:
        if len(torrents) == 0 and self.managed_count_provider() > 0:
            return self._reject(SyncHealth.SUSPECT_EMPTY_FULL, full_update=True)
        if self.snapshots and len(torrents) < len(self.snapshots) * self.max_snapshot_count_drop_ratio:
            return self._reject(SyncHealth.SUSPECT_DROP, full_update=True)
        return None

    def _reject(
        self,
        health: SyncHealth,
        error: str | None = None,
        *,
        full_update: bool = False,
    ) -> SyncResult:
        self.health = health
        self.high_risk_actions_allowed = False
        return SyncResult(self.health, self.rid, full_update, error)
