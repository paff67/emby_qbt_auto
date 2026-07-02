from __future__ import annotations
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Dict, Mapping
from .models import TorrentSnapshot
class SyncHealth(str, Enum):
    HEALTHY_FULL = "healthy_full"; HEALTHY_DELTA = "healthy_delta"; UNHEALTHY = "unhealthy"; AUTH_FAILED = "auth_failed"; BROKEN_RESPONSE = "broken_response"; SUSPECT_EMPTY_FULL = "suspect_empty_full"; SUSPECT_DROP = "suspect_drop"
@dataclass(frozen=True)
class SyncResult:
    health: SyncHealth; rid: int; full_update: bool = False; error: str | None = None
class QbtSyncCache:
    def __init__(self, client, managed_count_provider: Callable[[], int] | None = None, max_snapshot_count_drop_ratio: float = 0.5):
        self.client = client; self.managed_count_provider = managed_count_provider or (lambda: 0); self.max_snapshot_count_drop_ratio = max_snapshot_count_drop_ratio
        self.rid = 0; self.snapshots: Dict[str, TorrentSnapshot] = {}; self.server_state = {}; self.health = SyncHealth.UNHEALTHY; self.high_risk_actions_allowed = False
    def poll_once(self) -> SyncResult:
        try:
            payload = self.client.get_maindata(self.rid)
            if not isinstance(payload, Mapping): raise ValueError("maindata response is not a mapping")
        except PermissionError as e:
            self.health = SyncHealth.AUTH_FAILED; self.high_risk_actions_allowed = False; return SyncResult(self.health, self.rid, error=str(e))
        except Exception as e:
            self.health = SyncHealth.UNHEALTHY; self.high_risk_actions_allowed = False; return SyncResult(self.health, self.rid, error=str(e))
        full = bool(payload.get("full_update")); new_rid = int(payload.get("rid", self.rid)); torrents = payload.get("torrents") or {}
        if not isinstance(torrents, Mapping):
            self.health = SyncHealth.BROKEN_RESPONSE; self.high_risk_actions_allowed = False; return SyncResult(self.health, self.rid, full, "bad torrents payload")
        if full:
            if len(torrents) == 0 and self.managed_count_provider() > 0:
                self.health = SyncHealth.SUSPECT_EMPTY_FULL; self.high_risk_actions_allowed = False; return SyncResult(self.health, self.rid, True)
            if self.snapshots and len(torrents) < len(self.snapshots) * self.max_snapshot_count_drop_ratio:
                self.health = SyncHealth.SUSPECT_DROP; self.high_risk_actions_allowed = False; return SyncResult(self.health, self.rid, True)
            self.snapshots = {h: TorrentSnapshot.from_qbt(dict(v, hash=h)) for h, v in torrents.items()}; self.server_state = dict(payload.get("server_state") or {}); self.rid = new_rid; self.health = SyncHealth.HEALTHY_FULL
        else:
            for h in payload.get("torrents_removed") or []: self.snapshots.pop(h, None)
            for h, data in torrents.items(): self.snapshots[h] = TorrentSnapshot.from_qbt(dict(data, hash=h))
            self.server_state.update(dict(payload.get("server_state") or {})); self.rid = new_rid; self.health = SyncHealth.HEALTHY_DELTA
        self.high_risk_actions_allowed = True; return SyncResult(self.health, self.rid, full)
