from __future__ import annotations
from dataclasses import dataclass
from typing import Callable
from ..models import LifecycleState

@dataclass(frozen=True)
class TorrentHealthSample:
    dlspeed_bps: int
    upspeed_bps: int
    completed_bytes: int
    progress: float
    num_seeds: int
    num_peers: int
    active_since: int | None = None
    low_speed_since: int | None = None
    no_progress_since: int | None = None
    last_swarm_seen_at: int | None = None
    promote_ticks: int = 0

class HealthPolicy:
    def __init__(self, now: Callable[[], int] | None = None, active_to_soak_speed_bps: int = 100 * 1024):
        self.now = now or (lambda: int(__import__("time").time()))
        self.active_to_soak_speed_bps = active_to_soak_speed_bps
    def next_state(self, current: LifecycleState, sample: TorrentHealthSample, disk_budget_allows: bool = True, active_slot_available: bool = True) -> LifecycleState:
        now = self.now()
        if current == LifecycleState.ACTIVE:
            if now - (sample.active_since if sample.active_since is not None else now) >= 90 and now - (sample.low_speed_since if sample.low_speed_since is not None else now) >= 300 and sample.dlspeed_bps < self.active_to_soak_speed_bps:
                return LifecycleState.SOAK
        if current == LifecycleState.SOAK:
            if sample.last_swarm_seen_at is not None and sample.no_progress_since is not None and now - sample.last_swarm_seen_at >= 3600 and now - sample.no_progress_since >= 3600 and sample.num_seeds == 0 and sample.num_peers == 0:
                return LifecycleState.DEAD
            if sample.promote_ticks >= 2 and disk_budget_allows and active_slot_available:
                return LifecycleState.ACTIVE
        return current

