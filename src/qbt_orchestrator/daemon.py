from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable

from .policies.disk import classify_disk, emergency_pause_action
from .qbt_sync import QbtSyncCache


@dataclass(frozen=True)
class SafetyTickResult:
    disk_state: str
    sync_health: str
    sync_skipped: bool = False


class SafetyMonitor:
    def __init__(
        self,
        qbt,
        executor,
        free_bytes_provider,
        managed_count_provider=None,
        emergency_floor_bytes: int = 2 * 1024**3,
        sync_repeated_full_limit: int = 3,
        sync_degraded_interval_sec: float = 10.0,
        monotonic: Callable[[], float] = time.monotonic,
    ):
        self.qbt = qbt
        self.executor = executor
        self.free_bytes_provider = free_bytes_provider
        self.emergency_floor_bytes = int(emergency_floor_bytes)
        self.sync = QbtSyncCache(
            qbt,
            managed_count_provider=managed_count_provider or (lambda: 0),
            repeated_full_limit=sync_repeated_full_limit,
            degraded_interval_sec=sync_degraded_interval_sec,
            monotonic=monotonic,
        )

    def tick(self) -> SafetyTickResult:
        sync_result = self.sync.poll_once()
        disk = classify_disk(int(self.free_bytes_provider()), emergency_free_bytes=self.emergency_floor_bytes)
        if disk.state.value == "emergency":
            action = emergency_pause_action([vars(snapshot) for snapshot in self.sync.snapshots.values()])
            if action:
                emergency_post = getattr(self.executor, "emergency_qbt_post", self.executor.qbt_post)
                emergency_post(action.path, action.payload)
        return SafetyTickResult(disk.state.value, sync_result.health.value, sync_skipped=sync_result.skipped)
