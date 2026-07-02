from __future__ import annotations
from dataclasses import dataclass
from .policies.disk import classify_disk, emergency_pause_action
from .qbt_sync import QbtSyncCache
@dataclass(frozen=True)
class SafetyTickResult:
    disk_state: str; sync_health: str
class SafetyMonitor:
    def __init__(self, qbt, executor, free_bytes_provider, managed_count_provider=None):
        self.qbt = qbt; self.executor = executor; self.free_bytes_provider = free_bytes_provider; self.sync = QbtSyncCache(qbt, managed_count_provider=managed_count_provider or (lambda: 0))
    def tick(self) -> SafetyTickResult:
        sync_result = self.sync.poll_once(); disk = classify_disk(int(self.free_bytes_provider()))
        if disk.state.value == "emergency":
            action = emergency_pause_action([vars(s) for s in self.sync.snapshots.values()])
            if action: self.executor.qbt_post(action.path, action.payload)
        return SafetyTickResult(disk.state.value, sync_result.health.value)
