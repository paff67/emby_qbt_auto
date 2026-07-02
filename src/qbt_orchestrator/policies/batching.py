from __future__ import annotations
from typing import Iterable, Mapping
from ..models import BatchReservation, CleanupDecision

def compute_batch_reservation(files: Iterable[Mapping[str, int]], piece_size: int, filesystem_slack: int) -> BatchReservation:
    extents = list(files)
    payload = sum(int(f.get("size", 0)) for f in extents)
    overhead = 0 if not extents else 2 * int(piece_size)
    reserved = payload + overhead + int(filesystem_slack)
    return BatchReservation(payload, overhead, int(filesystem_slack), reserved, payload / reserved if reserved else 1.0)

def cleanup_decision(full_torrent: bool, remote_verified: bool) -> CleanupDecision:
    if not remote_verified: return CleanupDecision(False, "VerifyPending", "remote_not_verified")
    if full_torrent: return CleanupDecision(True, "CleanupReady", "full_torrent_verified")
    return CleanupDecision(False, "CleanupDeferred", "pipeline_batch_keeps_qbt_managed_files")
