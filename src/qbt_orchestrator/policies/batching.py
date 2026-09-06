from __future__ import annotations
from typing import Iterable, Mapping
from ..models import BatchReservation, CleanupDecision

def compute_batch_reservation(
    files: Iterable[Mapping[str, int]],
    piece_size: int,
    filesystem_slack: int,
    selected_extents: int | None = None,
) -> BatchReservation:
    rows = list(files)
    payload = sum(int(f.get("remaining_bytes", f.get("size", 0))) for f in rows)
    if selected_extents is None:
        indices = sorted({int(row["index"]) for row in rows if row.get("index") is not None})
        selected_extents = 0 if not rows else 1
        if indices:
            selected_extents = 1 + sum(1 for previous, current in zip(indices, indices[1:]) if current != previous + 1)
    overhead = 0 if not rows else 2 * int(piece_size) * max(1, int(selected_extents))
    reserved = payload + overhead + int(filesystem_slack)
    return BatchReservation(payload, overhead, int(filesystem_slack), reserved, payload / reserved if reserved else 1.0)

def cleanup_decision(full_torrent: bool, remote_verified: bool) -> CleanupDecision:
    if not remote_verified: return CleanupDecision(False, "VerifyPending", "remote_not_verified")
    if full_torrent: return CleanupDecision(True, "CleanupReady", "full_torrent_verified")
    return CleanupDecision(False, "CleanupDeferred", "pipeline_batch_keeps_qbt_managed_files")
