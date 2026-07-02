from __future__ import annotations
from ..models import LifecycleState

def desired_seq_dl(state: LifecycleState, seeds: int, peers: int, stalled_seconds: int, score: int = 100) -> bool:
    if state in {LifecycleState.SOAK, LifecycleState.DEAD, LifecycleState.CAROUSEL_PROBE}: return False
    if state != LifecycleState.ACTIVE: return False
    if stalled_seconds >= 60: return False
    return seeds >= 3 and peers >= 5 and score >= 70
