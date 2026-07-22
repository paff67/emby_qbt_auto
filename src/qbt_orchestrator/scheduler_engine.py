from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Iterable

from .work_items import WorkItem, WorkKind


@dataclass(frozen=True)
class SchedulerPlan:
    mode: str
    selected: list[WorkItem] = field(default_factory=list)
    rejection_counts: dict[str, int] = field(default_factory=dict)
    utility_score: int = 0
    incremental_growth_bytes: int = 0
    available_growth_bytes: int = 0
    max_slots: int = 0
    incumbent_hashes: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class _Selection:
    item_indexes: tuple[int, ...]
    utility_score: int
    growth_bytes: int
    stable_key: tuple[tuple[str, str], ...]


def utility(item: WorkItem) -> int:
    relief_ratio = min(
        500_000,
        max(0, int(item.releasable_bytes)) * 1000 // max(1, max(0, int(item.incremental_growth_bytes))),
    )
    probability = int(max(0.0, min(1.0, float(item.completion_probability))) * 100_000)
    age = min(50_000, max(0, int(item.wait_age_sec)) // 60)
    throughput = min(50_000, max(0, int(item.throughput_bps)) // 1024)
    return int(item.operator_priority) * 1_000_000 + relief_ratio + probability + age + throughput


def _better(left: _Selection, right: _Selection | None) -> bool:
    if right is None:
        return True
    return (
        -left.utility_score,
        left.growth_bytes,
        left.stable_key,
    ) < (
        -right.utility_score,
        right.growth_bytes,
        right.stable_key,
    )


class SchedulerEngine:
    """Deterministic sparse DP over slot and conservative capacity units."""

    def __init__(self, unit_bytes: int = 64 * 1024**2):
        if int(unit_bytes) <= 0:
            raise ValueError("unit_bytes must be positive")
        self.unit_bytes = int(unit_bytes)

    def select(
        self,
        items: Iterable[WorkItem],
        mode: str,
        available_growth_bytes: int,
        max_slots: int,
        incumbent_hashes: set[str] | None = None,
    ) -> SchedulerPlan:
        scheduler_mode = str(mode).lower()
        if scheduler_mode not in {"emergency", "drain", "normal", "explore"}:
            raise ValueError(f"unsupported scheduler mode: {scheduler_mode}")
        available = max(0, int(available_growth_bytes))
        slot_limit = max(0, int(max_slots))
        rejection_counts: dict[str, int] = {}
        eligible: list[WorkItem] = []
        incumbents = {str(item) for item in (incumbent_hashes or set())}

        def reject(reason: str) -> None:
            rejection_counts[reason] = rejection_counts.get(reason, 0) + 1

        for item in sorted(items, key=lambda candidate: candidate.stable_key):
            if bool(item.hold):
                reject("hold")
                continue
            if scheduler_mode == "emergency" or (
                scheduler_mode == "drain" and item.kind is not WorkKind.FULL_FINISH
            ):
                reject("mode_disallowed")
                continue
            if item.kind is WorkKind.BATCH_DELIVERY and (
                int(item.releasable_bytes) != 0 or int(item.pinned_after_success_bytes) <= 0
            ):
                reject("invalid_delivery_semantics")
                continue
            if max(0, int(item.incremental_growth_bytes)) > available:
                reject("budget_exceeded")
                continue
            eligible.append(item)

        capacity_units = math.ceil(available / self.unit_bytes) if available > 0 else 0
        states: dict[tuple[int, int], _Selection] = {
            (0, 0): _Selection((), 0, 0, ()),
        }
        for item_index, item in enumerate(eligible):
            growth = max(0, int(item.incremental_growth_bytes))
            item_units = math.ceil(growth / self.unit_bytes) if growth > 0 else 0
            # A newly admitted download needs enough uninterrupted time for the
            # Planner's health window to become meaningful.  Without this
            # dominant, bounded preference the instantaneous throughput term
            # can rotate a zero-speed incumbent out on the very next 15s tick.
            item_utility = utility(item) + (
                1_000_000_000_000 if item.hash in incumbents else 0
            )
            previous_states = list(states.items())
            for (used_slots, used_units), previous in previous_states:
                next_slots = used_slots + 1
                next_units = used_units + item_units
                next_growth = previous.growth_bytes + growth
                if next_slots > slot_limit or next_units > capacity_units or next_growth > available:
                    continue
                candidate = _Selection(
                    item_indexes=previous.item_indexes + (item_index,),
                    utility_score=previous.utility_score + item_utility,
                    growth_bytes=next_growth,
                    stable_key=previous.stable_key + (item.stable_key,),
                )
                state_key = (next_slots, next_units)
                if _better(candidate, states.get(state_key)):
                    states[state_key] = candidate

        best: _Selection | None = None
        for candidate in states.values():
            if _better(candidate, best):
                best = candidate
        assert best is not None
        selected = [eligible[index] for index in best.item_indexes]
        selected_ids = {item.id for item in selected}
        for item in eligible:
            if item.id not in selected_ids:
                reject("capacity_or_slot")
        return SchedulerPlan(
            mode=scheduler_mode,
            selected=selected,
            rejection_counts=dict(sorted(rejection_counts.items())),
            utility_score=best.utility_score,
            incremental_growth_bytes=best.growth_bytes,
            available_growth_bytes=available,
            max_slots=slot_limit,
            incumbent_hashes=sorted(
                item.hash for item in eligible if item.hash in incumbents
            ),
        )
