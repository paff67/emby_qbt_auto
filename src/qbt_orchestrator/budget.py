from __future__ import annotations

import math
from dataclasses import dataclass
from enum import Enum
from typing import Any, Iterable, Mapping


DEFAULT_CONSERVATIVE_INGRESS_BPS = 32 * 1024**2
OVERLAPPING_FUTURE_KINDS = frozenset({"active_download", "batch"})


class AccountingClass(str, Enum):
    """Whether a claim represents future writes or bytes already on disk."""

    FUTURE_GROWTH = "future_growth"
    CURRENT_PINNED = "current_pinned"


@dataclass(frozen=True)
class ResourceClaim:
    hash: str
    kind: str
    accounting_class: AccountingClass
    bytes: int


@dataclass(frozen=True)
class GrowthBudget:
    free_bytes: int
    emergency_floor_bytes: int
    dynamic_guard_bytes: int
    future_growth_reserved_bytes: int
    current_pinned_bytes: int
    available_growth_bytes: int


def accounting_class(value: Any) -> AccountingClass:
    """Coerce persisted values while preserving compatibility with old rows."""

    try:
        return AccountingClass(str(value or AccountingClass.FUTURE_GROWTH.value))
    except ValueError:
        # Unknown legacy values must remain conservative: treat them as future
        # growth so they reduce admission capacity instead of being ignored.
        return AccountingClass.FUTURE_GROWTH


def resource_claims_from_rows(rows: Iterable[Mapping[str, Any]]) -> list[ResourceClaim]:
    return [
        ResourceClaim(
            hash=str(row["hash"] or ""),
            kind=str(row["kind"] or ""),
            accounting_class=accounting_class(row["accounting_class"]),
            bytes=max(0, int(row["bytes"] or 0)),
        )
        for row in rows
    ]


def future_growth_by_hash(
    claims: Iterable[ResourceClaim],
    *,
    ignored_kinds: frozenset[str] | set[str] = frozenset(),
) -> dict[str, int]:
    """Collapse future claims using only the valid active/batch overlap rule.

    qBT active-download and batch reservations for the same torrent describe
    overlapping future bytes, so the larger bucket wins. Other kinds remain
    additive. Claims without a hash are deliberately isolated by sequence id;
    unrelated global claims must never overlap accidentally.
    """

    ignored = {str(kind) for kind in ignored_kinds}
    grouped: dict[str, dict[str, int]] = {}
    for index, claim in enumerate(claims):
        if claim.accounting_class is not AccountingClass.FUTURE_GROWTH:
            continue
        kind = str(claim.kind)
        if kind in ignored:
            continue
        key = str(claim.hash) or f"__unscoped__:{index}"
        bucket = grouped.setdefault(key, {"active_download": 0, "batch": 0, "other": 0})
        value = max(0, int(claim.bytes))
        if kind == "active_download":
            bucket["active_download"] += value
        elif kind == "batch":
            bucket["batch"] += value
        else:
            bucket["other"] += value
    return {
        key: max(bucket["active_download"], bucket["batch"]) + bucket["other"]
        for key, bucket in grouped.items()
    }


def calculate_growth_budget(
    free_bytes: int,
    emergency_floor_bytes: int,
    dynamic_guard_bytes: int,
    claims: Iterable[ResourceClaim],
) -> GrowthBudget:
    claims = list(claims)
    free = max(0, int(free_bytes))
    floor = max(0, int(emergency_floor_bytes))
    guard = max(0, int(dynamic_guard_bytes))
    future_reserved = sum(future_growth_by_hash(claims).values())
    current_pinned = sum(
        max(0, int(claim.bytes))
        for claim in claims
        if claim.accounting_class is AccountingClass.CURRENT_PINNED
    )
    return GrowthBudget(
        free_bytes=free,
        emergency_floor_bytes=floor,
        dynamic_guard_bytes=guard,
        future_growth_reserved_bytes=future_reserved,
        current_pinned_bytes=current_pinned,
        available_growth_bytes=max(0, free - floor - guard - future_reserved),
    )


def dynamic_guard_bytes(
    min_guard_bytes: int,
    ingress_p99_bps: int | None,
    control_p99_sec: float,
    stop_grace_sec: float,
    max_piece_size: int,
    filesystem_slack_bytes: int,
    *,
    conservative_ingress_bps: int = DEFAULT_CONSERVATIVE_INGRESS_BPS,
) -> int:
    """Calculate write headroom during the controller's worst stopping delay."""

    ingress = int(conservative_ingress_bps) if ingress_p99_bps is None else int(ingress_p99_bps)
    ingress = max(0, ingress)
    control_delay = max(0.0, float(control_p99_sec))
    stop_delay = max(0.0, float(stop_grace_sec))
    rate_guard = math.ceil(ingress * (control_delay + stop_delay))
    calculated = rate_guard + 2 * max(0, int(max_piece_size)) + max(0, int(filesystem_slack_bytes))
    return max(max(0, int(min_guard_bytes)), calculated)
