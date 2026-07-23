from __future__ import annotations

from typing import Any


def canonical_torrent_hash(value: Any) -> str:
    """Return the single canonical representation used for torrent identity."""

    return str(value or "").strip().lower()
