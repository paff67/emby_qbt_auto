from __future__ import annotations

import json
import logging
import sqlite3
import time
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from .db import write_transaction
from .observability import redact
from .torrent_ownership import (
    is_gc_eligible_add_tag,
    torrent_tags,
)

_LOG = logging.getLogger(__name__)


def compute_orphan_add_tag_candidates(
    *,
    global_tags: Sequence[str],
    snapshots: Mapping[str, Mapping[str, Any]],
    sqlite_refs: Sequence[str] | set[str],
) -> list[str]:
    """Return GC-eligible add tags unused by qBT snapshots and SQLite refs."""

    global_current = {
        str(tag).strip()
        for tag in global_tags
        if is_gc_eligible_add_tag(str(tag).strip())
    }
    assigned: set[str] = set()
    for raw in snapshots.values():
        assigned.update(
            tag for tag in torrent_tags(dict(raw)) if is_gc_eligible_add_tag(tag)
        )
    refs = {str(tag).strip() for tag in sqlite_refs if str(tag or "").strip()}
    return sorted(global_current - assigned - refs)


class QbtTagJanitor:
    """Delete orphaned global add-item-* tag definitions with dual evidence."""

    def __init__(
        self,
        repository,
        gateway,
        *,
        dry_run: bool = True,
        batch_limit: int = 25,
        now: Callable[[], int] | None = None,
        state_db: str | Path | None = None,
    ) -> None:
        self.repository = repository
        self.gateway = gateway
        self.dry_run = bool(dry_run)
        if isinstance(batch_limit, bool) or not isinstance(batch_limit, int) or batch_limit <= 0:
            raise ValueError("batch_limit")
        self.batch_limit = int(batch_limit)
        self.now = now or (lambda: int(time.time()))
        self.state_db = Path(state_db) if state_db is not None else Path(repository.state_db)

    def tick(
        self,
        snapshots: Mapping[str, Mapping[str, Any]] | None = None,
        *,
        sync_healthy: bool = True,
    ) -> dict[str, Any]:
        empty = {
            "status": "ok",
            "global_count": 0,
            "referenced_count": 0,
            "assigned_count": 0,
            "candidate_count": 0,
            "deleted": [],
            "fenced": [],
            "errors": 0,
        }
        if not sync_healthy:
            result = dict(empty)
            result["status"] = "suspended"
            self._event(
                "warning",
                "suspended_unhealthy_sync",
                "qbt tag janitor suspended because qBT sync is unhealthy",
                {"dry_run": self.dry_run},
            )
            return result

        global_tags = sorted(self.gateway.list_tags())
        current_global = [tag for tag in global_tags if is_gc_eligible_add_tag(tag)]
        snap = {
            str(key): dict(value)
            for key, value in dict(snapshots or {}).items()
        }
        sqlite_refs = set(self.repository.list_qbt_precheck_tag_refs())
        assigned = {
            tag
            for raw in snap.values()
            for tag in torrent_tags(raw)
            if is_gc_eligible_add_tag(tag)
        }
        candidates = compute_orphan_add_tag_candidates(
            global_tags=global_tags,
            snapshots=snap,
            sqlite_refs=sqlite_refs,
        )
        limited = candidates[: self.batch_limit]
        result = {
            "status": "dry_run" if self.dry_run else "ok",
            "global_count": len(current_global),
            "referenced_count": len(
                {tag for tag in sqlite_refs if is_gc_eligible_add_tag(tag)}
            ),
            "assigned_count": len(assigned),
            "candidate_count": len(candidates),
            "deleted": [],
            "fenced": [],
            "errors": 0,
        }
        if self.dry_run:
            self._event(
                "info",
                "dry_run_candidates",
                "qbt tag janitor dry-run candidate count",
                {
                    "candidate_count": len(candidates),
                    "sample_tags": limited[:5],
                    "dry_run": True,
                },
            )
            return result

        for tag in limited:
            try:
                if not self._pre_delete_guard(tag):
                    result["fenced"].append(tag)
                    continue
                if not self.gateway.delete_tags(
                    [tag],
                    guard=lambda t=tag: self._pre_delete_guard(t),
                ):
                    result["fenced"].append(tag)
                    continue
                result["deleted"].append(tag)
            except (ValueError, RuntimeError, TypeError, sqlite3.Error):
                result["errors"] += 1
                self._event(
                    "error",
                    "delete_failed",
                    "qbt tag janitor failed to delete orphan tag",
                    {"tag": tag},
                )
        if result["deleted"]:
            self._event(
                "info",
                "deleted_orphan_tags",
                "qbt tag janitor deleted orphan add tags",
                {
                    "deleted_count": len(result["deleted"]),
                    "deleted": list(result["deleted"]),
                    "fenced_count": len(result["fenced"]),
                },
            )
        return result

    def _pre_delete_guard(self, tag: str) -> bool:
        if not is_gc_eligible_add_tag(tag):
            return False
        if tag in self.repository.list_qbt_precheck_tag_refs():
            return False
        try:
            users = self.gateway.torrents_by_tag(tag)
        except (ValueError, RuntimeError, TypeError):
            return False
        return users == []

    def _event(
        self, level: str, event_type: str, message: str, data: Mapping[str, Any]
    ) -> None:
        payload = dict(redact(dict(data)))
        _LOG.log(
            logging.ERROR if level == "error" else logging.INFO,
            "%s %s %s",
            event_type,
            message,
            json.dumps(payload, ensure_ascii=False, sort_keys=True),
        )
        now = int(self.now())

        def txn(con: sqlite3.Connection) -> None:
            con.execute(
                "insert into events_v2(ts,level,component,event_type,message,data_json) "
                "values(?,?,?,?,?,?)",
                (
                    now,
                    str(level),
                    "qbt_tag_janitor",
                    str(event_type),
                    str(message)[:500],
                    json.dumps(payload, ensure_ascii=False, sort_keys=True),
                ),
            )

        try:
            write_transaction(self.state_db, txn)
        except Exception:
            # Observability must never fail the GC tick.
            return
