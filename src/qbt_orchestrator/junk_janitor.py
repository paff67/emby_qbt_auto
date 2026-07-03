from __future__ import annotations

import json
import re
import shutil
import sqlite3
import time
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Mapping, Sequence

from .db import write_transaction
from .observability import redact


DEFAULT_HARD_PATTERNS = (
    r"(?i)(最新地址|收藏不迷路|官方指定|博彩|赌场|telegram|996gg\.cc)",
    r"(?i)(直\s*播|聚\s*合\s*全\s*網|全\s*网.*直\s*播)",
    r"(?i)(manko\.fun|x18r\.tv|t66y\.com|草榴)",
)


def _connect(path: str | Path) -> sqlite3.Connection:
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    return con


def _tags(torrent: Mapping[str, Any]) -> set[str]:
    raw = str(torrent.get("tags") or "")
    return {p.strip() for p in raw.split(",") if p.strip()}


def _is_managed(torrent: Mapping[str, Any]) -> bool:
    tags = _tags(torrent)
    return (str(torrent.get("category") or "") == "auto" or "auto" in tags) and "hold" not in tags


class JunkJanitorService:
    """qBT-aware hard-junk guard.

    This service never blindly deletes qBT-managed files.  It first maps a
    candidate to a qBT file index, applies priority=0, and only on a later pass
    quarantines stable, small, hard-junk files whose qBT priority is already 0.
    """

    def __init__(
        self,
        state_db: str | Path,
        executor,
        managed_root: str | Path = "/data/downloads/active",
        trash_dir: str | Path = "/data/downloads/.orchestrator-trash",
        dry_run: bool = True,
        stable_mtime_sec: int = 60,
        max_auto_quarantine_bytes: int = 10 * 1024 * 1024,
        active_fast_download_bps: int = 2 * 1024 * 1024,
        hard_patterns: Sequence[str] = DEFAULT_HARD_PATTERNS,
        now: Callable[[], int] | None = None,
        host_downloads: str | None = None,
        container_downloads: str | None = None,
    ):
        self.state_db = Path(state_db)
        self.executor = executor
        self.managed_root = Path(managed_root)
        self.trash_dir = Path(trash_dir)
        self.dry_run = bool(dry_run)
        self.stable_mtime_sec = int(stable_mtime_sec)
        self.max_auto_quarantine_bytes = int(max_auto_quarantine_bytes)
        self.active_fast_download_bps = int(active_fast_download_bps)
        self.hard_patterns = [re.compile(p) for p in hard_patterns]
        self.now = now or (lambda: int(time.time()))
        self.host_downloads = host_downloads.rstrip("/") if host_downloads else None
        self.container_downloads = container_downloads.rstrip("/") if container_downloads else None

    def reconcile(
        self,
        snapshots: Mapping[str, Any],
        file_lists: Mapping[str, Sequence[Mapping[str, Any]]],
        sync_healthy: bool,
    ) -> dict[str, Any]:
        now = int(self.now())
        if not sync_healthy:
            self._event("warning", "suspended_unhealthy_sync", "junk janitor suspended because qBT sync is unhealthy", {"dry_run": self.dry_run})
            return {"suspended": True, "reason": "unhealthy_sync", "observed": 0, "set_prio_zero": [], "quarantined": [], "skipped": 0, "dry_run": self.dry_run}
        observed = 0
        set_prio_zero: list[str] = []
        quarantined: list[dict[str, Any]] = []
        skipped = 0
        current_batch_indices = self._current_batch_indices()
        for h, entries in file_lists.items():
            torrent = self._snapshot(snapshots, h)
            if not torrent or not _is_managed(torrent):
                continue
            for fallback_index, raw_entry in enumerate(entries):
                entry = dict(raw_entry)
                index = int(entry.get("index", fallback_index))
                name = str(entry.get("name") or "")
                if not name or not self._is_hard_junk(name):
                    continue
                observed += 1
                local_path = self._local_path(torrent, name)
                size = int(entry.get("size") or (local_path.stat().st_size if local_path.exists() else 0))
                priority = int(entry.get("priority") or 0)
                mtime = int(local_path.stat().st_mtime) if local_path.exists() else None
                if priority != 0:
                    self._set_file_priority_zero(str(h), index, priority, local_path, size, mtime)
                    set_prio_zero.append(f"{h}:{index}")
                    continue
                reason = self._skip_reason(str(h), index, torrent, local_path, size, mtime, current_batch_indices, now)
                if reason:
                    skipped += 1
                    self._record_event(str(h), index, local_path, size, "skipped", reason, priority, mtime, {})
                    continue
                dest = self._trash_destination(str(h), local_path, name)
                payload = {"from": str(local_path), "to": str(dest), "hash": str(h), "index": index}
                if self.dry_run:
                    self._action(str(h), index, local_path, dest, "dry_run", True)
                    self._record_event(str(h), index, local_path, size, "quarantine_dry_run", "hard_junk_priority_zero", priority, mtime, payload)
                else:
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(local_path), str(dest))
                    rule_id = self._learn_rule(Path(name).name, now)
                    self._action(str(h), index, local_path, dest, "succeeded", False)
                    self._record_event(str(h), index, dest, size, "quarantined", "hard_junk_priority_zero", priority, mtime, {"rule_id": rule_id, **payload}, rule_id=rule_id)
                quarantined.append({"hash": str(h), "index": index, "from": str(local_path), "to": str(dest)})
        return {
            "suspended": False,
            "observed": observed,
            "set_prio_zero": set_prio_zero,
            "quarantined": quarantined,
            "skipped": skipped,
            "dry_run": self.dry_run,
        }

    def _set_file_priority_zero(self, h: str, index: int, priority: int, path: Path, size: int, mtime: int | None) -> None:
        payload = {"hash": h, "id": str(index), "priority": "0"}
        if self.dry_run:
            self._action(h, index, path, path, "dry_run", True, action_type="junk_set_prio_zero", payload=payload)
        else:
            self.executor.qbt_post("/api/v2/torrents/filePrio", payload)
            self._action(h, index, path, path, "succeeded", False, action_type="junk_set_prio_zero", payload=payload)
        self._record_event(h, index, path, size, "set_prio_zero", "hard_junk_priority_not_zero", priority, mtime, payload)

    def _skip_reason(
        self,
        h: str,
        index: int,
        torrent: Mapping[str, Any],
        path: Path,
        size: int,
        mtime: int | None,
        current_batch_indices: dict[str, set[int]],
        now: int,
    ) -> str | None:
        if index in current_batch_indices.get(h, set()):
            return "current_batch"
        if size > self.max_auto_quarantine_bytes:
            return "size_over_limit"
        if mtime is None or (self.stable_mtime_sec > 0 and now - int(mtime) < self.stable_mtime_sec):
            return "mtime_unstable"
        if int(torrent.get("dlspeed_bps") or torrent.get("dlspeed") or 0) >= self.active_fast_download_bps:
            return "active_fast_download"
        if not path.exists():
            return "missing_local_path"
        if not self._is_relative_to(path.resolve(), self.managed_root.resolve()):
            return "outside_managed_root"
        return None

    def _current_batch_indices(self) -> dict[str, set[int]]:
        out: dict[str, set[int]] = {}
        con = _connect(self.state_db)
        rows = [dict(r) for r in con.execute("select hash,indices_json from torrent_batches where state in ('active','downloading','queued','selected')")]
        con.close()
        for row in rows:
            try:
                indices = {int(x) for x in json.loads(row.get("indices_json") or "[]")}
            except Exception:
                indices = set()
            out.setdefault(str(row["hash"]), set()).update(indices)
        return out

    def _local_path(self, torrent: Mapping[str, Any], name: str) -> Path:
        content = str(torrent.get("content_path") or "")
        mapped_content = self._map_to_host_path(content)
        base = Path(mapped_content) if mapped_content else self.managed_root / str(torrent.get("name") or torrent.get("hash") or "")
        return base / PurePosixPath(name)

    def _map_to_host_path(self, value: str) -> str:
        if not value:
            return ""
        if self.host_downloads and self.container_downloads:
            if value == self.container_downloads:
                return self.host_downloads
            if value.startswith(self.container_downloads + "/"):
                return self.host_downloads + value[len(self.container_downloads):]
        return value

    def _trash_destination(self, h: str, path: Path, name: str) -> Path:
        dest = self.trash_dir / h / PurePosixPath(name)
        if not dest.exists():
            return dest
        return dest.with_name(f"{dest.name}.{int(self.now())}")

    def _is_hard_junk(self, name: str) -> bool:
        return any(pattern.search(name) for pattern in self.hard_patterns)

    def _learn_rule(self, pattern: str, now: int) -> int:
        def txn(con: sqlite3.Connection) -> int:
            row = con.execute(
                "select id,hits from dynamic_junk_rules where pattern=? and pattern_type='literal' and source='janitor'",
                (pattern,),
            ).fetchone()
            if row:
                rule_id = int(row["id"])
                con.execute("update dynamic_junk_rules set hits=hits+1, updated_at=? where id=?", (now, rule_id))
            else:
                cur = con.execute(
                    "insert into dynamic_junk_rules(pattern,pattern_type,confidence,source,hits,created_at,updated_at,enabled) values(?,?,?,?,?,?,?,?)",
                    (pattern, "literal", "hard", "janitor", 1, now, now, 1),
                )
                rule_id = int(cur.lastrowid)
            return rule_id

        return int(write_transaction(self.state_db, txn))

    def _record_event(
        self,
        h: str,
        index: int,
        path: Path,
        size: int,
        action: str,
        reason: str,
        priority: int,
        mtime: int | None,
        data: dict[str, Any],
        rule_id: int | None = None,
    ) -> None:
        def txn(con: sqlite3.Connection) -> None:
            con.execute(
                "insert into junk_janitor_events(ts,hash,file_index,path,size,action,reason,rule_id,qbt_priority,mtime,data_json) values(?,?,?,?,?,?,?,?,?,?,?)",
                (int(self.now()), h, index, str(path), size, action, reason, rule_id, priority, mtime, json.dumps(redact(data), ensure_ascii=False)),
            )
            con.execute(
                "insert into events_v2(ts,level,component,event_type,hash,message,data_json) values(?,?,?,?,?,?,?)",
                (int(self.now()), "warning" if action in {"quarantined", "quarantine_dry_run"} else "info", "junk_janitor", f"junk_{action}", h, f"junk {action}: {reason}", json.dumps(redact(data), ensure_ascii=False)),
            )

        write_transaction(self.state_db, txn)

    def _action(
        self,
        h: str,
        index: int,
        src: Path,
        dest: Path,
        status: str,
        dry_run: bool,
        action_type: str = "junk_quarantine",
        payload: dict[str, Any] | None = None,
    ) -> None:
        payload = payload or {"hash": h, "index": index, "from": str(src), "to": str(dest)}
        write_transaction(
            self.state_db,
            lambda con: con.execute(
                "insert into action_log(ts,hash,action_type,path,payload_json,status,dry_run) values(?,?,?,?,?,?,?)",
                (int(self.now()), h, action_type, str(src), json.dumps(redact(payload), ensure_ascii=False), status, 1 if dry_run else 0),
            ),
        )

    def _event(self, level: str, event_type: str, message: str, data: dict[str, Any]) -> None:
        write_transaction(
            self.state_db,
            lambda con: con.execute(
                "insert into events_v2(ts,level,component,event_type,message,data_json) values(?,?,?,?,?,?)",
                (int(self.now()), level, "junk_janitor", event_type, message, json.dumps(redact(data), ensure_ascii=False)),
            ),
        )

    @staticmethod
    def _snapshot(snapshots: Mapping[str, Any], h: str) -> dict[str, Any]:
        raw = snapshots.get(h)
        if raw is None:
            return {}
        if hasattr(raw, "__dict__"):
            return dict(vars(raw))
        return dict(raw)

    @staticmethod
    def _is_relative_to(path: Path, parent: Path) -> bool:
        try:
            path.relative_to(parent)
            return True
        except ValueError:
            return False
