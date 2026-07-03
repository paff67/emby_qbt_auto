from __future__ import annotations

import json
import shutil
import sqlite3
import time
from pathlib import Path
from typing import Any, Callable, Mapping

from .db import write_transaction
from .observability import redact


def _connect(path: str | Path) -> sqlite3.Connection:
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    return con


class OrphanJanitorService:
    """qBT-aware orphan detector.

    Safety defaults mirror the design: run only with healthy qBT sync, require
    repeated confirmation, and quarantine to trash instead of deleting.
    """

    def __init__(
        self,
        state_db: str | Path,
        managed_root: str | Path,
        trash_dir: str | Path,
        dry_run: bool = True,
        min_age_sec: int = 86400,
        min_confirmations: int = 2,
        now: Callable[[], int] | None = None,
        host_downloads: str | None = None,
        container_downloads: str | None = None,
    ):
        self.state_db = Path(state_db)
        self.managed_root = Path(managed_root)
        self.trash_dir = Path(trash_dir)
        self.dry_run = bool(dry_run)
        self.min_age_sec = int(min_age_sec)
        self.min_confirmations = max(1, int(min_confirmations))
        self.now = now or (lambda: int(time.time()))
        self.host_downloads = host_downloads.rstrip("/") if host_downloads else None
        self.container_downloads = container_downloads.rstrip("/") if container_downloads else None

    def reconcile(self, snapshots: Mapping[str, Any], sync_healthy: bool) -> dict[str, Any]:
        now = int(self.now())
        if not sync_healthy:
            self._event("warning", "suspended_unhealthy_sync", "orphan janitor suspended because qBT sync is unhealthy", {"dry_run": self.dry_run})
            return {"suspended": True, "reason": "unhealthy_sync", "scanned": 0, "confirmed_orphans": [], "quarantined": []}
        protected = self._protected_top_level_paths(snapshots)
        candidates = self._scan_candidates(now, protected)
        confirmed: list[str] = []
        quarantined: list[dict[str, str]] = []
        for path in candidates:
            confirmations = self._record_candidate(path, now)
            if confirmations < self.min_confirmations:
                continue
            confirmed.append(str(path))
            if self.dry_run:
                self._mark_confirmed(path, now)
                self._action(path, path, "dry_run", True)
                continue
            dest = self._trash_destination(path)
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(path), str(dest))
            self._mark_quarantined(path, dest, now)
            self._action(path, dest, "succeeded", False)
            quarantined.append({"from": str(path), "to": str(dest)})
        return {
            "suspended": False,
            "scanned": len(candidates),
            "protected": len(protected),
            "confirmed_orphans": confirmed,
            "quarantined": quarantined,
            "dry_run": self.dry_run,
        }

    def _scan_candidates(self, now: int, protected: set[Path]) -> list[Path]:
        if not self.managed_root.exists() or not self.managed_root.is_dir():
            return []
        candidates: list[Path] = []
        trash_resolved = self.trash_dir.resolve()
        for entry in self.managed_root.iterdir():
            try:
                path = entry.resolve()
                if path == trash_resolved or self._is_relative_to(path, trash_resolved):
                    continue
                if path in protected:
                    continue
                age = now - int(entry.stat().st_mtime)
                if self.min_age_sec > 0 and age < self.min_age_sec:
                    continue
                candidates.append(path)
            except FileNotFoundError:
                continue
        return sorted(candidates, key=lambda p: str(p))

    def _protected_top_level_paths(self, snapshots: Mapping[str, Any]) -> set[Path]:
        protected: set[Path] = set()
        root = self.managed_root.resolve()
        for raw in snapshots.values():
            if hasattr(raw, "content_path"):
                content_path = getattr(raw, "content_path", "")
                save_path = getattr(raw, "save_path", "")
                name = getattr(raw, "name", "")
            else:
                item = dict(raw or {})
                content_path = str(item.get("content_path") or "")
                save_path = str(item.get("save_path") or "")
                name = str(item.get("name") or item.get("hash") or "")
            path_s = content_path or (str(Path(save_path) / name) if save_path and name else "")
            mapped = self._map_to_host_path(path_s)
            if not mapped:
                continue
            path = Path(mapped)
            try:
                resolved = path.resolve()
                if resolved == root:
                    protected.add(root)
                elif self._is_relative_to(resolved, root):
                    rel = resolved.relative_to(root)
                    if rel.parts:
                        protected.add(root / rel.parts[0])
            except OSError:
                continue
        return protected

    def _map_to_host_path(self, path_s: str) -> str:
        if not path_s:
            return ""
        if self.host_downloads and self.container_downloads:
            if path_s == self.container_downloads:
                return self.host_downloads
            if path_s.startswith(self.container_downloads + "/"):
                return self.host_downloads + path_s[len(self.container_downloads):]
        return path_s

    def _record_candidate(self, path: Path, now: int) -> int:
        def txn(con: sqlite3.Connection) -> int:
            row = con.execute("select confirmations from orphan_candidates where path=?", (str(path),)).fetchone()
            if row is None:
                confirmations = 1
                con.execute(
                    "insert into orphan_candidates(path,first_seen_at,last_seen_at,confirmations,state) values(?,?,?,?,?)",
                    (str(path), now, now, confirmations, "seen"),
                )
            else:
                confirmations = int(row["confirmations"] or 0) + 1
                con.execute(
                    "update orphan_candidates set last_seen_at=?, confirmations=?, state=? where path=?",
                    (now, confirmations, "confirmed" if confirmations >= self.min_confirmations else "seen", str(path)),
                )
            return confirmations

        return int(write_transaction(self.state_db, txn))

    def _mark_confirmed(self, path: Path, now: int) -> None:
        write_transaction(
            self.state_db,
            lambda con: con.execute("update orphan_candidates set state='confirmed', last_seen_at=? where path=?", (now, str(path))),
        )

    def _mark_quarantined(self, path: Path, dest: Path, now: int) -> None:
        write_transaction(
            self.state_db,
            lambda con: con.execute(
                "update orphan_candidates set state='quarantined', quarantined_at=?, trash_path=? where path=?",
                (now, str(dest), str(path)),
            ),
        )

    def _trash_destination(self, path: Path) -> Path:
        dest = self.trash_dir / path.name
        if not dest.exists():
            return dest
        suffix = int(self.now())
        return self.trash_dir / f"{path.name}.{suffix}"

    def _action(self, src: Path, dest: Path, status: str, dry_run: bool) -> None:
        now = int(self.now())
        payload = {"from": str(src), "to": str(dest)}
        def txn(con: sqlite3.Connection) -> None:
            con.execute(
                "insert into action_log(ts,action_type,path,payload_json,status,dry_run) values(?,?,?,?,?,?)",
                (now, "orphan_quarantine", str(src), json.dumps(redact(payload), ensure_ascii=False), status, 1 if dry_run else 0),
            )
            con.execute(
                "insert into events_v2(ts,level,component,event_type,message,data_json) values(?,?,?,?,?,?)",
                (now, "warning", "orphan_janitor", "orphan_quarantine", f"orphan quarantine {status}", json.dumps(redact(payload), ensure_ascii=False)),
            )

        write_transaction(self.state_db, txn)

    def _event(self, level: str, event_type: str, message: str, data: dict[str, Any]) -> None:
        now = int(self.now())
        write_transaction(
            self.state_db,
            lambda con: con.execute(
                "insert into events_v2(ts,level,component,event_type,message,data_json) values(?,?,?,?,?,?)",
                (now, level, "orphan_janitor", event_type, message, json.dumps(redact(data), ensure_ascii=False)),
            ),
        )

    @staticmethod
    def _is_relative_to(path: Path, parent: Path) -> bool:
        try:
            path.relative_to(parent)
            return True
        except ValueError:
            return False
