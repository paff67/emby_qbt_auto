from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path, PurePosixPath
from typing import Any, Mapping

from .db import write_transaction
from .observability import redact


def _tags(torrent: Mapping[str, Any]) -> set[str]:
    return {p.strip() for p in str(torrent.get("tags") or "").split(",") if p.strip()}


def _is_managed(torrent: Mapping[str, Any]) -> bool:
    tags = _tags(torrent)
    return (str(torrent.get("category") or "") == "auto" or "auto" in tags) and "hold" not in tags


def _norm_posix(path: str) -> str:
    if not path:
        return ""
    # PurePosixPath removes duplicate separators but keeps paths container-style
    # without consulting the host filesystem.
    out = str(PurePosixPath(path))
    if out != "/" and out.endswith("/"):
        out = out.rstrip("/")
    return out


def _under(path: str, root: str) -> bool:
    path = _norm_posix(path)
    root = _norm_posix(root)
    if not path or not root:
        return False
    return path == root or path.startswith(root + "/")


class QbtPathReconciler:
    """Read-only qBT path drift detector.

    The US1 live qBT has shown torrents whose ``save_path`` advertises
    ``/downloads/active`` while ``content_path`` still points at historical
    locations such as ``/downloads/BBAN-582``.  The daemon must not silently
    quarantine or cleanup around those paths; first it records precise drift
    evidence so operators can reconcile deliberately.
    """

    def __init__(
        self,
        state_db: str | Path,
        expected_save_path: str = "/downloads/active",
        allowed_temp_path: str = "/downloads/incomplete",
    ):
        self.state_db = Path(state_db)
        self.expected_save_path = _norm_posix(expected_save_path)
        self.allowed_temp_path = _norm_posix(allowed_temp_path)

    def reconcile(self, snapshots: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
        scanned = 0
        drifts: list[dict[str, Any]] = []
        for h, torrent_raw in snapshots.items():
            torrent = dict(torrent_raw)
            torrent.setdefault("hash", h)
            if not _is_managed(torrent):
                continue
            scanned += 1
            drift = self._detect_one(torrent)
            if drift is None:
                continue
            drifts.append(drift)
            self._record_once(drift)
        return {"scanned": scanned, "drift_count": len(drifts), "drifts": drifts, "dry_run": True}

    def _detect_one(self, torrent: Mapping[str, Any]) -> dict[str, Any] | None:
        save_path = _norm_posix(str(torrent.get("save_path") or ""))
        content_path = _norm_posix(str(torrent.get("content_path") or ""))
        allowed_roots = [self.expected_save_path]
        if self.allowed_temp_path:
            allowed_roots.append(self.allowed_temp_path)

        reason = ""
        if save_path and save_path != self.expected_save_path:
            reason = "save_path_mismatch"
        elif content_path and not any(_under(content_path, root) for root in allowed_roots):
            reason = "content_path_outside_managed_roots"
        if not reason:
            return None
        return {
            "hash": str(torrent.get("hash") or ""),
            "name": str(torrent.get("name") or ""),
            "reason": reason,
            "save_path": save_path,
            "content_path": content_path,
            "expected_save_path": self.expected_save_path,
            "allowed_roots": allowed_roots,
            "progress": float(torrent.get("progress") or 0.0),
        }

    def _record_once(self, drift: dict[str, Any]) -> None:
        data = redact(drift)
        data_json = json.dumps(data, ensure_ascii=False, sort_keys=True)
        con = sqlite3.connect(self.state_db)
        try:
            row = con.execute(
                "select data_json from events_v2 where component='qbt_reconcile' and event_type='path_drift' and hash=? order by id desc limit 1",
                (drift.get("hash"),),
            ).fetchone()
            if row and (row[0] or "") == data_json:
                return
            write_transaction(
                self.state_db,
                lambda wcon: wcon.execute(
                    "insert into events_v2(ts,level,component,event_type,hash,message,data_json) values(?,?,?,?,?,?,?)",
                    (
                        int(time.time()),
                        "warning",
                        "qbt_reconcile",
                        "path_drift",
                        drift.get("hash"),
                        f"qBT path drift: {drift.get('reason')}",
                        data_json,
                    ),
                ),
            )
        finally:
            con.close()
