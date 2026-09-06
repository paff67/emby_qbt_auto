from __future__ import annotations

import json
import re
import sqlite3
import time
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any, Callable, Mapping, Sequence

from .db import write_transaction
from .decision_recorder import DecisionRecorder
from .observability import redact
from .runtime import ObservabilityStore


VIDEO_EXTS = {".mp4", ".mkv", ".avi", ".mov", ".wmv", ".m4v", ".ts", ".webm"}
SUPPORT_EXTS = {".txt", ".nfo", ".srt", ".ass", ".ssa", ".vtt", ".sub", ".idx", ".jpg", ".jpeg", ".png", ".webp"}
OBSERVE_TAGS = ("metadata-timeout", "observe", "precheck")
ADD_TAGS = ("auto", "checked")

AD_KEYWORD_TOKENS = (
    "最新地址",
    "最新地址获取",
    "最新地址獲取",
    "地址获取",
    "地址獲取",
    "位址获取",
    "位址獲取",
    "收藏不迷路",
    "官方指定",
    "博彩",
    "赌场",
    "賭場",
    "广告",
    "廣告",
    "直播",
    "聚合全網",
    "聚合全网",
    "全网直播",
    "全網直播",
    "社区最新情报",
    "社區最新情報",
    "telegram",
    "t66y",
    "草榴",
    "996gg",
    "manko.fun",
    "x18r.tv",
)


@dataclass(frozen=True)
class ObservePromotionConfig:
    min_main_bytes: int = 100 * 1024 * 1024
    max_per_tick: int = 50


class ObservePromotionService:
    """Promote metadata-ready observe torrents into v2 auto management.

    Legacy enrollment adds `precheck,metadata-timeout,observe` while a magnet
    still lacks metadata.  Daemon v2 only manages `auto`, so once qBT exposes a
    real file list this service applies qBT-aware priorities, removes observe
    tags, adds `auto,checked`, sets category `auto`, and stops the torrent for
    the planner to resume under disk budget control.
    """

    def __init__(
        self,
        state_db,
        qbt,
        executor,
        dry_run: bool = True,
        config: ObservePromotionConfig | None = None,
        now: Callable[[], int] | None = None,
    ):
        self.state_db = state_db
        self.qbt = qbt
        self.executor = executor
        self.dry_run = bool(dry_run)
        self.config = config or ObservePromotionConfig()
        self.now = now or (lambda: int(time.time()))
        self.decision_recorder = DecisionRecorder(state_db, now=self.now)
        self.obs = ObservabilityStore(state_db, now=self.now)

    def promote_ready(self, snapshots: Mapping[str, Mapping[str, Any]], sync_healthy: bool) -> dict[str, Any]:
        if not sync_healthy:
            self._transition_event(
                "warning",
                None,
                "suspended",
                "unhealthy_sync",
                "suspended_unhealthy_sync",
                "observe promotion suspended because qBT sync is unhealthy",
                {"dry_run": self.dry_run},
            )
            self._record_loop_metrics(0, [], {}, suspended=True)
            return {"suspended": True, "reason": "unhealthy_sync", "scanned": 0, "promoted": [], "skipped": {}, "dropped_indices": {}, "dry_run": self.dry_run}

        # Reset the component-wide suspended state so a later outage is a new
        # transition, while avoiding a repetitive "healthy" event.
        self.decision_recorder.record(
            "observe_promotion", "", "running", "sync_healthy", {"dry_run": self.dry_run}
        )

        scanned = 0
        promoted: list[str] = []
        skipped: dict[str, str] = {}
        dropped_indices: dict[str, list[int]] = {}
        kept_indices: dict[str, list[int]] = {}
        for h, raw_torrent in snapshots.items():
            if scanned >= int(self.config.max_per_tick):
                break
            torrent = self._snapshot(raw_torrent)
            torrent_hash = str(torrent.get("hash") or h)
            if not self._is_candidate(torrent):
                continue
            scanned += 1
            if self._has_metadata_explicitly_false(torrent):
                skipped[torrent_hash] = "metadata_not_ready"
                self._transition_event(
                    "info",
                    torrent_hash,
                    "skipped",
                    "metadata_not_ready",
                    "skipped",
                    "observe torrent metadata is not ready",
                    {"reason": "metadata_not_ready"},
                )
                continue
            try:
                files = [dict(item) for item in self.qbt.torrent_files(torrent_hash)]
            except Exception as exc:
                skipped[torrent_hash] = "file_list_failed"
                self._transition_event(
                    "error",
                    torrent_hash,
                    "skipped",
                    "file_list_failed",
                    "file_list_failed",
                    str(redact(str(exc))),
                    {"reason": "file_list_failed"},
                )
                continue
            selection = self._select_files(files)
            if not selection["main_indices"]:
                skipped[torrent_hash] = "metadata_not_ready" if not files else "no_main_media"
                self._transition_event(
                    "info",
                    torrent_hash,
                    "skipped",
                    skipped[torrent_hash],
                    "skipped",
                    "observe torrent has no selectable main media yet",
                    {"reason": skipped[torrent_hash], "files": len(files)},
                )
                continue

            keep = selection["keep_indices"]
            drop = selection["drop_indices"]
            actions = self._promotion_actions(torrent_hash, keep, drop)
            failed = False
            for path, payload in actions:
                try:
                    self._qbt_post(torrent_hash, path, payload)
                except Exception as exc:
                    failed = True
                    skipped[torrent_hash] = "qbt_write_failed"
                    self._transition_event(
                        "error",
                        torrent_hash,
                        "promotion_failed",
                        "qbt_write_failed",
                        "qbt_write_failed",
                        str(redact(str(exc))),
                        {"path": path, "payload": payload},
                    )
                    break
            if failed:
                continue
            promoted.append(torrent_hash)
            dropped_indices[torrent_hash] = drop
            kept_indices[torrent_hash] = keep
            self._transition_event(
                "info",
                torrent_hash,
                "promoted",
                "promotion_complete",
                "promoted",
                "observe torrent promoted to auto",
                {
                    "kept_indices": keep,
                    "dropped_indices": drop,
                    "dry_run": self.dry_run,
                    "main_indices": selection["main_indices"],
                },
            )

        self._record_loop_metrics(scanned, promoted, skipped, suspended=False)
        return {
            "suspended": False,
            "scanned": scanned,
            "promoted": promoted,
            "skipped": skipped,
            "kept_indices": kept_indices,
            "dropped_indices": dropped_indices,
            "dry_run": self.dry_run,
        }

    def _promotion_actions(self, h: str, keep_indices: Sequence[int], drop_indices: Sequence[int]) -> list[tuple[str, dict[str, str]]]:
        actions: list[tuple[str, dict[str, str]]] = []
        if drop_indices:
            actions.append(("/api/v2/torrents/filePrio", {"hash": h, "id": "|".join(str(i) for i in drop_indices), "priority": "0"}))
        if keep_indices:
            actions.append(("/api/v2/torrents/filePrio", {"hash": h, "id": "|".join(str(i) for i in keep_indices), "priority": "1"}))
        actions.extend(
            [
                ("/api/v2/torrents/removeTags", {"hashes": h, "tags": ",".join(OBSERVE_TAGS)}),
                ("/api/v2/torrents/addTags", {"hashes": h, "tags": ",".join(ADD_TAGS)}),
                ("/api/v2/torrents/setCategory", {"hashes": h, "category": "auto"}),
                ("/api/v2/torrents/setForceStart", {"hashes": h, "value": "false"}),
                ("/api/v2/torrents/stop", {"hashes": h}),
            ]
        )
        return actions

    def _select_files(self, files: Sequence[Mapping[str, Any]]) -> dict[str, list[int]]:
        keep: list[int] = []
        drop: list[int] = []
        main: list[int] = []
        for fallback_index, raw in enumerate(files):
            index = int(raw.get("index", fallback_index))
            name = str(raw.get("name") or "")
            size = int(raw.get("size") or 0)
            ext = PurePosixPath(name).suffix.lower()
            hard_junk = self._is_hard_junk(name)
            is_main_video = ext in VIDEO_EXTS and size >= int(self.config.min_main_bytes) and not hard_junk
            keep_support = ext in SUPPORT_EXTS and not hard_junk
            if is_main_video:
                main.append(index)
                keep.append(index)
            elif keep_support:
                keep.append(index)
            else:
                drop.append(index)
        keep = sorted(set(keep))
        drop = sorted(set(drop) - set(keep))
        main = sorted(set(main))
        return {"keep_indices": keep, "drop_indices": drop, "main_indices": main}

    def _qbt_post(self, h: str, path: str, payload: dict[str, Any]) -> None:
        if self.dry_run:
            self._action(h, path, payload, "dry_run", True)
            return
        try:
            self.executor.qbt_post(path, payload)
        except Exception as exc:
            self._action(h, path, payload, "failed", False, str(exc))
            raise
        self._action(h, path, payload, "succeeded", False)

    def _action(self, h: str, path: str, payload: dict[str, Any], status: str, dry_run: bool, error: str | None = None) -> None:
        def txn(con: sqlite3.Connection) -> None:
            con.execute(
                "insert into action_log(ts,hash,action_type,path,payload_json,status,dry_run,error) values(?,?,?,?,?,?,?,?)",
                (int(self.now()), h, "observe_promotion", path, json.dumps(redact(payload), ensure_ascii=False), status, 1 if dry_run else 0, redact(error)),
            )

        write_transaction(self.state_db, txn)

    def _event(self, level: str, h: str | None, event_type: str, message: str, data: dict[str, Any]) -> None:
        def txn(con: sqlite3.Connection) -> None:
            con.execute(
                "insert into events_v2(ts,level,component,event_type,hash,message,data_json) values(?,?,?,?,?,?,?)",
                (int(self.now()), level, "observe_promotion", event_type, h, message, json.dumps(redact(data), ensure_ascii=False)),
            )

        write_transaction(self.state_db, txn)

    def _transition_event(
        self,
        level: str,
        h: str | None,
        decision: str,
        reason_code: str,
        event_type: str,
        message: str,
        data: dict[str, Any],
    ) -> bool:
        changed = self.decision_recorder.record(
            "observe_promotion", h, decision, reason_code, data
        )
        if changed:
            self._event(level, h, event_type, message, data)
        return changed

    def _record_loop_metrics(
        self,
        scanned: int,
        promoted: Sequence[str],
        skipped: Mapping[str, str],
        *,
        suspended: bool,
    ) -> None:
        reason_counts: dict[str, int] = {}
        for reason in skipped.values():
            reason_counts[str(reason)] = reason_counts.get(str(reason), 0) + 1
        self.obs.metric_snapshot(
            "observe_promotion",
            {
                "suspended": bool(suspended),
                "scanned": int(scanned),
                "promoted": len(promoted),
                **dict(sorted(reason_counts.items())),
                "sample_hashes": sorted(str(h) for h in skipped)[:3],
            },
        )

    @staticmethod
    def _snapshot(raw: Any) -> dict[str, Any]:
        if raw is None:
            return {}
        if hasattr(raw, "__dict__"):
            return dict(vars(raw))
        return dict(raw)

    @staticmethod
    def _tags(torrent: Mapping[str, Any]) -> set[str]:
        return {p.strip() for p in str(torrent.get("tags") or "").split(",") if p.strip()}

    def _is_candidate(self, torrent: Mapping[str, Any]) -> bool:
        tags = self._tags(torrent)
        if "hold" in tags:
            return False
        if not any(tag in tags for tag in OBSERVE_TAGS):
            return False
        state = str(torrent.get("state") or "").lower()
        if "meta" in state and state not in {"missingfiles"}:
            return False
        return True

    @staticmethod
    def _has_metadata_explicitly_false(torrent: Mapping[str, Any]) -> bool:
        value = torrent.get("has_metadata")
        return value is False or str(value).strip().lower() in {"0", "false", "no"}

    @staticmethod
    def _is_hard_junk(name: str) -> bool:
        compact = re.sub(r"\s+", "", name).lower()
        return any(token.lower() in compact for token in AD_KEYWORD_TOKENS)
