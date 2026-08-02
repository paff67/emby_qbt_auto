from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping

from .db import readonly_connect, write_transaction
from .observability import redact


_ALIAS_TYPES = frozenset(
    {"normalized_id", "infohash_v1", "infohash_v2", "torrent_name", "video_basename"}
)
_LIFECYCLE_RANK = {
    "observed": 10,
    "downloaded": 20,
    "uploaded": 30,
    "ingested_present": 40,
    "manual_delete_pending": 50,
    "manual_delete_failed": 55,
    "manual_deleted": 60,
    "missing_unknown": 5,
}


def _now_default() -> int:
    return int(time.time())


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _row_to_dict(row: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {str(key): row[key] for key in row.keys()}


class ProcessedMediaRepository:
    """Durable per-title lifecycle ledger with permanent deletion tombstones."""

    def __init__(
        self,
        state_db: str | Path,
        *,
        now: Callable[[], int] | None = None,
        enforce: bool = False,
    ):
        self.state_db = Path(state_db)
        self.now = now or _now_default
        self.enforce = bool(enforce)

    def get_by_normalized_id(self, normalized_id: str) -> dict[str, Any] | None:
        nid = str(normalized_id or "").strip()
        if not nid:
            return None
        con = readonly_connect(self.state_db)
        try:
            row = con.execute(
                "select * from processed_media where normalized_id=? collate nocase",
                (nid,),
            ).fetchone()
            return _row_to_dict(row)
        finally:
            con.close()

    def find_by_alias(self, alias_type: str, alias_value: str) -> dict[str, Any] | None:
        if alias_type not in _ALIAS_TYPES:
            raise ValueError("alias_type")
        value = str(alias_value or "").strip()
        if not value:
            return None
        digest = _sha256_text(value.casefold() if alias_type == "normalized_id" else value)
        con = readonly_connect(self.state_db)
        try:
            row = con.execute(
                "select m.* from processed_media_aliases a "
                "join processed_media m on m.id=a.processed_media_id "
                "where a.alias_type=? and a.alias_sha256=?",
                (alias_type, digest),
            ).fetchone()
            return _row_to_dict(row)
        finally:
            con.close()

    def is_permanently_blocked(self, normalized_id: str) -> dict[str, Any] | None:
        if not self.enforce:
            return None
        row = self.get_by_normalized_id(normalized_id)
        if row is None:
            row = self.find_by_alias("normalized_id", normalized_id)
        if row is None:
            return None
        if str(row.get("download_policy") or "") == "block_permanent":
            return row
        return None

    def observe(
        self,
        normalized_id: str,
        *,
        origin: str,
        display_title: str | None = None,
        actor_type: str = "system",
        actor_id: str | None = None,
        correlation_id: str | None = None,
        payload: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self._upsert_lifecycle(
            normalized_id,
            target_state="observed",
            origin=origin,
            display_title=display_title,
            actor_type=actor_type,
            actor_id=actor_id,
            correlation_id=correlation_id,
            event_type="media_observed",
            payload=payload,
        )

    def mark_downloaded(
        self,
        normalized_id: str,
        *,
        origin: str = "qbt",
        qbt_hash: str | None = None,
        actor_type: str = "system",
        actor_id: str | None = None,
        correlation_id: str | None = None,
        payload: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        extra = dict(payload or {})
        if qbt_hash:
            extra["qbt_hash"] = str(qbt_hash)[:128]
        return self._upsert_lifecycle(
            normalized_id,
            target_state="downloaded",
            origin=origin,
            actor_type=actor_type,
            actor_id=actor_id,
            correlation_id=correlation_id,
            event_type="media_downloaded",
            payload=extra,
            set_downloaded=True,
            last_qbt_hash=qbt_hash,
        )

    def mark_uploaded(
        self,
        normalized_id: str,
        *,
        origin: str = "upload",
        remote_path: str | None = None,
        remote_size: int | None = None,
        actor_type: str = "system",
        actor_id: str | None = None,
        correlation_id: str | None = None,
        payload: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        extra = dict(payload or {})
        if remote_path:
            extra["remote_path_sha256"] = _sha256_text(str(remote_path))
        if remote_size is not None:
            extra["remote_size"] = int(remote_size)
        return self._upsert_lifecycle(
            normalized_id,
            target_state="uploaded",
            origin=origin,
            actor_type=actor_type,
            actor_id=actor_id,
            correlation_id=correlation_id,
            event_type="media_uploaded",
            payload=extra,
            set_uploaded=True,
            last_remote_path=remote_path,
            last_remote_size=remote_size,
        )

    def mark_ingested(
        self,
        normalized_id: str,
        *,
        origin: str = "promotion",
        remote_path: str | None = None,
        remote_size: int | None = None,
        actor_type: str = "system",
        actor_id: str | None = None,
        correlation_id: str | None = None,
        payload: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self._upsert_lifecycle(
            normalized_id,
            target_state="ingested_present",
            origin=origin,
            actor_type=actor_type,
            actor_id=actor_id,
            correlation_id=correlation_id,
            event_type="emby_ingested",
            payload=payload,
            set_ingested=True,
            last_remote_path=remote_path,
            last_remote_size=remote_size,
        )

    def upsert_alias(
        self,
        normalized_id: str,
        *,
        alias_type: str,
        alias_value: str,
    ) -> bool:
        if alias_type not in _ALIAS_TYPES:
            raise ValueError("alias_type")
        value = str(alias_value or "").strip()
        if not value:
            raise ValueError("alias_value")
        digest = _sha256_text(value.casefold() if alias_type == "normalized_id" else value)
        now = int(self.now())

        def txn(con) -> bool:
            media = con.execute(
                "select id from processed_media where normalized_id=? collate nocase",
                (str(normalized_id).strip(),),
            ).fetchone()
            if media is None:
                raise ValueError("processed_media_missing")
            existing = con.execute(
                "select processed_media_id from processed_media_aliases "
                "where alias_type=? and alias_sha256=?",
                (alias_type, digest),
            ).fetchone()
            if existing is not None:
                return int(existing["processed_media_id"]) == int(media["id"])
            con.execute(
                "insert into processed_media_aliases("
                "processed_media_id,alias_type,alias_value,alias_sha256,created_at) "
                "values(?,?,?,?,?)",
                (int(media["id"]), alias_type, value[:512], digest, now),
            )
            return True

        return bool(write_transaction(self.state_db, txn))

    def history(
        self,
        normalized_id: str,
        *,
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        if limit < 1 or limit > 100:
            raise ValueError("limit")
        if offset < 0:
            raise ValueError("offset")
        con = readonly_connect(self.state_db)
        try:
            media = con.execute(
                "select id from processed_media where normalized_id=? collate nocase",
                (str(normalized_id).strip(),),
            ).fetchone()
            if media is None:
                return []
            rows = con.execute(
                "select * from processed_media_events where processed_media_id=? "
                "order by event_at desc,id desc limit ? offset ?",
                (int(media["id"]), int(limit), int(offset)),
            ).fetchall()
            return [_row_to_dict(row) or {} for row in rows]
        finally:
            con.close()

    def begin_manual_delete(
        self,
        normalized_id: str,
        *,
        actor_id: str,
        reason: str,
        deletion_batch_key: str | None = None,
        deletion_manifest: str | None = None,
        actor_type: str = "cli",
        correlation_id: str | None = None,
        create_if_missing: bool = False,
        origin: str = "manual_delete",
    ) -> dict[str, Any]:
        now = int(self.now())
        nid = str(normalized_id).strip()
        safe_reason = str(redact(str(reason)))[:500]
        safe_manifest = str(redact(str(deletion_manifest)))[:4000] if deletion_manifest else None

        def txn(con) -> dict[str, Any]:
            row = con.execute(
                "select * from processed_media where normalized_id=? collate nocase",
                (nid,),
            ).fetchone()
            if row is None:
                if not create_if_missing:
                    raise ValueError("processed_media_missing")
                cur = con.execute(
                    "insert into processed_media("
                    "normalized_id,display_title,origin,lifecycle_state,download_policy,"
                    "first_seen_at,manual_delete_requested_at,manually_deleted_by,"
                    "deletion_reason,deletion_batch_key,deletion_manifest,"
                    "created_at,updated_at) values(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        nid,
                        nid,
                        origin,
                        "manual_delete_pending",
                        "block_permanent",
                        now,
                        now,
                        str(actor_id)[:128],
                        safe_reason,
                        deletion_batch_key,
                        safe_manifest,
                        now,
                        now,
                    ),
                )
                media_id = int(cur.lastrowid)
            else:
                if str(row["download_policy"]) == "block_permanent" and str(
                    row["lifecycle_state"]
                ) in {"manual_delete_pending", "manual_deleted", "manual_delete_failed"}:
                    media_id = int(row["id"])
                else:
                    con.execute(
                        "update processed_media set lifecycle_state='manual_delete_pending',"
                        "download_policy='block_permanent',manual_delete_requested_at=?,"
                        "manually_deleted_by=?,deletion_reason=?,deletion_batch_key=?,"
                        "deletion_manifest=?,updated_at=?,row_version=row_version+1 "
                        "where id=?",
                        (
                            now if row["manual_delete_requested_at"] is None else row["manual_delete_requested_at"],
                            str(actor_id)[:128],
                            safe_reason,
                            deletion_batch_key,
                            safe_manifest,
                            now,
                            int(row["id"]),
                        ),
                    )
                    media_id = int(row["id"])
            self._insert_event(
                con,
                media_id,
                event_type="manual_delete_pending",
                event_at=now,
                actor_type=actor_type,
                actor_id=actor_id,
                correlation_id=correlation_id,
                payload={
                    "reason": safe_reason,
                    "deletion_batch_key": deletion_batch_key,
                    "manifest_sha256": (
                        _sha256_text(safe_manifest) if safe_manifest else None
                    ),
                },
            )
            self._ensure_normalized_alias(con, media_id, nid, now)
            return _row_to_dict(
                con.execute("select * from processed_media where id=?", (media_id,)).fetchone()
            ) or {}

        return write_transaction(self.state_db, txn)

    def finalize_manual_delete(
        self,
        normalized_id: str,
        *,
        actor_id: str,
        actor_type: str = "cli",
        correlation_id: str | None = None,
        payload: Mapping[str, Any] | None = None,
        stage_payload: Mapping[str, Any] | None = None,
        effective_at: int | None = None,
    ) -> dict[str, Any]:
        now = int(self.now())
        event_at = int(effective_at) if effective_at is not None else now

        def txn(con) -> dict[str, Any]:
            row = con.execute(
                "select * from processed_media where normalized_id=? collate nocase",
                (str(normalized_id).strip(),),
            ).fetchone()
            if row is None:
                raise ValueError("processed_media_missing")
            if str(row["download_policy"]) != "block_permanent":
                raise ValueError("download_policy")
            # Idempotent for already-completed deletions (normal saga resume).
            if (
                str(row["lifecycle_state"] or "") == "manual_deleted"
                and row["manually_deleted_at"] is not None
            ):
                return _row_to_dict(row) or {}
            con.execute(
                "update processed_media set lifecycle_state='manual_deleted',"
                "manually_deleted_at=?,manually_deleted_by=?,updated_at=?,"
                "row_version=row_version+1 where id=?",
                (event_at, str(actor_id)[:128], now, int(row["id"])),
            )
            event_payload = dict(payload or {})
            if effective_at is not None:
                event_payload.setdefault("deleted_at", int(effective_at))
            self._insert_event(
                con,
                int(row["id"]),
                event_type="manual_deleted",
                event_at=event_at,
                actor_type=actor_type,
                actor_id=actor_id,
                correlation_id=correlation_id,
                payload=event_payload,
            )
            # Persist final saga stage in the same write transaction so a later
            # crash cannot leave manual_deleted summary with a failed stage write.
            stage_body = dict(stage_payload or {})
            stage_body["stage"] = "manual_deleted"
            self._insert_event(
                con,
                int(row["id"]),
                event_type="manual_delete_stage",
                event_at=now,
                actor_type=actor_type,
                actor_id=actor_id,
                correlation_id=correlation_id,
                payload=stage_body,
            )
            return _row_to_dict(
                con.execute(
                    "select * from processed_media where id=?", (int(row["id"]),)
                ).fetchone()
            ) or {}

        return write_transaction(self.state_db, txn)

    def fail_manual_delete(
        self,
        normalized_id: str,
        *,
        actor_id: str,
        error: str,
        actor_type: str = "cli",
        correlation_id: str | None = None,
    ) -> dict[str, Any]:
        now = int(self.now())
        safe_error = str(redact(str(error)))[:500]

        def txn(con) -> dict[str, Any]:
            row = con.execute(
                "select * from processed_media where normalized_id=? collate nocase",
                (str(normalized_id).strip(),),
            ).fetchone()
            if row is None:
                raise ValueError("processed_media_missing")
            current = _row_to_dict(row) or {}
            media_id = int(row["id"])
            # Completed deletions must never become (or remain) manual_delete_failed.
            if current.get("manually_deleted_at") is not None:
                if str(current.get("lifecycle_state") or "") != "manual_deleted":
                    con.execute(
                        "update processed_media set lifecycle_state='manual_deleted',"
                        "download_policy='block_permanent',updated_at=?,"
                        "row_version=row_version+1 where id=?",
                        (now, media_id),
                    )
                    self._insert_event(
                        con,
                        media_id,
                        event_type="manual_delete_state_normalized",
                        event_at=now,
                        actor_type=actor_type,
                        actor_id=actor_id,
                        correlation_id=correlation_id,
                        payload={
                            "from_state": str(current.get("lifecycle_state") or ""),
                            "to_state": "manual_deleted",
                            "reason": "contradiction_repair",
                            "ignored_error": safe_error,
                        },
                    )
                    # Keep saga stage aligned; finalize early-return would otherwise
                    # leave the latest stage stuck before manual_deleted forever.
                    self._insert_event(
                        con,
                        media_id,
                        event_type="manual_delete_stage",
                        event_at=now,
                        actor_type=actor_type,
                        actor_id=actor_id,
                        correlation_id=correlation_id,
                        payload={
                            "stage": "manual_deleted",
                            "reason": "contradiction_repair",
                        },
                    )
                    return _row_to_dict(
                        con.execute(
                            "select * from processed_media where id=?", (media_id,)
                        ).fetchone()
                    ) or {}
                return current
            if str(current.get("lifecycle_state") or "") == "manual_deleted":
                return current
            requested_at = row["manual_delete_requested_at"] or now
            con.execute(
                "update processed_media set lifecycle_state='manual_delete_failed',"
                "download_policy='block_permanent',manual_delete_requested_at=?,"
                "deletion_reason=coalesce(deletion_reason,?),"
                "updated_at=?,row_version=row_version+1 where id=?",
                (requested_at, safe_error, now, media_id),
            )
            self._insert_event(
                con,
                media_id,
                event_type="manual_delete_failed",
                event_at=now,
                actor_type=actor_type,
                actor_id=actor_id,
                correlation_id=correlation_id,
                payload={"error": safe_error},
            )
            return _row_to_dict(
                con.execute(
                    "select * from processed_media where id=?", (media_id,)
                ).fetchone()
            ) or {}

        return write_transaction(self.state_db, txn)

    def register_tombstone(
        self,
        normalized_id: str,
        *,
        deleted_at: int,
        actor_id: str,
        reason: str,
        deletion_manifest: str | None = None,
        deletion_batch_key: str | None = None,
        create_from_audit: bool = False,
        origin: str = "audit",
    ) -> dict[str, Any]:
        row = self.begin_manual_delete(
            normalized_id,
            actor_id=actor_id,
            reason=reason,
            deletion_batch_key=deletion_batch_key,
            deletion_manifest=deletion_manifest,
            create_if_missing=create_from_audit,
            origin=origin,
        )
        return self.finalize_manual_delete(
            normalized_id,
            actor_id=actor_id,
            payload={"deleted_at": int(deleted_at), "source": "audit_manifest"},
            effective_at=int(deleted_at),
        )

    def backfill_from_sources(self, *, dry_run: bool = True) -> dict[str, int]:
        """Seed observed/ingested rows from existing media tables without claiming unverified success."""
        counts = {"inserted": 0, "updated": 0, "skipped": 0, "conflict": 0}
        con = readonly_connect(self.state_db)
        try:
            groups = list(
                con.execute(
                    "select media_group_key,normalized_id,emby_media_dir,created_at,updated_at "
                    "from media_groups order by id"
                )
            )
            verified = {
                str(row["normalized_id"]).strip().upper(): row
                for row in con.execute(
                    "select p.id,g.normalized_id,p.verified_at,p.source_remote,p.target_remote "
                    "from media_promotions p "
                    "join media_groups g on g.id=p.media_group_id "
                    "where p.state='verified' and g.normalized_id is not null"
                )
                if row["normalized_id"]
            }
        finally:
            con.close()

        for group in groups:
            nid = str(group["normalized_id"] or group["media_group_key"] or "").strip()
            if not nid or nid.lower() in {"unknown", "normalize_failed", "missing_remote"}:
                counts["skipped"] += 1
                continue
            existing = self.get_by_normalized_id(nid)
            verified_row = verified.get(nid.upper())
            if dry_run:
                if existing is None:
                    counts["inserted"] += 1
                elif verified_row is not None and str(existing.get("lifecycle_state")) != "ingested_present":
                    counts["updated"] += 1
                else:
                    counts["skipped"] += 1
                continue
            if verified_row is not None:
                before = existing
                self.mark_ingested(
                    nid,
                    origin="backfill",
                    remote_path=str(verified_row["target_remote"] or "") or None,
                    correlation_id=f"backfill:promotion:{verified_row['id']}",
                    payload={"source": "media_promotions"},
                )
                counts["inserted" if before is None else "updated"] += 1
            else:
                before = existing
                self.observe(
                    nid,
                    origin="backfill",
                    display_title=nid,
                    correlation_id=f"backfill:group:{group['media_group_key']}",
                    payload={"source": "media_groups"},
                )
                counts["inserted" if before is None else "updated"] += 1
        return counts

    def _upsert_lifecycle(
        self,
        normalized_id: str,
        *,
        target_state: str,
        origin: str,
        display_title: str | None = None,
        actor_type: str,
        actor_id: str | None,
        correlation_id: str | None,
        event_type: str,
        payload: Mapping[str, Any] | None,
        set_downloaded: bool = False,
        set_uploaded: bool = False,
        set_ingested: bool = False,
        last_qbt_hash: str | None = None,
        last_remote_path: str | None = None,
        last_remote_size: int | None = None,
    ) -> dict[str, Any]:
        nid = str(normalized_id or "").strip()
        if not nid:
            raise ValueError("normalized_id")
        now = int(self.now())
        safe_payload = redact(dict(payload or {}))

        def txn(con) -> dict[str, Any]:
            row = con.execute(
                "select * from processed_media where normalized_id=? collate nocase",
                (nid,),
            ).fetchone()
            if row is None:
                cur = con.execute(
                    "insert into processed_media("
                    "normalized_id,display_title,origin,lifecycle_state,download_policy,"
                    "first_seen_at,first_downloaded_at,first_uploaded_at,first_ingested_at,"
                    "last_ingested_at,ingestion_count,last_qbt_hash,last_remote_path,"
                    "last_remote_size,last_remote_seen_at,created_at,updated_at) "
                    "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        nid,
                        display_title or nid,
                        str(origin)[:64],
                        target_state,
                        "normal",
                        now,
                        now if set_downloaded else None,
                        now if set_uploaded else None,
                        now if set_ingested else None,
                        now if set_ingested else None,
                        1 if set_ingested else 0,
                        (str(last_qbt_hash)[:128] if last_qbt_hash else None),
                        (str(last_remote_path)[:1024] if last_remote_path else None),
                        last_remote_size,
                        now if last_remote_path else None,
                        now,
                        now,
                    ),
                )
                media_id = int(cur.lastrowid)
            else:
                media_id = int(row["id"])
                if str(row["download_policy"]) == "block_permanent":
                    # Never clear tombstones; still append lifecycle evidence events.
                    if set_ingested:
                        con.execute(
                            "update processed_media set ingestion_count=ingestion_count+1,"
                            "last_ingested_at=?,updated_at=?,row_version=row_version+1 where id=?",
                            (now, now, media_id),
                        )
                    else:
                        con.execute(
                            "update processed_media set updated_at=?,row_version=row_version+1 where id=?",
                            (now, media_id),
                        )
                else:
                    current_rank = _LIFECYCLE_RANK.get(str(row["lifecycle_state"]), 0)
                    next_rank = _LIFECYCLE_RANK.get(target_state, 0)
                    next_state = (
                        target_state
                        if next_rank >= current_rank
                        or (set_ingested and str(row["lifecycle_state"]) == "ingested_present")
                        else str(row["lifecycle_state"])
                    )
                    ingestion_count = int(row["ingestion_count"] or 0)
                    first_ingested = row["first_ingested_at"]
                    last_ingested = row["last_ingested_at"]
                    if set_ingested:
                        ingestion_count += 1
                        first_ingested = first_ingested or now
                        last_ingested = now
                        next_state = "ingested_present"
                    con.execute(
                        "update processed_media set display_title=coalesce(?,display_title),"
                        "lifecycle_state=?,first_downloaded_at=coalesce(first_downloaded_at,?),"
                        "first_uploaded_at=coalesce(first_uploaded_at,?),"
                        "first_ingested_at=?,last_ingested_at=?,ingestion_count=?,"
                        "last_qbt_hash=coalesce(?,last_qbt_hash),"
                        "last_remote_path=coalesce(?,last_remote_path),"
                        "last_remote_size=coalesce(?,last_remote_size),"
                        "last_remote_seen_at=case when ? is not null then ? else last_remote_seen_at end,"
                        "updated_at=?,row_version=row_version+1 where id=?",
                        (
                            display_title,
                            next_state,
                            now if set_downloaded else None,
                            now if set_uploaded else None,
                            first_ingested,
                            last_ingested,
                            ingestion_count,
                            (str(last_qbt_hash)[:128] if last_qbt_hash else None),
                            (str(last_remote_path)[:1024] if last_remote_path else None),
                            last_remote_size,
                            last_remote_path,
                            now,
                            now,
                            media_id,
                        ),
                    )
            self._insert_event(
                con,
                media_id,
                event_type=event_type,
                event_at=now,
                actor_type=actor_type,
                actor_id=actor_id,
                correlation_id=correlation_id,
                payload=safe_payload if isinstance(safe_payload, dict) else {},
            )
            self._ensure_normalized_alias(con, media_id, nid, now)
            return _row_to_dict(
                con.execute("select * from processed_media where id=?", (media_id,)).fetchone()
            ) or {}

        return write_transaction(self.state_db, txn)

    @staticmethod
    def _ensure_normalized_alias(con, media_id: int, normalized_id: str, now: int) -> None:
        digest = _sha256_text(normalized_id.casefold())
        con.execute(
            "insert or ignore into processed_media_aliases("
            "processed_media_id,alias_type,alias_value,alias_sha256,created_at) "
            "values(?,?,?,?,?)",
            (media_id, "normalized_id", normalized_id, digest, now),
        )

    @staticmethod
    def _insert_event(
        con,
        media_id: int,
        *,
        event_type: str,
        event_at: int,
        actor_type: str,
        actor_id: str | None,
        correlation_id: str | None,
        payload: Mapping[str, Any] | None,
    ) -> None:
        con.execute(
            "insert into processed_media_events("
            "processed_media_id,event_type,event_at,actor_type,actor_id,correlation_id,payload_json) "
            "values(?,?,?,?,?,?,?)",
            (
                media_id,
                str(event_type)[:64],
                int(event_at),
                str(actor_type)[:32],
                (str(actor_id)[:128] if actor_id else None),
                (str(correlation_id)[:128] if correlation_id else None),
                json.dumps(dict(payload or {}), ensure_ascii=False, sort_keys=True),
            ),
        )


def manifest_sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
