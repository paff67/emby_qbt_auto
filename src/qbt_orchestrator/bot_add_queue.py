from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import time
import uuid
from dataclasses import dataclass, fields as dataclass_fields
from pathlib import Path
from typing import Any, Callable, Mapping
from urllib.parse import urlsplit

from .db import readonly_connect, write_transaction


_ITEM_STATES = frozenset(
    {
        "received",
        "invalid",
        "resolving",
        "duplicate_local",
        "waiting_probe_slot",
        "metadata_wait",
        "metadata_retry_wait",
        "metadata_unavailable",
        "prechecking",
        "duplicate_remote",
        "needs_confirmation",
        "ready",
        "enrolling",
        "enrolled",
        "enrolled_hold",
        "failed",
        "cancelled",
    }
)
_AUTOMATIC_TERMINAL_ITEM_STATES = frozenset(
    {
        "invalid",
        "duplicate_local",
        "metadata_unavailable",
        "duplicate_remote",
        "enrolled",
        "enrolled_hold",
        "failed",
        "cancelled",
    }
)
_RAW_CLEAR_ITEM_STATES = _AUTOMATIC_TERMINAL_ITEM_STATES - {"metadata_unavailable"}
_SUBMITTED_BATCH_STATES = frozenset(
    {"queued", "processing", "awaiting_confirmation"}
)
_IDEMPOTENT_SUBMIT_STATES = _SUBMITTED_BATCH_STATES | {"complete"}
_CANCELLABLE_ITEM_STATES = _ITEM_STATES - {"enrolled", "enrolled_hold", "cancelled"}
_MANUAL_RETRY_TARGET_STATES = frozenset({"waiting_probe_slot", "metadata_retry_wait"})
_METADATA_CALLBACK_TARGETS = {
    "retry_now": "waiting_probe_slot",
    "retry_24h": "metadata_retry_wait",
    "cancel": "cancelled",
}
_METADATA_RETRY_DELAY_SEC = 24 * 60 * 60
_METADATA_APPROVAL_WINDOW_SEC = 15 * 60
_METADATA_RETRY_NOW_ATTEMPT = 0
_METADATA_RETRY_24H_STORED_ATTEMPT = 2
# Lease claims fence workers but deliberately do not mutate probe attempts.
# Before probing, the coordinator must use its fenced lease transition into
# metadata_wait to advance stored attempt 2 to the permitted final attempt 3.
_METADATA_LEASE_STATES = frozenset(
    {
        "resolving",
        "waiting_probe_slot",
        "metadata_wait",
        "metadata_retry_wait",
        "prechecking",
        "enrolling",
    }
)
_ALLOWED_TRANSITIONS = {
    "received": frozenset(
        {"invalid", "resolving", "duplicate_local", "waiting_probe_slot", "cancelled"}
    ),
    "resolving": frozenset(
        {
            "invalid",
            "duplicate_local",
            "waiting_probe_slot",
            "metadata_wait",
            "metadata_retry_wait",
            "metadata_unavailable",
            "prechecking",
            "failed",
            "cancelled",
        }
    ),
    "waiting_probe_slot": frozenset(
        {"metadata_wait", "metadata_retry_wait", "metadata_unavailable", "prechecking", "failed", "cancelled"}
    ),
    "metadata_wait": frozenset(
        {
            "waiting_probe_slot",
            "duplicate_local",
            "metadata_retry_wait",
            "metadata_unavailable",
            "prechecking",
            "failed",
            "cancelled",
        }
    ),
    "metadata_retry_wait": frozenset(
        {"waiting_probe_slot", "metadata_wait", "metadata_unavailable", "failed", "cancelled"}
    ),
    "metadata_unavailable": _MANUAL_RETRY_TARGET_STATES | {"cancelled"},
    "prechecking": frozenset(
        {
            "duplicate_local",
            "duplicate_remote",
            "needs_confirmation",
            "ready",
            "metadata_unavailable",
            "failed",
            "cancelled",
        }
    ),
    "needs_confirmation": frozenset(
        {"duplicate_remote", "ready", "enrolling", "failed", "cancelled"}
    ),
    "ready": frozenset({"enrolling", "cancelled"}),
    "enrolling": frozenset({"enrolled", "enrolled_hold", "failed", "cancelled"}),
    "enrolled_hold": frozenset({"enrolled"}),
}
_FIELD_ALLOWLIST = frozenset(
    {
        "canonical_identity",
        "infohash_v1",
        "infohash_v2",
        "display_name",
        "normalized_media_id",
        "total_size",
        "primary_video_size",
        "decision",
        "decision_reason",
        "qbt_hash",
        "qbt_precheck_tag",
        "remote_match_json",
        "metadata_probe_attempt",
        "metadata_probe_started_at",
        "metadata_probe_deadline",
        "metadata_next_poll_at",
        "metadata_retry_at",
        "approved_by",
        "approved_at",
        "attempts",
        "next_run_at",
        "last_error",
    }
)
_AUTOMATIC_TERMINAL_CLEAR_FIELDS = frozenset(
    {
        "metadata_probe_deadline",
        "metadata_next_poll_at",
        "metadata_retry_at",
        "metadata_lease_owner",
        "metadata_lease_until",
        "next_run_at",
        "last_error",
    }
)
_RAW_INPUT_FIELDS = frozenset({"raw_input", "raw_input_expires_at"})
_METADATA_PROGRESS_FIELDS = frozenset(
    {
        "qbt_hash",
        "qbt_precheck_tag",
        "metadata_probe_attempt",
        "metadata_probe_started_at",
        "metadata_probe_deadline",
        "metadata_next_poll_at",
        "metadata_retry_at",
        "next_run_at",
        "last_error",
        "display_name",
        "normalized_media_id",
        "total_size",
        "primary_video_size",
        "decision",
        "decision_reason",
        "remote_match_json",
    }
)
_SAFE_CODE = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")
_MAX_RAW_INPUT_TTL_SEC = 7 * 86400


@dataclass(frozen=True)
class AddQueueLimits:
    max_links_per_batch: int = 500
    shard_size: int = 50
    max_submitted_items: int = 1000
    max_link_bytes: int = 8192
    max_draft_bytes: int = 2 * 1024 * 1024
    draft_ttl_sec: int = 1800
    raw_input_ttl_sec: int = 7 * 86400

    def __post_init__(self) -> None:
        for field in dataclass_fields(self):
            value = getattr(self, field.name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(field.name)
        if self.shard_size > self.max_links_per_batch:
            raise ValueError("shard_size")
        if self.raw_input_ttl_sec > _MAX_RAW_INPUT_TTL_SEC:
            raise ValueError("raw_input_ttl_sec")


class BotAddQueueRepository:
    """Durable, bounded ingress repository for Telegram download batches.

    Every limit decision and its associated write happen under one SQLite
    ``BEGIN IMMEDIATE`` transaction.  The explicit lock is intentional: the
    per-process write actor provides efficient serialization, while SQLite is
    still the authority when multiple daemon processes overlap during a
    restart.
    """

    def __init__(
        self,
        state_db: str | Path,
        *,
        limits: AddQueueLimits | None = None,
        now: Callable[[], int] | None = None,
    ) -> None:
        self.state_db = Path(state_db)
        self.limits = limits or AddQueueLimits()
        if not isinstance(self.limits, AddQueueLimits):
            raise TypeError("limits")
        self._now = now or (lambda: int(time.time()))

    def open_draft(self, chat_id: str, user_id: str) -> dict[str, Any]:
        chat = self._identity(chat_id, "chat_id")
        user = self._identity(user_id, "user_id")
        now = self._timestamp()

        def txn(con: sqlite3.Connection) -> dict[str, Any]:
            self._begin_immediate(con)
            self._expire_due_in_transaction(con, now)
            row = con.execute(
                "select * from bot_add_batches where chat_id=? and user_id=? and state='draft'",
                (chat, user),
            ).fetchone()
            if row is not None:
                row = self._expire_draft_row_in_transaction(con, row, now)
                if str(row["state"]) == "draft":
                    return self._row(row)
            cursor = con.execute(
                "insert into bot_add_batches("
                "batch_key,chat_id,user_id,state,created_at,updated_at"
                ") values(?,?,?,?,?,?)",
                (uuid.uuid4().hex, chat, user, "draft", now, now),
            )
            return self._batch_in_transaction(con, int(cursor.lastrowid))

        return dict(write_transaction(self.state_db, txn))

    def append_message(
        self,
        batch_id: int,
        message_id: int,
        links: list[str],
    ) -> dict[str, Any]:
        batch_key = self._positive_id(batch_id, "batch_id")
        source_message_id = self._nonnegative_id(message_id, "message_id")
        proposed = self._validate_links(links)
        now = self._timestamp()

        def txn(con: sqlite3.Connection) -> dict[str, Any]:
            self._begin_immediate(con)
            self._expire_due_in_transaction(con, now)
            batch = con.execute(
                "select * from bot_add_batches where id=?", (batch_key,)
            ).fetchone()
            if batch is None:
                raise ValueError("batch_not_found")
            batch = self._expire_draft_row_in_transaction(con, batch, now)
            state = str(batch["state"])
            expected_signature = [
                (index, str(link["input_sha256"]))
                for index, link in enumerate(proposed)
            ]
            if state == "draft_expired":
                expired_message = list(
                    con.execute(
                        "select source_index,input_sha256 from bot_add_items "
                        "where batch_id=? and source_message_id=? order by source_index",
                        (batch_key, source_message_id),
                    )
                )
                if not expired_message:
                    return {"__error__": "draft_expired"}
                expired_signature = [
                    (int(row["source_index"]), str(row["input_sha256"]))
                    for row in expired_message
                ]
                if expired_signature != expected_signature:
                    return {"__error__": "source_message_conflict"}
                result = self._batch_in_transaction(con, batch_key)
                result.update({"inserted_count": 0, "idempotent": True})
                return result

            existing_message = list(
                con.execute(
                    "select i.batch_id,i.source_index,i.input_sha256 "
                    "from bot_add_items i join bot_add_batches b on b.id=i.batch_id "
                    "where b.chat_id=? and i.source_message_id=? "
                    "order by i.batch_id,i.source_index",
                    (str(batch["chat_id"]), source_message_id),
                )
            )
            if existing_message:
                existing_batch_ids = {int(row["batch_id"]) for row in existing_message}
                existing_signature = [
                    (int(row["source_index"]), str(row["input_sha256"]))
                    for row in existing_message
                ]
                if len(existing_batch_ids) != 1 or existing_signature != expected_signature:
                    raise ValueError("source_message_conflict")
                result = self._batch_in_transaction(con, existing_batch_ids.pop())
                result.update({"inserted_count": 0, "idempotent": True})
                return result

            if state != "draft":
                raise ValueError("batch_not_draft")

            count = int(
                con.execute(
                    "select count(*) from bot_add_items where batch_id=?", (batch_key,)
                ).fetchone()[0]
            )
            if count + len(proposed) > self.limits.max_links_per_batch:
                raise ValueError("batch_link_limit")

            existing_bytes = int(
                con.execute(
                    "select coalesce(sum(length(cast(raw_input as blob))),0) "
                    "from bot_add_items where batch_id=?",
                    (batch_key,),
                ).fetchone()[0]
            )
            incoming_bytes = sum(int(link["byte_length"]) for link in proposed)
            if existing_bytes + incoming_bytes > self.limits.max_draft_bytes:
                raise ValueError("draft_byte_limit")

            input_hashes = [str(link["input_sha256"]) for link in proposed]
            expires_at = now + self.limits.raw_input_ttl_sec
            for source_index, link in enumerate(proposed):
                cursor = con.execute(
                    "insert or ignore into bot_add_items("
                    "batch_id,source_message_id,source_index,input_kind,raw_input,"
                    "raw_input_expires_at,redacted_input,input_sha256,state,created_at,updated_at"
                    ") values(?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        batch_key,
                        source_message_id,
                        source_index,
                        link["input_kind"],
                        link["raw_input"],
                        expires_at,
                        link["redacted_input"],
                        link["input_sha256"],
                        "received",
                        now,
                        now,
                    ),
                )
                stored_source = con.execute(
                    "select input_sha256 from bot_add_items "
                    "where batch_id=? and source_message_id=? and source_index=?",
                    (batch_key, source_message_id, source_index),
                ).fetchone()
                if stored_source is not None and str(stored_source["input_sha256"]) != str(
                    link["input_sha256"]
                ):
                    raise ValueError("source_message_conflict")
                if cursor.rowcount != 1:
                    duplicate = con.execute(
                        "select 1 from bot_add_items where batch_id=? and input_sha256=?",
                        (batch_key, str(link["input_sha256"])),
                    ).fetchone()
                    if duplicate is not None:
                        raise ValueError("duplicate_input")
                    raise ValueError("ingress_conflict")
            self._refresh_batch_counters(con, batch_key, now)
            con.execute(
                "update bot_add_batches set updated_at=? where id=?",
                (now, batch_key),
            )
            self._event(
                con,
                batch_id=batch_key,
                item_id=None,
                event_type="message_appended",
                from_state="draft",
                to_state="draft",
                reason="message_accepted",
                now=now,
                evidence={
                    "source_message_id": source_message_id,
                    "item_count": len(proposed),
                    "message_input_sha256": hashlib.sha256(
                        "\n".join(input_hashes).encode("ascii")
                    ).hexdigest(),
                },
            )
            result = self._batch_in_transaction(con, batch_key)
            result.update({"inserted_count": len(proposed), "idempotent": False})
            return result

        result = dict(write_transaction(self.state_db, txn))
        self._raise_result_error(result)
        return result

    def submit(self, batch_id: int) -> dict[str, Any]:
        return self._submit_batch(batch_id, expected_updated_at=None)

    def submit_draft(self, batch_id: int, expected_updated_at: int) -> dict[str, Any]:
        """Submit only while the batch is still a draft with a matching updated_at."""
        if isinstance(expected_updated_at, bool) or not isinstance(expected_updated_at, int):
            raise ValueError("expected_updated_at")
        return self._submit_batch(batch_id, expected_updated_at=int(expected_updated_at))

    def _submit_batch(
        self, batch_id: int, *, expected_updated_at: int | None
    ) -> dict[str, Any]:
        batch_key = self._positive_id(batch_id, "batch_id")
        now = self._timestamp()

        def txn(con: sqlite3.Connection) -> dict[str, Any]:
            self._begin_immediate(con)
            self._expire_due_in_transaction(con, now)
            row = con.execute(
                "select * from bot_add_batches where id=?", (batch_key,)
            ).fetchone()
            if row is None:
                raise ValueError("batch_not_found")
            row = self._expire_draft_row_in_transaction(con, row, now)
            state = str(row["state"])
            if expected_updated_at is None and state in _IDEMPOTENT_SUBMIT_STATES:
                result = self._row(row)
                result["idempotent"] = True
                return result
            if state == "draft_expired":
                return {"__error__": "draft_expired"}
            if state != "draft":
                if expected_updated_at is not None:
                    raise ValueError("draft_generation_conflict")
                raise ValueError("batch_not_draft")
            if (
                expected_updated_at is not None
                and int(row["updated_at"] or 0) != expected_updated_at
            ):
                raise ValueError("draft_generation_conflict")

            item_ids = [
                int(item["id"])
                for item in con.execute(
                    "select id from bot_add_items where batch_id=? order by id", (batch_key,)
                )
            ]
            if not item_ids:
                raise ValueError("empty_batch")
            backlog = self._submitted_nonterminal_in_transaction(con)
            if backlog + len(item_ids) > self.limits.max_submitted_items:
                raise ValueError("global_backlog_limit")

            for shard_index, offset in enumerate(
                range(0, len(item_ids), self.limits.shard_size)
            ):
                item_count = len(item_ids[offset : offset + self.limits.shard_size])
                con.execute(
                    "insert into bot_add_shards("
                    "batch_id,shard_index,state,item_count,processed_count,created_at,updated_at"
                    ") values(?,?,?,?,?,?,?)",
                    (batch_key, shard_index, "queued", item_count, 0, now, now),
                )
            if expected_updated_at is None:
                cursor = con.execute(
                    "update bot_add_batches set state='queued',submitted_at=?,updated_at=? "
                    "where id=? and state='draft'",
                    (now, now, batch_key),
                )
            else:
                cursor = con.execute(
                    "update bot_add_batches set state='queued',submitted_at=?,updated_at=? "
                    "where id=? and state='draft' and updated_at=?",
                    (now, now, batch_key, expected_updated_at),
                )
            if cursor.rowcount != 1:
                raise ValueError("draft_generation_conflict")
            self._event(
                con,
                batch_id=batch_key,
                item_id=None,
                event_type="batch_submitted",
                from_state="draft",
                to_state="queued",
                reason="submitted",
                now=now,
                evidence={
                    "item_count": len(item_ids),
                    "shard_count": (len(item_ids) + self.limits.shard_size - 1)
                    // self.limits.shard_size,
                },
            )
            result = self._batch_in_transaction(con, batch_key)
            result["idempotent"] = False
            return result

        result = dict(write_transaction(self.state_db, txn))
        self._raise_result_error(result)
        return result

    def cancel_draft(
        self, batch_id: int, expected_updated_at: int, actor: str
    ) -> dict[str, Any]:
        """Cancel only while the batch is still a draft with a matching updated_at."""
        batch_key = self._positive_id(batch_id, "batch_id")
        actor_id = self._identity(actor, "actor")
        if isinstance(expected_updated_at, bool) or not isinstance(expected_updated_at, int):
            raise ValueError("expected_updated_at")
        expected = int(expected_updated_at)
        now = self._timestamp()

        def txn(con: sqlite3.Connection) -> dict[str, Any]:
            self._begin_immediate(con)
            self._expire_due_in_transaction(con, now)
            batch = con.execute(
                "select * from bot_add_batches where id=?", (batch_key,)
            ).fetchone()
            if batch is None:
                raise ValueError("batch_not_found")
            batch = self._expire_draft_row_in_transaction(con, batch, now)
            state = str(batch["state"])
            if state != "draft" or int(batch["updated_at"] or 0) != expected:
                raise ValueError("draft_generation_conflict")

            rows = list(
                con.execute(
                    "select id,state from bot_add_items where batch_id=? order by id",
                    (batch_key,),
                )
            )
            changed = 0
            for item in rows:
                old_state = str(item["state"])
                item_id = int(item["id"])
                if old_state not in _CANCELLABLE_ITEM_STATES:
                    continue
                cursor = con.execute(
                    "update bot_add_items set state='cancelled',raw_input=null,"
                    "raw_input_expires_at=null,metadata_probe_deadline=null,"
                    "metadata_next_poll_at=null,metadata_retry_at=null,"
                    "metadata_lease_owner=null,metadata_lease_until=null,"
                    "metadata_lease_generation=metadata_lease_generation+1,"
                    "approval_generation=approval_generation+?,next_run_at=null,"
                    "qbt_precheck_tag=null,updated_at=? where id=? and state=?",
                    (1 if old_state == "enrolling" else 0, now, item_id, old_state),
                )
                if cursor.rowcount != 1:
                    continue
                changed += 1
                self._event(
                    con,
                    batch_id=batch_key,
                    item_id=item_id,
                    event_type="state_transition",
                    from_state=old_state,
                    to_state="cancelled",
                    reason="draft_cancelled",
                    now=now,
                    actor=actor_id,
                )
            con.execute(
                "update bot_add_items set raw_input=null,raw_input_expires_at=null "
                "where batch_id=? and raw_input is not null",
                (batch_key,),
            )
            cursor = con.execute(
                "update bot_add_batches set state='cancelled',completed_at=?,updated_at=? "
                "where id=? and state='draft' and updated_at=?",
                (now, now, batch_key, expected),
            )
            if cursor.rowcount != 1:
                raise ValueError("draft_generation_conflict")
            self._refresh_batch_counters(con, batch_key, now)
            self._event(
                con,
                batch_id=batch_key,
                item_id=None,
                event_type="batch_cancelled",
                from_state="draft",
                to_state="cancelled",
                reason="cancelled_by_operator",
                now=now,
                actor=actor_id,
                evidence={"cancelled_item_count": changed},
            )
            result = self._batch_in_transaction(con, batch_key)
            result["idempotent"] = False
            return result

        result = dict(write_transaction(self.state_db, txn))
        self._raise_result_error(result)
        return result

    def cancel_batch(self, batch_id: int, actor: str) -> dict[str, Any]:
        batch_key = self._positive_id(batch_id, "batch_id")
        actor_id = self._identity(actor, "actor")
        now = self._timestamp()

        def txn(con: sqlite3.Connection) -> dict[str, Any]:
            self._begin_immediate(con)
            batch = con.execute(
                "select * from bot_add_batches where id=?", (batch_key,)
            ).fetchone()
            if batch is None:
                raise ValueError("batch_not_found")
            terminal_states = sorted(_AUTOMATIC_TERMINAL_ITEM_STATES)
            placeholders = ",".join("?" for _ in terminal_states)
            guarded = con.execute(
                f"select 1 from bot_add_items where batch_id=? "
                f"and state not in ({placeholders}) "
                "and (qbt_precheck_tag is not null or qbt_hash is not null) limit 1",
                (batch_key, *terminal_states),
            ).fetchone()
            if guarded is not None:
                raise ValueError("batch_requires_guarded_cancel")
            self._expire_due_in_transaction(con, now)
            batch = con.execute(
                "select * from bot_add_batches where id=?", (batch_key,)
            ).fetchone()
            assert batch is not None
            batch = self._expire_draft_row_in_transaction(con, batch, now)
            state = str(batch["state"])
            if state == "cancelled":
                result = self._row(batch)
                result["idempotent"] = True
                return result
            if state == "draft_expired":
                return {"__error__": "batch_not_cancellable"}
            if state == "complete":
                raise ValueError("batch_not_cancellable")

            rows = list(
                con.execute(
                    "select id,state from bot_add_items where batch_id=? order by id",
                    (batch_key,),
                )
            )
            changed = 0
            for item in rows:
                old_state = str(item["state"])
                item_id = int(item["id"])
                if old_state in _CANCELLABLE_ITEM_STATES:
                    con.execute(
                        "update bot_add_items set state='cancelled',raw_input=null,"
                        "raw_input_expires_at=null,metadata_probe_deadline=null,"
                        "metadata_next_poll_at=null,metadata_retry_at=null,"
                        "metadata_lease_owner=null,metadata_lease_until=null,"
                        "metadata_lease_generation=metadata_lease_generation+1,"
                        "approval_generation=approval_generation+?,next_run_at=null,"
                        "qbt_precheck_tag=null,updated_at=? where id=? and state=?",
                        (1 if old_state == "enrolling" else 0, now, item_id, old_state),
                    )
                    changed += 1
                    self._event(
                        con,
                        batch_id=batch_key,
                        item_id=item_id,
                        event_type="state_transition",
                        from_state=old_state,
                        to_state="cancelled",
                        reason="batch_cancelled",
                        now=now,
                        actor=actor_id,
                    )
            con.execute(
                "update bot_add_items set raw_input=null,raw_input_expires_at=null "
                "where batch_id=? and raw_input is not null",
                (batch_key,),
            )
            con.execute(
                "update bot_add_shards set state='cancelled',updated_at=? "
                "where batch_id=? and state!='cancelled'",
                (now, batch_key),
            )
            con.execute(
                "update bot_add_batches set state='cancelled',completed_at=?,updated_at=? where id=?",
                (now, now, batch_key),
            )
            self._refresh_batch_counters(con, batch_key, now)
            self._event(
                con,
                batch_id=batch_key,
                item_id=None,
                event_type="batch_cancelled",
                from_state=state,
                to_state="cancelled",
                reason="cancelled_by_operator",
                now=now,
                actor=actor_id,
                evidence={"cancelled_item_count": changed},
            )
            result = self._batch_in_transaction(con, batch_key)
            result["idempotent"] = False
            return result

        result = dict(write_transaction(self.state_db, txn))
        self._raise_result_error(result)
        return result

    def claim_metadata_lease(
        self, item_id: int, owner: str, lease_until: int
    ) -> dict[str, Any]:
        """Fence a worker lease; the coordinator owns probe-attempt advancement."""
        item_key = self._positive_id(item_id, "item_id")
        lease_owner = self._identity(owner, "owner")
        now = self._timestamp()
        until = self._future_timestamp(lease_until, now, "lease_until")

        def txn(con: sqlite3.Connection) -> dict[str, Any]:
            self._begin_immediate(con)
            row = self._item_in_transaction(con, item_key)
            current_owner = row["metadata_lease_owner"]
            current_until = row["metadata_lease_until"]
            if (
                current_owner is not None
                and current_until is not None
                and int(current_until) > now
            ):
                raise ValueError("metadata_lease_active")
            if str(row["state"]) not in _METADATA_LEASE_STATES:
                raise ValueError("metadata_lease_state")
            batch_state = self._batch_state_in_transaction(con, int(row["batch_id"]))
            if batch_state not in _SUBMITTED_BATCH_STATES:
                raise ValueError("batch_not_submitted")
            if str(row["state"]) == "metadata_retry_wait" and any(
                value is not None and int(value) > now
                for value in (
                    row["metadata_retry_at"],
                    row["metadata_next_poll_at"],
                    row["next_run_at"],
                )
            ):
                raise ValueError("metadata_retry_not_due")
            if str(row["state"]) not in {"prechecking", "enrolling"}:
                required_raw_until = max(
                    until,
                    int(row["metadata_probe_deadline"] or 0),
                )
                raw_expires_at = row["raw_input_expires_at"]
                if (
                    row["raw_input"] is None
                    or raw_expires_at is None
                    or int(raw_expires_at) < required_raw_until
                ):
                    self._terminalize_metadata_operation_without_raw(
                        con,
                        row,
                        now=now,
                        required_raw_until=required_raw_until,
                    )
                    return {"__error__": "raw_input_unavailable"}
            current_generation = int(row["metadata_lease_generation"] or 0)
            next_generation = current_generation + 1
            cursor = con.execute(
                "update bot_add_items set metadata_lease_owner=?,metadata_lease_generation=?,"
                "metadata_lease_until=?,metadata_retry_at=null,metadata_next_poll_at=null,"
                "next_run_at=null,updated_at=? where id=? and state=? "
                "and metadata_lease_generation=?",
                (
                    lease_owner,
                    next_generation,
                    until,
                    now,
                    item_key,
                    str(row["state"]),
                    current_generation,
                ),
            )
            if cursor.rowcount != 1:
                raise ValueError("metadata_lease_conflict")
            self._event(
                con,
                batch_id=int(row["batch_id"]),
                item_id=item_key,
                event_type="metadata_lease_claimed",
                from_state=str(row["state"]),
                to_state=str(row["state"]),
                reason="metadata_lease_claimed",
                now=now,
                evidence={"generation": next_generation},
            )
            return self._safe_item_in_transaction(con, item_key)

        result = dict(write_transaction(self.state_db, txn))
        self._raise_result_error(result)
        return result

    def renew_metadata_lease(
        self,
        item_id: int,
        owner: str,
        generation: int,
        lease_until: int,
    ) -> dict[str, Any]:
        item_key = self._positive_id(item_id, "item_id")
        lease_owner = self._identity(owner, "owner")
        token_generation = self._positive_id(generation, "metadata_lease_generation")
        now = self._timestamp()
        until = self._future_timestamp(lease_until, now, "lease_until")

        def txn(con: sqlite3.Connection) -> dict[str, Any]:
            self._begin_immediate(con)
            row = self._item_in_transaction(con, item_key)
            if (
                str(row["metadata_lease_owner"] or "") != lease_owner
                or int(row["metadata_lease_generation"] or 0) != token_generation
                or row["metadata_lease_until"] is None
                or int(row["metadata_lease_until"]) <= now
            ):
                raise ValueError("metadata_lease_conflict")
            if str(row["state"]) not in {"prechecking", "enrolling"}:
                required_raw_until = max(
                    until,
                    int(row["metadata_probe_deadline"] or 0),
                )
                raw_expires_at = row["raw_input_expires_at"]
                if (
                    row["raw_input"] is None
                    or raw_expires_at is None
                    or int(raw_expires_at) < required_raw_until
                ):
                    self._terminalize_metadata_operation_without_raw(
                        con,
                        row,
                        now=now,
                        required_raw_until=required_raw_until,
                    )
                    return {"__error__": "raw_input_unavailable"}
            cursor = con.execute(
                "update bot_add_items set metadata_lease_until=?,updated_at=? "
                "where id=? and metadata_lease_owner=? and metadata_lease_generation=? "
                "and metadata_lease_until>?",
                (until, now, item_key, lease_owner, token_generation, now),
            )
            if cursor.rowcount != 1:
                raise ValueError("metadata_lease_conflict")
            self._event(
                con,
                batch_id=int(row["batch_id"]),
                item_id=item_key,
                event_type="metadata_lease_renewed",
                from_state=str(row["state"]),
                to_state=str(row["state"]),
                reason="metadata_lease_renewed",
                now=now,
                evidence={"generation": token_generation},
            )
            return self._safe_item_in_transaction(con, item_key)

        result = dict(write_transaction(self.state_db, txn))
        self._raise_result_error(result)
        return result

    def release_metadata_lease(
        self, item_id: int, owner: str, generation: int
    ) -> dict[str, Any]:
        return self._finish_metadata_lease(
            item_id,
            owner,
            generation,
            fence=False,
        )

    def update_metadata_probe(
        self,
        item_id: int,
        owner: str,
        generation: int,
        fields: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Update probe progress while the caller still owns its lease."""
        item_key = self._positive_id(item_id, "item_id")
        lease_owner = self._identity(owner, "owner")
        token_generation = self._positive_id(generation, "metadata_lease_generation")
        proposed = dict(fields)
        if not proposed or not set(proposed) <= _METADATA_PROGRESS_FIELDS:
            raise ValueError("metadata_progress_fields")
        now = self._timestamp()

        def txn(con: sqlite3.Connection) -> dict[str, Any]:
            self._begin_immediate(con)
            row = self._item_in_transaction(con, item_key)
            if (
                str(row["metadata_lease_owner"] or "") != lease_owner
                or int(row["metadata_lease_generation"] or 0) != token_generation
                or row["metadata_lease_until"] is None
                or int(row["metadata_lease_until"]) <= now
            ):
                raise ValueError("metadata_lease_conflict")
            ordered = sorted(proposed)
            assignments = ",".join(f"{field}=?" for field in ordered)
            cursor = con.execute(
                f"update bot_add_items set {assignments},updated_at=? "
                "where id=? and metadata_lease_owner=? "
                "and metadata_lease_generation=? and metadata_lease_until>?",
                (
                    *(proposed[field] for field in ordered),
                    now,
                    item_key,
                    lease_owner,
                    token_generation,
                    now,
                ),
            )
            if cursor.rowcount != 1:
                raise ValueError("metadata_lease_conflict")
            return self._safe_item_in_transaction(con, item_key)

        return dict(write_transaction(self.state_db, txn))

    def fence_metadata_lease(
        self, item_id: int, owner: str, generation: int
    ) -> dict[str, Any]:
        return self._finish_metadata_lease(
            item_id,
            owner,
            generation,
            fence=True,
        )

    def _finish_metadata_lease(
        self,
        item_id: int,
        owner: str,
        generation: int,
        *,
        fence: bool,
    ) -> dict[str, Any]:
        item_key = self._positive_id(item_id, "item_id")
        lease_owner = self._identity(owner, "owner")
        token_generation = self._positive_id(generation, "metadata_lease_generation")
        now = self._timestamp()

        def txn(con: sqlite3.Connection) -> dict[str, Any]:
            self._begin_immediate(con)
            row = self._item_in_transaction(con, item_key)
            row = self._expire_item_raw_in_transaction(con, row, now)
            next_generation = token_generation + 1 if fence else token_generation
            cursor = con.execute(
                "update bot_add_items set metadata_lease_owner=null,"
                "metadata_lease_generation=?,metadata_lease_until=null,updated_at=? "
                "where id=? and metadata_lease_owner=? and metadata_lease_generation=?",
                (
                    next_generation,
                    now,
                    item_key,
                    lease_owner,
                    token_generation,
                ),
            )
            if cursor.rowcount != 1:
                raise ValueError("metadata_lease_conflict")
            event_type = "metadata_lease_fenced" if fence else "metadata_lease_released"
            self._event(
                con,
                batch_id=int(row["batch_id"]),
                item_id=item_key,
                event_type=event_type,
                from_state=str(row["state"]),
                to_state=str(row["state"]),
                reason=event_type,
                now=now,
                evidence={"generation": next_generation},
            )
            return self._safe_item_in_transaction(con, item_key)

        return dict(write_transaction(self.state_db, txn))

    def transition_item(
        self,
        item_id: int,
        expected: set[str],
        new_state: str,
        reason: str,
        fields: Mapping[str, Any] | None = None,
        *,
        metadata_lease_owner: str | None = None,
        metadata_lease_generation: int | None = None,
        approval_generation: int | None = None,
        metadata_action: str | None = None,
    ) -> dict[str, Any]:
        item_key = self._positive_id(item_id, "item_id")
        if not isinstance(expected, (set, frozenset)) or not expected:
            raise ValueError("expected_states")
        expected_states = {str(value) for value in expected}
        if not expected_states <= _ITEM_STATES:
            raise ValueError("expected_states")
        target_state = str(new_state)
        if target_state not in _ITEM_STATES:
            raise ValueError("new_state")
        reason_code = self._reason(reason)
        proposed_fields = dict(fields or {})
        if not set(proposed_fields) <= _FIELD_ALLOWLIST:
            raise ValueError("invalid_field")
        if (metadata_lease_owner is None) != (metadata_lease_generation is None):
            raise ValueError("metadata_lease_token")
        expected_lease_owner = (
            None
            if metadata_lease_owner is None
            else self._identity(metadata_lease_owner, "metadata_lease_owner")
        )
        expected_lease_generation = (
            None
            if metadata_lease_generation is None
            else self._positive_id(
                metadata_lease_generation, "metadata_lease_generation"
            )
        )
        expected_approval_generation = (
            None
            if approval_generation is None
            else self._positive_id(approval_generation, "approval_generation")
        )
        expected_metadata_action = None
        if metadata_action is not None:
            if (
                not isinstance(metadata_action, str)
                or metadata_action not in _METADATA_CALLBACK_TARGETS
            ):
                raise ValueError("metadata_action")
            expected_metadata_action = metadata_action
        now = self._timestamp()

        def txn(con: sqlite3.Connection) -> dict[str, Any]:
            self._begin_immediate(con)
            row = self._item_in_transaction(con, item_key)
            old_state = str(row["state"])
            if old_state not in expected_states:
                raise ValueError("state_conflict")
            batch_id = int(row["batch_id"])
            if target_state not in _ALLOWED_TRANSITIONS.get(old_state, frozenset()):
                raise ValueError("illegal_transition")
            batch_state = self._batch_state_in_transaction(con, batch_id)

            manual_retry = (
                old_state == "metadata_unavailable"
                and target_state in _MANUAL_RETRY_TARGET_STATES
            )
            metadata_callback = (
                old_state == "metadata_unavailable"
                and target_state in set(_METADATA_CALLBACK_TARGETS.values())
            )
            required_metadata_action = next(
                (
                    action
                    for action, action_target in _METADATA_CALLBACK_TARGETS.items()
                    if action_target == target_state
                ),
                None,
            )
            if metadata_callback and expected_metadata_action is None:
                raise ValueError("metadata_action_required")
            if metadata_callback and expected_metadata_action != required_metadata_action:
                raise ValueError("metadata_action_mismatch")
            if not metadata_callback and expected_metadata_action is not None:
                raise ValueError("metadata_action_unexpected")
            metadata_cancel = metadata_callback and target_state == "cancelled"
            held_release = old_state == "enrolled_hold" and target_state == "enrolled"
            cancelled_held_release = batch_state == "cancelled" and held_release
            if batch_state == "draft":
                raise ValueError("batch_not_submitted")
            if batch_state == "draft_expired" or (
                batch_state == "cancelled" and not cancelled_held_release
            ):
                raise ValueError("batch_not_active")
            if batch_state == "complete" and not (
                manual_retry or held_release or metadata_cancel
            ):
                raise ValueError("batch_not_active")
            if (
                batch_state not in _SUBMITTED_BATCH_STATES | {"complete"}
                and not cancelled_held_release
            ):
                raise ValueError("batch_not_active")

            row = self._expire_item_raw_in_transaction(con, row, now)
            active_lease = (
                row["metadata_lease_owner"] is not None
                and row["metadata_lease_until"] is not None
                and int(row["metadata_lease_until"]) > now
            )
            if active_lease and expected_lease_owner is None:
                raise ValueError("metadata_lease_token_required")
            if expected_lease_owner is not None and (
                str(row["metadata_lease_owner"] or "") != expected_lease_owner
                or int(row["metadata_lease_generation"] or 0)
                != expected_lease_generation
                or row["metadata_lease_until"] is None
                or int(row["metadata_lease_until"]) <= now
            ):
                raise ValueError("metadata_lease_conflict")

            approval_required = (
                (old_state == "enrolling" and target_state in {"enrolled", "enrolled_hold", "failed"})
                or (old_state == "needs_confirmation" and target_state in {"enrolling", "cancelled"})
                or held_release
                or metadata_callback
            )
            if approval_required and expected_approval_generation is None:
                raise ValueError("approval_token_required")
            if approval_required and int(row["approval_generation"] or 0) != int(
                expected_approval_generation
            ):
                raise ValueError("approval_generation_conflict")
            if not approval_required and expected_approval_generation is not None:
                raise ValueError("approval_token_unexpected")

            if manual_retry:
                raw_input = row["raw_input"]
                raw_expires_at = row["raw_input_expires_at"]
                if (
                    raw_input is None
                    or raw_expires_at is None
                    or int(raw_expires_at) <= now
                ):
                    return {"__error__": "raw_input_expired"}
                retry_approval_deadline = (
                    now
                    + _METADATA_RETRY_DELAY_SEC
                    + _METADATA_APPROVAL_WINDOW_SEC
                )
                if (
                    expected_metadata_action == "retry_24h"
                    and int(raw_expires_at) < retry_approval_deadline
                ):
                    raise ValueError("raw_input_ttl_insufficient")
                if (
                    self._submitted_nonterminal_in_transaction(con) + 1
                    > self.limits.max_submitted_items
                ):
                    raise ValueError("global_backlog_limit")
                if batch_state == "complete":
                    con.execute(
                        "update bot_add_batches set state='processing',completed_at=null,"
                        "updated_at=? where id=? and state='complete'",
                        (now, batch_id),
                    )

            assignments: dict[str, Any] = dict(proposed_fields)
            if target_state in {"enrolling", "metadata_unavailable", "needs_confirmation"}:
                assignments["approval_generation"] = int(
                    row["approval_generation"] or 0
                ) + 1
            elif old_state == "enrolling" and target_state == "cancelled":
                assignments["approval_generation"] = int(
                    row["approval_generation"] or 0
                ) + 1
            elif old_state == "needs_confirmation" and target_state == "cancelled":
                assignments["approval_generation"] = int(
                    row["approval_generation"] or 0
                ) + 1
            elif metadata_callback:
                assignments["approval_generation"] = int(
                    row["approval_generation"] or 0
                ) + 1
            if expected_metadata_action in {"retry_now", "retry_24h"}:
                assignments["metadata_probe_started_at"] = None
                assignments["metadata_probe_deadline"] = None
                assignments["metadata_lease_owner"] = None
                assignments["metadata_lease_until"] = None
            if expected_metadata_action == "retry_24h":
                retry_at = now + _METADATA_RETRY_DELAY_SEC
                assignments["metadata_probe_attempt"] = (
                    _METADATA_RETRY_24H_STORED_ATTEMPT
                )
                assignments["metadata_retry_at"] = retry_at
                assignments["metadata_next_poll_at"] = retry_at
                assignments["next_run_at"] = retry_at
            elif expected_metadata_action == "retry_now":
                assignments["metadata_probe_attempt"] = _METADATA_RETRY_NOW_ATTEMPT
                assignments["metadata_retry_at"] = now
                assignments["metadata_next_poll_at"] = None
                assignments["next_run_at"] = now
            if target_state in _AUTOMATIC_TERMINAL_ITEM_STATES:
                for field in _AUTOMATIC_TERMINAL_CLEAR_FIELDS:
                    assignments[field] = None
                assignments["attempts"] = 0
                if target_state not in {"enrolled", "enrolled_hold"}:
                    assignments["qbt_precheck_tag"] = None
                if target_state in _RAW_CLEAR_ITEM_STATES:
                    for field in _RAW_INPUT_FIELDS:
                        assignments[field] = None
                elif target_state == "metadata_unavailable":
                    raw_expires_at = row["raw_input_expires_at"]
                    if raw_expires_at is None or int(raw_expires_at) <= now:
                        for field in _RAW_INPUT_FIELDS:
                            assignments[field] = None
            if (
                row["metadata_lease_owner"] is not None
                and target_state not in _METADATA_LEASE_STATES
            ):
                assignments["metadata_lease_owner"] = None
                assignments["metadata_lease_until"] = None
                assignments["metadata_lease_generation"] = int(
                    row["metadata_lease_generation"] or 0
                ) + 1
            assignments["state"] = target_state
            assignments["updated_at"] = now
            ordered = sorted(assignments)
            sql = ",".join(f"{field}=?" for field in ordered)
            params = [assignments[field] for field in ordered]
            where = "id=? and state=?"
            where_params: list[Any] = [item_key, old_state]
            if expected_lease_owner is not None:
                where += " and metadata_lease_owner=? and metadata_lease_generation=?"
                where_params.extend(
                    [expected_lease_owner, expected_lease_generation]
                )
            if approval_required:
                where += " and approval_generation=?"
                where_params.append(expected_approval_generation)
            cursor = con.execute(
                f"update bot_add_items set {sql} where {where}",
                (*params, *where_params),
            )
            if cursor.rowcount != 1:
                raise ValueError("state_conflict")
            self._event(
                con,
                batch_id=batch_id,
                item_id=item_key,
                event_type="state_transition",
                from_state=old_state,
                to_state=target_state,
                reason=reason_code,
                now=now,
                evidence={"updated_fields": sorted(proposed_fields)},
            )
            self._refresh_batch_counters(con, batch_id, now)
            self._refresh_shards_and_batch_state(con, batch_id, now)
            return self._item_for_output(
                con.execute("select * from bot_add_items where id=?", (item_key,)).fetchone(),
                include_raw=False,
            )

        result = dict(write_transaction(self.state_db, txn))
        self._raise_result_error(result)
        return result

    def expire_drafts(self, *, limit: int = 10) -> int:
        maintenance_limit = self._maintenance_limit(limit)
        now = self._timestamp()

        def txn(con: sqlite3.Connection) -> int:
            self._begin_immediate(con)
            return self._expire_drafts_in_transaction(con, now, maintenance_limit)

        return int(write_transaction(self.state_db, txn))

    def expire_raw_inputs(self, *, limit: int = 100) -> dict[str, Any]:
        maintenance_limit = self._maintenance_limit(limit)
        now = self._timestamp()

        def txn(con: sqlite3.Connection) -> dict[str, Any]:
            self._begin_immediate(con)
            self._expire_drafts_in_transaction(con, now, min(10, maintenance_limit))
            return self._expire_raw_inputs_in_transaction(con, now, maintenance_limit)

        return dict(write_transaction(self.state_db, txn))

    def finalize_enrollment_marker(
        self, item_id: int, state: str, qbt_precheck_tag: str
    ) -> dict[str, Any]:
        item_key = self._positive_id(item_id, "item_id")
        expected_state = str(state)
        if expected_state not in {"enrolled", "enrolled_hold"}:
            raise ValueError("enrollment_state")
        tag = str(qbt_precheck_tag or "").strip()
        if not tag:
            raise ValueError("qbt_precheck_tag")
        now = self._timestamp()

        def txn(con: sqlite3.Connection) -> dict[str, Any]:
            self._begin_immediate(con)
            row = self._item_in_transaction(con, item_key)
            if str(row["state"]) != expected_state:
                raise ValueError("state_conflict")
            if row["qbt_precheck_tag"] is None:
                return self._safe_item_in_transaction(con, item_key)
            cursor = con.execute(
                "update bot_add_items set qbt_precheck_tag=null,updated_at=? "
                "where id=? and state=? and qbt_precheck_tag=?",
                (now, item_key, expected_state, tag),
            )
            if cursor.rowcount != 1:
                raise ValueError("enrollment_marker_conflict")
            return self._safe_item_in_transaction(con, item_key)

        return dict(write_transaction(self.state_db, txn))

    def record_enrollment_retry(
        self,
        item_id: int,
        *,
        metadata_lease_owner: str,
        metadata_lease_generation: int,
        error_code: str,
        next_run_at: int,
    ) -> dict[str, Any]:
        item_key = self._positive_id(item_id, "item_id")
        owner = self._identity(metadata_lease_owner, "owner")
        generation = int(metadata_lease_generation)
        if generation <= 0:
            raise ValueError("metadata_lease_generation")
        code = str(error_code or "").strip()
        if not _SAFE_CODE.fullmatch(code):
            raise ValueError("last_error")
        now = self._timestamp()
        due_at = self._future_timestamp(next_run_at, now, "next_run_at")

        def txn(con: sqlite3.Connection) -> dict[str, Any]:
            self._begin_immediate(con)
            row = self._item_in_transaction(con, item_key)
            if str(row["state"]) != "enrolling":
                raise ValueError("state_conflict")
            if (
                str(row["metadata_lease_owner"] or "") != owner
                or int(row["metadata_lease_generation"] or 0) != generation
            ):
                raise ValueError("metadata_lease_conflict")
            attempts = int(row["attempts"] or 0) + 1
            cursor = con.execute(
                "update bot_add_items set attempts=?, last_error=?, next_run_at=?, "
                "updated_at=? where id=? and state=? and metadata_lease_owner=? "
                "and metadata_lease_generation=?",
                (
                    attempts,
                    code,
                    due_at,
                    now,
                    item_key,
                    "enrolling",
                    owner,
                    generation,
                ),
            )
            if cursor.rowcount != 1:
                raise ValueError("state_conflict")
            return self._safe_item_in_transaction(con, item_key)

        return dict(write_transaction(self.state_db, txn))

    def list_qbt_precheck_tag_refs(self) -> set[str]:
        con = readonly_connect(self.state_db)
        try:
            rows = con.execute(
                "select distinct qbt_precheck_tag from bot_add_items "
                "where qbt_precheck_tag is not null and qbt_precheck_tag <> ''"
            )
            return {str(row[0]).strip() for row in rows if str(row[0] or "").strip()}
        finally:
            con.close()

    def get_batch(self, batch_id: int) -> dict[str, Any]:
        key = self._positive_id(batch_id, "batch_id")
        con = readonly_connect(self.state_db)
        try:
            row = con.execute("select * from bot_add_batches where id=?", (key,)).fetchone()
            if row is None:
                raise ValueError("batch_not_found")
            return self._row(row)
        finally:
            con.close()

    def list_batches(self, *, state: str | None = None) -> list[dict[str, Any]]:
        con = readonly_connect(self.state_db)
        try:
            if state is None:
                rows = con.execute("select * from bot_add_batches order by id")
            else:
                rows = con.execute(
                    "select * from bot_add_batches where state=? order by id", (str(state),)
                )
            return [self._row(row) for row in rows]
        finally:
            con.close()

    def get_item(self, item_id: int, *, include_raw: bool = False) -> dict[str, Any]:
        key = self._positive_id(item_id, "item_id")
        con = readonly_connect(self.state_db)
        try:
            row = con.execute("select * from bot_add_items where id=?", (key,)).fetchone()
            if row is None:
                raise ValueError("item_not_found")
            return self._item_for_output(row, include_raw=include_raw)
        finally:
            con.close()

    def list_items(
        self, batch_id: int, *, include_raw: bool = False
    ) -> list[dict[str, Any]]:
        key = self._positive_id(batch_id, "batch_id")
        con = readonly_connect(self.state_db)
        try:
            rows = list(
                con.execute(
                    "select * from bot_add_items where batch_id=? order by id", (key,)
                )
            )
            shard_rows = list(
                con.execute(
                    "select shard_index,item_count from bot_add_shards "
                    "where batch_id=? order by shard_index,id",
                    (key,),
                )
            )
            shard_indexes: list[int | None] = [None] * len(rows)
            offset = 0
            for shard in shard_rows:
                end = offset + int(shard["item_count"])
                if end > len(rows):
                    raise ValueError("shard_layout_invalid")
                shard_indexes[offset:end] = [int(shard["shard_index"])] * (end - offset)
                offset = end
            if shard_rows and offset != len(rows):
                raise ValueError("shard_layout_invalid")
            result = []
            for index, row in enumerate(rows):
                item = self._item_for_output(row, include_raw=include_raw)
                item["shard_index"] = shard_indexes[index]
                result.append(item)
            return result
        finally:
            con.close()

    def list_shards(self, batch_id: int) -> list[dict[str, Any]]:
        key = self._positive_id(batch_id, "batch_id")
        con = readonly_connect(self.state_db)
        try:
            return [
                self._row(row)
                for row in con.execute(
                    "select * from bot_add_shards where batch_id=? order by shard_index,id",
                    (key,),
                )
            ]
        finally:
            con.close()

    def list_events(self, batch_id: int) -> list[dict[str, Any]]:
        key = self._positive_id(batch_id, "batch_id")
        con = readonly_connect(self.state_db)
        try:
            return [
                self._row(row)
                for row in con.execute(
                    "select * from bot_add_events where batch_id=? order by id", (key,)
                )
            ]
        finally:
            con.close()

    def submitted_nonterminal_count(self) -> int:
        con = readonly_connect(self.state_db)
        try:
            return self._submitted_nonterminal_in_transaction(con)
        finally:
            con.close()

    def _validate_links(self, links: list[str]) -> list[dict[str, Any]]:
        if not isinstance(links, list) or not links:
            raise ValueError("empty_message")
        if len(links) > self.limits.max_links_per_batch:
            raise ValueError("batch_link_limit")
        result: list[dict[str, Any]] = []
        hashes: set[str] = set()
        for raw in links:
            if not isinstance(raw, str):
                raise ValueError("link_type")
            value = raw.strip()
            if not value:
                raise ValueError("empty_link")
            try:
                byte_length = len(value.encode("utf-8"))
            except UnicodeEncodeError as exc:
                raise ValueError("link_encoding") from exc
            if byte_length > self.limits.max_link_bytes:
                raise ValueError("link_byte_limit")
            kind = self._classify(value)
            digest = hashlib.sha256(value.encode("utf-8")).hexdigest()
            if digest in hashes:
                raise ValueError("duplicate_input")
            hashes.add(digest)
            result.append(
                {
                    "raw_input": value,
                    "byte_length": byte_length,
                    "input_kind": kind,
                    "input_sha256": digest,
                    "redacted_input": self._redact_input(value, kind),
                }
            )
        if sum(int(link["byte_length"]) for link in result) > self.limits.max_draft_bytes:
            raise ValueError("draft_byte_limit")
        return result

    @staticmethod
    def _classify(value: str) -> str:
        lowered = value.lower()
        if lowered.startswith("magnet:"):
            return "magnet"
        if lowered.startswith("https://"):
            return "https_url"
        if lowered.startswith("http://"):
            return "http_url"
        if lowered.startswith("bc://"):
            return "bc_link"
        raise ValueError("unsupported_link_scheme")

    @staticmethod
    def _redact_input(value: str, kind: str) -> str:
        if kind == "magnet":
            return "magnet:[redacted]"
        if kind == "bc_link":
            return "bc:[redacted]"
        try:
            parsed = urlsplit(value)
            host = parsed.hostname or ""
            if ":" in host and not host.startswith("["):
                host = f"[{host}]"
            return f"{parsed.scheme.lower()}://{host}/[redacted]"
        except (TypeError, ValueError):
            return f"{kind.split('_', 1)[0]}:[redacted]"

    def _expire_due_in_transaction(self, con: sqlite3.Connection, now: int) -> None:
        self._expire_drafts_in_transaction(con, now, 10)
        self._expire_raw_inputs_in_transaction(con, now, 100)

    def _expire_drafts_in_transaction(
        self, con: sqlite3.Connection, now: int, limit: int
    ) -> int:
        rows = list(
            con.execute(
                "select * from bot_add_batches where state='draft' and updated_at<=? "
                "order by updated_at,id limit ?",
                (now - self.limits.draft_ttl_sec, limit),
            )
        )
        for row in rows:
            self._expire_draft_row_in_transaction(con, row, now)
        return len(rows)

    def _expire_draft_row_in_transaction(
        self, con: sqlite3.Connection, row: sqlite3.Row, now: int
    ) -> sqlite3.Row:
        if (
            str(row["state"]) != "draft"
            or int(row["updated_at"]) > now - self.limits.draft_ttl_sec
        ):
            return row
        batch_id = int(row["id"])
        con.execute(
            "update bot_add_batches set state='draft_expired',completed_at=?,updated_at=? "
            "where id=? and state='draft'",
            (now, now, batch_id),
        )
        con.execute(
            "update bot_add_items set raw_input=null,raw_input_expires_at=null,updated_at=? "
            "where batch_id=? and raw_input is not null",
            (now, batch_id),
        )
        self._event(
            con,
            batch_id=batch_id,
            item_id=None,
            event_type="draft_expired",
            from_state="draft",
            to_state="draft_expired",
            reason="draft_ttl_elapsed",
            now=now,
        )
        refreshed = con.execute(
            "select * from bot_add_batches where id=?", (batch_id,)
        ).fetchone()
        if refreshed is None:  # pragma: no cover - protected by the transaction
            raise ValueError("batch_not_found")
        return refreshed

    def _expire_raw_inputs_in_transaction(
        self, con: sqlite3.Connection, now: int, limit: int
    ) -> dict[str, Any]:
        rows = list(
            con.execute(
                "select i.id,i.batch_id,b.state as batch_state "
                "from bot_add_items i join bot_add_batches b on b.id=i.batch_id "
                "where i.raw_input is not null and i.raw_input_expires_at<=? "
                "order by i.raw_input_expires_at,i.id limit ?",
                (now, limit),
            )
        )
        draft_batches = {int(row["batch_id"]) for row in rows if row["batch_state"] == "draft"}
        for batch_id in sorted(draft_batches):
            con.execute(
                "update bot_add_batches set state='draft_expired',completed_at=?,updated_at=? "
                "where id=? and state='draft'",
                (now, now, batch_id),
            )
            con.execute(
                "update bot_add_items set raw_input=null,raw_input_expires_at=null,updated_at=? "
                "where batch_id=? and raw_input is not null",
                (now, batch_id),
            )
            self._event(
                con,
                batch_id=batch_id,
                item_id=None,
                event_type="draft_expired",
                from_state="draft",
                to_state="draft_expired",
                reason="raw_input_ttl_elapsed",
                now=now,
            )
        expired = [row for row in rows if int(row["batch_id"]) not in draft_batches]
        for row in expired:
            item_id = int(row["id"])
            batch_id = int(row["batch_id"])
            con.execute(
                "update bot_add_items set raw_input=null,raw_input_expires_at=null,updated_at=? "
                "where id=? and raw_input is not null",
                (now, item_id),
            )
            self._event(
                con,
                batch_id=batch_id,
                item_id=item_id,
                event_type="raw_input_expired",
                from_state=None,
                to_state=None,
                reason="raw_input_ttl_elapsed",
                now=now,
            )
        has_more = (
            con.execute(
                "select 1 from bot_add_items where raw_input is not null "
                "and raw_input_expires_at<=? limit 1",
                (now,),
            ).fetchone()
            is not None
        )
        return {"expired_count": len(rows), "has_more": has_more}

    def _refresh_batch_counters(
        self, con: sqlite3.Connection, batch_id: int, now: int
    ) -> None:
        counts = con.execute(
            "select count(*) as received_count,"
            "sum(case when state!='invalid' then 1 else 0 end) as valid_count,"
            "sum(case when state in ('enrolled','enrolled_hold') then 1 else 0 end) as enrolled_count,"
            "sum(case when state in ('duplicate_local','duplicate_remote') then 1 else 0 end) as duplicate_count,"
            "sum(case when state='needs_confirmation' then 1 else 0 end) as confirmation_count,"
            "sum(case when state in ('invalid','metadata_unavailable','failed') then 1 else 0 end) as failed_count,"
            "sum(case when decision='blocked_manual_deleted' then 1 else 0 end) as blocked_history_count "
            "from bot_add_items where batch_id=?",
            (batch_id,),
        ).fetchone()
        con.execute(
            "update bot_add_batches set received_count=?,valid_count=?,enrolled_count=?,"
            "duplicate_count=?,confirmation_count=?,failed_count=?,blocked_history_count=?,"
            "updated_at=? where id=?",
            (
                int(counts["received_count"] or 0),
                int(counts["valid_count"] or 0),
                int(counts["enrolled_count"] or 0),
                int(counts["duplicate_count"] or 0),
                int(counts["confirmation_count"] or 0),
                int(counts["failed_count"] or 0),
                int(counts["blocked_history_count"] or 0),
                now,
                batch_id,
            ),
        )

    def _refresh_shards_and_batch_state(
        self, con: sqlite3.Connection, batch_id: int, now: int
    ) -> None:
        batch = con.execute(
            "select state,completed_at from bot_add_batches where id=?", (batch_id,)
        ).fetchone()
        if batch is None or str(batch["state"]) not in _SUBMITTED_BATCH_STATES:
            return
        items = list(
            con.execute(
                "select state from bot_add_items where batch_id=? order by id", (batch_id,)
            )
        )
        shards = list(
            con.execute(
                "select shard_index,item_count,state,completed_at from bot_add_shards "
                "where batch_id=? order by shard_index,id",
                (batch_id,),
            )
        )
        offset = 0
        for shard in shards:
            item_count = int(shard["item_count"])
            states = [str(row["state"]) for row in items[offset : offset + item_count]]
            if len(states) != item_count:
                raise ValueError("shard_layout_invalid")
            offset += item_count
            processed = sum(state in _AUTOMATIC_TERMINAL_ITEM_STATES for state in states)
            if processed == len(states):
                shard_state = "complete"
                completed_at = (
                    int(shard["completed_at"])
                    if str(shard["state"]) == "complete"
                    and shard["completed_at"] is not None
                    else now
                )
            elif any(state != "received" for state in states):
                shard_state = "processing"
                completed_at = None
            else:
                shard_state = "queued"
                completed_at = None
            con.execute(
                "update bot_add_shards set state=?,processed_count=?,completed_at=?,updated_at=? "
                "where batch_id=? and shard_index=?",
                (
                    shard_state,
                    processed,
                    completed_at,
                    now,
                    batch_id,
                    int(shard["shard_index"]),
                ),
            )
        if shards and offset != len(items):
            raise ValueError("shard_layout_invalid")
        nonterminal = sum(
            str(row["state"]) not in _AUTOMATIC_TERMINAL_ITEM_STATES
            for row in items
        )
        if nonterminal == 0:
            state = "complete"
            completed_at = (
                int(batch["completed_at"])
                if str(batch["state"]) == "complete" and batch["completed_at"] is not None
                else now
            )
        elif any(str(row["state"]) == "needs_confirmation" for row in items):
            state = "awaiting_confirmation"
            completed_at = None
        else:
            state = "processing"
            completed_at = None
        con.execute(
            "update bot_add_batches set state=?,completed_at=?,updated_at=? where id=?",
            (state, completed_at, now, batch_id),
        )

    @staticmethod
    def _submitted_nonterminal_in_transaction(con: sqlite3.Connection) -> int:
        terminal = sorted(_AUTOMATIC_TERMINAL_ITEM_STATES)
        terminal_placeholders = ",".join("?" for _ in terminal)
        submitted = sorted(_SUBMITTED_BATCH_STATES)
        submitted_placeholders = ",".join("?" for _ in submitted)
        return int(
            con.execute(
                f"select count(*) from bot_add_items i join bot_add_batches b on b.id=i.batch_id "
                f"where b.state in ({submitted_placeholders}) and i.state not in ({terminal_placeholders})",
                (*submitted, *terminal),
            ).fetchone()[0]
        )

    @staticmethod
    def _begin_immediate(con: sqlite3.Connection) -> None:
        if not con.in_transaction:
            con.execute("begin immediate")

    @staticmethod
    def _batch_in_transaction(con: sqlite3.Connection, batch_id: int) -> dict[str, Any]:
        row = con.execute("select * from bot_add_batches where id=?", (batch_id,)).fetchone()
        if row is None:  # pragma: no cover - protected by FK/transaction invariants
            raise ValueError("batch_not_found")
        return dict(row)

    @staticmethod
    def _batch_state_in_transaction(con: sqlite3.Connection, batch_id: int) -> str:
        row = con.execute(
            "select state from bot_add_batches where id=?", (batch_id,)
        ).fetchone()
        if row is None:  # pragma: no cover - protected by FK
            raise ValueError("batch_not_found")
        return str(row["state"])

    @staticmethod
    def _item_in_transaction(con: sqlite3.Connection, item_id: int) -> sqlite3.Row:
        row = con.execute("select * from bot_add_items where id=?", (item_id,)).fetchone()
        if row is None:
            raise ValueError("item_not_found")
        return row

    def _safe_item_in_transaction(
        self, con: sqlite3.Connection, item_id: int
    ) -> dict[str, Any]:
        return self._item_for_output(
            self._item_in_transaction(con, item_id), include_raw=False
        )

    def _terminalize_metadata_operation_without_raw(
        self,
        con: sqlite3.Connection,
        row: sqlite3.Row,
        *,
        now: int,
        required_raw_until: int,
    ) -> None:
        item_id = int(row["id"])
        batch_id = int(row["batch_id"])
        old_state = str(row["state"])
        old_lease_generation = int(row["metadata_lease_generation"] or 0)
        assignments: dict[str, Any] = {
            field: None for field in _AUTOMATIC_TERMINAL_CLEAR_FIELDS
        }
        assignments.update(
            {
                "approval_generation": int(row["approval_generation"] or 0) + 1,
                "last_error": "raw_input_unavailable",
                "metadata_lease_generation": old_lease_generation + 1,
                "metadata_probe_started_at": None,
                "raw_input": None,
                "raw_input_expires_at": None,
                "state": "metadata_unavailable",
                "updated_at": now,
            }
        )
        ordered = sorted(assignments)
        cursor = con.execute(
            f"update bot_add_items set {','.join(f'{field}=?' for field in ordered)} "
            "where id=? and state=? and metadata_lease_generation=?",
            (
                *(assignments[field] for field in ordered),
                item_id,
                old_state,
                old_lease_generation,
            ),
        )
        if cursor.rowcount != 1:
            raise ValueError("metadata_lease_conflict")
        raw_expires_at = row["raw_input_expires_at"]
        if (
            row["raw_input"] is not None
            and raw_expires_at is not None
            and int(raw_expires_at) <= now
        ):
            self._event(
                con,
                batch_id=batch_id,
                item_id=item_id,
                event_type="raw_input_expired",
                from_state=old_state,
                to_state=old_state,
                reason="raw_input_ttl_elapsed",
                now=now,
            )
        self._event(
            con,
            batch_id=batch_id,
            item_id=item_id,
            event_type="state_transition",
            from_state=old_state,
            to_state="metadata_unavailable",
            reason="raw_input_unavailable",
            now=now,
            evidence={"required_raw_until": required_raw_until},
        )
        self._refresh_batch_counters(con, batch_id, now)
        self._refresh_shards_and_batch_state(con, batch_id, now)

    def _expire_item_raw_in_transaction(
        self, con: sqlite3.Connection, row: sqlite3.Row, now: int
    ) -> sqlite3.Row:
        expires_at = row["raw_input_expires_at"]
        if (
            row["raw_input"] is None
            or expires_at is None
            or int(expires_at) > now
        ):
            return row
        item_id = int(row["id"])
        batch_id = int(row["batch_id"])
        con.execute(
            "update bot_add_items set raw_input=null,raw_input_expires_at=null,updated_at=? "
            "where id=? and raw_input is not null and raw_input_expires_at<=?",
            (now, item_id, now),
        )
        self._event(
            con,
            batch_id=batch_id,
            item_id=item_id,
            event_type="raw_input_expired",
            from_state=str(row["state"]),
            to_state=str(row["state"]),
            reason="raw_input_ttl_elapsed",
            now=now,
        )
        return self._item_in_transaction(con, item_id)

    @staticmethod
    def _event(
        con: sqlite3.Connection,
        *,
        batch_id: int,
        item_id: int | None,
        event_type: str,
        from_state: str | None,
        to_state: str | None,
        reason: str,
        now: int,
        actor: str | None = None,
        evidence: Mapping[str, Any] | None = None,
    ) -> None:
        con.execute(
            "insert into bot_add_events("
            "batch_id,item_id,event_type,from_state,to_state,actor_user_id,actor_role,"
            "reason_code,safe_evidence_json,created_at"
            ") values(?,?,?,?,?,?,?,?,?,?)",
            (
                batch_id,
                item_id,
                event_type,
                from_state,
                to_state,
                actor,
                "operator" if actor is not None else "system",
                reason,
                json.dumps(dict(evidence or {}), ensure_ascii=False, separators=(",", ":")),
                now,
            ),
        )

    @staticmethod
    def _row(row: sqlite3.Row) -> dict[str, Any]:
        return dict(row)

    @staticmethod
    def _item_for_output(row: sqlite3.Row, *, include_raw: bool) -> dict[str, Any]:
        result = dict(row)
        if not include_raw:
            result["raw_input"] = None
        return result

    def _timestamp(self) -> int:
        value = self._now()
        if isinstance(value, bool):
            raise ValueError("now")
        return int(value)

    @staticmethod
    def _future_timestamp(value: int, now: int, field: str) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value <= now:
            raise ValueError(field)
        return value

    @staticmethod
    def _maintenance_limit(value: int) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 1000:
            raise ValueError("limit")
        return value

    @staticmethod
    def _raise_result_error(result: dict[str, Any]) -> None:
        error = result.pop("__error__", None)
        if error is not None:
            raise ValueError(str(error))

    @staticmethod
    def _identity(value: str, field: str) -> str:
        if not isinstance(value, str) or not value or len(value) > 256:
            raise ValueError(field)
        return value

    @staticmethod
    def _positive_id(value: int, field: str) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(field)
        return value

    @staticmethod
    def _nonnegative_id(value: int, field: str) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(field)
        return value

    @staticmethod
    def _reason(value: str) -> str:
        if not isinstance(value, str) or not _SAFE_CODE.fullmatch(value):
            raise ValueError("reason")
        return value


__all__ = ["AddQueueLimits", "BotAddQueueRepository"]
