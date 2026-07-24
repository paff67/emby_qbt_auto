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
_TERMINAL_ITEM_STATES = frozenset(
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
_SUBMITTED_BATCH_STATES = frozenset(
    {"queued", "processing", "awaiting_confirmation"}
)
_IDEMPOTENT_SUBMIT_STATES = _SUBMITTED_BATCH_STATES | {"complete"}
_CANCELLABLE_ITEM_STATES = _ITEM_STATES - _TERMINAL_ITEM_STATES - {"enrolling"}
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
        "approval_generation",
        "metadata_probe_attempt",
        "metadata_probe_started_at",
        "metadata_probe_deadline",
        "metadata_next_poll_at",
        "metadata_retry_at",
        "metadata_lease_owner",
        "metadata_lease_generation",
        "metadata_lease_until",
        "approved_by",
        "approved_at",
        "attempts",
        "next_run_at",
        "last_error",
    }
)
_TERMINAL_CLEAR_FIELDS = frozenset(
    {
        "raw_input",
        "raw_input_expires_at",
        "metadata_probe_deadline",
        "metadata_next_poll_at",
        "metadata_retry_at",
        "metadata_lease_owner",
        "metadata_lease_until",
        "next_run_at",
        "qbt_precheck_tag",
    }
)
_SAFE_CODE = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")


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
            state = str(batch["state"])
            if state == "draft_expired":
                return {"__error__": "draft_expired"}

            existing_message = list(
                con.execute(
                    "select i.batch_id,i.source_index,i.input_sha256 "
                    "from bot_add_items i join bot_add_batches b on b.id=i.batch_id "
                    "where b.chat_id=? and i.source_message_id=? "
                    "order by i.batch_id,i.source_index",
                    (str(batch["chat_id"]), source_message_id),
                )
            )
            expected_signature = [
                (index, str(link["input_sha256"]))
                for index, link in enumerate(proposed)
            ]
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
            placeholders = ",".join("?" for _ in input_hashes)
            if con.execute(
                f"select 1 from bot_add_items where batch_id=? and input_sha256 in ({placeholders}) limit 1",
                (batch_key, *input_hashes),
            ).fetchone():
                raise ValueError("duplicate_input")

            expires_at = now + self.limits.raw_input_ttl_sec
            for source_index, link in enumerate(proposed):
                con.execute(
                    "insert into bot_add_items("
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
            state = str(row["state"])
            if state in _IDEMPOTENT_SUBMIT_STATES:
                result = self._row(row)
                result["idempotent"] = True
                return result
            if state == "draft_expired":
                return {"__error__": "draft_expired"}
            if state != "draft":
                raise ValueError("batch_not_draft")

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
            con.execute(
                "update bot_add_batches set state='queued',submitted_at=?,updated_at=? where id=?",
                (now, now, batch_key),
            )
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

    def cancel_batch(self, batch_id: int, actor: str) -> dict[str, Any]:
        batch_key = self._positive_id(batch_id, "batch_id")
        actor_id = self._identity(actor, "actor")
        now = self._timestamp()

        def txn(con: sqlite3.Connection) -> dict[str, Any]:
            self._begin_immediate(con)
            self._expire_due_in_transaction(con, now)
            batch = con.execute(
                "select * from bot_add_batches where id=?", (batch_key,)
            ).fetchone()
            if batch is None:
                raise ValueError("batch_not_found")
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
                        "metadata_lease_owner=null,metadata_lease_until=null,next_run_at=null,"
                        "qbt_precheck_tag=null,updated_at=? where id=? and state=?",
                        (now, item_id, old_state),
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

    def transition_item(
        self,
        item_id: int,
        expected: set[str],
        new_state: str,
        reason: str,
        fields: Mapping[str, Any] | None = None,
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
        now = self._timestamp()

        def txn(con: sqlite3.Connection) -> dict[str, Any]:
            self._begin_immediate(con)
            row = con.execute(
                "select * from bot_add_items where id=?", (item_key,)
            ).fetchone()
            if row is None:
                raise ValueError("item_not_found")
            old_state = str(row["state"])
            if old_state not in expected_states:
                raise ValueError("state_conflict")
            batch_id = int(row["batch_id"])

            assignments: dict[str, Any] = dict(proposed_fields)
            if target_state in _TERMINAL_ITEM_STATES:
                for field in _TERMINAL_CLEAR_FIELDS:
                    assignments[field] = None
            assignments["state"] = target_state
            assignments["updated_at"] = now
            ordered = sorted(assignments)
            sql = ",".join(f"{field}=?" for field in ordered)
            params = [assignments[field] for field in ordered]
            cursor = con.execute(
                f"update bot_add_items set {sql} where id=? and state=?",
                (*params, item_key, old_state),
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

        return dict(write_transaction(self.state_db, txn))

    def expire_drafts(self) -> int:
        now = self._timestamp()

        def txn(con: sqlite3.Connection) -> int:
            self._begin_immediate(con)
            return self._expire_drafts_in_transaction(con, now)

        return int(write_transaction(self.state_db, txn))

    def expire_raw_inputs(self) -> int:
        now = self._timestamp()

        def txn(con: sqlite3.Connection) -> int:
            self._begin_immediate(con)
            self._expire_drafts_in_transaction(con, now)
            return self._expire_raw_inputs_in_transaction(con, now)

        return int(write_transaction(self.state_db, txn))

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
        self._expire_drafts_in_transaction(con, now)
        self._expire_raw_inputs_in_transaction(con, now)

    def _expire_drafts_in_transaction(self, con: sqlite3.Connection, now: int) -> int:
        rows = list(
            con.execute(
                "select id from bot_add_batches where state='draft' and updated_at<=? order by id",
                (now - self.limits.draft_ttl_sec,),
            )
        )
        for row in rows:
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
        return len(rows)

    def _expire_raw_inputs_in_transaction(self, con: sqlite3.Connection, now: int) -> int:
        rows = list(
            con.execute(
                "select i.id,i.batch_id,b.state as batch_state "
                "from bot_add_items i join bot_add_batches b on b.id=i.batch_id "
                "where i.raw_input is not null and i.raw_input_expires_at<=? order by i.id",
                (now,),
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
        return len(rows)

    def _refresh_batch_counters(
        self, con: sqlite3.Connection, batch_id: int, now: int
    ) -> None:
        counts = con.execute(
            "select count(*) as received_count,"
            "sum(case when state!='invalid' then 1 else 0 end) as valid_count,"
            "sum(case when state in ('enrolled','enrolled_hold') then 1 else 0 end) as enrolled_count,"
            "sum(case when state in ('duplicate_local','duplicate_remote') then 1 else 0 end) as duplicate_count,"
            "sum(case when state='needs_confirmation' then 1 else 0 end) as confirmation_count,"
            "sum(case when state in ('invalid','metadata_unavailable','failed') then 1 else 0 end) as failed_count "
            "from bot_add_items where batch_id=?",
            (batch_id,),
        ).fetchone()
        con.execute(
            "update bot_add_batches set received_count=?,valid_count=?,enrolled_count=?,"
            "duplicate_count=?,confirmation_count=?,failed_count=?,updated_at=? where id=?",
            (
                int(counts["received_count"] or 0),
                int(counts["valid_count"] or 0),
                int(counts["enrolled_count"] or 0),
                int(counts["duplicate_count"] or 0),
                int(counts["confirmation_count"] or 0),
                int(counts["failed_count"] or 0),
                now,
                batch_id,
            ),
        )

    def _refresh_shards_and_batch_state(
        self, con: sqlite3.Connection, batch_id: int, now: int
    ) -> None:
        batch = con.execute(
            "select state from bot_add_batches where id=?", (batch_id,)
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
                "select shard_index,item_count from bot_add_shards "
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
            processed = sum(state in _TERMINAL_ITEM_STATES for state in states)
            if processed == len(states):
                shard_state = "complete"
                completed_at = now
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
        nonterminal = sum(str(row["state"]) not in _TERMINAL_ITEM_STATES for row in items)
        if nonterminal == 0:
            state = "complete"
            completed_at = now
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
        terminal = sorted(_TERMINAL_ITEM_STATES)
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
