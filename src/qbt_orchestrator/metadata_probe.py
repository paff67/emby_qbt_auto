from __future__ import annotations

from dataclasses import dataclass, fields
import hashlib
import time
import uuid
from typing import Any, Callable, Mapping

from .db import readonly_connect


@dataclass(frozen=True)
class MetadataProbeConfig:
    slots: int = 3
    poll_interval_sec: int = 5
    windows_sec: tuple[int, int, int] = (300, 600, 900)
    backoffs_sec: tuple[int, int] = (1800, 21600)
    payload_limit_bps: int = 1024
    lease_sec: int = 30
    visibility_grace_sec: int = 30

    def __post_init__(self) -> None:
        for field in fields(self):
            value = getattr(self, field.name)
            values = value if isinstance(value, tuple) else (value,)
            if not values or any(
                isinstance(item, bool) or not isinstance(item, int) or item <= 0
                for item in values
            ):
                raise ValueError(field.name)
        if len(self.windows_sec) != 3:
            raise ValueError("windows_sec")
        if len(self.backoffs_sec) != 2:
            raise ValueError("backoffs_sec")


class MetadataProbeCoordinator:
    """Durable, bounded metadata probing for submitted bot add items."""

    def __init__(
        self,
        repository,
        gateway,
        *,
        config: MetadataProbeConfig | None = None,
        owner: str | None = None,
        now: Callable[[], int] | None = None,
        warning_service=None,
        notifications=None,
    ) -> None:
        self.repository = repository
        self.gateway = gateway
        self.config = config or MetadataProbeConfig()
        self.owner = str(owner or f"metadata-probe-{uuid.uuid4().hex}")
        self.now = now or (lambda: int(time.time()))
        # Prefer WarningService (inbox-first). Legacy notifications= is ignored.
        _ = notifications
        self.warning_service = warning_service

    def tick(
        self,
        sync_healthy: bool = True,
        snapshots: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        result: dict[str, Any] = {
            "suspended": not bool(sync_healthy),
            "started": [],
            "ready": [],
            "timed_out": [],
            "recovered": [],
            "requeued": [],
            "duplicates": [],
            "errors": 0,
        }
        if not sync_healthy:
            return result
        if snapshots is not None:
            self.gateway.set_snapshots(snapshots)
        now = int(self.now())
        for row in self._due_active(now):
            item_id = int(row["id"])
            token = self._take_or_renew(row, now, result)
            if token is None:
                continue
            self._poll_item(item_id, token, now, result)

        for row in self._expired_active(now):
            item_id = int(row["id"])
            if item_id in result["ready"]:
                continue
            token = self._take_or_renew(row, now, result)
            if token is None:
                continue
            self._timeout_item(item_id, token, now, result)

        active = self._active_count()
        capacity = max(0, self.config.slots - active)
        if self._has_probe_errors():
            return result
        for row in self._fair_candidates(now, capacity):
            self._start_item(dict(row), now, result)
            if self._has_probe_errors():
                break
        return result

    def _start_item(
        self, row: dict[str, Any], now: int, result: dict[str, Any]
    ) -> None:
        item_id = int(row["id"])
        try:
            lease = self.repository.claim_metadata_lease(
                item_id, self.owner, now + self.config.lease_sec
            )
        except ValueError:
            return
        generation = int(lease["metadata_lease_generation"])
        token = (self.owner, generation)
        raw = self.repository.get_item(item_id, include_raw=True)
        attempt = int(raw.get("metadata_probe_attempt") or 0) + 1
        if attempt > len(self.config.windows_sec):
            self._safe_release(item_id, token)
            return
        deadline = now + self.config.windows_sec[attempt - 1]
        raw_expires_at = int(raw.get("raw_input_expires_at") or 0)
        if raw_expires_at < deadline:
            try:
                unavailable = self.repository.transition_item(
                    item_id,
                    {"waiting_probe_slot", "metadata_retry_wait"},
                    "metadata_unavailable",
                    "metadata_raw_window_too_short",
                    {"last_error": "raw_input_window_too_short"},
                    metadata_lease_owner=self.owner,
                    metadata_lease_generation=generation,
                )
                self._notify_metadata_unavailable(unavailable)
                result["errors"] = int(result["errors"]) + 1
            except Exception:
                self._safe_release(item_id, token)
            return
        tag = str(raw.get("qbt_precheck_tag") or self._tag_for(raw))
        expected_hash = self.gateway.expected_hash(str(raw.get("raw_input") or ""))
        try:
            current = self.repository.transition_item(
                item_id,
                {"waiting_probe_slot", "metadata_retry_wait"},
                "metadata_wait",
                "metadata_probe_started",
                {
                    "metadata_probe_attempt": attempt,
                    "metadata_probe_started_at": now,
                    "metadata_probe_deadline": deadline,
                    "metadata_next_poll_at": now + self.config.poll_interval_sec,
                    "metadata_retry_at": None,
                    "next_run_at": None,
                    "qbt_precheck_tag": tag,
                    "qbt_hash": expected_hash,
                    "last_error": None,
                },
                metadata_lease_owner=self.owner,
                metadata_lease_generation=generation,
            )
        except Exception:
            self._safe_release(item_id, token)
            return
        result["started"].append(item_id)
        self._drive_item(
            self.repository.get_item(item_id, include_raw=True),
            token,
            now,
            result,
            allow_add=True,
        )

    def _poll_item(
        self,
        item_id: int,
        token: tuple[str, int],
        now: int,
        result: dict[str, Any],
    ) -> None:
        try:
            item = self.repository.get_item(item_id, include_raw=True)
            self._drive_item(item, token, now, result, allow_add=False)
        except Exception:
            self._record_failure(item_id, token, now, result)

    def _drive_item(
        self,
        item: Mapping[str, Any],
        token: tuple[str, int],
        now: int,
        result: dict[str, Any],
        *,
        allow_add: bool,
    ) -> None:
        item_id = int(item["id"])
        tag = str(item.get("qbt_precheck_tag") or "")
        try:
            snapshot = self.gateway.find_by_tag(tag)
            expected_hash = str(item.get("qbt_hash") or "").lower()
            if snapshot is None and expected_hash:
                snapshot = self.gateway.find_by_hash(expected_hash)
            if snapshot is None and allow_add:
                raw = str(item.get("raw_input") or "")
                if str(item.get("input_kind")) != "magnet" or not raw:
                    raise ValueError("unsupported_precheck_input")
                self._require_write(
                    self.gateway.add_magnet(
                        raw,
                        tag,
                        guard=self._lease_guard(item_id, token),
                    )
                )
                snapshot = self.gateway.find_by_tag(tag)
                if snapshot is None:
                    snapshot = self.gateway.find_by_hash(expected_hash)
            if snapshot is None:
                started_at = int(item.get("metadata_probe_started_at") or now)
                if now - started_at >= self.config.visibility_grace_sec:
                    self._requeue_missing(item, token, result)
                    return
                self._schedule_poll(item_id, token, now, clear_error=False)
                return
            torrent_hash = str(snapshot.get("hash") or "").lower()
            stored_hash = str(item.get("qbt_hash") or "").lower()
            if stored_hash and stored_hash != torrent_hash:
                raise ValueError("qbt_precheck_hash_mismatch")
            current = self.gateway.torrent_info(torrent_hash)
            if not str(current.get("state") or "").strip():
                self._schedule_poll(item_id, token, now, clear_error=False)
                return
            current_hash = str(current.get("hash") or torrent_hash).lower()
            if current_hash != torrent_hash:
                raise ValueError("qbt_precheck_hash_mismatch")
            if not self._has_exact_tag(current, tag):
                self._finish_existing_duplicate(
                    item_id, token, torrent_hash, result
                )
                return
            snapshot = current
            self.repository.update_metadata_probe(
                item_id,
                token[0],
                token[1],
                {"qbt_hash": torrent_hash, "last_error": None},
            )
            if not self.gateway.metadata_ready(snapshot):
                self._schedule_poll(item_id, token, now)
                return
            self._require_write(
                self.gateway.stop(
                    torrent_hash,
                    guard=self._owned_write_guard(
                        item_id, token, torrent_hash, tag
                    ),
                )
            )
            files = self.gateway.torrent_files(torrent_hash)
            if not files:
                self._schedule_poll(item_id, token, now)
                return
            self.repository.renew_metadata_lease(
                item_id,
                token[0],
                token[1],
                int(self.now()) + self.config.lease_sec,
            )
            self._require_write(
                self.gateway.zero_file_priorities(
                    torrent_hash,
                    files,
                    guard=self._owned_write_guard(
                        item_id, token, torrent_hash, tag
                    ),
                )
            )
            verified = self.gateway.torrent_files(torrent_hash)
            if not self.gateway.all_priorities_zero(verified):
                raise ValueError("qbt_precheck_priority_verify_failed")
            current = self.gateway.torrent_info(torrent_hash)
            current_hash = str(current.get("hash") or torrent_hash).lower()
            if current_hash != torrent_hash:
                raise ValueError("qbt_precheck_hash_mismatch")
            if not self._has_exact_tag(current, tag):
                self._finish_existing_duplicate(
                    item_id, token, torrent_hash, result
                )
                return
            if not self._is_stopped_state(current.get("state")):
                raise ValueError("qbt_precheck_not_stopped")
            self.repository.transition_item(
                item_id,
                {"metadata_wait"},
                "prechecking",
                "metadata_ready",
                {
                    "qbt_hash": torrent_hash,
                    "metadata_probe_deadline": None,
                    "metadata_next_poll_at": None,
                    "metadata_retry_at": None,
                    "last_error": None,
                },
                metadata_lease_owner=token[0],
                metadata_lease_generation=token[1],
            )
            self.repository.release_metadata_lease(item_id, token[0], token[1])
            result["ready"].append(item_id)
        except Exception:
            self._record_failure(item_id, token, now, result)

    def _requeue_missing(
        self,
        item: Mapping[str, Any],
        token: tuple[str, int],
        result: dict[str, Any],
    ) -> None:
        item_id = int(item["id"])
        attempt = max(0, int(item.get("metadata_probe_attempt") or 0) - 1)
        self.repository.transition_item(
            item_id,
            {"metadata_wait"},
            "waiting_probe_slot",
            "qbt_add_not_visible",
            {
                "qbt_hash": None,
                "metadata_probe_attempt": attempt,
                "metadata_probe_started_at": None,
                "metadata_probe_deadline": None,
                "metadata_next_poll_at": None,
                "metadata_retry_at": None,
                "next_run_at": None,
                "last_error": "qbt_add_not_visible",
            },
            metadata_lease_owner=token[0],
            metadata_lease_generation=token[1],
        )
        self.repository.release_metadata_lease(item_id, token[0], token[1])
        result["requeued"].append(item_id)

    def _timeout_item(
        self,
        item_id: int,
        token: tuple[str, int],
        now: int,
        result: dict[str, Any],
    ) -> None:
        try:
            item = self.repository.get_item(item_id, include_raw=True)
            tag = str(item.get("qbt_precheck_tag") or "")
            snapshot = self.gateway.find_by_tag(tag)
            stored_hash = str(item.get("qbt_hash") or "").lower()
            if snapshot is None and stored_hash:
                snapshot = self.gateway.find_by_hash(stored_hash)
            if snapshot is not None:
                torrent_hash = str(snapshot.get("hash") or "").lower()
                if stored_hash and stored_hash != torrent_hash:
                    raise ValueError("qbt_precheck_hash_mismatch")
                current = self.gateway.torrent_info(torrent_hash)
                if not str(current.get("state") or "").strip():
                    self._record_failure(item_id, token, now, result)
                    return
                current_hash = str(current.get("hash") or torrent_hash).lower()
                if current_hash != torrent_hash:
                    raise ValueError("qbt_precheck_hash_mismatch")
                if not self._has_exact_tag(current, tag):
                    self._finish_existing_duplicate(
                        item_id, token, torrent_hash, result
                    )
                    return
                self._require_write(
                    self.gateway.stop(
                        torrent_hash,
                        guard=self._owned_write_guard(
                            item_id, token, torrent_hash, tag
                        ),
                    )
                )
                self._require_write(
                    self.gateway.remove_registration(
                        torrent_hash,
                        guard=self._owned_write_guard(
                            item_id, token, torrent_hash, tag
                        ),
                    )
                )
            attempt = int(item.get("metadata_probe_attempt") or 0)
            fields = {
                "qbt_hash": None,
                "metadata_probe_started_at": None,
                "metadata_probe_deadline": None,
                "last_error": None,
            }
            if attempt < len(self.config.windows_sec):
                retry_at = now + self.config.backoffs_sec[attempt - 1]
                fields.update(
                    {
                        "metadata_retry_at": retry_at,
                        "metadata_next_poll_at": retry_at,
                        "next_run_at": retry_at,
                    }
                )
                self.repository.transition_item(
                    item_id,
                    {"metadata_wait"},
                    "metadata_retry_wait",
                    "metadata_probe_timeout",
                    fields,
                    metadata_lease_owner=token[0],
                    metadata_lease_generation=token[1],
                )
                self.repository.release_metadata_lease(item_id, token[0], token[1])
            else:
                fields.update(
                    {
                        "metadata_retry_at": None,
                        "metadata_next_poll_at": None,
                        "next_run_at": None,
                    }
                )
                unavailable = self.repository.transition_item(
                    item_id,
                    {"metadata_wait"},
                    "metadata_unavailable",
                    "metadata_probe_exhausted",
                    fields,
                    metadata_lease_owner=token[0],
                    metadata_lease_generation=token[1],
                )
                self._notify_metadata_unavailable(unavailable)
            result["timed_out"].append(item_id)
        except Exception:
            self._record_failure(item_id, token, now, result)

    def _notify_metadata_unavailable(self, item: Mapping[str, Any]) -> None:
        if self.warning_service is None:
            return
        try:
            generation = int(item["approval_generation"])
            item_id = int(item["id"])
            batch_id = int(item["batch_id"])
            self.warning_service.report(
                warning_key=f"checked_add:metadata_unavailable:{item_id}",
                severity="warning",
                topic="metadata_probe",
                safe_message="暂时无法获取元数据，请从批次详情选择重试或取消。",
                related_batch_id=batch_id,
                related_item_id=item_id,
                projection_payload={
                    "item_id": item_id,
                    "batch_id": batch_id,
                    "reply_markup": {
                        "inline_keyboard": [
                            [
                                {
                                    "text": "立即重试",
                                    "callback_data": f"i:r:{item_id}:{generation}",
                                },
                                {
                                    "text": "取消",
                                    "callback_data": f"i:x:{item_id}:{generation}",
                                },
                            ],
                            [
                                {
                                    "text": "查看批次",
                                    "callback_data": f"n:b:{batch_id}:0",
                                }
                            ],
                        ]
                    },
                },
            )
        except Exception:
            return

    def _schedule_poll(
        self,
        item_id: int,
        token: tuple[str, int],
        now: int,
        *,
        clear_error: bool = True,
    ) -> None:
        fields: dict[str, Any] = {
            "metadata_next_poll_at": now + self.config.poll_interval_sec,
        }
        if clear_error:
            fields["last_error"] = None
        self.repository.update_metadata_probe(
            item_id,
            token[0],
            token[1],
            fields,
        )

    def _record_failure(
        self,
        item_id: int,
        token: tuple[str, int],
        now: int,
        result: dict[str, Any],
    ) -> None:
        is_new = True
        try:
            is_new = (
                self.repository.get_item(item_id).get("last_error")
                != "qbt_precheck_failed"
            )
        except Exception:
            pass
        if is_new:
            result["errors"] = int(result["errors"]) + 1
        try:
            self.repository.update_metadata_probe(
                item_id,
                token[0],
                token[1],
                {
                    "metadata_next_poll_at": now + self.config.poll_interval_sec,
                    "last_error": "qbt_precheck_failed",
                },
            )
        except Exception:
            pass

    def _take_or_renew(
        self,
        row: Mapping[str, Any],
        now: int,
        result: dict[str, Any],
    ) -> tuple[str, int] | None:
        item_id = int(row["id"])
        owner = str(row.get("metadata_lease_owner") or "")
        lease_until = int(row.get("metadata_lease_until") or 0)
        generation = int(row.get("metadata_lease_generation") or 0)
        try:
            if owner == self.owner and lease_until > now:
                renewed = self.repository.renew_metadata_lease(
                    item_id,
                    self.owner,
                    generation,
                    now + self.config.lease_sec,
                )
                return self.owner, int(renewed["metadata_lease_generation"])
            if owner and lease_until > now:
                return None
            claimed = self.repository.claim_metadata_lease(
                item_id, self.owner, now + self.config.lease_sec
            )
            if str(row.get("state")) == "metadata_wait":
                result["recovered"].append(item_id)
            return self.owner, int(claimed["metadata_lease_generation"])
        except ValueError:
            return None

    def _safe_release(self, item_id: int, token: tuple[str, int]) -> None:
        try:
            self.repository.release_metadata_lease(item_id, token[0], token[1])
        except ValueError:
            pass

    def _lease_guard(
        self, item_id: int, token: tuple[str, int]
    ) -> Callable[[], bool]:
        def current() -> bool:
            try:
                item = self.repository.get_item(item_id)
                return (
                    str(item.get("metadata_lease_owner") or "") == token[0]
                    and int(item.get("metadata_lease_generation") or 0) == token[1]
                    and int(item.get("metadata_lease_until") or 0)
                    > int(self.now())
                )
            except Exception:
                return False

        return current

    def _owned_write_guard(
        self,
        item_id: int,
        token: tuple[str, int],
        torrent_hash: str,
        tag: str,
    ) -> Callable[[], bool]:
        lease_guard = self._lease_guard(item_id, token)

        def current() -> bool:
            if not lease_guard():
                return False
            try:
                info = self.gateway.torrent_info(torrent_hash)
                return (
                    str(info.get("hash") or torrent_hash).lower()
                    == torrent_hash
                    and bool(str(info.get("state") or "").strip())
                    and self._has_exact_tag(info, tag)
                )
            except Exception:
                return False

        return current

    def _finish_existing_duplicate(
        self,
        item_id: int,
        token: tuple[str, int],
        torrent_hash: str,
        result: dict[str, Any],
    ) -> None:
        self.repository.transition_item(
            item_id,
            {"metadata_wait"},
            "duplicate_local",
            "existing_torrent_without_precheck_tag",
            {
                "qbt_hash": torrent_hash,
                "decision": "duplicate_local",
                "decision_reason": "existing_torrent_without_precheck_tag",
                "last_error": None,
            },
            metadata_lease_owner=token[0],
            metadata_lease_generation=token[1],
        )
        result["duplicates"].append(item_id)

    @staticmethod
    def _has_exact_tag(snapshot: Mapping[str, Any], tag: str) -> bool:
        return tag in {
            part.strip()
            for part in str(snapshot.get("tags") or "").split(",")
            if part.strip()
        }

    @staticmethod
    def _require_write(applied: bool) -> None:
        if not applied:
            raise ValueError("metadata_lease_conflict")

    def _due_active(self, now: int) -> list[dict[str, Any]]:
        return self._query(
            "select i.* from bot_add_items i join bot_add_batches b on b.id=i.batch_id "
            "where i.state='metadata_wait' and b.state in "
            "('queued','processing','awaiting_confirmation') "
            "and i.metadata_next_poll_at is not null and i.metadata_next_poll_at<=? "
            "order by i.metadata_next_poll_at,i.id limit ?",
            (now, self.config.slots),
        )

    def _expired_active(self, now: int) -> list[dict[str, Any]]:
        return self._query(
            "select i.* from bot_add_items i join bot_add_batches b on b.id=i.batch_id "
            "where i.state='metadata_wait' and b.state in "
            "('queued','processing','awaiting_confirmation') "
            "and i.metadata_probe_deadline is not null and i.metadata_probe_deadline<=? "
            "order by i.metadata_probe_deadline,i.id limit ?",
            (now, self.config.slots),
        )

    def _active_count(self) -> int:
        rows = self._query(
            "select count(*) as count from bot_add_items i join bot_add_batches b "
            "on b.id=i.batch_id where i.state='metadata_wait' "
            "and b.state in ('queued','processing','awaiting_confirmation')",
            (),
        )
        return int(rows[0]["count"] if rows else 0)

    def _has_probe_errors(self) -> bool:
        rows = self._query(
            "select count(*) as count from bot_add_items i join bot_add_batches b "
            "on b.id=i.batch_id where i.state='metadata_wait' "
            "and i.last_error is not null and b.state in "
            "('queued','processing','awaiting_confirmation')",
            (),
        )
        return bool(rows and int(rows[0]["count"]) > 0)

    def _fair_candidates(self, now: int, limit: int) -> list[dict[str, Any]]:
        if limit <= 0:
            return []
        rows = self._query(
            "select i.* from bot_add_items i join bot_add_batches b on b.id=i.batch_id "
            "where b.state in ('queued','processing','awaiting_confirmation') and "
            "(i.state='waiting_probe_slot' or (i.state='metadata_retry_wait' "
            "and coalesce(i.metadata_retry_at,i.next_run_at,0)<=?)) "
            "and (i.metadata_lease_until is null or i.metadata_lease_until<=?) "
            "order by b.updated_at,b.id,i.id",
            (now, now),
        )
        active_batches = {
            int(row["batch_id"])
            for row in self._query(
                "select distinct i.batch_id from bot_add_items i "
                "join bot_add_batches b on b.id=i.batch_id "
                "where i.state='metadata_wait' and b.state in "
                "('queued','processing','awaiting_confirmation')",
                (),
            )
        }
        eligible_batches = {int(row["batch_id"]) for row in rows}
        if len(active_batches | eligible_batches) >= 3:
            rows = [
                row
                for row in rows
                if int(row["batch_id"]) not in active_batches
            ]
        by_batch: dict[int, list[dict[str, Any]]] = {}
        order: list[int] = []
        for row in rows:
            batch_id = int(row["batch_id"])
            if batch_id not in by_batch:
                by_batch[batch_id] = []
                order.append(batch_id)
            by_batch[batch_id].append(row)
        selected: list[dict[str, Any]] = []
        while len(selected) < limit:
            progressed = False
            for batch_id in order:
                if by_batch[batch_id] and len(selected) < limit:
                    selected.append(by_batch[batch_id].pop(0))
                    progressed = True
            if not progressed:
                break
        return selected

    def _query(self, sql: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
        con = readonly_connect(self.repository.state_db)
        try:
            return [dict(row) for row in con.execute(sql, params)]
        finally:
            con.close()

    @staticmethod
    def _tag_for(item: Mapping[str, Any]) -> str:
        material = f"{int(item['id'])}:{str(item.get('input_sha256') or '')}"
        return "add-item-" + hashlib.sha256(material.encode("utf-8")).hexdigest()[:32]

    @staticmethod
    def _is_stopped_state(value: Any) -> bool:
        return str(value or "").strip().lower() in {
            "stopped",
            "stoppeddl",
            "stoppedup",
            "paused",
            "pauseddl",
            "pausedup",
        }
