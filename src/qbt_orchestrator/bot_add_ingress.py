"""Advance submitted bot-add items out of ``received`` into the probe pipeline."""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any, Callable

from .db import readonly_connect
from .download_links import (
    HttpMetainfoResolver,
    LinkResolutionError,
    parse_download_link,
)

LOGGER = logging.getLogger(__name__)

_DEFAULT_MAX_ITEMS = 20
_STUCK_RECEIVED_AGE_SEC = 60
_STUCK_WARNING_KEY = "bot_add:ingress:oldest_received"


class BotAddIngressCoordinator:
    """Resolve submitted ``received`` items into ``waiting_probe_slot`` / ``invalid``.

    Bridges Telegram draft submit and MetadataProbeCoordinator. Does not talk to
    qBT; it only classifies links and writes durable item fields.
    """

    def __init__(
        self,
        repository,
        *,
        http_resolver: HttpMetainfoResolver | None = None,
        warning_service=None,
        max_items: int = _DEFAULT_MAX_ITEMS,
        stuck_age_sec: int = _STUCK_RECEIVED_AGE_SEC,
        now: Callable[[], int] | None = None,
    ) -> None:
        self.repository = repository
        self.http_resolver = http_resolver or HttpMetainfoResolver()
        self.warning_service = warning_service
        self.max_items = max(1, int(max_items))
        self.stuck_age_sec = max(1, int(stuck_age_sec))
        repo_now = getattr(repository, "_now", None)
        self.now = now or repo_now or (lambda: int(time.time()))

    def tick(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "processed": [],
            "waiting_probe_slot": [],
            "invalid": [],
            "errors": 0,
            "oldest_received_age": 0,
            "stuck_alerted": False,
        }
        now = int(self.now())
        oldest_age = self._oldest_received_age(now)
        result["oldest_received_age"] = oldest_age
        if oldest_age > self.stuck_age_sec:
            result["stuck_alerted"] = self._alert_stuck(oldest_age)

        for row in self._claim_candidates(limit=self.max_items):
            item_id = int(row["id"])
            try:
                outcome = self._process_one(row)
            except Exception:
                LOGGER.exception("bot add ingress failed item_id=%s", item_id)
                result["errors"] = int(result["errors"]) + 1
                continue
            result["processed"].append(item_id)
            if outcome == "waiting_probe_slot":
                result["waiting_probe_slot"].append(item_id)
            elif outcome == "invalid":
                result["invalid"].append(item_id)
        return result

    def _process_one(self, row: dict[str, Any]) -> str:
        item_id = int(row["id"])
        state = str(row["state"])
        if state == "received":
            self.repository.transition_item(
                item_id,
                {"received"},
                "resolving",
                "ingress_started",
            )
        elif state != "resolving":
            raise ValueError("unexpected_ingress_state")

        current = self.repository.get_item(item_id, include_raw=True)
        raw = str(current.get("raw_input") or "")
        if not raw:
            self.repository.transition_item(
                item_id,
                {"resolving"},
                "invalid",
                "raw_input_unavailable",
                {"last_error": "raw_input_unavailable"},
            )
            return "invalid"

        try:
            resolved = self._resolve_link(raw)
        except LinkResolutionError as exc:
            self.repository.transition_item(
                item_id,
                {"resolving"},
                "invalid",
                str(exc.reason or "invalid_link")[:64],
                {"last_error": str(exc.reason or "invalid_link")[:200]},
            )
            return "invalid"

        infohash_v1 = resolved.infohash_v1
        infohash_v2 = resolved.infohash_v2
        if not infohash_v1 and not infohash_v2:
            self.repository.transition_item(
                item_id,
                {"resolving"},
                "invalid",
                "missing_infohash",
                {"last_error": "missing_infohash"},
            )
            return "invalid"

        identity = str(infohash_v1 or infohash_v2)
        fields: dict[str, Any] = {
            "infohash_v1": infohash_v1,
            "infohash_v2": infohash_v2,
            "canonical_identity": identity,
            "qbt_hash": infohash_v1 or identity,
            "last_error": None,
        }
        self.repository.transition_item(
            item_id,
            {"resolving"},
            "waiting_probe_slot",
            "ingress_resolved",
            fields,
        )
        return "waiting_probe_slot"

    def _resolve_link(self, raw: str):
        parsed = parse_download_link(raw)
        if parsed.kind in {"http_url", "https_url"}:
            return self.http_resolver.resolve(raw)
        if parsed.infohash_v1 or parsed.infohash_v2:
            return parsed
        raise LinkResolutionError("missing_infohash")

    def _claim_candidates(self, *, limit: int) -> list[dict[str, Any]]:
        state_db = Path(self.repository.state_db)
        con = readonly_connect(state_db)
        try:
            rows = list(
                con.execute(
                    "select i.* from bot_add_items i "
                    "join bot_add_batches b on b.id=i.batch_id "
                    "where i.state in ('received','resolving') "
                    "and b.state in ('queued','processing','awaiting_confirmation') "
                    "order by i.created_at asc, i.id asc limit ?",
                    (int(limit),),
                )
            )
            return [dict(row) for row in rows]
        finally:
            con.close()

    def _oldest_received_age(self, now: int) -> int:
        state_db = Path(self.repository.state_db)
        con = readonly_connect(state_db)
        try:
            row = con.execute(
                "select min(i.created_at) as oldest from bot_add_items i "
                "join bot_add_batches b on b.id=i.batch_id "
                "where i.state='received' "
                "and b.state in ('queued','processing','awaiting_confirmation')"
            ).fetchone()
        finally:
            con.close()
        if row is None or row["oldest"] is None:
            return 0
        return max(0, int(now) - int(row["oldest"]))

    def _alert_stuck(self, oldest_age: int) -> bool:
        if self.warning_service is None:
            return False
        try:
            self.warning_service.report(
                warning_key=_STUCK_WARNING_KEY,
                severity="warning",
                topic="bot_add_ingress",
                safe_message=(
                    "添加队列有链接超过 "
                    f"{self.stuck_age_sec} 秒仍停留在 received，"
                    f"当前最久 {int(oldest_age)} 秒。"
                ),
            )
            return True
        except Exception:
            LOGGER.exception("failed to report stuck received ingress warning")
            return False
