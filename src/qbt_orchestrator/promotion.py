from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from .db import readonly_connect, write_transaction
from .observability import redact


class MediaPromotionRepository:
    def __init__(self, state_db: str | Path, now=None):
        self.state_db = Path(state_db)
        self.now = now or (lambda: int(time.time()))

    def enqueue(
        self,
        *,
        upload_job_id: int,
        hash: str | None,
        media_group_id: int | None,
        normalized_id: str,
        metadata_title: str,
        display_title: str,
        source_remote: str,
        target_remote: str,
        expected_size: int,
        expected_hashes: dict[str, str] | None = None,
        max_attempts: int = 6,
    ) -> int:
        now = int(self.now())
        hashes_json = json.dumps(
            expected_hashes or {}, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )

        def txn(con):
            con.execute(
                "insert or ignore into media_promotions("
                "upload_job_id,hash,media_group_id,normalized_id,metadata_title,display_title,"
                "source_remote,target_remote,expected_size,expected_hashes_json,state,max_attempts,created_at,updated_at"
                ") values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    int(upload_job_id),
                    hash,
                    media_group_id,
                    str(normalized_id),
                    str(metadata_title),
                    str(display_title),
                    str(source_remote),
                    str(target_remote),
                    int(expected_size),
                    hashes_json,
                    "planned",
                    int(max_attempts),
                    now,
                    now,
                ),
            )
            row = con.execute(
                "select id from media_promotions where upload_job_id=? and source_remote=? and target_remote=?",
                (int(upload_job_id), str(source_remote), str(target_remote)),
            ).fetchone()
            return int(row["id"])

        return int(write_transaction(self.state_db, txn))

    def claim_next(self, *, owner: str = "local", lease_sec: int = 1800) -> dict[str, Any] | None:
        now = int(self.now())

        def txn(con):
            row = con.execute(
                "select * from media_promotions where attempts<max_attempts "
                "and state in ('planned','retry_wait') "
                "and (state!='retry_wait' or next_run_at is null or next_run_at<=?) "
                "order by id limit 1",
                (now,),
            ).fetchone()
            if not row:
                return None
            con.execute(
                "update media_promotions set state='moving',attempts=attempts+1,lease_owner=?,lease_until=?,updated_at=? where id=?",
                (str(owner), now + int(lease_sec), now, int(row["id"])),
            )
            return dict(
                con.execute(
                    "select * from media_promotions where id=?", (int(row["id"]),)
                ).fetchone()
            )

        return write_transaction(self.state_db, txn)

    def schedule_retry(self, promotion_id: int, error: str, *, delay_sec: int = 60) -> None:
        now = int(self.now())
        write_transaction(
            self.state_db,
            lambda con: con.execute(
                "update media_promotions set state='retry_wait',next_run_at=?,last_error=?,"
                "lease_owner=null,lease_until=null,updated_at=? where id=?",
                (
                    now + int(delay_sec),
                    str(redact(str(error)))[:500],
                    now,
                    int(promotion_id),
                ),
            ),
        )

    def record_verified(
        self,
        promotion_id: int,
        *,
        method: str,
        details: dict[str, Any],
    ) -> None:
        now = int(self.now())
        write_transaction(
            self.state_db,
            lambda con: con.execute(
                "update media_promotions set state='verified',verification_method=?,"
                "verification_result_json=?,verified_at=?,next_run_at=null,last_error=null,"
                "lease_owner=null,lease_until=null,updated_at=? where id=?",
                (
                    str(method),
                    json.dumps(details, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
                    now,
                    now,
                    int(promotion_id),
                ),
            ),
        )

    def record_failed(self, promotion_id: int, error: str, *, state: str = "failed") -> None:
        now = int(self.now())
        write_transaction(
            self.state_db,
            lambda con: con.execute(
                "update media_promotions set state=?,last_error=?,lease_owner=null,lease_until=null,updated_at=? where id=?",
                (str(state), str(redact(str(error)))[:500], now, int(promotion_id)),
            ),
        )

    def get(self, promotion_id: int) -> dict[str, Any]:
        con = readonly_connect(self.state_db)
        try:
            row = con.execute(
                "select * from media_promotions where id=?", (int(promotion_id),)
            ).fetchone()
            if not row:
                raise KeyError(promotion_id)
            return dict(row)
        finally:
            con.close()

    def pending_for_upload(self, upload_job_id: int) -> list[dict[str, Any]]:
        con = readonly_connect(self.state_db)
        try:
            return [
                dict(row)
                for row in con.execute(
                    "select * from media_promotions where upload_job_id=? and state!='verified' order by id",
                    (int(upload_job_id),),
                )
            ]
        finally:
            con.close()
