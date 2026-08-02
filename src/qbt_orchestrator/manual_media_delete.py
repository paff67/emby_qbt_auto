from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping

from .observability import redact
from .processed_media import ProcessedMediaRepository


@dataclass(frozen=True)
class ManualDeleteResult:
    normalized_id: str
    ok: bool
    lifecycle_state: str
    download_policy: str
    error: str | None = None


class ManualMediaDeleteService:
    """CLI-only deletion/tombstone saga for P1.

    Flow: manual_delete_pending + block_permanent → Drive Trash → mount absent →
    Emby cleanup → manual_deleted. Failures keep the permanent block.
    """

    def __init__(
        self,
        state_db: str | Path,
        *,
        processed_media: ProcessedMediaRepository | None = None,
        drive_trasher: Callable[[str], None] | None = None,
        mount_checker: Callable[[str], bool] | None = None,
        emby_cleaner: Callable[[str], None] | None = None,
        warning_service=None,
        now: Callable[[], int] | None = None,
    ):
        self.state_db = Path(state_db)
        self.processed_media = processed_media or ProcessedMediaRepository(self.state_db)
        self.drive_trasher = drive_trasher
        self.mount_checker = mount_checker
        self.emby_cleaner = emby_cleaner
        self.warning_service = warning_service
        self.now = now or (lambda: int(time.time()))

    def delete(
        self,
        normalized_id: str,
        *,
        actor_id: str,
        reason: str,
        remote_path: str | None = None,
        deletion_manifest: str | None = None,
        create_if_missing: bool = False,
    ) -> ManualDeleteResult:
        nid = str(normalized_id).strip()
        pending = self.processed_media.begin_manual_delete(
            nid,
            actor_id=actor_id,
            reason=reason,
            deletion_manifest=deletion_manifest,
            create_if_missing=create_if_missing,
        )
        path = remote_path or pending.get("last_remote_path")
        try:
            if self.drive_trasher is not None and path:
                self.drive_trasher(str(path))
            if self.mount_checker is not None and path:
                if self.mount_checker(str(path)):
                    raise RuntimeError("mount_path_still_present")
            if self.emby_cleaner is not None:
                self.emby_cleaner(nid)
            final = self.processed_media.finalize_manual_delete(nid, actor_id=actor_id)
            return ManualDeleteResult(
                normalized_id=nid,
                ok=True,
                lifecycle_state=str(final["lifecycle_state"]),
                download_policy=str(final["download_policy"]),
            )
        except Exception as exc:
            failed = self.processed_media.fail_manual_delete(
                nid, actor_id=actor_id, error=str(exc)
            )
            if self.warning_service is not None:
                try:
                    self.warning_service.report(
                        warning_key=f"checked_add:manual_delete_failed:{nid}",
                        severity="error",
                        topic="manual_delete",
                        safe_message=str(redact(f"手动删除失败并保持永久拦截：{nid}")),
                    )
                except Exception:
                    pass
            return ManualDeleteResult(
                normalized_id=nid,
                ok=False,
                lifecycle_state=str(failed["lifecycle_state"]),
                download_policy=str(failed["download_policy"]),
                error=str(redact(str(exc)))[:500],
            )
