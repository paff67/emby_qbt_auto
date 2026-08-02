from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Protocol
from urllib.parse import urlsplit

from .db import readonly_connect, write_transaction
from .observability import redact
from .processed_media import ProcessedMediaRepository


class DriveTrashAdapter(Protocol):
    def trash_exact_directory(self, remote_dir: str) -> str:
        """Move an exact remote directory into the trash remote; return trash path."""


class MountPresenceAdapter(Protocol):
    def path_exists(self, host_or_mount_path: str) -> bool: ...


class EmbyRefreshAdapter(Protocol):
    def refresh_normalized_id(self, normalized_id: str, media_path: str) -> None: ...


@dataclass(frozen=True)
class ManualDeleteResult:
    normalized_id: str
    ok: bool
    lifecycle_state: str
    download_policy: str
    stage: str
    error: str | None = None


_STAGE_ORDER = (
    "pending",
    "remote_trashed",
    "mount_absent",
    "emby_refreshed",
    "manual_deleted",
)


def _exact_remote_dir_for_id(normalized_id: str, remote_path: str) -> str:
    nid = str(normalized_id).strip()
    raw = str(remote_path or "").strip()
    if not nid or not raw:
        raise ValueError("remote_path_required")
    # Accept rclone remote paths like gcrypt:/BBAN-582 or gcrypt:/BBAN-582/file.mp4
    if ":" in raw:
        remote, path = raw.split(":", 1)
        path = "/" + path.lstrip("/")
    else:
        remote, path = "", raw
    posix = PurePosixPath(path)
    parts = [part for part in posix.parts if part not in {"/", "."}]
    if not parts:
        raise ValueError("remote_path_invalid")
    # Exact directory basename must equal normalized ID (case-insensitive).
    if parts[0].casefold() != nid.casefold():
        # If path points at a file under the ID directory, require parent basename.
        if len(parts) >= 2 and parts[-2].casefold() == nid.casefold():
            dir_path = PurePosixPath("/")
            for part in parts[:-1]:
                dir_path = dir_path / part
        else:
            raise ValueError("normalized_id_path_mismatch")
    else:
        dir_path = PurePosixPath("/") / parts[0]
    if remote:
        return f"{remote}:/{str(dir_path).lstrip('/')}"
    return str(dir_path)


class RcloneDriveTrashAdapter:
    def __init__(self, rclone, *, trash_remote: str):
        self.rclone = rclone
        self.trash_remote = str(trash_remote).rstrip(":") + ":"

    def trash_exact_directory(self, remote_dir: str) -> str:
        source = str(remote_dir).rstrip("/")
        basename = PurePosixPath(source.split(":", 1)[-1]).name
        if not basename:
            raise ValueError("remote_path_invalid")
        target = f"{self.trash_remote}{basename}"
        self.rclone.moveto(source, target)
        return target


class LocalMountPresenceAdapter:
    def __init__(self, mount_root: str | Path):
        self.mount_root = Path(mount_root)

    def path_exists(self, host_or_mount_path: str) -> bool:
        path = Path(host_or_mount_path)
        if not path.is_absolute():
            path = self.mount_root / path
        return path.exists()


class EmbyPathRefreshAdapter:
    def __init__(self, emby, *, media_prefix: str = "/media/gcrypt"):
        self.emby = emby
        self.media_prefix = str(media_prefix).rstrip("/")

    def refresh_normalized_id(self, normalized_id: str, media_path: str) -> None:
        path = media_path or f"{self.media_prefix}/{normalized_id}"
        self.emby.refresh_path(path)


class ManualMediaDeleteService:
    """CLI-only deletion/tombstone saga with required adapters and staged resume."""

    def __init__(
        self,
        state_db: str | Path,
        *,
        drive_trasher: DriveTrashAdapter,
        mount_checker: MountPresenceAdapter,
        emby_cleaner: EmbyRefreshAdapter,
        processed_media: ProcessedMediaRepository | None = None,
        warning_service=None,
        mount_path_for_id: Callable[[str], str] | None = None,
        now: Callable[[], int] | None = None,
    ):
        if drive_trasher is None or mount_checker is None or emby_cleaner is None:
            raise ValueError("manual_delete_adapters_required")
        self.state_db = Path(state_db)
        self.drive_trasher = drive_trasher
        self.mount_checker = mount_checker
        self.emby_cleaner = emby_cleaner
        self.processed_media = processed_media or ProcessedMediaRepository(self.state_db)
        self.warning_service = warning_service
        self.mount_path_for_id = mount_path_for_id or (
            lambda nid: str(Path("/media/gcrypt") / nid)
        )
        self.now = now or (lambda: int(time.time()))

    def delete(
        self,
        normalized_id: str,
        *,
        actor_id: str,
        reason: str,
        remote_path: str,
        deletion_manifest: str | None = None,
        create_if_missing: bool = False,
    ) -> ManualDeleteResult:
        nid = str(normalized_id).strip()
        if not nid:
            raise ValueError("normalized_id")
        exact_dir = _exact_remote_dir_for_id(nid, remote_path)
        stage = self._load_stage(nid) or "pending"
        if stage == "pending":
            self.processed_media.begin_manual_delete(
                nid,
                actor_id=actor_id,
                reason=reason,
                deletion_manifest=deletion_manifest,
                create_if_missing=create_if_missing,
            )
            self._save_stage(nid, "pending", {"remote_dir": exact_dir})
            stage = "pending"

        saved = self._load_payload(nid)
        # Resume must reuse the persisted exact directory; never re-guess.
        remote_dir = str(saved.get("remote_dir") or exact_dir)
        if PurePosixPath(remote_dir.split(":", 1)[-1]).name.casefold() != nid.casefold():
            raise ValueError("normalized_id_path_mismatch")

        try:
            if stage == "pending":
                trash_path = self.drive_trasher.trash_exact_directory(remote_dir)
                self._save_stage(
                    nid, "remote_trashed", {"remote_dir": remote_dir, "trash_path": trash_path}
                )
                stage = "remote_trashed"
            if stage == "remote_trashed":
                mount_path = self.mount_path_for_id(nid)
                if self.mount_checker.path_exists(mount_path):
                    raise RuntimeError("mount_path_still_present")
                self._save_stage(
                    nid,
                    "mount_absent",
                    {
                        "remote_dir": remote_dir,
                        "trash_path": saved.get("trash_path"),
                        "mount_path": mount_path,
                    },
                )
                stage = "mount_absent"
            if stage == "mount_absent":
                mount_path = str(saved.get("mount_path") or self.mount_path_for_id(nid))
                self.emby_cleaner.refresh_normalized_id(nid, mount_path)
                self._save_stage(nid, "emby_refreshed", saved)
                stage = "emby_refreshed"
            if stage == "emby_refreshed":
                final = self.processed_media.finalize_manual_delete(nid, actor_id=actor_id)
                self._save_stage(nid, "manual_deleted", saved)
                return ManualDeleteResult(
                    normalized_id=nid,
                    ok=True,
                    lifecycle_state=str(final["lifecycle_state"]),
                    download_policy=str(final["download_policy"]),
                    stage="manual_deleted",
                )
            row = self.processed_media.get_by_normalized_id(nid) or {}
            return ManualDeleteResult(
                normalized_id=nid,
                ok=True,
                lifecycle_state=str(row.get("lifecycle_state") or "manual_deleted"),
                download_policy=str(row.get("download_policy") or "block_permanent"),
                stage=stage,
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
                stage=stage,
                error=str(redact(str(exc)))[:500],
            )

    def _load_stage(self, normalized_id: str) -> str | None:
        con = readonly_connect(self.state_db)
        try:
            row = con.execute(
                "select payload_json from processed_media_events "
                "where processed_media_id=(select id from processed_media where normalized_id=? collate nocase) "
                "and event_type='manual_delete_stage' order by id desc limit 1",
                (normalized_id,),
            ).fetchone()
            if row is None:
                return None
            import json

            payload = json.loads(row["payload_json"] or "{}")
            stage = str(payload.get("stage") or "")
            return stage if stage in _STAGE_ORDER else None
        finally:
            con.close()

    def _load_payload(self, normalized_id: str) -> dict[str, Any]:
        con = readonly_connect(self.state_db)
        try:
            row = con.execute(
                "select payload_json from processed_media_events "
                "where processed_media_id=(select id from processed_media where normalized_id=? collate nocase) "
                "and event_type='manual_delete_stage' order by id desc limit 1",
                (normalized_id,),
            ).fetchone()
            if row is None:
                return {}
            import json

            payload = json.loads(row["payload_json"] or "{}")
            return dict(payload) if isinstance(payload, dict) else {}
        finally:
            con.close()

    def _save_stage(self, normalized_id: str, stage: str, payload: dict[str, Any]) -> None:
        import json

        now = int(self.now())
        body = dict(payload)
        body["stage"] = stage

        def txn(con) -> None:
            media = con.execute(
                "select id from processed_media where normalized_id=? collate nocase",
                (normalized_id,),
            ).fetchone()
            if media is None:
                raise ValueError("processed_media_missing")
            con.execute(
                "insert into processed_media_events("
                "processed_media_id,event_type,event_at,actor_type,actor_id,payload_json) "
                "values(?,?,?,?,?,?)",
                (
                    int(media["id"]),
                    "manual_delete_stage",
                    now,
                    "cli",
                    "manual_media_delete",
                    json.dumps(body, ensure_ascii=False, sort_keys=True),
                ),
            )

        write_transaction(self.state_db, txn)
