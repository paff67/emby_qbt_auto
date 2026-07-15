from __future__ import annotations

import csv
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
import re
import threading
import time
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any, Iterable, Mapping

from .naming import canonical_file_basename, canonical_media_name
from .observability import redact
from .db import write_transaction
from .io_governor import JobPriority

_ID = re.compile(r"(?i)([A-Z]{2,10})[-_ ]+[-_ ]*(\d{2,6})")
_VIDEO_EXTS = {
    ".mp4",
    ".mkv",
    ".avi",
    ".mov",
    ".wmv",
    ".m4v",
    ".ts",
    ".webm",
    ".flv",
    ".mpg",
    ".mpeg",
    ".iso",
}
_SUBTITLE_EXTS = {".srt", ".ass", ".ssa", ".sub", ".vtt"}
_IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp"}


@dataclass(frozen=True)
class MigrationAction:
    action_id: str
    kind: str
    normalized_id: str
    source: str
    target: str
    expected_size: int
    expected_hashes: dict[str, str] = field(default_factory=dict)
    state: str = "planned"
    reason: str = "canonicalize"


@dataclass(frozen=True)
class MigrationReview:
    source: str
    normalized_id: str
    reason: str
    details: str = ""


@dataclass
class MigrationPlan:
    actions: list[MigrationAction]
    review: list[MigrationReview]
    inventory_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "inventory_count": int(self.inventory_count),
            "actions": [asdict(item) for item in self.actions],
            "review": [asdict(item) for item in self.review],
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "MigrationPlan":
        return cls(
            actions=[MigrationAction(**dict(item)) for item in value.get("actions", [])],
            review=[MigrationReview(**dict(item)) for item in value.get("review", [])],
            inventory_count=int(value.get("inventory_count") or 0),
        )


@dataclass(frozen=True)
class MigrationResult:
    attempted: int
    verified: int
    failed: int
    skipped: int


def _relative_path(row: Mapping[str, Any]) -> str:
    return str(
        row.get("Path")
        or row.get("path")
        or row.get("relative_path")
        or row.get("Name")
        or row.get("name")
        or ""
    ).replace("\\", "/").lstrip("/")


def _hashes(row: Mapping[str, Any]) -> dict[str, str]:
    raw = row.get("Hashes") or row.get("hashes") or {}
    if not isinstance(raw, Mapping):
        return {}
    return {
        str(key).strip().lower(): str(value).strip().lower()
        for key, value in raw.items()
        if str(key).strip() and str(value).strip()
    }


def _title_snapshot(
    titles: Mapping[str, Any],
) -> dict[str, tuple[str, float]]:
    out: dict[str, tuple[str, float]] = {}
    for raw_id, raw_value in titles.items():
        media_id = str(raw_id).strip().upper()
        if isinstance(raw_value, str):
            title, confidence = raw_value.strip(), 1.0
        elif isinstance(raw_value, Mapping):
            title = str(
                raw_value.get("title")
                or raw_value.get("metadata_title")
                or raw_value.get("original_title")
                or ""
            ).strip()
            confidence = float(raw_value.get("confidence", 1.0) or 0.0)
        else:
            continue
        if media_id:
            out[media_id] = (title, confidence)
    return out


def _normalized_id(path: str) -> str:
    match = _ID.search(path)
    return f"{match.group(1).upper()}-{match.group(2)}" if match else ""


def _kind_and_target_name(path: str, canonical) -> tuple[str, str] | None:
    name = PurePosixPath(path).name
    stem = PurePosixPath(name).stem
    suffix = PurePosixPath(name).suffix.lower()
    lower = stem.lower()
    if suffix in _VIDEO_EXTS:
        return "video", f"{canonical_file_basename(canonical, name)}{suffix}"
    if suffix == ".nfo":
        return "nfo", f"{canonical.canonical_basename}.nfo"
    if suffix in _SUBTITLE_EXTS:
        return "subtitle", f"{canonical_file_basename(canonical, name)}{suffix}"
    if suffix in _IMAGE_EXTS:
        if "extrafanart" in path.lower():
            return "extrafanart", f"extrafanart/{name}"
        for kind in ("poster", "fanart", "thumb"):
            if lower == kind or lower.endswith(f"-{kind}"):
                generic = name if lower == kind else f"{canonical.canonical_basename}-{kind}{suffix}"
                return kind, generic
    return None


def _remote_path(relative: str, remote: str) -> str:
    return f"{remote.rstrip('/')}/{relative.lstrip('/')}"


def build_migration_plan(
    inventory: Iterable[Mapping[str, Any]],
    titles: Mapping[str, Any],
    *,
    min_confidence: float = 0.95,
    remote: str = "gcrypt:",
) -> MigrationPlan:
    rows = [dict(row) for row in inventory if not bool(row.get("IsDir"))]
    title_map = _title_snapshot(titles)
    existing = {
        _remote_path(_relative_path(row), remote)
        for row in rows
        if _relative_path(row)
    }
    actions: list[MigrationAction] = []
    review: list[MigrationReview] = []
    reserved_targets: set[str] = set()

    for row in sorted(rows, key=_relative_path):
        relative = _relative_path(row)
        if not relative:
            continue
        source = _remote_path(relative, remote)
        media_id = _normalized_id(relative)
        title_entry = title_map.get(media_id)
        if not media_id or title_entry is None or not title_entry[0]:
            review.append(
                MigrationReview(source, media_id, "missing_title", "title snapshot has no trusted match")
            )
            continue
        title, confidence = title_entry
        if confidence < float(min_confidence):
            review.append(
                MigrationReview(source, media_id, "low_confidence", f"confidence={confidence:.4f}")
            )
            continue
        canonical = canonical_media_name(media_id, title)
        classified = _kind_and_target_name(relative, canonical)
        if classified is None:
            continue
        kind, target_name = classified
        target = f"{canonical.remote_dir(remote)}/{target_name}"
        if source == target:
            continue
        if target in existing or target in reserved_targets:
            review.append(
                MigrationReview(source, media_id, "target_conflict", target)
            )
            continue
        action_id = hashlib.sha256(
            f"{source}\0{target}".encode("utf-8")
        ).hexdigest()
        actions.append(
            MigrationAction(
                action_id=action_id,
                kind=kind,
                normalized_id=media_id,
                source=source,
                target=target,
                expected_size=int(row.get("Size") or row.get("size") or 0),
                expected_hashes=_hashes(row),
            )
        )
        reserved_targets.add(target)

    return MigrationPlan(actions=actions, review=review, inventory_count=len(rows))


def _verified(action: MigrationAction, row: Mapping[str, Any] | None) -> bool:
    if row is None:
        return False
    if int(row.get("Size") or row.get("size") or 0) != int(action.expected_size):
        return False
    actual_hashes = _hashes(row)
    common = set(action.expected_hashes) & set(actual_hashes)
    return not common or all(
        action.expected_hashes[name] == actual_hashes[name] for name in common
    )


def action_verified(action: MigrationAction, remote_client) -> bool:
    return remote_client.stat(action.source) is None and _verified(
        action, remote_client.stat(action.target)
    )


def render_canonical_nfo(
    existing: bytes | str | None,
    normalized_id: str,
    metadata_title: str,
) -> bytes:
    canonical = canonical_media_name(normalized_id, metadata_title)
    raw = existing.encode("utf-8") if isinstance(existing, str) else existing
    try:
        root = ET.fromstring(raw) if raw else ET.Element("movie")
    except (ET.ParseError, ValueError):
        root = ET.Element("movie")
    if root.tag != "movie":
        wrapper = ET.Element("movie")
        wrapper.append(root)
        root = wrapper

    def set_text(tag: str, value: str) -> None:
        node = root.find(tag)
        if node is None:
            node = ET.SubElement(root, tag)
        node.text = value

    set_text("title", canonical.display_title)
    set_text("originaltitle", canonical.metadata_title)
    set_text("id", canonical.normalized_id)
    set_text("sorttitle", canonical.normalized_id)
    ET.indent(root, space="  ")
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


_HASH_PREFIX = re.compile(r"(?i)(?:torrent-|[-.])([0-9a-f]{12})(?:/|$)")


def reconcile_verified_migrations(
    state_db: str | Path,
    plan: MigrationPlan,
    remote_client,
    *,
    nfo_verified_ids: set[str],
    now: int,
) -> int:
    """Release only upload jobs whose matching video and rewritten NFO are verified."""
    observed_at = int(now)
    groups: dict[str, list[MigrationAction]] = {}
    prefixes: dict[str, set[str]] = {}
    for action in plan.actions:
        if action.kind != "video" or action.normalized_id not in nfo_verified_ids:
            continue
        if not action_verified(action, remote_client):
            continue
        groups.setdefault(action.normalized_id, []).append(action)
        match = _HASH_PREFIX.search(action.source)
        if match:
            prefixes.setdefault(action.normalized_id, set()).add(
                match.group(1).lower()
            )

    changed = 0
    for media_id, actions in groups.items():
        final_manifest = [
            {"remote_path": action.target, "size": int(action.expected_size)}
            for action in sorted(actions, key=lambda item: item.target)
        ]
        canonical_dir = actions[0].target.rsplit("/", 1)[0]
        canonical_basename = PurePosixPath(actions[0].target).stem
        metadata_title = canonical_basename
        prefix = f"{media_id} "
        if metadata_title.startswith(prefix):
            metadata_title = metadata_title[len(prefix) :]

        def txn(con) -> int:
            group_rows = []
            for hash_prefix in prefixes.get(media_id, set()):
                group_rows.extend(
                    con.execute(
                        "select * from torrent_jobs where job_type='upload' and lower(hash) like ? "
                        "and state in ('promotion_wait','cleanup_wait')",
                        (f"{hash_prefix}%",),
                    ).fetchall()
                )
            con.execute(
                "insert or ignore into media_groups(media_group_key,normalized_id,emby_media_dir,created_at,updated_at) values(?,?,?,?,?)",
                (
                    media_id,
                    media_id,
                    f"/media/gcrypt/{media_id}",
                    observed_at,
                    observed_at,
                ),
            )
            group_id = int(
                con.execute(
                    "select id from media_groups where media_group_key=?", (media_id,)
                ).fetchone()["id"]
            )
            count = 0
            for upload in group_rows:
                upload_id = int(upload["id"])
                upload_payload = json.loads(upload["payload_json"] or "{}")
                upload_payload.update(
                    {
                        "remote": canonical_dir,
                        "canonical_remote_verified": True,
                        "final_manifest": final_manifest,
                    }
                )
                con.execute(
                    "update torrent_jobs set state='cleanup_wait',phase='cleanup_wait',payload_json=?,updated_at=? where id=?",
                    (
                        json.dumps(upload_payload, ensure_ascii=False),
                        observed_at,
                        upload_id,
                    ),
                )
                con.execute(
                    "insert or ignore into media_pipeline_runs(upload_manifest_id,media_group_id,state,metadata_policy,metadata_quality,created_at,updated_at,canonical_remote_dir,canonical_basename,canonical_video_manifest_json) "
                    "values(?,?,?,?,?,?,?,?,?,?)",
                    (
                        f"upload-job-{upload_id}",
                        group_id,
                        "SidecarVerified",
                        "sidecar",
                        "migrated",
                        observed_at,
                        observed_at,
                        canonical_dir,
                        canonical_basename,
                        json.dumps(final_manifest, ensure_ascii=False),
                    ),
                )
                con.execute(
                    "update media_pipeline_runs set state='SidecarVerified',metadata_policy='sidecar',metadata_quality='migrated',"
                    "canonical_remote_dir=?,canonical_basename=?,canonical_video_manifest_json=?,updated_at=? "
                    "where upload_manifest_id=? and media_group_id=?",
                    (
                        canonical_dir,
                        canonical_basename,
                        json.dumps(final_manifest, ensure_ascii=False),
                        observed_at,
                        f"upload-job-{upload_id}",
                        group_id,
                    ),
                )
                for action in actions:
                    con.execute(
                        "insert or ignore into media_promotions(upload_job_id,hash,media_group_id,normalized_id,metadata_title,display_title,"
                        "source_remote,target_remote,expected_size,expected_hashes_json,state,verification_method,verification_result_json,"
                        "created_at,updated_at,verified_at) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            upload_id,
                            upload["hash"],
                            group_id,
                            media_id,
                            metadata_title,
                            canonical_basename,
                            action.source,
                            action.target,
                            action.expected_size,
                            json.dumps(action.expected_hashes, sort_keys=True),
                            "verified",
                            "migration_path_size",
                            '{"verified":true,"mismatches":[]}',
                            observed_at,
                            observed_at,
                            observed_at,
                        ),
                    )
                    con.execute(
                        "update media_promotions set state='verified',verification_method='migration_path_size',"
                        "verification_result_json=?,verified_at=?,updated_at=? "
                        "where upload_job_id=? and source_remote=? and target_remote=?",
                        (
                            '{"verified":true,"mismatches":[]}',
                            observed_at,
                            observed_at,
                            upload_id,
                            action.source,
                            action.target,
                        ),
                    )
                cleanup_payload = {
                    "upload_job_id": upload_id,
                    "hash": upload["hash"],
                    "batch_id": upload["batch_id"],
                    "remote": canonical_dir,
                    "remote_verified": True,
                    "canonical_remote_verified": True,
                    "final_manifest": final_manifest,
                    "cleanup_policy_snapshot": dict(
                        upload_payload.get("cleanup_policy_snapshot") or {}
                    ),
                }
                cleanup = con.execute(
                    "select id from torrent_jobs where job_type='cleanup_full_torrent' and parent_job_id=?",
                    (upload_id,),
                ).fetchone()
                if cleanup:
                    con.execute(
                        "update torrent_jobs set state='queued',payload_json=?,last_stderr_tail=null,next_run_at=null,updated_at=? where id=?",
                        (
                            json.dumps(cleanup_payload, ensure_ascii=False),
                            observed_at,
                            int(cleanup["id"]),
                        ),
                    )
                else:
                    con.execute(
                        "insert into torrent_jobs(hash,batch_id,job_type,state,priority,payload_json,parent_job_id,created_at,updated_at) values(?,?,?,?,?,?,?,?,?)",
                        (
                            upload["hash"],
                            upload["batch_id"],
                            "cleanup_full_torrent",
                            "queued",
                            int(JobPriority.FULL_TORRENT_RELEASE_UPLOAD),
                            json.dumps(cleanup_payload, ensure_ascii=False),
                            upload_id,
                            observed_at,
                            observed_at,
                        ),
                    )
                con.execute(
                    "insert into action_log(ts,hash,job_id,action_type,path,payload_json,status,dry_run) values(?,?,?,?,?,?,?,?)",
                    (
                        observed_at,
                        upload["hash"],
                        upload_id,
                        "reconcile_verified_migration",
                        canonical_dir,
                        json.dumps(
                            {"normalized_id": media_id, "final_manifest": final_manifest},
                            ensure_ascii=False,
                        ),
                        "done",
                        0,
                    ),
                )
                count += 1
            refresh_payload = json.dumps(
                {
                    "media_group_key": media_id,
                    "trigger_state": "CanonicalMigrationVerified",
                },
                ensure_ascii=False,
            )
            emby_dir = f"/media/gcrypt/{media_id}"
            existing_refresh = con.execute(
                "select id,max_run_at from emby_refresh_tasks where emby_media_dir=? and state='queued' order by id limit 1",
                (emby_dir,),
            ).fetchone()
            if existing_refresh:
                con.execute(
                    "update emby_refresh_tasks set earliest_run_at=?,max_run_at=?,payload_json=?,updated_at=? where id=?",
                    (
                        min(
                            int(existing_refresh["max_run_at"] or observed_at + 900),
                            observed_at + 300,
                        ),
                        max(
                            int(existing_refresh["max_run_at"] or 0),
                            observed_at + 900,
                        ),
                        refresh_payload,
                        observed_at,
                        int(existing_refresh["id"]),
                    ),
                )
            else:
                con.execute(
                    "insert into emby_refresh_tasks(emby_media_dir,state,earliest_run_at,max_run_at,payload_json,created_at,updated_at) values(?,?,?,?,?,?,?)",
                    (
                        emby_dir,
                        "queued",
                        observed_at + 300,
                        observed_at + 900,
                        refresh_payload,
                        observed_at,
                        observed_at,
                    ),
                )
            return count

        changed += int(write_transaction(state_db, txn))
    return changed


_JOURNAL_LOCK = threading.Lock()


def _append_journal(path: str | Path, action: MigrationAction, state: str, **extra: Any) -> None:
    journal = Path(path)
    journal.parent.mkdir(parents=True, exist_ok=True)
    record = {**asdict(action), "state": state, **redact(extra)}
    with _JOURNAL_LOCK:
        with journal.open("a", encoding="utf-8", newline="\n") as handle:
            handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
            handle.flush()
            os.fsync(handle.fileno())


def apply_migration(
    plan: MigrationPlan,
    remote_client,
    *,
    journal_path: str | Path,
    batch_size: int | None = None,
    workers: int = 1,
    verify_attempts: int = 6,
    verify_delay_sec: float = 0.0,
) -> MigrationResult:
    actions = plan.actions[: max(0, int(batch_size))] if batch_size is not None else plan.actions
    attempts = max(1, int(verify_attempts))
    base_delay = max(0.0, float(verify_delay_sec))
    latest: dict[str, dict[str, Any]] = {}
    journal = Path(journal_path)
    if journal.exists():
        for line in journal.read_text(encoding="utf-8").splitlines():
            if line.strip():
                record = json.loads(line)
                latest[str(record.get("action_id") or "")] = record

    def verified_stat(
        action: MigrationAction, path: str
    ) -> Mapping[str, Any] | None:
        row = None
        for attempt in range(attempts):
            row = remote_client.stat(path)
            if _verified(action, row):
                return row
            if attempt + 1 < attempts and base_delay > 0:
                time.sleep(base_delay * (2 ** min(attempt, 4)))
        return row

    def process(action: MigrationAction) -> tuple[int, int, int]:
        previous = latest.get(action.action_id)
        if previous and previous.get("state") == "verified":
            target_row = remote_client.stat(action.target)
            if _verified(action, target_row) or (
                action.kind == "nfo" and target_row is not None
            ):
                return 1, 0, 0
        source_row = remote_client.stat(action.source)
        target_row = remote_client.stat(action.target)
        if source_row is None:
            target_row = verified_stat(action, action.target)
            if _verified(action, target_row):
                _append_journal(journal_path, action, "verified", idempotent=True)
                return 1, 0, 0
            _append_journal(journal_path, action, "failed", error="source_absent_or_mismatch")
            return 0, 1, 0
        if not _verified(action, source_row):
            _append_journal(journal_path, action, "failed", error="source_absent_or_mismatch")
            return 0, 1, 0
        if target_row is not None:
            _append_journal(journal_path, action, "failed", error="target_conflict")
            return 0, 1, 0
        _append_journal(journal_path, action, "moving")
        try:
            remote_client.moveto(action.source, action.target)
            target_row = verified_stat(action, action.target)
            if not _verified(action, target_row):
                raise RuntimeError("destination_verification_failed")
        except Exception as exc:
            _append_journal(journal_path, action, "rollback_wait", error=str(exc))
            target_row = verified_stat(action, action.target)
            if _verified(action, target_row):
                _append_journal(
                    journal_path,
                    action,
                    "verified",
                    recovered_after_verification_delay=True,
                )
                return 1, 0, 0
            try:
                source_row = verified_stat(action, action.source)
                if target_row is not None and not _verified(action, source_row):
                    remote_client.moveto(action.target, action.source)
                    source_row = verified_stat(action, action.source)
                if not _verified(action, source_row):
                    raise RuntimeError("rollback_source_verification_failed")
                _append_journal(journal_path, action, "rolled_back")
            except Exception as rollback_exc:
                _append_journal(
                    journal_path,
                    action,
                    "failed",
                    error=str(exc),
                    rollback_error=str(rollback_exc),
                )
            else:
                _append_journal(journal_path, action, "failed", error=str(exc))
            return 0, 1, 0
        _append_journal(journal_path, action, "verified")
        return 1, 0, 0

    worker_count = max(1, int(workers))
    if worker_count == 1:
        results = [process(action) for action in actions]
    else:
        action_groups: dict[str, list[MigrationAction]] = {}
        for action in actions:
            remote_path = action.target.split(":/", 1)[-1]
            target_root = remote_path.split("/", 1)[0]
            action_groups.setdefault(target_root, []).append(action)

        def process_group(group: list[MigrationAction]) -> list[tuple[int, int, int]]:
            return [process(action) for action in group]

        with ThreadPoolExecutor(
            max_workers=worker_count, thread_name_prefix="remote-migration"
        ) as pool:
            grouped_results = list(pool.map(process_group, action_groups.values()))
        results = [result for group in grouped_results for result in group]
    verified = sum(result[0] for result in results)
    failed = sum(result[1] for result in results)
    skipped = sum(result[2] for result in results)
    return MigrationResult(len(actions), verified, failed, skipped)


def rollback_migration(journal_path: str | Path, remote_client) -> MigrationResult:
    records = [
        json.loads(line)
        for line in Path(journal_path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    latest: dict[str, dict[str, Any]] = {}
    for record in records:
        latest[str(record["action_id"])] = record
    verified = failed = skipped = 0
    for record in reversed(list(latest.values())):
        if record.get("state") != "verified":
            skipped += 1
            continue
        action = MigrationAction(
            **{key: record[key] for key in MigrationAction.__dataclass_fields__}
        )
        if remote_client.stat(action.source) is not None:
            skipped += 1
            continue
        target_row = remote_client.stat(action.target)
        if not _verified(action, target_row) and not (
            action.kind == "nfo" and target_row is not None
        ):
            failed += 1
            continue
        try:
            remote_client.moveto(action.target, action.source)
        except Exception:
            failed += 1
        else:
            verified += 1
    return MigrationResult(len(latest), verified, failed, skipped)


def audit_migration(plan: MigrationPlan, remote_client) -> dict[str, int]:
    result = {"verified": 0, "pending": 0, "conflict": 0}
    for action in plan.actions:
        source = remote_client.stat(action.source)
        target = remote_client.stat(action.target)
        if source is None and (
            _verified(action, target) or (action.kind == "nfo" and target is not None)
        ):
            result["verified"] += 1
        elif source is not None and target is None:
            result["pending"] += 1
        else:
            result["conflict"] += 1
    return result


def write_plan_bundle(plan: MigrationPlan, output_dir: str | Path) -> Path:
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    (root / "plan.json").write_text(
        json.dumps(plan.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (root / "review.json").write_text(
        json.dumps([asdict(item) for item in plan.review], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    with (root / "actions.csv").open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(MigrationAction.__dataclass_fields__))
        writer.writeheader()
        for action in plan.actions:
            row = asdict(action)
            row["expected_hashes"] = json.dumps(row["expected_hashes"], sort_keys=True)
            writer.writerow(row)
    summary = {
        "inventory_count": plan.inventory_count,
        "action_count": len(plan.actions),
        "review_count": len(plan.review),
        "action_bytes": sum(action.expected_size for action in plan.actions),
    }
    (root / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return root
