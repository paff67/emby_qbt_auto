#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import asdict
from pathlib import Path, PurePosixPath

HERE = Path(__file__).resolve()
for candidate in (HERE.parents[2] / "src", Path("/opt/qbt-orchestrator/current/src")):
    if candidate.exists():
        sys.path.insert(0, str(candidate))
        break

from qbt_orchestrator.integrations.rclone import RcloneClient
from qbt_orchestrator.naming import canonical_media_name
from qbt_orchestrator.remote_migration import (
    MigrationPlan,
    action_verified,
    apply_migration,
    audit_migration,
    build_migration_plan,
    reconcile_verified_migrations,
    render_canonical_nfo,
    rollback_migration,
    write_plan_bundle,
)


def _client(ns) -> RcloneClient:
    return RcloneClient(
        config_path=ns.rclone_config,
        transfers=1,
        checkers=2,
    )


def _load_json(path: str | Path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _load_plan(path: str | Path) -> MigrationPlan:
    return MigrationPlan.from_dict(_load_json(path))


def _title(titles: dict, media_id: str) -> str:
    value = titles.get(media_id) or titles.get(media_id.lower()) or {}
    if isinstance(value, str):
        return value.strip()
    return str(
        value.get("title")
        or value.get("metadata_title")
        or value.get("original_title")
        or ""
    ).strip()


def _append_jsonl(path: Path, record: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def rewrite_verified_nfos(
    plan: MigrationPlan,
    titles: dict,
    remote: RcloneClient,
    *,
    report_dir: Path,
) -> set[str]:
    """Back up then replace NFOs only for already verified video migrations."""
    report_dir.mkdir(parents=True, exist_ok=True)
    backup_dir = report_dir / "nfo-backups"
    generated_dir = report_dir / "nfo-generated"
    backup_dir.mkdir(parents=True, exist_ok=True)
    generated_dir.mkdir(parents=True, exist_ok=True)
    journal = report_dir / "nfo-rewrite.jsonl"
    latest: dict[str, dict] = {}
    if journal.exists():
        for line in journal.read_text(encoding="utf-8").splitlines():
            if line.strip():
                record = json.loads(line)
                latest[str(record.get("normalized_id") or "")] = record
    verified_ids: set[str] = set()
    for media_id in sorted({a.normalized_id for a in plan.actions if a.kind == "video"}):
        videos = [a for a in plan.actions if a.kind == "video" and a.normalized_id == media_id]
        if not videos or not all(action_verified(action, remote) for action in videos):
            continue
        title = _title(titles, media_id)
        if not title:
            continue
        canonical = canonical_media_name(media_id, title)
        canonical_dir = videos[0].target.rsplit("/", 1)[0]
        nfo_target = f"{canonical_dir}/{canonical.canonical_basename}.nfo"
        previous = latest.get(media_id)
        if previous and previous.get("state") == "verified" and previous.get("target") == nfo_target:
            current = remote.stat(nfo_target)
            if current is not None and int(current.get("Size") or 0) == int(previous.get("size") or -1):
                verified_ids.add(media_id)
                continue
        nfo_actions = [a for a in plan.actions if a.kind == "nfo" and a.normalized_id == media_id]
        restore_remote = nfo_actions[0].source if nfo_actions else nfo_target
        existing = remote.stat(nfo_target)
        backup_path = backup_dir / f"{media_id}.nfo"
        old_bytes = None
        if existing is not None:
            remote.copyto(nfo_target, str(backup_path))
            old_bytes = backup_path.read_bytes()
        rendered = render_canonical_nfo(old_bytes, media_id, title)
        # Keep the local staging component short. The remote canonical basename can
        # legitimately exceed Linux's 255-byte filename limit when it contains
        # multibyte titles, even though Google Drive accepts that target name.
        generated = generated_dir / f"{media_id}.nfo"
        generated.write_bytes(rendered)
        _append_jsonl(
            journal,
            {
                "normalized_id": media_id,
                "state": "uploading",
                "target": nfo_target,
                "restore_remote": restore_remote,
                "backup_path": str(backup_path) if backup_path.exists() else "",
            },
        )
        remote.copyto(str(generated), nfo_target)
        result = remote.stat(nfo_target)
        if result is None or int(result.get("Size") or 0) != len(rendered):
            _append_jsonl(
                journal,
                {"normalized_id": media_id, "state": "failed", "target": nfo_target},
            )
            continue
        _append_jsonl(
            journal,
            {
                "normalized_id": media_id,
                "state": "verified",
                "target": nfo_target,
                "restore_remote": restore_remote,
                "backup_path": str(backup_path) if backup_path.exists() else "",
                "size": len(rendered),
            },
        )
        verified_ids.add(media_id)
    return verified_ids


def restore_nfo_backups(path: Path, remote: RcloneClient) -> dict[str, int]:
    latest = {}
    if not path.exists():
        return {"restored": 0, "skipped": 0}
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            record = json.loads(line)
            latest[record.get("normalized_id")] = record
    restored = skipped = 0
    for record in latest.values():
        backup = Path(str(record.get("backup_path") or ""))
        restore_remote = str(record.get("restore_remote") or "")
        if record.get("state") != "verified" or not backup.is_file() or not restore_remote:
            skipped += 1
            continue
        remote.copyto(str(backup), restore_remote)
        restored += 1
    return {"restored": restored, "skipped": skipped}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Safely canonicalize Emby media layout")
    parser.add_argument("--rclone-config", default="/root/.config/rclone/rclone.conf")
    parser.add_argument("--remote", default="gcrypt:")
    sub = parser.add_subparsers(dest="command", required=True)

    plan = sub.add_parser("plan")
    plan.add_argument("--titles", required=True)
    plan.add_argument("--output-dir", required=True)
    plan.add_argument("--min-confidence", type=float, default=0.95)

    apply = sub.add_parser("apply")
    apply.add_argument("--plan", required=True)
    apply.add_argument("--titles", required=True)
    apply.add_argument("--journal", required=True)
    apply.add_argument("--report-dir", required=True)
    apply.add_argument("--batch-size", type=int)
    apply.add_argument("--workers", type=int, default=1)
    apply.add_argument("--verify-attempts", type=int, default=6)
    apply.add_argument("--verify-delay-sec", type=float, default=2.0)
    apply.add_argument("--state-db")

    rollback = sub.add_parser("rollback")
    rollback.add_argument("--journal", required=True)
    rollback.add_argument("--nfo-journal")

    audit = sub.add_parser("audit")
    audit.add_argument("--plan", required=True)
    return parser


def main(argv=None) -> int:
    ns = build_parser().parse_args(argv)
    remote = _client(ns)
    if ns.command == "plan":
        inventory = remote.lsjson(ns.remote, recursive=True)
        plan = build_migration_plan(
            inventory,
            _load_json(ns.titles),
            min_confidence=ns.min_confidence,
            remote=ns.remote,
        )
        root = write_plan_bundle(plan, ns.output_dir)
        (root / "inventory.json").write_text(
            json.dumps(inventory, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(json.dumps({"report_dir": str(root), **_load_json(root / "summary.json")}, ensure_ascii=False))
        return 0
    if ns.command == "apply":
        plan = _load_plan(ns.plan)
        result = apply_migration(
            plan,
            remote,
            journal_path=ns.journal,
            batch_size=ns.batch_size,
            workers=ns.workers,
            verify_attempts=ns.verify_attempts,
            verify_delay_sec=ns.verify_delay_sec,
        )
        nfo_ids = rewrite_verified_nfos(
            plan, _load_json(ns.titles), remote, report_dir=Path(ns.report_dir)
        )
        reconciled = 0
        if ns.state_db:
            reconciled = reconcile_verified_migrations(
                ns.state_db,
                plan,
                remote,
                nfo_verified_ids=nfo_ids,
                now=int(time.time()),
            )
        print(json.dumps({**asdict(result), "nfo_verified": len(nfo_ids), "jobs_reconciled": reconciled}, ensure_ascii=False))
        return 1 if result.failed else 0
    if ns.command == "rollback":
        result = rollback_migration(ns.journal, remote)
        nfo = restore_nfo_backups(Path(ns.nfo_journal), remote) if ns.nfo_journal else {"restored": 0, "skipped": 0}
        print(json.dumps({**asdict(result), "nfo": nfo}, ensure_ascii=False))
        return 1 if result.failed else 0
    plan = _load_plan(ns.plan)
    print(json.dumps(audit_migration(plan, remote), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
