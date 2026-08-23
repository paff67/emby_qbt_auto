#!/usr/bin/env python3
"""Rewrite live SQLite path references after the gcrypt /av and /other migration."""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
from pathlib import Path
from typing import Iterable


ORCHESTRATOR_COLUMNS = {
    "bot_add_items": ("remote_match_json",),
    "checked_add_requests": ("matched_remote_paths",),
    "emby_refresh_tasks": ("emby_media_dir", "payload_json"),
    "media_groups": ("emby_media_dir",),
    "media_pipeline_runs": (
        "normalize_result_json",
        "canonical_remote_dir",
        "canonical_video_manifest_json",
    ),
    "media_promotions": ("source_remote", "target_remote"),
    "processed_media": ("last_remote_path",),
    "remote_media_index": ("video_path",),
    "sidecar_manifests": ("artifacts_json", "artifact_manifest_json"),
    "torrent_jobs": ("payload_json",),
}

BACKFILL_COLUMNS = {"items": ("remote_dir", "video_path")}


def build_mappings(plan_path: Path) -> list[tuple[str, str]]:
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    mappings: list[tuple[str, str]] = []
    for item in plan["plans"]:
        if item.get("action") == "moveto":
            mappings.append((item["source"], item["target"]))
    for item in plan.get("root_file_plans", []):
        mappings.append((item["source"], item["target"]))

    tangxin_source = "gcrypt:/糖心Vlog-eb348bf68986"
    tangxin_target = "gcrypt:/other/糖心Vlog"
    mappings.extend(
        [
            (f"{tangxin_source}/糖心Vlog", tangxin_target),
            (f"{tangxin_source}/incomplete", f"{tangxin_target}/incomplete"),
        ]
    )

    mount_mappings = []
    for source, target in mappings:
        if source.startswith("gcrypt:/") and target.startswith("gcrypt:/"):
            mount_mappings.append(
                (
                    "/media/gcrypt/" + source.removeprefix("gcrypt:/"),
                    "/media/gcrypt/" + target.removeprefix("gcrypt:/"),
                )
            )
    mappings.extend(mount_mappings)
    return sorted(set(mappings), key=lambda pair: len(pair[0]), reverse=True)


def rewrite_text(value: str, mappings: Iterable[tuple[str, str]]) -> str:
    rewritten = value
    for source, target in mappings:
        # Match a complete directory/file path component, not similarly prefixed
        # staging names such as ABF-217-aaa95fa321dd.
        rewritten = re.sub(
            re.escape(source) + r"(?=$|[/\\\"'])",
            lambda _match, replacement=target: replacement,
            rewritten,
        )
    return rewritten


def existing_columns(connection: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in connection.execute(f'PRAGMA table_info("{table}")')}


def rewrite_database(
    db_path: Path,
    table_columns: dict[str, tuple[str, ...]],
    mappings: list[tuple[str, str]],
) -> dict[str, int]:
    connection = sqlite3.connect(db_path)
    connection.execute("PRAGMA busy_timeout=30000")
    changes: dict[str, int] = {}
    try:
        connection.execute("BEGIN IMMEDIATE")
        for table, requested_columns in table_columns.items():
            available = existing_columns(connection, table)
            for column in requested_columns:
                if column not in available:
                    continue
                rows = connection.execute(
                    f'SELECT rowid, "{column}" FROM "{table}" '
                    f'WHERE "{column}" IS NOT NULL'
                ).fetchall()
                changed = 0
                for rowid, value in rows:
                    if not isinstance(value, str):
                        continue
                    rewritten = rewrite_text(value, mappings)
                    if rewritten == value:
                        continue
                    connection.execute(
                        f'UPDATE "{table}" SET "{column}" = ? WHERE rowid = ?',
                        (rewritten, rowid),
                    )
                    changed += 1
                if changed:
                    changes[f"{table}.{column}"] = changed
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
    return changes


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--orchestrator-db", type=Path, required=True)
    parser.add_argument("--backfill-db", type=Path)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()

    mappings = build_mappings(args.plan)
    report = {
        "mapping_count": len(mappings),
        "orchestrator": rewrite_database(
            args.orchestrator_db, ORCHESTRATOR_COLUMNS, mappings
        ),
    }
    if args.backfill_db and args.backfill_db.exists():
        report["backfill"] = rewrite_database(
            args.backfill_db, BACKFILL_COLUMNS, mappings
        )
    args.report.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
