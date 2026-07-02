#!/usr/bin/env python3
from __future__ import annotations

import json
import sqlite3
import tempfile
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))


class RecordingBackfill:
    def __init__(self, result):
        self.result = result
        self.calls = []

    def scrape_one(self, media_group_key, manifest_id):
        self.calls.append((media_group_key, manifest_id))
        return self.result


def rows(db: Path, sql: str):
    con = sqlite3.connect(db)
    con.row_factory = sqlite3.Row
    out = [dict(r) for r in con.execute(sql)]
    con.close()
    return out


def test_persistent_media_pipeline_dedupes_multi_cd_sidecar_and_emby_refresh():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.media import MediaPipelineService, UploadedFile

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        clock = {"now": 1000}
        staging = "/var/lib/qbt-orchestrator/sidecar-staging/ABC-123"
        backfill = RecordingBackfill(
            {
                "status": "sidecar_verified",
                "staging_dir": staging,
                "artifacts": [
                    {"local": f"{staging}/ABC-123.nfo", "remote": "gcrypt:/ABC-123/ABC-123.nfo", "size": 100},
                    {"local": f"{staging}/ABC-123-poster.jpg", "remote": "gcrypt:/ABC-123/ABC-123-poster.jpg", "size": 200},
                ],
            }
        )
        service = MediaPipelineService(db, backfill=backfill, now=lambda: clock["now"])
        files = [
            UploadedFile("gcrypt:/ABC-123/ABC-123-CD1.mp4", size=1024**3, duration_sec=120),
            UploadedFile("gcrypt:/ABC-123/ABC-123-CD2.mp4", size=1024**3, duration_sec=120),
        ]

        first = service.handle_upload_verified("manifest-1", files)
        clock["now"] = 1100
        second = service.handle_upload_verified("manifest-2", [files[1]])

        assert first.media_group_key == "ABC-123"
        assert second.media_group_key == "ABC-123"
        assert first.state == "SidecarVerified"
        assert second.state == "SidecarVerified"
        assert backfill.calls == [("ABC-123", "manifest-1")]

        groups = rows(db, "select * from media_groups")
        assert len(groups) == 1
        assert groups[0]["media_group_key"] == "ABC-123"
        assert groups[0]["emby_media_dir"] == "/media/gcrypt/ABC-123"

        sidecars = rows(db, "select * from sidecar_manifests")
        assert len(sidecars) == 1
        assert sidecars[0]["state"] == "sidecar_verified"

        jobs = rows(db, "select * from torrent_jobs order by id")
        assert [j["job_type"] for j in jobs] == ["sidecar_upload", "sidecar_upload"]
        payloads = [json.loads(j["payload_json"]) for j in jobs]
        assert payloads[0]["local"].startswith(staging)
        assert payloads[0]["remote"] == "gcrypt:/ABC-123/ABC-123.nfo"
        assert payloads[0]["full_torrent"] is False

        refresh = rows(db, "select * from emby_refresh_tasks")
        assert len(refresh) == 1
        assert refresh[0]["emby_media_dir"] == "/media/gcrypt/ABC-123"
        assert refresh[0]["earliest_run_at"] == 1400
        assert refresh[0]["max_run_at"] == 1900


def test_persistent_media_pipeline_allows_unknown_metadata_passthrough_but_blocks_junk():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.media import MediaPipelineService, UploadedFile

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        backfill = RecordingBackfill({"status": "not_found", "artifacts": []})
        service = MediaPipelineService(db, backfill=backfill, now=lambda: 2000)

        allowed = service.handle_upload_verified(
            "manifest-ok",
            [UploadedFile("gcrypt:/UNKNOWN-001/UNKNOWN-001.mp4", size=1024**3, duration_sec=None)],
        )
        blocked = service.handle_upload_verified(
            "manifest-junk",
            [UploadedFile("gcrypt:/AD/最新地址.url", size=1024, duration_sec=None)],
        )

        assert allowed.media_group_key == "UNKNOWN-001"
        assert allowed.state == "PassthroughAllowed"
        assert blocked.state == "content_gate_failed"
        assert rows(db, "select count(*) as n from emby_refresh_tasks")[0]["n"] == 1
        assert rows(db, "select count(*) as n from torrent_jobs")[0]["n"] == 0
        assert backfill.calls == [("UNKNOWN-001", "manifest-ok")]


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
    print("ok")
