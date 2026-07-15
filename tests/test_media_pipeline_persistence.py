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


class RecordingNormalizer:
    def __init__(self, result):
        self.result = result
        self.calls = []

    def normalize(self, raw_filename):
        self.calls.append(raw_filename)
        return dict(self.result)


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
                    {"local": f"{staging}/ABC-123-fanart.jpg", "remote": "gcrypt:/ABC-123/ABC-123-fanart.jpg", "size": 300},
                ],
                "artifact_manifest": f"{staging}/artifact_manifest.json",
                "returncode": 0,
                "scraper_log_tail": "[javinizer] ok",
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
        assert first.state == "SidecarUploadQueued"
        assert second.state == "SidecarUploadQueued"
        assert backfill.calls == [("ABC-123", "manifest-1")]

        groups = rows(db, "select * from media_groups")
        assert len(groups) == 1
        assert groups[0]["media_group_key"] == "ABC-123"
        assert groups[0]["emby_media_dir"] == "/media/gcrypt/ABC-123"

        sidecars = rows(db, "select * from sidecar_manifests")
        assert len(sidecars) == 1
        assert sidecars[0]["state"] == "local_sidecar_validated"
        assert sidecars[0]["local_artifact_dir"] == staging
        assert sidecars[0]["artifact_total_bytes"] == 600
        assert json.loads(sidecars[0]["artifact_manifest_json"])["artifact_manifest"] == f"{staging}/artifact_manifest.json"
        assert sidecars[0]["scraper_exit_code"] == 0

        jobs = rows(db, "select * from torrent_jobs order by id")
        assert [j["job_type"] for j in jobs] == ["sidecar_upload", "sidecar_upload", "sidecar_upload"]
        payloads = [json.loads(j["payload_json"]) for j in jobs]
        assert payloads[0]["local"].startswith(staging)
        assert payloads[0]["remote"] == "gcrypt:/ABC-123/ABC-123.nfo"
        assert payloads[0]["full_torrent"] is False

        assert rows(db, "select count(*) as n from emby_refresh_tasks")[0]["n"] == 0


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


def test_persistent_media_pipeline_uses_filename_normalizer_for_grouping_and_scrape_id():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.media import MediaPipelineService, UploadedFile

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        staging = "/var/lib/qbt-orchestrator/sidecar-staging/BBAN-582"
        normalizer = RecordingNormalizer(
            {
                "normalized_id": "BBAN-582",
                "confidence": 0.95,
                "raw_basename": "489155.com@BBAN-582",
                "reason": "domain_prefix_removed_and_standard_jav_id_matched",
            }
        )
        backfill = RecordingBackfill(
            {
                "status": "sidecar_verified",
                "staging_dir": staging,
                "artifacts": [
                    {"local": f"{staging}/movie.nfo", "remote": "gcrypt:/BBAN-582/movie.nfo", "size": 100},
                    {"local": f"{staging}/poster.jpg", "remote": "gcrypt:/BBAN-582/poster.jpg", "size": 200},
                    {"local": f"{staging}/fanart.jpg", "remote": "gcrypt:/BBAN-582/fanart.jpg", "size": 300},
                ],
                "artifact_manifest": f"{staging}/artifact_manifest.json",
                "returncode": 0,
            }
        )
        service = MediaPipelineService(db, backfill=backfill, normalizer=normalizer, now=lambda: 4000)

        result = service.handle_upload_verified(
            "manifest-normalized",
            [UploadedFile("gcrypt:/raw-upload/489155.com@BBAN-582.mp4", size=1024**3, duration_sec=120)],
        )

        assert result.media_group_key == "BBAN-582"
        assert result.state == "SidecarUploadQueued"
        assert normalizer.calls == ["489155.com@BBAN-582.mp4"]
        assert backfill.calls == [("BBAN-582", "manifest-normalized")]

        groups = rows(db, "select media_group_key,normalized_id,emby_media_dir from media_groups")
        assert groups == [{"media_group_key": "BBAN-582", "normalized_id": "BBAN-582", "emby_media_dir": "/media/gcrypt/BBAN-582"}]
        run = rows(db, "select state,metadata_policy,metadata_quality,passthrough_reason,normalize_result_json from media_pipeline_runs")[0]
        assert run["state"] == "SidecarUploadQueued"
        assert run["metadata_policy"] == "sidecar"
        assert run["metadata_quality"] == "normalized"
        assert run["passthrough_reason"] is None
        assert json.loads(run["normalize_result_json"])["reason"] == "domain_prefix_removed_and_standard_jav_id_matched"
        assert rows(db, "select count(*) as n from torrent_jobs where job_type='sidecar_upload'")[0]["n"] == 3
        assert rows(db, "select count(*) as n from emby_refresh_tasks")[0]["n"] == 0


def test_verified_ingest_enqueues_canonical_promotion_and_colocated_sidecars():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.media import MediaPipelineService, UploadedFile

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        staging = "/var/lib/qbt-orchestrator/sidecar-staging/WAAA-614"
        normalizer = RecordingNormalizer(
            {
                "normalized_id": "WAAA-614",
                "confidence": 0.95,
                "raw_basename": "489155.com@WAAA-614",
                "reason": "domain_prefix_removed_and_standard_jav_id_matched",
            }
        )
        backfill = RecordingBackfill(
            {
                "status": "sidecar_verified",
                "staging_dir": staging,
                "normalized_id": "WAAA-614",
                "metadata_title": "影片名称",
                "display_title": "WAAA-614 影片名称",
                "canonical_basename": "WAAA-614 影片名称",
                "canonical_remote_dir": "gcrypt:/WAAA-614",
                "artifacts": [
                    {"local": f"{staging}/WAAA-614 影片名称.nfo", "remote": "gcrypt:/WAAA-614/WAAA-614 影片名称.nfo", "size": 100},
                    {"local": f"{staging}/WAAA-614 影片名称-poster.jpg", "remote": "gcrypt:/WAAA-614/WAAA-614 影片名称-poster.jpg", "size": 200},
                    {"local": f"{staging}/WAAA-614 影片名称-fanart.jpg", "remote": "gcrypt:/WAAA-614/WAAA-614 影片名称-fanart.jpg", "size": 300},
                ],
            }
        )
        service = MediaPipelineService(
            db,
            backfill=backfill,
            normalizer=normalizer,
            now=lambda: 4_200,
        )

        result = service.handle_upload_verified(
            "upload-job-7",
            [
                UploadedFile(
                    "gcrypt:/WAAA-614-8cfce204ec0e/489155.com@WAAA-614.mp4",
                    size=5_542_877_598,
                    duration_sec=120,
                )
            ],
            upload_job_id=7,
            torrent_hash="8cfce204",
        )

        assert result.media_group_key == "WAAA-614"
        assert result.state == "SidecarUploadQueued"
        promotion = rows(db, "select * from media_promotions")[0]
        assert promotion["source_remote"] == "gcrypt:/WAAA-614-8cfce204ec0e/489155.com@WAAA-614.mp4"
        assert promotion["target_remote"] == "gcrypt:/WAAA-614/WAAA-614 影片名称.mp4"
        assert promotion["expected_size"] == 5_542_877_598

        payloads = [
            json.loads(row["payload_json"])
            for row in rows(
                db,
                "select payload_json from torrent_jobs where job_type='sidecar_upload' order by id",
            )
        ]
        assert [payload["remote"] for payload in payloads] == [
            "gcrypt:/WAAA-614/WAAA-614 影片名称.nfo",
            "gcrypt:/WAAA-614/WAAA-614 影片名称-poster.jpg",
            "gcrypt:/WAAA-614/WAAA-614 影片名称-fanart.jpg",
        ]
        run = rows(
            db,
            "select canonical_remote_dir,canonical_basename,canonical_video_manifest_json from media_pipeline_runs",
        )[0]
        assert run["canonical_remote_dir"] == "gcrypt:/WAAA-614"
        assert run["canonical_basename"] == "WAAA-614 影片名称"
        assert json.loads(run["canonical_video_manifest_json"]) == [
            {
                "remote_path": "gcrypt:/WAAA-614/WAAA-614 影片名称.mp4",
                "size": 5_542_877_598,
            }
        ]
        assert rows(db, "select * from emby_refresh_tasks") == []


def test_media_pipeline_propagates_passthrough_policy_to_sidecar_upload_jobs():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.media import MediaPipelineService, UploadedFile

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        staging = "/var/lib/qbt-orchestrator/sidecar-staging/ABC-123"
        backfill = RecordingBackfill(
            {
                "status": "sidecar_verified",
                "staging_dir": staging,
                "artifacts": [
                    {"local": f"{staging}/movie.nfo", "remote": "gcrypt:/ABC-123/movie.nfo", "size": 100},
                    {"local": f"{staging}/poster.jpg", "remote": "gcrypt:/ABC-123/poster.jpg", "size": 200},
                    {"local": f"{staging}/fanart.jpg", "remote": "gcrypt:/ABC-123/fanart.jpg", "size": 300},
                ],
            }
        )
        service = MediaPipelineService(db, backfill=backfill, allow_unrecognized_passthrough=False, now=lambda: 4100)

        result = service.handle_upload_verified(
            "manifest-sidecar-policy",
            [UploadedFile("gcrypt:/ABC-123/ABC-123.mp4", size=1024**3, duration_sec=120)],
        )

        assert result.state == "SidecarUploadQueued"
        payloads = [json.loads(row["payload_json"]) for row in rows(db, "select payload_json from torrent_jobs order by id")]
        assert payloads
        assert {payload["allow_unrecognized_passthrough"] for payload in payloads} == {False}


def test_persistent_media_pipeline_low_confidence_normalize_passthrough_skips_scrape():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.media import MediaPipelineService, UploadedFile

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        normalizer = RecordingNormalizer({"normalized_id": "UNKNOWN", "confidence": 0.2, "reason": "no_jav_id"})
        backfill = RecordingBackfill({"status": "sidecar_verified", "artifacts": [{"local": "/tmp/a.nfo", "remote": "gcrypt:/UNKNOWN/a.nfo", "size": 1}]})
        service = MediaPipelineService(db, backfill=backfill, normalizer=normalizer, min_normalize_confidence=0.8, now=lambda: 5000)

        result = service.handle_upload_verified(
            "manifest-low",
            [UploadedFile("gcrypt:/raw-upload/random_home_video.mp4", size=1024**3, duration_sec=120)],
        )

        assert result.state == "PassthroughAllowed"
        assert normalizer.calls == ["random_home_video.mp4"]
        assert backfill.calls == []
        run = rows(db, "select state,metadata_policy,metadata_quality,passthrough_reason,normalize_confidence from media_pipeline_runs")[0]
        assert run == {
            "state": "PassthroughAllowed",
            "metadata_policy": "passthrough",
            "metadata_quality": "raw",
            "passthrough_reason": "normalize_low_confidence",
            "normalize_confidence": 0.2,
        }
        assert rows(db, "select count(*) as n from torrent_jobs")[0]["n"] == 0
        assert rows(db, "select emby_media_dir from emby_refresh_tasks") == [{"emby_media_dir": "/media/gcrypt/raw-upload"}]


def test_persistent_media_pipeline_requires_core_sidecar_artifacts_before_upload_job():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.media import MediaPipelineService, UploadedFile

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        staging = "/var/lib/qbt-orchestrator/sidecar-staging/BBAN-582"
        normalizer = RecordingNormalizer({"normalized_id": "BBAN-582", "confidence": 0.95, "reason": "standard_jav_id_matched"})
        backfill = RecordingBackfill(
            {
                "status": "sidecar_verified",
                "staging_dir": staging,
                "artifacts": [
                    {"local": f"{staging}/movie.nfo", "remote": "gcrypt:/BBAN-582/movie.nfo", "size": 100},
                    {"local": f"{staging}/poster.jpg", "remote": "gcrypt:/BBAN-582/poster.jpg", "size": 200},
                ],
                "artifact_manifest": f"{staging}/artifact_manifest.json",
                "returncode": 0,
            }
        )
        service = MediaPipelineService(db, backfill=backfill, normalizer=normalizer, now=lambda: 6000)

        result = service.handle_upload_verified(
            "manifest-missing-fanart",
            [UploadedFile("gcrypt:/BBAN-582/BBAN-582.mp4", size=1024**3, duration_sec=120)],
        )

        assert result.state == "PassthroughAllowed"
        assert backfill.calls == [("BBAN-582", "manifest-missing-fanart")]
        sidecars = rows(db, "select state,artifacts_json,artifact_manifest_json from sidecar_manifests")
        assert len(sidecars) == 1
        assert sidecars[0]["state"] == "sidecar_verify_failed"
        assert "fanart" in json.loads(sidecars[0]["artifact_manifest_json"])["missing_outputs"]
        assert rows(db, "select count(*) as n from torrent_jobs where job_type='sidecar_upload'")[0]["n"] == 0
        run = rows(db, "select state,metadata_policy,passthrough_reason from media_pipeline_runs")[0]
        assert run == {"state": "PassthroughAllowed", "metadata_policy": "passthrough", "passthrough_reason": "sidecar_verify_failed"}


def test_media_pipeline_job_runner_processes_recoverable_jobs():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.media import MediaPipelineJobRunner, MediaPipelineService
    from qbt_orchestrator.runtime import TorrentJobRepository

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        backfill = RecordingBackfill({"status": "not_found", "artifacts": []})
        repo = TorrentJobRepository(db, now=lambda: 3000)
        job_id = repo.enqueue(
            "h1",
            None,
            "media_pipeline",
            {
                "upload_manifest_id": "manifest-h1",
                "files": [{"remote_path": "gcrypt:/ABC-123/ABC-123.mp4", "size": 1024**3, "duration_sec": 120}],
            },
            priority=1,
        )
        service = MediaPipelineService(db, backfill=backfill, now=lambda: 3000)
        runner = MediaPipelineJobRunner(repo, service)

        assert runner.run_next() == job_id
        assert repo.get(job_id)["state"] == "done"
        assert rows(db, "select media_group_key from media_groups") == [{"media_group_key": "ABC-123"}]
        assert rows(db, "select emby_media_dir from emby_refresh_tasks") == [{"emby_media_dir": "/media/gcrypt/ABC-123"}]


def test_emby_refresh_worker_calls_precise_media_updated_and_rejects_root_path():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.media import EmbyRefreshWorker
    from tests.fakes import FakeEmby

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        con.execute(
            "insert into emby_refresh_tasks(emby_media_dir,state,earliest_run_at,max_run_at,payload_json,created_at,updated_at) values(?,?,?,?,?,?,?)",
            ("/media/gcrypt/ABC-123", "queued", 3900, 4500, "{}", 3000, 3000),
        )
        con.execute(
            "insert into emby_refresh_tasks(emby_media_dir,state,earliest_run_at,max_run_at,payload_json,created_at,updated_at) values(?,?,?,?,?,?,?)",
            ("/media/gcrypt", "queued", 3900, 4500, "{}", 3000, 3000),
        )
        con.commit()
        con.close()
        emby = FakeEmby()
        worker = EmbyRefreshWorker(db, emby=emby, now=lambda: 4000, media_prefix="/media/gcrypt")

        assert worker.run_next() == 1
        assert emby.refreshes == [{"Updates": [{"Path": "/media/gcrypt/ABC-123", "UpdateType": "Created"}]}]
        assert worker.run_next() == 2

        states = rows(db, "select id,state,last_error from emby_refresh_tasks order by id")
        assert states[0]["state"] == "done"
        assert states[1]["state"] == "blocked"
        assert "too broad" in states[1]["last_error"]


def test_transient_emby_failure_retries_with_backoff_and_attempt_limit():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.media import EmbyRefreshWorker

    class TransientEmby:
        def __init__(self):
            self.calls = 0

        def media_updated(self, path):
            self.calls += 1
            if self.calls == 1:
                raise TimeoutError("emby timeout")
            return {"ok": True}

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        con.execute(
            "insert into emby_refresh_tasks(emby_media_dir,state,earliest_run_at,max_run_at,payload_json,created_at,updated_at) "
            "values('/media/gcrypt/ABC-123','queued',100,500,'{}',1,1)"
        )
        con.commit()
        con.close()
        clock = [200]
        emby = TransientEmby()
        worker = EmbyRefreshWorker(db, emby, now=lambda: clock[0], retry_delay_sec=60)

        assert worker.run_next() == 1
        assert rows(db, "select state,attempts,next_run_at from emby_refresh_tasks") == [
            {"state": "retry_wait", "attempts": 1, "next_run_at": 260}
        ]
        assert worker.run_next() is None

        clock[0] = 261
        assert worker.run_next() == 1
        assert rows(db, "select state,attempts,next_run_at from emby_refresh_tasks") == [
            {"state": "done", "attempts": 2, "next_run_at": 260}
        ]


if __name__ == "__main__":
    inspect = __import__("inspect")
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn) and not inspect.signature(fn).parameters:
            fn()
    print("ok")
