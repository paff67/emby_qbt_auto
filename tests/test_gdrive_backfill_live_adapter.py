#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))


class Proc:
    def __init__(self, returncode=0, stdout="ok"):
        self.returncode = returncode
        self.stdout = stdout


def test_gdrive_backfill_scraper_runs_script_only_in_local_staging_and_returns_uploadworker_artifacts():
    from qbt_orchestrator.integrations.gdrive_backfill import GDriveBackfillScraper

    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        script = root / "javinizer_scrape_one.sh"
        script.write_text("#!/bin/sh\n", encoding="utf-8")
        staging_root = root / "staging"
        calls = []

        def runner(cmd, env, timeout):
            calls.append((cmd, env, timeout))
            work = Path(cmd[1])
            assert work.is_relative_to(staging_root)
            assert cmd == [str(script), str(work), "BBAN-582"]
            assert "gcrypt:" not in " ".join(cmd)
            (work / "BBAN-582.nfo").write_text("<movie/>", encoding="utf-8")
            (work / "BBAN-582-poster.jpg").write_bytes(b"poster")
            (work / ".javinizer_result").write_text("[javinizer] ok\n", encoding="utf-8")
            return Proc(0, "scraped")

        scraper = GDriveBackfillScraper(
            script_path=script,
            staging_root=staging_root,
            remote="gcrypt:",
            runner=runner,
            timeout_sec=123,
        )

        result = scraper.scrape_one("BBAN-582", "manifest-7")

        assert result["status"] == "sidecar_verified"
        assert result["media_group_key"] == "BBAN-582"
        assert result["manifest_id"] == "manifest-7"
        assert Path(result["staging_dir"]).is_relative_to(staging_root)
        assert calls and calls[0][2] == 123
        artifacts = sorted(result["artifacts"], key=lambda x: x["remote"])
        assert [Path(a["local"]).name for a in artifacts] == ["BBAN-582-poster.jpg", "BBAN-582.nfo"]
        assert [a["remote"] for a in artifacts] == [
            "gcrypt:/BBAN-582/BBAN-582-poster.jpg",
            "gcrypt:/BBAN-582/BBAN-582.nfo",
        ]
        assert all(a["size"] > 0 for a in artifacts)


def test_gdrive_backfill_scraper_reports_not_found_without_remote_or_rclone_bypass():
    from qbt_orchestrator.integrations.gdrive_backfill import GDriveBackfillScraper

    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        script = root / "javinizer_scrape_one.sh"
        script.write_text("#!/bin/sh\n", encoding="utf-8")

        def runner(cmd, env, timeout):
            work = Path(cmd[1])
            (work / "scraper.stdout.log").write_text("not found", encoding="utf-8")
            return Proc(2, "not found")

        scraper = GDriveBackfillScraper(script, root / "staging", runner=runner)
        result = scraper.scrape_one("UNKNOWN-404", "manifest-x")

        assert result["status"] == "not_found"
        assert result["artifacts"] == []
        assert "not found" in result["error"]


def test_cli_builds_live_gdrive_backfill_adapter_when_enabled(monkeypatch):
    from qbt_orchestrator.cli import _build_backfill_from_env
    from qbt_orchestrator.integrations.gdrive_backfill import GDriveBackfillScraper

    with tempfile.TemporaryDirectory() as td:
        script = Path(td) / "javinizer_scrape_one.sh"
        script.write_text("#!/bin/sh\n", encoding="utf-8")
        monkeypatch.setenv("QBT_ORCH_BACKFILL_SCRAPER", "1")
        monkeypatch.setenv("QBT_ORCH_BACKFILL_SCRIPT", str(script))
        monkeypatch.setenv("QBT_ORCH_SIDECAR_STAGING_ROOT", str(Path(td) / "staging"))

        adapter = _build_backfill_from_env(os.environ)

        assert isinstance(adapter, GDriveBackfillScraper)
