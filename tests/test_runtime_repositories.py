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


def test_observability_store_persists_redacted_events_actions_and_trace():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.runtime import ObservabilityStore

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        obs = ObservabilityStore(db)
        obs.event("warning", "telegram", "bot_rejected", "token " + "123456:" + "secret-token", {"magnet": "mag" + "net:?xt=urn:btih:" + "A" * 40}, hash="h1")
        obs.action(hash="h1", job_id=7, action_type="qbt_post", path="/api/v2/torrents/stop", payload={"hashes": "h1"}, status="succeeded", dry_run=False)
        trace = obs.trace("h1")
        dumped = json.dumps(trace)
        assert "secret-token" not in dumped
        assert "magnet:?" not in dumped
        assert trace["actions"][0]["path"] == "/api/v2/torrents/stop"
        assert trace["events"][0]["event_type"] == "bot_rejected"


def test_upload_job_runner_claims_job_updates_done_and_verify_pending():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.runtime import TorrentJobRepository, UploadJobRunner
    from tests.fakes import FakeExecutor, FakeRclone

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        repo = TorrentJobRepository(db)
        good = repo.enqueue("h1", 1, "upload", {"local": "/tmp/a.mp4", "remote": "gcrypt:/A/a.mp4", "size": 100, "full_torrent": True}, priority=1)
        bad = repo.enqueue("h2", 2, "upload", {"local": "/tmp/b.mp4", "remote": "gcrypt:/B/b.mp4", "size": 100, "full_torrent": True}, priority=2)

        runner = UploadJobRunner(repo, FakeRclone(copy_ok=True, remote_sizes={"gcrypt:/A/a.mp4": 100, "gcrypt:/B/b.mp4": 99}), FakeExecutor())
        assert runner.run_next() == good
        assert repo.get(good)["state"] == "done"
        assert runner.run_next() == bad
        assert repo.get(bad)["state"] == "verify_pending"
        assert repo.get(bad)["last_stderr_tail"] == "remote size mismatch"


def test_command_processor_executes_safe_commands_and_requires_cleanup_approval():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.runtime import BotCommandRepository, CommandProcessor
    from tests.fakes import FakeExecutor

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        commands = BotCommandRepository(db)
        commands.insert_command("c1", 100, 2, "pause", {"args": ["h1"]})
        commands.insert_command("c2", 100, 2, "resume", {"args": ["h1"]})
        commands.insert_command("c3", 100, 3, "cleanup", {"args": ["h2"]})
        executor = FakeExecutor()
        processor = CommandProcessor(commands, executor)

        assert processor.run_next() == "c1"
        assert processor.run_next() == "c2"
        assert processor.run_next() == "c3"
        assert executor.posts == [
            ("/api/v2/torrents/stop", {"hashes": "h1"}),
            ("/api/v2/torrents/start", {"hashes": "h1"}),
        ]
        assert commands.get("c3")["state"] == "approval_required"
        assert commands.pending_approvals()[0]["action"] == "cleanup"


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
    print("ok")

