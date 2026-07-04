#!/usr/bin/env python3
from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))


class FakeExecutor:
    def __init__(self):
        self.posts = []

    def qbt_post(self, path, payload):
        self.posts.append((path, payload))


class FakeQbt:
    def __init__(self, files_by_hash):
        self.files_by_hash = files_by_hash
        self.file_calls = []
        self.rids = []

    def torrent_files(self, h):
        self.file_calls.append(h)
        return list(self.files_by_hash.get(h, []))

    def get_maindata(self, rid):
        self.rids.append(rid)
        return {
            "rid": rid + 1,
            "full_update": True,
            "torrents": {
                "h1": {
                    "hash": "h1",
                    "name": "dori-136.torrent",
                    "category": "",
                    "tags": "metadata-timeout, observe, precheck",
                    "state": "stoppedDL",
                    "amount_left": 1,
                    "size": 3_700_000_000,
                    "progress": 0.98,
                    "has_metadata": True,
                    "content_path": "/downloads/incomplete/dori-136",
                }
            },
            "server_state": {},
        }


def _dori_files():
    return [
        {"index": 0, "name": "dori-136/dori-136.mp4", "size": 3_619_608_573, "progress": 1.0, "priority": 1},
        {"index": 1, "name": "dori-136/台 妹 子 線 上 現 場 直 播 各 式 花 式 表 演.mp4", "size": 23_888_566, "progress": 0.003, "priority": 1},
        {"index": 2, "name": "dori-136/最 新 位 址 獲 取.txt", "size": 136, "progress": 0, "priority": 1},
        {"index": 3, "name": "dori-136/社 區 最 新 情 報.mp4", "size": 15_089_802, "progress": 0, "priority": 1},
        {"index": 4, "name": "dori-136/聚 合 全 網 H 直 播.html", "size": 145, "progress": 0, "priority": 1},
    ]


def test_observe_metadata_ready_promotes_to_auto_and_drops_ad_files():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.observe_promotion import ObservePromotionService

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        qbt = FakeQbt({"h1": _dori_files()})
        executor = FakeExecutor()
        service = ObservePromotionService(db, qbt, executor, dry_run=False, now=lambda: 1000)

        result = service.promote_ready(
            {
                "h1": {
                    "hash": "h1",
                    "name": "dori-136.torrent",
                    "category": "",
                    "tags": "metadata-timeout, observe, precheck",
                    "has_metadata": True,
                }
            },
            sync_healthy=True,
        )

        assert result["promoted"] == ["h1"]
        assert result["dropped_indices"]["h1"] == [1, 2, 3, 4]
        assert qbt.file_calls == ["h1"]
        assert executor.posts == [
            ("/api/v2/torrents/filePrio", {"hash": "h1", "id": "1|2|3|4", "priority": "0"}),
            ("/api/v2/torrents/filePrio", {"hash": "h1", "id": "0", "priority": "1"}),
            ("/api/v2/torrents/removeTags", {"hashes": "h1", "tags": "metadata-timeout,observe,precheck"}),
            ("/api/v2/torrents/addTags", {"hashes": "h1", "tags": "auto,checked"}),
            ("/api/v2/torrents/setCategory", {"hashes": "h1", "category": "auto"}),
            ("/api/v2/torrents/setForceStart", {"hashes": "h1", "value": "false"}),
            ("/api/v2/torrents/stop", {"hashes": "h1"}),
        ]
        con = sqlite3.connect(db)
        event = con.execute("select component,event_type,hash from events_v2 where component='observe_promotion'").fetchone()
        con.close()
        assert event == ("observe_promotion", "promoted", "h1")


def test_observe_promotion_keeps_non_ad_txt_selected():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.observe_promotion import ObservePromotionService

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        qbt = FakeQbt(
            {
                "h1": [
                    {"index": 0, "name": "ABC-123.mp4", "size": 2_000_000_000, "progress": 0.2, "priority": 1},
                    {"index": 1, "name": "password.txt", "size": 80, "progress": 0, "priority": 1},
                    {"index": 2, "name": "聚合全網H直播.html", "size": 120, "progress": 0, "priority": 1},
                ]
            }
        )
        executor = FakeExecutor()
        service = ObservePromotionService(db, qbt, executor, dry_run=False, now=lambda: 1000)

        result = service.promote_ready(
            {"h1": {"hash": "h1", "name": "ABC-123", "category": "", "tags": "observe", "has_metadata": True}},
            sync_healthy=True,
        )

        assert result["promoted"] == ["h1"]
        assert result["dropped_indices"]["h1"] == [2]
        assert ("/api/v2/torrents/filePrio", {"hash": "h1", "id": "0|1", "priority": "1"}) in executor.posts


def test_observe_promotion_skips_without_metadata_or_unhealthy_sync():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.observe_promotion import ObservePromotionService

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        qbt = FakeQbt({"h1": _dori_files()})
        executor = FakeExecutor()
        service = ObservePromotionService(db, qbt, executor, dry_run=False, now=lambda: 1000)

        unhealthy = service.promote_ready({"h1": {"hash": "h1", "tags": "observe", "has_metadata": True}}, sync_healthy=False)
        no_metadata = service.promote_ready({"h1": {"hash": "h1", "tags": "observe", "has_metadata": False}}, sync_healthy=True)

        assert unhealthy["suspended"] is True
        assert no_metadata["skipped"]["h1"] == "metadata_not_ready"
        assert qbt.file_calls == []
        assert executor.posts == []


def test_daemon_file_batch_loop_runs_observe_promotion_before_managed_batching():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.observe_promotion import ObservePromotionService
    from qbt_orchestrator.service import DaemonRuntime

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        qbt = FakeQbt({"h1": _dori_files()})
        executor = FakeExecutor()
        observe_service = ObservePromotionService(db, qbt, executor, dry_run=False, now=lambda: 1000)
        daemon = DaemonRuntime(
            state_db=db,
            qbt=qbt,
            executor=executor,
            free_bytes_provider=lambda: 6 * 1024**3,
            dry_run=False,
            safety_interval=0,
            observe_promotion_service=observe_service,
            file_batch_dry_run=True,
            planner_dry_run=True,
        )

        daemon.run(max_safety_ticks=1)

        assert ("/api/v2/torrents/addTags", {"hashes": "h1", "tags": "auto,checked"}) in executor.posts
        con = sqlite3.connect(db)
        loop_payload = con.execute("select data_json from events_v2 where component='file_batch' and event_type='loop_tick' order by id desc limit 1").fetchone()[0]
        con.close()
        assert "observe_promotion" in loop_payload


if __name__ == "__main__":
    inspect = __import__("inspect")
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn) and not inspect.signature(fn).parameters:
            fn()
    print("ok")
