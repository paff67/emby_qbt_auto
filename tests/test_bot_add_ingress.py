from __future__ import annotations

from urllib.parse import parse_qs, urlsplit

import pytest

from qbt_orchestrator.db import migrate, readonly_connect


class Clock:
    def __init__(self, value: int = 1_900_000_000):
        self.value = value

    def __call__(self) -> int:
        return self.value

    def advance(self, seconds: int) -> None:
        self.value += seconds


class FakeHttpResolver:
    def __init__(self, *, infohash_v1: str = "b" * 40, fail: str | None = None):
        self.infohash_v1 = infohash_v1
        self.fail = fail
        self.calls: list[str] = []

    def resolve(self, url: str):
        from qbt_orchestrator.download_links import (
            LinkResolutionError,
            ResolvedDownloadLink,
            parse_download_link,
        )

        self.calls.append(url)
        if self.fail:
            raise LinkResolutionError(self.fail)
        base = parse_download_link(url)
        return ResolvedDownloadLink(
            kind=base.kind,
            original=base.original,
            redacted=base.redacted,
            input_sha256=base.input_sha256,
            infohash_v1=self.infohash_v1,
            infohash_v2=None,
            metainfo=b"d4:infod6:lengthi1eee",
        )


class FakeProbeGateway:
    def __init__(self):
        self.by_tag: dict[str, dict] = {}
        self.files_by_hash: dict[str, list[dict]] = {}
        self.added: list[tuple[str, str]] = []
        self.stopped: list[str] = []
        self.zeroed: list[str] = []
        self.info_reads: list[str] = []
        self.info_state: str | None = None
        self.info_tags: str | None = None

    def set_snapshots(self, _snapshots):
        return None

    @staticmethod
    def expected_hash(magnet: str):
        query = parse_qs(urlsplit(magnet).query)
        return query["xt"][0].split(":")[-1].lower()

    def add_magnet(self, magnet: str, tag: str, *, guard=None):
        if guard is not None and not guard():
            return False
        self.added.append((magnet, tag))
        torrent_hash = self.expected_hash(magnet)
        self.by_tag.setdefault(
            tag,
            {"hash": torrent_hash, "tags": f"precheck,{tag},hold", "state": "metaDL"},
        )
        self.files_by_hash.setdefault(
            torrent_hash,
            [{"index": 0, "name": "video.mkv", "priority": 1}],
        )
        return True

    def find_by_tag(self, tag: str):
        row = self.by_tag.get(tag)
        return None if row is None else dict(row)

    def find_by_hash(self, torrent_hash: str):
        for row in self.by_tag.values():
            if str(row.get("hash") or "").lower() == torrent_hash.lower():
                return dict(row)
        return None

    @staticmethod
    def metadata_ready(snapshot):
        return "meta" not in str(snapshot.get("state") or "").lower()

    def stop(self, torrent_hash: str, *, guard=None):
        if guard is not None and not guard():
            return False
        self.stopped.append(torrent_hash)
        return True

    def torrent_files(self, torrent_hash: str):
        return [dict(row) for row in self.files_by_hash.get(torrent_hash, [])]

    def torrent_info(self, torrent_hash: str):
        self.info_reads.append(torrent_hash)
        matching = next(
            (
                row
                for row in self.by_tag.values()
                if str(row.get("hash") or "").lower() == torrent_hash.lower()
            ),
            {},
        )
        tags = self.info_tags
        if tags is None:
            tags = str(matching.get("tags") or "")
        state = self.info_state
        if state is None:
            state = str(matching.get("state") or "")
        return {"hash": torrent_hash, "state": state, "tags": tags}

    def zero_file_priorities(self, torrent_hash: str, files, *, guard=None):
        if guard is not None and not guard():
            return False
        self.zeroed.append(torrent_hash)
        for row in self.files_by_hash[torrent_hash]:
            row["priority"] = 0
        return True

    @staticmethod
    def all_priorities_zero(files):
        return bool(files) and all(int(row.get("priority") or 0) == 0 for row in files)

    def remove_registration(self, torrent_hash: str, *, guard=None):
        if guard is not None and not guard():
            return False
        return True


def test_ingress_advances_magnet_from_received_to_waiting_probe_slot(tmp_path):
    from qbt_orchestrator.bot_add_ingress import BotAddIngressCoordinator
    from qbt_orchestrator.bot_add_queue import BotAddQueueRepository

    db = tmp_path / "state.sqlite"
    migrate(db)
    clock = Clock()
    queue = BotAddQueueRepository(db, now=clock)
    batch = queue.open_draft("7", "42")
    magnet = "magnet:?" + "xt=urn:btih:" + "a" * 40
    queue.append_message(batch["id"], 1, [magnet])
    queue.submit(batch["id"])
    item = queue.list_items(batch["id"])[0]
    assert item["state"] == "received"

    ingress = BotAddIngressCoordinator(queue, now=clock)
    result = ingress.tick()

    assert result["waiting_probe_slot"] == [item["id"]]
    updated = queue.get_item(item["id"])
    assert updated["state"] == "waiting_probe_slot"
    assert updated["infohash_v1"] == "a" * 40
    assert updated["canonical_identity"] == "a" * 40


def test_ingress_marks_invalid_links_and_resolves_http(tmp_path):
    from qbt_orchestrator.bot_add_ingress import BotAddIngressCoordinator
    from qbt_orchestrator.bot_add_queue import BotAddQueueRepository

    db = tmp_path / "state.sqlite"
    migrate(db)
    clock = Clock()
    queue = BotAddQueueRepository(db, now=clock)
    batch = queue.open_draft("7", "42")
    queue.append_message(
        batch["id"],
        1,
        [
            "magnet:?" + "xt=urn:btih:not-a-hash",
            "https://example.test/file.torrent",
        ],
    )
    queue.submit(batch["id"])
    items = queue.list_items(batch["id"])
    resolver = FakeHttpResolver(infohash_v1="c" * 40)
    ingress = BotAddIngressCoordinator(
        queue, http_resolver=resolver, now=clock
    )

    result = ingress.tick()

    by_id = {item["id"]: queue.get_item(item["id"]) for item in items}
    assert sorted(result["invalid"]) == [items[0]["id"]]
    assert sorted(result["waiting_probe_slot"]) == [items[1]["id"]]
    assert by_id[items[0]["id"]]["state"] == "invalid"
    assert by_id[items[1]["id"]]["state"] == "waiting_probe_slot"
    assert by_id[items[1]["id"]]["infohash_v1"] == "c" * 40
    assert resolver.calls == ["https://example.test/file.torrent"]


def test_ingress_alerts_when_oldest_received_exceeds_threshold(tmp_path):
    from qbt_orchestrator.bot_add_ingress import BotAddIngressCoordinator
    from qbt_orchestrator.bot_add_queue import BotAddQueueRepository
    from qbt_orchestrator.warning_inbox import WarningService

    db = tmp_path / "state.sqlite"
    migrate(db)
    clock = Clock()
    queue = BotAddQueueRepository(db, now=clock)
    warnings = WarningService(db, now=clock)
    batch = queue.open_draft("7", "42")
    queue.append_message(
        batch["id"], 1, ["magnet:?" + "xt=urn:btih:" + "d" * 40]
    )
    queue.submit(batch["id"])
    clock.advance(61)

    class FrozenQueue:
        """Keep items stuck in received while still exposing age via DB."""

        def __init__(self, inner):
            self.inner = inner
            self.state_db = inner.state_db
            self._now = inner._now

        def transition_item(self, *args, **kwargs):
            raise RuntimeError("forced stall")

        def get_item(self, *args, **kwargs):
            return self.inner.get_item(*args, **kwargs)

    ingress = BotAddIngressCoordinator(
        FrozenQueue(queue),
        warning_service=warnings,
        now=clock,
        stuck_age_sec=60,
    )
    result = ingress.tick()
    assert result["oldest_received_age"] >= 61
    assert result["stuck_alerted"] is True
    con = readonly_connect(db)
    try:
        row = con.execute(
            "select warning_key,occurrence_count,topic from bot_warning_inbox "
            "where warning_key='bot_add:ingress:oldest_received'"
        ).fetchone()
    finally:
        con.close()
    assert row is not None
    assert row["topic"] == "bot_add_ingress"
    assert int(row["occurrence_count"]) == 1


def test_telegram_submit_to_prechecking_without_manual_state_jumps(tmp_path):
    """E2E: submit leaves received; ingress + probe advance to prechecking."""
    from qbt_orchestrator.bot_add_ingress import BotAddIngressCoordinator
    from qbt_orchestrator.bot_add_queue import BotAddQueueRepository
    from qbt_orchestrator.metadata_probe import MetadataProbeCoordinator
    from qbt_orchestrator.telegram_control import TelegramAuthorizer
    from qbt_orchestrator.telegram_router import TelegramUpdateRouter

    db = tmp_path / "state.sqlite"
    migrate(db)
    clock = Clock()
    queue = BotAddQueueRepository(db, now=clock)

    class Api:
        def __init__(self):
            self.messages = []
            self.edits = []
            self.callbacks = []
            self._next_id = 10

        def send_message(self, chat_id, text, reply_markup=None):
            self._next_id += 1
            self.messages.append((chat_id, text, reply_markup))
            return {"ok": True, "result": {"message_id": self._next_id}}

        def edit_message_text(self, chat_id, message_id, text, reply_markup=None):
            self.edits.append((chat_id, message_id, text, reply_markup))
            return {"ok": True}

        def answer_callback_query(self, callback_query_id, text=None):
            self.callbacks.append((callback_query_id, text))
            return {"ok": True}

    api = Api()
    router = TelegramUpdateRouter(
        api=api,
        authorizer=TelegramAuthorizer(admins={42}, single_admin_id=42),
        state_db=db,
        add_queue=queue,
        panel_enabled=True,
        admin_user_id="42",
        now=clock,
    )
    magnet = "magnet:?" + "xt=urn:btih:" + "e" * 40
    router.handle_update(
        {
            "update_id": 1,
            "message": {
                "message_id": 11,
                "chat": {"id": 7},
                "from": {"id": 42},
                "text": magnet,
            },
        }
    )
    draft = queue.open_draft("7", "42")
    batch_id = int(draft["id"])
    gen = int(draft["updated_at"])
    router.handle_update(
        {
            "update_id": 2,
            "callback_query": {
                "id": "cb-submit",
                "from": {"id": 42},
                "message": {"message_id": 5, "chat": {"id": 7}},
                "data": f"a:s:{batch_id}:{gen}",
            },
        }
    )
    item = queue.list_items(batch_id)[0]
    assert item["state"] == "received"

    ingress = BotAddIngressCoordinator(queue, now=clock)
    gateway = FakeProbeGateway()
    probe = MetadataProbeCoordinator(queue, gateway, owner="probe", now=clock)

    assert ingress.tick()["waiting_probe_slot"] == [item["id"]]
    assert queue.get_item(item["id"])["state"] == "waiting_probe_slot"

    started = probe.tick(sync_healthy=True)
    assert started["started"] == [item["id"]]
    waiting = queue.get_item(item["id"])
    assert waiting["state"] == "metadata_wait"
    gateway.by_tag[waiting["qbt_precheck_tag"]]["state"] = "stoppedDL"
    clock.advance(5)
    ready = probe.tick(sync_healthy=True)
    assert ready["ready"] == [item["id"]]
    assert queue.get_item(item["id"])["state"] == "prechecking"
