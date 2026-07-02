#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))


class RecordingRunner:
    def __init__(self, outputs=None, returncodes=None):
        self.calls = []
        self.outputs = list(outputs or [])
        self.returncodes = list(returncodes or [])

    def __call__(self, argv, input_text=None, timeout=None):
        self.calls.append((list(argv), input_text, timeout))
        out = self.outputs.pop(0) if self.outputs else "Ok."
        rc = self.returncodes.pop(0) if self.returncodes else 0
        return rc, out, ""


def test_qbt_docker_client_uses_container_local_api_and_parses_json():
    from qbt_orchestrator.integrations.qbt import QbtDockerClient

    runner = RecordingRunner(outputs=[
        json.dumps({"rid": 2, "full_update": False, "torrents": {}}),
        json.dumps([{"hash": "h1", "seq_dl": False}]),
        json.dumps([{"name": "a.mp4", "size": 10}, {"index": 9, "name": "b.nfo", "size": 1}]),
        "Ok.",
    ])
    client = QbtDockerClient(container="qbittorrent", api_base="http://127.0.0.1:8080", runner=runner)

    assert client.get_maindata(1)["rid"] == 2
    assert client.torrent_info("h1")["seq_dl"] is False
    assert client.torrent_files("h1") == [{"name": "a.mp4", "size": 10, "index": 0}, {"index": 9, "name": "b.nfo", "size": 1}]
    assert client.post("/api/v2/torrents/stop", {"hashes": "h1"}) == "Ok."

    first = runner.calls[0][0]
    assert first[:5] == ["docker", "exec", "qbittorrent", "curl", "-fsS"]
    assert "--connect-timeout" in first
    assert "--max-time" in first
    assert "http://127.0.0.1:8080/api/v2/sync/maindata?rid=1" in first
    assert runner.calls[3][1] == "hashes=h1"


def test_rclone_client_copyto_and_lsjson_size_use_root_config_without_logging_secret():
    from qbt_orchestrator.integrations.rclone import RcloneClient

    runner = RecordingRunner(outputs=["", json.dumps([{"Name": "a.mp4", "Size": 123}])])
    client = RcloneClient(config_path="/root/.config/rclone/rclone.conf", transfers=1, checkers=2, runner=runner)

    assert client.copyto("/tmp/a.mp4", "gcrypt:/A/a.mp4") is True
    assert client.lsjson_size("gcrypt:/A/a.mp4") == 123
    assert runner.calls[0][0][:4] == ["rclone", "--config", "/root/.config/rclone/rclone.conf", "--transfers"]
    assert "copyto" in runner.calls[0][0]
    assert "lsjson" in runner.calls[1][0]


def test_emby_client_posts_precise_media_updated_payload_and_blocks_root():
    from qbt_orchestrator.integrations.emby import EmbyClient

    sent = []
    def transport(url, payload, headers, timeout):
        sent.append((url, payload, headers, timeout))
        return {"ok": True}

    client = EmbyClient(base_url="http://127.0.0.1:8096", api_key="secret", media_prefix="/media/gcrypt", transport=transport)
    assert client.media_updated("/media/gcrypt/ABC-123") == {"ok": True}
    assert sent[0][0] == "http://127.0.0.1:8096/Library/Media/Updated"
    assert sent[0][1] == {"Updates": [{"Path": "/media/gcrypt/ABC-123", "UpdateType": "Created"}]}
    assert sent[0][2]["X-Emby-Token"] == "secret"

    try:
        client.media_updated("/media/gcrypt")
    except ValueError as e:
        assert "too broad" in str(e)
    else:
        raise AssertionError("library root refresh must be blocked")


def test_telegram_polling_writes_commands_and_rejects_unauthorized_users():
    from qbt_orchestrator.integrations.telegram import TelegramPollingService
    from qbt_orchestrator.telegram_control import TelegramAuthorizer

    updates = [
        {"update_id": 10, "message": {"message_id": 1, "chat": {"id": 100}, "from": {"id": 1}, "text": "/status disk"}},
        {"update_id": 11, "message": {"message_id": 2, "chat": {"id": 101}, "from": {"id": 99}, "text": "/cleanup h1"}},
    ]
    sent = []
    class Store:
        def __init__(self): self.commands = []
        def insert_command(self, command_id, chat_id, user_id, command, payload):
            self.commands.append((command_id, chat_id, user_id, command, payload))

    class Api:
        def get_updates(self, offset, timeout): return updates if offset is None else []
        def send_message(self, chat_id, text, reply_markup=None): sent.append((chat_id, text, reply_markup))

    store = Store()
    service = TelegramPollingService(api=Api(), authorizer=TelegramAuthorizer(viewers={1}), command_store=store)
    assert service.poll_once() == 2
    assert store.commands == [("tg-10", 100, 1, "status", {"args": ["disk"], "text": "/status disk"})]
    assert sent == [(101, "unauthorized", None)]
    assert service.next_offset == 12


def test_telegram_polling_errors_are_counted_not_raised():
    from qbt_orchestrator.integrations.telegram import TelegramPollingService
    from qbt_orchestrator.telegram_control import TelegramAuthorizer

    class BadApi:
        def get_updates(self, offset, timeout): raise RuntimeError("telegram down")
        def send_message(self, chat_id, text, reply_markup=None): pass

    service = TelegramPollingService(api=BadApi(), authorizer=TelegramAuthorizer(viewers={1}), command_store=None)
    assert service.poll_once() == 0
    assert service.consecutive_failures == 1


def test_telegram_callback_approval_updates_store_once_and_rejects_duplicate_click():
    from qbt_orchestrator.integrations.telegram import TelegramPollingService
    from qbt_orchestrator.telegram_control import TelegramAuthorizer

    updates = [
        {
            "update_id": 20,
            "callback_query": {
                "id": "cb-1",
                "from": {"id": 3},
                "message": {"chat": {"id": 100}},
                "data": "approve:approval-c3",
            },
        },
        {
            "update_id": 21,
            "callback_query": {
                "id": "cb-2",
                "from": {"id": 3},
                "message": {"chat": {"id": 100}},
                "data": "approve:approval-c3",
            },
        },
        {
            "update_id": 22,
            "callback_query": {
                "id": "cb-3",
                "from": {"id": 1},
                "message": {"chat": {"id": 101}},
                "data": "deny:approval-c4",
            },
        },
    ]
    sent = []

    class Store:
        def __init__(self):
            self.approvals = []

        def approve_once(self, approval_id, user_id):
            self.approvals.append(("approve", approval_id, user_id))
            return len(self.approvals) == 1

        def deny_once(self, approval_id, user_id):
            self.approvals.append(("deny", approval_id, user_id))
            return True

    class Api:
        def get_updates(self, offset, timeout):
            return updates if offset is None else []

        def send_message(self, chat_id, text, reply_markup=None):
            sent.append((chat_id, text, reply_markup))

    store = Store()
    service = TelegramPollingService(api=Api(), authorizer=TelegramAuthorizer(viewers={1}, admins={3}), command_store=store)

    assert service.poll_once() == 3
    assert store.approvals == [
        ("approve", "approval-c3", 3),
        ("approve", "approval-c3", 3),
    ]
    assert sent == [
        (100, "approved", None),
        (100, "approval unavailable", None),
        (101, "unauthorized", None),
    ]
    assert service.next_offset == 23


def test_telegram_notification_sender_sends_queued_messages_and_retries_failures():
    import tempfile
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.integrations.telegram import TelegramNotificationSender
    from qbt_orchestrator.runtime import BotNotificationRepository

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        repo = BotNotificationRepository(db, now=lambda: 100)
        first = repo.enqueue(100, "status", "hello")
        second = repo.enqueue(100, "status", "will retry")

        sent = []

        class Api:
            def __init__(self):
                self.calls = 0

            def get_updates(self, offset, timeout):
                return []

            def send_message(self, chat_id, text, reply_markup=None):
                self.calls += 1
                if self.calls == 2:
                    raise RuntimeError("telegram down token " + "123456:" + "secret-token")
                sent.append((chat_id, text, reply_markup))
                return {"ok": True}

        sender = TelegramNotificationSender(repo, Api(), retry_delay=60)

        assert sender.send_next() == first
        assert sender.send_next() == second

        assert sent == [(100, "hello", None)]
        assert repo.get(first)["state"] == "sent"
        failed = repo.get(second)
        assert failed["state"] == "retry_wait"
        assert failed["next_run_at"] == 160
        assert "secret-token" not in failed["last_error"]


def test_qbt_docker_client_rate_limits_api_calls_with_token_bucket():
    from qbt_orchestrator.integrations.qbt import QbtDockerClient

    class FakeClock:
        def __init__(self):
            self.now = 0.0
            self.sleeps = []
        def monotonic(self):
            return self.now
        def sleep(self, seconds):
            self.sleeps.append(round(seconds, 3))
            self.now += seconds

    runner = RecordingRunner(outputs=[json.dumps({"rid": 2, "full_update": False, "torrents": {}}), "Ok."])
    clock = FakeClock()
    client = QbtDockerClient(runner=runner, api_max_requests_per_sec=1, clock=clock.monotonic, sleeper=clock.sleep)

    client.get_maindata(1)
    client.post("/api/v2/torrents/stop", {"hashes": "h1"})

    assert clock.sleeps == [1.0]
    assert len(runner.calls) == 2


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
    print("ok")
