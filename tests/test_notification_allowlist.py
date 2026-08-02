from __future__ import annotations

import json

from qbt_orchestrator.db import migrate, readonly_connect
from qbt_orchestrator.integrations.telegram import TelegramNotificationSender
from qbt_orchestrator.runtime import BotNotificationRepository


class FakeApi:
    def __init__(self):
        self.messages: list[tuple] = []

    def send_message(self, chat_id, text, reply_markup=None):
        self.messages.append((chat_id, text, reply_markup))
        return {"ok": True}


def test_sender_allows_confirmation_and_final_summary_only(tmp_path):
    db = tmp_path / "n.sqlite"
    migrate(db)
    repo = BotNotificationRepository(db, now=lambda: 1)
    api = FakeApi()
    sender = TelegramNotificationSender(repo, api)

    repo.enqueue(
        chat_id="7",
        topic="disk_threshold",
        message="should suppress",
        dedupe_key="disk-1",
    )
    repo.enqueue(
        chat_id="7",
        topic="add_batch_summary",
        message="initial should suppress",
        payload={"summary": "initial"},
        dedupe_key="batch-initial",
    )
    repo.enqueue(
        chat_id="7",
        topic="add_batch_summary",
        message="final ok",
        payload={"summary": "final"},
        dedupe_key="batch-final",
    )
    repo.enqueue(
        chat_id="7",
        topic="download_confirmation",
        message="confirm ok",
        dedupe_key="confirm-1",
    )

    for _ in range(4):
        sender.send_next()

    con = readonly_connect(db)
    try:
        rows = {
            row["dedupe_key"]: row["state"]
            for row in con.execute(
                "select dedupe_key,state from bot_notifications"
            )
        }
    finally:
        con.close()
    assert rows["disk-1"] == "suppressed"
    assert rows["batch-initial"] == "suppressed"
    assert rows["batch-final"] == "sent"
    assert rows["confirm-1"] == "sent"
    assert {text for _, text, _ in api.messages} == {"final ok", "confirm ok"}
    assert "should suppress" not in json.dumps(api.messages)
