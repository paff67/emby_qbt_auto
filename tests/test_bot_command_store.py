#!/usr/bin/env python3
from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))


def test_sqlite_bot_command_store_upserts_command_once():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.telegram_control import SQLiteBotCommandStore

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        store = SQLiteBotCommandStore(db)
        store.insert_command("tg-10", 100, 1, "status", {"args": ["disk"]})
        store.insert_command("tg-10", 100, 1, "status", {"args": ["disk"]})
        con = sqlite3.connect(db)
        rows = con.execute("select command_id, chat_id, user_id, command, state, payload_json from bot_commands").fetchall()
        con.close()
        assert len(rows) == 1
        assert rows[0][:5] == ("tg-10", "100", "1", "status", "queued")
        assert "disk" in rows[0][5]


if __name__ == "__main__":
    test_sqlite_bot_command_store_upserts_command_once()
    print("ok")
