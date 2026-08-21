from __future__ import annotations

from urllib.parse import parse_qs, urlsplit

from qbt_orchestrator.db import migrate


class Clock:
    def __init__(self, value=2_100_000_000):
        self.value = value

    def __call__(self):
        return self.value


class Gateway:
    def __init__(self):
        self.posts = []

    @staticmethod
    def expected_hash(magnet):
        return parse_qs(urlsplit(magnet).query)["xt"][0].split(":")[-1]

    def stop(self, torrent_hash, *, guard=None):
        return self._write("stop", torrent_hash, None, guard)

    def set_download_limit(self, torrent_hash, limit, *, guard=None):
        return self._write("limit", torrent_hash, limit, guard)

    def add_tags(self, torrent_hash, tags, *, guard=None):
        return self._write("add_tags", torrent_hash, tags, guard)

    def set_category(self, torrent_hash, category, *, guard=None):
        return self._write("category", torrent_hash, category, guard)

    def remove_tags(self, torrent_hash, tags, *, guard=None):
        return self._write("remove_tags", torrent_hash, tags, guard)

    def _write(self, name, torrent_hash, value, guard):
        if guard is not None and not guard():
            return False
        self.posts.append((name, torrent_hash, value))
        return True


def _snapshot(torrent_hash, *, metadata=False, tags="auto, checked"):
    return {
        "hash": torrent_hash,
        "state": "stoppedDL",
        "has_metadata": metadata,
        "tags": tags,
        "name": "START-591",
        "magnet_uri": "magnet:?xt=urn:btih:" + torrent_hash + "&dn=START-591",
    }


def test_adopter_registers_only_owned_metadata_less_qbt_tasks(tmp_path):
    from qbt_orchestrator.bot_add_queue import BotAddQueueRepository
    from qbt_orchestrator.managed_metadata_adoption import ManagedMetadataAdopter

    db = tmp_path / "state.sqlite"
    migrate(db)
    clock = Clock()
    repository = BotAddQueueRepository(db, now=clock)
    gateway = Gateway()
    adopter = ManagedMetadataAdopter(repository, gateway, now=clock)
    owned = "a" * 40

    result = adopter.tick(
        {
            owned: _snapshot(owned),
            "b" * 40: _snapshot("b" * 40, metadata=True),
            "c" * 40: _snapshot("c" * 40, tags="checked"),
        },
        sync_healthy=True,
    )

    assert result["adopted"] == [owned]
    batches = repository.list_batches(state="processing")
    assert len(batches) == 1
    item = repository.list_items(batches[0]["id"], include_raw=True)[0]
    assert item["state"] == "waiting_probe_slot"
    assert item["qbt_hash"] == owned
    assert item["qbt_precheck_tag"].startswith("add-item-")
    assert gateway.posts == [
        ("stop", owned, None),
        ("limit", owned, 1024),
        ("add_tags", owned, f"precheck,metadata-probe,hold,{item['qbt_precheck_tag']}"),
        ("category", owned, "precheck"),
        ("remove_tags", owned, "auto"),
    ]

    assert adopter.tick({owned: _snapshot(owned)}, sync_healthy=True)["adopted"] == []


def test_adopter_suspends_without_healthy_qbt_sync(tmp_path):
    from qbt_orchestrator.bot_add_queue import BotAddQueueRepository
    from qbt_orchestrator.managed_metadata_adoption import ManagedMetadataAdopter

    db = tmp_path / "state.sqlite"
    migrate(db)
    gateway = Gateway()
    adopter = ManagedMetadataAdopter(BotAddQueueRepository(db), gateway)

    result = adopter.tick({"a" * 40: _snapshot("a" * 40)}, sync_healthy=False)

    assert result == {"suspended": True, "adopted": [], "failed": [], "skipped": 0}
    assert gateway.posts == []
