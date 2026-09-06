from __future__ import annotations


def test_partial_delta_preserves_unchanged_torrent_fields():
    from qbt_orchestrator.snapshot_store import TorrentRawSnapshotStore

    store = TorrentRawSnapshotStore()
    store.replace_full(
        {
            "h": {
                "name": "A",
                "category": "auto",
                "tags": "auto",
                "amount_left": 100,
                "size": 200,
                "progress": 0.5,
            }
        }
    )

    store.apply_delta({"h": {"dlspeed": 123}}, removed=[])

    snap = store.snapshots()["h"]
    assert snap.category == "auto"
    assert snap.tags == "auto"
    assert snap.amount_left == 100
    assert snap.size == 200
    assert snap.progress == 0.5
    assert snap.dlspeed_bps == 123


def test_snapshot_store_applies_removal_and_full_replacement():
    from qbt_orchestrator.snapshot_store import TorrentRawSnapshotStore

    store = TorrentRawSnapshotStore()
    store.replace_full({"removed": {"size": 1}, "kept": {"size": 2}})
    store.apply_delta({"kept": {"amount_left": 1}}, removed=["removed"])

    assert set(store.snapshots()) == {"kept"}
    assert store.snapshots()["kept"].size == 2

    assert store.replace_full({"new": {"size": 3}}) is None
    assert set(store.snapshots()) == {"new"}


def test_snapshot_store_copies_inputs_before_publishing_snapshots():
    from qbt_orchestrator.snapshot_store import TorrentRawSnapshotStore

    full = {"h": {"category": "auto", "size": 10}}
    delta = {"h": {"dlspeed": 20}}
    store = TorrentRawSnapshotStore()
    store.replace_full(full)
    store.apply_delta(delta, removed=[])

    full["h"]["category"] = "mutated"
    delta["h"]["dlspeed"] = 999

    snap = store.snapshots()["h"]
    assert snap.category == "auto"
    assert snap.dlspeed_bps == 20
