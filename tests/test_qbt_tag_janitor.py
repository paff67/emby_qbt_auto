from __future__ import annotations

from qbt_orchestrator.db import migrate
from qbt_orchestrator.qbt_tag_janitor import (
    QbtTagJanitor,
    compute_orphan_add_tag_candidates,
)


class FakeGateway:
    def __init__(self, tags: set[str] | None = None, users: dict[str, list] | None = None):
        self.tags = set(tags or set())
        self.users = {str(k): list(v) for k, v in dict(users or {}).items()}
        self.deleted: list[str] = []
        self.delete_calls = 0
        self._sqlite_ref_hook = None
        self._user_hook = None

    def list_tags(self):
        return set(self.tags)

    def torrents_by_tag(self, tag):
        if self._user_hook is not None:
            self._user_hook(tag)
        return list(self.users.get(str(tag), []))

    def delete_tags(self, tags, *, guard=None):
        self.delete_calls += 1
        if guard is not None and not guard():
            return False
        for tag in tags:
            self.deleted.append(str(tag))
            self.tags.discard(str(tag))
            self.users.pop(str(tag), None)
        return True


class FakeRepo:
    def __init__(self, state_db, refs: set[str] | None = None):
        self.state_db = state_db
        self.refs = set(refs or set())
        self._ref_hook = None

    def list_qbt_precheck_tag_refs(self):
        if self._ref_hook is not None:
            self._ref_hook()
        return set(self.refs)


def _tag(char: str = "a", n: int = 32) -> str:
    return "add-item-" + char * n


def test_compute_orphan_candidates_requires_dual_evidence():
    orphan = _tag("a")
    assigned = _tag("b")
    referenced = _tag("c")
    legacy = _tag("d", 16)
    candidates = compute_orphan_add_tag_candidates(
        global_tags=[orphan, assigned, referenced, legacy, "checked"],
        snapshots={"h1": {"tags": f"checked,{assigned}"}},
        sqlite_refs={referenced},
    )
    assert candidates == [orphan]


def test_janitor_deletes_orphan_current_tag(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    orphan = _tag("a")
    gateway = FakeGateway({orphan, "checked"})
    repo = FakeRepo(db)
    janitor = QbtTagJanitor(repo, gateway, dry_run=False, batch_limit=25)

    result = janitor.tick({}, sync_healthy=True)

    assert result["status"] == "ok"
    assert result["candidate_count"] == 1
    assert result["deleted"] == [orphan]
    assert orphan not in gateway.tags


def test_janitor_keeps_tag_assigned_in_qbt(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    tag = _tag("a")
    gateway = FakeGateway({tag}, users={tag: [{"hash": "h1"}]})
    janitor = QbtTagJanitor(FakeRepo(db), gateway, dry_run=False)

    result = janitor.tick({"h1": {"tags": tag}}, sync_healthy=True)

    assert result["candidate_count"] == 0
    assert result["deleted"] == []
    assert gateway.delete_calls == 0


def test_janitor_keeps_sqlite_referenced_tag_in_any_state(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    tag = _tag("a")
    gateway = FakeGateway({tag})
    janitor = QbtTagJanitor(FakeRepo(db, {tag}), gateway, dry_run=False)

    result = janitor.tick({}, sync_healthy=True)

    assert result["candidate_count"] == 0
    assert result["deleted"] == []


def test_janitor_rejects_non_32_hex_names(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    legacy = _tag("a", 16)
    long_tag = _tag("b", 64)
    gateway = FakeGateway({legacy, long_tag, "add-item-user-label"})
    janitor = QbtTagJanitor(FakeRepo(db), gateway, dry_run=False)

    result = janitor.tick({}, sync_healthy=True)

    assert result["global_count"] == 0
    assert result["candidate_count"] == 0
    assert result["deleted"] == []


def test_janitor_pre_delete_guard_fails_when_sqlite_gains_ref(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    tag = _tag("a")
    gateway = FakeGateway({tag})
    repo = FakeRepo(db)
    janitor = QbtTagJanitor(repo, gateway, dry_run=False)
    calls = {"n": 0}

    def gain_ref():
        calls["n"] += 1
        if calls["n"] >= 2:
            repo.refs.add(tag)

    repo._ref_hook = gain_ref
    result = janitor.tick({}, sync_healthy=True)

    assert result["candidate_count"] == 1
    assert result["deleted"] == []
    assert result["fenced"] == [tag]
    assert gateway.delete_calls == 0


def test_janitor_pre_delete_guard_fails_when_qbt_gains_user(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    tag = _tag("a")
    gateway = FakeGateway({tag})
    janitor = QbtTagJanitor(FakeRepo(db), gateway, dry_run=False)
    calls = {"n": 0}

    def gain_user(queried):
        calls["n"] += 1
        if calls["n"] >= 1:
            gateway.users[queried] = [{"hash": "h1"}]

    gateway._user_hook = gain_user
    result = janitor.tick({}, sync_healthy=True)

    assert result["candidate_count"] == 1
    assert result["deleted"] == []
    assert result["fenced"] == [tag]


def test_janitor_batch_limit_and_second_run_idempotent(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    tags = {_tag(chr(ord("a") + i)) for i in range(5)}
    gateway = FakeGateway(set(tags))
    janitor = QbtTagJanitor(FakeRepo(db), gateway, dry_run=False, batch_limit=2)

    first = janitor.tick({}, sync_healthy=True)
    assert len(first["deleted"]) == 2
    assert first["candidate_count"] == 5

    second = janitor.tick({}, sync_healthy=True)
    assert len(second["deleted"]) == 2
    assert second["candidate_count"] == 3

    third = janitor.tick({}, sync_healthy=True)
    assert len(third["deleted"]) == 1
    fourth = janitor.tick({}, sync_healthy=True)
    assert fourth["deleted"] == []
    assert fourth["candidate_count"] == 0


def test_janitor_dry_run_and_unhealthy_sync_do_not_write(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    tag = _tag("a")
    gateway = FakeGateway({tag})
    dry = QbtTagJanitor(FakeRepo(db), gateway, dry_run=True)
    dry_result = dry.tick({}, sync_healthy=True)
    assert dry_result["status"] == "dry_run"
    assert dry_result["candidate_count"] == 1
    assert dry_result["deleted"] == []
    assert gateway.delete_calls == 0

    live = QbtTagJanitor(FakeRepo(db), gateway, dry_run=False)
    suspended = live.tick({}, sync_healthy=False)
    assert suspended["status"] == "suspended"
    assert gateway.delete_calls == 0


def test_repository_list_qbt_precheck_tag_refs_ignores_state(tmp_path):
    from qbt_orchestrator.bot_add_queue import BotAddQueueRepository
    from qbt_orchestrator.db import write_transaction

    db = tmp_path / "state.sqlite"
    migrate(db)
    repo = BotAddQueueRepository(db, now=lambda: 2_000_000_000)
    batch = repo.open_draft("1", "1")
    repo.append_message(batch["id"], 1, ["magnet:?" + "xt=urn:btih:" + "a" * 40])
    repo.submit(batch["id"])
    item = repo.list_items(batch["id"])[0]
    tag = _tag("f")
    write_transaction(
        db,
        lambda con: con.execute(
            "update bot_add_items set state=?, qbt_precheck_tag=? where id=?",
            ("cancelled", tag, item["id"]),
        ),
    )
    assert repo.list_qbt_precheck_tag_refs() == {tag}
