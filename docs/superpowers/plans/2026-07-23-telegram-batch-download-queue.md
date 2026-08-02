# Telegram Batch Download Queue Implementation Plan

> **Superseded in part (2026-08-02):** WarningInbox read state, `bot_warning_reads`, Telegram panel transport/routing, and manual-deletion duplicate presentation are overridden by the design-spec “2026-08-02 P1 override” and the processed-media tombstone plan. Do not recreate `bot_warning_reads`; use single-admin `resolved` on `bot_warning_inbox` and `blocked_manual_deleted` for tombstones.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver the approved Telegram control panel and a restart-safe, fair, bounded queue for batches of magnet, HTTP/HTTPS metainfo, and `bc://bt/` links.

**Architecture:** Telegram updates write intent to SQLite; dedicated repositories and workers own all state transitions. Link resolution, duplicate matching, metadata probing, enrollment, notification delivery, and UI rendering are separate focused modules. qBT mutations remain serialized, while three leased metadata slots are polled asynchronously and fairly across logical batches.

**Tech Stack:** Python 3.11 stdlib, SQLite, urllib, qBittorrent Web API v5.1.4, Telegram Bot API, pytest, systemd immutable releases.

---

## File map

- Create `src/qbt_orchestrator/download_links.py`: supported-scheme parsing, identity normalization, safe HTTP metainfo resolution, and BitComet decoding.
- Create `src/qbt_orchestrator/bot_add_queue.py`: batch/item/shard repositories, state transitions, leases, ingress limits, and summaries.
- Create `src/qbt_orchestrator/checked_add.py`: remote index refresh, video selection, normalization adapter, duplicate decisions, and enrollment.
- Create `src/qbt_orchestrator/metadata_probe.py`: three-slot coordinator, qBT precheck lifecycle, polling, backoff, and fencing.
- Create `src/qbt_orchestrator/telegram_ui.py`: natural-language page renderers and inline keyboards.
- Create `src/qbt_orchestrator/warning_inbox.py`: warning persistence, unread state, copy summary, and bounded text export.
- Modify `src/qbt_orchestrator/db.py`: queue, shard, event, remote index, warning, and read-state tables.
- Modify `src/qbt_orchestrator/integrations/qbt.py`: form/multipart-compatible add helpers and tag-filter queries.
- Modify `src/qbt_orchestrator/integrations/telegram.py`: edit/callback/document API methods and general update router hooks.
- Modify `src/qbt_orchestrator/telegram_control.py`: `/start`, menu, add, queue, warning, and approval authorization.
- Modify `src/qbt_orchestrator/service.py`, `src/qbt_orchestrator/cli.py`, `src/qbt_orchestrator/runtime.py`: worker wiring and effective configuration.
- Create `tests/test_download_links.py`, `tests/test_bot_add_queue.py`, `tests/test_metadata_probe.py`, `tests/test_telegram_ui.py`, `tests/test_warning_inbox.py`.
- Modify `tests/test_runtime_integrations.py`, `tests/test_daemon_runtime.py`, `tests/test_cli_observability.py`.

### Task 1: Add durable Bot queue and warning schema

**Files:**
- Modify: `src/qbt_orchestrator/db.py`
- Create: `tests/test_bot_add_queue.py`
- Create: `tests/test_warning_inbox.py`

- [ ] **Step 1: Write the failing schema test**

```python
from qbt_orchestrator.db import migrate, readonly_connect


def test_bot_queue_schema_contains_all_durable_state(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    con = readonly_connect(db)
    try:
        tables = {row[0] for row in con.execute("select name from sqlite_master where type='table'")}
        assert {"bot_add_batches", "bot_add_shards", "bot_add_items", "bot_add_events",
                "remote_media_index", "bot_warning_inbox"} <= tables
        # superseded 2026-08-02: bot_warning_reads must be absent
        item_columns = {row[1] for row in con.execute("pragma table_info(bot_add_items)")}
        assert {"raw_input", "raw_input_expires_at", "metadata_probe_attempt",
                "metadata_probe_deadline", "metadata_retry_at", "metadata_lease_generation"} <= item_columns
    finally:
        con.close()
```

- [ ] **Step 2: Run the schema test and verify failure**

Run: `python -m pytest tests/test_bot_add_queue.py::test_bot_queue_schema_contains_all_durable_state -v`

Expected: FAIL because the tables are missing.

- [ ] **Step 3: Add migration 16**

Add explicit `create table if not exists` statements matching the approved design. Use these state constraints and indexes:

```sql
create table if not exists bot_add_batches(
  id integer primary key autoincrement,batch_key text not null unique,
  chat_id text not null,user_id text not null,state text not null,
  panel_message_id integer,received_count integer not null default 0,
  valid_count integer not null default 0,enrolled_count integer not null default 0,
  duplicate_count integer not null default 0,confirmation_count integer not null default 0,
  failed_count integer not null default 0,initial_summary_sent_at integer,
  created_at integer not null,submitted_at integer,completed_at integer,updated_at integer not null);
create unique index if not exists idx_bot_add_open_draft
  on bot_add_batches(chat_id,user_id) where state='draft';
create table if not exists bot_add_shards(
  id integer primary key autoincrement,batch_id integer not null,shard_index integer not null,
  state text not null,item_count integer not null default 0,processed_count integer not null default 0,
  created_at integer not null,completed_at integer,updated_at integer not null,
  unique(batch_id,shard_index));
```

Create `bot_add_items` with every field in the design, including unique `(batch_id,source_message_id,source_index)`, and indexes on `(state,metadata_retry_at,id)`, `(metadata_probe_deadline,state)`, `canonical_identity`, and `qbt_hash`. Create append-only `bot_add_events`; `remote_media_index(video_path primary key,normalized_id,size,raw_basename,status,source,updated_at)`; and `bot_warning_inbox` (single-admin `resolved` read state; do not create `bot_warning_reads`). Insert schema migration version 16.

- [ ] **Step 4: Run migration/idempotency tests**

Run: `python -m pytest tests/test_bot_add_queue.py::test_bot_queue_schema_contains_all_durable_state tests/test_production_invariants.py -v`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/qbt_orchestrator/db.py tests/test_bot_add_queue.py tests/test_warning_inbox.py
git commit -m "Add durable Telegram add queue schema"
```

### Task 2: Implement atomic batch ingress, limits, and sharding

**Files:**
- Create: `src/qbt_orchestrator/bot_add_queue.py`
- Modify: `tests/test_bot_add_queue.py`

- [ ] **Step 1: Write failing ingress tests**

```python
def test_ingress_accepts_multi_message_batch_and_creates_fifty_item_shards(queue):
    batch = queue.open_draft("1", "2")
    queue.append_message(batch["id"], message_id=10, links=[("magnet:?" + f"xt=urn:btih:{i:040x}") for i in range(75)])
    submitted = queue.submit(batch["id"])
    assert submitted["received_count"] == 75
    assert [row["item_count"] for row in queue.list_shards(batch["id"])] == [50, 25]


def test_overflow_message_is_rejected_atomically(queue):
    batch = queue.open_draft("1", "2")
    queue.append_message(batch["id"], 10, [("magnet:?" + f"xt=urn:btih:{i:040x}") for i in range(490)])
    try:
        queue.append_message(batch["id"], 11, [("magnet:?" + f"xt=urn:btih:{i + 600:040x}") for i in range(20)])
    except ValueError as exc:
        assert str(exc) == "batch_link_limit"
    assert queue.get_batch(batch["id"])["received_count"] == 490
```

Add tests for 8-KiB links, 2-MiB raw drafts, one open draft per chat/user, a 1,000 submitted-item backlog, repeated Telegram message IDs, draft expiry, and raw-input clearing.

- [ ] **Step 2: Run tests and verify failure**

Run: `python -m pytest tests/test_bot_add_queue.py -k 'ingress or overflow' -v`

Expected: FAIL with missing repository.

- [ ] **Step 3: Implement `BotAddQueueRepository`**

Define validated configuration and atomic methods:

```python
@dataclass(frozen=True)
class AddQueueLimits:
    max_links_per_batch: int = 500
    shard_size: int = 50
    max_submitted_items: int = 1000
    max_link_bytes: int = 8192
    max_draft_bytes: int = 2 * 1024 * 1024
    draft_ttl_sec: int = 1800
    raw_input_ttl_sec: int = 7 * 86400
```

Implement `BotAddQueueRepository.open_draft(chat_id: str, user_id: str) -> dict`, `append_message(batch_id: int, message_id: int, links: list[str]) -> dict`, `submit(batch_id: int) -> dict`, `cancel_batch(batch_id: int, actor: str) -> dict`, and `transition_item(item_id: int, expected: set[str], new_state: str, reason: str, fields: dict | None = None) -> dict`. Each method uses `write_transaction`. `append_message()` validates all proposed rows before inserting any, uses `insert or ignore` for update idempotency, and recomputes counters in the same transaction. `submit()` checks global backlog, creates deterministic shards by item ID order, and moves `draft` to `queued` atomically.

- [ ] **Step 4: Run queue tests**

Run: `python -m pytest tests/test_bot_add_queue.py -v`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/qbt_orchestrator/bot_add_queue.py tests/test_bot_add_queue.py
git commit -m "Implement bounded Telegram batch ingress"
```

### Task 3: Parse supported links and safely resolve metainfo

**Files:**
- Create: `src/qbt_orchestrator/download_links.py`
- Create: `tests/test_download_links.py`

- [ ] **Step 1: Write failing parser tests**

```python
def test_magnet_normalizes_hex_and_base32_btih():
    assert parse_download_link("magnet:?" + "xt=urn:btih:" + "AB" * 20).infohash_v1 == "ab" * 20
    assert parse_download_link("magnet:?" + "xt=urn:btih:AERUKZ4JVPG66AJDIVTYTK6N54ASGRLH").infohash_v1 == "0123456789abcdef0123456789abcdef01234567"


def test_http_resolver_rejects_private_redirect(fake_http):
    fake_http.redirect("https://safe.invalid/a.torrent", "http://169.254.169.254/latest/meta-data")
    with pytest.raises(LinkResolutionError, match="restricted_address"):
        HttpMetainfoResolver(transport=fake_http).resolve("https://safe.invalid/a.torrent")


def test_bc_link_extracts_identity():
    parsed = parse_download_link(make_bc_link(name="A.mkv", size=10, infohash="ab" * 20))
    assert parsed.kind == "bc_link"
    assert parsed.infohash_v1 == "ab" * 20
```

Add tests for malformed magnets, unsupported schemes, URL credentials, redirect limits, 10-MiB response limit, invalid bencode, v1 infohash computation from canonical `info` bytes, and raw query redaction.

- [ ] **Step 2: Run tests and verify failure**

Run: `python -m pytest tests/test_download_links.py -v`

Expected: FAIL with missing module.

- [ ] **Step 3: Implement parsing and a minimal canonical bencode reader**

Expose:

```python
@dataclass(frozen=True)
class ResolvedDownloadLink:
    kind: str
    original: str
    redacted: str
    input_sha256: str
    infohash_v1: str | None
    infohash_v2: str | None
    metainfo: bytes | None = None
```

Implement `parse_download_link(value: str) -> ResolvedDownloadLink` and `HttpMetainfoResolver.resolve(url: str) -> ResolvedDownloadLink`. The resolver constructor accepts `transport`, `timeout_sec=15`, `max_bytes=10*1024*1024`, `max_redirects=5`, and `allowed_private_hosts=None`. Use `urllib.parse`, `socket.getaddrinfo`, `ipaddress.ip_address`, `hashlib`, and a byte-offset bencode parser. Hash the exact raw byte span of the `info` dictionary, not a re-encoded Python object. Reject loopback, private, link-local, multicast, unspecified, and reserved destinations unless the hostname is explicitly allowlisted. Never log the unredacted URL.

- [ ] **Step 4: Run parser tests**

Run: `python -m pytest tests/test_download_links.py -v`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/qbt_orchestrator/download_links.py tests/test_download_links.py
git commit -m "Resolve supported BitTorrent download links safely"
```

### Task 4: Port remote-index and duplicate decisions into the repository

**Files:**
- Create: `src/qbt_orchestrator/checked_add.py`
- Modify: `tests/test_bot_add_queue.py`

- [ ] **Step 1: Write failing duplicate tests**

```python
def test_duplicate_matcher_classifies_exact_size_and_variant(db, normalizer):
    index = RemoteMediaIndex(db, backfill_db=None, now=lambda: 100)
    index.replace_rows([{"video_path": "gcrypt:/BBAN-582/a.mp4", "normalized_id": "BBAN-582", "size": 1000,
                         "raw_basename": "a.mp4", "status": "done"}])
    matcher = DuplicateMatcher(db, normalizer=normalizer, size_tolerance_ratio=0.15)
    assert matcher.decide("BBAN-582.mp4", 1050).decision == "duplicate_remote"
    assert matcher.decide("BBAN-582.mp4", 2000).decision == "needs_confirmation"


def test_fuzzy_name_is_warning_only(db, normalizer):
    matcher = DuplicateMatcher(db, normalizer=normalizer, size_tolerance_ratio=0.15)
    result = matcher.decide("BBAN-582-remaster.mp4", 2000, fuzzy_matches=["BBAN-583"])
    assert result.decision == "ready"
    assert result.warnings == ("fuzzy_name_only",)
```

- [ ] **Step 2: Run tests and verify failure**

Run: `python -m pytest tests/test_bot_add_queue.py -k 'duplicate_matcher or fuzzy_name' -v`

Expected: FAIL with missing classes.

- [ ] **Step 3: Implement focused checked-add services**

Implement `RemoteMediaIndex.refresh()` with the live source query:

```sql
select video_path,normalized_id,size,raw_basename,status
from items
where normalized_id is not null and normalized_id!=''
  and video_path is not null and video_path!=''
  and coalesce(status,'') not in ('missing_remote','duplicate_alias','normalize_failed')
```

Refresh in one transaction, use a six-hour TTL, and keep the previous index if the backfill database is unavailable. Implement `select_primary_video(files, min_bytes=100*1024*1024)`, the configured filename-normalizer adapter, `DuplicateDecision`, and `DuplicateMatcher.decide()` with exact identity, normalized ID, and 15% size tolerance. Fuzzy matches only append warning evidence.

- [ ] **Step 4: Run checked-add tests**

Run: `python -m pytest tests/test_bot_add_queue.py -k 'duplicate or primary_video or remote_index' -v`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/qbt_orchestrator/checked_add.py tests/test_bot_add_queue.py
git commit -m "Port durable checked-add duplicate decisions"
```

### Task 5: Add qBT precheck primitives

**Files:**
- Modify: `src/qbt_orchestrator/integrations/qbt.py`
- Modify: `tests/test_runtime_integrations.py`

- [ ] **Step 1: Write failing qBT adapter tests**

```python
def test_qbt_add_precheck_uses_form_fields_and_unique_tag(recording_transport):
    client = QbtHttpClient(api_base="http://qbt", auth_mode="none", transport=recording_transport)
    client.add_precheck("magnet:?" + "xt=urn:btih:" + "ab" * 20, tag="add-item-a1", dl_limit_bps=1024)
    request = recording_transport.requests[-1]
    assert request.path == "/api/v2/torrents/add"
    assert request.form["urls"].startswith("magnet:")
    assert request.form["tags"] == "precheck,metadata-probe,add-item-a1,hold"
    assert request.form["dlLimit"] == "1024"


def test_qbt_remove_precheck_never_deletes_files(recording_transport):
    client = QbtHttpClient(api_base="http://qbt", auth_mode="none", transport=recording_transport)
    client.remove_precheck("ab" * 20)
    assert recording_transport.requests[-1].form == {"hashes": "ab" * 20, "deleteFiles": "false"}
```

- [ ] **Step 2: Run tests and verify failure**

Run: `python -m pytest tests/test_runtime_integrations.py -k 'add_precheck or remove_precheck' -v`

Expected: FAIL because helpers are missing.

- [ ] **Step 3: Implement identical helpers on HTTP and Docker clients**

Add `add_precheck(url, tag, dl_limit_bps)`, `list_by_tag(tag)`, `stop(hash)`, `start(hash)`, `set_file_priorities_zero(hash)`, `set_category(hash, category)`, `add_tags(hash,tags)`, `remove_tags(hash,tags)`, and `remove_precheck(hash)` to both clients. For Docker `/torrents/add`, use curl `-F`. For HTTP `/torrents/add`, add a bounded `_request_multipart()` and send `urls`, `category`, `tags`, `stopped=false`, and `dlLimit`; retain form-urlencoded bodies for all other writes. `remove_precheck()` must always send `deleteFiles=false`.

- [ ] **Step 4: Run qBT integration tests**

Run: `python -m pytest tests/test_runtime_integrations.py -v`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/qbt_orchestrator/integrations/qbt.py tests/test_runtime_integrations.py
git commit -m "Add isolated qBT metadata precheck operations"
```

### Task 6: Implement the fair, non-blocking metadata coordinator

**Files:**
- Create: `src/qbt_orchestrator/metadata_probe.py`
- Create: `tests/test_metadata_probe.py`

- [ ] **Step 1: Write failing slot/backoff tests**

```python
def test_three_slots_rotate_across_batches(probe_fixture):
    probe_fixture.queue_batches([5, 5, 5, 5])
    probe_fixture.coordinator.tick()
    active = probe_fixture.items(state="metadata_wait")
    assert len(active) == 3
    assert len({row["batch_id"] for row in active}) == 3


def test_timeout_releases_slot_and_schedules_backoff(probe_fixture):
    item = probe_fixture.active_probe(attempt=1, deadline=100)
    probe_fixture.clock.value = 101
    probe_fixture.coordinator.tick()
    row = probe_fixture.item(item)
    assert row["state"] == "metadata_retry_wait"
    assert row["metadata_retry_at"] == 101 + 1800
    assert probe_fixture.qbt.removed == [(row["qbt_hash"], False)]


def test_metadata_ready_stops_and_zeroes_files(probe_fixture):
    item = probe_fixture.active_probe(attempt=1, deadline=500)
    probe_fixture.qbt.set_metadata_ready(item, files=[{"index": 0, "name": "BBAN-582.mp4", "size": 1000}])
    probe_fixture.coordinator.tick()
    assert probe_fixture.qbt.stopped == [probe_fixture.hash_for(item)]
    assert probe_fixture.qbt.zeroed == [probe_fixture.hash_for(item)]
    assert probe_fixture.item(item)["state"] == "prechecking"
```

Add tests for 5/10/15-minute windows, 30-minute/6-hour backoffs, final `metadata_unavailable`, five-second due polling, lease-generation fencing, restart recovery, one-slot-per-batch fairness, and manual 24-hour retry.

- [ ] **Step 2: Run tests and verify failure**

Run: `python -m pytest tests/test_metadata_probe.py -v`

Expected: FAIL with missing coordinator.

- [ ] **Step 3: Implement coordinator configuration and tick**

```python
@dataclass(frozen=True)
class MetadataProbeConfig:
    slots: int = 3
    poll_interval_sec: int = 5
    windows_sec: tuple[int, int, int] = (300, 600, 900)
    backoffs_sec: tuple[int, int] = (1800, 21600)
    payload_limit_bps: int = 1024
    lease_sec: int = 30


class MetadataProbeCoordinator:
    def tick(self) -> dict[str, int]:
        self._recover_expired_leases()
        completed = self._poll_due_active()
        timed_out = self._expire_due_active()
        started = self._fill_slots_fairly()
        return {"completed": completed, "timed_out": timed_out, "started": started}
```

Every transition uses `(item_id,state,metadata_lease_generation)` fencing in one transaction. Starting qBT is serialized through the existing action dispatcher. Polling performs no sleeps. Timeout stops/revalidates/removes the temporary registration with `deleteFiles=false` and persists the next state before releasing the lease.

- [ ] **Step 4: Run metadata tests**

Run: `python -m pytest tests/test_metadata_probe.py -v`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/qbt_orchestrator/metadata_probe.py tests/test_metadata_probe.py
git commit -m "Schedule fair nonblocking metadata probes"
```

### Task 7: Implement validation, confirmation, and enrollment

**Files:**
- Modify: `src/qbt_orchestrator/checked_add.py`
- Modify: `src/qbt_orchestrator/bot_add_queue.py`
- Modify: `tests/test_bot_add_queue.py`

- [ ] **Step 1: Write failing end-to-end item tests**

```python
def test_same_id_different_size_requires_confirmation_and_stays_held(add_service):
    item = add_service.prechecked_item(name="BBAN-582.mp4", size=2000, remote_size=1000)
    result = add_service.validate(item)
    assert result["state"] == "needs_confirmation"
    approved = add_service.approve_hold(item, actor="42", approval_generation=result["approval_generation"])
    assert approved["state"] == "enrolled_hold"
    assert "hold" in add_service.qbt.tags(item)
    assert add_service.qbt.started == []


def test_allow_scheduling_is_separate_idempotent_action(add_service):
    item = add_service.enrolled_hold_item()
    first = add_service.allow_scheduling(item, actor="42")
    second = add_service.allow_scheduling(item, actor="42")
    assert first["state"] == "enrolled"
    assert second["state"] == "enrolled"
    assert add_service.qbt.remove_tag_calls.count((item["qbt_hash"], "hold")) == 1
```

Add tests for unique enrollment, definite duplicate cleanup, cancellation, stale approval generation, raw-input expiry, and failure notification dedupe.

- [ ] **Step 2: Run tests and verify failure**

Run: `python -m pytest tests/test_bot_add_queue.py -k 'different_size or allow_scheduling' -v`

Expected: FAIL with missing service methods.

- [ ] **Step 3: Implement `CheckedAddService`**

The service claims only `prechecking` items, reads qBT files, selects/normalizes the primary video, calls `DuplicateMatcher`, and atomically records evidence before qBT tag/category changes. Exact duplicate/cancel removes only the precheck registration. Unique enrollment sets category `auto`, removes precheck/metadata/hold tags, adds `checked`, and leaves the torrent stopped for Planner. Ambiguous approval sets category `auto`, removes precheck/metadata tags, retains `hold`, adds `checked,maybe-duplicate`, and records actor/generation. `allow_scheduling()` removes only `hold`; it never directly starts qBT.

- [ ] **Step 4: Run checked-add queue tests**

Run: `python -m pytest tests/test_bot_add_queue.py -v`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/qbt_orchestrator/checked_add.py src/qbt_orchestrator/bot_add_queue.py tests/test_bot_add_queue.py
git commit -m "Enroll checked downloads through durable confirmation"
```

### Task 8: Render the Telegram panel and route callbacks

**Files:**
- Create: `src/qbt_orchestrator/telegram_ui.py`
- Modify: `src/qbt_orchestrator/integrations/telegram.py`
- Modify: `src/qbt_orchestrator/telegram_control.py`
- Create: `tests/test_telegram_ui.py`
- Modify: `tests/test_runtime_integrations.py`

- [ ] **Step 1: Write failing UI/API tests**

```python
def test_home_panel_uses_natural_language_and_queue_counts():
    view = render_home(HomeView(active=[], free_bytes=3 * 1024**3, total_bytes=68 * 1024**3,
                                capacity_state="capacity_deadlock", safe_reclaim_candidates=0,
                                queue=QueueCounts(checking=1, confirming=2, scheduled=5, failed=0),
                                completed_downloads=28, completed_ingests=24, errors=3, reclaims=0, unread=3))
    assert "当前没有能够安全回收的任务，需要人工处理" in view.text
    assert "capacity_deadlock" not in view.text
    assert "添加队列（8）" in view.text


def test_callback_is_answered_and_edits_same_message(fake_api, router):
    router.handle(callback_update(data="nav:queue:g1", message_id=77, user_id=42))
    assert fake_api.answered == [callback_query_id()]
    assert fake_api.edits[-1]["message_id"] == 77
```

- [ ] **Step 2: Run tests and verify failure**

Run: `python -m pytest tests/test_telegram_ui.py tests/test_runtime_integrations.py -k 'callback or panel' -v`

Expected: FAIL with missing renderers/API methods.

- [ ] **Step 3: Extend the Telegram protocol/client**

Add protocol/client methods `edit_message_text(chat_id: int, message_id: int, text: str, reply_markup: dict | None = None) -> Any`, `answer_callback_query(callback_query_id: str, text: str = "") -> Any`, and `send_document(chat_id: int, filename: str, content: bytes, caption: str = "") -> Any`. `edit_message_text()` calls `_post("editMessageText", payload)` and JSON-encodes `reply_markup`; `answer_callback_query()` calls `_post("answerCallbackQuery", payload)`; `send_document()` uses a random MIME boundary, `Content-Disposition: form-data`, `application/octet-stream`, and `urllib.request` with the existing timeout. Reject content above the configured export byte limit before building multipart data. Never include the Bot token in errors.

- [ ] **Step 4: Implement UI renderers and general callback routing**

Add `start`, `status`, `queue`, `add`, and `warnings` to viewer/operator/admin sets as designed. Route compact callback tokens through persisted generation checks. Render home, queue, draft, confirmation, batch initial summary, and final summary pages. Every callback calls `answerCallbackQuery`; navigation edits the same panel message.

Add a read-only `DashboardRepository` in `telegram_ui.py` that queries active qBT snapshots plus SQLite counts for completed downloads, completed ingests, unresolved errors, automatic reclaims, queue states, and unread warnings. Its processed-history methods return timestamp plus normalized media ID or torrent name for each category. Render at most three active tasks on home and paginate all history/queue lists; no raw mode/reason codes appear outside the technical-detail page.

- [ ] **Step 5: Run Telegram tests**

Run: `python -m pytest tests/test_telegram_ui.py tests/test_runtime_integrations.py -v`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/qbt_orchestrator/telegram_ui.py src/qbt_orchestrator/integrations/telegram.py src/qbt_orchestrator/telegram_control.py tests/test_telegram_ui.py tests/test_runtime_integrations.py
git commit -m "Add Telegram control panel and callbacks"
```

### Task 9: Add warning inbox, native copy summary, and full export

**Files:**
- Create: `src/qbt_orchestrator/warning_inbox.py`
- Modify: `src/qbt_orchestrator/integrations/telegram.py`
- Modify: `tests/test_warning_inbox.py`

- [ ] **Step 1: Write failing inbox/export tests**

```python
def test_warning_reads_are_per_user(inbox):
    warning_id = inbox.upsert("capacity", "critical", "空间不足", related={"hash": "h"})
    inbox.mark_read([warning_id], chat_id="1", user_id="2")
    assert inbox.unread_count("1", "2") == 0
    assert inbox.unread_count("1", "3") == 1


def test_copy_summary_is_256_chars_and_export_is_redacted(inbox):
    inbox.upsert("qbt", "warning", "password=secret token=abc", related={})
    summary = inbox.copy_summary(chat_id="1", user_id="2")
    export = inbox.export_text(chat_id="1", user_id="2", max_rows=1000, max_bytes=512_000)
    assert 1 <= len(summary) <= 256
    assert b"secret" not in export.content
    assert b"abc" not in export.content
```

- [ ] **Step 2: Run tests and verify failure**

Run: `python -m pytest tests/test_warning_inbox.py -v`

Expected: FAIL with missing inbox.

- [ ] **Step 3: Implement inbox and export**

Use a stable `(topic,related identity)` fingerprint for upsert. Store redacted messages only. `copy_summary()` returns at most 256 Unicode characters. `export_text()` orders by severity/time, caps rows and UTF-8 bytes, and appends an explicit truncation line. The sender writes bytes to a `NamedTemporaryFile(delete=False)`, sends it, and removes it in `finally`.

- [ ] **Step 4: Run warning tests**

Run: `python -m pytest tests/test_warning_inbox.py -v`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/qbt_orchestrator/warning_inbox.py src/qbt_orchestrator/integrations/telegram.py tests/test_warning_inbox.py
git commit -m "Persist Telegram warnings and safe exports"
```

### Task 10: Wire workers and validated configuration into the daemon

**Files:**
- Modify: `src/qbt_orchestrator/service.py`
- Modify: `src/qbt_orchestrator/cli.py`
- Modify: `src/qbt_orchestrator/runtime.py`
- Modify: `tests/test_daemon_runtime.py`
- Modify: `tests/test_cli_observability.py`

- [ ] **Step 1: Write failing runtime/config test**

```python
def test_daemon_processes_add_queue_without_blocking_safety_loop(runtime_fixture):
    runtime = runtime_fixture.with_add_queue(slots=3)
    started = runtime.monotonic()
    result = runtime.process_bot_add_queue(max_transitions=20)
    assert result["active_metadata"] <= 3
    assert runtime.monotonic() - started < 1.0


def test_effective_config_reports_batch_and_probe_limits(cli_runtime):
    config = cli_runtime.effective_config()
    assert config["telegram_add"]["max_links_per_batch"] == 500
    assert config["telegram_add"]["metadata_slots"] == 3
    assert config["telegram_add"]["probe_windows_sec"] == [300, 600, 900]
```

- [ ] **Step 2: Run tests and verify failure**

Run: `python -m pytest tests/test_daemon_runtime.py tests/test_cli_observability.py -k 'add_queue or probe_limits' -v`

Expected: FAIL because workers/config are not wired.

- [ ] **Step 3: Add daemon processing hooks**

Create `process_bot_add_queue(max_transitions=20)` that expires drafts/raw inputs, resolves queued links, ticks metadata coordinator, validates ready items, finalizes shard/batch counters, and queues initial/final summaries. Call it from the existing background event worker group; when background workers are disabled, call it once per main loop. It must never sleep.

- [ ] **Step 4: Resolve configuration**

Add validated environment settings with approved defaults:

```text
QBT_ORCH_TELEGRAM_ADD_ENABLED=1
QBT_ORCH_TELEGRAM_ADD_MAX_LINKS=500
QBT_ORCH_TELEGRAM_ADD_SHARD_SIZE=50
QBT_ORCH_TELEGRAM_ADD_MAX_PENDING=1000
QBT_ORCH_TELEGRAM_METADATA_SLOTS=3
QBT_ORCH_TELEGRAM_METADATA_WINDOWS_SEC=300,600,900
QBT_ORCH_TELEGRAM_METADATA_BACKOFFS_SEC=1800,21600
QBT_ORCH_TELEGRAM_METADATA_POLL_SEC=5
QBT_ORCH_TELEGRAM_METADATA_DL_LIMIT_BPS=1024
QBT_ORCH_TELEGRAM_REMOTE_INDEX_TTL_SEC=21600
QBT_ORCH_TELEGRAM_BACKFILL_DB=/opt/qbt/gdrive-backfill/state/backfill.sqlite
```

Reject invalid lengths, negative values, shard sizes above batch size, and slot counts outside 1-8. Include only safe values in effective-config logs.

- [ ] **Step 5: Run runtime/config tests**

Run: `python -m pytest tests/test_daemon_runtime.py tests/test_cli_observability.py -v`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/qbt_orchestrator/service.py src/qbt_orchestrator/cli.py src/qbt_orchestrator/runtime.py tests/test_daemon_runtime.py tests/test_cli_observability.py
git commit -m "Run Telegram add queue in daemon"
```

### Task 11: Full local verification

**Files:**
- Test: all test files

- [ ] **Step 1: Run all focused Telegram tests**

```bash
python -m pytest tests/test_download_links.py tests/test_bot_add_queue.py tests/test_metadata_probe.py tests/test_telegram_ui.py tests/test_warning_inbox.py tests/test_runtime_integrations.py tests/test_daemon_runtime.py tests/test_cli_observability.py -q
```

Expected: all selected tests PASS.

- [ ] **Step 2: Run the full suite**

Run: `python -m pytest -q`

Expected: all tests PASS.

- [ ] **Step 3: Run static repository checks**

```powershell
rg -n "metadata_timeout_sec.?=.?(900|15 \* 60)|metadata-timeout.*observe|time\.sleep\(" src/qbt_orchestrator
git diff --check
git status --short
```

Expected: no new synchronous metadata wait or indefinite observe path; no whitespace errors.

### Task 12: Deploy canary, validate Telegram, and observe one hour

**Files/services:**
- Deploy: `/opt/emby_qbt_auto/releases/$sha`
- Switch: `/opt/emby_qbt_auto/current`
- Service: `qbt-orchestrator-daemon.service`
- Preserve: `/opt/qbt-orchestrator/qbt_add_checked.py` and its existing SQLite tables for rollback/read-only history

- [ ] **Step 1: Archive and upload the verified release**

```powershell
$sha = (git rev-parse --short=12 HEAD).Trim()
New-Item -ItemType Directory -Force artifacts | Out-Null
git archive --format=tar.gz -o "artifacts/emby_qbt_auto-$sha.tar.gz" HEAD
scp "artifacts/emby_qbt_auto-$sha.tar.gz" "paff-vps:/tmp/emby_qbt_auto-$sha.tar.gz"
```

- [ ] **Step 2: Capture pre-deploy evidence and back up live state**

First state the root operations, impact, and rollback. Then use the approved root channel:

```bash
stamp=$(date +%Y%m%d-%H%M%S)
mkdir -p /opt/emby_qbt_auto/backups/$stamp
cp -a /etc/qbt-orchestrator/daemon.env /etc/qbt-orchestrator/config.json \
  /etc/systemd/system/qbt-orchestrator-daemon.service /opt/qbt-orchestrator/qbt_add_checked.py \
  /opt/emby_qbt_auto/backups/$stamp/
sqlite3 /var/lib/qbt-orchestrator/state.sqlite ".backup '/opt/emby_qbt_auto/backups/$stamp/state.sqlite'"
readlink -f /opt/emby_qbt_auto/current > /opt/emby_qbt_auto/backups/$stamp/previous-release
```

- [ ] **Step 3: Install with callbacks disabled and smoke-test read-only UI**

Reuse PowerShell `$sha`:

```powershell
ssh paff-vps "sha='$sha'; mkdir -p /opt/emby_qbt_auto/releases/`$sha; tar -xzf /tmp/emby_qbt_auto-`$sha.tar.gz -C /opt/emby_qbt_auto/releases/`$sha; ln -sfn /opt/emby_qbt_auto/releases/`$sha /opt/emby_qbt_auto/current.new; mv -Tf /opt/emby_qbt_auto/current.new /opt/emby_qbt_auto/current; systemctl restart qbt-orchestrator-daemon.service"
```

Set `QBT_ORCH_TELEGRAM_ADD_ENABLED=0` for the first restart. Verify `/start`, status, warning counts, `editMessageText`, callback answers, service health, and qBT authentication without adding a torrent.

- [ ] **Step 4: Enable add callbacks and run the canary batch**

Set `QBT_ORCH_TELEGRAM_ADD_ENABLED=1`, restart only the daemon, and submit from the authorized admin chat:

```text
one valid unique magnet
one exact duplicate magnet
one invalid HTTP URL
one magnet known not to return metadata within five minutes
```

If a safe same-ID/different-size canary exists, include it and verify `仍然添加（保持暂停）` leaves `hold` present. Do not force any canary to start; Planner remains the sole download authority.

- [ ] **Step 5: Verify database and qBT isolation**

```bash
sqlite3 -json /var/lib/qbt-orchestrator/state.sqlite \
  "select id,batch_id,state,decision,qbt_hash,metadata_probe_attempt,metadata_retry_at,last_error from bot_add_items order by id desc limit 20;"
sqlite3 -json /var/lib/qbt-orchestrator/state.sqlite \
  "select id,state,received_count,enrolled_count,duplicate_count,confirmation_count,failed_count from bot_add_batches order by id desc limit 5;"
journalctl -u qbt-orchestrator-daemon.service --since '-20 minutes' --no-pager
```

Expected: at most three active metadata items, timeout item releases its slot and enters retry wait, duplicates are not enrolled, and unique/approved tasks remain stopped until Planner selects them.

- [ ] **Step 6: Observe one hour and leave persistent if healthy**

Every five minutes verify service restarts, Telegram poll failures, queue transitions, notification retries, SQLite errors, qBT tags/categories, metadata slot count, and disk free space. Expected: zero unexpected daemon restarts, no unhandled exceptions, no indefinite `observe` task, and correct initial/final summaries.

- [ ] **Step 7: Roll back on acceptance failure**

```bash
previous=$(cat /opt/emby_qbt_auto/backups/$stamp/previous-release)
ln -sfn "$previous" /opt/emby_qbt_auto/current.rollback
mv -Tf /opt/emby_qbt_auto/current.rollback /opt/emby_qbt_auto/current
systemctl restart qbt-orchestrator-daemon.service
systemctl is-active qbt-orchestrator-daemon.service
```

Leave additive tables intact. Do not remove already enrolled torrents automatically; list any precheck torrents for explicit review.
