# qBT Orchestrator Production Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 修复 qBT 编排器在真实 VPS 上暴露的调度阻塞、同步错误、预算账本漂移、批次状态漂移、重试/清理隐患，并建立只告警不自动处理的容量死锁保护。

**Architecture:** 保留现有 SQLite durable queue 和模块边界，先用小补丁恢复 Safety Loop，再逐步引入持久 qBT session、独立周期 worker、future-growth 预算账本、集中式 planner 和严格 upload/cleanup 状态机。所有高风险行为通过 feature flag、shadow plan 和 canary 逐步启用；容量死锁仅进入状态、阻止新探索并告警，绝不自动删除本地数据或修改 `hold`。

**Tech Stack:** Python 3.11+、SQLite WAL、qBittorrent Web API v2、rclone、systemd、pytest。

---

## Scope and non-goals

- 不自动删除当前 `/data/downloads/incomplete` 数据。
- 不自动删除 qBT torrent，不自动解除 `hold`，不自动执行碎片淘汰。
- 不在本计划中处理当前 VPS 的物理容量恢复；只实现可检测、可解释、可告警的 `capacity_deadlock`。
- 每个迭代必须可单独发布和回滚，禁止一次性替换整个 daemon。

## Target file structure

New files:

- `src/qbt_orchestrator/snapshot_store.py`: 原始 qBT snapshot merge、不可变快照发布。
- `src/qbt_orchestrator/periodic.py`: 固定频率周期 worker、deadline/overrun 统计。
- `src/qbt_orchestrator/action_dispatcher.py`: qBT 写操作串行化和 emergency 优先级。
- `src/qbt_orchestrator/budget.py`: future-growth 预算、动态 guard、模式滞回。
- `src/qbt_orchestrator/work_items.py`: 统一调度 work item 和候选评分。
- `src/qbt_orchestrator/scheduler_engine.py`: 全局候选选择、shadow/live plan。
- `src/qbt_orchestrator/capacity_state.py`: capacity deadlock 状态机和聚合告警。
- `tests/test_snapshot_store.py`
- `tests/test_periodic_runtime.py`
- `tests/test_budget_engine.py`
- `tests/test_scheduler_engine.py`
- `tests/test_capacity_state.py`
- `tests/test_production_invariants.py`

Major modified files:

- `src/qbt_orchestrator/service.py`: runtime 拆分、集中计划应用。
- `src/qbt_orchestrator/qbt_sync.py`: raw delta merge。
- `src/qbt_orchestrator/integrations/qbt.py`: 持久 session、线程安全限速、sync 指标。
- `src/qbt_orchestrator/db.py`: 持久 writer connection、schema v3、批量事务。
- `src/qbt_orchestrator/planner.py`: 逐步降级为候选构建和兼容适配器。
- `src/qbt_orchestrator/soak_queue.py`: partial debt cap、模式门禁。
- `src/qbt_orchestrator/file_batch.py`: 快速预算门禁、全局 batch 候选、lease reconcile。
- `src/qbt_orchestrator/runtime.py`: job lease、phase、retry、优先级。
- `src/qbt_orchestrator/upload.py`: copy/verify/cleanup 分阶段。
- `src/qbt_orchestrator/maintenance.py`: lease recovery、状态清理和 retention。
- `src/qbt_orchestrator/preferences.py`: drift 去噪。
- `src/qbt_orchestrator/path_reconcile.py`: 稳定 identity 去重。
- `src/qbt_orchestrator/junk_janitor.py`: round-robin 和 transition logging。
- `src/qbt_orchestrator/media.py`: Emby transient retry。
- `deploy/systemd/qbt-orchestrator-daemon.env.example`: 新 feature flags。

---

## Iteration 1: Restore the Safety Loop without changing allocation policy

### Task 1: Add a global fast gate before batch inventory calls

**Files:**
- Modify: `src/qbt_orchestrator/file_batch.py`
- Modify: `src/qbt_orchestrator/service.py`
- Test: `tests/test_file_batch_service.py`
- Test: `tests/test_daemon_runtime.py`

- [ ] **Step 1: Write a failing test proving insufficient global budget makes zero heavy qBT calls**

```python
def test_file_batch_skips_all_inventory_calls_when_global_budget_is_below_minimum(db):
    qbt = FakeQbt()
    qbt.files_by_hash = {"h1": [{"index": 0, "name": "A.mp4", "size": 5 * 1024**3}]}
    svc = FileBatchService(
        db,
        dry_run=False,
        qbt=qbt,
        executor=FakeExecutor(),
        batch_pipeline_enabled=True,
        disk_floor_bytes=2 * 1024**3,
        filesystem_slack_bytes=128 * 1024**2,
    )
    snapshots = {"h1": {"hash": "h1", "category": "auto", "state": "stoppedDL", "size": 5 * 1024**3, "amount_left": 5 * 1024**3}}

    result = svc.sync_completed(snapshots, free_bytes=2 * 1024**3 + 120 * 1024**2, sync_healthy=True, scheduler_mode="drain")

    assert qbt.heavy_calls == []
    assert result.batches_created == 0
    assert result.batches_blocked == 1
    assert result.blocked_reasons == {"mode_disallows_batch": 1}
```

- [ ] **Step 2: Run the test and confirm it fails**

Run: `python -m pytest tests/test_file_batch_service.py::test_file_batch_skips_all_inventory_calls_when_global_budget_is_below_minimum -v`

Expected: FAIL because `scheduler_mode` and `blocked_reasons` do not exist and qBT inventory is called.

- [ ] **Step 3: Add the fast gate**

```python
@dataclass(frozen=True)
class FileBatchResult:
    scanned: int
    eligible: int
    enqueued: int
    skipped_existing: int
    dry_run: int = 0
    batches_created: int = 0
    batches_blocked: int = 0
    blocked_reasons: dict[str, int] = field(default_factory=dict)

def _batch_admission_allowed(self, free_bytes: int, scheduler_mode: str) -> tuple[bool, str]:
    if scheduler_mode in {"drain", "emergency"}:
        return False, "mode_disallows_batch"
    minimum = self.filesystem_slack_bytes + 32 * 1024**2
    if self._safe_batch_budget(free_bytes) < minimum:
        return False, "global_batch_budget_below_minimum"
    return True, "ok"
```

Call this once at the start of `sync_completed()`. Continue scanning completed full torrents for disk-releasing uploads, but skip `_maybe_create_pipeline_batch()` for incomplete torrents when admission is false.

- [ ] **Step 4: Add a daemon test proving the Safety thread is not delayed by batch inventory when drain mode is active**

```python
def test_daemon_drain_file_batch_does_not_call_torrent_files(db):
    qbt = FakeQbt()
    runtime = build_runtime(db, qbt=qbt, free_bytes=int(2.2 * 1024**3), batch_pipeline_enabled=True)
    result = runtime.file_batch_tick()
    assert qbt.heavy_calls == []
    assert result["batches_blocked"] >= 1
```

- [ ] **Step 5: Run focused and full tests**

Run:

```bash
python -m pytest tests/test_file_batch_service.py tests/test_daemon_runtime.py -q
python -m pytest -q
```

Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/qbt_orchestrator/file_batch.py src/qbt_orchestrator/service.py tests/test_file_batch_service.py tests/test_daemon_runtime.py
git commit -m "fix: short-circuit batch inventory under disk pressure"
```

### Task 2: Add loop duration and deadline-miss metrics before moving threads

**Files:**
- Modify: `src/qbt_orchestrator/service.py`
- Modify: `src/qbt_orchestrator/db.py`
- Test: `tests/test_daemon_runtime.py`

- [ ] **Step 1: Write a failing test for duration and overrun recording**

```python
def test_loop_task_records_duration_and_deadline_miss(db):
    clock = FakeClock([0.0, 0.0, 7.5])
    task = LoopTask("file_batch", 60, lambda: {"ok": True}, max_runtime_sec=5)
    runtime = build_runtime(db, monotonic=clock)
    runtime.loop_tasks = [task]
    runtime.run_due_loop_tasks()
    row = read_last_metric(db, "loop_runtime:file_batch")
    assert row["duration_ms"] == 7500
    assert row["deadline_missed"] is True
```

- [ ] **Step 2: Run the test and confirm it fails**

Run: `python -m pytest tests/test_daemon_runtime.py::test_loop_task_records_duration_and_deadline_miss -v`

Expected: FAIL because `max_runtime_sec` and loop runtime metrics are absent.

- [ ] **Step 3: Implement bounded metrics**

```python
@dataclass
class LoopTask:
    name: str
    interval_sec: float
    callback: Callable[[], object]
    next_due: float = 0.0
    max_runtime_sec: float = 1.0

def _loop_metric(self, task: LoopTask, duration: float) -> None:
    self.obs.metric(
        f"loop_runtime:{task.name}",
        {
            "duration_ms": int(duration * 1000),
            "deadline_missed": duration > task.max_runtime_sec,
            "max_runtime_ms": int(task.max_runtime_sec * 1000),
        },
    )
```

Set limits: planner 2s, file_batch 5s, maintenance 5s, carousel 2s. Do not insert one metric per Safety tick; keep a rolling aggregate row or one sample per minute.

- [ ] **Step 4: Run tests and commit**

```bash
python -m pytest tests/test_daemon_runtime.py -q
python -m pytest -q
git add src/qbt_orchestrator/service.py src/qbt_orchestrator/db.py tests/test_daemon_runtime.py
git commit -m "feat: record scheduler deadline misses"
```

---

## Iteration 2: Make qBT synchronization correct and measurable

### Task 3: Merge partial delta payloads instead of replacing snapshots

**Files:**
- Create: `src/qbt_orchestrator/snapshot_store.py`
- Modify: `src/qbt_orchestrator/qbt_sync.py`
- Test: `tests/test_snapshot_store.py`
- Test: `tests/test_new_system_behaviors.py`

- [ ] **Step 1: Write the regression test reproducing the current category/size loss**

```python
def test_partial_delta_preserves_unchanged_torrent_fields():
    store = TorrentRawSnapshotStore()
    store.replace_full({"h": {"name": "A", "category": "auto", "tags": "auto", "amount_left": 100, "size": 200, "progress": 0.5}})
    store.apply_delta({"h": {"dlspeed": 123}}, removed=[])
    snap = store.snapshots()["h"]
    assert snap.category == "auto"
    assert snap.amount_left == 100
    assert snap.size == 200
    assert snap.dlspeed_bps == 123
```

- [ ] **Step 2: Run and confirm failure**

Run: `python -m pytest tests/test_snapshot_store.py::test_partial_delta_preserves_unchanged_torrent_fields -v`

Expected: FAIL because `TorrentRawSnapshotStore` does not exist.

- [ ] **Step 3: Implement the raw merge store**

```python
class TorrentRawSnapshotStore:
    def __init__(self) -> None:
        self._raw: dict[str, dict[str, Any]] = {}
        self._lock = threading.RLock()

    def replace_full(self, torrents: Mapping[str, Mapping[str, Any]]) -> None:
        with self._lock:
            self._raw = {str(h): {**dict(row), "hash": str(h)} for h, row in torrents.items()}

    def apply_delta(self, torrents: Mapping[str, Mapping[str, Any]], removed: Iterable[str]) -> None:
        with self._lock:
            for h in removed:
                self._raw.pop(str(h), None)
            for h, delta in torrents.items():
                key = str(h)
                self._raw[key] = {**self._raw.get(key, {}), **dict(delta), "hash": key}

    def snapshots(self) -> dict[str, TorrentSnapshot]:
        with self._lock:
            return {h: TorrentSnapshot.from_qbt(dict(row)) for h, row in self._raw.items()}
```

Update `QbtSyncCache.poll_once()` to call `replace_full()` or `apply_delta()` and then publish snapshots.

- [ ] **Step 4: Test removal, full replacement, and suspect-full behavior**

Add tests proving:

```python
assert "removed" not in store.snapshots()
assert store.replace_full({"new": {"size": 1}}) is None
assert set(store.snapshots()) == {"new"}
```

- [ ] **Step 5: Run tests and commit**

```bash
python -m pytest tests/test_snapshot_store.py tests/test_new_system_behaviors.py -q
python -m pytest -q
git add src/qbt_orchestrator/snapshot_store.py src/qbt_orchestrator/qbt_sync.py tests/test_snapshot_store.py tests/test_new_system_behaviors.py
git commit -m "fix: merge qbt partial sync deltas"
```

### Task 4: Detect broken sync sessions and use a persistent authenticated session

**Files:**
- Modify: `src/qbt_orchestrator/integrations/qbt.py`
- Modify: `src/qbt_orchestrator/cli.py`
- Modify: `deploy/systemd/qbt-orchestrator-daemon.env.example`
- Test: `tests/test_runtime_integrations.py`
- Test: `tests/test_cli_observability.py`

- [ ] **Step 1: Write a failing session test**

```python
def test_qbt_sync_session_reuses_sid_and_observes_delta():
    transport = FakeQbtSessionTransport(
        login_cookie="SID=abc",
        responses=[
            {"rid": 10, "full_update": True, "torrents": {"h": {"size": 1}}},
            {"rid": 11, "full_update": False, "torrents": {"h": {"dlspeed": 2}}},
        ],
    )
    client = QbtHttpClient(username="u", password="p", transport=transport, auth_mode="required")
    first = client.get_maindata(0)
    second = client.get_maindata(first["rid"])
    assert second["full_update"] is False
    assert transport.request_cookies == ["SID=abc", "SID=abc"]
```

- [ ] **Step 2: Add sync-session health tracking**

```python
@dataclass
class SyncSessionStats:
    full_updates: int = 0
    delta_updates: int = 0
    repeated_full_updates: int = 0

    def observe(self, full: bool, previous_rid: int, new_rid: int) -> None:
        if full:
            self.full_updates += 1
            if previous_rid > 0:
                self.repeated_full_updates += 1
        else:
            self.delta_updates += 1
```

After three consecutive full updates with nonzero previous rid, emit `sync_session_degraded`. If credentials/session are unavailable, keep correctness by accepting full snapshots but increase sync interval to a configured degraded interval rather than claiming delta operation.

- [ ] **Step 3: Add explicit configuration**

```text
QBT_ORCH_QBT_AUTH_MODE=required
QBT_ORCH_SYNC_DEGRADED_INTERVAL_SEC=10
QBT_ORCH_SYNC_REPEATED_FULL_LIMIT=3
```

Do not log username, password, SID or cookie values.

- [ ] **Step 4: Make the token bucket thread-safe**

```python
class TokenBucket:
    def __init__(self, ...):
        ...
        self._lock = threading.Lock()

    def acquire(self) -> None:
        with self._lock:
            self._acquire_locked()
```

The lock must cover token calculation; release it while sleeping, then reacquire and recompute tokens to avoid blocking unrelated callers for the full sleep duration.

- [ ] **Step 5: Run tests and commit**

```bash
python -m pytest tests/test_runtime_integrations.py tests/test_cli_observability.py -q
python -m pytest -q
git add src/qbt_orchestrator/integrations/qbt.py src/qbt_orchestrator/cli.py deploy/systemd/qbt-orchestrator-daemon.env.example tests/test_runtime_integrations.py tests/test_cli_observability.py
git commit -m "fix: maintain qbt sync session and expose degradation"
```

---

## Iteration 3: Isolate periodic work from Safety

### Task 5: Introduce fixed-rate periodic workers and immutable snapshots

**Files:**
- Create: `src/qbt_orchestrator/periodic.py`
- Create: `src/qbt_orchestrator/action_dispatcher.py`
- Modify: `src/qbt_orchestrator/service.py`
- Modify: `src/qbt_orchestrator/executor.py`
- Test: `tests/test_periodic_runtime.py`
- Test: `tests/test_daemon_runtime.py`

- [ ] **Step 1: Write a test proving a 40-second file batch cannot delay Safety**

```python
def test_blocking_inventory_worker_does_not_delay_safety_ticks():
    safety = CountingCallback()
    blocker = BlockingCallback(duration=40)
    runtime = ThreadedRuntime(
        safety_interval=2,
        safety_callback=safety,
        periodic_tasks=[PeriodicTask("file_batch", 60, blocker)],
        clock=VirtualClock(),
    )
    runtime.run_for(10)
    assert safety.call_times == [0, 2, 4, 6, 8, 10]
```

- [ ] **Step 2: Implement fixed-rate scheduling**

```python
@dataclass(frozen=True)
class PeriodicTask:
    name: str
    interval_sec: float
    callback: Callable[[], object]

class PeriodicWorker:
    def run(self) -> None:
        next_due = self.monotonic()
        while not self.stop_event.is_set():
            now = self.monotonic()
            if now < next_due:
                self.stop_event.wait(next_due - now)
                continue
            self._run_once()
            missed = max(0, int((self.monotonic() - next_due) // self.task.interval_sec))
            next_due += (missed + 1) * self.task.interval_sec
```

Missed periods are skipped, not replayed in a burst.

- [ ] **Step 3: Separate workers**

Create worker groups:

- Safety: 2 seconds, dedicated thread, only sync/disk/emergency action.
- Planner: 15 seconds, dedicated thread.
- Inventory: file batch/observe/junk, 60 seconds.
- Maintenance: 300 seconds.
- Carousel: 1800 seconds.
- Existing event workers remain separate.

All workers read a copied immutable snapshot from `TorrentRawSnapshotStore`; no worker directly mutates the shared dict.

- [ ] **Step 4: Serialize qBT writes with emergency priority**

```python
class ActionPriority(IntEnum):
    EMERGENCY = 0
    CONTROL = 10
    MAINTENANCE = 20

@dataclass(order=True)
class DispatchedAction:
    priority: int
    sequence: int
    path: str = field(compare=False)
    payload: dict[str, Any] = field(compare=False)
```

Emergency stop bypasses normal queue backlog by priority but still uses the same single qBT write dispatcher.

- [ ] **Step 5: Run concurrency tests**

Run:

```bash
python -m pytest tests/test_periodic_runtime.py tests/test_daemon_runtime.py -q
python -m pytest -q
```

Expected: Safety cadence remains correct while inventory and maintenance block.

- [ ] **Step 6: Commit**

```bash
git add src/qbt_orchestrator/periodic.py src/qbt_orchestrator/action_dispatcher.py src/qbt_orchestrator/service.py src/qbt_orchestrator/executor.py tests/test_periodic_runtime.py tests/test_daemon_runtime.py
git commit -m "refactor: isolate safety from periodic workers"
```

---

## Iteration 4: Remove SQLite write amplification and log only transitions

### Task 6: Keep one writer connection open and batch planner writes

**Files:**
- Modify: `src/qbt_orchestrator/db.py`
- Modify: `src/qbt_orchestrator/planner.py`
- Modify: `src/qbt_orchestrator/soak_queue.py`
- Test: `tests/test_db_actor_full_write_queue.py`
- Test: `tests/test_download_planner.py`

- [ ] **Step 1: Write a test proving one connection serves multiple transactions**

```python
def test_sync_write_actor_reuses_one_connection(db, monkeypatch):
    opened = []
    monkeypatch.setattr(db_module, "_connect", lambda path: opened.append(path) or real_connect(path))
    write_execute(db, "insert into events_v2(ts) values(?)", (1,))
    write_execute(db, "insert into events_v2(ts) values(?)", (2,))
    flush_write_actor(db)
    assert len(opened) == 1
```

- [ ] **Step 2: Refactor `_SyncWriteActor._run()`**

Open the connection once before the worker loop, set WAL/busy timeout once, use `commit()`/`rollback()` per transaction, and close in `finally` when the actor stops.

- [ ] **Step 3: Add one atomic planner commit API**

```python
@dataclass(frozen=True)
class PlannerPersistenceBatch:
    allocations: list[dict[str, Any]]
    health_rows: list[dict[str, Any]]
    reservation_upserts: list[dict[str, Any]]
    reservation_releases: list[str]
    decisions: list[dict[str, Any]]

def persist_planner_batch(state_db: Path, batch: PlannerPersistenceBatch) -> None:
    def txn(con: sqlite3.Connection) -> None:
        con.executemany(ALLOCATION_UPSERT_SQL, [allocation_params(x) for x in batch.allocations])
        con.executemany(HEALTH_UPSERT_SQL, [health_params(x) for x in batch.health_rows])
        apply_reservations(con, batch)
        con.executemany(DECISION_INSERT_SQL, [decision_params(x) for x in batch.decisions])
    write_transaction(state_db, txn)
```

Replace per-hash `_allocation()` and `_decision()` writes inside the planner loop with in-memory collection followed by one commit.

- [ ] **Step 4: Add a test bounding planner write transactions**

```python
def test_planner_uses_at_most_two_write_transactions_for_one_tick(db, write_counter):
    planner = build_planner(db, torrent_count=100)
    planner.plan_and_apply(...)
    assert write_counter.count <= 2
```

- [ ] **Step 5: Run tests and commit**

```bash
python -m pytest tests/test_db_actor_full_write_queue.py tests/test_download_planner.py tests/test_soak_queue.py -q
python -m pytest -q
git add src/qbt_orchestrator/db.py src/qbt_orchestrator/planner.py src/qbt_orchestrator/soak_queue.py tests/test_db_actor_full_write_queue.py tests/test_download_planner.py
git commit -m "perf: batch scheduler sqlite writes"
```

### Task 7: Record decisions only when state changes

**Files:**
- Modify: `src/qbt_orchestrator/db.py`
- Create: `src/qbt_orchestrator/decision_recorder.py`
- Modify: `src/qbt_orchestrator/planner.py`
- Modify: `src/qbt_orchestrator/file_batch.py`
- Modify: `src/qbt_orchestrator/observe_promotion.py`
- Modify: `src/qbt_orchestrator/junk_janitor.py`
- Modify: `src/qbt_orchestrator/maintenance.py`
- Test: `tests/test_production_invariants.py`

- [ ] **Step 1: Add schema and failing transition test**

```sql
create table if not exists decision_state(
  component text not null,
  hash text not null default '',
  decision text not null,
  reason_code text not null,
  data_fingerprint text not null,
  updated_at integer not null,
  primary key(component,hash)
);
create index if not exists idx_events_component_type_ts on events_v2(component,event_type,ts);
create index if not exists idx_decisions_component_hash_ts on decision_log(component,hash,ts);
create index if not exists idx_decisions_ts_id on decision_log(ts,id);
```

```python
def test_same_decision_is_logged_once_until_transition(db):
    recorder = DecisionRecorder(db, now=lambda: 100)
    assert recorder.record("planner", "h", "soak", "budget") is True
    assert recorder.record("planner", "h", "soak", "budget") is False
    assert recorder.record("planner", "h", "active", "budget_fit") is True
    assert count_decisions(db) == 2
```

- [ ] **Step 2: Implement stable fingerprinting**

```python
def stable_fingerprint(data: Mapping[str, Any], ignored: set[str] = frozenset({"progress", "free_bytes", "budget_bytes"})) -> str:
    stable = {k: data[k] for k in sorted(data) if k not in ignored}
    return hashlib.sha256(json.dumps(stable, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
```

- [ ] **Step 3: Replace per-torrent repeated blocks with one aggregate metric**

For file batch and observe promotion, write one `metrics_snapshots` row per loop:

```json
{"batch_budget_insufficient": 71, "metadata_not_ready": 5, "sample_hashes": ["h1", "h2", "h3"]}
```

Keep at most three sample hashes and write individual events only on state transition.

- [ ] **Step 4: Extend retention**

Add `junk_janitor_events` to bounded retention and delete in batches using its indexed `ts` column.

- [ ] **Step 5: Run a virtual 24-hour test**

```python
def test_unchanged_100_torrent_day_produces_bounded_log_rows(db):
    simulate_ticks(db, planner_ticks=5760, file_batch_ticks=1440, unchanged_torrents=100)
    assert table_count(db, "decision_log") <= 500
    assert table_count(db, "metrics_snapshots") <= 8000
```

- [ ] **Step 6: Run tests and commit**

```bash
python -m pytest tests/test_production_invariants.py tests/test_maintenance.py -q
python -m pytest -q
git add src/qbt_orchestrator/db.py src/qbt_orchestrator/decision_recorder.py src/qbt_orchestrator/planner.py src/qbt_orchestrator/file_batch.py src/qbt_orchestrator/observe_promotion.py src/qbt_orchestrator/junk_janitor.py src/qbt_orchestrator/maintenance.py tests/test_production_invariants.py tests/test_maintenance.py
git commit -m "perf: log scheduler transitions instead of repeated decisions"
```

---

## Iteration 5: Introduce a correct resource ledger and manual-only capacity deadlock state

### Task 8: Separate future growth from current pinned inventory

**Files:**
- Create: `src/qbt_orchestrator/budget.py`
- Modify: `src/qbt_orchestrator/db.py`
- Modify: `src/qbt_orchestrator/runtime.py`
- Modify: `src/qbt_orchestrator/file_batch.py`
- Test: `tests/test_budget_engine.py`

- [ ] **Step 1: Add schema columns**

```sql
alter table resource_reservations add column accounting_class text not null default 'future_growth';
alter table resource_reservations add column owner text;
alter table resource_reservations add column lease_generation integer not null default 0;
alter table resource_reservations add column last_observed_at integer;
update resource_reservations set accounting_class='current_pinned' where kind='cleanup_pending';
```

Migration must tolerate columns already existing.

- [ ] **Step 2: Write the double-count regression test**

```python
def test_current_pinned_inventory_is_not_subtracted_from_df_free():
    claims = [
        ResourceClaim("h1", "cleanup_pending", AccountingClass.CURRENT_PINNED, 5 * GIB),
        ResourceClaim("h2", "active_download", AccountingClass.FUTURE_GROWTH, 1 * GIB),
    ]
    budget = calculate_growth_budget(free_bytes=10 * GIB, emergency_floor_bytes=2 * GIB, dynamic_guard_bytes=1 * GIB, claims=claims)
    assert budget.available_growth_bytes == 6 * GIB
    assert budget.current_pinned_bytes == 5 * GIB
```

- [ ] **Step 3: Implement the budget types**

```python
class AccountingClass(str, Enum):
    FUTURE_GROWTH = "future_growth"
    CURRENT_PINNED = "current_pinned"

@dataclass(frozen=True)
class ResourceClaim:
    hash: str
    kind: str
    accounting_class: AccountingClass
    bytes: int

@dataclass(frozen=True)
class GrowthBudget:
    free_bytes: int
    emergency_floor_bytes: int
    dynamic_guard_bytes: int
    future_growth_reserved_bytes: int
    current_pinned_bytes: int
    available_growth_bytes: int
```

Preserve existing same-hash overlap rule only among future-growth `active_download` and `batch` claims.

- [ ] **Step 4: Add dynamic guard**

```python
def dynamic_guard_bytes(
    min_guard_bytes: int,
    ingress_p99_bps: int,
    control_p99_sec: float,
    stop_grace_sec: float,
    max_piece_size: int,
    filesystem_slack_bytes: int,
) -> int:
    rate_guard = math.ceil(ingress_p99_bps * (control_p99_sec + stop_grace_sec))
    return max(min_guard_bytes, rate_guard + 2 * max_piece_size + filesystem_slack_bytes)
```

If metrics are absent, use a configured conservative ingress rate rather than zero.

- [ ] **Step 5: Run tests and commit**

```bash
python -m pytest tests/test_budget_engine.py tests/test_runtime_repositories.py tests/test_file_batch_service.py -q
python -m pytest -q
git add src/qbt_orchestrator/budget.py src/qbt_orchestrator/db.py src/qbt_orchestrator/runtime.py src/qbt_orchestrator/file_batch.py tests/test_budget_engine.py
git commit -m "feat: separate future growth and pinned inventory accounting"
```

### Task 9: Add scheduler mode hysteresis and manual-only capacity deadlock

**Files:**
- Create: `src/qbt_orchestrator/capacity_state.py`
- Modify: `src/qbt_orchestrator/db.py`
- Modify: `src/qbt_orchestrator/service.py`
- Modify: `src/qbt_orchestrator/alerts.py`
- Test: `tests/test_capacity_state.py`

- [ ] **Step 1: Add persistent state**

```sql
create table if not exists capacity_state(
  id integer primary key check(id=1),
  scheduler_mode text not null,
  state text not null,
  entered_at integer not null,
  last_evaluated_at integer not null,
  reason text,
  details_json text not null default '{}'
);
```

- [ ] **Step 2: Write hysteresis and deadlock tests**

```python
def test_drain_mode_requires_exit_watermark_to_recover():
    c = ModeController(emergency_enter=1.5*GIB, drain_enter=3*GIB, drain_exit=5*GIB, explore_enter=8*GIB)
    assert c.next_mode("normal", 2.9*GIB) == "drain"
    assert c.next_mode("drain", 4.9*GIB) == "drain"
    assert c.next_mode("drain", 5.1*GIB) == "normal"

def test_capacity_deadlock_never_creates_delete_or_hold_actions():
    result = detect_capacity_state(mode="drain", managed_incomplete=10, feasible_full_finish=0, disk_releasing_jobs=0)
    assert result.state == "capacity_deadlock"
    assert result.actions == []
```

- [ ] **Step 3: Implement deadlock detection**

```python
def detect_capacity_state(*, mode: str, managed_incomplete: int, feasible_full_finish: int, disk_releasing_jobs: int) -> CapacityResult:
    if mode == "drain" and managed_incomplete > 0 and feasible_full_finish == 0 and disk_releasing_jobs == 0:
        return CapacityResult("capacity_deadlock", "no_finishable_or_releasing_work", actions=[])
    return CapacityResult("progress_possible", "feasible_work_exists", actions=[])
```

On transition into deadlock, enqueue one deduplicated Telegram notification containing only counts, required minimum growth and top manual candidates. Do not enqueue cleanup/delete/config commands.

- [ ] **Step 4: Add startup effective-config snapshot**

Write a redacted event containing resolved thresholds and feature flags so live behavior can be audited without reading the root-only environment file.

- [ ] **Step 5: Run tests and commit**

```bash
python -m pytest tests/test_capacity_state.py tests/test_daemon_runtime.py -q
python -m pytest -q
git add src/qbt_orchestrator/capacity_state.py src/qbt_orchestrator/db.py src/qbt_orchestrator/service.py src/qbt_orchestrator/alerts.py tests/test_capacity_state.py
git commit -m "feat: detect capacity deadlock without automatic cleanup"
```

---

## Iteration 6: Replace competing planners with one global scheduler

### Task 10: Introduce work items and a deterministic capacity-constrained selector

**Files:**
- Create: `src/qbt_orchestrator/work_items.py`
- Create: `src/qbt_orchestrator/scheduler_engine.py`
- Modify: `src/qbt_orchestrator/planner.py`
- Modify: `src/qbt_orchestrator/service.py`
- Test: `tests/test_scheduler_engine.py`
- Test: `tests/test_download_planner.py`

- [ ] **Step 1: Define work item types**

```python
class WorkKind(str, Enum):
    FULL_FINISH = "full_finish"
    BATCH_DELIVERY = "batch_delivery"
    SOAK_PROBE = "soak_probe"

@dataclass(frozen=True)
class WorkItem:
    id: str
    hash: str
    kind: WorkKind
    incremental_growth_bytes: int
    releasable_bytes: int
    pinned_after_success_bytes: int
    completion_probability: float
    throughput_bps: int
    wait_age_sec: int
    operator_priority: int
    hold: bool = False
```

- [ ] **Step 2: Write selection tests**

```python
def test_drain_selects_finish_and_release_work_not_probe_or_batch():
    items = [
        WorkItem("finish", "a", WorkKind.FULL_FINISH, 300*MIB, 6*GIB, 0, 0.8, 2*MIB, 3600, 0),
        WorkItem("probe", "b", WorkKind.SOAK_PROBE, 128*MIB, 0, 128*MIB, 0.9, 4*MIB, 7200, 0),
        WorkItem("batch", "c", WorkKind.BATCH_DELIVERY, 200*MIB, 0, 5*GIB, 0.9, 4*MIB, 7200, 0),
    ]
    plan = SchedulerEngine(unit_bytes=64*MIB).select(items, mode="drain", available_growth_bytes=400*MIB, max_slots=2)
    assert [x.id for x in plan.selected] == ["finish"]

def test_hold_is_never_selected_automatically():
    item = WorkItem("held", "h", WorkKind.FULL_FINISH, 1, 10*GIB, 0, 1.0, 1, 999999, 100, hold=True)
    assert SchedulerEngine().select([item], "normal", 10*GIB, 5).selected == []
```

- [ ] **Step 3: Implement deterministic bounded DP**

Use 64 MiB capacity units and state `(used_slots, used_units)`. Utility must be deterministic:

```python
def utility(item: WorkItem) -> int:
    relief_ratio = min(500_000, item.releasable_bytes * 1000 // max(1, item.incremental_growth_bytes))
    probability = int(max(0.0, min(1.0, item.completion_probability)) * 100_000)
    age = min(50_000, item.wait_age_sec // 60)
    throughput = min(50_000, item.throughput_bps // 1024)
    return item.operator_priority * 1_000_000 + relief_ratio + probability + age + throughput
```

Tie-break by lower growth bytes, then stable hash. In drain mode filter to `FULL_FINISH`. In emergency mode select nothing.

- [ ] **Step 4: Build candidates from snapshots**

For `FULL_FINISH`:

```text
incremental_growth_bytes = amount_left + piece_uncertainty
releasable_bytes = current_local_bytes + amount_left only when remote verify and seed policy can eventually permit cleanup
```

Use `completed_bytes` as conservative current-local estimate until file inventory provides a better figure.

- [ ] **Step 5: Add shadow mode**

```text
QBT_ORCH_SCHEDULER_ENGINE=legacy|shadow|live
```

`shadow` computes and persists comparison metrics but applies only the legacy plan. Record selected hash differences, budget differences and unsafe-plan rejection counts.

- [ ] **Step 6: Run tests and commit**

```bash
python -m pytest tests/test_scheduler_engine.py tests/test_download_planner.py tests/test_daemon_runtime.py -q
python -m pytest -q
git add src/qbt_orchestrator/work_items.py src/qbt_orchestrator/scheduler_engine.py src/qbt_orchestrator/planner.py src/qbt_orchestrator/service.py tests/test_scheduler_engine.py tests/test_download_planner.py tests/test_daemon_runtime.py
git commit -m "feat: add global capacity constrained scheduler"
```

### Task 11: Make the central planner the only allocation owner

**Files:**
- Modify: `src/qbt_orchestrator/planner.py`
- Modify: `src/qbt_orchestrator/soak_queue.py`
- Modify: `src/qbt_orchestrator/carousel.py`
- Modify: `src/qbt_orchestrator/file_batch.py`
- Modify: `src/qbt_orchestrator/db.py`
- Test: `tests/test_scheduler_engine.py`
- Test: `tests/test_carousel.py`
- Test: `tests/test_soak_queue.py`

- [x] **Step 1: Add intent and generation fields**

```sql
alter table scheduler_allocations add column owner text not null default 'legacy';
alter table scheduler_allocations add column plan_generation integer not null default 0;
create table if not exists scheduler_intents(
  component text not null,
  hash text not null,
  intent text not null,
  priority integer not null,
  expires_at integer,
  data_json text not null,
  primary key(component,hash)
);
```

- [x] **Step 2: Change soak/carousel/batch to emit intents**

```python
Intent(component="soak", hash=h, intent="probe", priority=30, expires_at=now+120, data={"exposure_bytes": exposure})
Intent(component="carousel", hash=h, intent="availability_probe", priority=40, expires_at=now+1800, data={})
Intent(component="batch", hash=h, intent="protect_batch", priority=20, expires_at=lease_until, data={"batch_id": batch_id})
```

These modules must stop writing `scheduler_allocations` directly.

- [x] **Step 3: Apply one generation atomically**

The central planner reads all nonexpired intents, produces final allocations, writes one `plan_generation`, then dispatcher applies only actions for that generation. An older worker result cannot overwrite a newer generation.

- [x] **Step 4: Fix never-seen-swarm dead detection**

Initialize `no_swarm_since` at first health observation when seeds and peers are both zero. Mark dead when both no-swarm and no-progress durations exceed the configured threshold, including torrents that have never seen swarm.

- [x] **Step 5: Run tests and commit**

```bash
python -m pytest tests/test_scheduler_engine.py tests/test_carousel.py tests/test_soak_queue.py tests/test_download_planner.py -q
python -m pytest -q
git add src/qbt_orchestrator/planner.py src/qbt_orchestrator/soak_queue.py src/qbt_orchestrator/carousel.py src/qbt_orchestrator/file_batch.py src/qbt_orchestrator/db.py tests/test_scheduler_engine.py tests/test_carousel.py tests/test_soak_queue.py tests/test_download_planner.py
git commit -m "refactor: centralize scheduler allocation ownership"
```

---

## Iteration 7: Bound future partial debt without automatic eviction

### Task 12: Gate soak by mode, swarm and partial-debt limits

**Files:**
- Modify: `src/qbt_orchestrator/soak_queue.py`
- Modify: `src/qbt_orchestrator/cli.py`
- Modify: `deploy/systemd/qbt-orchestrator-daemon.env.example`
- Test: `tests/test_soak_queue.py`

- [x] **Step 1: Add tests**

```python
def test_soak_never_starts_new_resident_in_drain_mode(db):
    result = build_soak(db).run_once(snapshots_with_candidates(), free_bytes=4*GIB, sync_healthy=True, scheduler_mode="drain")
    assert result.started == []
    assert result.blocked_reason == "mode_disallows_new_probe"

def test_soak_blocks_zero_swarm_candidate_and_respects_partial_debt_cap(db):
    candidates = [torrent("a", seeds=0, peers=0), torrent("b", seeds=2, peers=3)]
    svc = build_soak(db, max_cold_partial_bytes=1*GIB, current_cold_partial_bytes=1*GIB)
    result = svc.run_once(candidates, free_bytes=10*GIB, sync_healthy=True, scheduler_mode="normal")
    assert result.started == []
```

- [x] **Step 2: Add configuration**

```text
QBT_ORCH_SOAK_ALLOWED_MODES=normal,explore
QBT_ORCH_SOAK_REQUIRE_SWARM=1
QBT_ORCH_MAX_COLD_PARTIAL_GB=4
QBT_ORCH_MAX_COLD_PARTIAL_TORRENTS=8
QBT_ORCH_SOAK_MAX_NEW_PER_HOUR=4
```

- [x] **Step 3: Track debt but never delete**

Estimate cold partial bytes from stopped incomplete snapshots using `completed_bytes`, persist the aggregate metric, and block new probes when either cap is reached. Existing data remains untouched.

- [x] **Step 4: Remove recovery exposure zeroing**

Existing residents kept in recovery must retain a future-growth claim. If the claim does not fit, emit an intent to stop; never preserve a running resident with `exposure_bytes=0`.

- [x] **Step 5: Run tests and commit**

```bash
python -m pytest tests/test_soak_queue.py tests/test_daemon_runtime.py -q
python -m pytest -q
git add src/qbt_orchestrator/soak_queue.py src/qbt_orchestrator/cli.py deploy/systemd/qbt-orchestrator-daemon.env.example tests/test_soak_queue.py tests/test_daemon_runtime.py
git commit -m "fix: bound soak partial debt and disable probes in drain mode"
```

---

## Iteration 8: Repair batch lifecycle and global selection

### Task 13: Add renewable batch leases and file-index ownership

**Files:**
- Modify: `src/qbt_orchestrator/db.py`
- Modify: `src/qbt_orchestrator/file_batch.py`
- Modify: `src/qbt_orchestrator/maintenance.py`
- Test: `tests/test_file_batch_service.py`
- Test: `tests/test_maintenance.py`

- [x] **Step 1: Add schema**

```sql
alter table torrent_batches add column lease_until integer;
alter table torrent_batches add column last_progress_at integer;
alter table torrent_batches add column last_progress_bytes integer not null default 0;
alter table torrent_batches add column source_present integer not null default 1;
create table if not exists batch_file_claims(
  batch_id integer not null,
  hash text not null,
  file_index integer not null,
  state text not null,
  created_at integer not null,
  released_at integer,
  primary key(batch_id,file_index)
);
create unique index if not exists idx_batch_file_claim_active
on batch_file_claims(hash,file_index)
where state='active';
```

- [x] **Step 2: Test lease renewal and expiry semantics**

```python
def test_batch_progress_renews_lease_and_expiry_never_silently_releases(db):
    batch = create_batch(db, lease_until=100, last_progress_bytes=10)
    reconcile_batch(db, batch, observed_progress_bytes=20, now=90)
    assert get_batch(db, batch)["lease_until"] > 100
    reconcile_batch(db, batch, observed_progress_bytes=20, now=1000)
    assert get_batch(db, batch)["state"] == "suspect_expired"
    assert active_claim_count(db, batch) == 1
```

- [x] **Step 3: Reconcile before release**

Rules:

- progress advanced: renew lease and reservation;
- qBT source absent: mark `source_absent`, release claims/reservations;
- stopped by central planner: set selected priorities to 0, then mark paused and release future-growth claim;
- expired without observation: mark `suspect_expired`, block new batch work for that hash;
- paused batch retains historical file ownership until priorities are confirmed reset.

- [x] **Step 4: Fix piece spill accounting**

```python
def compute_batch_reservation(files, piece_size, filesystem_slack, selected_extents):
    payload = sum(int(f["remaining_bytes"]) for f in files)
    spill = 2 * int(piece_size) * max(1, int(selected_extents))
    reserved = payload + spill + int(filesystem_slack)
    return BatchReservation(payload, spill, filesystem_slack, reserved, payload / reserved if reserved else 1.0)
```

- [x] **Step 5: Clean stale DB rows without touching disk**

Maintenance may mark absent-qBT nonterminal batches `source_absent` and release logical claims. It must not call qBT delete, unlink or move files.

- [x] **Step 6: Run tests and commit**

```bash
python -m pytest tests/test_file_batch_service.py tests/test_maintenance.py -q
python -m pytest -q
git add src/qbt_orchestrator/db.py src/qbt_orchestrator/file_batch.py src/qbt_orchestrator/maintenance.py src/qbt_orchestrator/policies/batching.py src/qbt_orchestrator/service.py tests/test_file_batch_service.py tests/test_maintenance.py docs/
git commit -m "fix: make batch leases renewable and reconcile ownership"
```

Implemented verification: targeted batch/maintenance regression `37 passed`; full suite `300 passed`; `compileall` and `git diff --check` passed. Legacy batches lazily backfill file-index claims on first observation. Terminal reservation history is never reactivated. Source-absence maintenance is enabled only from a healthy complete qBT snapshot and performs logical state release only.

### Task 14: Build batch candidates globally and treat them as delivery-only work

**Files:**
- Modify: `src/qbt_orchestrator/db.py`
- Modify: `src/qbt_orchestrator/file_batch.py`
- Modify: `src/qbt_orchestrator/work_items.py`
- Modify: `src/qbt_orchestrator/scheduler_engine.py`
- Modify: `src/qbt_orchestrator/service.py`
- Modify: `src/qbt_orchestrator/cli.py`
- Test: `tests/test_file_batch_service.py`
- Test: `tests/test_scheduler_engine.py`
- Test: `tests/test_cli_observability.py`

- [x] **Step 1: Add a test proving snapshot order cannot change batch selection**

```python
def test_global_batch_selection_is_independent_of_snapshot_order(db):
    first = build_global_batch_plan(db, snapshots={"large_cost": t1, "high_value": t2})
    second = build_global_batch_plan(db, snapshots={"high_value": t2, "large_cost": t1})
    assert first.selected_ids == second.selected_ids
```

- [x] **Step 2: Split discovery from selection**

`FileBatchService` returns `WorkItem(kind=BATCH_DELIVERY)` candidates. The central scheduler selects across all torrents. Batch candidates must have `releasable_bytes=0` and nonzero `pinned_after_success_bytes`, so drain mode rejects them automatically.

- [x] **Step 3: Add bounded rotating inventory**

Cache `piece_size` for the life of a torrent. Refresh file lists only for:

- torrents whose state/progress changed;
- hashes selected by a persisted round-robin cursor;
- at most `QBT_ORCH_BATCH_INVENTORY_LIMIT` hashes per minute.

Default limit: 8.

- [x] **Step 4: Run tests and commit**

```bash
python -m pytest tests/test_file_batch_service.py tests/test_scheduler_engine.py -q
python -m pytest -q
git add src/qbt_orchestrator/db.py src/qbt_orchestrator/file_batch.py src/qbt_orchestrator/work_items.py src/qbt_orchestrator/scheduler_engine.py src/qbt_orchestrator/service.py src/qbt_orchestrator/cli.py tests/test_file_batch_service.py tests/test_scheduler_engine.py tests/test_cli_observability.py docs/
git commit -m "refactor: allocate delivery batches globally"
```

Implementation notes: discovery reads bounded cached qBT inventory and produces `BATCH_DELIVERY` work items; the shared deterministic `SchedulerEngine` selects all torrent candidates together before any reservation or `filePrio` mutation. Delivery work declares zero releasable bytes and nonzero pinned-after-success bytes, and invalid delivery semantics are rejected. Migration 7 persists a per-minute round-robin cursor plus inventory/piece-size cache. `QBT_ORCH_BATCH_INVENTORY_LIMIT` defaults to 8.

Implemented verification: focused batch/scheduler/CLI regression `63 passed`; full suite `304 passed`; `compileall`, `git diff --check`, and destructive-call scan passed.

---

## Iteration 9: Harden upload, verification, cleanup and job recovery

### Task 15: Split upload copy, verify and cleanup phases

**Files:**
- Modify: `src/qbt_orchestrator/db.py`
- Modify: `src/qbt_orchestrator/runtime.py`
- Modify: `src/qbt_orchestrator/upload.py`
- Modify: `src/qbt_orchestrator/integrations/rclone.py`
- Modify: `src/qbt_orchestrator/file_batch.py`
- Modify: `src/qbt_orchestrator/service.py`
- Test: `tests/test_runtime_repositories.py`
- Test: `tests/test_new_system_behaviors.py`

- [x] **Step 1: Add upload phase and cleanup job types**

```sql
alter table torrent_jobs add column phase text;
```

Phases:

```text
queued_copy → copying → copied → verifying → verified → cleanup_wait → done
```

Use a distinct `cleanup_full_torrent` job for destructive qBT deletion.

- [x] **Step 2: Test verify retry does not copy again**

```python
def test_verify_retry_does_not_repeat_copy(db):
    rclone = FakeRclone(copy_ok=True, verify_results=[False, True])
    runner = build_upload_runner(db, rclone)
    runner.run_next()
    runner.run_next()
    assert rclone.copy_calls == 1
    assert rclone.verify_calls == 2
```

- [x] **Step 3: Add manifest verification**

```python
@dataclass(frozen=True)
class VerifyResult:
    verified: bool
    method: str
    mismatches: list[str]

def verify_manifest(self, files: list[dict[str, Any]], remote_root: str) -> VerifyResult:
    # Prefer backend hashes when all expected files expose compatible hashes.
    # Otherwise require exact relative-path and size equality for every file.
```

Persist the verification method and result. Cleanup cannot be enqueued unless verification is true.

- [x] **Step 4: Run tests and commit**

```bash
python -m pytest tests/test_runtime_repositories.py tests/test_new_system_behaviors.py -q
python -m pytest -q
git add src/qbt_orchestrator/db.py src/qbt_orchestrator/runtime.py src/qbt_orchestrator/upload.py src/qbt_orchestrator/integrations/rclone.py src/qbt_orchestrator/file_batch.py src/qbt_orchestrator/service.py tests/test_runtime_repositories.py tests/test_new_system_behaviors.py docs/
git commit -m "refactor: separate upload copy verify and cleanup phases"
```

Implementation notes: migration 8 adds durable phase/copy/verification fields plus an idempotent parent-child relation for `cleanup_full_torrent`. Both repository and DbActor upload entrypoints start at `queued_copy`; legacy `verify_pending` rows backfill to `verifying`. Copy completion is durable, so verification retries and verification exceptions never repeat copy. Manifest verification prefers one compatible backend hash across every expected file, otherwise requires an exact relative-path set and size match. Full-torrent verification only queues a dormant cleanup job; UploadWorker never calls qBT delete, and Task 16 owns cleanup policy/execution.

Implemented verification: focused runtime/upload regression `45 passed`; full suite `307 passed`; `compileall`, `git diff --check`, and upload delete-call scan passed.

### Task 16: Enforce seed policy and make disk-releasing uploads highest priority

**Files:**
- Create: `src/qbt_orchestrator/cleanup_policy.py`
- Modify: `src/qbt_orchestrator/file_batch.py`
- Modify: `src/qbt_orchestrator/runtime.py`
- Modify: `src/qbt_orchestrator/io_governor.py`
- Test: `tests/test_runtime_repositories.py`
- Test: `tests/test_file_batch_service.py`

- [ ] **Step 1: Define cleanup eligibility**

```python
@dataclass(frozen=True)
class CleanupEligibility:
    allowed: bool
    reason: str
    next_check_at: int | None

def cleanup_eligibility(torrent, *, remote_verified: bool, min_seed_sec: int, min_ratio: float) -> CleanupEligibility:
    if not remote_verified:
        return CleanupEligibility(False, "remote_not_verified", None)
    if "seed-long" in tags(torrent):
        return CleanupEligibility(False, "seed_long", None)
    if int(torrent.get("seeding_time") or 0) < min_seed_sec:
        return CleanupEligibility(False, "seed_time", int(time.time()) + 300)
    if float(torrent.get("ratio") or 0) < min_ratio:
        return CleanupEligibility(False, "ratio", int(time.time()) + 300)
    return CleanupEligibility(True, "policy_satisfied", None)
```

- [ ] **Step 2: Assign explicit priorities**

```python
class JobPriority(IntEnum):
    EMERGENCY_CONTROL = 0
    FULL_TORRENT_RELEASE_UPLOAD = 10
    PREEMPTION_RELEASE_UPLOAD = 15
    BATCH_DELIVERY_UPLOAD = 50
    SIDECAR_UPLOAD = 70
    MEDIA_PIPELINE = 80
```

Upload backpressure may block batch/sidecar work but must not block a verified disk-releasing full-torrent upload.

- [ ] **Step 3: Test seed-long and transient seed wait**

```python
def test_verified_seed_long_torrent_is_never_auto_deleted():
    decision = cleanup_eligibility({"tags": "auto,seed-long", "seeding_time": 999999, "ratio": 99}, remote_verified=True, min_seed_sec=900, min_ratio=1.0)
    assert decision.allowed is False
    assert decision.reason == "seed_long"
```

- [ ] **Step 4: Run tests and commit**

```bash
python -m pytest tests/test_runtime_repositories.py tests/test_file_batch_service.py -q
python -m pytest -q
git add src/qbt_orchestrator/cleanup_policy.py src/qbt_orchestrator/file_batch.py src/qbt_orchestrator/runtime.py src/qbt_orchestrator/io_governor.py tests/test_runtime_repositories.py tests/test_file_batch_service.py
git commit -m "feat: enforce cleanup seed policy and release priorities"
```

### Task 17: Automatically recover leases and enforce attempts

**Files:**
- Modify: `src/qbt_orchestrator/runtime.py`
- Modify: `src/qbt_orchestrator/maintenance.py`
- Modify: `src/qbt_orchestrator/media.py`
- Test: `tests/test_runtime_repositories.py`
- Test: `tests/test_maintenance.py`

- [ ] **Step 1: Restrict claim queries**

```sql
select * from torrent_jobs
where job_type=?
  and attempts < max_attempts
  and state in ('queued','verify_pending','retry_wait')
  and (state!='retry_wait' or next_run_at is null or next_run_at<=?)
order by priority,id
limit 1;
```

- [ ] **Step 2: Run job reconcile during maintenance**

Expired `running` leases move to `retry_wait` with bounded exponential backoff. Exhausted attempts move to `failed`. This logic must execute automatically every maintenance tick.

- [ ] **Step 3: Make Emby errors retryable**

Classify path validation errors as permanent `blocked`; HTTP timeout, connection error and 5xx become `retry_wait` with backoff and max attempts.

- [ ] **Step 4: Add tests**

```python
def test_transient_emby_failure_retries_but_invalid_root_is_blocked(db):
    transient = build_emby_worker(db, error=TimeoutError())
    transient.run_next()
    assert refresh_state(db) == "retry_wait"
    invalid = build_emby_worker(db, path="/media/gcrypt")
    invalid.run_next()
    assert refresh_state(db) == "blocked"
```

- [ ] **Step 5: Run tests and commit**

```bash
python -m pytest tests/test_runtime_repositories.py tests/test_maintenance.py tests/test_media_pipeline_persistence.py -q
python -m pytest -q
git add src/qbt_orchestrator/runtime.py src/qbt_orchestrator/maintenance.py src/qbt_orchestrator/media.py tests/test_runtime_repositories.py tests/test_maintenance.py tests/test_media_pipeline_persistence.py
git commit -m "fix: recover job leases and retry transient emby failures"
```

---

## Iteration 10: Fix auxiliary drift, fairness and stale-state handling

### Task 18: Remove repetitive preference/path/janitor noise

**Files:**
- Modify: `src/qbt_orchestrator/preferences.py`
- Modify: `src/qbt_orchestrator/path_reconcile.py`
- Modify: `src/qbt_orchestrator/junk_janitor.py`
- Modify: `src/qbt_orchestrator/maintenance.py`
- Test: `tests/test_qbt_preferences_guard.py`
- Test: `tests/test_qbt_path_reconcile.py`
- Test: `tests/test_junk_janitor.py`

- [ ] **Step 1: Treat desired `None` as unmanaged**

When `desired_incomplete_files_ext is None`, return an `observed` field but do not add drift or warning.

- [ ] **Step 2: Stabilize path-drift identity**

```python
def drift_identity(drift: Mapping[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(drift.get("hash") or ""),
        str(drift.get("reason") or ""),
        str(drift.get("save_path") or ""),
        str(drift.get("content_path") or ""),
    )
```

Exclude progress and free-space fields from dedupe identity.

- [ ] **Step 3: Persist a round-robin junk cursor**

```sql
create table if not exists scan_cursors(
  scanner text primary key,
  last_hash text,
  updated_at integer not null
);
```

Select the next N sorted hashes after `last_hash`, wrapping once. Repeated skipped junk events use transition logging.

- [ ] **Step 4: Add a read-only missing-files audit**

Maintenance records aggregate unmanaged `missingFiles` counts and hash samples. It must not remove torrents or files.

- [ ] **Step 5: Reconcile carousel before applying disk guard**

Always expire/stop existing carousel probes first; disk guard only prevents starting new probes.

- [ ] **Step 6: Run tests and commit**

```bash
python -m pytest tests/test_qbt_preferences_guard.py tests/test_qbt_path_reconcile.py tests/test_junk_janitor.py tests/test_carousel.py -q
python -m pytest -q
git add src/qbt_orchestrator/preferences.py src/qbt_orchestrator/path_reconcile.py src/qbt_orchestrator/junk_janitor.py src/qbt_orchestrator/maintenance.py src/qbt_orchestrator/carousel.py tests/test_qbt_preferences_guard.py tests/test_qbt_path_reconcile.py tests/test_junk_janitor.py tests/test_carousel.py
git commit -m "fix: dedupe drift events and rotate maintenance scans"
```

---

## Iteration 11: Production simulation and release gates

### Task 19: Add invariant and long-run simulation tests

**Files:**
- Create: `tests/test_production_invariants.py`
- Modify: `tests/fakes.py`
- Modify: `docs/traceability/requirements-map.md`

- [ ] **Step 1: Add hard invariants**

```python
def assert_plan_invariants(plan, budget):
    assert sum(x.incremental_growth_bytes for x in plan.selected) <= budget.available_growth_bytes
    assert all(not x.hold for x in plan.selected)
    assert not (plan.mode == "drain" and any(x.kind != WorkKind.FULL_FINISH for x in plan.selected))
    assert not (plan.capacity_state == "capacity_deadlock" and plan.actions)
```

- [ ] **Step 2: Simulate 24 hours with 100 torrents**

The virtual-time simulation must cover:

- repeated zero-swarm torrents;
- partial qBT delta payloads;
- file inventory latency of 40 seconds;
- batch progress longer than one hour;
- upload verify retry;
- disk crossing mode thresholds;
- no feasible recovery candidate.

Assertions:

```python
assert simulation.safety_gap_p99 <= 3.0
assert simulation.safety_gap_max <= 5.0
assert simulation.planner_runtime_p95 <= 0.5
assert simulation.repeated_decision_rows <= 500
assert simulation.unowned_active_batch_claims == 0
assert simulation.automatic_delete_actions_in_deadlock == 0
```

- [ ] **Step 3: Add qBT API call budgets**

Steady-state expectation for 100 unchanged torrents:

```python
assert fake_qbt.calls_per_minute("torrents/files") <= 8
assert fake_qbt.calls_per_minute("torrents/properties") <= 8
assert fake_qbt.delta_ratio >= 0.99
```

- [ ] **Step 4: Run full test suite and commit**

```bash
python -m pytest tests/test_production_invariants.py -v
python -m pytest -q
git add tests/test_production_invariants.py tests/fakes.py docs/traceability/requirements-map.md
git commit -m "test: add production scheduler invariants"
```

### Task 20: Add shadow/canary rollout and rollback documentation

**Files:**
- Modify: `deploy/systemd/qbt-orchestrator-daemon.env.example`
- Create: `docs/operations/scheduler-v3-rollout.md`
- Modify: `deploy/scripts/run-dry-run.sh`
- Modify: `deploy/scripts/rollback.sh`

- [ ] **Step 1: Define feature flags**

```text
QBT_ORCH_SCHEDULER_ENGINE=legacy
QBT_ORCH_PERIODIC_WORKERS=0
QBT_ORCH_RESOURCE_LEDGER_V2=0
QBT_ORCH_BATCH_LEASES_V2=0
QBT_ORCH_UPLOAD_PHASES_V2=0
QBT_ORCH_CAPACITY_DEADLOCK_ALERTS=1
```

- [ ] **Step 2: Document staged rollout**

Stages:

1. Deploy with all new behavior flags off; migration and tests only.
2. Enable transition logging and metrics.
3. Enable periodic workers; verify Safety P99 for 24 hours.
4. Enable qBT delta session; verify delta ratio for 24 hours.
5. Enable resource ledger in shadow; require zero unsafe divergence for 24 hours.
6. Enable scheduler shadow for 48 hours.
7. Enable scheduler live for an allowlisted torrent set.
8. Enable soak/batch v2 only after planner metrics remain healthy.
9. Enable upload phases v2 after copy/verify shadow comparison succeeds.

- [ ] **Step 3: Define rollback checks**

Rollback switches `QBT_ORCH_SCHEDULER_ENGINE=legacy` and disables new worker/ledger flags. Schema additions remain additive; no downgrade migration or state deletion is required.

- [ ] **Step 4: Add release acceptance commands**

```bash
python -m pytest -q
python -m qbt_orchestrator.cli migrate --dry-run --state-db /tmp/qbt-v3.sqlite
python -m qbt_orchestrator.cli once --dry-run --state-db /tmp/qbt-v3.sqlite
python -m qbt_orchestrator.cli status --json --state-db /tmp/qbt-v3.sqlite
```

Expected:

- all tests pass;
- migration dry-run reports additive statements only;
- once dry-run emits no qBT delete or filesystem mutation action;
- status includes loop deadlines, sync full/delta counts, future-growth budget, pinned inventory and capacity state.

- [ ] **Step 5: Commit**

```bash
git add deploy/systemd/qbt-orchestrator-daemon.env.example docs/operations/scheduler-v3-rollout.md deploy/scripts/run-dry-run.sh deploy/scripts/rollback.sh
git commit -m "docs: add scheduler v3 rollout and rollback gates"
```

---

## Final release acceptance criteria

- Safety interval P99 `< 3s`, maximum `< 5s` over 24 hours.
- Planner runtime P95 `< 500ms`.
- qBT sync: one startup full update, subsequent delta ratio `>= 99%`; degraded mode must be explicit.
- Heavy inventory API calls `<= 8` hashes/minute by default.
- Planner persistence `<= 2` write transactions/tick.
- Repeated unchanged torrent decisions do not create repeated `decision_log` rows.
- `current_pinned` is reported but not subtracted twice from `df` free space.
- Drain mode never starts soak, carousel or delivery-only batch work.
- Capacity deadlock never emits delete, cleanup, hold-removal or filesystem mutation actions.
- No active batch claim exists without a present qBT source or a `suspect_expired` reconcile block.
- Normal upload cannot exceed `max_attempts`; expired leases are recovered automatically.
- Full-torrent cleanup requires remote verification and seed policy approval.
- All schema changes are additive and legacy scheduler rollback remains available.
