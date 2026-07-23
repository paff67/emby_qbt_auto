# Shared Capacity Assessment and Reclaim Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Planner, capacity-state detection, and live reclaim consume one fenced capacity assessment so persistently nonviable partial torrents can be reclaimed safely without depending on `desired_state='dead'`.

**Architecture:** Add a focused `capacity_assessment.py` module containing immutable per-torrent evidence, the aggregate assessment, and its SQLite store. `DaemonRuntime.planner_tick()` builds and commits one assessment generation, passes it to Planner and the reclaimer, and persists the same generation in capacity state. Reclaim selection uses `reclaimable_since` plus current assessment evidence and repeats all destructive safety checks immediately before deletion.

**Tech Stack:** Python 3.11, stdlib dataclasses/SQLite, pytest, qBittorrent Web API v5.1.4, systemd immutable releases.

---

## File map

- Create `src/qbt_orchestrator/capacity_assessment.py`: immutable assessment model, builder, and generation/reclaimability store.
- Create `tests/test_capacity_assessment.py`: assessment and persistence unit tests.
- Modify `src/qbt_orchestrator/db.py`: additive schema migration.
- Modify `src/qbt_orchestrator/capacity_state.py`: derive aggregate observation and persist assessment generation.
- Modify `src/qbt_orchestrator/planner.py`: exclude assessment-nonviable torrents from ordinary finish candidates.
- Modify `src/qbt_orchestrator/capacity_reclaim.py`: assessment-driven candidate selection, cooldown checks, and generation fencing.
- Modify `src/qbt_orchestrator/service.py`: build/commit/pass one assessment per planner tick.
- Modify `src/qbt_orchestrator/alerts.py`: natural-language no-candidate warning with episode dedupe.
- Modify `src/qbt_orchestrator/cli.py`: validated environment defaults and construction.
- Modify `tests/test_capacity_state.py`, `tests/test_capacity_reclaim.py`, `tests/test_download_planner.py`, `tests/test_daemon_runtime.py`, `tests/test_cli_observability.py`: integration and regression coverage.

### Task 1: Add the assessment and reclaim evidence schema

**Files:**
- Modify: `src/qbt_orchestrator/db.py`
- Test: `tests/test_capacity_assessment.py`

- [ ] **Step 1: Write the failing migration test**

```python
from qbt_orchestrator.db import migrate, readonly_connect


def test_capacity_assessment_schema_is_additive(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    con = readonly_connect(db)
    try:
        health = {row[1] for row in con.execute("pragma table_info(torrent_health)")}
        capacity = {row[1] for row in con.execute("pragma table_info(capacity_state)")}
        reclaims = {row[1] for row in con.execute("pragma table_info(capacity_reclaims)")}
        assert {"reclaimable_since", "capacity_viable", "capacity_reason", "capacity_assessed_at", "capacity_generation"} <= health
        assert "assessment_generation" in capacity
        assert {"reclaimable_since", "capacity_generation", "capacity_reason", "assessment_json"} <= reclaims
        row = con.execute("select current_generation from capacity_assessment_state where id=1").fetchone()
        assert row is None
    finally:
        con.close()
```

- [ ] **Step 2: Run the test and verify it fails**

Run: `python -m pytest tests/test_capacity_assessment.py::test_capacity_assessment_schema_is_additive -v`

Expected: FAIL because the new columns/table do not exist.

- [ ] **Step 3: Append migration 15 statements**

Add these statements to `migration_sql()` before the migration marker inserts:

```python
"alter table torrent_health add column reclaimable_since integer",
"alter table torrent_health add column capacity_viable integer",
"alter table torrent_health add column capacity_reason text",
"alter table torrent_health add column capacity_assessed_at integer",
"alter table torrent_health add column capacity_generation integer",
"create table if not exists capacity_assessment_state("
"id integer primary key check(id=1),current_generation integer not null,"
"observed_at integer not null,summary_json text not null default '{}')",
"alter table capacity_state add column assessment_generation integer not null default 0",
"alter table capacity_reclaims add column reclaimable_since integer",
"alter table capacity_reclaims add column capacity_generation integer",
"alter table capacity_reclaims add column capacity_reason text",
"alter table capacity_reclaims add column assessment_json text",
"insert or ignore into schema_migrations(version,name,applied_at) "
"values(15,'shared_capacity_assessment_v1',strftime('%s','now'))",
```

- [ ] **Step 4: Run migration tests**

Run: `python -m pytest tests/test_capacity_assessment.py::test_capacity_assessment_schema_is_additive tests/test_production_invariants.py -v`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/qbt_orchestrator/db.py tests/test_capacity_assessment.py
git commit -m "Add shared capacity assessment schema"
```

### Task 2: Build immutable per-torrent capacity evidence

**Files:**
- Create: `src/qbt_orchestrator/capacity_assessment.py`
- Test: `tests/test_capacity_assessment.py`

- [ ] **Step 1: Write failing builder tests**

```python
from qbt_orchestrator.capacity_assessment import CapacityAssessmentBuilder


def test_leech_peer_does_not_make_incomplete_torrent_viable():
    assessment = CapacityAssessmentBuilder(viability_stale_sec=1800).build(
        {"h": {"hash": "h", "category": "auto", "amount_left": 900, "completed": 100,
               "availability": 0.5, "num_seeds": 0, "num_complete": 0, "num_peers": 4}},
        {"h": {"completed_bytes": 100, "progress": 0.1, "no_progress_since": 100}},
        observed_at=7300,
        scheduler_mode="drain",
        free_bytes=100,
        target_free_bytes=1000,
        available_growth_bytes=100,
        selected_hashes=set(),
        disk_releasing_jobs=0,
    )
    item = assessment.torrents["h"]
    assert item.viable is False
    assert item.viability_reason == "stale_without_complete_source"
    assert item.complete_sources == 0


def test_unknown_availability_is_not_reclaimable_evidence():
    assessment = CapacityAssessmentBuilder(viability_stale_sec=1800).build(
        {"h": {"hash": "h", "category": "auto", "amount_left": 900, "completed": 100,
               "availability": -1, "num_seeds": 0, "num_peers": 0}},
        {"h": {"completed_bytes": 100, "progress": 0.1, "no_progress_since": 100}},
        observed_at=7300, scheduler_mode="drain", free_bytes=100,
        target_free_bytes=1000, available_growth_bytes=100,
        selected_hashes=set(), disk_releasing_jobs=0,
    )
    assert assessment.torrents["h"].availability is None
```

- [ ] **Step 2: Run the tests and verify they fail**

Run: `python -m pytest tests/test_capacity_assessment.py -k 'leech_peer or unknown_availability' -v`

Expected: FAIL with missing module/class.

- [ ] **Step 3: Implement the immutable model and builder**

Create `src/qbt_orchestrator/capacity_assessment.py` with these public types and rules:

```python
from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Any, Mapping


@dataclass(frozen=True)
class TorrentCapacityEvidence:
    hash: str
    managed: bool
    incomplete: bool
    amount_left: int
    completed_bytes: int
    availability: float | None
    complete_sources: int
    no_progress_since: int | None
    viable: bool
    viability_reason: str


@dataclass(frozen=True)
class CapacityAssessment:
    generation: int
    observed_at: int
    scheduler_mode: str
    free_bytes: int
    target_free_bytes: int
    available_growth_bytes: int
    selected_hashes: frozenset[str]
    disk_releasing_jobs: int
    torrents: Mapping[str, TorrentCapacityEvidence] = field(default_factory=dict)

    @property
    def managed_incomplete(self) -> int:
        return sum(1 for item in self.torrents.values() if item.managed and item.incomplete)

    @property
    def viable_finish(self) -> int:
        return sum(1 for item in self.torrents.values() if item.managed and item.incomplete and item.viable)

    @property
    def nonviable_finish(self) -> int:
        return self.managed_incomplete - self.viable_finish

    def with_generation(self, generation: int) -> "CapacityAssessment":
        return replace(self, generation=int(generation))


class CapacityAssessmentBuilder:
    def __init__(self, viability_stale_sec: int = 1800):
        self.viability_stale_sec = max(0, int(viability_stale_sec))

    def build(self, snapshots, health_by_hash, *, observed_at, scheduler_mode,
              free_bytes, target_free_bytes, available_growth_bytes,
              selected_hashes, disk_releasing_jobs):
        items: dict[str, TorrentCapacityEvidence] = {}
        for fallback_hash, raw in snapshots.items():
            torrent = dict(raw)
            torrent_hash = str(torrent.get("hash") or fallback_hash)
            tags = {part.strip() for part in str(torrent.get("tags") or "").split(",") if part.strip()}
            managed = (str(torrent.get("category") or "") == "auto" or "auto" in tags) and "hold" not in tags
            amount_left = max(0, int(torrent.get("amount_left") or 0))
            raw_availability = torrent.get("availability")
            availability = None if raw_availability is None or float(raw_availability) < 0 else float(raw_availability)
            complete_sources = max(0, int(torrent.get("num_seeds") or 0), int(torrent.get("num_complete") or 0))
            health = dict(health_by_hash.get(torrent_hash) or {})
            no_progress_since = health.get("no_progress_since")
            dlspeed = max(0, int(torrent.get("dlspeed_bps") or torrent.get("dlspeed") or 0))
            has_complete_source = complete_sources > 0 or (availability is not None and availability >= 1.0)
            recent_progress = dlspeed > 0 or no_progress_since is None or int(observed_at) - int(no_progress_since) < self.viability_stale_sec
            viable = has_complete_source or recent_progress
            reason = "complete_source" if has_complete_source else "recent_progress" if recent_progress else "stale_without_complete_source"
            items[torrent_hash] = TorrentCapacityEvidence(
                hash=torrent_hash, managed=managed, incomplete=amount_left > 0,
                amount_left=amount_left,
                completed_bytes=max(0, int(torrent.get("completed_bytes") or torrent.get("completed") or torrent.get("downloaded") or 0)),
                availability=availability, complete_sources=complete_sources,
                no_progress_since=None if no_progress_since is None else int(no_progress_since),
                viable=bool(viable), viability_reason=reason,
            )
        return CapacityAssessment(
            generation=0, observed_at=int(observed_at), scheduler_mode=str(scheduler_mode),
            free_bytes=max(0, int(free_bytes)), target_free_bytes=max(0, int(target_free_bytes)),
            available_growth_bytes=max(0, int(available_growth_bytes)),
            selected_hashes=frozenset(str(item) for item in selected_hashes),
            disk_releasing_jobs=max(0, int(disk_releasing_jobs)), torrents=items,
        )
```

- [ ] **Step 4: Run the builder tests**

Run: `python -m pytest tests/test_capacity_assessment.py -k 'leech_peer or unknown_availability' -v`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/qbt_orchestrator/capacity_assessment.py tests/test_capacity_assessment.py
git commit -m "Build immutable capacity assessments"
```

### Task 3: Persist generations and independent reclaimable time

**Files:**
- Modify: `src/qbt_orchestrator/capacity_assessment.py`
- Test: `tests/test_capacity_assessment.py`

- [ ] **Step 1: Write failing store tests**

```python
from qbt_orchestrator.capacity_assessment import CapacityAssessmentStore


def test_reclaimable_since_survives_scheduler_state_changes(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    assessment = nonviable_assessment(observed_at=30_000, no_progress_since=1_000)
    first = CapacityAssessmentStore(db, min_no_progress_sec=21_600).commit(assessment)
    second = CapacityAssessmentStore(db, min_no_progress_sec=21_600).commit(
        nonviable_assessment(observed_at=30_300, no_progress_since=1_000)
    )
    con = readonly_connect(db)
    row = con.execute("select reclaimable_since,capacity_generation from torrent_health where hash='h'").fetchone()
    con.close()
    assert first.generation == 1
    assert second.generation == 2
    assert row["reclaimable_since"] == 30_000
    assert row["capacity_generation"] == 2


def test_complete_availability_clears_reclaimable_since(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    store = CapacityAssessmentStore(db, min_no_progress_sec=21_600)
    store.commit(nonviable_assessment(observed_at=30_000, no_progress_since=1_000))
    store.commit(viable_assessment(observed_at=30_300, availability=1.0))
    con = readonly_connect(db)
    value = con.execute("select reclaimable_since from torrent_health where hash='h'").fetchone()[0]
    con.close()
    assert value is None
```

Define `nonviable_assessment()` and `viable_assessment()` in the test file using the builder from Task 2; do not mutate frozen objects.

- [ ] **Step 2: Run the tests and verify they fail**

Run: `python -m pytest tests/test_capacity_assessment.py -k 'reclaimable_since or complete_availability' -v`

Expected: FAIL because `CapacityAssessmentStore` is missing.

- [ ] **Step 3: Implement `CapacityAssessmentStore.commit()`**

Add a store that performs one `write_transaction`, increments `capacity_assessment_state.current_generation`, preserves `reclaimable_since` only while the core predicate remains true, writes compact summary JSON, and returns `assessment.with_generation(generation)`.

The core predicate must be exactly:

```python
def _core_reclaimable(item: TorrentCapacityEvidence, observed_at: int, min_no_progress_sec: int) -> bool:
    return bool(
        item.managed
        and item.incomplete
        and not item.viable
        and item.availability is not None
        and 0.0 <= item.availability < 1.0
        and item.complete_sources == 0
        and item.no_progress_since is not None
        and int(observed_at) - int(item.no_progress_since) >= int(min_no_progress_sec)
    )
```

Use this update inside the transaction:

```python
previous = con.execute(
    "select reclaimable_since from torrent_health where hash=?", (item.hash,)
).fetchone()
reclaimable_since = (
    int(previous["reclaimable_since"])
    if core and previous and previous["reclaimable_since"] is not None
    else int(assessment.observed_at) if core else None
)
con.execute(
    "update torrent_health set reclaimable_since=?,capacity_viable=?,capacity_reason=?,"
    "capacity_assessed_at=?,capacity_generation=? where hash=?",
    (reclaimable_since, 1 if item.viable else 0, item.viability_reason,
     assessment.observed_at, generation, item.hash),
)
```

If no health row exists, first execute `insert or ignore into torrent_health(hash,sampled_at,updated_at) values(?,?,?)` with `(item.hash, assessment.observed_at, assessment.observed_at)`. Store only aggregate counts in `summary_json`; do not persist full raw qBT snapshots.

- [ ] **Step 4: Run all assessment tests**

Run: `python -m pytest tests/test_capacity_assessment.py -v`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/qbt_orchestrator/capacity_assessment.py tests/test_capacity_assessment.py
git commit -m "Persist fenced capacity assessment evidence"
```

### Task 4: Make capacity state and Planner consume the assessment

**Files:**
- Modify: `src/qbt_orchestrator/capacity_state.py`
- Modify: `src/qbt_orchestrator/planner.py`
- Test: `tests/test_capacity_state.py`
- Test: `tests/test_download_planner.py`

- [ ] **Step 1: Write failing Planner and capacity-state tests**

```python
def test_planner_skips_assessment_nonviable_torrent(tmp_path):
    planner, executor, db = planner_fixture(tmp_path)
    assessment = nonviable_assessment(observed_at=30_000, no_progress_since=1_000).with_generation(7)
    result = planner.plan_and_apply(
        snapshots_for("h", amount_left=100), free_bytes=10_000,
        sync_healthy=True, capacity_assessment=assessment,
    )
    assert "h" not in result.selected_hashes
    assert executor.posts == []


def test_capacity_state_persists_assessment_generation(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    store = CapacityStateStore(db, now=lambda: 50)
    transition = store.persist("drain", CapacityResult("capacity_deadlock", "none"), {}, assessment_generation=9)
    assert transition.assessment_generation == 9
```

Use existing test helpers from each test file rather than creating a second fake executor implementation.

- [ ] **Step 2: Run the tests and verify they fail**

Run: `python -m pytest tests/test_download_planner.py -k assessment_nonviable tests/test_capacity_state.py -k assessment_generation -v`

Expected: FAIL because the signatures do not accept the assessment/generation.

- [ ] **Step 3: Add Planner gating**

Add `capacity_assessment: CapacityAssessment | None = None` to `plan_and_apply()`, `_plan_and_apply_impl()`, and `_candidate_lists()`. Before adding a regular candidate:

```python
evidence = None if capacity_assessment is None else capacity_assessment.torrents.get(h)
if evidence is not None and not evidence.viable and h not in forced_active_hashes:
    skipped[h] = "capacity_nonviable"
    continue
```

Forced `availability_probe` intents remain allowed; ordinary budget selection does not override assessment viability.

- [ ] **Step 4: Persist the same generation in capacity state**

Add `assessment_generation: int` to `CapacityTransition`, accept a keyword argument in `CapacityStateStore.persist()`, write it to `capacity_state`, and include it in the returned transition. Add a helper that derives `CapacityObservation` counts from `CapacityAssessment` rather than recomputing `finish_viability()`.

- [ ] **Step 5: Run focused tests**

Run: `python -m pytest tests/test_download_planner.py tests/test_capacity_state.py -v`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/qbt_orchestrator/capacity_state.py src/qbt_orchestrator/planner.py tests/test_capacity_state.py tests/test_download_planner.py
git commit -m "Share capacity viability with Planner"
```

### Task 5: Replace dead-state reclaim selection and add execution fencing

**Files:**
- Modify: `src/qbt_orchestrator/capacity_reclaim.py`
- Test: `tests/test_capacity_reclaim.py`

- [ ] **Step 1: Add failing selector tests**

Add tests proving all of these facts in `tests/test_capacity_reclaim.py`:

```python
def test_nonviable_soak_torrent_is_selected_after_reclaim_grace(reclaim_fixture):
    assessment = reclaim_fixture.assessment(generation=4, availability=0.5, viable=False)
    reclaim_fixture.health("h", reclaimable_since=1000, no_progress_since=100)
    reclaim_fixture.allocation("h", desired_state="soak")
    result = reclaim_fixture.reclaimer(now=5000, min_reclaimable_age_sec=3600).run(
        reclaim_fixture.snapshots(peers=3), assessment=assessment,
        capacity_state="capacity_deadlock", free_bytes=0, target_free_bytes=10_000,
    )
    assert result.planned == 1
    assert result.candidates[0]["hash"] == "h"


def test_stale_assessment_generation_fences_delete(reclaim_fixture):
    assessment = reclaim_fixture.assessment(generation=4, availability=0.5, viable=False)
    reclaim_fixture.set_current_assessment_generation(5)
    result = reclaim_fixture.live_reclaimer.run(
        reclaim_fixture.snapshots(), assessment=assessment,
        capacity_state="capacity_deadlock", free_bytes=0, target_free_bytes=10_000,
    )
    assert result.reclaimed == 0
    assert result.rejection_counts["stale_assessment"] == 1
    assert reclaim_fixture.path.exists()
```

Also add one test each for complete availability, unknown availability, active upload job, active reservation, active `soak_state.cooldown_until`, protected tag, path overlap, and below-64-MiB allocated bytes.

- [ ] **Step 2: Run selector tests and verify failure**

Run: `python -m pytest tests/test_capacity_reclaim.py -k 'nonviable_soak or stale_assessment' -v`

Expected: FAIL because `run()` does not accept an assessment and still queries `desired_state='dead'`.

- [ ] **Step 3: Change `DeadPartialReclaimer` inputs and eligibility state**

Rename the age parameter to `min_reclaimable_age_sec` and change the public call to:

```python
def run(self, snapshots, *, assessment, capacity_state, free_bytes, target_free_bytes):
    if assessment.generation <= 0:
        return CapacityReclaimResult(dry_run=self.dry_run, rejection_counts={"uncommitted_assessment": 1})
```

Replace `_eligibility_state()` with a query that returns health evidence for all hashes and separate sets for open jobs, active reservations, and active cooldowns. It must contain no `scheduler_allocations` join and no `desired_state` predicate:

```sql
select hash,reclaimable_since,no_progress_since,capacity_viable,
       capacity_reason,capacity_generation
from torrent_health
```

```sql
select hash from soak_state where cooldown_until is not null and cooldown_until>?
```

Start candidates from `assessment.torrents.items()` and require the stored `capacity_generation` to equal `assessment.generation`, `capacity_viable=0`, and `now-reclaimable_since >= min_reclaimable_age_sec`.

- [ ] **Step 4: Add targeted pre-delete revalidation**

Before `audit.begin()` and filesystem removal, call `_revalidate_candidate(candidate, assessment)`. It must:

```python
current_generation = self._current_assessment_generation()
if current_generation != assessment.generation:
    return "stale_assessment"
current = self.executor.qbt.torrent_info(candidate["hash"])
availability = current.get("availability")
if availability is None or float(availability) < 0:
    return "availability_unknown"
if float(availability) >= 1.0 or max(int(current.get("num_seeds") or 0), int(current.get("num_complete") or 0)) > 0:
    return "complete_source"
if self._has_open_job_or_reservation_or_cooldown(candidate["hash"]):
    return "active_protection"
return None
```

Then stop the torrent, re-read it until it reaches `stoppedDL`/`pausedDL` within a bounded timeout, re-resolve the path, and only then begin the durable audit and delete. Any changed evidence skips deletion and increments a stable rejection reason.

- [ ] **Step 5: Store the new audit identity and evidence**

Use `f"{hash}:{reclaimable_since}"` as `reclaim_key`. Persist `reclaimable_since`, `capacity_generation`, `capacity_reason`, and a redacted compact `assessment_json`. Keep the existing magnet/name/released-bytes notification.

- [ ] **Step 6: Run all reclaim tests**

Run: `python -m pytest tests/test_capacity_reclaim.py -v`

Expected: PASS, including all existing path and audit regressions.

- [ ] **Step 7: Commit**

```bash
git add src/qbt_orchestrator/capacity_reclaim.py tests/test_capacity_reclaim.py
git commit -m "Select and fence nonviable capacity reclaims"
```

### Task 6: Wire one assessment through the runtime

**Files:**
- Modify: `src/qbt_orchestrator/service.py`
- Modify: `src/qbt_orchestrator/cli.py`
- Test: `tests/test_daemon_runtime.py`
- Test: `tests/test_cli_observability.py`

- [ ] **Step 1: Write failing runtime test**

```python
def test_planner_capacity_and_reclaimer_share_generation(runtime_fixture):
    runtime = runtime_fixture.build_with_recording_reclaimer()
    payload = runtime.planner_tick()
    generation = payload["capacity"]["assessment_generation"]
    assert generation > 0
    assert runtime_fixture.planner_assessment_generation == generation
    assert runtime_fixture.reclaimer_assessment_generation == generation
    assert payload["capacity_reclaim"]["assessment_generation"] == generation
```

- [ ] **Step 2: Run the test and verify failure**

Run: `python -m pytest tests/test_daemon_runtime.py -k share_generation -v`

Expected: FAIL because runtime builds a separate `CapacityObservation` after Planner.

- [ ] **Step 3: Build and commit one assessment in `planner_tick()`**

After soak/cooldown collection and before scheduler-engine selection:

```python
assessment = self.capacity_assessment_builder.build(
    snapshots,
    capacity_health,
    observed_at=planner_now,
    scheduler_mode=scheduler_mode,
    free_bytes=free_bytes,
    target_free_bytes=self.drain_exit_bytes,
    available_growth_bytes=max(0, int(free_bytes) - int(self.disk_floor_bytes) - int(soak_result.reserved_bytes)),
    selected_hashes=incumbent_hashes,
    disk_releasing_jobs=self._disk_releasing_job_count(),
)
assessment = self.capacity_assessment_store.commit(assessment)
```

Use `assessment.torrents[h].viable` for scheduler-engine filtering, pass `capacity_assessment=assessment` to `DownloadPlanner.plan_and_apply()`, derive capacity observation from the same object, pass `assessment_generation=assessment.generation` to `CapacityStateStore.persist()`, and pass `assessment=assessment` to the reclaimer. Include `assessment_generation` in runtime payloads and metrics.

- [ ] **Step 4: Resolve configuration defaults in `cli.py`**

Construct the store/builder with:

```python
min_no_progress_sec = int(os.environ.get(
    "QBT_ORCH_CAPACITY_RECLAIM_MIN_NO_PROGRESS_SEC",
    os.environ.get("QBT_ORCH_CAPACITY_RECLAIM_MIN_DEAD_SEC", "21600"),
))
min_reclaimable_sec = int(os.environ.get("QBT_ORCH_CAPACITY_RECLAIM_MIN_RECLAIMABLE_SEC", "3600"))
```

Reject negative values at startup. Keep `QBT_ORCH_CAPACITY_RECLAIM_MIN_DEAD_SEC` only as a backward-compatible fallback and report both effective values in `_effective_config_snapshot()`.

- [ ] **Step 5: Run runtime/config tests**

Run: `python -m pytest tests/test_daemon_runtime.py tests/test_cli_observability.py -v`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/qbt_orchestrator/service.py src/qbt_orchestrator/cli.py tests/test_daemon_runtime.py tests/test_cli_observability.py
git commit -m "Wire shared capacity assessments through runtime"
```

### Task 7: Add natural-language no-candidate alerts

**Files:**
- Modify: `src/qbt_orchestrator/alerts.py`
- Modify: `src/qbt_orchestrator/service.py`
- Test: `tests/test_capacity_state.py`

- [ ] **Step 1: Write the failing alert test**

```python
def test_capacity_deadlock_without_candidate_uses_natural_language_and_dedupes(alert_fixture):
    transition = alert_fixture.transition(state="capacity_deadlock", entered_at=100, assessment_generation=8)
    first = alert_fixture.service.enqueue_capacity_deadlock(
        transition, required_minimum_growth_bytes=1000,
        top_manual_candidates=[], mature_reclaim_candidates=0,
        rejection_fingerprint="availability_unknown:3|protected_tag:1",
    )
    second = alert_fixture.service.enqueue_capacity_deadlock(
        transition, required_minimum_growth_bytes=1000,
        top_manual_candidates=[], mature_reclaim_candidates=0,
        rejection_fingerprint="availability_unknown:3|protected_tag:1",
    )
    assert len(first) == 1
    assert second == []
    row = alert_fixture.notifications()[0]
    assert "当前没有能够安全回收的任务，需要人工处理" in row["message"]
    assert "capacity_deadlock" not in row["message"]
```

- [ ] **Step 2: Run the test and verify failure**

Run: `python -m pytest tests/test_capacity_state.py -k natural_language_and_dedupes -v`

Expected: FAIL because the alert does not accept mature candidate/rejection evidence.

- [ ] **Step 3: Implement stable episode dedupe**

Build the dedupe key from chat ID, `transition.entered_at`, and the SHA-256 prefix of the rejection fingerprint. Render exactly one of:

```python
message = (
    "可用空间不足，已暂停启动新任务；系统正在安全释放无效文件。"
    if mature_reclaim_candidates > 0
    else "可用空间不足，当前没有能够安全回收的任务，需要人工处理。"
)
```

Keep raw state/reason/generation only in redacted `payload_json`, not message text.

- [ ] **Step 4: Run capacity alert tests**

Run: `python -m pytest tests/test_capacity_state.py -v`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/qbt_orchestrator/alerts.py src/qbt_orchestrator/service.py tests/test_capacity_state.py
git commit -m "Report safe capacity reclaim status naturally"
```

### Task 8: Run the full local verification gate

**Files:**
- Test: all test files

- [ ] **Step 1: Run focused capacity tests**

Run:

```bash
python -m pytest tests/test_capacity_assessment.py tests/test_capacity_state.py tests/test_capacity_reclaim.py tests/test_download_planner.py tests/test_daemon_runtime.py tests/test_cli_observability.py -q
```

Expected: all selected tests PASS.

- [ ] **Step 2: Run the full suite**

Run: `python -m pytest -q`

Expected: all tests PASS with no collection warnings introduced by the change.

- [ ] **Step 3: Verify forbidden selection dependency is gone**

Run:

```powershell
rg -n "where sa\.desired_state='dead'|desired_state.?=.?['\"]dead['\"]" src/qbt_orchestrator/capacity_reclaim.py
```

Expected: no matches.

- [ ] **Step 4: Verify diff hygiene and commit any test-only corrections**

Run: `git diff --check && git status --short`

Expected: no whitespace errors; only intentional plan/implementation changes remain.

### Task 9: Deploy the capacity release and observe live behavior

**Files/services:**
- Deploy: `/opt/emby_qbt_auto/releases/$sha`, where `$sha` is assigned from `git rev-parse --short=12 HEAD` in Step 1
- Switch: `/opt/emby_qbt_auto/current`
- Back up: `/etc/qbt-orchestrator/daemon.env`, `/var/lib/qbt-orchestrator/state.sqlite*`, `/etc/systemd/system/qbt-orchestrator-daemon.service`
- Restart: `qbt-orchestrator-daemon.service`

- [ ] **Step 1: Record the release SHA and create local archive**

```powershell
$sha = (git rev-parse --short=12 HEAD).Trim()
New-Item -ItemType Directory -Force artifacts | Out-Null
git archive --format=tar.gz -o "artifacts/emby_qbt_auto-$sha.tar.gz" HEAD
Get-FileHash "artifacts/emby_qbt_auto-$sha.tar.gz" -Algorithm SHA256
```

Expected: one archive and SHA-256 digest.

- [ ] **Step 2: Take read-only pre-deploy evidence**

```powershell
ssh -o BatchMode=yes paff-vps "systemctl is-active qbt-orchestrator-daemon.service; readlink -f /opt/emby_qbt_auto/current; df -h /data/downloads; journalctl -u qbt-orchestrator-daemon.service -n 80 --no-pager"
```

Expected: service active, current release path printed, filesystem and recent logs captured.

- [ ] **Step 3: Back up state and configuration**

Before execution, state the exact root operations and rollback path as required by the VPS skill. Then run through the approved root channel:

```bash
stamp=$(date +%Y%m%d-%H%M%S)
mkdir -p /opt/emby_qbt_auto/backups/$stamp
cp -a /etc/qbt-orchestrator/daemon.env /etc/systemd/system/qbt-orchestrator-daemon.service /opt/emby_qbt_auto/backups/$stamp/
sqlite3 /var/lib/qbt-orchestrator/state.sqlite ".backup '/opt/emby_qbt_auto/backups/$stamp/state.sqlite'"
readlink -f /opt/emby_qbt_auto/current > /opt/emby_qbt_auto/backups/$stamp/previous-release
```

Expected: backup directory contains environment, unit, consistent SQLite backup, and previous release pointer.

- [ ] **Step 4: Install and switch the immutable release**

Reuse the PowerShell `$sha` value from Step 1:

```powershell
scp "artifacts/emby_qbt_auto-$sha.tar.gz" "paff-vps:/tmp/emby_qbt_auto-$sha.tar.gz"
ssh paff-vps "sha='$sha'; mkdir -p /opt/emby_qbt_auto/releases/`$sha; tar -xzf /tmp/emby_qbt_auto-`$sha.tar.gz -C /opt/emby_qbt_auto/releases/`$sha; ln -sfn /opt/emby_qbt_auto/releases/`$sha /opt/emby_qbt_auto/current.new; mv -Tf /opt/emby_qbt_auto/current.new /opt/emby_qbt_auto/current; systemctl restart qbt-orchestrator-daemon.service"
```

Expected impact: only the orchestrator daemon restarts; qBittorrent remains running.

- [ ] **Step 5: Verify schema and first shared generation**

```bash
sqlite3 -json /var/lib/qbt-orchestrator/state.sqlite \
  "select current_generation,observed_at,summary_json from capacity_assessment_state where id=1;"
sqlite3 -json /var/lib/qbt-orchestrator/state.sqlite \
  "select scheduler_mode,state,assessment_generation,details_json from capacity_state where id=1;"
systemctl status qbt-orchestrator-daemon.service --no-pager -n 30
```

Expected: active service; both generation values are positive and equal.

- [ ] **Step 6: Observe until a candidate matures or manual handling is reported**

For at least one hour, sample every five minutes:

```bash
journalctl -u qbt-orchestrator-daemon.service --since '-6 minutes' --no-pager
sqlite3 -json /var/lib/qbt-orchestrator/state.sqlite \
  "select hash,reclaimable_since,capacity_viable,capacity_reason,capacity_generation from torrent_health where reclaimable_since is not null order by reclaimable_since;"
sqlite3 -json /var/lib/qbt-orchestrator/state.sqlite \
  "select id,hash,state,allocated_bytes,reclaimable_since,capacity_generation,reclaimed_at from capacity_reclaims order by id desc limit 5;"
df -B1 /data/downloads
```

Expected: no unexpected restarts/SQLite exceptions; a mature candidate is safely reclaimed and audited, or the natural-language no-safe-candidate warning is persisted without repeated spam.

- [ ] **Step 7: Roll back if acceptance fails**

```bash
previous=$(cat /opt/emby_qbt_auto/backups/$stamp/previous-release)
ln -sfn "$previous" /opt/emby_qbt_auto/current.rollback
mv -Tf /opt/emby_qbt_auto/current.rollback /opt/emby_qbt_auto/current
systemctl restart qbt-orchestrator-daemon.service
systemctl is-active qbt-orchestrator-daemon.service
```

Expected: previous daemon release is active; additive schema remains harmless.
