# Shared Capacity Assessment and Live Reclaim Design

Date: 2026-07-23
Status: Approved design
Scope: qBT Orchestrator capacity planning and live partial-payload reclaim on `ssh.paff-67.top`

## 1. Outcome

Planner, capacity-state detection, and the live reclaimer consume the same immutable `CapacityAssessment` for each scheduler tick. The reclaimer no longer treats `scheduler_allocations.desired_state='dead'` as an eligibility gate, so a torrent cannot escape capacity reclaim merely because health classification oscillates between `soak` and `dead`.

Automatic reclaim remains conservative and auditable. It acts only during a persisted capacity deadlock, only after sustained evidence that an incomplete torrent cannot finish, and only when deleting its validated payload can release at least 64 MiB. A leech-only peer is not considered a complete source.

The effective production policy is:

```text
minimum no-progress age:             6 hours
minimum continuous reclaimable age: 1 hour
minimum allocated bytes released:   64 MiB
maximum live reclaims per tick:      1
reclaim evaluation interval:         5 minutes
```

## 2. Current defect

`capacity_state.build_capacity_observation()` already evaluates finish viability from qBT availability, complete-source evidence, and recent progress. The current `DeadPartialReclaimer`, however, first joins `scheduler_allocations` to `torrent_health` with:

```sql
where sa.desired_state='dead'
```

This creates two conflicting truths. Capacity detection can report every incomplete torrent as nonviable while the reclaimer sees no eligible rows. The health policy also clears `dead_since` when a leech peer appears, even when availability remains below one and downloaded bytes have not advanced. Turning live mode on changes only execution after selection; it does not fix an empty candidate set.

The runtime symptom is `capacity_deadlock` with zero finishable jobs, zero disk-releasing jobs, and repeated reclaim results with `planned=0`, `reclaimed=0`, and no executor error.

## 3. Shared assessment model

### 3.1 Immutable tick object

Introduce one immutable `CapacityAssessment` produced after qBT synchronization and health persistence:

```text
CapacityAssessment
  generation
  observed_at
  scheduler_mode
  free_bytes
  target_free_bytes
  available_growth_bytes
  aggregate
    managed_incomplete
    viable_finish
    nonviable_finish
    feasible_full_finish
    disk_releasing_jobs
    required_minimum_growth_bytes
  torrents[hash]
    managed
    incomplete
    amount_left
    completed_bytes
    allocated_bytes_hint
    availability
    complete_sources
    no_progress_since
    viable
    viability_reason
```

`CapacityAssessmentBuilder` owns all finish-viability rules. Planner, `CapacityStateStore`, alert formatting, metrics, and `CapacityReclaimer` receive the same object from `service.py`; they do not independently recalculate viability with different thresholds or database joins.

`generation` is monotonically increasing and persisted with the assessment evidence. It identifies the exact scheduler observation used to propose a reclaim. After qBT synchronization, the builder produces an unnumbered draft; `CapacityAssessmentStore.commit()` atomically increments the durable generation, updates the per-torrent evidence, and returns the immutable committed object consumed by the rest of the tick.

### 3.2 Viability decision

For a managed incomplete torrent:

- a complete source exists when `max(num_seeds, num_complete) > 0` or known availability is at least `1.0`;
- recent byte progress makes the torrent viable even without a currently observed complete source;
- a torrent is nonviable when it has no complete source and its completed bytes have not advanced for the configured viability-stale window;
- `num_peers > 0` alone does not make the torrent viable;
- missing, negative, stale, or otherwise unknown availability is `unknown`, not safely reclaimable.

The aggregate capacity state is built directly from the per-torrent decisions. `top_manual_candidates` remains diagnostic output derived from the same list.

## 4. Persistent reclaimability state

### 4.1 Schema

Extend `torrent_health` with:

```text
reclaimable_since       integer null
capacity_viable         integer null
capacity_reason         text null
capacity_assessed_at    integer null
capacity_generation     integer null
```

Create a single-row `capacity_assessment_state` table:

```text
id                       fixed at 1
current_generation
observed_at
summary_json
```

Add `assessment_generation` to `capacity_state` so the persisted capacity episode points to the exact assessment that produced it. Pre-delete fencing compares the candidate generation with `capacity_assessment_state.current_generation`.

`capacity_viable` is `1`, `0`, or `NULL` for viable, nonviable, or unknown. The migration initializes all new fields to `NULL`; it does not backfill `reclaimable_since` from `dead_since`, because the historical `soak/dead` classification does not prove continuous availability below one.

Extend `capacity_reclaims` with the evidence needed to reproduce an action:

```text
reclaimable_since
capacity_generation
capacity_reason
assessment_json
```

Existing reclaim audit rows remain valid and readable.

### 4.2 State transition

After building an assessment, update each managed incomplete torrent as follows:

1. The core reclaimability predicate is true only when:
   - the assessment says the torrent cannot finish;
   - known availability is `0 <= availability < 1`;
   - no complete source exists;
   - `no_progress_since` is at least six hours old.
2. When the predicate changes from false to true, set `reclaimable_since=observed_at`.
3. While the predicate remains true, preserve the original timestamp even if scheduler allocation changes between `soak`, `dead`, `paused`, or another non-terminal state.
4. Clear `reclaimable_since` when availability reaches one, a complete source appears, byte progress resumes, the torrent completes, it leaves orchestrator management, or evidence becomes unknown.

The one-hour reclaim grace is measured from `reclaimable_since`. Thus a newly observed incomplete swarm cannot be deleted from one transient sample, even when `no_progress_since` is old.

## 5. Candidate selection

The live selector starts from `assessment.torrents`, not `scheduler_allocations.desired_state`. A candidate must satisfy every gate below at selection time:

- capacity state is `capacity_deadlock` and free space is below the reclaim target;
- managed and incomplete;
- assessment viability is false for the current generation;
- `reclaimable_since` is at least one hour old;
- no progress for at least six hours;
- known availability is below one and no complete source exists;
- no protected tag such as `hold` or `seed-long`;
- no active upload, verification, promotion, cleanup, or other open torrent job;
- no active resource reservation or lease for the hash;
- no active soak cooldown or other scheduler cooldown;
- no explicit manual lock;
- content path is inside the configured managed download root;
- content path does not overlap another torrent path;
- allocated on-disk bytes are at least 64 MiB.

Leech-only peers do not reject a candidate. They remain part of diagnostic evidence, but only complete-source and progress evidence can restore viability.

Candidates are ordered by releasable allocated bytes descending, then progress ascending, then hash for deterministic tie-breaking. Selection stops after one candidate or after planned bytes meet the target-free-space deficit, whichever happens first.

## 6. Fenced live execution

Selection never directly authorizes deletion. Immediately before changing qBT or the filesystem, the reclaimer performs a targeted safety revalidation:

1. Confirm that the capacity deadlock episode is still active.
2. Confirm that the selected assessment generation is still the latest committed generation.
3. Re-read the torrent from qBT and verify its hash, incomplete state, current availability, complete-source count, tags, and content path.
4. Re-read jobs, reservations, upload leases, cooldown, and manual protection from SQLite.
5. Stop the torrent and wait until qBT reports a stopped download state.
6. Resolve and revalidate the host path below the managed root, overlap rules, and allocated bytes.
7. Start the durable audit row using `hash:reclaimable_since` as the idempotency key.
8. Remove only the validated payload path; never remove the torrent registration.
9. Request qBT recheck so the retained torrent can return from zero if complete availability later appears.
10. Complete the audit row and enqueue the existing Telegram reclaim notice containing name, magnet link, released bytes, and recheck result.

If any revalidation differs from the proposal, skip the action and record a structured rejection. A stale generation is never reused. A filesystem or qBT failure produces a failed audit row and does not continue to another destructive candidate in the same tick.

## 7. Planner and user-visible behavior

Planner uses the shared assessment to avoid selecting nonviable torrents for full-finish budget. It may still issue a bounded availability probe, but a probe does not clear `reclaimable_since` unless new progress or complete availability is actually observed.

Raw internal codes remain available in structured logs and detailed diagnostics. User-facing Telegram status maps them to natural language:

```text
progress_possible:
  空间充足，任务正在按计划处理。

capacity_deadlock with a safe candidate:
  可用空间不足，已暂停启动新任务；系统正在安全释放无效文件。

capacity_deadlock without a safe candidate:
  可用空间不足，当前没有能够安全回收的任务，需要人工处理。
```

The no-candidate warning is deduplicated by capacity episode and rejection fingerprint. Repeated five-minute evaluations update metrics but do not repeatedly push identical Telegram messages.

## 8. Observability

Each assessment emits one compact metric snapshot containing generation, aggregate counts, free/target bytes, and the number of currently mature reclaimable candidates. Per-torrent decision logs are emitted only when viability, reason, or reclaimability changes.

Each reclaim evaluation records:

- assessment generation;
- candidate and selected counts;
- planned and reclaimed bytes;
- rejection counts by stable reason code;
- stale-generation or revalidation failures;
- executor and audit errors.

Health sampling remains at its reduced interval; the design does not add per-second SQLite writes.

## 9. Tests

Focused tests cover:

1. Planner, capacity-state detection, and reclaimer receive the same assessment generation.
2. A nonviable `soak` torrent becomes reclaimable without `desired_state='dead'`.
3. A leech peer with availability `0.5` does not block reclaimability.
4. Availability `1.0`, a seed, a complete peer, or resumed byte progress clears `reclaimable_since`.
5. Unknown availability cannot mature into a live candidate.
6. `reclaimable_since` survives `soak/dead` allocation changes.
7. The six-hour no-progress and one-hour continuous-reclaimable timers are both enforced.
8. Active upload jobs, reservations, leases, cooldown, protected tags, unsafe paths, overlaps, and sub-64-MiB payloads are rejected.
9. A generation change between selection and execution prevents deletion.
10. The reclaimer stops and revalidates qBT before deleting the payload.
11. A successful action persists the name, magnet link, evidence, released bytes, and Telegram notice.
12. No-candidate alerts use natural language and deduplicate within an episode.
13. Existing capacity-state and reclaim audit migrations remain backward compatible.

The full test suite must pass in addition to focused capacity tests.

## 10. Live rollout

1. Run focused and full local tests.
2. Commit and push an immutable release.
3. Back up the live daemon environment, current release pointer, unit definition, state database including WAL/SHM, and recent orchestrator logs.
4. Deploy the schema and code without changing qBittorrent.
5. Verify the first assessment generation, aggregate counts, and per-torrent reasons against the live qBT snapshot.
6. Confirm `reclaimable_since` starts as `NULL` and is populated only from new continuous observations; do not seed it from `dead_since`.
7. Keep the existing live reclaim configuration enabled with dry-run disabled. The new one-hour continuous evidence gate prevents an immediate destructive action after deployment.
8. Monitor every five-minute evaluation until at least one candidate matures or the capacity state recovers.
9. When a candidate matures, verify the pre-delete evidence, audit row, qBT retained registration/recheck, released bytes, free-space increase, and Telegram notification.
10. Continue monitoring for one hour after the first live action. If no candidate is safe, verify the natural-language manual-action warning and leave the service running normally.

Only the orchestrator daemon is restarted. The qBittorrent container and active BitTorrent sessions are not restarted.

## 11. Rollback

Code rollback repoints the live release to the previous immutable revision and restores the prior daemon environment. Added SQLite columns are backward compatible and remain unused by the previous release; the database is not destructively downgraded.

Disabling `QBT_ORCH_CAPACITY_RECLAIM` immediately stops future reclaim actions without stopping Planner. Completed payload deletion cannot be reversed, which is why continuous evidence, pre-delete revalidation, one-action-per-tick, and audit persistence are mandatory.

## 12. Acceptance criteria

The capacity change is complete only when all of the following are demonstrated:

1. One shared `CapacityAssessment` generation is visible in Planner, capacity-state, metrics, and reclaim evaluation for the same tick.
2. No reclaimer query or gate depends on `scheduler_allocations.desired_state='dead'`.
3. `reclaimable_since` is independent of `soak/dead` transitions and clears on restored progress or complete availability.
4. A torrent with leech peers but availability below one can mature when all other gates remain true.
5. Protected, leased, uploading, cooling, unknown-availability, unsafe-path, and low-value candidates are never deleted.
6. A stale assessment generation or changed qBT snapshot prevents deletion.
7. A live reclaim releases the recorded allocated bytes, retains the torrent in qBT, requests recheck, persists the recovery magnet, and sends the Telegram notice.
8. When no safe candidate exists, the welcome/status UI says that manual handling is required and does not expose internal mode codes.
9. The service remains active with zero unexpected restarts, the full test suite passes, and live logs contain no unhandled reclaim or SQLite errors.
