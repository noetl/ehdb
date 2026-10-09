# Retention + object-store archival tier for the EHDB event log

**Status:** draft · **Opened:** 2026-10-09 · **Closes:** [noetl/ai-meta#457](https://github.com/noetl/ai-meta/issues/457)
(the hot volume's ~74-day disk headroom)

A completed execution older than a configurable window is copied to an object store,
indexed for discovery by date, and only then removed from the hot EHDB volume. The hot
volume stops being an unbounded accumulator and becomes a window.

---

## 1. Problem

`/data` on `noetl-server-rust-embedded-0` is a 9.7 GiB PVC. Measured 2026-10-09:

```
/data                       3.0G used   6.7G avail   31%
  /data/ehdb-embedded       2.4G          (substrate 1.2G + local 1.2G — two replicas, ONE PVC)
    substrate/parts          317 files, ~3.7 MiB each, all shard-0
    local/parts              323 files
    substrate/manifest       17 objects, 58.1 MiB
  /data/ehdb-chain          684.6M       (170,118 files — the chain store, separate concern)
```

Nothing removes event data. At the ~82 MiB/day measured on #457 the volume fills, and the
failure mode is already documented: when `/data/cmdbus` reached 100% on 2026-09-01 every
`POST /api/execute` returned 500 for hours while the writer pod reported `Ready`,
`restarts=0` and **zero ERROR lines** ([ehdb#345](https://github.com/noetl/ehdb/issues/345)).
A full disk here is silent.

---

## 2. Audit — what already exists (measured, 2026-10-09)

**Do not rebuild these.**

| capability | state | where |
| :-- | :-- | :-- |
| Retention planner | ✅ **exists** — `plan_retention(manifest, keep_from_sequence)`; drops whole parts below a sort-key floor and **never splits a part** | `ehdb-l0/src/retention.rs` (128 lines) |
| Retention executor | ✅ **exists** — `L0Engine::apply_retention(keep_from_sequence)`: manifest swap + object reclaim | `ehdb-l0/src/engine.rs:1299` |
| Per-part sequence bounds | ✅ `PartMeta.min_sequence` / `.max_sequence`, and the part **filename** encodes them (`shard-0-seq-<min>-<max>.eslog`) | `catalog.rs` |
| Per-execution locality | ✅ `Dataset::index_key` = `execution_id`; `PartMeta.execution_bloom` | `dataset.rs:127` |
| GCS object client | ✅ **live in prod** — `ObjectBackend`/`GcsBackend` with `put`/`get`/`list`/`delete`, `from_env()` | `server/src/services/object_backend.rs` |
| GCS auth | ✅ **live** — `NOETL_OBJECT_STORE_GCS_AUTH=auto` (ADC via Workload Identity), SA `noetl-result-tier@…`, 21,569 objects written to the results bucket | prod env |
| Archive bucket | ✅ **exists** — `gs://shastaratech-noetl-prod-eventlog-archive-20260831` (US-CENTRAL1) | GCS |

⚠⚠ **Does NOT exist** — and the gaps are what this spec builds:

1. **No cloud `DurableSubstrate`.** The only two implementations are `LocalFsSubstrate` and
   `InMemorySubstrate`. The `on_upload_done` / `replica_writes` / under-replication
   machinery is about replica copies **between local substrates** — nothing is uploaded
   off-box today, and prod's "second replica" is the same PVC.
2. **`apply_retention` has ZERO callers in the server.** Verified against `origin/main`:
   every match is inside `vendor/ehdb-l0-0.1.0/`, none in `src/`, with a control grep
   confirming the method finds symbols the server does use. **The retention mechanism is
   built and dormant** — the [#332](https://github.com/noetl/ai-meta/issues/332)
   "built ahead of its consumers" pattern.
3. **No time- or completion-based retention.** The existing floor is a raw
   `global_sequence`. Nothing maps "completed 48h ago" to a sequence.
4. No per-execution archival, no `execution_id=` layout, no by-date index, no
   `NOETL_EHDB_RETENTION_HOURS`.
5. The archive bucket holds **5 objects**, all from a one-off 2026-08-31 manual snapshot
   (a Postgres dump + three `tier/eventbus/*.jsonl` exports). It is a **dated record**,
   not a running tier — correct precisely by not updating. Nothing writes to it.
6. ⚠ **`noetl-result-tier` has no IAM binding on the archive bucket** and **zero
   project-level roles**. Its results-bucket access is a per-bucket
   `roles/storage.objectAdmin` binding. **One new permission is required** — see §9.

`ehdb-reference/src/object.rs` (1,782 lines) is a content-addressed object engine for
**state shards and the result tier** — platform artifacts, explicitly not the event log —
and the server does not link `ehdb-reference` at all. Out of scope here.

---

## 3. The constraint that shapes everything

```rust
fn partition(record: &EventRecord, shard_count: u32) -> u32 {
    shard_for_execution(&record.execution_id, shard_count)   // DEFAULT_SHARD_COUNT = 1
}
fn index_key(record: &EventRecord) -> &str { &record.execution_id }
fn sort_key(record: &EventRecord) -> u64 { record.global_sequence }
```

**A part holds many executions, interleaved by append sequence.** Prod runs
`shard_count = 1`, so every execution is in the same partition and each ~3.7 MiB part is a
time-slice of the whole fleet's events.

Three consequences:

- **An execution owns no part.** Per-execution archival cannot be a file move; it is an
  extraction.
- **`execution_bloom` cannot enumerate.** It answers *"might execution X be in this part"*
  with false positives and no false negatives. So *"is every execution in this part
  archivable?"* is not a question a part can answer without being read.
- ⚠⚠ **Dropping an old part can destroy the early events of a still-running execution.**
  A long execution appends over hours; its first events sit in old parts. `noetl.event` is
  append-only and replay is the source of truth, so that is data loss, not eviction.

### The resolution: decouple the two granularities

- **Archive per execution — exact.** Read that execution's records by `index_key`, write
  them under `execution_id=<id>/`.
- **Prune per part — conservative.** Advance a **floor** and let the existing
  `apply_retention` drop only parts entirely below it.

The floor is the bridge:

```
floor = min(global_sequence of any record belonging to an execution that is NOT yet archived)
```

Every part with `max_sequence < floor` contains only archived executions, so dropping it
loses nothing that is not already durable in the object store. Parts are never rewritten,
never split, and remain immutable and content-stable.

---

## 4. Layout

As required, two prefixes in one bucket.

### 4.1 Data — by execution

```
gs://<bucket>/execution_id=<execution_id>/events-<min_seq>-<max_seq>.eslog
gs://<bucket>/execution_id=<execution_id>/manifest.json
```

`manifest.json` carries what a retrieval needs to be *verifiable*, not just fetchable:
`execution_id`, `started_at`, `completed_at`, `status`, `record_count`, `min_sequence`,
`max_sequence`, `sha256` per object, `archived_at`, `schema_version`, and the
`FORMAT_VERSION` the records were written under.

### 4.2 Reference index — by date

```
gs://<bucket>/date=YYYY-MM-DD/execution_id=<execution_id>.json
```

⭐ **One small object per execution, not one appended manifest per day.** GCS has no
append, so a single per-day object would be read-modify-write and would lose entries under
concurrency. One object per execution makes every write **independent and idempotent**, and
discovery by date becomes `ObjectBackend::list(prefix)` — which already exists. A rolled-up
`date=YYYY-MM-DD/index.json` may later be *derived* by a compaction job, but it must never
be the write path.

The date is the execution's **start** date, per the requirement. Measured: 39 distinct
start dates across 6,467 executions, busiest day 1,073.

---

## 5. Configuration

| variable | default | meaning |
| :-- | :-- | :-- |
| `NOETL_EHDB_RETENTION_HOURS` | **48** | a completed execution older than this is archivable. 24 is a supported setting. |
| `NOETL_EHDB_ARCHIVE_ENABLED` | **false** | run the archive pass (additive — writes to GCS, deletes nothing) |
| `NOETL_EHDB_PRUNE_ENABLED` | **false** | allow the floor to advance and parts to drop (**deletes hot data**) |
| `NOETL_EHDB_ARCHIVE_BUCKET` | unset | archive target; unset ⇒ archiving is off regardless of the flag |
| `NOETL_EHDB_ARCHIVE_INTERVAL_SECS` | 900 | pass cadence |
| `NOETL_EHDB_ARCHIVE_MAX_PER_PASS` | 200 | bound the work per pass |

⭐⭐ **Archive and prune are two separate flags, deliberately.** Archiving is additive and
can run for days while its output is checked; pruning destroys the only other copy. There
must be a state where the archive is provably complete and nothing has been deleted. A
single flag would make "verify before deleting" impossible.

Both default **false**: a retention tier that defaults on is a tier that deletes data in
whichever deployment nobody configured.

---

## 6. Flow

### 6.1 Archive pass

1. List executions whose status is terminal (`COMPLETED` / `FAILED` / `CANCELLED`) and
   whose `completed_at < now - retention`. ⚠ **Status must be event-derived, never
   `noetl.execution.status`** — that column froze at the Python retirement and still claims
   3,225 executions RUNNING ([#235](https://github.com/noetl/ai-meta/issues/235)).
   ⚠ `GET /api/executions` caps `limit` at **100** and says so in `x-noetl-limit-applied` /
   `x-noetl-limit-capped`; the pass must page on `offset` (6,467 executions came back at
   100/request; one call returns 100).
2. Skip any already recorded as archived.
3. Read that execution's records from the hot engine by `index_key`.
4. `put_if_absent` the data objects, then `manifest.json`, then the `date=` reference.
   Content-addressed and immutable, so a retried upload is a no-op rather than an error.
5. **Verify durability: read back** and compare `sha256` and `record_count` against the
   manifest. An upload is not durable because `put` returned `Ok`.
6. Record `archived_at` + the verified digest in durable state (D8 or a dedicated dataset).

### 6.2 Prune pass — separate, separately gated

1. Recompute `floor` from §3 over all **non-archived** executions.
2. ⚠ **Refuse to advance if any execution lacks a verified archive**, including
   non-terminal ones. The floor is a function of archive state, never of a clock.
3. `apply_retention(floor)`.
4. Report bytes reclaimed and parts dropped.

**Order is the guarantee.** Upload → verify → record → *then* prune. A crash at any point
leaves the hot copy intact, because the prune step derives its floor only from durably
recorded, verified archives. There is no window in which the only copy is in flight.

---

## 7. Retrieval

**By execution id.** `GET /api/executions/{id}/events` must fall through to the archive
when the hot store has pruned the range: hot first, archive on miss, merge by
`global_sequence`.

⚠⚠ **A pruned execution must never read as an empty one.** This codebase's most expensive
recurring failure is a wrong lookup answering with silence rather than an error — a wrong
GCP project returns an **empty collection, not a 404**, disguised by correct 404-tolerance
upstream. So the read path must distinguish three states explicitly: *live in hot*,
*archived (served from GCS)*, *unknown*. "No events" is only ever a valid answer for the
third, and the response must say which case it is.

**By date.** `GET /api/archive/executions?date=YYYY-MM-DD` → `list("date=YYYY-MM-DD/")`,
returning execution ids with their manifest summaries.

---

## 8. The hard parts, stated honestly

1. **⚠⚠ The floor is pinned by the oldest non-archivable execution.** One stuck
   non-terminal execution blocks **all** reclamation — and every counter would read
   healthy while zero bytes come back. Measured today: **0 of 6,467 executions are
   non-terminal**, so the floor is free to advance. That is a point-in-time reading, not a
   property. **Mitigation is a metric, not an assumption:** export the floor, the age of
   the execution pinning it, and the droppable-part count, and alert on "archiving
   succeeds while reclaimed bytes stay 0". This is the representation-drift hazard of the
   whole design.
2. **Atomicity.** Addressed by ordering (§6) plus idempotent `put_if_absent`. The residual
   risk is a *partially uploaded* execution that is never completed: it is detectable
   (manifest absent or digest mismatch) and must be treated as not-archived, which keeps
   the floor behind it. Never infer archived-ness from object presence alone.
3. **The by-date index under concurrency.** Resolved structurally by one object per
   execution (§4.2) rather than by locking.
4. **FAILED is terminal but not disposable.** 458 of 6,467 are FAILED; they archive
   exactly like COMPLETED. Terminal means archivable, never discardable.
5. **Manifest bloat is adjacent and not fixed here.** `substrate/manifest` is already
   58.1 MiB over 17 objects. Pruning parts should shrink it, and §6.2 must report
   manifest bytes too so the effect is measured rather than hoped for.
6. **Retention is not GDPR deletion.** Archiving moves bytes; it does not erase them. Any
   erasure requirement is a different feature against the archive tier.

---

## 9. Permission required — flagged, not taken

The server's SA `noetl-result-tier@shastaratech-noetl-prod.iam.gserviceaccount.com` has
**no binding on the archive bucket** and no project-level roles. It needs the same grant it
already has on the results bucket:

```bash
gcloud storage buckets add-iam-policy-binding \
  gs://shastaratech-noetl-prod-eventlog-archive-20260831 \
  --member="serviceAccount:noetl-result-tier@shastaratech-noetl-prod.iam.gserviceaccount.com" \
  --role=roles/storage.objectAdmin
```

⚠ **Not applied.** IAM is owner-gated. Until it is granted, archiving on prod fails closed
(and `NOETL_EHDB_ARCHIVE_ENABLED` stays false), which is the correct behaviour — but it
means the prod phase of this work is blocked on that one command.

---

## 10. Acceptance criteria

- [ ] AC1 `NOETL_EHDB_RETENTION_HOURS` defaults to 48, honours 24, and rejects 0 / garbage loudly.
- [ ] AC2 A terminal execution older than the window is archived to `execution_id=<id>/` with data objects + `manifest.json`.
- [ ] AC3 A `date=YYYY-MM-DD/execution_id=<id>.json` reference is written for the **start** date.
- [ ] AC4 Archive is idempotent: a second pass over the same execution uploads nothing new and does not error.
- [ ] AC5 **The hot copy is pruned only after read-back verification** of digest and record count. A planted digest mismatch must block the prune — proven with a deliberate corruption.
- [ ] AC6 `apply_retention` is **reached from the server** (the gap in §2.2), under `NOETL_EHDB_PRUNE_ENABLED`.
- [ ] AC7 A part straddling the floor is kept whole; a non-terminal execution's events are never dropped — proven with a synthetic long-running execution.
- [ ] AC8 Retrieval by execution id returns the full event sequence after pruning, and a pruned execution is **distinguishable from an unknown one**.
- [ ] AC9 Retrieval by date lists the expected execution ids.
- [ ] AC10 Disk reclaimed is **measured** (before/after `du`), with the archived byte count as the denominator.
- [ ] AC11 Metrics: retention floor, pinning-execution age, droppable parts, archived/verified/failed counts, bytes reclaimed — all **pinned at 0 unconditionally** so absence is distinguishable from zero.
- [ ] AC12 Both flags default false; with either off the pass is a no-op and says so.

## 11. Verification plan

RED before GREEN on each. Controls that matter:

- **A planted digest mismatch must block the prune** (AC5). Without this control, "verified"
  is a word rather than a check — the class of defect where a verification read the store it
  was verifying ([#332 AC14](https://github.com/noetl/ai-meta/issues/332)).
- **A synthetic execution spanning the floor** must survive a prune pass intact (AC7).
- **Measure the reclaim with `du` before and after**, and publish the denominator (parts
  examined, parts dropped, bytes archived) — a "0 reclaimed" must be distinguishable from
  "nothing was droppable".
- **Kind first, prod second**, per `agents/rules/deployment-validation.md`. Prod runs
  archive-only until its output is checked; prune is a separate, later, owner-visible step.

## 12. Phases

| phase | content | gate |
| :-- | :-- | :-- |
| P1 | Config + the archivable-set query (paged, event-derived status). No writes. | tests |
| P2 | Archive one execution to GCS in the exact layout + the date reference; idempotent. | tests + kind |
| P3 | Read-back verification and durable archived-state; **no pruning yet**. | tests + kind |
| P4 | Floor computation + `apply_retention` wiring behind `NOETL_EHDB_PRUNE_ENABLED`. | tests + kind, incl. the straddling-part control |
| P5 | Retrieval by execution id (hot→archive fallthrough) and by date. | tests + kind |
| P6 | Metrics + the pinned-floor alert. | tests |
| P7 | Prod: IAM grant (owner), archive-only, measure. | owner |
| P8 | Prod: enable prune, measure reclaimed bytes. | owner |

## 13. Related

- [noetl/ai-meta#457](https://github.com/noetl/ai-meta/issues/457) — the disk-headroom risk this closes.
- [noetl/ai-meta#455](https://github.com/noetl/ai-meta/issues/455) — the north star; D8 holds archived-state.
- [ehdb#345](https://github.com/noetl/ehdb/issues/345) — a full volume is silent; why this matters.
- `agents/rules/representation-drift.md` — §8.1 is a drift hazard by construction.
- `agents/rules/self-sufficiency.md` — an object store is a *dependency*, not an external
  datastore NoETL must operate; the hot path stays EHDB-only.
