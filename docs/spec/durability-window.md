# Spec — bounding the D1 durability window

**Status:** normative specification. **Required before the event-log tier serves
primary.**
**Tracks:** [noetl/ehdb#322](https://github.com/noetl/ehdb/issues/322), under
[#324](https://github.com/noetl/ehdb/issues/324).

> Nothing here flips a flag. This bounds a window that is currently **unbounded
> in time**, and specifies how it is measured before anything relies on it.

## 1. The window, stated exactly

D1 is the source-of-truth event log. The question this answers is: **if the
writer's node is lost, which acknowledged events do not come back?**

Two implementations exist and they have different answers. Both are in the tree.

### 1.1 `durable_eventlog_shared.rs` (#254 slice 3)

Publish is **synchronous on the append path** — `append()` calls
`publish_shard(shard)?` before returning, deliberately, because "a
cross-replica reader must see a just-appended event". The window is the gap
between the local `fsync` and the shared `append_segment` returning. Narrow, not
zero.

⚠ It has a shipped asymmetry: `local.append` has already committed when
`publish_shard` runs, so **an append that returns `Err` is still durable
locally** and is published by the *next* append's `publish_shard`, which
recomputes from the segment's current length. If the writer is fenced first, it
is lost instead. See
[`writer-election-and-fencing.md`](writer-election-and-fencing.md) §5.2.

### 1.2 `ehdb-l0` — where the real window is

L0 defaults to posture A, `FlushPolicy::EveryAppend`, and "the substrate
replication adds N-way durability **asynchronously**".

⚠ The prod writer does **not** run that default: `FeedWriter` calls
`engine.set_flush_policy(FlushPolicy::CallerDriven)` and pays **one `fsync` per
batch itself** (group commit). Durability of the ack is unchanged — `append_batch`
"returns only after the `fsync`" — but anyone reading `EveryAppend` in the L0
docs and concluding that is what prod does would be wrong. The conclusion holds
either way: **an acknowledged event is on one disk**, and reaches the durable
substrate later.

The append path is architecturally barred from closing this: "The append path
**never** calls the substrate; only the background [uploader does]" — a
deliberate property so appends do not regress when uploads are slow.

**When is "later"? Only when the part seals.** And:

```rust
pub fn should_seal(&self) -> bool {
    self.record_count > 0
        && (self.byte_len >= self.seal_max_bytes || self.record_count >= self.seal_max_records)
}
```

## 2. ⚠⚠ Finding: the window is bounded by volume, not by time

**There is no time- or age-based seal trigger anywhere in `ehdb-l0`.** The only
triggers are `seal_max_bytes` (default 8 MiB) and `seal_max_records` (default
1024).

The consequence is not a slow window — it is an **unbounded** one. A shard that
appends 3 events and then goes quiet holds them in an active part that never
seals, therefore never uploads, therefore never replicates. Those events sit on
exactly one disk for as long as the shard stays quiet. Losing the node loses
them.

⚠ This bites hardest on precisely the shards an operator would least suspect:
busy shards seal constantly and have a window of seconds; **idle shards have an
unbounded one.** The system is least durable where it is least active, which
inverts the intuition anyone reasoning about it will bring.

### 2.1 ⚠⚠ The existing metric cannot see this

`upload_lag_micros_total` is accumulated as `job.sealed_at.elapsed()` — measured
from **seal**, not from append. An event waiting in an unsealed active part
contributes **nothing** to it. The dominant term of the durability window is
structurally invisible to the only metric that names lag.

A dashboard built on it would read healthy — low lag, uploads succeeding — in
exactly the scenario where events are sitting unreplicated. This is the
absent-≠-zero shape: the number is not wrong, it is answering a different
question than the one the reader is asking.

⚠ Two further traps for whoever writes the alert:

- **The only exposed statistic is a mean** (`mean_upload_lag_micros` =
  `total / uploads`). A durability window is bounded by its **maximum**, not its
  mean. A mean of 50 ms is consistent with a p99 of 30 s.
- **`ehdb_feed_shard_lag` / `ehdb_feed_total_lag` are consumer backlog, not
  durability lag.** They already exist, they are already scraped, and their names
  are the ones an alert author will reach for first. They measure unacked
  commands. Alerting on them tells you nothing about replication.

## 2.2 ⚠⚠ In prod, the "durable substrate" is the same disk

`LocalFsSubstrate` is the **only** `DurableSubstrate` implementation in the tree.
There is no object-store substrate. The substrate is a filesystem path, so
whether it is a distinct failure domain is decided entirely by what is mounted
there — and nothing in the code enforces or checks that it is.

In prod it is not. The writer StatefulSet sets:

```
NOETL_EVENT_BUS_WRITER_DIR   = /data/eventbus
NOETL_EHDB_TIER_SERVICE_DIR  = /data/eventbus/ehdb-tier
```

and `/data/eventbus` is one PVC, `noetl-eventbus-writer-0-data`. **The substrate
copy lives in a subdirectory of the same volume as the part it is a copy of.**

So replication currently buys **no independent failure domain**. Losing the
volume loses both copies; the upload converts an unsealed local part into a
sealed local part beside it. GCE persistent disks are replicated within a zone
by the platform, so this is not a claim that loss is likely — it is a claim that
**the durability story the code tells and the one the deployment implements are
different**, and only the deployment is load-bearing.

⚠ This is why §6's RF > 1 is blocked on more than a `replicas` list: there is no
second substrate implementation to replicate *to*.

## 3. Required: a time-based seal trigger

**Requirement D-1.** *`should_seal()` gains an age trigger: an active part with
`record_count > 0` seals once its oldest record is older than `seal_max_age`.*

- `seal_max_age` default: **5 s**.
- This bounds the pre-seal term at `seal_max_age` regardless of traffic, which
  is what converts the window from unbounded to bounded. It is the single
  load-bearing change in this document.
- It requires a timer that fires on an otherwise-idle shard, since by
  construction no append will arrive to trigger the check.

⚠ **Cost, stated honestly:** sealing on age produces small parts on idle shards,
which increases part count and merge pressure. That is a real trade and L0.3
compaction is what absorbs it. 5 s is a starting value to be tuned against
measured part counts, not a constant to defend.

## 4. Required: measure the window that matters

**Requirement D-2.** *Instrument the window from **append**, not from seal.*

| metric | meaning |
| :-- | :-- |
| `ehdb_l0_unreplicated_age_seconds` (gauge, per shard) | age of the **oldest** acknowledged record not yet durable on the substrate. The window itself. |
| `ehdb_l0_unreplicated_records` (gauge, per shard) | how many such records. |
| `ehdb_l0_replicated_lag_seconds` (histogram) | append → substrate-durable, end to end. Quantiles, not a mean. |

- The gauge must be **pinned to 0 at process start for every owned shard**, so
  absent means "this binary predates the metric" and 0 means "nothing pending" —
  distinguishable. Per house rule, `Registry::gather` prunes empty families, so
  an unpinned labelled gauge is invisible until it first fires.
- `ehdb_l0_unreplicated_age_seconds` is the **alerting** signal.
  `upload_lag_micros_total` stays as a post-seal diagnostic; it is not the SLO.

## 5. The SLO

**SLO D-3.** *p99 of `ehdb_l0_replicated_lag_seconds` ≤ **10 s**, and
`max(ehdb_l0_unreplicated_age_seconds)` ≤ **30 s** across all owned shards.*

### Where these numbers come from

Measured, not chosen. `cargo run -p ehdb-l0 --example durability_soak`, four load
shapes on separate shards, 20 s:

| arm | age trigger **off** | `seal_max_age = 5 s` |
| :-- | --: | --: |
| A saturating | 5.5 s | 4.8 s |
| **B quiet** | **20.0 s** — the entire run, still climbing | **5.0 s** |
| **C trickle** | **20.0 s** | **5.1 s** |
| D bursty | 20.0 s | 5.1 s |
| seals | 4 | 10 |

Three things follow, and each fixes a threshold:

1. **The quiet and trickle arms are unbounded without the age trigger.** They did
   not plateau at 20 s; 20 s is where the run stopped. So **no threshold on
   `unreplicated_age` is satisfiable until [#329](https://github.com/noetl/ehdb/issues/329)
   is enabled** — the SLO is *conditional on D-1*, not merely aspirational.
2. **With `seal_max_age = 5 s` every arm lands at 4.8–5.1 s.** The observed
   ceiling is ~`1.02 × seal_max_age`. **30 s is therefore 6× headroom over a 5 s
   trigger** — wide enough that a page means something is wrong rather than
   merely busy.
3. **p99 ≤ 10 s is 2× the trigger.** The soak's mean append→durable was 4.87 s
   with the trigger off and 4.93 s with it on: append→durable is dominated by the
   *pre-seal* wait, so the trigger sets the p99 almost directly.

⚠ These are **local-filesystem** numbers. They bound the *pre-seal* term, which is
the dominant one; substrate latency on a real medium adds to the post-seal term
and must be re-measured before the thresholds are trusted on prod hardware.

### ⚠⚠ The number that decides which metric the SLO can use

Same instant, same engine, from the same soak run:

```
mean append→durable             4.869 s   ← what D-3 measures
seal-relative upload_lag mean   0.042 s   ← upload_lag_micros_total
```

**116× apart.** Not a calibration offset — a different quantity. An SLO written
against `upload_lag_micros_total` would have been met continuously throughout the
run in which two arms held records unreplicated for its entire duration.

### The metric names D-3 is written against

| threshold | metric |
| :-- | :-- |
| `max(...) ≤ 30 s` | `ehdb_l0_unreplicated_age_seconds{shard}` |
| p99 ≤ 10 s | `ehdb_l0_replicated_lag_seconds` (histogram) |
| — | `ehdb_l0_unreplicated_records{shard}` (how many, for triage) |
| — | `ehdb_l0_durability_sample_ok` (⚠ 0 ⇒ readings in that window are **unknown**, not healthy) |

⚠ **Not** `upload_lag_micros_total`, `mean_upload_lag_micros`, or any
`ehdb_feed_*_lag` series — the first two are seal-relative and the third is
consumer backlog. See §2.1.

| alert | condition | meaning |
| :-- | :-- | :-- |
| `EhdbUnreplicatedWindowExceeded` | `max(ehdb_l0_unreplicated_age_seconds) > 30` for 2 m | events acked and on one disk beyond the window |
| `EhdbReplicationStalled` | `max(...) > 300` for 5 m | replication is not progressing; treat as an outage of durability |
| `EhdbSealAgeTriggerInert` | `seal_max_age` configured, yet a shard's unreplicated age exceeds `2 × seal_max_age` | the age trigger exists but is not firing |

The third alert is the positive control. Without it, a broken age trigger and a
perfectly idle system produce the same reading — and a check that can only pass
is not a check.

## 6. Optional: synchronous quorum ack

For deployments that cannot accept **any** window, an opt-in policy:

**`AckPolicy::QuorumDurable`** — an append does not return until the record is
durable on `W` of `N` substrate replicas.

- Requires RF > 1. **L0 writes a single replica today** (`PartMeta::replicas`
  and a per-part write loop are the designed-in seam; N-way copy is the additive
  step). So this is **blocked on RF > 1**, not merely unimplemented.
- Because parts are immutable, N-way copy needs no consensus — the HDFS
  block-replication model. Quorum ack here is a *waiting* policy, not a
  consensus protocol, and does not reintroduce the retired per-shard-Raft plan.
- ⚠ It cannot be built on the sealed-part path as it stands: quorum ack must
  cover the **record**, and records become substrate-visible only at seal. Either
  the ack waits for a seal (coupling append latency to `seal_max_age` — 5 s,
  unacceptable) or the active part itself replicates. **The second is the real
  work**, and naming it is the point of this section: quorum ack is not a small
  addition on top of D-1 and D-2.

Default stays `AckPolicy::LocalDurable` (today's behaviour). This section
specifies a shape; it does not commit to building it.

## 7. Gate on primary-serve

⚠⚠ **This gate does not read on the event-log tier, which is already `primary`
and serving on prod (since 2026-08-13).** Stating it as a precondition would be
false. The honest reading is the uncomfortable one: **a tier is in production
carrying the unbounded window of §2 and the same-disk substrate of §2.2.** That
makes D-1 and D-2 remediation of a live gap, not preparation for a future one,
and it raises their priority rather than lowering it.

The gate below therefore binds **further promotions** — projection, KV, object —
and the remediation items bind the event-log tier now.

Before any further tier serves primary:

- [ ] D-1 age-based seal trigger implemented, with a test that an idle shard
      seals on age alone.
- [ ] D-2 metrics emitted and **pinned**, measured from append.
- [ ] D-3 SLO alerts applied, including the `EhdbSealAgeTriggerInert` positive
      control, and each shown to fire once against an injected fault.
- [ ] Measured p99 replication lag over a soak at target rate, recorded with its
      as-of. ⚠ The soak **must include a deliberately quiet arm** — a saturating
      run cannot detect the defect it is being run to measure. Harness:
      `cargo run -p ehdb-l0 --example durability_soak`.
- [ ] [#321](https://github.com/noetl/ehdb/issues/321) merged and implemented —
      fencing prevents a fork; it does not prevent this loss. Both are required.

⚠ RF > 1 is **not** on this list. The window can be bounded and observed at
RF=1; that is what D-1..D-3 achieve. RF > 1 changes the window's *consequence*
(node loss stops being data loss) and is tracked separately. Conflating the two
would block a shippable improvement behind a much larger one.

## 8. Related

[#321](https://github.com/noetl/ehdb/issues/321) election + fencing ·
[#254](https://github.com/noetl/ehdb/issues/254) durable event-log backend ·
[#261](https://github.com/noetl/ehdb/issues/261) load/perf at target scale ·
[#241](https://github.com/noetl/ehdb/issues/241) completion program ·
[`SCOPE.md`](../SCOPE.md) four-engine scope.
