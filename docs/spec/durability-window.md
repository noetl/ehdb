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

L0 uses posture A, `FlushPolicy::EveryAppend`: the local part is `fsync`'d before
the append returns, and "the substrate replication adds N-way durability
**asynchronously**". So an acknowledged event is on **one** disk, and reaches
the durable substrate later.

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

Before the event-log tier serves primary:

- [ ] D-1 age-based seal trigger implemented, with a test that an idle shard
      seals on age alone.
- [ ] D-2 metrics emitted and **pinned**, measured from append.
- [ ] D-3 SLO alerts applied, including the `EhdbSealAgeTriggerInert` positive
      control, and each shown to fire once against an injected fault.
- [ ] Measured p99 replication lag over a soak at target rate, recorded with its
      as-of.
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
