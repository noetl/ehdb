# Spec — single-writer election and fencing

**Status:** normative specification. **This is a hard gate.**
**Tracks:** [noetl/ehdb#321](https://github.com/noetl/ehdb/issues/321), under
[#324](https://github.com/noetl/ehdb/issues/324).

> ## ⛔ No tier is promoted to primary-serve until this document is merged
> and its mechanism implemented. Shadow operation is unaffected.

## 1. What rests on this

Every tier's ordering guarantee reduces to one assumption: **for each shard, at
most one node appends at a time.** `global_sequence` is a per-shard counter
assigned by that writer; parity comparators, the projection fold, the catalog
relation and recovery all treat its order as authoritative.

Single-writer with **no failover** is not a consensus problem. Single-writer
**with failover** is one in disguise: the moment ownership can move, two nodes
can believe they own the same shard, and the log can fork or lose a suffix.

## 2. What exists today — and what does not

Established by reading the tree, not assumed:

- **No epoch, fencing token, lease or election exists anywhere.** A search for
  `epoch|fencing|lease|election|single.writer` across `crates/` returns only
  unrelated matches (lock release, `plan_selection`).
- Single-writer is enforced **only by deployment cardinality**:
  `statefulset/noetl-cmdbus-writer` with `replicas: 1`.
- The shared-store contract does not reject anything:
  - `put_segment(shard, segment_id, bytes)` **overwrites unconditionally** —
    "overwrites atomically (a reader never sees a partial object)";
  - `append_delta(committed_len, delta)` appends at a length the **caller**
    supplies.
- The segment key is `noetl.ehdb.seg.{shard:08x}.{segment_id:016x}` — 40 chars,
  fixed width, asserted by a test. **It carries no epoch.**

### ⚠ The gap, stated exactly

`replicas: 1` is an **orchestration preference, not a mutual-exclusion
primitive.** A partitioned node whose kubelet is unreachable has its pod shown
as `Terminating` while the process keeps running — and keeps appending. During
that window Kubernetes may schedule a replacement. Two writers then hold the
same shard, and **nothing in the storage contract will refuse either of them**:
one overwrites the other's segment, or both `append_delta` at the same
`committed_len` and the prefix is corrupted.

That is the failure this spec closes. It has not happened, which is not evidence
that it cannot.

## 3. Election

**Mechanism: Kubernetes `coordination.k8s.io/v1` Lease objects. Not Raft.**

One Lease per shard, named `ehdb-shard-{shard:08x}`, in the writer's namespace.

- A node acquires ownership by successfully writing the Lease with itself as
  `holderIdentity`, using the resource's `resourceVersion` for compare-and-swap.
  The API server's optimistic concurrency is the mutual exclusion.
- `leaseDurationSeconds` = **15**. The holder renews every **5** seconds
  (`renewTime`). Three missed renewals lose the lease.
- A candidate may acquire only when `now > renewTime + leaseDurationSeconds`.

**Why Leases and not Raft.** Raft here would mean operating a consensus cluster
to protect a decision the API server already makes with a compare-and-swap, and
etcd — which backs the API server — *is* the Raft cluster. Building a second one
adds an operational surface without adding a guarantee. §7 records the migration
path if that stops being true.

⚠ **A Lease alone is not sufficient**, and this is the point most implementations
miss. Lease expiry is decided by *clocks*; a paused or partitioned holder can
believe it still holds a lease that has expired elsewhere. The Lease elects; it
does not fence. Fencing is §4.

## 4. Fencing

### 4.1 The token

Each Lease acquisition mints a **monotonically increasing epoch** for that shard.
The Lease's `spec.leaseTransitions` — incremented by the API server on every
holder change — is the source, so monotonicity is guaranteed by the same
compare-and-swap that granted the lease. The writer records
`epoch = leaseTransitions` at acquisition and stamps it into **every segment
frame it appends**.

### 4.2 ⭐ The token must be *checked*, not merely carried

A token in the frame is data. Unless something **rejects** on it, a stale writer
writes a frame that says `epoch=4` into a log that has already seen `epoch=5`,
and the frame is accepted. So the enforcement point is the storage contract:

**Invariant F.** *For each shard, the durable store must refuse any append whose
epoch is lower than the highest epoch it has already durably accepted for that
shard.*

This requires two changes to a contract that today refuses nothing:

1. **`put_segment` and `append_delta` take the writer's `epoch`** and return a
   typed `StaleEpoch { observed, highest }` rejection rather than overwriting.
2. **The shared store persists `highest_epoch` per shard**, updated in the same
   atomic operation that commits bytes. A separate marker updated afterwards
   would leave a window in which the bytes are committed and the epoch is not.

⚠ A stale writer must be rejected *by the store*, not asked to check first. A
writer that checks and then writes has a race between the two.

### 4.3 What a fenced writer does

On `StaleEpoch` the writer **stops appending immediately**, drops its in-memory
shard state, and does not retry: it has been superseded and its unpublished
suffix is not authoritative. It must not attempt to reconcile — a fenced writer
reconciling is how a fork becomes durable.

## 5. Failover semantics

### 5.1 The window

```
t0   holder H renews; epoch E
t1   H partitions or pauses. It may still be appending locally.
t2   t1 + 15 s: the Lease expires
t3   candidate C acquires; leaseTransitions -> E+1
t4   C cold-loads the shard from the shared store and resumes at epoch E+1
```

**Between t1 and t4 the shard has no writer** — bounded by
`leaseDurationSeconds` (15 s) plus cold-load time. Appends fail; they do not
silently succeed against a stale owner.

Between t1 and t3, H may still write. Those writes carry epoch `E`. Until t3 they
are legitimate — H genuinely still holds the lease as far as the store knows.
**After t3 they are refused by Invariant F**, because the store has accepted
`E+1`.

### 5.2 What is guaranteed about the suffix

**Guarantee.** *After failover, the log's committed prefix is exactly what the
store durably accepted before the new epoch's first append. Nothing accepted is
lost, and nothing from a fenced epoch is admitted afterwards.*

⚠ **What is not guaranteed:** an append H had committed **locally** but had not
yet published to the shared store is invisible to C, which cold-loads from
shared. This is *not* a publish backlog — publish is synchronous on the append
path (`append` calls `publish_shard(shard)?` before returning), so the window is
the gap between the local `fsync` and the shared `append_segment` returning,
per append. It is narrow, and it is not zero.

⚠⚠ **There is a shipped asymmetry here that the fencing design must not paper
over.** `local.append` has *already committed* when `publish_shard` runs, so an
append whose publish fails returns `Err` to the caller while remaining durable on
the writer's local disk. If that writer survives, the next `publish_shard`
recomputes from the segment's current length and the "failed" event is published
after all. If it is fenced first, the event is lost instead. **The same failure
therefore resolves two different ways depending on whether the writer keeps its
lease.** Bounding and closing that is
[#322](https://github.com/noetl/ehdb/issues/322)'s; **fencing prevents a fork, it
does not prevent this loss**, and neither issue closes it alone.

### 5.3 Cold load and fungible writers

The existing design already lets a new owner with an empty local disk cold-load
from the shared store. Fencing does not change that; it adds one step. The new
owner reads `highest_epoch` for the shard, asserts its own epoch exceeds it, and
only then appends. A new owner whose epoch is **not** greater has been fenced
before writing a byte — the correct outcome, and the cheapest place to discover
it.

## 6. Split-brain invariant

**Invariant S.** *At most one node may hold shard S's Lease at any instant, and
at most one epoch may have appends durably accepted for S at any point in the
log.*

The first half is the API server's compare-and-swap. The second is Invariant F.
Both are needed: the first can be violated by clock skew, the second cannot.

**How it is verified rather than asserted:**

- A conformance test drives two writers at epochs `E` and `E+1` against one
  store and asserts every `E` append after the first `E+1` append is rejected
  with `StaleEpoch`.
- A **positive control** in the same test asserts an `E+1` append succeeds — so
  a store that rejected everything could not pass.
- The `highest_epoch` marker is asserted to advance monotonically and never
  regress across a simulated crash between byte commit and marker update.

## 7. Migration path

If per-shard write rate outgrows a Lease renewal, or the Kubernetes API server
becomes an unacceptable dependency, the election mechanism can be replaced
without touching the storage contract: **Invariant F is independent of how the
epoch is minted.** Any source of monotonically increasing per-shard epochs — a
Raft group, a sequencer — substitutes for `leaseTransitions`. That separation is
deliberate, so the expensive part is not coupled to the cheap part.

## 8. Conformance checklist

- [ ] Lease per shard, 15 s duration, 5 s renewal, CAS acquisition.
- [ ] Epoch = `leaseTransitions`, stamped into every segment frame.
- [ ] `put_segment` / `append_delta` accept an epoch and return `StaleEpoch`.
- [ ] `highest_epoch` persisted per shard, atomic with the byte commit.
- [ ] Fenced writer halts; no retry, no reconcile.
- [ ] Two-writer conformance test with its positive control.
- [ ] Monotonicity across a crash between commit and marker update.

## 9. Related

[#322](https://github.com/noetl/ehdb/issues/322) bounds the loss window this
spec deliberately does not close · [#254](https://github.com/noetl/ehdb/issues/254)
durable event-log backend · [#261](https://github.com/noetl/ehdb/issues/261)
load/perf at target scale · [#241](https://github.com/noetl/ehdb/issues/241)
completion program · [`docs/SCOPE.md`](../SCOPE.md) four-engine scope.
