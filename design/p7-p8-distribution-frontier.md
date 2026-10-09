# P7 / P8 — the distribution frontier: what a real second copy and a real single writer require

**Status:** design only · **Opened:** 2026-10-09 · Program:
[noetl/ai-meta#455](https://github.com/noetl/ai-meta/issues/455) P7/P8

> ⚠⚠ **No prod changes, no durability-semantics changes, nothing built.** This is the
> audit and the phasing. Several phases change durability or safety semantics and are
> explicitly marked as needing an owner decision *before* any build.

This document does **not** re-specify work that is already specified. Four specs already
exist and this one defers to them:

| | what it covers | state |
| :-- | :-- | :-- |
| [`docs/spec/second-substrate.md`](../docs/spec/second-substrate.md) | what a real independent failure domain requires | shipped the abstraction; inert |
| [`docs/spec/second-substrate-choice.md`](../docs/spec/second-substrate-choice.md) | the candidate comparison; recommends **GCS** | awaiting owner prerequisites |
| [`docs/spec/writer-election-and-fencing.md`](../docs/spec/writer-election-and-fencing.md) | election + fencing design, failover semantics | state machine shipped in `ehdb-reference`; not wired |
| [`docs/spec/lease-election-k8s-binding.md`](../docs/spec/lease-election-k8s-binding.md) | the Kubernetes `Lease` adapter | **specified, not built** |

What this document adds: a **measured** picture of today's state, the **honest bounds** on
what P7/P8 can promise, one **architectural option the existing specs do not consider**,
and a phasing with risk tiers.

---

## 1. Measured state, 2026-10-09

### 1.1 ⚠⚠⚠ Prod runs RF=1, and every durability signal reads healthy

```
/data/ehdb-embedded/substrate  -> device 66320        }  the SAME device,
/data/ehdb-embedded/local      -> device 66320        }  /dev/nvme0n5

ehdb_l0_replica_domain_violations{dataset="d1_event_log"} 0
ehdb_l0_parts_under_replicated{dataset="d1_event_log"}    0
ehdb_l0_parts_local_only{dataset="d1_event_log"}          0
ehdb_l0_manifest_parts{dataset="d1_event_log"}          319
```

**That 0 is not a verdict. It was never evaluated.**

`L0Engine::open(config, substrate)` wraps the substrate in
`vec![ReplicaTarget::new("replica-0", substrate)]` — a replica set of **exactly one** — and
the failure-domain check is gated on `if replicas.len() >= 2`, because "one replica makes no
spreading claim". The server calls `open()`. So the check short-circuits, the counter is
pinned at 0 by the healthy path, and an operator watching it sees a green number for a
configuration that cannot survive losing the node.

The code already names this shape, one rung above where prod sits:

> *"An RF of N over one domain is an RF of 1 wearing a larger number, and until this call
> site existed the larger number is all anyone could see."* — `engine.rs:424`

Prod is the rung below: **RF=1, and the check that would say so is unreachable.**

⚠ `ehdb_l0_replica_writes = 2` is not a contradiction — it is a since-boot counter and the
pod rolled recently. It is not a replication-factor signal and must not be read as one.

### 1.2 `FailureDomain::Remote` exists and no production path can produce it

The enum already has what P7 needs:

```rust
pub enum FailureDomain {
    LocalDevice { device_id: u64, root: PathBuf },
    Remote { provider: String, bucket: String },   // ← exists
    Ephemeral { instance: String },
    Undeclared,
}
```

`FailureDomain::Remote` is constructed in **tests only**. `for_path` yields `LocalDevice`
or `Ephemeral`. And `survives_node_loss` requires at least one `Remote`:

> *"two separate local disks on one node are different domains and still both die with the
> node."*

So **`survives_node_loss` is false by construction today**, for every deployment, and
nothing calls it in production either. This is the "built ahead of its consumers" pattern
([#332](https://github.com/noetl/ai-meta/issues/332)) in its purest form: the model is
right, the predicate is right, and no code path can make either of them true.

### 1.3 Only two substrates exist, both local

`LocalFsSubstrate` and `InMemorySubstrate`. Nothing writes off-box. The
`on_upload_done` / `replica_writes` / under-replication machinery is replica copies
**between local substrates**.

### 1.4 ⚠⚠ The exposure is the unsealed tail, and on the tier it is unbounded in time

Sealed-part replication is implemented. The tail is not — and the two seal triggers are
configured differently on the two stores:

```
bus    NOETL_EHDB_SEAL_MAX_AGE_MS     = 5000           → seals every 5s
tier   NOETL_EHDB_TIER_SEAL_MAX_BYTES = 268435456      → seals every 256 MiB
tier   (no age bound configured)
```

⭐ **Verified in code, not recalled.** `with_seal_max_age` is called at exactly three
sites, all in `noetl/worker` — `command_bus.rs:225`, `event_bus.rs:262` and
`ehdb/eventlog_backend.rs:1379`. `ehdb/tier_store.rs` reads **only**
`NOETL_EHDB_TIER_SEAL_MAX_BYTES` and calls `with_seal_max_age` **zero** times, and the
library default is `None`.

So the **tier's unsealed tail is bounded by bytes and not by time**: it can hold up to
256 MiB of acked events, on one PVC, for as long as it takes to accumulate them. The two
buses *and* the eventlog backend all get the 5 s bound — so the reassuring number is real
and applies to all three of the engines that are not the store at risk. This is the
per-engine asymmetry that has bitten before: the writer pod hosts two L0 engines plus the
tier, and configuring one is not configuring another.

⚠ Separately: `noetl_ehdb_crossstore_pending_total{tier="eventlog"} = 552` — a different
concern (mirror lag), recorded here so it is not conflated with the tail.

### 1.5 Election and fencing: a state machine, no binding, and a shipped asymmetry

`ehdb-reference::election` + `::fencing` ship the state machine against a `LeaseStore` with
real compare-and-swap. **The server links neither** — its only ehdb dependencies are
`ehdb-feed` and `ehdb-l0` at `v0.5.0`. The Kubernetes adapter is specified and not built,
parked on a dependency decision (`kube` drags `k8s-openapi` + `tower` + `hyper` + TLS into
a crate tree with none of it).

⚠⚠ And there is operational precedent that the fencing *placement* is the hard part, not
the algorithm: on 2026-09 the M5 fencing guard was "ACTIVE, ENFORCING" and **served a
superseded writer**, because the guard sat on `append_segment` and the publish path skips
it when the stale writer's segment is no longer than the holder's. The existing spec
already states the rule that follows — *"a stale writer must be rejected **by the store**,
not asked to check first"* — and that rule is the whole of P8's difficulty.

---

## 2. Honest bounds — what P7/P8 can and cannot promise

This section exists because the program's headline is *"like etcd but truly distributed"*,
and that phrase oversells what is being built. Stating the limits is cheaper now than
discovering them in an incident.

### 2.1 What etcd actually guarantees

- **Linearizable** reads and writes over one Raft-replicated keyspace.
- **Quorum-based**: needs 2f+1 members; survives f failures.
- **CP**: without quorum it refuses writes — it does not degrade to stale accepts.
- **No sharding at all.** etcd has one Raft group; there is no cross-shard story to get
  wrong because there are no shards.

### 2.2 What EHDB is, structurally

A **per-shard single-writer append log** with asynchronous replication and **no consensus
protocol**. Its ordering guarantee is per-shard, per-writer. There is no quorum, no
agreement round, and no voting.

⭐ **So EHDB is not etcd-with-sharding; it is a WAL with a lease.** The honest comparison:

| | etcd | EHDB today | EHDB with P7+P8 |
| :-- | :-- | :-- | :-- |
| write safety under partition | quorum, CP | ⚠ nothing prevents two writers | lease + **fenced at the store** ⇒ at most one accepted writer |
| durability on ack | replicated to quorum | ⚠ one local device | sealed parts in a remote domain; **tail still local** (see §3.2) |
| read consistency | linearizable | per-shard read-your-writes from the owner | unchanged — P7/P8 do not touch read consistency |
| cross-shard atomicity | n/a (no shards) | none | **none, and this is a theorem, not a gap** |
| availability without quorum | refuses | accepts | minority side must **stop accepting** |

### 2.3 The three lower bounds, named

**FLP.** No deterministic consensus in a fully asynchronous system with one crash failure.
Any election is therefore **partially synchronous** — it rests on leases and timeouts. The
consequence is concrete: **a failover window cannot be eliminated, only bounded**, and the
bound is a function of lease duration and clock error. Any claim of "instant, safe
failover" is false.

**CAP, as it actually bites here.** A lease-based single writer means that under partition
the minority side **must stop accepting writes**. That is choosing C over A *for the write
path*, and it is a real availability cost: a writer that cannot reach the lease store must
fence itself, even if its clients can reach it. ⚠ This is the opposite of today's
behaviour, where a partitioned writer keeps accepting — so P8 **reduces write availability
in exchange for safety.** That trade is the decision, and it is the owner's.

**CALM.** A program has a coordination-free, consistent implementation **iff** it is
monotonic.

- **Appends are monotonic** ⇒ the append path can stay coordination-free. This is why
  EHDB's core is sound without consensus, and it is a real result, not a workaround.
- **"The latest value of X", cross-shard snapshots, and any read that must observe the
  absence of a later write are non-monotonic** ⇒ they *require* coordination.

⭐ Therefore: **there is no coordination-free linearizable cross-shard read, and there
never will be.** Not "not yet implemented" — it is excluded by the theorem. A cross-shard
linearizable read needs a coordination round (a global sequencer, a transaction manager, or
reading a quorum), and each of those costs the latency this architecture exists to avoid.

What EHDB *can* offer cross-shard, coordination-free, is a **monotonic** read: a consistent
*prefix* per shard, with a merge that is correct under reordering. That is strictly weaker
than linearizable and it is enough for replay, projection and audit — the things this log is
actually for.

### 2.4 ⚠ Two things that must not be claimed after P7/P8

1. **"Truly distributed" ≠ consensus.** P8 gives **mutual exclusion**, not agreement. At
   most one writer is *accepted*; there is no voting on content, no quorum, and no
   tolerance of writer loss without a failover gap. Calling it a consensus system would be
   wrong.
2. **A second copy is not a quorum.** P7 makes data survive a node; it does not make the
   system available when the node is gone. Failover availability is P8's job, and P8's
   bound is §2.3's failover window.

---

## 3. P7 — a real second copy

### 3.1 ⭐ The architectural option the existing specs do not consider

`second-substrate-choice.md` lists as owner prerequisite 4:

> *"no ehdb crate speaks GCS or HTTP today. This adds the first such client to the
> workspace — the same class of decision that parked F1's kube adapter."*

**That prerequisite can be retired without adding any dependency to ehdb.**

`DurableSubstrate` is a **trait**, and `L0Engine::open_replicated` takes
`Vec<ReplicaTarget>` built from `Arc<dyn DurableSubstrate>`. So a `GcsSubstrate` can be
implemented **in the server's own crate**, where a working GCS client already lives, and
*injected*. `ehdb-l0` gains nothing.

Two measured facts make this viable rather than merely expressible:

- **The trait is entirely synchronous** (`fn put_if_absent(&self, …) -> Result<bool>`, no
  `async`), and
- **the substrate write happens on the uploader's dedicated `std::thread`**
  (`engine.rs:651` `std::thread::Builder::new()`; the put is at `engine.rs:1768`), **not on
  the append path.**

So a blocking GCS put is architecturally acceptable *there* — a sync HTTP client, or a
`block_on` against a dedicated runtime, costs the uploader thread latency and nothing on
the hot path. ⚠ This would be unacceptable if the substrate were written during `append`;
it is not.

⚠ And #459 already retired the *other* half of prerequisite 4 for the server: a GCS client
with Workload Identity is live in prod there, having written 21,569 objects.

### 3.2 ⚠⚠ But the #459 archive is **not** a P7 solution, and it is important not to claim it is

The archive is a genuine off-box copy, and it is tempting to read it as "P7, done". It is
not, for three reasons:

1. **It only holds terminal, expired executions.** Archiving triggers on
   *completed + older than the retention window*. The data most at risk — just-acked,
   not-yet-sealed — is never in it.
2. **It is asynchronous by hours to days.** The retention window is 48h by default.
3. **It is downstream of sealing.** It reads what the hot store already has; it cannot
   contain anything the hot store lost.

⭐ What the archive *does* give P7: proof that the off-box path, the auth, and the
dependency are all workable, plus an operational precedent. It makes P7a cheap. It does not
make it done.

### 3.3 The split that matters

**P7a — sealed-part replication to a remote domain.** Mechanically ready. Inject
`GcsSubstrate` as a second `ReplicaTarget`; the uploader is already the write path; the
conformance suite in `second-substrate.md` is the acceptance test and must pass
**unchanged**. Additive, flag-gated, no semantics change on the append path. **Low risk.**

**P7b — the unsealed tail.** Genuinely hard, and the only part that changes durability
semantics. Three shapes, none free:

| option | what it costs | what it buys |
| :-- | :-- | :-- |
| **(i) synchronous remote append per record** | ⚠ **estimated, not measured**: a same-region GCS `put` is conventionally single-digit-to-tens of ms against the ~4 ms local `fsync` this architecture already treats as its append ceiling. Order-of-magnitude on every append. **B1 must measure a real put from the writer pod before this option is dismissed on the number** | tail RF>1, no loss window |
| **(ii) a streaming replica** (a second EHDB instance tailing the WAL) | a new deployable, its own failure modes, and the replication protocol this architecture has avoided | tail RF>1 with low added latency; **this is the real "truly distributed" step** |
| **(iii) bound the window** (set `seal_max_age` on the tier, shrink `seal_max_bytes`) | more, smaller parts ⇒ more manifest churn, and memory is O(parts) | a **bounded, stated** loss window — not elimination |

⚠⚠ **(iii) is the honest first move and it is a semantics change even so**: it converts an
unbounded-in-time exposure into a bounded one, which is a genuine improvement *and* a
change in part-size distribution that affects memory and merge cost. It must be measured,
not assumed.

⭐ **Recommendation: do (iii) first, measured, and treat (ii) as the actual P7b.** ⚠ (i) is
*provisionally* rejected on the latency arithmetic above — but that arithmetic rests on an
**unmeasured** put latency, so B1 owes a real number from the writer pod before the
rejection is treated as settled. Rejecting an option on an estimate and then citing the
rejection as established is how a guess becomes a constraint.

---

## 4. P8 — a lease that actually excludes

### 4.1 What is already settled, and must not be redesigned

`writer-election-and-fencing.md` already establishes:

- a `Lease` alone is **not** sufficient (clock skew);
- the fencing **token must be checked by the store**, not by the writer;
- the split-brain invariant needs **both** the lease and the monotonic epoch, because the
  first can be violated by clock skew and the second cannot;
- the failover window's semantics, and what is *not* guaranteed about the suffix — an
  append that had committed **locally** but not been acked may or may not survive.

`lease-election-k8s-binding.md` establishes the Kubernetes mapping, and the one subtlety
worth repeating because getting it wrong silently mints duplicate epochs: **`leaseTransitions`
is incremented by the caller, from the value read under the same `resourceVersion` being
swapped on.** And ⭐ **the 409 *is* the mutual exclusion** — an adapter that "helpfully"
retries a 409 by re-reading and writing again has thrown the guarantee away.

### 4.2 What this document adds

**The placement, not the algorithm, is the risk.** The M5 incident is the proof: fencing
that is active and enforcing can still serve a superseded writer if the check sits on a
path the write can skip. So P8's acceptance test is not "does the state machine elect
correctly" — that is already proven — but:

- [ ] **enumerate every path that mutates durable state**, and show each one passes through
      the fence;
- [ ] a **planted stale writer** is rejected on *each* of those paths, not on the one the
      test author thought of;
- [ ] `ehdb_fencing_stale_observed_total` is observed at 0 over a soak **before** the gate
      is enforced, so a non-zero reading afterwards means something.

⚠⚠ That first bullet is the one that was skipped last time, and it is the difference between
a fence and a fence-shaped object.

### 4.3 The dependency decision is still open

The adapter needs a Kubernetes client. Two ways out, and it is an owner choice:

- **(A) `kube` in ehdb** — the dependency the spec parked on.
- **(B) ⭐ the same injection trick as §3.1**: `LeaseStore` is a trait, so the *server*
  implements it against the API server (it already speaks HTTP) and injects it. `ehdb`
  gains nothing. ⚠ But note the asymmetry with the substrate: the lease is on the **writer**
  (the `cmdbus-writer` pod), not the server, so this only helps if the writer is the thing
  that grows the client — which is a different binary and a different decision.

---

## 5. Phasing with risk tiers

| phase | content | changes semantics? | risk | gate |
| :-- | :-- | :-- | :-- | :-- |
| **A1** | Make RF=1 **visible**: call `survives_node_loss` and expose it; stop pinning a green 0 for an unevaluated check | no — observability only | 🟢 low | mine |
| **A2** | `GcsSubstrate` in the server + the conformance suite passing **unchanged**, wired as a second `ReplicaTarget` **behind a default-off flag** | no while off | 🟢 low | ⚠ needs the §3.1 injection decision |
| **A3** | Enable RF>1 on one non-critical dataset, measure uploader latency and part-write amplification | durability improves; write path unchanged | 🟡 medium | **owner** |
| **B1** | Measure (a) the tier's actual tail size/age distribution — the exposure above is inferred from config, not observed — and (b) a real same-region GCS `put` latency from the writer pod, which option (i) was provisionally rejected on | no | 🟢 low | mine |
| **B2** | Bound the tail: set `seal_max_age` on the tier, measure the part-count / memory / merge cost | ⚠ **yes** — part distribution, memory O(parts) | 🟡 medium | **owner** |
| **B3** | Streaming tail replica (the real P7b) | ⚠⚠ **yes** — a new deployable and a replication protocol | 🔴 high | **owner, design-first** |
| **C1** | Enumerate every durable-state mutation path and prove each passes the fence; `fencing_stale_observed` at 0 over a soak | no — audit | 🟢 low | mine |
| **C2** | `LeaseStore` binding (A or B of §4.3) | no while unwired | 🟡 medium | ⚠ **owner: dependency decision** |
| **C3** | Enforce the fence on the live writer path | ⚠⚠ **yes** — a partitioned writer stops accepting; **write availability drops** | 🔴 high | **owner, with the §2.3 trade stated** |

### 5.1 ⚠ What must not be built blind

**B3** and **C3**. B3 introduces a replication protocol; C3 trades write availability for
safety and will make a partitioned writer refuse work that it accepts today. Both need the
owner to accept the trade explicitly, and C3 needs §2.3's failover-window bound written
down as a number before it is armed.

**A1, B1 and C1 are pure measurement/observability** and are the right next work: all three
replace an inferred belief with a number, and two of them (A1, C1) address a signal that is
currently green for the wrong reason.

---

## 6. Acceptance criteria for this document

- [x] Today's state measured, not inferred — including the finding that prod's durability
      signals are green because the check is unreachable.
- [x] The bounds stated: FLP ⇒ a failover window exists; CAP ⇒ P8 reduces write
      availability; CALM ⇒ no coordination-free linearizable cross-shard read, ever.
- [x] The comparison to etcd made honestly, including that EHDB has no consensus and etcd
      has no shards.
- [x] The #459 archive's role stated without overclaiming it as P7.
- [x] Phases tiered, with the three that change semantics marked owner-gated.
- [ ] Owner decisions recorded: §3.1 injection, §4.3 lease binding, and the B2/B3/C3 trades.

## 7. Related

- [`second-substrate.md`](../docs/spec/second-substrate.md) ·
  [`second-substrate-choice.md`](../docs/spec/second-substrate-choice.md) ·
  [`writer-election-and-fencing.md`](../docs/spec/writer-election-and-fencing.md) ·
  [`lease-election-k8s-binding.md`](../docs/spec/lease-election-k8s-binding.md)
- [`retention-archival-tier.md`](retention-archival-tier.md) — the off-box path that exists.
- `agents/rules/self-sufficiency.md` — *no external datastore* does not mean *no
  dependencies*; a library is not a service. Both the GCS client and a SWIM crate are
  libraries.
- `agents/rules/representation-drift.md` — §1.1 is a worked example: a green gauge for an
  unevaluated check.
