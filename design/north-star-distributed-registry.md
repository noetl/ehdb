# North star: EHDB as NoETL's distributed registry and control plane

**"etcd, but truly distributed"** — plus an append log, catalog relations, a topology /
service registry with discovery, secret *references*, and vectors for AI.

This spec is **measure-first**: every "exists" below was checked against `origin/main` on
**2026-10-08**, not recalled. Three of my own prior claims were wrong and are corrected
inline, because a spec built on a bad inventory specifies work that is already done.

## Scope boundary, first

This is **internal EHDB**: self-sufficient, **no external datastore, ever**. Libraries are
welcome; a second service to operate is not. The **business catalog** (travel's domain data
in Firestore) is explicitly *not* this and must not be pulled in — see
`noetl/travel` `docs/architecture/business-catalog-firestore.md`.

## Inventory: what exists vs what is new

| capability | state | evidence |
| :-- | :-- | :-- |
| **Append log** | ✅ **done and now measured** | `ehdb-l0` L0 engine; 267 rec/s posture A, 5 450 group-committed; read linear at ~55 ns/record to 1 024 then a 15.6x step |
| **Catalog relations** | ✅ **done** | `noetl/catalog` on EHDB — 4 datasets, reverse indexes for type/attribute/edge, full CRUD over `/api/catalog/*` |
| **Service registry** | ✅ **largely done — corrected** | `ehdb-l0/src/runtime.rs` (**D8**, `d8_runtime`): `register` · `heartbeat` · `deregister` · `get` · `list_live` · `list_live_since`. ⚠ I had recorded D8 as "implemented and never wired" — it is in the **production** crate and exported from `lib.rs`. |
| **Vectors** | ✅ **largely done — corrected** | `ehdb-l0/src/vector.rs` (`cosine`), `ehdb-reference/src/vector.rs` (`cosine_similarity`, `upsert_query_ranks_by_cosine`), `ehdb-retrieval`. ⚠ I had listed vectors as a "hard part to build". Storage + cosine ranking exist. |
| **Gossip membership** | ⚠ crate exists, decision made | `ehdb-gossip` — identity, origin-trust, notification→D8 seam; **foca** chosen (ai-meta#332) |
| **Replication** | ⚠ partial, and the gap is specific | sealed-part N-way copy **is** implemented; the **unsealed tail is RF=1**, and prod's second "replica" is the *same PVC* |
| **Leases / TTL liveness** | ⚠ **two half-answers** | `LeaseRecord` lives in `ehdb-reference/src/election.rs`; D8 liveness is a **monotonic heartbeat counter**, not a wall-clock TTL |
| **Watch / subscribe** | ❌ **absent** | **zero** `pub fn watch`/`subscribe` in the workspace. The substrate exists: `read_partition_after(shard, after_seq)` is a cursor read. |
| **Ephemeral runtime units** (live executions, TTL) | ❌ | D8 is worker-shaped: the key is `worker_id`, the payload a `contract` string |
| **Secret references** | ⚠ thin | 12 hits / 4 files. The *platform* rule already exists (keychain alias, resolved at step execution) — EHDB has no reference **type** |

**The headline: four of the seven capabilities are substantially built.** The genuine gaps
are **watch**, **time-based leases**, **generic/ephemeral registration**, **secret-reference
types**, and **tail replication**.

## A. "Truly distributed" — and the honest lower bounds

What etcd gives: linearizable reads/writes via Raft, watch, leases, and a membership
protocol. EHDB is **event-sourced and coordination-averse**, so it cannot simply adopt that
shape — and should not pretend the difference away.

| requirement | EHDB's answer | honesty |
| :-- | :-- | :-- |
| **Replication** | sealed-part N-way copy (done) + tail | ❌ the **unsealed tail is RF=1**. This is *the* durability gap, and it is not a design question — it is unbuilt. |
| **Consensus** | **deliberately avoided** for the data path | The log is append-only and per-shard single-writer, so ordering needs no agreement. ⚠ But **writer election** does, and that is `ehdb-reference/src/election.rs` + `fencing.rs` — a lease, not consensus. A lease needs a time bound nobody can violate, which on a partition means **fencing**, not optimism. |
| **Linearizable reads** | **not offered, by choice** | A follower read is bounded-stale. `ehdb-core`'s plan layer already models `nearest` vs `bounded` and **refuses rather than silently serving from a non-owner** — the right shape. |
| **Watch** | ❌ absent; `read_partition_after` is the substrate | A poll loop over a cursor is a watch with worse latency and no server-side fan-out. Honest framing: **start with poll, name the latency**, do not call it a push API. |
| **Leases / TTL** | ⚠ heartbeat counter, not wall-clock | See §C. |
| **Membership** | `ehdb-gossip` + foca | ⚠ Chosen precisely because hand-rolling SWIM **fails silently** (`self-sufficiency.md`). |

⚠⚠ **The lower bound that cannot be designed away.** With a coordination-free data path you
get CALM-style availability, and you do **not** get linearizable cross-shard reads. Any
north-star text implying "etcd semantics, distributed better" is wrong. The accurate claim
is: **per-shard ordered, coordination-free, with explicitly-typed staleness at the read
boundary** — which is a *different and in places weaker* guarantee than etcd, traded for no
external service and no quorum to operate.

## B. Topology registry + discovery

D8 already registers and lists. Three things are missing to make it the *fleet* registry.

**B1 — Generic identity.** Today the key is `worker_id` and the payload a `contract`
string. A fleet registry needs `(kind, id)` where `kind ∈ {server, gateway, ehdb, worker,
playbook, execution, …}` so a NoETL server API, a gateway, an EHDB instance and a live
execution all register in one place and are discoverable by kind.

**B2 — Wall-clock lease, not a heartbeat counter.** `list_live_since(min_heartbeat)` makes
the **caller** decide what stale means, in units of a logical counter. A registry's liveness
should be `list_live_at(now, ttl)`: a record is live iff `now - last_seen < ttl`. Same
append-only mechanics, a decidable question.

**B3 — Watch.** `watch(kind, after_seq) -> changes` over `read_partition_after`. Poll-based
first, latency stated.

**Ephemeral runtime units** then fall out of B1+B2: a live execution registers as
`(execution, <id>)` with a short TTL and is discoverable exactly like a service. It
disappears by *not* being renewed — no tombstone, no reaper, which is the property that makes
TTL the right primitive for ephemera.

## C. Secret references

A **reference type**, never material. `secret_ref` is `(provider, path, version?)` — an
alias resolved at step-execution time by the keychain, exactly as
`agents/rules/execution-model.md` already requires. EHDB stores the pointer and **must
refuse a value that looks like material** (a non-scalar `auth:`, an inline mapping) the same
way `catalog-extract` already refuses to catalogue an inline credential mapping.

Scope: a type + a validation rule. Not a secret store.

## D. Vectors

⚠ Correcting myself: this is **not** a greenfield build. `cosine` exists in `ehdb-l0`,
`cosine_similarity` + cosine-ranked upsert/query in `ehdb-reference`, and `ehdb-retrieval`
has its own. What is missing is **integration**, not arithmetic:

- embeddings attached to **catalog objects** (an `embedding` attribute on a catalog resource,
  or a D-dataset keyed by `(resource_type, path)`);
- a query path that returns catalog objects ranked by similarity;
- an **honest recall measure** — cosine over a brute-force scan is exact and O(n); an ANN
  index is not, and "semantic search works" is unfalsifiable without a recall@k number
  against a known-answer set.

## Phasing — lowest risk first

| phase | work | risk | why here |
| :-- | :-- | :-- | :-- |
| **P1** | **wall-clock TTL liveness** on D8 (`list_live_at`) | **low** | additive, no format change, decidable, and it is what makes "service registry" true rather than "worker table" |
| **P2** | generic `(kind, id)` registration | low–med | a payload/key change on an append-only dataset; needs a compatibility story for existing `worker_id` records |
| **P3** | `watch(kind, after_seq)` poll API + stated latency | low | `read_partition_after` already exists |
| **P4** | ephemeral execution registration with TTL | low | falls out of P1+P2 |
| **P5** | `secret_ref` type + refuse-material validation | low | mirrors an existing catalog-extract rule |
| **P6** | vectors attached to catalog objects + **recall@k** measure | **med** | arithmetic exists; integration and an honest recall number do not |
| **P7** | **unsealed-tail replication** | **high** | the real durability gap; touches the write path and the crash window |
| **P8** | writer election + fencing on the production path | **high** | `election.rs`/`fencing.rs` are in `ehdb-reference`; a lease without fencing is not safe under partition |

**P7 and P8 are the hard parts** — not vectors, and not the registry. Both change durability
or safety semantics, so both want their own spec and prod sign-off.

## Acceptance

Every phase is held to `docs/measures/complete-and-measured.md`: invariants **enforced**,
costs **measured with a resolution control**, health **observable with pinned-at-zero series
proven to move**, regressions **loud**. A phase that ships without a measure is not done.

## Related

- `docs/measures/complete-and-measured.md` · `docs/measures/l0-benchmarks.md`
- `docs/spec/writer-election-and-fencing.md` · `docs/spec/lease-election-k8s-binding.md`
- `docs/spec/second-substrate.md` — why an independent failure domain needs an off-node store
- `agents/rules/self-sufficiency.md` in noetl/ai-meta — no external datastore; proven
  libraries welcome
