# EHDB scope — four engines

**Status:** normative. Supersedes any five-engine framing in `README.md`,
`AGENTS.md`, or the roadmap.
**Tracks:** [noetl/ehdb#320](https://github.com/noetl/ehdb/issues/320),
under [#324](https://github.com/noetl/ehdb/issues/324).

## The core set

EHDB owns **four** engines:

| engine | role |
| :-- | :-- |
| **event log** | the source of truth; append-only, single-writer-per-shard, `global_sequence` total order |
| **projection** | every read model — including analytical views **and** vectorized retrieval |
| **KV** | keyed lookup |
| **object** | immutable parts in an object store |

**Two engines are removed from scope**, and this is a *removal*, not an
externalisation — no external engine is introduced in their place:

- the **OLAP query engine** (the ClickHouse-replacement path), and
- the **standalone ANN / vector engine** (the Qdrant-replacement path).

## Why they collapse onto projections

EHDB stores a fixed set of internal datasets with **predefined access paths**.
The analytical questions and the retrieval queries over that data are therefore
known in advance — which is exactly what a read model materialises.

**Analytical needs become projections.** There is no requirement for ad-hoc SQL
over internal core data, so there is no query engine to build: no planner, no
predicate pushdown, no cost model, no distributed executor. A new analytical view
is a new projection.

**Vector retrieval becomes a vectorized projection.** Embeddings are materialised
as read models and searched over **bounded, execution-scoped candidate sets**
(tenant / namespace / model / execution). Because retrieval is always scoped to a
small slice, **exact cosine is sufficient and no ANN index is needed.**

Both removals are the L0 invariant applied consistently: a general-purpose query
engine and an ANN engine are precisely the generality EHDB exists to reject.

## ⭐ The code already works this way

This is a scope correction, not a rewrite. What is built today already matches
the four-engine model:

- `ehdb-reference/src/vector.rs` is **"a bounded cosine-similarity search over
  the collection's live points"** using `cosine_similarity` — there is **no ANN
  index to remove**.
- `ehdb-retrieval`'s `VectorSearch` already "scopes candidates by tenant,
  namespace, and embedding model" — already the bounded, projection-scoped shape.
- `README.md` already records ANN indexes, Qdrant adapters, query planners and
  distributed query execution as **not built**.

The gap was in the *promise*, not the implementation: the README's opening and
Goals still committed to absorbing Qdrant and ClickHouse, and the roadmap still
carried a five-tier list with vector as an owned engine. **#320 stops deferring
those two and removes them.** "Future surface" and "out of scope" are different
commitments, and only one of them is true.

## The boundary, stated deliberately

EHDB commits to **bounded, projection-scoped retrieval and predefined analytical
views only**:

- ❌ no unbounded global similarity search across all history;
- ❌ no ad-hoc analytical query over internal core data.

A genuinely novel aggregate, or a cross-execution similarity search, is answered
by **adding a projection**, or by reading raw parts offline. It is never answered
by an ad-hoc engine.

⚠ This is a deliberate decision and should be read as one, not as a gap someone
forgot to fill. If a future requirement genuinely needs unbounded search, the
answer is to revisit *this document* — not to quietly add a planner.

## What stays

The columnar Arrow/Parquet **write** path stays where a non-analytical tier
(object/result, retention) needs it. It is never used to *serve* queries.

## Relationship to the tier and store models

⚠ Two different vocabularies are in play, and conflating them causes confusion:

- **Engine** (this document): the four capabilities EHDB owns.
- **StoreTier** (`noetl/worker`): the tiers with a **durable store behind the
  tier service** — currently `eventlog`, `projection`, `catalog`.

`catalog` is **not a fifth engine.** It is a projection: the catalog relation is
a fold of the catalog log, and it has its own store for the same reason the
projection tier does — an isolated file, not an isolated engine. A tier gains a
`StoreTier` variant when it gains a store; it does not thereby gain an engine.

`QueryTier` in the worker additionally names `kv`, `object` and `vector` as
shadow-driver read surfaces. Under this scope `vector` is a **vectorized
projection**, not an owned engine; it has no `StoreTier`, no durable store, and
is absent from `SERVE_WIRED_TIERS`.

## Cutover gate

No tier is promoted to primary-serve until:

1. [#321](https://github.com/noetl/ehdb/issues/321) — writer election + fencing — is **merged**;
2. [#322](https://github.com/noetl/ehdb/issues/322) — the D1 durability window — is **bounded and monitored**;
3. [#261](https://github.com/noetl/ehdb/issues/261) — load/perf — **passes at target scale** for these four engines.

See [#324](https://github.com/noetl/ehdb/issues/324). Related:
[#241](https://github.com/noetl/ehdb/issues/241) (completion program),
[#254](https://github.com/noetl/ehdb/issues/254) (durable event-log backend).
