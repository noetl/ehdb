# A tiered A2A/ReAct agent mesh over the EHDB event log

**Status:** design + POC · **merged to `main`** · nothing enabled, nothing deployed
**POC:** `crates/ehdb-signal-mesh/` (`cargo run -p ehdb-signal-mesh --bin signal-mesh-demo`)

📐 **Looking for the architecture overview?** This document is the
implementation/proof spec — grounding, file:line evidence, and the POC. The
team-facing blueprint is
[`docs/architecture/a2a-signal-mesh-blueprint.md`](../architecture/a2a-signal-mesh-blueprint.md):
diagram-forward, readable without opening any code.

## 1. The problem

Thousands of devices emit thousands of signals. A hierarchy of agents reduces
them: tier-0 agents reason locally over one device or signal class; higher tiers
aggregate; the top emits **one number and one boolean** — a numerical
definition-function of the accumulated reasoning. The cascade is event-driven: a
signal arriving at the bottom propagates upward.

The design question is not "can agents talk" — it is **what makes a
thousand-agent cascade reproducible, auditable, and safe to run twice.**

## 2. Grounding — VERIFIED vs ASSUMED

⚠ The brief carried several seeded facts. Some did not survive contact with the
tree, and saying so is load-bearing: a design that cites a branch which does not
exist cannot be checked by its reader.

### Verified in the tree (file:line)

| Claim | Evidence |
| :-- | :-- |
| Catalog register endpoint exists | `noetl/server` `src/main.rs:89` → `POST /api/catalog/register` → `handlers::catalog::register` |
| `kind: mcp` tool with a **runtime-chosen model** | `noetl/ops` `automation/agents/troubleshoot/diagnose_execution.yaml:301,306` (`model: "{{ resolve_triage_backend.model }}"`) |
| Gemma model pin | same file `:75` — `triage_model: "gemma3:4b"` |
| Runtime fan-out `loop.in` over a **step result** | `noetl/noetl` `tests/fixtures/playbooks/batch_execution/heavy_payload_pipeline_in_step/heavy_payload_pipeline_in_step.yaml:125` (`in: '{{ load_items_for_execution.command_0.rows }}'`), `:304` |
| `loop.spec.mode` + `iterator` | `test_simple_loop.yaml:33-37` |
| Child-playbook invocation | `noetl/ops` `automation/boot.yaml:15-16` (`kind: playbook`, `path: setup/bootstrap.yaml`) |
| `playbook` is a registered tool kind | `noetl/tools` `src/registry.rs:406` |
| SLM context event model — 7 payload kinds, `Unknown` fallback | **now on `main`**: `crates/ehdb-slm-context/src/event.rs:193-211` |
| Pure fold `fold(events, up_to_seq, version)`, `canonical_bytes()`, `FoldError{UnsortedInput,ForeignExecution,Malformed}` | **now on `main`**: `crates/ehdb-slm-context/src/fold.rs` |
| Multi-region primitives | **now on `main`** (v0.3.0): `crates/ehdb-core/src/plan.rs`, `crates/ehdb-l0/src/closed_timestamp.rs`, `membership.rs`, `placement.rs` |
| D1 event log / D6 vector datasets | `crates/ehdb-l0/src/dataset.rs` (`DATASET_D1_EVENT_LOG`), `src/vector.rs` (`DATASET_D6_VECTOR`) |

### ⚠ Corrections — including three of my own

**I got three of these wrong first, and the error is worth stating.** I searched
`noetl/ehdb` for *paths* named `design/slm-ehdb-context` and
`docs/multiregion-ehdb-plan`, found nothing, and wrote them up as "does not
exist". They are **ai-meta branch names**, not paths, and both carry substantial
specs. Searching one repo for a name that lives in another, then reporting the
absence as fact, is the same "wrong denominator" failure this programme keeps
producing — here applied to my own verification.

| Seeded claim | Finding |
| :-- | :-- |
| branch `design/slm-ehdb-context` | ✅ **EXISTS** — an **ai-meta** branch carrying `specs/active/2026-09-19-slm-ehdb-context/` (S0–S6, 1,473 lines). The *code* lives on `noetl/ehdb@feat/slm-context-s*`. |
| `docs/multiregion-ehdb-plan`, M0/M3 | ✅ **EXISTS** — an **ai-meta** branch carrying `specs/active/2026-09-18-multiregion-ehdb/` (M0–M8, 2,448 lines), incl. `M3-closed-timestamp.md`. |
| "gemma4" | ✅ **EXISTS** as a milestone — `S6-gemma4-serving.md`. ⚠ The *deployed pin* is `gemma3:4b` (`diagnose_execution.yaml:75`); the two are different things and the doc should not conflate them. |
| A2A "v0.3.0 / v1.0.1 May 2026" | ❌ Latest release is **1.0.0**. Version negotiation uses `Major.Minor`; patch is excluded. |
| Agent Card at `/.well-known/agent.json` | ❌ Renamed to **`/.well-known/agent-card.json`** in v0.3 (2025-07-30); registered as an RFC 8615 well-known URI in v1.0. |
| Task states "submitted/working/completed/failed/canceled" | ❌ **Eight** states. The five omit `input-required`, `auth-required` (both *interrupted*, NOT terminal) and `rejected` (terminal). Collapsing interrupted into failed is the most common A2A dispatcher bug. |
| SLM crate has "7 payload kinds" | ✅ **Correct** — 7 variants. ⚠ Its own module doc says "Six"; the doc is stale by one (`ContextSummarised`). |
| Runtime-**chosen child playbook** (templated `path:`) | ❌ **NOT VERIFIED.** `kind: playbook` + literal `path:` exists; no templated `path:` appears anywhere in the tree. Treat dynamic child-playbook selection as an open fork (§10). |

**M3 already defines the bounded read this design needs**, and the mesh should
adopt its vocabulary rather than invent a parallel one:

| M3 flag | Values | Default |
| :-- | :-- | :-- |
| `NOETL_EHDB_READ_CONSISTENCY` | `strong` \| `bounded` \| `exact` | `strong` |
| `NOETL_EHDB_MAX_STALENESS_MS` | integer ms | `0` |

The mesh's `up_to_seq` is the single-engine, sequence-valued form of the same
idea: a read that names its bound instead of asking for "latest".

**As merged, the POC wires to the real type** — `fold::admits` takes
`ehdb_core::plan::ReadConsistency` (`Strong` / `Bounded` / `Exact`) rather than
declaring a parallel vocabulary, so the mesh says `bounded` the way the rest of
the platform does.

⚠⚠ **The units are NOT the same, and the code says so rather than typechecking
its way past it.** `ReadConsistency::Bounded` carries `max_staleness_millis` —
wall-clock, because M3's closed timestamp is a time. The POC has **no clock**;
its staleness is a *sequence gap*. So `admits` takes an explicit
`seq_per_milli` exchange rate as an argument, and `Exact { at_millis }` is
**refused** (`FreshnessRefusal::ExactUnsupported`) rather than approximated. A
real implementation carries an HLC and calls
`ehdb_l0::closed_timestamp::admits` directly.

### Framework citations

- **ReAct** — Yao et al., *Synergizing Reasoning and Acting in Language Models*, arXiv:2210.03629 (ICLR 2023). Interleaves thought/action/observation.
- **HAMMR** — Castrejon et al., *HierArchical MultiModal React agents for generic VQA*, arXiv:2404.05465 (NeurIPS 2024 Workshop). A dispatcher ReAct agent **whose actions are themselves ReAct agents**; +16.3% over naive LLM+tools on their VQA suite. This is exactly the tier relationship below.
- **A2A** — <https://a2a-protocol.org/latest/specification/>, Linux Foundation.

## 3. Concept → primitive mapping

| Vision concept | Existing primitive | Why it fits |
| :-- | :-- | :-- |
| Signal | **D1 event** (`DATASET_D1_EVENT_LOG`) | Append-only, sequenced, replayable — a signal is already an event |
| Agent reasoning trace | D1 event (`mesh.agent.reasoned`) | Makes a *decision* replayable, not merely logged |
| Tier aggregate | D1 event (`mesh.aggregate.emitted`) | The tier above reads it as data, not as an RPC result |
| Per-agent working context | **Pure fold** over an event prefix | Same contract as `ehdb-slm-context::fold` |
| Cross-tier read | `up_to_seq` **bounded read** | Names a watermark instead of "latest" — see §6 |
| Agent discovery | **A2A Agent Card** ↔ noetl **catalog entry** | Reuses the SLM F2 "catalog as carrier" pattern (`/api/catalog/register`) |
| Inter-agent call | **A2A Task** | Stateful, multi-turn, 8 states |
| Knowledge base | **D6 vector tier** | ⚠ platform-context only (§8) |
| Collector | Sharded ingestion → D1 | Ties to `placement.rs` on `feat/mr-cluster-a` |
| Special-case handling | noetl playbook + SLM stepgen in **PROPOSE** mode | Validate and count; never execute (§8) |

## 4. The model

```
                      ┌─────────────────────────────┐
  TIER 2 (synthesis)  │  t2-fleet   ReAct loop      │ ──▶ ONE number + ONE boolean
                      │  Reduction::Max             │     mesh.verdict.synthesised
                      └──────────────▲──────────────┘
                         A2A Task    │ bounded read @ W2
                      ┌──────────────┴──────────────┐
  TIER 1 (aggregate)  │  t1-site    ReAct loop      │ ──▶ mesh.aggregate.emitted
                      │  Reduction::WeightedMean    │     (value, population, up_to_seq)
                      └──────▲───────────────▲──────┘
                 A2A Task    │               │  bounded read @ W1
                      ┌──────┴─────┐  ┌──────┴─────┐
  TIER 0 (raw)        │  t0-temp   │  │  t0-vibe   │ ──▶ mesh.aggregate.emitted
                      │ ReAct loop │  │ ReAct loop │
                      └──────▲─────┘  └──────▲─────┘
                             │  bounded read @ W0
                      ┌──────┴───────────────┴──────┐
  COLLECTORS          │  shard by device/region     │ ──▶ mesh.signal.observed
                      └──────▲───────────────▲──────┘
                        device signals (thousands × thousands)

  EVERYTHING above is an append to the SAME D1 log.  Replay = re-fold.
```

**Each tier is a ReAct loop** (observe → reason → act). **The tier relationship
is HAMMR**: a tier-N agent's `Act` is "send an A2A Task to a tier-(N-1) agent",
and that sub-agent runs its own loop. Tiers reduce; they do not merely forward.

## 5. Event kinds (additive)

Six new kinds alongside the existing `slm.*`, same envelope rules:

| Kind | Carries |
| :-- | :-- |
| `mesh.signal.observed` | device_id, signal_class, value, device_seq |
| `mesh.agent.reasoned` | agent_id, tier, phase, thought, observed_seqs |
| `mesh.aggregate.emitted` | agent_id, tier, value, **input_count**, **up_to_seq** |
| `mesh.verdict.synthesised` | value, decision, threshold, up_to_seq |
| `mesh.task.transitioned` | task_id, from/to agent, A2A state |
| `mesh.agent.card_published` | agent_id, tier, card_digest |

⚠ **No `deny_unknown_fields`, and an `Unknown` fallback arm** — copied from the
SLM model deliberately. A tier mesh upgrades tier-by-tier, so a lower tier
*will* emit kinds an upper tier does not know yet. Failing the fold there turns
a rolling upgrade into an outage.

## 6. The bounded-read cascade — and the trap the POC found

A tier never reads "the latest" from below. It reads **as of a watermark it
names**, exactly as `fold(events, up_to_seq, version)` does on the SLM branch.
Re-running a tier at the same watermark yields the same aggregate whatever has
been appended since. That is what makes the cascade reproducible.

⚠⚠ **The first POC implementation fixed ONE watermark for the whole cascade, and
it was wrong in an instructive way.** Tier 0 emits its aggregates at sequences
*above* the entry watermark, so every tier above read a prefix that structurally
could not contain them and reduced **zero inputs to 0.0** — a confident,
well-formed, entirely wrong answer, with no error anywhere.

The fix: the watermark **advances per tier**. Tier N reads at the log head as of
the moment tier N-1 finished, and that per-tier watermark is recorded on every
aggregate. Determinism survives because the watermark is still *named* rather
than "latest". Within a tier all agents share one watermark, so siblings cannot
see each other's output and the tier is order-independent.

**Staleness is reported, never hidden.** `TierContext::staleness()` is the gap
between the requested watermark and what was actually folded. An agent that
cannot distinguish "I read everything" from "I read what existed" will publish a
confident aggregate over a partial prefix.

## 7. Scaling to thousands × thousands

- **Fan-out is the existing DSL primitive.** `loop.in` over a runtime step result
  (verified at `heavy_payload_pipeline_in_step.yaml:125`) is how a tier
  materialises N children without N being known at authoring time.
- **Partitioning** reuses `placement.rs` / `membership.rs` on `feat/mr-cluster-a`.
  Collectors shard by device/region; a tier-0 agent is pinned to the shard that
  owns its devices, so its fold is engine-local.
- **The fold is O(prefix)**, so a tier-0 agent must fold only *its own* slice.
  This is the main scaling constraint: a per-agent stream (or an indexed prefix)
  is required, because folding the whole log per agent is quadratic in agents.
  ⚠ The POC deliberately folds one shared log and therefore does **not**
  demonstrate this (§11).
- **Aggregates are small.** A tier passes `(value, input_count, up_to_seq)`, not
  its inputs, so upward bandwidth is O(agents) not O(signals).
- **`input_count` must be the summed population, not the child count.** Passing
  the child count silently converts a weighted mean into a plain one, one tier
  up, with no visible symptom.

## 8. Safety gates

| Gate | Rule |
| :-- | :-- |
| Knowledge base | D6 vector tier is **platform-context only** (the #197/#252 bound). User documents are NOT wired into `rag::ingest`. |
| Generated steps | SLM stepgen runs in **PROPOSE** mode — validate and count. **No generated code executes**, in the POC or the design. |
| Postgres | Untouched. The mesh appends to D1 only. |
| Arming | Everything behind `NOETL_SIGNAL_MESH`, default off. |
| Cards | An Agent Card with no `securitySchemes` is treated as not publishable outside the cluster. |
| Determinism | The default reasoner is arithmetic. A model-backed reasoner is flag-gated and **no test asserts on it**. |

## 9. Flag matrix

| Flag | Default | Effect | Revert |
| :-- | :-- | :-- | :-- |
| `NOETL_SIGNAL_MESH` | *(unset)* = off | Master arm | unset |
| `NOETL_SIGNAL_MESH_REASONER` | *(unset)* = deterministic | `ollama` selects a model-backed reasoner | unset |
| `NOETL_SIGNAL_MESH_MAX_TIER` | `3` | Caps cascade depth | lower it |
| `NOETL_EHDB_READ_CONSISTENCY` / `NOETL_EHDB_MAX_STALENESS_MS` | `strong` / `0` | **M3's flags, reused.** The POC consumes the `ReadConsistency` *type*; it does not read these env vars itself. ⚠ No mesh-private staleness knob — a second name for one concept is a second thing to keep true. | n/a (POC reads neither) |

Only the exact string `"true"` arms — the house convention (`seal_max_age`,
fencing, the repair sweep).

## 10. Open forks (recommended defaults)

1. **Dynamic child-playbook selection.** A templated `path:` is *not verified*
   anywhere in the tree. **Default: don't rely on it.** Select the child by
   `loop.in` over a runtime list plus a static dispatch step, which is verified.
2. **Per-agent stream vs one shared log.** **Default: per-agent stream**, so a
   fold is O(own prefix). One shared log is simpler and does not scale (§7).
3. **A2A transport.** JSON-RPC / gRPC / HTTP+JSON are all spec bindings.
   **Default: HTTP+JSON** to match the existing relay style.
4. **Card registry.** **Default: the noetl catalog** (`/api/catalog/register`),
   reusing the F2 carrier pattern, with `/.well-known/agent-card.json` served
   as a projection of it.
5. **Threshold ownership.** **Default: the top tier owns it** and records it on
   the verdict, so a replay can tell a changed threshold from a changed input.
6. **Back-pressure.** Unresolved. Thousands of devices can outrun a tier.
   **Default: bound the collector, not the agents** — a dropped signal must be
   visible as a gap, never as a silently smaller denominator.

## 11. What the POC does NOT prove

Stated plainly, because a demo that runs is easy to over-read.

1. **No durability.** The log is an in-memory `Vec`. Nothing about D1's fsync,
   sealing, replication or recovery is exercised.
2. **No real A2A transport.** Agent Cards and Tasks are the **data model only** —
   no HTTP, no JSON-RPC, no `/.well-known/` serving, no auth. Interop with a
   third-party A2A agent is unproven.
3. **No scale evidence.** 6 devices, 4 agents, one shared log folded per agent.
   The O(prefix) problem in §7 is described, not solved or measured.
4. **No model quality claim.** The default reasoner is arithmetic. Nothing here
   says an LLM would reduce these signals well.
5. **No concurrency.** The cascade is single-threaded and sequential. Ordering,
   contention and partial failure between tiers are untested.
6. **No EHDB integration.** It does not call `ehdb-l0`. It *mirrors* the SLM
   event/fold contract; it does not reuse the crate.
7. **No playbook execution.** The noetl DSL mapping in §3 and §7 is a design
   claim grounded in cited fixtures, not something the POC runs.
8. **No back-pressure or failure injection.** Every append succeeds.
9. **The freshness wiring is a type reuse, not a time model.** `admits` speaks
   `ReadConsistency`, but converts a millisecond budget to a sequence gap via a
   caller-supplied rate. Nothing here validates that rate, and `Exact` is
   refused outright. Do not read this as "the mesh implements M3".
10. **Merging changed nothing at runtime.** The crate is additive and off by
    default; no existing crate imports it, and no binary constructs a `Mesh`.

## 12. Staged plan

| Stage | What | Risk |
| :-- | :-- | :-- |
| 0 | This design + POC | none — nothing wired |
| 1 | Event kinds into a real D1 stream behind the flag, shadow | low, additive |
| 2 | Per-agent streams + the fold on `ehdb-l0` | medium — the §7 scaling fork |
| 3 | Agent Cards into the catalog; `/.well-known/agent-card.json` projection | low |
| 4 | Real A2A transport (HTTP+JSON) between two tiers | medium |
| 5 | Model-backed reasoner behind the flag, PROPOSE-only | gated |
| 6 | Collector sharding on `placement.rs` | the scale step |

## 13. Related

- `docs/spec/durability-window.md`, `writer-election-and-fencing.md` (this repo)
- ai-meta `docs/rfc/domain-slm-platform.md`, `ehdb-layered-platform.md`,
  `decoupled-context-event-chain.md`
- Branches (cited, **not merged**): `feat/slm-context-s1-s2-events-fold`,
  `feat/slm-context-s0-frame-invariants`, `feat/mr-cluster-a`, `feat/mr-cluster-b`
