# ehdb-signal-mesh — POC

A tiered **A2A / ReAct** agent mesh over an EHDB-shaped event log. Design doc:
[`docs/spec/a2a-react-signal-mesh.md`](../../docs/spec/a2a-react-signal-mesh.md).

⚠ **POC only.** Not wired into any production binary, not merged, flag-gated,
and it executes no generated code. Read §11 of the design doc ("what the POC
does NOT prove") before drawing conclusions from it — in particular there is no
durability, no real A2A transport, and no scale evidence.

## Run it

```bash
cargo run -p ehdb-signal-mesh --bin signal-mesh-demo
```

Deterministic: same output every run. No clock, no network, no model.

You should see, in order:

1. **Agent Cards published** — four agents across three tiers, each with a
   content digest, and the RFC 8615 well-known path.
2. **Collector appends** six device signals (two signal classes).
3. **The cascade** at a named watermark, tier by tier.
4. **The ReAct trace** — `observe` / `reason` / `act` for every agent, each one
   an event in the log.
5. **The verdict** — ONE number and ONE boolean.
6. **A replay check** — re-folding the same prefix reproduces the same context.

Expected verdict with the shipped fixture: `52.5000`, `true` (threshold `50.0`).

## Test it

```bash
cargo test -p ehdb-signal-mesh
```

9 tests, all deterministic.

## Prove the tests actually catch things

The suite is only worth the defects it fails on. Three defects on the core
aggregation/fold logic, each caught by exactly one test:

| Plant | Edit | Test that fails |
| :-- | :-- | :-- |
| D1 | `Reduction::WeightedMean` → plain mean of means | `weighted_mean_respects_population_not_child_count` |
| D2 | `population()` → `ctx.inputs.len()` | `population_is_summed_not_counted` |
| D3 | `if r.seq > up_to_seq` → `if false` | `the_bounded_read_excludes_everything_after_the_watermark` |

⚠ **D1 is the interesting one.** A weighted mean that drops its weights is
invisible whenever the weights are equal — which is exactly what a tidy fixture
makes them. The test uses populations of 1 and 99 on purpose, and its second
half asserts that an *equal*-weight fixture cannot tell the two implementations
apart. A test for this defect written the natural way would pass against the bug.

## Shape

| File | What |
| :-- | :-- |
| `src/event.rs` | Six mesh event kinds; `Unknown` fallback for rolling upgrades |
| `src/a2a.rs` | Agent Card + Task (all **8** `TaskState`s), canonical digest |
| `src/fold.rs` | The pure bounded-read fold + the three reductions |
| `src/react.rs` | The observe→reason→act loop; `Reasoner` trait |
| `src/mesh.rs` | Log, agents, and the per-tier cascade |
| `src/bin/demo.rs` | The runnable end-to-end story |

## Flags

Everything is off unless armed, and only the exact string `"true"` arms:

- `NOETL_SIGNAL_MESH` — master arm
- `NOETL_SIGNAL_MESH_REASONER` — opt into a model-backed reasoner (default:
  deterministic; **no test asserts on the model path**)
