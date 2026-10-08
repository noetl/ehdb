# Measuring EHDB's L0 engine

What is measured, how, and the numbers as of **2026-10-08**. Re-measure rather than quoting
this file — it carries an as-of date because a number without one is not evidence.

```bash
cargo bench -p ehdb-l0 --bench l0_engine       # performance, release
cargo test  -p ehdb-l0 --test invariant_measures -- --nocapture   # invariants, with controls
```

Hardware for the numbers below: Apple Silicon (arm64), local APFS, `--release`. **Absolute
figures are machine-specific; the ratios and the shapes are the result.**

## Why this exists

Measured before any of it was written:

| crate | `.rs` files | bench files |
| :-- | --: | --: |
| **`ehdb-l0`** — the production engine | **88** | **0** |
| `ehdb-feed` | 36 | 0 |
| `ehdb-reference` — the *reference* model | 22 | 2 |
| `ehdb-service` / `-transaction` / `-catalog` / `-storage` | 3 / 3 / 2 / 2 | 1 each |

**82% of the code had no benchmark, and the engine on the production path had none at all
while the reference implementation had two.** The existing numbers were real and about the
wrong thing — the same shape as measuring a tier that is not on the read path.

## The rule: every instrument carries a resolution control

A benchmark that cannot resolve the effect it is looking for prints a clean number and a
wrong conclusion. Each group plants a **known** effect — a 10x payload, a 10x batch, a 10x
chain — and the output must separate them. **If the planted case does not differ, the
measurement is not evidence, whatever the headline says.**

This is not theoretical. **Three of three** append-side instruments were wrong before they
were right:

| # | instrument defect | how it was caught |
| :-- | :-- | :-- |
| 1 | `iter_batched` rebuilt the engine + a temp dir per batch, so **setup dominated**: 8.3 ms/append | its own control FAILED — a **10x payload came out *faster*** (7.79 vs 8.31 ms) with overlapping CIs |
| 2 | `iter_custom` fixed it; 267 appends/s still looked implausible | the engine's own docs say `fsync-per-append, posture A` and cite "a ~4 ms `fsync`" — **267/s is real**, and the measurement independently confirms the documented figure |
| 3 | group commit read **flat** (192 → 184/s over a 10x batch) | I had left `FlushPolicy::EveryAppend` on, so `take_sync_handles` added a **second** fsync. **The instrument was wrong, not the engine** — I nearly published "EHDB's batching does not amortise" |

## Performance

### Write path — the fsync decomposition

| path | records/s | µs/record |
| :-- | --: | --: |
| posture A (`EveryAppend`, fsync per append) | **267** | 3749 |
| group commit (`CallerDriven`), batch 8 | **1 010** | 990 |
| group commit (`CallerDriven`), batch 80 | **5 450** | 184 |

**Group commit at batch 80 is 20.4x posture A, so the `fsync` is ~95% of posture-A
per-record cost.** That is the number that decides whether a workload needs batching.

⚠ Group commit requires `engine.set_flush_policy(FlushPolicy::CallerDriven)`. `FeedWriter`
does this; a bare `L0Engine` caller must too, and without it batching costs *more*.

Control: payload 64 B → 640 B must cost visibly more. ✅ 3.749 ms → 4.045 ms, **CIs do not
overlap**.

### Read path — and a 15.6x step at the seal boundary

`read_index_after(key, 0)` is the fold's input, so its cost **is** the fold's input cost.

| records on one index key | time | ns/record |
| --: | --: | --: |
| 10 | 711 ns | 71.2 |
| 100 | 5.52 µs | 55.2 |
| 900 | 49.1 µs | **54.6** |
| 1 100 | 933.8 µs | **848.9** |
| 2 200 | 1.888 ms | 858.3 |
| 4 400 | 3.760 ms | 854.6 |

⚠⚠ **A 22% increase in length (900 → 1100) costs 19x.** The knee is exactly
`DEFAULT_SEAL_MAX_RECORDS = 1024`: below it a key's records are in the **in-memory active
part**; above it they are in a **sealed part on disk**. Per-record cost steps **15.6x**
(54.6 → ~850 ns) and is then **flat again** — so this is a one-time step, **not** quadratic
growth.

**Consequence for the chain work:** an execution whose chain exceeds 1024 events reads at
~850 ns/record instead of ~55. A 5 000-event chain costs ~4 ms to fold its input, against
the ~0.3 ms a naive linear extrapolation from the sub-1024 figures would predict — a **13x**
underestimate. Extrapolating across the boundary is the mistake to avoid.

Control: lengths span 10x twice, cost must grow monotonically, and the read asserts
`got.len() == len` **inside** the timed closure — *a read that returns nothing is fast and
worthless, and that is how a benchmark reports a great number for a broken path.*

### Partition scan (the follower / catch-up path)

| records | time | ns/record |
| --: | --: | --: |
| 100 | 5.27 µs | 52.7 |
| 1 000 | 49.1 µs | 49.1 |

## Invariant measures

`crates/ehdb-l0/tests/invariant_measures.rs`. These are not pass/fail tests — they compute a
number over a realistic population, assert it, **and plant a defect to prove the measure can
see it**. Each prints `population=N`, because *zero violations over zero records is what a
broken measure looks like*.

| measure | healthy result | planted control |
| :-- | :-- | :-- |
| **ordering** — descending adjacent pairs in every index read | 0 over **500** records, `compared == population` | a fully reversed read must report **every** adjacent pair; a single swap must report **exactly 1** |
| **fold determinism** — 5 repeated reads of an unchanged log | **1** distinct digest over 300 records | the fold must change on **one swap** and on **one dropped record**, or determinism over it is vacuous |
| **reopen parity** — cold reopen reproduces the set | **set equality**, not counts, over 400 records | removing **one** element must fail the comparison |

⚠ A timing measure was written here first and **removed**: `cargo test` builds **debug**, and
a timing assertion in a debug build is noise wearing a measure's clothes. Debug read cost
was 2049 / 287 / 2256 ns per record at 50 / 500 / 2000 — a 10x length step cost only 1.4x,
because a ~100 µs fixed floor dominates. Its resolution control failed, correctly. The same
measurement resolves cleanly in `--release`, so it lives in the benchmark.

## What is NOT measured yet

Named so the gap is visible rather than implied:

- **p99** — criterion reports mean/median with CIs, not tail percentiles. Tail latency needs
  its own histogram harness.
- **Memory** — no allocation or RSS measurement exists.
- **Seal / merge / reclaim cost** — `tick()`'s three lifecycle calls are unmeasured, and the
  merge path is the one that showed quadratic manifest growth in prod
  (noetl/ai-meta 2026-09-01).
- **Multi-shard and concurrent writers** — every figure here is single-shard, single-threaded.
- **`ehdb-feed`** — 36 files, still 0 benches.

## Related

- `crates/ehdb-l0/benches/l0_engine.rs` — the benchmarks.
- `crates/ehdb-l0/tests/invariant_measures.rs` — the invariant measures.
- `agents/rules/representation-drift.md` in noetl/ai-meta — "print the denominator", and
  "volume is not duration".
