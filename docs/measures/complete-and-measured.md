# What "complete and measured" means for EHDB

A standard the engine can be held to, not a one-off report. Each criterion is **checkable**:
it names the command that checks it and what a failure looks like.

Status as of **2026-10-08**. ✅ met · ⚠ partial · ❌ not met.

## Why this is written as acceptance criteria

The 2026-10-08 audit expected stubs and found none — **0** `todo!`/`unimplemented!`/`TODO`
across 177 `.rs` files (positive-controlled: the same grep finds 979 `pub fn`), **1120**
tests, and every `stub` hit a deliberate negative control. EHDB was never *incomplete* in
the sense the word usually means. It was **unmeasured**: the engine on the production path
had 88 files and **zero** benchmarks while the reference model had two.

So "complete" here cannot mean "no TODOs". It has to mean *the invariants are enforced, the
costs are known, and a regression in either is loud*. That is what follows.

## A. Invariants are ENFORCED, not just documented

A documented invariant with no enforcement point is a comment. The audit's crude proxy —
mentions-on-an-assert-line — read: `divergence` 191 mentions / 27 enforcing, `sort_key`
185 / 13, `monotonic` 94 / 2, `ascending` 66 / 2, `determinism` 47 / **0**.

| # | criterion | check | status |
| :-- | :-- | :-- | :-- |
| A1 | An append that does not advance its shard tail is **refused or counted**, never silently accepted | the ascending-contract canary (`engine.rs:792`) + `metrics.rs:90` | ✅ |
| A2 | Ordering is **measured** over a realistic population, with a planted inversion proving the measure sees it | `cargo test -p ehdb-l0 --test invariant_measures` | ✅ 0 violations / 500 records; a reversed read flags every pair, one swap exactly one |
| A3 | Fold determinism is **measured**, and the fold is proven **order- and completeness-sensitive** | same | ✅ 1 distinct digest / 5 reads; changes on one swap *and* one dropped record |
| A4 | A cold reopen reproduces the log by **set equality**, with a one-element drop proven to fail it | same | ✅ 400 records |
| A5 | Fold determinism has an **enforcement point in the engine**, not only a test | — | ❌ 47 mentions, 0 on an assert/`Err` line |
| A6 | Single-root chain | — | ❌ **not an EHDB invariant at all** — `single root` has zero mentions in this repo; it lives in `noetl/server` |

## B. Costs are KNOWN, with a resolution control on every instrument

An instrument that cannot resolve the effect it names prints a clean number and a wrong
conclusion. **Three of three** append instruments were wrong before they were right, each
caught by its own planted control. So the control is a criterion, not a nicety.

| # | criterion | status |
| :-- | :-- | :-- |
| B1 | Every bench group plants a **known effect** the output must separate | ✅ 10x payload, 10x batch, 10x chain, 2% × 5 ms tail |
| B2 | Write path cost is decomposed into **engine vs fsync** | ✅ 267 rec/s posture A vs 5 450 group-committed @80 — fsync is ~95% |
| B3 | Read cost is characterised **against chain length**, with the non-linearity located | ✅ linear at ~55 ns/record to 1 024, then a **15.6x step** at `seal_max_records` |
| B4 | **Tail** latency (p50/p90/p99/max), with `n` printed on every line | ✅ append p99 7 550 µs vs p50 4 014 µs |
| B5 | **Lifecycle** (seal/merge/reclaim) cost measured against size, with the known prod failure mode as the control | ✅ manifest exponent **1.23** bounded vs **1.94** pre-fix |
| B6 | **Memory** vs chain length and part count | ❌ |
| B7 | **Concurrent writers / multi-shard** — contention, and invariants under concurrency | ❌ every figure is single-shard, single-threaded |
| B8 | `ehdb-feed`'s mirror/drain path | ❌ 36 files, 0 benches |

## C. Health is OBSERVABLE — a `0` means healthy, not inert

⚠⚠ **This column's first scoring was wrong, and the correction is instructive.** It said
"`metrics.rs` exports **7** public functions for an 88-file engine" and concluded the
counters were missing. That count matched `pub fn` only — **17 of the 20 incrementers are
`pub(crate) fn`**, and all **27** metric fields are `pub`. A follow-up pass then reported 4
fields as never written; also wrong, a line-oriented grep missing
`self.field\n    .fetch_add(..)`. Checked multi-line-aware: **0 of 27 are unwritten.**

The real gap was narrower and was not about missing counters at all: **27 good counters that
nothing could scrape.** `L0Metrics` was in-process only, and the workspace's single
Prometheus exposition lives in `ehdb-feed/src/scaler.rs` for consumer lag, not the engine.

| # | criterion | check | status |
| :-- | :-- | :-- | :-- |
| C1 | Counters for seal / merge / reclaim / read | `metrics.rs` | ✅ **27 fields, all written** — `seals`, `merges`, `parts_merged`, `orphans_reclaimed`, `parts_dropped`, `reads`, … |
| C2 | Those counters are **scrapable** | `render_prometheus` | ✅ Prometheus text v0.0.4, `dataset` label, plus a derived mean-lag gauge |
| C3 | Every series **pinned**, present at 0 on a fresh engine | `metrics_exposition.rs` | ✅ 27/27 emitted at 0; `build_info` pinned at 1 so an absent series can be told from an old binary |
| C4 | Each series **RED-proven to move** | same | ✅ `appends` 512, `seals` 32, `merges` 7, `parts_merged` 28, `reads` 5, `manifest_versions_retained` 26 — and the ascending canary **stays 0** |
| C5 | The exposition's **denominator is self-maintaining** | same | ✅ a snapshot field with no series fails the build-or-test; mutation-proven |
| C6 | Divergence and queue depth | — | ❌ still absent; `replica_domain_violations` is the nearest thing |

⚠ **Why pinning matters rather than using `prometheus::Registry`**: `Registry::gather`
**prunes metric families with no children**, so a labelled metric is *absent* until something
increments it — and an absent series and a healthy zero are indistinguishable to every
alert. Rendering from a plain snapshot has no label children to be empty, so the pin holds
by construction.

## D. A regression is LOUD

| # | criterion | what counts as a regression | status |
| :-- | :-- | :-- | :-- |
| D1 | The lifecycle measure fails if the manifest quadratic returns | bounded byte exponent **≥ 1.5**, or the pre-fix control exponent **≤ 1.6** (which would mean the measure stopped watching the mechanism) | ✅ asserted |
| D2 | The lifecycle measure fails if retention stops bounding the file count | bounded file exponent **≥ 0.3** | ✅ asserted |
| D3 | Read measures fail if a read stops returning what it claims | `got.len() == len` asserted **inside** the timed region | ✅ |
| D4 | Tail measures fail if percentiles stop being meaningful | non-monotonic percentiles, wrong `n`, or group commit not materially cheaper | ✅ |
| D5 | Benchmarks run in CI on a comparable machine, with a stored baseline | — | ❌ benches are not run by CI; numbers are machine-local and only a human comparison catches drift |
| D6 | Latency **thresholds** gated | deliberately **not** asserted — a latency bound on shared CI hardware is flaky, and a flaky measure gets deleted | n/a by choice |

## How to check all of it

```bash
cargo test  -p ehdb-l0 --test invariant_measures  -- --nocapture
cargo test  -p ehdb-l0 --test lifecycle_measures  -- --nocapture
cargo test  -p ehdb-l0 --test tail_latency        -- --nocapture
cargo bench -p ehdb-l0 --bench l0_engine
cargo clippy --workspace --all-targets -- -D warnings   # a real gate in this repo
```

## The shortest honest summary

**A nearly met · B met except memory/concurrency/feed · C met except divergence and queue
depth · D met except CI enforcement.**

The remaining gap is **B6–B8** (memory, concurrency/multi-shard, `ehdb-feed`), **C6**
(divergence + queue depth), **A5** (fold determinism has no engine-side enforcement point)
and **D5** (CI does not run the benches, so there is no stored baseline).

⚠ **A6 is not a gap — it is a category error I made.** Single-root is **not an EHDB
invariant**: `single root` has zero mentions in this repo. It belongs to `noetl/server`'s
chain work. Recorded so nobody looks for it here.

See [`l0-benchmarks.md`](l0-benchmarks.md) for the numbers and the methodology.
