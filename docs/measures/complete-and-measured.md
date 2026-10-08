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
| B6 | **Memory** vs chain length and part count | ✅ exponent **0.52** over records — **O(parts), not O(records)**: ~16.6 KB fixed + ~1.8 KB/part, control = a planted 1 MiB leak checked FIRST |
| B7 | **Concurrent writers / multi-shard** — contention, and invariants under concurrency | ✅ single-writer by construction (throughput **falls** 10% from 1→8 threads); multi-shard union **set-equal**; reader interleaving carries a coverage control that fails at 0 mid-write reads |
| B8 | `ehdb-feed`'s mirror/drain path | ✅ **530x** batch-1 vs batch-1024; the obvious attribution was **refuted by its own control** (4 polls of 1024 ≈ 1 poll of 4000) |

⚠ **B7 found a caller trap rather than an engine defect.** An `AtomicU64` sequencer plus a
`Mutex` is **not** sufficient: minting the sequence *before* taking the append lock reorders
**34.2% of appends** while losing and duplicating exactly nothing. That is prod's
[ai-meta#362](https://github.com/noetl/ai-meta/issues/362) mechanism — ids minted before
insert, so commit order is not id order — reproduced in 20 ms. The first draft of that test
asserted the sequencer was sufficient, and the canary caught it.

⚠ **B8 found a tradeoff nobody had measured.** The batch cap that fixed
[ai-meta#298](https://github.com/noetl/ai-meta/issues/298)'s unbounded memory makes a drain
**quadratic (exponent 2.06)** for backlogs above the cap. Both the bound and the quadratic
are real; the tradeoff between them was never quantified. Separately, `poll_assign` **without**
acking is **2.86x slower than with**, because the redelivery scan walks the whole in-flight
map on every poll — doing less work costs more, and lazy acking is O(n²) in the consumer.

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
| C6 | Divergence and queue depth | `state_gauges.rs`, `inflight_exposition.rs` | ✅ **4 engine state gauges + 2 feed gauges**, pinned at 0 and each proven to move — see below |

### C6, as landed

Four **state gauges** on the engine — current depth, as against the 27 cumulative counters
— all computed from in-RAM state with no substrate I/O: `manifest_parts`,
`parts_local_only`, `parts_under_replicated`, `dedupe_window_records`. Plus
`ehdb_feed_shard_inflight` / `ehdb_feed_total_inflight` on the delivery side.

Two of these close holes rather than adding numbers:

- ⭐ **`parts_under_replicated`.** The uploader assigns `p.replicas = locations` — the
  *successful* writes only — so a part that landed 1 of 2 copies is recorded durable,
  `on_upload_done` fires, and the age-based durability window reports it **done**.
  `replica_writes` is cumulative and climbs while the deficit stands. Nothing reported a
  standing replication deficit before. Proven with a write-refusing substrate: 7 parts,
  `is_durable() == true`, each holding 1 of 2 copies.
- ⭐ **`inflight`.** `ShardConsumerGroup::inflight_len` existed all along and **no
  `render_*` emitted it** — the "recorder exists, nothing calls it" shape, where the
  accessor's existence makes the capability look present. It is not derivable from lag,
  which counts undelivered **plus** unacked: a lag of 10 with 0 in flight (stalled) and a
  lag of 10 with 4 in flight (busy) are **measured to be the same lag** and are opposite
  operational conditions.

⚠ `manifest_parts` is refreshed on manifest **mutations** and at open, not per append: a
per-append manifest walk would be O(parts) per append, which is precisely the quadratic
shape the gauge exists to detect. *Instrumenting a thing must not reproduce the defect it
measures.* `refresh_state_gauges()` is public so a scrape handler can force a refresh.

⚠ The pin at open is **unconditional, including all-zero**. A pin inside a config branch is
not a pin — server#315 pinned a reason set inside `if event_bus_mode.publishes_ehdb()` and
left it absent on exactly the configuration whose value someone would be reading.

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
| D5 | Benchmarks run in CI, with the result shape enforced and the numbers recorded | missing benchmark, or an inverted batch ordering beyond 25% noise | ✅ `bench` job runs them; `ci/bench_ratios.py` gates shape; estimates uploaded per run |
| D6 | Latency **thresholds** gated | deliberately **not** asserted — a latency bound on shared CI hardware is flaky, and a flaky measure gets deleted | n/a by choice |

### D5, as landed — and why it gates ratios, not times

CI previously ran `cargo bench --workspace --no-run`: it compiled every benchmark and
executed none. **Running them is most of the value on its own**, because each case asserts
inside its timed region that it got the records it claims — which is what caught
`ChangeFeed::poll` returning **2,000 of a 10,000** backlog. A short read and a fast drain
are otherwise the same number, and `--no-run` structurally cannot see the difference.

The guard gates **shape**, consistent with D6's refusal to gate absolute latency:

1. **The denominator first.** It prints `expected=13 parsed=13` and fails on any missing
   benchmark, because a parser that finds nothing asserts nothing and exits 0.
2. **Monotonicity.** A larger batch must never be slower, with 25% noise tolerance. The
   ratio is taken within one run on one runner, so machine speed cancels out — which is
   what makes it stable where a threshold is not.
3. **Reported, never gated:** the 530x batch effect, the 0.999x poll-count control, the 32x
   per-poll figure, the 2.86x lazy-ack penalty. Gating any of these would fail CI the day
   somebody *improves* the drain path. A guard against progress is the wrong guard.

Both negative controls were run before it was trusted: removing one `estimates.json` fails
naming it, and planting an inverted batch ordering fails naming the regression. A guard
that has never fired is indistinguishable from one that cannot.

⚠ **No baseline file is committed.** A checked-in set of absolute times from one machine is
a representation that nothing forces to stay true, and it would drift silently the first
time the runner image changed. The per-run estimates are uploaded as an artifact instead,
and the figures in `l0-benchmarks.md` are taken deliberately with the machine named.

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
