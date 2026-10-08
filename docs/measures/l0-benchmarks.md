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

## Lifecycle: seal / merge / reclaim, and the manifest quadratic

`crates/ehdb-l0/tests/lifecycle_measures.rs`. Bytes on disk, not timing — deterministic, so
it is a `cargo test`.

**The question.** On 2026-09-01 the manifest path reached **6,770 snapshots / 19.4 GB behind
71.8 MB of real data**, filled the prod volume, and made every append fail: snapshot *size*
grows with part count while snapshot *count* grew with write count. `manifest_retain`
(noetl/ehdb#344) was the fix. **Is the quadratic actually gone, or still latent?**

**The control is a supported configuration.** `manifest_retain = 0` disables pruning — it
*is* the pre-fix behaviour by its own doc comment. So the planted defect is not a mutation;
it is a config the engine still accepts, which makes it the strongest available instrument
check. If bounded and unbounded measured the same, nothing else here would be evidence.

| appends | `retain=32` bytes | files | `retain=0` bytes | files |
| --: | --: | --: | --: | --: |
| 1 024 | 427 636 | 35 | 734 326 | 79 |
| 2 048 | 1 076 282 | 39 | 2 772 298 | 159 |
| 4 096 | 2 347 994 | 39 | 10 827 728 | 320 |

Reported as a **growth exponent** — `bytes ~ appends^e`, so `e = ln(ratio)/ln(append_ratio)`.
A raw ratio cannot be compared across span sizes; an exponent can, and that is what makes
this a measure rather than an observation. `e = 1` linear, `e = 2` the 2026-09-01 quadratic:

| | bytes | files |
| :-- | --: | --: |
| `retain=32` (default) | **1.23** | **0.08** |
| `retain=0` (pre-fix) | **1.94** | **1.01** |

**Verdict: the quadratic is fixed, and the measure proves it by still seeing it.** The
pre-fix config reproduces it at **e = 1.94**; retention brings byte growth to **1.23** and
holds the file **count flat** at ~39 (e = 0.08) across 4x the appends.

⚠ **Residual, asserted so it cannot drift unnoticed.** Bounded bytes still grow *faster than
linear* (e = 1.23), because retention bounds how MANY snapshots exist, not how BIG one is —
each retained snapshot legitimately lists more parts as the log grows. Not pathological, not
free, and calling it "linear" would be wrong. The test asserts `e > 1.0` too: if that ever
measures sub-linear, the snapshot has stopped listing every part and this measure's premise
changed.

⚠ **The three lifecycle calls are caller-owned.** `seal_aged_parts`, `run_pending_merges`
and `reclaim_orphans` run only when a caller drives them. Measured: driving them each seal
over 1 024 appends performed **15 merges**; a single call at the end performed **8**. A
measure that never drove them would report 0 and read as "no merges needed" rather than
"nobody asked" — the configured-but-unreachable shape.

## Tail latency

`crates/ehdb-l0/tests/tail_latency.rs`. Criterion reports mean/median with CIs and **no
percentiles**, so it cannot answer "how bad is the slow one". Explicit samples, sorted,
nearest-rank percentiles — no bucketing, because bucketing loses the max and the max is the
interesting one.

**400 samples each, debug build** (`cargo test`). ⚠ For these I/O-bound paths debug tracks
release closely — append p50 4 014 µs here against 3 749 µs in the release benchmark, read
at 900 records 66 µs against 49 µs — because the cost is `fsync` and page cache, not codegen.
Do not assume that for a CPU-bound path.

| path | n | p50 | p90 | p99 | max |
| :-- | --: | --: | --: | --: | --: |
| append, posture A (fsync/append) | 400 | 4 014 µs | 4 815 µs | **7 550 µs** | **11 706 µs** |
| append, group-committed (no sync) | 400 | **21.4 µs** | 50.4 µs | 75.2 µs | 121 µs |
| `read_index_after`, len 900 | 400 | 66.0 µs | 110 µs | 248 µs | 928 µs |
| `read_index_after`, len 1 100 | 400 | **1 910 µs** | 2 013 µs | 2 364 µs | 8 409 µs |

Three things the mean hides:

- **The fsync tail is real**: p99 is **1.9x** p50 and max is **2.9x** p50. A system quoted at
  "3.7 ms per append" occasionally takes 11.7 ms.
- **Group commit is 187x cheaper at p50**, far more than the 20.4x the throughput figure
  shows — because the throughput number includes the batch sync, while the append itself
  skips the fsync entirely. Both are true; they answer different questions.
- **The seal boundary is sharper at p50 than in the mean**: 900 → 1 100 records is **29x** at
  p50 here, against 19x in the criterion mean.

Every line prints `n`. **A percentile over an unstated sample count is not a percentile** — a
p99 over 50 samples is the single worst of 50, a max wearing a percentile's name.

Resolution control: 2% of samples planted at 5 ms among 1 µs samples. p50 must **not** move
(1.0 µs) and p99 **must** (5 000 µs). Without it, a p99 equal to p50 could mean "no tail" or
"the percentile code is broken", and those are not the same.

⚠ This file asserts **no latency bound**. It asserts monotonic percentiles, the stated sample
count, that group commit is materially cheaper, and the planted control. A latency threshold
would make it flaky on shared CI hardware, which is how a real measure gets deleted.

## Memory — O(parts), not O(records)

`tests/memory_measures.rs`, via a counting `#[global_allocator]`. The question: does an
engine that has been up for a week still hold RAM proportional to every record it ever saw?

**It does not.** Growth exponent of live bytes over record count is **0.52** — sublinear.

| records | parts | live bytes | B/record | B/part |
| --: | --: | --: | --: | --: |
| 500 | 1 | 17,319 | 34.6 | 17,319 |
| 2,000 | 7 | 29,484 | 14.7 | 4,212 |
| 8,000 | 31 | 73,482 | 9.2 | 2,370 |

Two-point fit over **parts**, which divides out the fixed cost of opening an engine:

> **~16.6 KB fixed + ~1.8 KB per part.**
> 1 M records at `seal_max_records=1024` is ~976 parts, so **~1.7 MiB held**.

The bound that matters is therefore **part count**, and merge is what governs it — which
ties this figure to the Lifecycle section rather than making it an independent reading.

Sealing is confirmed to be the releasing mechanism, not assumed: the same 8,000 records with
sealing disabled hold **331,604 B vs 73,482 B**, a **4.5x** difference. Had those been
equal, the bound above would be real but attributed to the wrong mechanism.

**The control runs first**, deliberately: leak a known 1 MiB and require the counter to see
it. A counting allocator that is not actually installed reports a steady, confident **0
bytes of growth** — which is also the best possible result. Measuring first and controlling
second would let a blind instrument read as a perfect one.

⚠ **This file contains exactly one `#[test]` and must keep containing one.** The counter is
process-global and `cargo test` runs test functions in parallel threads, so two measuring
tests would each attribute the other's allocations to itself.

⚠ **A denominator bug caught in this file's own first run:** the part count was initially
taken by walking the directory tree for filenames containing the substring `part`. It
reported **1 part for 16,000 records at `seal_max_records=256`**, because what it matched was
the `parts` *directory*. Every per-part number was wrong by ~60x and none of them looked
wrong. `manifest_snapshot()` publishes the count; the fix was to ask the engine rather than
infer from the filesystem.

## Concurrency and multi-shard

`tests/concurrency_measures.rs`. Before it, **no test in `ehdb-l0` had ever spawned a
thread** — every concurrency property the engine has was un-measured.

### The first finding is in the signature

`append_record(&mut self)`. A single `L0Engine` is a **single writer by construction**;
concurrency is a property of whatever wraps it. Measured, behind one `Mutex`:

| threads | appends/s | out_of_order |
| --: | --: | --: |
| 1 | 294 | 0 |
| 2 | 271 | 0 |
| 4 | 264 | 0 |
| 8 | 263 | 0 |

Throughput **decreases** ~10% from 1 to 8 threads: the write path is serialised, so threads
buy nothing and cost lock handoff. The absolute figure is dominated by the substrate's
per-append durability (see the fsync decomposition), not by the mutex.

### The second finding reproduces prod's #362 in a unit test

An `AtomicU64` sequencer plus a `Mutex` **is not sufficient**, and the first draft of this
file asserted that it was — the canary fired 3 times with 2 writers and 32 times across 8
shards. The window is:

```text
let s = seq.fetch_add(1);     // A gets 5, B gets 6
let mut g = engine.lock();    // B wins the race
g.append_record(..s..);       // 6 is appended before 5
```

**The sequence must be minted while holding the append lock.** Minted outside it, allocation
order and append order are independent, and the log is no longer ascending in its own sort
key. Measured:

| pattern | out_of_order | lost | duplicated |
| :-- | --: | --: | --: |
| mint under the lock | **0** of 1,600 | 0 | 0 |
| mint before the lock | **547** of 1,600 (34.2%) | 0 | 0 |
| per-thread sequence ranges | 473 of 1,200 (39.4%) | 0 | 0 |

**Nothing is lost and nothing is duplicated in any row.** The ids stay perfectly unique and
perfectly dense; only their order is wrong. That is exactly why the defect is hard to see in
production — and it is the same mechanism as
[ai-meta#362](https://github.com/noetl/ai-meta/issues/362), where snowflake ids minted
before insert made commit order diverge from id order, so an id-ordered read stopped being
an append-only prefix. It reopened #360 after a green re-ramp; here it reproduces in 20 ms.

Replay stays strictly ascending in every row — the engine sorts on read. The damage is to
the **tail canary**, not to read order, which is why `out_of_order_appends` is the only
signal that sees it.

### Readers concurrent with a writer

3 readers against 1 writer over 400 appends: **142,945 reads, 142,935 strictly mid-write, 0
invariant violations** — no reader ever observed a non-ascending or duplicated view.

**The coverage control is the load-bearing part.** A clean result with zero mid-write reads
means the writer finished before any reader looked, which reads identically to "concurrent
reads are safe". The test counts mid-write reads and **fails at zero** — the same
"coverage was ~0 by construction" shape as
[ai-meta#307](https://github.com/noetl/ai-meta/issues/307).

### Multi-shard under contention

8 shards, 4 concurrent writers, keys deliberately shared across all threads so every shard
is written by every thread: per-shard `[152, 244, 276, 176, 224, 76, 220, 232]`, and the
union over shards is **set-equal** to what was appended — not merely equal in count. Each
record also re-hashes to the shard it was read from. `out_of_order_appends == 0`.

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

## The feed drain path — and what an unbatched relay costs

`crates/ehdb-feed/benches/drain.rs`. `ehdb-feed` is 36 files and had **0 benchmarks**: the
networked publish path has an attribution harness run by hand
(`examples/dispatch_bench.rs`), but the *drain* primitives every consumer runs in a hot
loop had nothing measured at all.

### The batch axis, which has prod history

[ai-meta#344](https://github.com/noetl/ai-meta/issues/344) was **100% transport timeouts**
because the relay never batched — **0 of 80,264** fan-outs used a batch. The fix was real
and the cost it avoided was never quantified. Draining a 4,000-record backlog:

| batch limit | total drain | per-record throughput |
| --: | --: | --: |
| 1 | 1.353 s | 2.95 K/s |
| 16 | 84.8 ms | 47.1 K/s |
| 256 | 6.47 ms | 617 K/s |
| 1024 | 2.55 ms | 1.56 M/s |
| 4000 *(one poll)* | 2.55 ms | 1.57 M/s |

> **530x** between a batch of 1 and a batch of 1024. That number is what not batching
> costs.

### ⚠ The attribution control refuted the obvious explanation

The reading that suggests itself is "each poll re-scans the backlog, so the cost is poll
count × backlog". **The control refutes it:** 4 polls of 1,024 (2.551 ms) and **1** poll of
4,000 (2.554 ms) are the same to within 0.1%. Poll count does not dominate once the batch
is large.

So a second probe holds the limit at 1 and varies the backlog, reporting cost **per poll**:

| backlog | polls | total | **per poll** |
| --: | --: | --: | --: |
| 200 | 200 | 1.45 ms | **7.3 µs** |
| 2,000 | 2,000 | 465.7 ms | **232.8 µs** |

Per-poll cost rises **32x for a 10x backlog** — so it is not a fixed overhead either.

Three measured facts, then: per-poll cost is **not** fixed; poll count does **not** dominate
at part-sized batches; and total drain cost goes as **~n^2.5** at limit 1 while staying
linear at limit ≥ 256. Together they say the cost is governed by **how far the cursor
advances per poll** — a batch smaller than the span a poll must touch re-reads that span
once per record. The precise mechanism in the read path is **not yet attributed**, and is
tracked rather than asserted here.

### Drain cost vs backlog, and a quadratic the #298 fix introduced

Draining the **whole** backlog, repeated polls, default batch limit:

| backlog | total | throughput |
| --: | --: | --: |
| 100 | 7.45 µs | 13.4 M/s |
| 1,000 | 69.3 µs | 14.4 M/s |
| 10,000 | 7.96 ms | 1.26 M/s |

**Throughput collapses 11x between 1,000 and 10,000** — growth exponent **2.06**, i.e.
quadratic. The transition is at the batch cap: `ChangeFeed::poll` uses
`default_batch_limit()` = **2,000** (`EHDB_FEED_BATCH_LIMIT`), bounded deliberately because
unbounded is what caused [ai-meta#298](https://github.com/noetl/ai-meta/issues/298). Below
the cap a drain is one poll and linear; above it, the drain is quadratic.

Both the bound and the quadratic are real. **The tradeoff between them had never been
measured** — which is the finding.

⚠ **An in-region assertion is the only reason the cap surfaced at all.** The first version
of the backlog sweep called `poll` once and asserted it got the whole backlog; at 10,000 it
got **2,000**. Without the check it would have reported a comfortable time for draining
**20% of the backlog**: a short read and a fast drain are the same number.

### ⭐ Lazy acking is quadratic in the consumer

| loop | 2,000 records | throughput |
| :-- | --: | --: |
| `poll_assign` + `ack` | 2.08 ms | 960 K/s |
| `poll_assign` only (never ack) | 5.93 ms | 337 K/s |

**Doing less work is 2.86x slower.** The mechanism is visible in the source:

```rust
let expired = self.inflight.iter().find(|(_, f)| f.deadline <= now)
```

`inflight` is a `BTreeMap` ordered by **sort key, not deadline**, so when nothing is
expired `find` examines **every** entry — on every poll. A consumer holding k records in
flight pays O(k) per delivery, so draining n while acking lazily is O(n²).

This connects directly to the new `ehdb_feed_shard_inflight` gauge: the operational state
that gauge exists to reveal — high lag with high in-flight — is also the state in which the
drain loop degrades quadratically. Tracked for a fix (a deadline-ordered index makes it
O(log n)).

Subject-routed drain, for comparison: 2.49 ms / 802 K/s for the same 2,000 records, so
per-record subject matching costs ~20% over the plain group.

## Queue depth and divergence are now scrapable

`ehdb-l0` gained four **state gauges** — current depth, as against the 27 cumulative
counters — all computed from in-RAM state with no substrate I/O:

| gauge | what is outstanding |
| :-- | :-- |
| `manifest_parts` | live parts. **Predicts memory** (~1.8 KB each, per Memory above) and is what merge bounds |
| `parts_local_only` | sealed parts with **no** durable copy — the upload backlog |
| `parts_under_replicated` | parts with **some but not enough** copies |
| `dedupe_window_records` | records held in the idempotency window |

⭐ **`parts_under_replicated` closes a real hole.** The uploader assigns
`p.replicas = locations` — the *successful* writes only — so a part that landed 1 of 2
copies is recorded durable, `on_upload_done` fires, and **the age-based durability window
reports it done**. `replica_writes` cannot fill the gap either: it is cumulative, so it
climbs while the deficit stands. Proven in `tests/state_gauges.rs` with a substrate that
refuses writes: 7 parts, `is_durable() == true`, each holding 1 of 2 copies.

And `ehdb_feed_shard_inflight` + `ehdb_feed_total_inflight` now expose
`ShardConsumerGroup::inflight_len`, which **existed all along with no `render_*` emitting
it**. It is not derivable from lag, because lag counts undelivered **plus** unacked:

| lag | inflight | state |
| --: | --: | :-- |
| 10 | **0** | stalled — consumers absent or not polling |
| 10 | **4** | busy — saturated and making progress |

Measured, in `tests/inflight_exposition.rs`: **identical lag of 10 in both**. An alert on
lag alone pages for both and distinguishes neither.

## The benchmarks run in CI

`.github/workflows/ci.yml` gained a `bench` job. Previously CI ran
`cargo bench --workspace --no-run` — it compiled the benches and never executed one.

**Running them is most of the value on its own**, because every case asserts inside its
timed region that it got the records it claims; that is what caught the 2,000-of-10,000
short read. `--no-run` structurally cannot catch it.

Absolute times are **not** gated, per D6: a latency bound on a shared runner is flaky and a
flaky measure gets deleted. `ci/bench_ratios.py` gates the **shape** instead —

- **the denominator first**: it states `expected=13 parsed=13` and fails on a missing
  benchmark, because a parser that finds nothing asserts nothing and exits 0;
- **monotonicity**: a larger batch must never be slower (25% noise tolerance). Ratios are
  taken within one run on one runner, so machine speed cancels;
- **reported, never gated**: the 530x, the 0.999x control, the 32x per-poll figure and the
  2.86x lazy-ack penalty. A guard asserting "batch 1 is ≥50x slower" would fail the day
  somebody fixes small batches — that is a guard against progress.

Both negative controls were run: removing one `estimates.json` fails with the missing name,
and planting an inverted batch ordering fails with the regression named. A guard that has
never fired is indistinguishable from one that cannot.

## Vectors (P6) — recall is 1.0 by construction, and cost is O(ops)

`crates/ehdb-l0/tests/vector_recall.rs` + `crates/ehdb-l0/benches/vector_query.rs`.

### Measuring first changed what the number means

The north-star spec asked for "an honest recall@k number, because *semantic search works*
is unfalsifiable without one". `VectorStore::top_k` turns out to be an **exact brute-force
scan** — every live point, cosine over all, sort, truncate, no approximation anywhere. So:

> **recall@k is 1.0 by construction, and a reported 1.0 is not evidence that search
> works.** It is evidence that an exhaustive scan was exhaustive.

Recall becomes falsifiable the moment an ANN index exists and **not before**. Publishing
"recall@10 = 1.00" today would be a vanity metric — unfalsifiable in exactly the way the
spec warned about, one level up. So recall is kept as a **correctness guard** (anything
below 1.0 is a defect in the scan or the fold) with a control proving the measure **can**
report less than 1.0: a complete candidate set scores **1.000**, a damaged one **0.333**,
and an empty expected set is **undefined rather than 1.0** — otherwise a known-answer set
that failed to load reports a perfect score.

⚠ **The first known-answer set was silently degenerate.** It built 64 points over 16
dimensions, so every point with `axis >= 16` carried **no dominant component**, and at
jitter 0 the query vector was the **zero vector** — cosine against which is undefined.
`recall@1` read **0.0** and looked like an engine defect. `planted()` now panics on
`axis >= dim` and each vector's norm and dominant axis are asserted before it is used as
ground truth. *A fixture that cannot express the answer it claims to know makes every
assertion about it meaningless.*

### Cost is the number that actually bounds the design

| axis | span | result |
| :-- | :-- | :-- |
| dimension (1,000 live) | 32 → 1,024 | 2.36 ms → 54.5 ms — exponent **0.90**, linear, as `cosine` being O(d) predicts |
| live points (dim 128) | 100 → 5,000 | 33 µs → 66.1 ms — **super-linear** |
| **op-log depth, live size held at 500** | 1x → 10x | **172 µs → 64.9 ms, a 378x rise for the same 500 live points and the same answer** |

⭐⭐ **Cost tracks op-log depth, not live points.** The decisive comparison is two rows with
the same op count and a 10x difference in live size:

| collection | ops | live points | query |
| :-- | --: | --: | --: |
| 5,000 written once | 5,000 | 5,000 | **66.1 ms** |
| 500 re-embedded 10x | 5,000 | **500** | **64.9 ms** |

Within **2%**. So **re-embedding a collection is as expensive as growing it** — which is a
different operational story from "vector search is O(n)", and the same shape as the manifest
cost that grew with write count rather than data size (ehdb#344).

### ⚠⚠ And compaction does not fix it — verified, not inferred

The obvious remedy is compaction, so it was measured. After a merge actually ran (1 merge),
the 10x collection went **64.8 ms → 70.5 ms** — **9% worse, not better.** Reading the code
rather than guessing why:

- `live_points` calls `read_index_after(collection, 0)` — **every op ever written** for the
  collection — then folds latest-wins in memory. Cost is O(ops) in both I/O and fold.
- `VectorDataset` defines **no `dedupe_key`**, and the `Dataset` trait exposes **no
  supersede or compaction hook at all** — only an append-time idempotency window, which is
  a different thing.
- Engine merge carries no latest-wins logic: it combines small **parts** into larger parts.

So **merge structurally cannot drop a superseded op.** The prerequisite for vectors at scale
is a **key-level compaction primitive**, not an approximate index — and the engine has no
place to put one today. Tracked as noetl/ehdb#391.

This is the same root cause as the note already standing against the catalog work: *EHDB has
no `Projection`/`Fold` trait, and the pattern is `read_index_after(key, 0)` folded
latest-wins.* It is cheap at small op counts and quadratic-feeling at large ones.

### The catalog attachment needs no new dataset

`collection` is already both the partition and the index dimension, and `point_id` is
free-form, so a catalog object's `(resource_type, path)` maps onto `(collection, point_id)`
directly. Pinned in `vector_recall.rs`: a query on `playbook` returns exactly the playbook
paths (**set equality**, so cross-type leakage fails the test), a path containing `/`
round-trips, an object is its own nearest neighbour, and a deleted object leaves the
results.

⚠ A `point_id` may contain `/` because it is a **payload field**, not a substrate key —
unlike the runtime registry's id, whose charset is deliberately narrow precisely because it
becomes a key. The test pins both so the two are not conflated.

## What is NOT measured yet

Named so the gap is visible rather than implied.

- ~~p99~~ — **done**, see Tail latency.
- ~~Seal / merge / reclaim~~ — **done**, see Lifecycle.
- ~~Memory~~ — **done**, see Memory.
- ~~Multi-shard and concurrent writers~~ — **done**, see Concurrency and multi-shard.
- ~~`ehdb-feed`~~ — **done**, see The feed drain path.
- ~~Queue depth~~ — **done**, see Queue depth and divergence.
- ~~Benches are not enforced~~ — **done**, see The benchmarks run in CI.
- **The drain cost mechanism is measured but not attributed.** Three facts constrain it
  (per-poll cost is not fixed; poll count does not dominate at part-sized batches; ~n^2.5
  at limit 1) and the exact read-path cause is not yet pinned down.
- **The networked transport is still hand-run.** `examples/dispatch_bench.rs` attributes
  publish→claim across the real socket topology, and nothing in CI executes it; the
  benches that run are the in-process drain primitives.
- **No ANN index exists**, so `recall@k` is not yet a real measurement — see Vectors (P6).
  It becomes one when an approximate index does.
- **Nothing measures a multi-node deployment.** Every figure here is one process. Tail
  replication and election (P7/P8 of the north star) are specced, not built, so there is
  nothing to measure yet — and a single-process number must not be quoted as a
  distributed one.

⚠ **This section itself drifted once.** It previously claimed `metrics.rs` "exports **7**
public functions ... three of which are counters" and that "pinned-at-zero series with
RED-proven movement are not yet in place". Both were stale: `metrics.rs` carries **31**
atomic counters and gauges, a 31-row `SERIES` table that `every_snapshot_field_is_exported`
forces to equal them exactly, and `tests/metrics_exposition.rs` holds exactly the three
guards claimed missing. A "not measured yet" list is a representation like any other, and
it drifts in the direction that understates the work.

⚠ And the row count was itself miscounted while writing that correction:
`grep -cE '^\s*\("'` over the `SERIES` block returns **10**, because only 10 rows are
short enough for rustfmt to keep the leading string on the same line as the opening paren.
The rest wrap. Counting `^\s*\(` gives the real number. **A line-oriented grep over Rust
source counts formatting, not code** — the same shape as the `self.field\n    .fetch_add`
miscount that once reported 4 counters as never written when the real answer was 0 of 27.

## Related

- `crates/ehdb-l0/benches/l0_engine.rs` — the benchmarks.
- `crates/ehdb-l0/tests/invariant_measures.rs` — the invariant measures.
- `crates/ehdb-l0/tests/memory_measures.rs` — the allocator instrument (one `#[test]`, by
  necessity).
- `crates/ehdb-l0/tests/concurrency_measures.rs` — concurrent writers, the mint-order trap,
  reader interleaving with its coverage control, multi-shard set-equality.
- `crates/ehdb-feed/benches/drain.rs` — the drain path, the batch axis, and the per-poll
  attribution probe.
- `crates/ehdb-l0/tests/state_gauges.rs` — the four state gauges, pinned and proven to
  move, with a write-refusing substrate for the replica deficits.
- `crates/ehdb-feed/tests/inflight_exposition.rs` — in-flight depth, and the two states
  lag renders identically.
- `crates/ehdb-l0/tests/vector_recall.rs` — the recall guard, its control, and the catalog
  key convention.
- `crates/ehdb-l0/benches/vector_query.rs` — query cost vs live points, dimension, and
  op-log depth, plus the post-compaction row.
- `ci/bench_ratios.py` — the shape guard CI runs.
- `agents/rules/representation-drift.md` in noetl/ai-meta — "print the denominator", and
  "volume is not duration".
