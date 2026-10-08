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

## What is NOT measured yet

Named so the gap is visible rather than implied.

- ~~p99~~ — **done**, see Tail latency.
- ~~Seal / merge / reclaim~~ — **done**, see Lifecycle.
- ~~Memory~~ — **done**, see Memory.
- ~~Multi-shard and concurrent writers~~ — **done**, see Concurrency and multi-shard.
- **`ehdb-feed`** — 36 files, still 0 benches; the mirror/drain path is unmeasured. This is
  the next gap, and the one with prod history: ai-meta#344 was 100% transport timeouts
  because the relay never batched, 0 of 80,264.
- **Queue depth has an accessor and no exposition.** `ShardConsumerGroup::inflight_len` and
  `SubjectConsumerGroup::inflight_len` exist and **no `render_*` function emits them** — the
  "recorder exists, nothing calls it" shape from the reachability lens. A scrape cannot see
  consumer backlog today.
- **Benches are not enforced.** Every number on this page was produced by hand. Nothing in
  CI runs a benchmark or compares one against a baseline, so a regression is silent.

⚠ **This section itself drifted.** It previously claimed `metrics.rs` "exports **7** public
functions ... three of which are counters" and that "pinned-at-zero series with RED-proven
movement are not yet in place". Both were stale: `metrics.rs` carries **27** atomic
counters, a **27**-row `SERIES` table that `every_snapshot_field_is_exported` forces to
equal them exactly, and `tests/metrics_exposition.rs` holds
exactly the three guards claimed missing — a denominator guard that reads the field list out
of the source, a pinned-at-zero check on a fresh engine, and a proven-to-move check driven
by the lifecycle. A "not measured yet" list is a representation like any other, and it
drifts in the direction that understates the work.

⚠ And the row count above was itself miscounted once while writing this paragraph:
`grep -cE '^\s*\("'` over the `SERIES` block returns **10**, because only 10 of the 27 rows
are short enough for rustfmt to keep the leading string on the same line as the opening
paren. The other 17 wrap. Counting `^\s*\(` returns 27, which is what
`every_snapshot_field_is_exported` independently requires. **A line-oriented grep over Rust
source counts formatting, not code** — the same shape as the `self.field\n    .fetch_add`
miscount that once reported 4 counters as never written when the real answer was 0 of 27.

## Related

- `crates/ehdb-l0/benches/l0_engine.rs` — the benchmarks.
- `crates/ehdb-l0/tests/invariant_measures.rs` — the invariant measures.
- `crates/ehdb-l0/tests/memory_measures.rs` — the allocator instrument (one `#[test]`, by
  necessity).
- `crates/ehdb-l0/tests/concurrency_measures.rs` — concurrent writers, the mint-order trap,
  reader interleaving with its coverage control, multi-shard set-equality.
- `agents/rules/representation-drift.md` in noetl/ai-meta — "print the denominator", and
  "volume is not duration".
