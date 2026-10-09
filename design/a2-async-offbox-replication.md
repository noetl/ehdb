# A2 — asynchronous off-box replication: design

**Status:** design only · **Opened:** 2026-10-09 ·
Program: [noetl/ai-meta#460](https://github.com/noetl/ai-meta/issues/460) A2 ·
Parent: [`p7-p8-distribution-frontier.md`](p7-p8-distribution-frontier.md)

> ⚠⚠ **Design only. No durability-semantics change is built here.** A3 (running RF>1) and
> B3 (the unsealed tail) are held until the archival tier is verified in prod.

---

## 1. The measurement that decides the shape

`noetl_object_store_put_seconds{backend="gcs",outcome="ok"}` on prod v3.135.0, init seed
subtracted:

| samples | sum | mean |
| :-- | :-- | :-- |
| 4 | 1.874 s | 468.5 ms |
| 7 | 2.169 s | 309.9 ms |
| **14** | **3.101 s** | **221.5 ms** |

⚠ The mean is **falling as samples accumulate** (468 → 310 → 221 ms), so the early readings
were dominated by a few slow puts. Treat 221 ms as the current best estimate of a central
value, not a settled p50 — and note these are *result-tier* objects of unmeasured size.

Against the ~4 ms local `fsync` this architecture already treats as its append ceiling,
that is **~55×**.

### 1.1 Synchronous per-record replication is a non-starter — confirmed

Adding a ~221 ms remote round trip to each append would make the remote store the append
latency, and the local `fsync` irrelevant. This is no longer an estimate: it is the
measured ratio. **Rejected.**

---

## 2. ⭐⭐ The finding that matters more: sealed-part replication is *already* asynchronous

The obvious conclusion from §1 is "design A2 around async replication". The stronger and
more useful conclusion is that **the existing design is already async, and the 221 ms costs
the write path nothing.**

Verified in `ehdb-l0`:

- `DurableSubstrate` is **entirely synchronous** (no `async` methods), and
- the substrate write happens in the **uploader**, on a dedicated `std::thread`
  (`engine.rs:651` `std::thread::Builder::new()`; the `put_if_absent` at `engine.rs:1768`),
- **not** on the append path. The module doc describes the flow as *"read sealed part bytes
  → `substrate.put_if_absent` → record a replica → rewrite durable manifest"*.

So injecting a `GcsSubstrate` as a second `ReplicaTarget` puts the 221 ms on the uploader
thread and **zero milliseconds on any append**. A2 needs no new asynchrony — it inherits
it.

⚠ This is only true for **sealed** parts. The tail is a different problem (§4), and
conflating them is how A2 gets oversold.

### 2.1 Capacity: is one uploader thread enough?

Measured production rates:

| store | growth | unit | one unit every |
| :-- | :-- | :-- | :-- |
| server embedded shadow | ~82 MiB/day | ~3.7 MiB part | **~1.1 h** |
| writer tier | ~630 MiB/day | 256 MiB segment | **~9.8 h** |

A single uploader doing one put per part — even at seconds per put for a multi-MiB object —
has **three orders of magnitude of headroom** against a part every 1.1 hours. ⚠ The 221 ms
figure is for smaller result-tier objects; a 3.7 MiB part will be slower, and **B1 should
measure a part-sized put before A3 is armed**. But no plausible value changes the capacity
conclusion.

⭐ So the A2 risk is **not** throughput. It is the durability window (§3) and the failure
behaviour (§5).

---

## 3. What async buys, and the window it leaves — stated precisely

The durability window for any given event is:

```
window = (append → seal)  +  (seal → remote put acked)
          ^^^^^^^^^^^^^^      ^^^^^^^^^^^^^^^^^^^^^^^^
          dominant            sub-second to seconds
```

**The second term is what A2 adds, and it is the small one.** The first term is the unsealed
tail, and on the writer's tier it is **bounded by bytes and not by time** — the measured
`catalog.jsonl` sat unsealed for **~7 hours** at 7.1 MiB.

### ⚠⚠ So A2 does NOT meaningfully shrink the durability window

A2 gives a genuine second **failure domain** for data that is **already sealed**, which is
the bulk of history. It does **nothing** for in-flight events. If the primary's disk dies:

| data | survives A2? |
| :-- | :-- | 
| sealed parts with a remote copy acked | ✅ yes |
| sealed parts not yet uploaded | ❌ no — observable as `parts_under_replicated` |
| **the unsealed tail** (up to 256 MiB, up to hours) | ❌ **no — this is B3** |

⭐ **A2's honest value proposition: it makes `survives_node_loss` true for sealed history
and leaves the tail exactly as exposed as it is today.** Anyone reading "we now have a
second copy" should read it as "of everything older than the seal boundary".

### 3.1 The consistency implication

Async replication means the remote copy is a **lagging prefix** of the local one. Concretely:

- **Reads never see it.** The remote is a replica target for durability, not a read source
  — `read_*` paths consult the local tier. So async introduces **no read-consistency
  change**: there is no window in which a reader observes stale data, because no reader
  observes the replica at all.
- **Recovery from the remote is a prefix recovery.** After a primary loss, the recovered
  store is the log up to the last acked remote put, which is a **consistent prefix** —
  monotonic, ordered, and missing a suffix. That is exactly the shape replay tolerates, and
  it is why this works without consensus (CALM: appends are monotonic).
- ⚠ **It is not a point-in-time snapshot of the primary.** The suffix loss is real and its
  size is the window above. Any claim of "no data loss" would be false; the correct claim is
  "**bounded** loss, bounded by the seal boundary, and observable before the fact."

---

## 4. Out of scope here

**B3 — the unsealed tail.** Held. It is the dominant term in §3 and needs a replication
protocol rather than a substrate, and it changes durability semantics.

**A3 — running RF>1 live.** Held until archival is verified in prod.

---

## 5. The design

### 5.1 Where `GcsSubstrate` lives

**In the server crate, injected** — not in `ehdb-l0`.

`DurableSubstrate` is a trait and `open_replicated` takes `Vec<ReplicaTarget>` built from
`Arc<dyn DurableSubstrate>`, so the implementation can live where a working GCS client
already does. ⭐ This retires `second-substrate-choice.md`'s prerequisite 4 (*"no ehdb crate
speaks GCS or HTTP"*) **without adding any dependency to ehdb**, and the sync-trait /
dedicated-thread facts in §2 make a blocking implementation acceptable there.

### 5.2 ⚠⚠ Two buckets are ONE failure domain

`FailureDomain::label()` collapses `Remote { provider, .. }` to **`remote-{provider}`**,
ignoring the bucket. So:

- `LocalDevice{66320}` + `Remote{gcs}` → **two domains**, `survives_node_loss` **true**. ✅
- `Remote{gcs, bucket-a}` + `Remote{gcs, bucket-b}` → **one domain**. ❌

Any "spread across two buckets" plan is a no-op against this predicate, and the predicate is
right: two buckets at one provider have correlated failure.

### 5.3 The conformance suite is the acceptance test

`second-substrate.md` ships a substrate conformance suite that must pass **unchanged** — it
already caught one unspecified contract point (`get_range` past end) the moment a second
implementation existed. A `GcsSubstrate` that needs the suite relaxed is a `GcsSubstrate`
that is wrong.

⚠ One contract point to settle before coding: `put_if_absent` must map to
`x-goog-if-generation-match: 0` so a concurrent duplicate upload is a no-op rather than an
overwrite. Parts are content-stable, so this is a safety net rather than a correctness
dependency — but it must be the real precondition header, not a read-then-write.

### 5.4 Rollout shape (for A3, not built here)

1. `GcsSubstrate` + conformance suite green. **No wiring.**
2. Wire as an additional `ReplicaTarget` behind a **default-off** flag.
3. Enable on one non-critical dataset; measure a **part-sized** put, `replica_writes`,
   `parts_under_replicated`, and uploader queue depth.
4. ⭐ `noetl_ehdb_survives_node_loss` flips **0 → 1**, which is the single verification that
   A2/A3 did what they claim. It is already shipped and already reads 0 on prod.

---

## 6. Open questions

- **A part-sized put latency.** The 221 ms is result-tier objects; a 3.7 MiB part is
  unmeasured. Needed before A3, not before A2's code.
- **Uploader failure policy.** Today a failed upload leaves the part local-only and
  `parts_under_replicated` climbs. With a remote target, a GCS outage must not block sealing
  or stall the uploader indefinitely — retry with a cap, and let the gauge carry the truth.
  ⚠ This is the one place where A2 could affect the local write path if designed carelessly:
  an unbounded retry on a saturated uploader eventually backs up into sealing.
- **Does the archive tier's bucket get reused?** Separate prefixes in one bucket would be
  operationally simpler and are the **same failure domain** either way (§5.2), so the choice
  is about blast radius and lifecycle rules, not durability.

## 7. Related

- [`p7-p8-distribution-frontier.md`](p7-p8-distribution-frontier.md) — the program and its bounds.
- [`second-substrate.md`](../docs/spec/second-substrate.md) · [`second-substrate-choice.md`](../docs/spec/second-substrate-choice.md)
- [`retention-archival-tier.md`](retention-archival-tier.md) — the off-box path that proved the auth and the dependency.
