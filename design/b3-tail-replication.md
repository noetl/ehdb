# B3 — replicating the unsealed tail

**Status:** design only. Nothing built. Three decisions need an owner before any code.
**Tracks:** [noetl/ai-meta#460](https://github.com/noetl/ai-meta/issues/460)
**Depends on:** A2 (`GcsSubstrate`, merged) and B2 (age-based sealing, merged).

## 1. The exposure B3 closes

A2 replicates **sealed parts** off-box. After A2 the honest durability statement is:

> Sealed parts survive node loss. The unsealed tail does not. Replication is asynchronous.
> Recovery is a bounded-loss **consistent prefix**.

B2 bounds how long the tail can live by sealing on age. It does not remove the window — it
only makes it finite. B3 is what removes it.

Prod's measured shape, as of 2026-10-09:

| | |
| :-- | :-- |
| replica set | **RF=1**, one local PVC (`survives_node_loss=0`) |
| tail observed on the tier | 7.1 MiB unsealed for **~7 hours** |
| full durability window | now published as `noetl_ehdb_unreplicated_*` — ⚠ **read it before sizing anything here** |

⚠ The last row matters: until today the server published no measurement of the window at all,
and the 7-hour figure came from a different store. **Size B3 from the gauges, not from this
document.**

## 2. The mechanism exists — and it already sees the tail

This was the open question, and it resolves favourably.

`ChangeFeed::poll` → `L0Engine::read_partition_after_limited`, which iterates the manifest's
sealed parts **and then** reads `writer.pending_records()` for the shard:

```rust
// The active (unsealed) hot buffer for this shard — the tail, so it is
// only worth reading when the limit has not already been reached.
if out.len() < limit {
    if let Some(writer) = self.writers.get(&shard) {
        for rec in writer.pending_records() { ... }
    }
}
```

So **the tail is readable through an existing, bounded, cursor-based primitive.** B3 does not
need a new read path, and in particular it does **not** need the shape I first assumed —
periodically re-uploading the whole active part — which costs O(size) per interval and is
quadratic over a part's life (a 7 MiB part re-uploaded every 60 s for 7 h is ~1.5 GB of puts
for ~7 MiB of data).

⚠ Note what this does **not** mean: `ChangeFeed` is explicitly *"not a persisted cursor and
not an ack"*, because a cursor that survives a restart and outruns a freshly-emptied in-memory
index is the [#119](https://github.com/noetl/ai-meta/issues/119) stall. B3 must not introduce
one either; see §5.

## 3. Shape

One task per shard:

```
loop {
    sleep(interval)
    batch = feed.poll_limited(engine, N)      // includes unsealed records
    if batch.is_empty() { continue }
    key = tail/<dataset>/<shard>/<first_seq>-<last_seq>.frames
    remote.put_if_absent(key, encode(batch))  // immutable, write-once
}
```

**Each batch is its own immutable object.** That is the point: `put_if_absent` already
guarantees write-once, parts are content-stable, and nothing is ever rewritten. Many small
objects, each written exactly once — no quadratic re-upload.

Loss after node death becomes bounded by **the poll interval**, not the seal interval.

### Recovery

1. Cold-load from the replica as A2 already does → the sealed prefix.
2. List `tail/<dataset>/<shard>/`, take objects whose range starts above the sealed tip.
3. Replay them in sequence order, **deduplicating by `sort_key`** — a batch can legitimately
   overlap a part that sealed and uploaded after the batch was written.

⚠ Step 3's dedup is not optional and not cosmetic. The same record can exist in both a tail
object and a sealed part, and D1's `global_sequence` is the key that makes dedup exact.
[#335](https://github.com/noetl/ai-meta/issues/335) is the standing reminder that a
double-applied event is a real failure mode here, and that 1 of 4 accumulators in
`apply_event` had no dedup guard.

## 4. Costs, stated

| | |
| :-- | :-- |
| puts | one per interval **per shard with pending records**. At `shard_count=1` and a 1 s interval, ≤3,600/h. Off the append path (p50 77 ms measured), so it does not touch append latency. |
| objects | the tail accumulates small objects until cleaned. **Needs a cleanup policy — see D2.** |
| bytes | ~the data volume, once. Not quadratic. |
| manifest | **untouched.** Tail objects are not parts and must never enter the manifest, or `plan_retention` could drop them as if they were. |

## 5. ⚠⚠ What this does NOT achieve

Stating these so the feature is not oversold the way "no data loss" would be:

- **It is still asynchronous.** Loss is bounded by the poll interval, not eliminated. A
  synchronous remote append per record would eliminate it and costs **p50 77 ms** against a
  ~4 ms local fsync — ~19x, measured, which is why #460 closed that option.
- **It is not consensus and not a quorum.** No acknowledgement from the remote gates the local
  append. A caller that got `Ok` may still lose that record if the node dies inside the
  interval.
- **It does not make the store multi-writer safe.** That is C2/C3 (election + fencing), which
  remain held, and **election must be live and minting epochs before fencing enforces**.
- **It does not give a linearizable cross-shard read.** By CALM that is unavailable
  coordination-free, as a theorem rather than a gap.

The honest post-B3 statement would be: *sealed parts and tail batches both survive node loss;
loss is bounded by the poll interval; recovery is a consistent prefix.*

## 6. Decisions needed before any code

### D1 — the poll interval

Directly trades durability against put volume, and there is no default that is right for both.

| interval | worst-case loss | puts/h/shard |
| :-- | :-- | :-- |
| 250 ms | 250 ms of appends | 14,400 |
| 1 s | 1 s | 3,600 |
| 5 s | 5 s | 720 |
| 30 s | 30 s | 120 |

⚠ Pick this from the measured `noetl_ehdb_unreplicated_oldest_age_seconds` on prod, not from
this table. If the current window is hours, then even 30 s is a ~100x improvement and the
cheap option is the right one.

### D2 — tail-object cleanup

A tail object is redundant once a part covering its range is sealed **and durable**. Options:

1. **Delete on upload-done** — cheapest, but a delete is irreversible and the window where
   both exist is the window recovery needs. Must only fire after the covering part is
   confirmed durable on the same replica.
2. **TTL on the bucket** — simplest, no code, but a TTL is a representation that drifts: it
   knows nothing about whether the covering part uploaded.
3. **Leave them** — correct and unbounded. Cost grows with total write volume.

⚠ My recommendation is (1) **gated on the covering part's durability**, with the delete
counted by its own metric so "cleanup is not running" is visible rather than inferred from a
bucket size. But this is a data-deletion policy on the event log, so it is an owner call.

### D3 — whether tail replication is in the write path's failure domain at all

If a tail put fails repeatedly, what happens?

1. **Log and continue** (my recommendation): the local append is already durable locally;
   failing the append because a *replica* is unreachable converts a durability feature into an
   availability regression. Surface it as a gauge + alert.
2. **Back-pressure the append** — stronger durability, but now a GCS outage stops dispatch.
   ⚠ This is the same trade as A2's "open at RF=1 rather than refuse", and A2 chose to
   degrade-but-measure. Choosing differently here would be inconsistent without a reason.

## 7. Verification plan

Written before the code, because the failure modes here are all quiet.

1. **RED first.** With tail replication off, kill the engine mid-part and show the records are
   gone. Without this, every later green is unfalsifiable.
2. **Byte-level proof, outside the test.** List the bucket with `gcloud` and confirm the tail
   objects, as A2's proof does — a test asserting its own writes is weak evidence.
3. **Recovery from the remote ALONE**, fresh local root, local substrate not passed in — the
   same shape as A2's `sealed_parts_reach_gcs_and_the_log_cold_loads_from_it_alone`.
4. **Overlap dedup**, explicitly: write a batch, seal and upload the covering part, recover,
   and assert **no duplicate `global_sequence`**. This is the #335 shape.
5. **A negative control on the bucket**: recovery from an untouched prefix must recover
   nothing, or (3) is also satisfied by reading a local directory.
6. **The interval is measured, not assumed**: assert the observed worst-case loss is within
   the configured interval under a forced kill, rather than asserting the config value.

## 8. Related

- [`a2-async-offbox-replication.md`](a2-async-offbox-replication.md) — the sealed-part half.
- [`p7-p8-distribution-frontier.md`](p7-p8-distribution-frontier.md) — bounds, and why P8 is
  mutual exclusion rather than consensus.
- [ai-meta#460](https://github.com/noetl/ai-meta/issues/460) — the umbrella.
