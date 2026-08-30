# Spec — what a real independent failure domain requires

Tracks [noetl/ehdb#332](https://github.com/noetl/ehdb/issues/332) (F5), under
[#324](https://github.com/noetl/ehdb/issues/324). Companion to
[`durability-window.md`](durability-window.md) §2.2 and §6.

> ⚠⚠ **No prod storage changes here.** This delivers the abstraction, a second
> implementation, a conformance suite and a guard. Where prod writes bytes is
> unchanged.

## 1. Where things stand

`LocalFsSubstrate` is the only *storage-backing* `DurableSubstrate`
(`CountingSubstrate` is a transparent decorator over another). So the substrate
is a filesystem path, and whether that path is a distinct failure domain is
decided entirely by what is mounted there.

In prod, nothing distinct is:

```
NOETL_EVENT_BUS_WRITER_DIR   = /data/eventbus          # the writer's own data
NOETL_EHDB_TIER_SERVICE_DIR  = /data/eventbus/ehdb-tier # its "durable" copy
```

One PVC, `noetl-eventbus-writer-0-data`. **The copy lives in a subdirectory of
the volume holding the thing it copies.** The upload turns an unsealed local part
into a sealed local part beside it.

⚠ Not a claim that loss is likely — GCE persistent disks are replicated within a
zone by the platform. A claim that **the durability story the code tells and the
one the deployment implements are different**, and only the deployment is
load-bearing.

## 2. What shipped with this spec

- **`FailureDomain`** — a substrate declares where its bytes live.
  `LocalFsSubstrate` resolves it from the root's **device id**, not its path:
  two directories on one PVC have different paths and the same device, which is
  exactly the production case a path comparison would call independent.
- **`validate_replica_domains`** — refuses a replica set whose members share a
  domain, or whose roots are nested. It reports **every** violation, not the
  first; fixing a misconfiguration one round-trip at a time is how the rest of it
  survives review.
- **`InMemorySubstrate`** — a second implementation, so the trait's contract is
  testable against something that is not a filesystem.
- **A conformance suite** run against both.

### ⭐ What the second implementation found immediately

`get_range` past the end of an object: `LocalFsSubstrate` used `read_exact` and
**errored**; the in-memory draft **clamped**; and the trait specified neither.
With one implementation that ambiguity was invisible.

Resolved **fail-closed** — an over-read is now a hard error everywhere, because
the manifest states every part's length, so an over-read means the caller and the
store disagree, and a short read would let a **truncated part read as intact**.
That is now in the trait docs and pinned by the suite.

## 3. What a real independent failure domain requires

Passing `validate_replica_domains` is necessary and **not sufficient**. Two local
disks on one machine are two domains and one node — `survives_node_loss` is the
separate predicate, and it requires at least one `FailureDomain::Remote`.

A real second substrate must be:

1. **Off-node.** Losing the writer's node, its kubelet, or its PVC must not lose
   the copy. This is the only property that turns node loss from data loss into a
   cold-load.
2. **Independently available.** If it is reachable only through the writer's own
   network path, it shares the writer's partition.
3. **Write-once addressable.** Parts are immutable, so the store needs no
   consensus — the HDFS block-replication model. `put_if_absent` is already the
   right primitive.
4. **Range-readable.** Cold-load reads ranges, not whole objects. A store without
   ranged GET forces whole-part reads and changes the read cost model.
5. **Durable on ack.** `put_if_absent` returning `Ok` must mean the bytes
   survive the store losing a node, or the replica is decorative.

⚠ Zonal separation is the minimum that makes (1) true in GKE. A regional bucket
or a second-zone disk qualifies; a second PVC on the same node does not.

## 4. Why the obvious candidate is not built here

An object-store substrate (GCS/S3) satisfies all five. It is **not** implemented
because this workspace has no HTTP client, and adding one is a dependency
decision for the owner, not a detail to slip into a durability fix. The trait is
the seam; the decision is separable from everything above.

## 5. Sequence to RF > 1

1. ✅ Failure-domain declaration + validation + a second impl (this change).
2. ⬜ Owner decision: HTTP/object-store dependency.
3. ⬜ Object-store `DurableSubstrate`, passing the conformance suite unchanged.
4. ⬜ `ReplicaTarget` set validated at open — refuse to start on a replica set
   that does not spread domains.
5. ⬜ RF > 1 in prod, which is a **storage change** and owner-gated.
6. ⬜ Only then is `AckPolicy::QuorumDurable`
   ([`durability-window.md`](durability-window.md) §6) meaningful.

⚠ Step 4 is where this becomes load-bearing: a validated replica set means a
misconfigured one **fails to start** rather than silently providing RF 1. That is
a behavior change and belongs to its own gate.
