# Second substrate — the choice, and what it needs from the owner

Item 7 / [noetl/ehdb#332](https://github.com/noetl/ehdb/issues/332). **No prod
storage changed.** This surfaces the decision rather than taking it.

## What already landed (inert)

- `DurableSubstrate` — the abstraction, unchanged.
- `FailureDomain` + `validate_replica_domains` / `survives_node_loss` — declared
  and, since worker#295, **computed at open** and published as gauges.
- `InMemorySubstrate` — a second implementation, so the trait's contract is
  testable against something that is not a filesystem.
- A **conformance suite** run against all four implementations, made
  self-enforcing so a fifth cannot skip it.

⚠ `InMemorySubstrate` is **not** a candidate: it is `FailureDomain::Ephemeral`,
and `survives_node_loss` correctly refuses to count it. It exists to pin the
contract, not to store anything.

## Why this is now purely about RF > 1

With item 1 decided as **(d) single-writer by construction**, this work no longer
carries any fencing or election requirement. Its only job is to give replication
somewhere to go that **does not die with the writer's node**.

Today it has nowhere: `NOETL_EHDB_TIER_SERVICE_DIR=/data/eventbus/ehdb-tier` sits
inside `NOETL_EVENT_BUS_WRITER_DIR=/data/eventbus`, one `ReadWriteOnce` PVC. The
"replica" shares a disk with the primary.

⚠ And note the coupling to (d): the RWO attachment guarantee that makes (d) safe
is the *same* property that makes the current substrate node-local. **Moving to a
genuinely shared substrate un-parks the fencing work** (G1/G2/G6) — see
`SEQUENCING.md`. That is a consequence of this choice, not a separate decision.

## The candidates

| | independent of the node? | new dependency | notes |
| :-- | :-- | :-- | :-- |
| **(A) GCS bucket** | ✅ regional — replicated across zones | ⚠ GCS client crate + Workload Identity | A regional bucket already exists in-project |
| **(B) A second PVC** | ❌ still one node, one zone | none | Passes `validate_replica_domains` and **fails** `survives_node_loss` — exactly the distinction that predicate exists to draw |
| **(C) Filestore / NFS** | ⚠ partially — regional tiers exist | mount + provisioning | Keeps the filesystem impl; no new client crate |

**(B) is a trap worth naming**: it would make the domain check pass while
changing nothing about node loss. The gauges would go green and the risk would be
identical.

## Recommendation: (A) GCS

`shastaratech-noetl-prod-results` already exists at **US-CENTRAL1** (regional —
replicated across zones within the region, so genuinely independent of the
writer's zonal PD). A second bucket, or a prefix in that one, is the smallest
step to a real failure domain.

⚠ It also satisfies the five properties in `second-substrate.md` §3 —
off-node, independently available, write-once addressable (`put_if_absent` maps
to `x-goog-if-generation-match: 0`), range-readable, and durable on ack.

## 🔴 Owner-run prerequisites — I have not done any of these

1. **A dedicated ServiceAccount for the writer.** It currently runs as
   **`default` with no annotations**. ⚠ Worth fixing regardless of this decision:
   `default` should not carry identity, and Workload Identity cannot be attached
   to it safely.
2. **A GSA**, e.g. `noetl-ehdb-substrate@shastaratech-noetl-prod.iam.gserviceaccount.com`,
   granted `roles/storage.objectAdmin` on the chosen bucket only.
3. **Both halves of the Workload Identity binding** — the
   `roles/iam.workloadIdentityUser` grant on the GSA **and** the
   `iam.gke.io/gcp-service-account` annotation on the KSA. ⚠ One without the
   other fails at runtime, and the pattern already exists in this cluster to copy
   from: `noetl-server-rust` → `noetl-result-tier@…`.
4. **A dependency decision:** no ehdb crate speaks GCS or HTTP today. This adds
   the first such client to the workspace — the same class of decision that
   parked F1's kube adapter.

## Sequence once approved

1. Owner: SA + GSA + IAM + annotation (1–3 above).
2. Owner: approve the client-crate dependency (4).
3. Me: implement `GcsSubstrate`, which must pass the existing conformance suite
   **unchanged** — that suite is the acceptance test, and it already caught one
   unspecified contract point (`get_range` past end) the moment a second
   implementation existed.
4. Me: wire it as an additional `ReplicaTarget` behind a default-off flag.
5. Owner: enable RF > 1, then G4 validation at open — ⚠ in that order, because
   validation would fail on today's layout by construction.
