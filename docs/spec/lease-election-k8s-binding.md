# The Kubernetes binding for shard election

Companion to [`writer-election-and-fencing.md`](writer-election-and-fencing.md).
Tracks [noetl/ehdb#331](https://github.com/noetl/ehdb/issues/331) (F1).

> ⚠⚠ **Not implemented.** `ehdb-reference::election` ships the election *state
> machine* and proves it against a `LeaseStore` with real compare-and-swap
> semantics. The Kubernetes adapter below is specified, not built.

## Why it is not built yet

This workspace has **no HTTP or `kube` client**. Pulling one in drags
`k8s-openapi`, `tower`, `hyper` and a TLS stack into a crate tree that currently
has none of it. That is a dependency decision, not an implementation detail, and
it belongs to the owner rather than to this change.

`LeaseStore` is the seam that keeps the decision open, and it is the *only* thing
the adapter has to satisfy.

## The mapping

| `LeaseRecord` field | Kubernetes `coordination.k8s.io/v1` Lease |
| :-- | :-- |
| `holder` | `spec.holderIdentity` |
| `transitions` | `spec.leaseTransitions` |
| `renewed_at_millis` | `spec.renewTime` |
| `duration_secs` | `spec.leaseDurationSeconds` |
| `version` | `metadata.resourceVersion` |

- `read` → `GET /apis/coordination.k8s.io/v1/namespaces/{ns}/leases/{name}`
- `create` → `POST` the collection; `409 Conflict` maps to `Ok(false)`.
- `compare_and_swap` → `PUT` with `metadata.resourceVersion` set to
  `expected_version`; `409 Conflict` maps to `Ok(false)`.

⭐ **That 409 is the mutual exclusion.** The API server's optimistic concurrency
does the work; an adapter that retries a 409 by re-reading and writing again has
thrown the guarantee away.

⚠ `spec.leaseTransitions` is incremented **by the caller**, not by the API
server, when `holderIdentity` changes. The adapter must set it, and must set it
from the value it read under the same `resourceVersion` it is swapping on —
otherwise two nodes can mint the same epoch.

## RBAC — owner-run

The writer's ServiceAccount needs leases in its own namespace. **I have not
applied this**; IAM and RBAC changes are owner-run.

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: ehdb-shard-election
  namespace: noetl
rules:
  - apiGroups: ["coordination.k8s.io"]
    resources: ["leases"]
    verbs: ["get", "list", "watch", "create", "update"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: ehdb-shard-election
  namespace: noetl
subjects:
  - kind: ServiceAccount
    name: <the cmdbus-writer's ServiceAccount>
    namespace: noetl
roleRef:
  kind: Role
  name: ehdb-shard-election
  apiGroup: rbac.authorization.k8s.io
```

⚠ No `delete`. A lease is released by letting it lapse, never by deleting the
object — deleting it discards `leaseTransitions`, and the next acquisition would
mint epoch 1 again. **A reused epoch is worse than no epoch**, because the store
would accept a superseded writer as current.

## Promotion — owner-gated

Running the election changes nothing on its own. Making it authoritative means:

1. The writer refuses to append without a token (`epoch() == None` ⇒ do not
   write).
2. `FencingMode::Enforce` on the store ([#330](https://github.com/noetl/ehdb/issues/330)).
3. Single-writer stops resting on `replicas: 1`.

⚠⚠ Step 1 **changes the live writer path** — a writer that cannot reach the API
server would stop writing. That trade (refuse rather than risk a fork) is the
right one, but it is the owner's to make, and it needs the API server's
availability treated as a dependency of the write path.

## Verification before promotion

- [ ] Adapter conformance: the same suite `InMemoryLeaseStore` passes, run
      against a real API server in kind.
- [ ] A forced holder change (delete the pod, not the lease) advances
      `leaseTransitions` by exactly 1.
- [ ] A partitioned holder's token stops being issued within
      `leaseDurationSeconds`.
- [ ] `ehdb_fencing_stale_observed_total` observed at 0 over a soak **before**
      enforcement — and if it is not 0, that is the finding.
