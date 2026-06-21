# EHDB

EHDB is the Event Horizon Database for the NoETL ecosystem.

It is an Arrow-native, NoETL-domain storage system that stores
operational metadata transactionally, stores analytical/historical data,
carries event streams, and serves AI/RAG retrieval needs for NoETL
workloads.

EHDB is not a generic database first. It is a focused storage substrate
for the NoETL multitenant distributed operating-system cloud platform.
Over time it should absorb the platform roles currently served by
PostgreSQL, NATS JetStream, external object stores, Qdrant, and
ClickHouse.

## Goals

- Store NoETL system metadata and catalog data without relying on an
  external PostgreSQL catalog at the self-hosting milestone.
- Keep the catalog inside the database as first-class transactional
  state.
- Provide EHDB-native event streams, durable consumers, replay cursors,
  and retention semantics for NoETL execution state.
- Support RAG primitives: documents, chunks, embedding metadata, vector
  index metadata, retrieval policy, tenant context, and lineage.
- Store NoETL system WASM library manifests and environment/channel
  bindings so system playbook functionality can be hot-replaced without
  crate semantic-version churn.
- Use Apache Arrow datatypes and Arrow IPC/Flight as native boundaries.
- Store immutable analytical data files in S3, GCS, Azure Blob, and
  compatible object stores.
- Separate write nodes, read nodes, and bounded maintenance jobs.

## Workspace

```text
crates/
|-- ehdb-core      # identifiers, errors, Arrow schema helpers
|-- ehdb-catalog   # catalog model and reference in-memory catalog
|-- ehdb-storage   # object-store traits and local reference adapter
|-- ehdb-stream    # stream logs, durable consumers, replay cursors
|-- ehdb-retrieval # RAG documents, chunks, embeddings, retrieval metadata
|-- ehdb-system    # system WASM library manifests and environment bindings
`-- ehdb-transaction # transaction records, replay, local durable log
```

Future workspace areas include analytical read paths, service APIs, and
NoETL integration surfaces.

## Local Durability

`ehdb-transaction` includes `LocalJsonlTransactionLog`, a reference
append-only transaction log for the local developer loop and
crash/restart tests. It writes one serialized `TransactionRecord` per
line, calls `sync_data` after each append, and rebuilds replay state on
open using the same duplicate transaction ID and sequence checks as the
in-memory log.

`ehdb-stream` includes `LocalJsonlStreamLog`, a reference append-only
stream journal for the local developer loop and restart tests. It
persists create-stream, create-consumer, publish, and ack operations;
on open it rebuilds stream records, retention state, next sequence, and
durable consumer cursors.

These are not the production consensus or replicated stream layers.
Raft/Paxos and distributed stream storage belong behind these boundaries
once the metadata, stream, and NoETL integration contracts stabilize.

`ehdb-system` includes `LocalJsonlSystemLibraryCatalog`, a reference
append-only journal for system WASM library manifests and bindings. It
persists publish and bind operations, then rebuilds immutable manifests
and environment/channel bindings on open so hot-replacement state
survives restart.

## System WASM Libraries

`ehdb-system` models NoETL system playbook functionality as compiled
WASM libraries stored in EHDB. It mirrors NoETL's worker-side WASM
dispatch shape: a system module resolves to `{ path, version, digest,
entry }`, while EHDB owns the durable catalog side.

The model separates:

- Immutable module manifests: path, revision, digest, entry export,
  target, object path, byte length, host capabilities, and transaction
  provenance.
- Mutable bindings: tenant, namespace, environment, channel, and path
  resolving to a specific module revision/digest.

That lets `kind`, `gke-prod`, `azure-dev`, or tenant-specific
environments run different implementations smoothly. A stable channel
can be rebound to a new digest/revision for a hot fix without changing
the Rust crate version or forcing every caller to chase semver bumps.
The local JSONL journal preserves those rebinding decisions across
restart for the developer loop.

## Developer Loop

```bash
cargo fmt --all
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo bench --workspace --no-run
cargo bench -p ehdb-transaction --bench reference_models
```

Current reference benchmark baseline on the initial local models:

| Benchmark | Workload | Baseline |
|---|---|---|
| `stream_publish_replay_1000` | 1000 stream publishes + full replay | ~626 us |
| `transaction_append_replay_1000` | 1000 transaction appends + full replay | ~1.04 ms |
| `local_transaction_jsonl/append_reopen_100` | 100 fsynced JSONL appends + reopen + full replay | ~448 ms |
| `local_stream_jsonl/publish_reopen_100` | 100 fsynced stream publishes + reopen + full replay | ~456 ms |

## Design

The design source of truth lives in the project wiki:

- https://github.com/noetl/ehdb/wiki
- https://github.com/noetl/ehdb/wiki/Architecture
- https://github.com/noetl/ehdb/wiki/Roadmap
