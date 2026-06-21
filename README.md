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
`-- ehdb-retrieval # RAG documents, chunks, embeddings, retrieval metadata
```

Future workspace areas include analytical read paths, service APIs, and
NoETL integration surfaces.

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
| `stream_publish_replay_1000` | 1000 stream publishes + full replay | ~466 us |
| `transaction_append_replay_1000` | 1000 transaction appends + full replay | ~843 us |

## Design

The design source of truth lives in the project wiki:

- https://github.com/noetl/ehdb/wiki
- https://github.com/noetl/ehdb/wiki/Architecture
- https://github.com/noetl/ehdb/wiki/Roadmap
