# EHDB AI Instructions

## Scope

Applies to the entire `ehdb` repository.

## Mission

EHDB is the Event Horizon Database for NoETL: an Arrow-native,
NoETL-domain storage system for operational metadata, catalog state,
event streams, RAG retrieval state, and historical analytical data.

This is not a generic database project. Favor NoETL-specific storage
semantics over broad database scope. The long-term target is to collapse
NoETL's ordinary dependence on PostgreSQL, NATS JetStream, external
object stores, Qdrant, and ClickHouse into EHDB-owned capabilities.

## Execution Model Boundary

Keep EHDB aligned with NoETL's execution model:

```text
gateway = gatekeeper
worker = atomic compute
playbook = ephemeral blueprint
shared cache = state vehicle
event log = source of truth
```

Do not introduce gateway-side data-touch logic, long-lived per-tenant
agent processes, or worker designs that hold slots across external
waits. EHDB service APIs own storage behavior; clients use explicit
protocols.

## Engineering Rules

- Rust-first implementation.
- Treat Arrow datatypes as native schema primitives.
- Keep catalog metadata transactional and first-class.
- Treat stream logs, durable consumers, replay cursors, and retention as
  EHDB-native design concerns.
- Treat RAG documents, chunks, embeddings, vector-index metadata, tenant
  context, and lineage as first-class NoETL storage concerns.
- Keep object data immutable by default.
- Add wiki updates with public architecture or workflow changes.
- Avoid secrets, tokens, and tenant-sensitive values in examples.
- Containerized changes must validate on local kind before GKE.
