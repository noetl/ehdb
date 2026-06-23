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
|-- ehdb-reference # replay applier over the local reference catalogs
|-- ehdb-storage   # object-store traits and local reference adapter
|-- ehdb-stream    # stream logs, durable consumers, replay cursors
|-- ehdb-retrieval # RAG documents, chunks, embeddings, retrieval metadata
|-- ehdb-system    # system WASM library manifests and environment bindings
|-- ehdb-service   # service-facing request/result boundaries
`-- ehdb-transaction # transaction records, replay, local durable log
```

Future workspace areas include network services, analytical execution,
and NoETL integration surfaces.

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

## Immutable Objects

`ehdb-storage` includes a local immutable object-store adapter for tests
and the developer loop. Stored `ObjectRef` values now carry the object
path, byte length, SHA-256 digest, geo location, and data-gravity shard
pointer. `get_verified` reads through the object reference and rejects
length or digest mismatches instead of returning corrupt bytes.

The storage crate also provides a deterministic table/snapshot object
path helper:

```text
{tenant}/{namespace}/tables/{table}/snapshots/{snapshot}/{file}
```

This is the local reference boundary for catalog-addressable data files.
Production cloud object APIs remain adapter details behind the EHDB
storage layer.

Geo placement and data-gravity shards are design-time distributed
storage pointers. They allow later read/write nodes, replicators, and
placement planners to route object files toward the region, cloud, and
workload gravity that owns the data, without moving data-touch logic
into the gateway.

`PlacementPolicy` adds the local contract for those future planners: a
policy has exactly one primary placement, one data-gravity shard shared
by all targets, a minimum copy count, and no duplicate geo/shard
targets. It is metadata only; it does not copy objects or schedule
replication by itself.

`plan_replication` compares an object plus known replicas with a
placement policy and emits deterministic actions: already satisfied
placements or copy-needed targets. The plan is still metadata; future
replicator workers can execute it without changing the gateway role.

`InMemoryObjectReplicaRegistry` records durable replica inventory for
the reference model. It keeps object path, byte length, digest, geo
placement, and data-gravity shard together, rejects conflicting metadata
for the same object path, and can feed `plan_replication` from registry
state instead of caller-supplied arrays.

`LocalReplicationExecutor` is the bounded local execution reference for
copy-needed plans. It verifies source bytes through the immutable object
store and records successful target replicas by appending
`StorageMutation::RegisterReplica` transactions through
`LocalReferenceRuntime`. Already-satisfied plans are no-ops. It does not
add background schedulers, cloud transfer adapters, or gateway
data-touch behavior.

`LocalArrowIpcTableStore` is the first local analytical data-path
fixture. It writes an Arrow `RecordBatch` as an immutable IPC object,
commits a catalog snapshot over the content-checked object reference,
and reads the latest snapshot back through verified object reads before
decoding Arrow. This is not an Arrow Flight service yet; it proves the
catalog/object boundary for columnar data.

`LocalArrowSnapshotScanner` adds the first local scan fixture on top of
that boundary. It resolves the latest table snapshot, verifies each
Arrow IPC object before decoding, and can project named columns in the
requested order. Predicate pushdown, SQL planning, distributed execution,
and Arrow Flight remain future service surfaces.

The scanner also supports a first local equality-filter fixture. Filters
are single-column equality predicates over UTF-8 or Int64 columns,
applied after verified Arrow IPC decode and before optional projection.
This is intentionally not predicate pushdown; object statistics,
partition pruning, SQL planning, distributed execution, and Arrow Flight
remain future work.

`ehdb-service` adds the first service-facing scan boundary. Its local
adapter wraps `LocalArrowSnapshotScanner` with a typed latest-table scan
request and an `ArrowScanResult` carrying schema, batches, and row
count. This prepares the Arrow Flight read path without introducing a
network server, SQL planner, distributed executor, or gateway direct
read behavior.

The service crate also includes the first Arrow Flight scan ticket
codec. `ScanFlightTicket` serializes latest-table scan requests into a
versioned payload, round-trips through Arrow Flight `Ticket`, and builds
command `FlightDescriptor` values for the future Flight read API. This
is still a contract fixture: there is no Flight server, client, SQL
planner, predicate pushdown, distributed executor, or gateway direct
read path yet.

`ArrowScanResult` can now encode its batches into Arrow Flight
`FlightData` messages and decode those messages back into a validated
result. This proves the local result-stream contract for the future
`do_get` path while remaining pre-network and pre-distributed.

`ArrowScanResult` can also build a pre-network Arrow Flight `FlightInfo`
value from a `ScanFlightTicket`. The fixture includes schema IPC bytes,
a command descriptor, one endpoint ticket, ordered result metadata, row
count, and encoded byte count. It is metadata only; no Flight server or
gateway data-touch path is introduced.

`LocalArrowFlightService` ties those fixtures together in process. It
can build `FlightInfo`, return `SchemaResult`, and execute `do_get` for
a latest-table scan against the same decoded Arrow Flight ticket
contract. This is still a local service facade, not a network server,
SQL planner, or gateway read path.

`LocalArrowFlightServer` is the first generated Arrow Flight service
trait adapter. It implements `get_flight_info`, `get_schema`, and
`do_get` over the local facade, maps EHDB errors to gRPC statuses,
streams `FlightData` responses, and returns explicit unimplemented
statuses for non-scan Flight methods. It enforces the configured request
metadata auth policy and optional tenant/namespace scan scope policy on
implemented scan methods, but it does not bind a port, start a daemon,
implement TLS/external identity, or give the gateway direct storage
access.

`LocalArrowFlightServerConfig` adds the first bounded lifecycle
configuration surface. It validates bind address, message sizes,
concurrency, auth policy, scan scope, scan grants, and access-log
policy, then constructs the generated service with message limits
applied. Unauthenticated mode is valid only for loopback local-reference
use. The reference
`HeaderToken` policy validates a lowercase ASCII metadata header name
and non-empty token, then requires that token on scan calls. This is an
auth-boundary contract for tests and local harnesses, not production
TLS, identity federation, ACL enforcement, or gateway read routing.

The configured `max_concurrent_requests` value is enforced by a local
fail-fast semaphore on implemented scan methods: `get_flight_info`,
`get_schema`, and `do_get`. When the local request budget is exhausted,
the generated service returns gRPC `RESOURCE_EXHAUSTED`. This is a
local reference guard, not a request scheduler or distributed admission
controller.

`FlightAccessLogPolicy` keeps scan access summaries bounded and
DEBUG-only by default. `DebugOnly` emits structured summaries for
decoded `get_flight_info`, `get_schema`, and `do_get` requests with
method, gRPC code, row/message counts, projection count, predicate
presence, and which metadata guards were required. It does not log
tokens, principal values, tenant/table identifiers, object paths,
predicate values, or Arrow payloads. `Disabled` emits no scan access
summaries.

`FlightScanScopePolicy` adds the first tenant/namespace request scope
guard for scan calls. When enabled, it requires `x-ehdb-tenant` and
`x-ehdb-namespace` metadata to match the decoded scan request before
the local scanner runs. Missing scope metadata returns
`UNAUTHENTICATED`; mismatched scope metadata returns
`PERMISSION_DENIED`. This is the metadata contract that future catalog
ACL checks can attach to, not ACL enforcement by itself.

`FlightScanGrantPolicy` can require `x-ehdb-principal` metadata and
check the replayed catalog scan grants before `get_flight_info` or
`do_get` reaches the local scanner. Missing or invalid principal
metadata returns `UNAUTHENTICATED`; a principal without
`CatalogScanGrant` for the requested table returns `PERMISSION_DENIED`.
This is local reference enforcement over EHDB catalog state, not
production IAM, TLS identity, revocation, or policy composition.

`LocalArrowFlightListener` is a loopback-only reference harness behind
that config. It binds an ephemeral or configured loopback address,
reports the actual local address, serves the generated Flight service,
and exits through an explicit shutdown future. Non-loopback exposure,
TLS/auth implementation, request scheduling, gateway integration, SQL
planning, predicate pushdown, and distributed execution remain out of
scope.

The loopback client smoke path starts that listener and connects with
the Arrow Flight client over tonic/gRPC transport. It calls
`get_flight_info`, follows the returned endpoint ticket with `do_get`,
and decodes the returned Arrow batches. A second smoke path proves the
same flow with the header-token auth policy enabled, and a third proves
tenant/namespace scope metadata over real tonic/gRPC transport. A fourth
proves catalog-backed scan grant enforcement with principal metadata
over the same loopback path. This remains a local-reference test path,
not a gateway integration.

## Catalog Snapshots And Scan Grants

`ehdb-catalog` stores immutable table snapshot metadata over
content-checked object references. A snapshot carries a snapshot ID,
optional parent snapshot, object file references, and the committing
transaction ID. The in-memory reference catalog rejects missing tables,
empty file sets, duplicate snapshots, and parent-chain mismatches.

`CatalogMutation::CommitSnapshot` makes snapshot metadata replayable
through the transaction log, and `ehdb-reference` rebuilds latest table
snapshot state from replay alongside catalog tables, streams, retrieval
metadata, and system library bindings.

`CatalogScanGrant` adds the first durable catalog-side scan grant
reference model. A grant ties a tenant, namespace, table ID, principal,
and granting transaction together, and `InMemoryCatalog::can_scan`
answers whether that principal has scan access to the table. The
reference catalog rejects grants for missing tables and duplicate grants.
`CatalogMutation::GrantScan` makes the metadata replayable through
`ehdb-reference` and `LocalReferenceRuntime`. `ehdb-service` can now use
that replayed metadata through `FlightScanGrantPolicy` for local Arrow
Flight scan authorization. Production IAM, policy evaluation,
revocation, and non-loopback service exposure remain future surfaces.

`ehdb-retrieval` now includes a local exact cosine-similarity fixture
over registered chunk embeddings. `VectorSearch` scopes candidates by
tenant, namespace, and embedding model, validates finite non-zero query
and embedding vectors, applies dimension compatibility, and returns
deterministically ordered `VectorSearchHit` results. This is a local
reference RAG primitive only; ANN indexes, retrieval services,
production IAM, Qdrant adapters, and distributed query execution remain
future surfaces.

`ehdb-service` exposes that local vector lookup through
`LocalRetrievalSearchService`, an in-process service-facing request and
result boundary over replayed `LocalReferenceRuntime` retrieval state.
`SearchSimilarChunksRequest` returns ranked chunk hits with document
identity, text, checksum, model, dimensions, and score while avoiding raw
embedding vectors in the response. This is not a network service,
gateway route, production retrieval API, or persistent daemon.
The same service boundary also exposes exact local text matching through
`SearchTextChunksRequest`, returning deterministic chunk hits with match
counts for tenant/namespace-scoped RAG lookup fixtures. Full-text
indexes, BM25 ranking, external search adapters, and distributed query
execution remain future surfaces.
`SearchHybridChunksRequest` combines exact cosine similarity and exact
text match counts with caller-provided non-negative weights, producing
deterministic hybrid RAG hits over replayed retrieval state. This is a
local scoring fixture, not an ANN index, full-text engine, query
planner, gateway route, or distributed retrieval service.
`AssembleRetrievalContextRequest` builds on that hybrid path to produce
bounded, citation-ready `RetrievalContextBlock` values with chunk and
document identity, checksums, score metadata, clipped text, and total
text budget accounting. This remains a local worker/playbook-shaped
fixture; it does not invoke an LLM, render prompt templates, open a
network API, or add a persistent retrieval daemon.
`RetrievalContextRequestPayload` and `RetrievalContextResultPayload`
add versioned JSON byte codecs for those local worker/playbook
boundaries, rejecting malformed payloads and unsupported versions before
execution or handoff. They are not an RPC protocol, Arrow Flight
endpoint, gateway route, prompt engine, or production retrieval API.
`LocalRetrievalSearchService::execute_context_payload` completes the
local handoff loop by decoding a request payload, assembling context
from replayed state, and returning an encoded result payload. This is an
in-process worker/playbook executor, not a daemon or network endpoint.
`RetrievalContextPayloadExecutorConfig` bounds that local handoff with
positive request/result byte limits. Oversized request payloads are
rejected before decode, and oversized encoded result payloads are
rejected before returning bytes.
`RetrievalContextPayloadScope` adds an optional local tenant/namespace
guard for worker/playbook execution, rejecting decoded request payloads
whose embedded scope does not match the expected execution scope. This
is not production IAM, an ACL engine, or gateway authorization.
`RetrievalContextPayloadExecutionSummary` adds a redacted local
execution summary for the same handoff, reporting request/result byte
counts, context block count, total text chars, truncation status, and
whether a local scope guard was required. It intentionally excludes
tenant IDs, namespace values, query text, chunk text, tokens, vectors,
payload bytes, object paths, and principals; it is metrics/audit
metadata for local worker/playbook tests, not a logging sink or
production policy surface.
`RetrievalContextPayloadExecutionReceiptPayload` wraps that redacted
summary in a versioned JSON byte codec so future event-log/audit
plumbing has a durable receipt shape. It still contains only summary
fields and does not publish events, emit logs, expose a network API, or
carry retrieval-sensitive content. Receipt encode/decode validates
positive request/result byte counts and rejects text chars without a
context block.
`RetrievalContextPayloadExecution::encode_receipt_payload` gives local
worker/playbook tests a direct way to produce those receipt bytes from
an execution result without reassembling the codec wrapper.
`RetrievalContextPayloadExecutionArtifacts` is the bounded local
handoff shape for returning result payload bytes together with redacted
receipt payload bytes. `RetrievalContextPayloadExecutorConfig` now also
limits receipt payload bytes before artifact helpers return. Artifact
validation decodes the receipt and rejects empty payloads or result-byte
length mismatches.
`RetrievalContextPayloadExecutionReceiptEventPayload` wraps the
validated receipt bytes in a versioned JSON event envelope with the
stable subject `ehdb.retrieval.context.execution.receipt`. This is a
local stream-ready payload shape for future EHDB audit/event plumbing;
it does not publish to a stream, log, expose a service, or carry result
payload/context bytes.
`RetrievalContextReceiptEventStreamTarget` can build and explicitly
create the receipt event stream with caller-selected retention before
publication. Stream setup is caller-driven; publish helpers do not
auto-create streams.
`RetrievalContextReceiptEventStreamTarget` and
`RetrievalContextReceiptEventStreamLog` add an explicit local publisher
contract for worker/playbook tests that choose to write that event
payload into an EHDB stream log. The caller supplies tenant, namespace,
stream, mutable stream log, and transaction id; no background task,
gateway path, or automatic publication is introduced.
`RetrievalContextReceiptEventStreamRecord` and the read-side
`RetrievalContextReceiptEventStreamReadLog` replay validated receipt
event stream records from caller-supplied local logs. Replay validates
the stable subject and event payload while preserving stream sequence
and transaction id for local audit assertions; it does not start a
consumer loop, subscription, network API, or service process.
`RetrievalContextReceiptEventDurableConsumerLog` adds explicit local
durable-consumer helpers for creating a consumer, replaying pending
receipt events for that consumer, and acking a receipt event sequence.
Consumer state is caller-driven and local-log backed; there is still no
background consumer, scheduler, gateway path, or persistent process.

## Replay Reference

`ehdb-reference` applies replayed `TransactionRecord` values to the
local reference catalogs. Transaction mutations carry enough durable
facts to rebuild catalog tables, stream records, retrieval documents and
embeddings, system WASM library bindings, and object replica inventory
from the log alone.

This keeps the reference implementation aligned with the NoETL rule
that the event/transaction log is the source of truth. A replay mismatch
such as an unexpected stream sequence fails deterministically instead of
being silently repaired.

`LocalReferenceRuntime` combines the local JSONL transaction log with
the replay applier. It previews and applies a transaction to cloned
reference state before the durable append; if projection fails, the JSONL
log is not advanced. Reopening the runtime rebuilds the same local
reference state from replay.

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
cargo bench -p ehdb-catalog --bench snapshots
cargo bench -p ehdb-storage --bench local_store
cargo bench -p ehdb-reference --bench local_runtime
cargo bench -p ehdb-service --bench local_scan_service
cargo bench -p ehdb-transaction --bench reference_models
```

Current reference benchmark baseline on the initial local models:

| Benchmark | Workload | Baseline |
|---|---|---|
| `local_arrow_scan_service/filter_project_latest_100` | 100 service-boundary latest-snapshot scans with equality filter and two-column projection | ~12.0 ms |
| `local_arrow_scan/filter_project_latest_100` | 100 verified latest-snapshot scans with equality filter and two-column projection | ~13.0 ms |
| `local_arrow_ipc_table/write_read_10` | 10 Arrow IPC write + catalog snapshot + verified read cycles | ~111 ms |
| `local_replication_executor/register_25` | 25 verified source reads + fsynced replica-registration transactions + reopen | ~159 ms |
| `replication_plan_from_registry_1000` | 1000 three-target replication plans from registry state | ~3.77 ms |
| `replication_plan_1000` | 1000 three-target replication plans | ~2.89 ms |
| `replica_registry_register_1000` | 1000 object replica registrations | ~1.09 ms |
| `placement_policy_validate_1000` | 1000 three-target placement policy validations | ~1.36 ms |
| `catalog_commit_snapshots_1000` | 1000 catalog snapshot commits + latest lookup | ~2.03 ms |
| `local_object_store/put_get_verified_100` | 100 immutable 4 KiB local object puts + verified reads | ~15.6 ms |
| `stream_publish_replay_1000` | 1000 stream publishes + full replay | ~640 us |
| `transaction_append_replay_1000` | 1000 replay-complete transaction appends + full replay | ~1.15 ms |
| `local_reference_runtime/append_reopen_100` | create stream + 100 projection-validated fsynced transaction appends + reopen + replay | ~543 ms |
| `local_transaction_jsonl/append_reopen_100` | 100 fsynced replay-complete JSONL appends + reopen + full replay | ~461 ms |
| `local_stream_jsonl/publish_reopen_100` | 100 fsynced stream publishes + reopen + full replay | ~464 ms |

## Design

The design source of truth lives in the project wiki:

- https://github.com/noetl/ehdb/wiki
- https://github.com/noetl/ehdb/wiki/Architecture
- https://github.com/noetl/ehdb/wiki/Roadmap
