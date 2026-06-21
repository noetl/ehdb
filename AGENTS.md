# EHDB AI Instructions

## Scope

Applies to the entire `ehdb` repository.

## Mission

EHDB is the Event Horizon Database for NoETL: an Arrow-native
distributed database and catalog platform for operational metadata,
catalog state, and historical analytical data in multi-cloud object
storage.

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
- Keep object data immutable by default.
- Add wiki updates with public architecture or workflow changes.
- Avoid secrets, tokens, and tenant-sensitive values in examples.
- Containerized changes must validate on local kind before GKE.
