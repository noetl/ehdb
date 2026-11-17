# Spec — manifest format changes: expand first, refuse what you cannot read

**Status:** normative specification. Applies to every change to what a durable
manifest (`Manifest`, `PartMeta`, `Bloom`, `ReplicaLocation`, …) serializes.
**Tracks:** [#439](https://github.com/noetl/ehdb/issues/439) (closed by
[#440](https://github.com/noetl/ehdb/pull/440), v0.15.1); first applied to the bloom
scheme change [#438](https://github.com/noetl/ehdb/pull/438) (v0.15.0) for
[noetl/ai-meta#517](https://github.com/noetl/ai-meta/issues/517).

## 1. Why this needs a rule

A manifest is JSON written by whichever binary sealed the last part, and read back by
whichever binary opens the store next: a later release, a rollback, or a replica on
another version. Two failures follow from that, and they look nothing alike:

- **Loud:** a reader cannot decode the manifest, and the open fails. The store is down
  until a compatible binary runs. That is recoverable.
- **Silent:** a reader cannot decode the manifest and does something else instead. Before
  v0.15.1, `load_durable_manifest` treated every decode error as a torn write and fell
  back to the newest versioned snapshot it *could* decode. On kind (2026-11-04), v0.14.12
  opened a v0.15.0 store holding scheme-2 parts that way:
  - it re-adopted the newer parts from local disk against the older snapshot;
  - it republished `LATEST` as a **different** manifest under a version that already
    existed.

  Reads stayed correct only because every part was local. A substrate-only part would
  have left the catalog with no error anywhere.

The silent case is the one that matters. Every rule below exists to keep a format change
in the loud case.

## 2. The rules

### 2.1 Expand first

A new field or encoding ships in two releases:

- **A — readers.** Every reader learns to decode the new shape. **Writers keep writing
  the old one byte for byte**, enforced by a golden test against a real
  prior-format manifest.
- **B — writers.** Only after **every** reader of a store runs release A (and has
  soaked) does anything write the new shape. Switch per store, with an environment
  variable on the writing process, so B is reversible by unsetting it.

Release B's rollback is to **stop writing** the new shape. A store that holds new-shape
data must never be opened by a binary that predates release A.

### 2.2 Tolerate unknown fields where the type nests into `Manifest`

`#[serde(deny_unknown_fields)]` on a type nested inside `Manifest` (`ReplicaLocation`,
`Bloom`, `EventRecord`) turns any field a newer writer adds into a decode failure of the
**whole manifest**. Those types tolerate unknown fields so that an additive change does
not need a release A at all. Required fields remain required: tolerance is not laxity.
`tests/manifest_forward_compat.rs` carries the negative control.

### 2.3 Refuse, never fall back, on a newer format (v0.15.1+)

`load_durable_manifest` distinguishes two kinds of undecodable copy:

| bytes | meaning | behaviour |
| :-- | :-- | :-- |
| not JSON (torn, truncated, garbage) | a crash mid-write | fall back to the newest decodable versioned snapshot |
| well-formed JSON that does not decode as `Manifest` | a **newer writer's format** | **refuse the open** with `EhdbError::Storage` naming the location, the version and #439 |

The refusal applies to:

- `LATEST` on **any** replica, even when another replica decodes;
- any versioned snapshot newer than the newest copy that decoded. Skipping it would roll
  the catalog back past a version a newer writer published.

`load_durable_manifest` is the **only** manifest decode site. A new decode site must call
`refuse_if_newer_format` on its error path, or it reintroduces #439.

⚠ This protects **binaries built from v0.15.1 on**. Nothing can change a binary that has
already shipped: v0.14.x still falls back silently on a scheme-2 manifest. That is why
2.1 forbids opening a release-B store with a pre-release-A binary, and why a rollback
target must be checked against this rule rather than assumed safe.

## 3. Checklist for a format change

1. Is the change additive on a type that tolerates unknown fields? Then it needs no
   release A. Otherwise plan A and B.
2. Release A: a golden test proves the writer's bytes are unchanged, and a test proves
   the reader decodes the new shape.
3. Before B, decode **every** live manifest of the target stores with the new reader. A
   stricter reader that refuses today's data is an outage.
4. Before B, pin the rollback image to a build at or above release A (and ≥ v0.15.1, so
   a later unknown format refuses rather than falls back).
5. Arm B one store at a time and confirm the new shape on the *newest* sealed parts. A
   false-negative check against part bytes is the positive control for any bloom
   change.

## 4. Applied: bloom scheme 2 on NoETL prod

| step | when | |
| :-- | :-- | :-- |
| A: v0.15.0 readers | 2026-11-06 | every reader, then a 29 h soak |
| refusal: v0.15.1 readers | 2026-11-07 | every reader; all five prod manifests were decoded with v0.15.1 first (checklist 3) |
| B: `EHDB_BLOOM_SCHEME=2` | 2026-11-07 16:09Z / 16:34Z | noetl-server embedded stores, then the cmdbus-writer's buses |

After the arm, prod showed no false negatives on either scheme. The newest legacy bus
parts measured 0.37% and 0.20% false positives, against 0 for scheme 2. The record is in
ai-meta `playbooks/517-floor-exact/README.md` and on the
[Bloom filters](https://github.com/noetl/ehdb/wiki/Bloom-Filters) wiki page.
