# The A2A / ReAct signal mesh lives at `noetl/signal-mesh`

**Moved 2026-09-21.** Three things left this repository together:

| was here | now |
| :-- | :-- |
| `crates/ehdb-signal-mesh/` | the whole of <https://github.com/noetl/signal-mesh> (crate renamed `signal-mesh`, at the repo root) |
| `docs/architecture/a2a-signal-mesh-blueprint.md` | [`docs/architecture/a2a-signal-mesh-blueprint.md`](https://github.com/noetl/signal-mesh/blob/main/docs/architecture/a2a-signal-mesh-blueprint.md) — the team blueprint, and the source of truth for the wiki copy |
| `docs/spec/a2a-react-signal-mesh.md` | [`docs/spec/a2a-react-signal-mesh.md`](https://github.com/noetl/signal-mesh/blob/main/docs/spec/a2a-react-signal-mesh.md) — the implementation/proof spec, including §11 *"what the POC does NOT prove"* |

## Why it moved

The mesh is a **consumer** of EHDB, not a part of it. It folds an
EHDB-shaped event log and uses `ehdb-core`'s `ReadConsistency`; nothing in
EHDB uses it back. Keeping it in this workspace meant every EHDB build
compiled a POC that no EHDB crate depends on, and every EHDB release
tagged a component with its own lifecycle.

It now depends on this repository the way any other consumer would — as a
library, pinned by tag:

```toml
ehdb-core = { git = "https://github.com/noetl/ehdb", tag = "v0.3.0" }
```

That pin is the load-bearing consequence of the split: a change to
`ehdb-core::plan::ReadConsistency` here no longer breaks the mesh's build
silently. It breaks it at the tag bump, deliberately, in that repository.

## History

The six commits that built the crate and the two documents were carried
across with `git filter-repo --path crates/ehdb-signal-mesh --path
docs/spec/a2a-react-signal-mesh.md --path
docs/architecture/a2a-signal-mesh-blueprint.md --path-rename
crates/ehdb-signal-mesh/:`, so authorship and dates are the originals,
not a squashed import. They remain in this repository's history too —
`git log -- crates/ehdb-signal-mesh` still resolves.

## What stayed behind, on purpose

`crates/ehdb-core/tests/workspace_target_hygiene.rs`. The guard was
written *because* of this crate — the `.gitignore` rule `**/[Bb]in/*`
silently swallowed `crates/ehdb-signal-mesh/src/bin/demo.rs`, every local
signal stayed green, and CI was the first thing in the world that could
notice. The guard protects the workspace, not that crate, so it stays
here; `crates/ehdb-reference/src/bin/ehdb-local-reference.rs` is the
`src/bin` target it now exercises. A copy adapted to a single-crate layout
runs in the new repository as `tests/target_hygiene.rs`.
