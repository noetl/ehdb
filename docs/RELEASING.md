# Releasing ehdb

## What a release is here

ehdb is a **library consumed by git tag** — not a crate on crates.io, not a container
image. `noetl/server` depends on it as:

```toml
ehdb-feed = { git = "https://github.com/noetl/ehdb", tag = "v0.4.5" }
ehdb-l0   = { git = "https://github.com/noetl/ehdb", tag = "v0.4.5" }
```

So the deliverable of a release is **a tag whose tree a consumer can actually build**, and
that is what `release-ehdb` proves.

⚠⚠ **Both pins must move together.** `ehdb-feed` depends on `ehdb-l0`, so pinning them at
different tags puts **two `ehdb-l0` versions in one dependency graph** — `D1EventLog` from
one is not `D1EventLog` from the other, and the type errors are bewildering.

## The tag is the version authority, not `Cargo.toml`

Every crate in this workspace is `version = "0.1.0"` and always has been; the published
versions are the `v0.4.x` tags. **Nothing asserts tag == `Cargo.toml`, and nothing should.**

`.releaserc.json` deliberately omits `@semantic-release/git`, so **nothing is ever pushed to
`main`**. That is not an oversight — it is what makes this repo immune to GH006:

> `@semantic-release/git` commits the version bump with `[skip ci]`. A required status check
> never runs on a `[skip ci]` commit, so it stays "expected" forever and the protected
> branch rejects the push. noetl/server hit this, and its `release.yml` carries the scar
> tissue: it used to assert tag == `Cargo.toml`, which held only *because* the bot pushed
> the bump first.

## How to cut a release

1. Merge a PR to `main` whose **merge commit subject** is a conventional commit. The subject
   is what semantic-release reads.

   | you write | it releases |
   | :-- | :-- |
   | `fix: …` | patch |
   | `feat: …` | minor |
   | `feat!: …` or `BREAKING CHANGE:` in the body | major |
   | `chore:` / `docs:` / `test:` / `refactor:` | **nothing** |
   | anything non-conventional | **nothing, silently** |

   That last row costs a cycle to discover. If a change needs to reach a consumer, it needs
   a release-triggering type.

2. **Merge commits, not squash.** These repos forbid squash, and the merge subject is what
   is read.

3. Read the tag back. Do not assume it:

   ```bash
   gh run watch --repo noetl/ehdb
   git fetch origin --tags && git tag --sort=-v:refname | head -1
   ```

## ⚠⚠ Releases are GATED OFF by default

The `release` job in `semantic-release.yml` runs only when the repository variable
**`EHDB_RELEASE_ENABLED`** is exactly `true`. Until an owner sets it, a push to `main`
reaches the workflow and does nothing.

That is deliberate. Cutting an ehdb tag moves the version `noetl/server` pins **for the
production event store**, and both of its pins must move together. It is a human decision,
not a side effect of a merge. The variable fails closed: unset, or an unavailable `vars`
context, both evaluate to "not enabled".

To exercise the pipeline without releasing, dispatch it with `dry_run` (the default):

```bash
gh workflow run semantic-release.yml --repo noetl/ehdb --field dry_run=true
```

### Why the gate does not make the automation unfalsifiable

A skipped job reports success, so the gate alone would leave "the release pipeline works" as
an untestable claim. The falsifiable half is the **`release-dryrun`** job in `ci.yml`: on
every pull request it runs the real plugin chain against the real history and **fails if
`analyzeCommits` never completes**.

It asserts on that step rather than on a zero exit on purpose — semantic-release exits **0**
when a branch "is not configured to release", which is exactly what a broken branch config
produces: a green run that examined nothing. Proven by planting a nonexistent branch in
`.releaserc.json` and confirming the guard fails.

## Why `release.yml` is dispatched explicitly

⚠⚠ **A tag pushed with `GITHUB_TOKEN` does not trigger workflows.** GitHub suppresses it to
prevent recursion. `release-ehdb` is configured on `push: tags: ['v*']` and would therefore
**never** fire for a semantic-release tag. noetl/server lost a crates.io publish to exactly
this and had to dispatch by hand.

So `semantic-release.yml` ends with an explicit `gh workflow run release.yml`, which is why
it needs `actions: write`.

## What `release-ehdb` verifies

- the tag has a leading `v` (refusing anything else);
- `cargo build --release --workspace` succeeds at the tag — the consumer's build;
- the three library artifacts exist **by name** (`libehdb_l0`, `libehdb_feed`,
  `libehdb_core`), with the count printed, because a loop over an empty list also exits 0;
- `cargo test --workspace` passes at the tag;
- the on-disk **`FORMAT_VERSION`** is read and recorded. It is a storage library's
  compatibility contract — the constant that refuses to open a store written by an
  incompatible build — so a silent change to it is the thing a consumer most needs to know
  about a new tag. The step fails loudly if the grep finds nothing, rather than reporting an
  empty version.

The result is attached to the GitHub Release as `build-manifest.txt`, carrying the tag, the
commit, the `FORMAT_VERSION`, the rustc version, and the two pin lines a consumer needs.

## Related

- `agents/rules/release-versioning.md` in noetl/ai-meta — the fleet-wide rule, including the
  2026-08-03 regression caused by a manual `Cargo.toml` bump racing semantic-release.
- noetl/ai-meta#455 — the north-star program whose P1–P6 work is waiting on a tag.
