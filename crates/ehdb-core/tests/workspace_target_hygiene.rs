//! **Every declared build target must resolve to a git-tracked file.**
//!
//! # The failure this exists to catch
//!
//! A manifest can declare a target whose source is not in the repository. The
//! crate then builds perfectly on the machine that wrote it — the file is on
//! disk — and fails on a clean checkout with `file ... does not exist`.
//!
//! It happened here. `.gitignore` carries `**/[Bb]in/*` from the Visual Studio
//! template, which matches Cargo's `src/bin/` convention, so
//! `crates/ehdb-signal-mesh/src/bin/demo.rs` was silently excluded:
//!
//! * the crate built locally, because the file was on disk;
//! * `git add <dir>` skipped it **without a word**;
//! * `git status` never showed it, because ignored files are not listed;
//! * `cargo fmt --all --check` on the runner was the first thing in the world
//!   that could notice, and it failed the build.
//!
//! ⚠ **Every local signal was green.** That is the defining property of this
//! failure class and the reason it needs a guard rather than care: there is no
//! amount of attention that makes an ignored file visible in `git status`.
//!
//! # Why this lives in `ehdb-core`
//!
//! The check is about the workspace, not about any one crate, and `ehdb-core`
//! is the crate the workspace is rooted on. A dedicated hygiene crate would be
//! more honest about ownership and less honest about cost.
//!
//! # Cost
//!
//! Two subprocesses (`cargo metadata --no-deps`, `git ls-files`), no network.

use std::collections::BTreeSet;
use std::path::Path;
use std::process::Command;

/// A declared build target: which crate, which kind, and the source it names.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct DeclaredTarget {
    pub package: String,
    pub name: String,
    pub kind: String,
    /// Path relative to the workspace root, forward-slashed.
    pub src_path: String,
}

/// **The check, as a pure function.**
///
/// Separated from the data-gathering on purpose: a guard whose logic can only
/// run against the live repository cannot be tested for the case it is supposed
/// to catch, and would silently always-pass if the gathering broke.
pub fn untracked_targets<'a>(
    targets: &'a [DeclaredTarget],
    tracked: &BTreeSet<String>,
) -> Vec<&'a DeclaredTarget> {
    targets
        .iter()
        .filter(|t| !tracked.contains(&t.src_path))
        .collect()
}

fn workspace_root() -> String {
    // The test runs from its crate directory; the manifest above it is the
    // workspace. `cargo metadata` resolves that without hard-coding `..`.
    let out = Command::new(env!("CARGO"))
        .args(["metadata", "--format-version", "1", "--no-deps"])
        .output()
        .expect("cargo metadata must run — if cargo is missing the guard cannot be trusted");
    assert!(
        out.status.success(),
        "cargo metadata failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let v: serde_json::Value =
        serde_json::from_slice(&out.stdout).expect("cargo metadata emits JSON");
    v["workspace_root"]
        .as_str()
        .expect("metadata carries workspace_root")
        .to_string()
}

/// Every target declared by every workspace member.
fn declared_targets(root: &str) -> Vec<DeclaredTarget> {
    let out = Command::new(env!("CARGO"))
        .args(["metadata", "--format-version", "1", "--no-deps"])
        .output()
        .expect("cargo metadata must run");
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).expect("JSON");
    let mut targets = Vec::new();
    for pkg in v["packages"].as_array().expect("packages is an array") {
        let pkg_name = pkg["name"].as_str().unwrap_or("?").to_string();
        for t in pkg["targets"].as_array().expect("targets is an array") {
            let abs = t["src_path"].as_str().unwrap_or_default();
            // Anything outside the workspace root belongs to a registry
            // dependency and is not ours to police.
            let Ok(rel) = Path::new(abs).strip_prefix(root) else {
                continue;
            };
            targets.push(DeclaredTarget {
                package: pkg_name.clone(),
                name: t["name"].as_str().unwrap_or("?").to_string(),
                kind: t["kind"]
                    .as_array()
                    .and_then(|k| k.first())
                    .and_then(|k| k.as_str())
                    .unwrap_or("?")
                    .to_string(),
                src_path: rel.to_string_lossy().replace('\\', "/"),
            });
        }
    }
    targets
}

/// Files git is tracking, relative to the workspace root.
fn tracked_files(root: &str) -> BTreeSet<String> {
    let out = Command::new("git")
        .current_dir(root)
        .args(["ls-files"])
        .output()
        .expect("git ls-files must run — without it the guard cannot be trusted");
    assert!(
        out.status.success(),
        "git ls-files failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// ⚠⚠ **The guard.** A declared target pointing at an untracked file breaks
/// every clean checkout while looking fine locally.
#[test]
fn every_declared_target_resolves_to_a_tracked_file() {
    let root = workspace_root();
    let targets = declared_targets(&root);
    let tracked = tracked_files(&root);

    // ⚠ Assert the extraction before asserting about it. Both inputs come from
    // subprocesses, and either returning nothing would make the check below
    // pass over an empty set — a green result computed from zero bytes.
    assert!(
        targets.len() >= 20,
        "implausibly few declared targets ({}) — `cargo metadata` is not being \
         read correctly, and this guard would pass vacuously",
        targets.len()
    );
    assert!(
        tracked.len() >= 100,
        "implausibly few tracked files ({}) — `git ls-files` is not being read \
         correctly, and every target would look untracked",
        tracked.len()
    );

    let bad = untracked_targets(&targets, &tracked);
    assert!(
        bad.is_empty(),
        "{} declared target(s) point at a file git is not tracking. A clean \
         checkout will fail to build even though it works here.\n{}\n\
         Fix the .gitignore rule that excludes the source — do NOT `git add -f`, \
         which leaves the trap set for the next crate.",
        bad.len(),
        bad.iter()
            .map(|t| format!("  {} [{}] {} -> {}", t.package, t.kind, t.name, t.src_path))
            .collect::<Vec<_>>()
            .join("\n")
    );

    // Print the denominator, so a reader can sanity-check the scope rather than
    // trusting a bare "ok".
    eprintln!(
        "workspace target hygiene: {} declared targets checked against {} tracked files",
        targets.len(),
        tracked.len()
    );
}

/// ⚠ **The discriminating control.**
///
/// A guard that has never produced a finding is indistinguishable from one that
/// cannot. This reproduces the exact bug — one target's source missing from the
/// tracked set — and asserts the guard reports **precisely that one**.
///
/// Done against a synthetic tracked-set rather than by touching the index: a
/// test that ran `git rm --cached` would mutate the repository it is inspecting
/// and fail differently depending on what else was staged.
#[test]
fn the_guard_reports_exactly_the_untracked_target() {
    let root = workspace_root();
    let targets = declared_targets(&root);
    let mut tracked = tracked_files(&root);

    // Clean baseline first, or "it found the one" proves nothing.
    assert!(
        untracked_targets(&targets, &tracked).is_empty(),
        "baseline must be clean before planting"
    );

    // Plant: forget one real target's source, exactly as .gitignore did.
    let victim = targets
        .iter()
        .find(|t| t.src_path.contains("/src/bin/"))
        .or_else(|| targets.first())
        .expect("the workspace declares at least one target")
        .clone();
    assert!(
        tracked.remove(&victim.src_path),
        "the victim must have been tracked to begin with"
    );

    let bad = untracked_targets(&targets, &tracked);
    assert_eq!(
        bad.len(),
        1,
        "planting one untracked source must yield exactly one finding, got {}: {:?}",
        bad.len(),
        bad
    );
    assert_eq!(
        bad[0].src_path, victim.src_path,
        "the finding must name the planted target, not some other one"
    );

    // Restoring it must clear the finding — so the guard is reacting to the
    // plant and not to something ambient.
    tracked.insert(victim.src_path.clone());
    assert!(
        untracked_targets(&targets, &tracked).is_empty(),
        "restoring the source must clear the finding"
    );
}

/// The `src/bin/` case specifically, since that is the rule that bit us.
///
/// Not a duplicate of the guard above: this asserts the *shape* is covered, so
/// a future refactor that stopped enumerating bin targets would fail here with
/// a message about bins rather than silently narrowing the guard's scope.
#[test]
fn cargo_src_bin_targets_are_in_scope() {
    let root = workspace_root();
    let targets = declared_targets(&root);
    let bins: Vec<_> = targets
        .iter()
        .filter(|t| t.src_path.contains("/src/bin/"))
        .collect();
    assert!(
        !bins.is_empty(),
        "no `src/bin/` target is being enumerated. Either the workspace has \
         none (then delete this test), or the enumeration has narrowed and the \
         exact case that broke CI is no longer covered"
    );
    let tracked = tracked_files(&root);
    for b in &bins {
        assert!(
            tracked.contains(&b.src_path),
            "src/bin target {} is untracked — the .gitignore negation for \
             Cargo's src/bin convention has regressed",
            b.src_path
        );
    }
}
