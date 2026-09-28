//! **M3 — the configuration surface for bounded-staleness reads.**
//!
//! `ReadConsistency`, `resolve_visibility` and `admits` have existed for
//! months. What did not exist was any way to *configure* them: both
//! `NOETL_EHDB_READ_CONSISTENCY` and `NOETL_EHDB_MAX_STALENESS_MS` occurred
//! **zero** times in this repository and in `noetl/server`, while downstream
//! design documents cited them as existing platform knobs.
//!
//! ⚠ That is the fleet's recurring shape: a type that exists is not a path
//! that runs. These tests assert the whole chain — env → parse → `AxisConfig`
//! → `resolve_visibility` → a visibility plan that actually differs.

use ehdb_core::plan::{
    resolve_visibility, AxisConfig, Enforcement, Locality, ReadConsistency, ReadLocality,
    SurvivalGoal,
};

fn cfg(consistency: ReadConsistency) -> AxisConfig {
    AxisConfig {
        locality: Locality::undeclared(),
        survival: SurvivalGoal::default(),
        enforcement: Enforcement::default(),
        read_locality: ReadLocality::Owner,
        consistency,
    }
}

// ------------------------------------------------------------ parsing

/// Unset means `Strong` — today's behaviour, unchanged by this existing.
#[test]
fn absent_configuration_is_todays_behaviour() {
    assert_eq!(
        ReadConsistency::parse(None, None),
        Ok(ReadConsistency::Strong)
    );
    assert_eq!(
        ReadConsistency::parse(Some(""), None),
        Ok(ReadConsistency::Strong)
    );
    assert_eq!(
        ReadConsistency::parse(Some("strong"), None),
        Ok(ReadConsistency::Strong)
    );
    // A stray bound on `strong` is ignored, not an error: it is meaningless
    // there, and refusing would break a deployment that sets both defensively.
    assert_eq!(
        ReadConsistency::parse(Some("strong"), Some("500")),
        Ok(ReadConsistency::Strong)
    );
}

#[test]
fn bounded_and_exact_carry_their_millisecond_bound() {
    assert_eq!(
        ReadConsistency::parse(Some("bounded"), Some("250")),
        Ok(ReadConsistency::Bounded {
            max_staleness_millis: 250
        })
    );
    assert_eq!(
        ReadConsistency::parse(Some(" BOUNDED "), Some(" 250 ")),
        Ok(ReadConsistency::Bounded {
            max_staleness_millis: 250
        }),
        "case and surrounding whitespace must not change the meaning"
    );
    assert_eq!(
        ReadConsistency::parse(Some("exact"), Some("1700000000000")),
        Ok(ReadConsistency::Exact {
            at_millis: 1_700_000_000_000
        })
    );
}

/// ⚠⚠ **A malformed relaxation is an ERROR, never a silent `Strong`.**
///
/// `ReadLocality::parse` maps a typo to `Owner`, and that is correct there —
/// the fallback is strictly safer. Inverted here it would be a defect: the
/// operator asked to relax freshness, and silently handing back the strict
/// setting is a performance cliff with no diagnostic. Inventing a budget would
/// be worse still — the system choosing its own correctness bound.
#[test]
fn a_malformed_relaxation_refuses_instead_of_defaulting() {
    for (level, bound, why) in [
        (Some("bounded"), None, "no bound at all"),
        (Some("bounded"), Some(""), "empty bound"),
        (Some("bounded"), Some("soon"), "non-numeric bound"),
        (Some("bounded"), Some("-1"), "negative bound"),
        (Some("bounded"), Some("1.5"), "fractional bound"),
        (Some("exact"), None, "exact without a timestamp"),
        (Some("eventual"), Some("100"), "a level that does not exist"),
    ] {
        let got = ReadConsistency::parse(level, bound);
        assert!(
            got.is_err(),
            "{why}: expected a refusal, got {got:?} — a silent fallback here \
             hands back a DIFFERENT freshness contract than the one configured"
        );
    }
}

/// The refusal has to name the offending value, or an operator cannot act on it.
#[test]
fn the_refusal_names_what_was_wrong() {
    let e = ReadConsistency::parse(Some("bounded"), Some("soon")).unwrap_err();
    assert!(
        e.contains("soon"),
        "the message must quote the bad value: {e}"
    );
    let e2 = ReadConsistency::parse(Some("eventual"), None).unwrap_err();
    assert!(
        e2.contains("eventual"),
        "the message must quote the bad level: {e2}"
    );
    assert!(
        e2.contains("strong") && e2.contains("bounded") && e2.contains("exact"),
        "and list what IS accepted: {e2}"
    );
}

// ------------------------------------------- the chain, end to end

/// ⭐ **The point of the milestone.** Configuration must reach a visibility
/// plan that actually differs — not merely parse into a different enum.
#[test]
fn configuration_changes_the_visibility_plan() {
    let now = 1_000_000u64;

    let strong = resolve_visibility(&cfg(ReadConsistency::parse(None, None).unwrap()), now);
    let bounded = resolve_visibility(
        &cfg(ReadConsistency::parse(Some("bounded"), Some("250")).unwrap()),
        now,
    );
    let exact = resolve_visibility(
        &cfg(ReadConsistency::parse(Some("exact"), Some("999000")).unwrap()),
        now,
    );

    eprintln!("  strong={strong:?}\n  bounded={bounded:?}\n  exact={exact:?}");

    // Strong admits everything the owner has: no freshness predicate.
    assert_eq!(
        strong.require_closed_ts, None,
        "strong must not demand a closed ts"
    );
    assert_eq!(strong.floor_hlc, None);

    // Bounded demands a closed timestamp exactly `max_staleness` behind now.
    assert_eq!(
        bounded.require_closed_ts,
        Some(now - 250),
        "bounded must demand now - budget"
    );
    assert_eq!(bounded.floor_hlc, None, "bounded sets no floor");

    // Exact pins both.
    assert_eq!(exact.require_closed_ts, Some(999_000));
    assert_eq!(exact.floor_hlc, Some(999_000));

    // ⚠ The three must be mutually distinct, or "it parsed" would be the only
    // thing proven.
    assert_ne!(strong, bounded);
    assert_ne!(bounded, exact);
    assert_ne!(strong, exact);
}

/// ⚠ The budget must be the number configured, not a constant that happens to
/// look right at one value. Two budgets, two different plans.
#[test]
fn the_configured_budget_is_the_one_used() {
    let now = 50_000u64;
    let a = resolve_visibility(
        &cfg(ReadConsistency::parse(Some("bounded"), Some("100")).unwrap()),
        now,
    );
    let b = resolve_visibility(
        &cfg(ReadConsistency::parse(Some("bounded"), Some("900")).unwrap()),
        now,
    );
    eprintln!(
        "  budget 100 -> {:?}   budget 900 -> {:?}",
        a.require_closed_ts, b.require_closed_ts
    );
    assert_eq!(a.require_closed_ts, Some(49_900));
    assert_eq!(b.require_closed_ts, Some(49_100));
    assert_ne!(a, b, "a constant would make these equal");
}

/// A budget larger than `now` must saturate rather than wrap.
#[test]
fn an_oversized_budget_saturates_instead_of_wrapping() {
    let p = resolve_visibility(
        &cfg(ReadConsistency::parse(Some("bounded"), Some("999999")).unwrap()),
        1_000,
    );
    assert_eq!(
        p.require_closed_ts,
        Some(0),
        "a budget beyond `now` must clamp at 0, not underflow to u64::MAX"
    );
}

// ------------------------------------------------- the single-reader guard

/// ⚠⚠ **Exactly one place may read these variables.**
///
/// They were absent for weeks while being cited as real. The opposite failure
/// is just as bad: two readers, drifting, so a deployment gets one answer on
/// the write path and another on the read path. `read_consistency_from_env` is
/// the only reader, and this fails the build if a second appears.
#[test]
fn the_env_vars_have_exactly_one_reader() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let mut readers = Vec::new();
    let mut scanned = 0usize;
    let mut stack = vec![root.join("crates")];
    while let Some(dir) = stack.pop() {
        for e in std::fs::read_dir(&dir).expect("readable") {
            let p = e.expect("entry").path();
            if p.is_dir() {
                if p.file_name().is_some_and(|n| n == "target") {
                    continue;
                }
                stack.push(p);
            } else if p.extension().is_some_and(|x| x == "rs") {
                let src = std::fs::read_to_string(&p).unwrap_or_default();
                scanned += 1;
                // A READ is `env::var` naming either const. Prose and the const
                // declarations themselves must not count — a comment mentioning
                // a variable is how a scan over-reports.
                for (n, line) in src.lines().enumerate() {
                    let l = line.trim();
                    if l.starts_with("//") || l.starts_with("pub const") {
                        continue;
                    }
                    if l.contains("env::var")
                        && (l.contains("READ_CONSISTENCY_ENV")
                            || l.contains("MAX_STALENESS_MS_ENV"))
                    {
                        // ⚠⚠ file:LINE, not the file. The first version of this
                        // guard pushed the path and then de-duplicated, so it
                        // counted FILES rather than read sites — and a second
                        // reader added to this very file was invisible. The
                        // planted-defect battery caught it; the guard as
                        // written could not have.
                        readers.push(format!(
                            "{}:{}",
                            p.strip_prefix(root).unwrap().display(),
                            n + 1
                        ));
                    }
                }
            }
        }
    }
    readers.sort();
    readers.dedup(); // identical file:line only; distinct sites survive
                     // ⚠ Assert the extraction before asserting about it.
    assert!(
        scanned >= 50,
        "implausibly few .rs files scanned ({scanned}) — this guard would pass vacuously"
    );
    eprintln!("read-consistency env: {scanned} .rs files scanned, readers = {readers:?}");
    // ⚠⚠ The invariant is ONE READ PER VARIABLE, ALL IN ONE FILE — not "one
    // line". The legitimate reader calls `env::var` twice, once per variable,
    // on adjacent lines, so a naive one-site rule fails on correct code. And
    // the first version of this guard deduplicated by FILE, which made a second
    // reader added to this very file invisible. Both were caught by the planted
    // battery, neither by reading the guard.
    let files: std::collections::BTreeSet<&str> = readers
        .iter()
        .map(|r| r.split(':').next().unwrap())
        .collect();
    assert_eq!(
        files.len(),
        1,
        "the consistency env vars must be read in exactly one file, found {files:?}"
    );
    assert!(files.iter().next().unwrap().contains("region_routing.rs"));
    assert_eq!(
        readers.len(),
        2,
        "expected exactly 2 read sites (one per variable) in one function, found \
         {}: {readers:?} — a third site means a second reader, and two readers \
         is how one of them drifts",
        readers.len()
    );
}
