//! **HLC properties** (multi-region spec M2). Clock only — nothing here
//! stamps a record, and no write path is involved.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use ehdb_core::hlc::{Hlc, HlcClock};

/// A controllable wall clock.
fn fake() -> (Arc<AtomicU64>, Box<dyn Fn() -> u64 + Send + Sync>) {
    let t = Arc::new(AtomicU64::new(1_000));
    let r = t.clone();
    (t, Box::new(move || r.load(Ordering::SeqCst)))
}

#[test]
fn parts_round_trip_and_order_by_physical_then_logical() {
    let a = Hlc::from_parts(1_700_000_000_000, 0);
    let b = Hlc::from_parts(1_700_000_000_000, 1);
    let c = Hlc::from_parts(1_700_000_000_001, 0);
    assert!(a < b && b < c, "must order physical-major, logical-minor");
    assert_eq!(a.physical_millis(), 1_700_000_000_000);
    assert_eq!(b.logical(), 1);
    assert_eq!(Hlc::from_raw(c.as_u64()), c);
}

/// 48 bits of ms is ~8,900 years; assert the layout actually holds a real
/// present-day timestamp without truncation.
#[test]
fn a_real_timestamp_survives_the_48_bit_physical_field() {
    let now_ms = 1_789_000_000_000u64; // ~2026
    assert_eq!(Hlc::from_parts(now_ms, 7).physical_millis(), now_ms);
}

#[test]
fn now_is_strictly_increasing_within_one_millisecond() {
    let (_t, src) = fake();
    let c = HlcClock::with_source(src);
    let mut prev = c.now();
    for _ in 0..1_000 {
        let next = c.now();
        assert!(next > prev, "{next:?} must exceed {prev:?}");
        prev = next;
    }
    assert_eq!(prev.physical_millis(), 1_000);
    assert_eq!(prev.logical(), 1_000);
}

/// ⭐ **The backwards-clock case.** NTP steps back, or a VM resumes. The clock
/// must keep issuing increasing values rather than re-issuing old ones.
#[test]
fn now_never_goes_backwards_when_the_wall_clock_does() {
    let (t, src) = fake();
    let c = HlcClock::with_source(src);
    t.store(5_000, Ordering::SeqCst);
    let before = c.now();
    assert_eq!(before.physical_millis(), 5_000);

    t.store(4_000, Ordering::SeqCst); // a 1-second step BACKWARDS
    let after = c.now();
    assert!(
        after > before,
        "clock went backwards: {after:?} <= {before:?} after a wall-clock step back"
    );
    assert_eq!(after.physical_millis(), 5_000);
    assert_eq!(after.logical(), 1);
}

#[test]
fn a_forward_jump_is_adopted_and_resets_the_logical_counter() {
    let (t, src) = fake();
    let c = HlcClock::with_source(src);
    let _ = c.now();
    let _ = c.now();
    assert_eq!(c.last().logical(), 1);
    t.store(9_999, Ordering::SeqCst);
    let jumped = c.now();
    assert_eq!(jumped.physical_millis(), 9_999);
    assert_eq!(
        jumped.logical(),
        0,
        "a real tick forward resets the tie-break"
    );
}

/// ⚠ The counter must not wrap. 65,536 events in one millisecond is the limit;
/// the next one steps the physical field rather than re-issuing.
#[test]
fn the_logical_counter_steps_physical_instead_of_wrapping() {
    let (_t, src) = fake();
    let c = HlcClock::seeded_from(Hlc::from_parts(1_000, 65_535), src);
    let next = c.now();
    assert!(
        next > Hlc::from_parts(1_000, 65_535),
        "must not wrap to logical 0 at 1_000"
    );
    assert_eq!(next.physical_millis(), 1_001);
    assert_eq!(next.logical(), 0);
}

/// ⭐ **Restart.** A fresh process seeded from the durable high-water mark
/// must not re-issue timestamps the previous process used, even though its
/// wall clock reads lower.
#[test]
fn a_restart_seeded_from_the_log_does_not_reissue() {
    let (t1, src1) = fake();
    t1.store(8_000, Ordering::SeqCst);
    let first = HlcClock::with_source(src1);
    let highest = (0..5).map(|_| first.now()).last().unwrap();

    let (t2, src2) = fake();
    t2.store(7_500, Ordering::SeqCst);
    let restarted = HlcClock::seeded_from(highest, src2);
    let after = restarted.now();
    assert!(
        after > highest,
        "restart re-issued a used timestamp: {after:?} <= {highest:?}"
    );
}

/// ⚠ **Negative control for the test above.** An UNSEEDED clock on a
/// backwards wall clock *does* regress. This is what makes `seeded_from`
/// load-bearing rather than decorative — if this test ever fails, the restart
/// test above is passing for the wrong reason.
#[test]
fn an_unseeded_restart_does_reissue_which_is_why_seeding_exists() {
    let (t1, src1) = fake();
    t1.store(8_000, Ordering::SeqCst);
    let first = HlcClock::with_source(src1);
    let highest = (0..5).map(|_| first.now()).last().unwrap();

    let (t2, src2) = fake();
    t2.store(7_500, Ordering::SeqCst);
    let unseeded = HlcClock::with_source(src2); // NOT seeded
    let after = unseeded.now();
    assert!(
        after < highest,
        "expected an unseeded clock to regress; if it does not, seeding is untested"
    );
}

#[test]
fn observing_a_higher_remote_advances_us_past_it() {
    let (_t, src) = fake();
    let c = HlcClock::with_source(src);
    let remote = Hlc::from_parts(50_000, 3);
    c.observe(remote);
    assert!(
        c.now() > remote,
        "a message received must order after it was sent"
    );
}

#[test]
fn observing_a_lower_remote_does_not_regress_us() {
    let (t, src) = fake();
    t.store(50_000, Ordering::SeqCst);
    let c = HlcClock::with_source(src);
    let ours = c.now();
    c.observe(Hlc::from_parts(10, 0));
    assert!(c.now() > ours);
}

/// `#[serde(transparent)]`: an Hlc is a bare integer in JSON, not an object.
/// A record carrying one costs one number, and the representation cannot
/// drift with field renames.
#[test]
fn serialises_as_a_bare_integer() {
    let h = Hlc::from_parts(1_700_000_000_000, 5);
    let json = serde_json::to_string(&h).unwrap();
    assert_eq!(json, h.as_u64().to_string());
    assert_eq!(serde_json::from_str::<Hlc>(&json).unwrap(), h);
}

#[test]
fn concurrent_callers_never_collide() {
    let (_t, src) = fake();
    let c = Arc::new(HlcClock::with_source(src));
    let mut handles = Vec::new();
    for _ in 0..8 {
        let c = c.clone();
        handles.push(std::thread::spawn(move || {
            (0..500).map(|_| c.now()).collect::<Vec<_>>()
        }));
    }
    let mut all: Vec<Hlc> = handles
        .into_iter()
        .flat_map(|h| h.join().unwrap())
        .collect();
    let total = all.len();
    all.sort();
    all.dedup();
    assert_eq!(
        all.len(),
        total,
        "two threads were issued the same timestamp"
    );
}

// ---------------------------------------------------------------------------
// Wire-encoding pin.
//
// ⚠ Added because a mutation battery found the gap: flipping LOGICAL_BITS
// from 16 to 15 left every other test in this file GREEN. All of them use
// `from_parts` and the accessors symmetrically, so they verify the layout is
// self-consistent and say nothing about what it actually is.
//
// That matters because the packed u64 is what lands on disk. A record written
// by a 16-bit binary and read by a 15-bit one decodes to a different
// timestamp -- no error, no panic, just a wrong answer. Exactly the format
// break C3 forbids, arriving through a constant rather than a struct field.
// ---------------------------------------------------------------------------

/// Pins the exact packed integer. If this fails, the on-disk meaning of every
/// stored Hlc has changed and a compat path is required -- do not "fix" it by
/// updating the expected value.
#[test]
fn the_packed_layout_is_48_bits_physical_over_16_bits_logical() {
    // 1000 << 16 | 5
    assert_eq!(Hlc::from_parts(1_000, 5).as_u64(), 65_536_005);
    // The boundary: the largest logical value, and the first value that must
    // carry into the physical field.
    assert_eq!(Hlc::from_parts(0, 65_535).as_u64(), 65_535);
    assert_eq!(Hlc::from_parts(1, 0).as_u64(), 65_536);
    // A present-day timestamp, spelled out.
    assert_eq!(
        Hlc::from_parts(1_789_000_000_000, 1).as_u64(),
        1_789_000_000_000u64 * 65_536 + 1
    );
}

/// The masks agree with the layout above: logical saturates at 65_535 and
/// never bleeds into the physical field.
#[test]
fn the_logical_field_is_exactly_16_bits_wide() {
    assert_eq!(Hlc::from_parts(7, 65_535).logical(), 65_535);
    assert_eq!(Hlc::from_parts(7, 65_535).physical_millis(), 7);
    // One past the mask wraps within the field, never into physical.
    assert_eq!(Hlc::from_parts(7, 65_536).physical_millis(), 7);
    assert_eq!(Hlc::from_parts(7, 65_536).logical(), 0);
}
