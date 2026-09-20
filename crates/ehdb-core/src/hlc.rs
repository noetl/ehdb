//! **Hybrid Logical Clock** (multi-region spec M2).
//!
//! ## Why this exists, and why it is not the snowflake id
//!
//! EHDB has two ordering facilities today and neither can order events across
//! regions:
//!
//! * **`global_sequence`** is assigned per engine (`ehdb-l0` `engine.rs`
//!   `self.global_sequence + 1`) and is gapless **only because a single writer
//!   serialises appends**. A second region's engine mints the same integers.
//!   It is a per-shard order, not a global one.
//! * **The snowflake id** (`noetl-server` `src/snowflake.rs`) is 41-bit ms ‖
//!   10-bit machine ‖ 12-bit sequence. It has **no logical component and no
//!   uncertainty bound**, so two ids from two machines are comparable only to
//!   the accuracy of two unsynchronised wall clocks.
//!
//! ⛔ **Do not reuse the snowflake timestamp as an HLC.** Conflating them
//! silently weakens both: the snowflake gains a correctness claim it cannot
//! support, and the HLC inherits a layout with no room for a logical counter.
//!
//! ## The shape
//!
//! 48 bits of physical milliseconds ‖ 16 bits of logical counter, packed into a
//! `u64` so it costs one column and compares with `<`.
//!
//! * 48 bits of ms ≈ 8,900 years from the Unix epoch — no wrap to design for.
//! * 16 bits of logical ⇒ 65,536 events per physical millisecond per node
//!   before the counter would need to borrow from the physical field. [`Hlc`]
//!   **refuses to wrap silently**: [`HlcClock::now`] steps the physical field
//!   forward instead, which keeps monotonicity at the cost of running slightly
//!   ahead of the wall clock. Returning a duplicate would be worse.
//!
//! ## What this module does NOT do
//!
//! It does not stamp anything. Nothing calls [`HlcClock::now`] on any write
//! path in this commit — the stamp site is in the engine's append, which this
//! change does not own. This is the clock only.

use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// Bits reserved for the logical counter.
const LOGICAL_BITS: u32 = 16;
/// Maximum logical counter value before the physical field must advance.
const LOGICAL_MASK: u64 = (1 << LOGICAL_BITS) - 1;

/// A hybrid logical timestamp: physical milliseconds ‖ logical counter.
///
/// Ordering is the natural `u64` ordering, which is why the physical field
/// occupies the high bits: an `Hlc` compares first by wall-clock millisecond
/// and only then by logical counter.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct Hlc(u64);

impl Hlc {
    /// Build from parts. `logical` is masked to [`LOGICAL_BITS`]; callers that
    /// care about overflow should use [`HlcClock`], which never silently wraps.
    pub const fn from_parts(physical_millis: u64, logical: u64) -> Self {
        Self((physical_millis << LOGICAL_BITS) | (logical & LOGICAL_MASK))
    }

    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Wall-clock milliseconds since the Unix epoch.
    pub const fn physical_millis(self) -> u64 {
        self.0 >> LOGICAL_BITS
    }

    /// The tie-break counter within one physical millisecond.
    pub const fn logical(self) -> u64 {
        self.0 & LOGICAL_MASK
    }
}

/// A monotonic HLC source.
///
/// ⭐ **The property that matters: [`now`](Self::now) never returns a value
/// less than or equal to one it already returned**, whatever the wall clock
/// does. NTP steps backwards, VMs suspend and resume, and a clock that can go
/// backwards produces two events whose recorded order contradicts their real
/// order — a defect that is invisible until someone reads the log.
pub struct HlcClock {
    state: Mutex<Hlc>,
    /// Injectable for tests. Returns milliseconds since the Unix epoch.
    now_millis: Box<dyn Fn() -> u64 + Send + Sync>,
}

impl std::fmt::Debug for HlcClock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HlcClock")
            .field("last", &self.state.lock().map(|g| *g).ok())
            .finish_non_exhaustive()
    }
}

impl Default for HlcClock {
    fn default() -> Self {
        Self::system()
    }
}

impl HlcClock {
    /// A clock reading the system wall clock.
    pub fn system() -> Self {
        Self::with_source(Box::new(|| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0)
        }))
    }

    /// A clock over an injected millisecond source (tests, and the restart
    /// path below).
    pub fn with_source(now_millis: Box<dyn Fn() -> u64 + Send + Sync>) -> Self {
        Self {
            state: Mutex::new(Hlc::default()),
            now_millis,
        }
    }

    /// ⭐ **Restart safety.** A fresh process must not re-issue timestamps a
    /// previous process already used, and the wall clock alone does not
    /// guarantee that (the clock may have stepped back while we were down).
    /// Seed from the highest [`Hlc`] the durable log already carries.
    ///
    /// ⚠ Seeding with `Hlc::default()` — i.e. forgetting to call this — is
    /// safe only if the wall clock never moved backwards across the restart.
    /// Do not rely on that.
    pub fn seeded_from(highest_seen: Hlc, now_millis: Box<dyn Fn() -> u64 + Send + Sync>) -> Self {
        Self {
            state: Mutex::new(highest_seen),
            now_millis,
        }
    }

    /// The next timestamp. Strictly greater than every value previously
    /// returned by this clock.
    pub fn now(&self) -> Hlc {
        let physical = (self.now_millis)();
        let mut last = self.state.lock().expect("hlc mutex poisoned");
        let next = if physical > last.physical_millis() {
            // Wall clock advanced: take it, reset the logical counter.
            Hlc::from_parts(physical, 0)
        } else {
            // Wall clock is behind or equal (skew, a step back, or several
            // events inside one millisecond). Keep our physical value and
            // advance the logical counter instead.
            let logical = last.logical() + 1;
            if logical > LOGICAL_MASK {
                // 65,536 events in one millisecond on one node. Step the
                // physical field rather than wrap, so ordering survives; this
                // runs the clock marginally ahead of the wall clock, which is
                // the lesser harm.
                Hlc::from_parts(last.physical_millis() + 1, 0)
            } else {
                Hlc::from_parts(last.physical_millis(), logical)
            }
        };
        *last = next;
        next
    }

    /// Fold in a timestamp observed from a peer, so causality survives a
    /// message crossing nodes.
    pub fn observe(&self, remote: Hlc) {
        let mut last = self.state.lock().expect("hlc mutex poisoned");
        if remote > *last {
            *last = remote;
        }
    }

    /// The highest value issued or observed so far.
    pub fn last(&self) -> Hlc {
        *self.state.lock().expect("hlc mutex poisoned")
    }
}
