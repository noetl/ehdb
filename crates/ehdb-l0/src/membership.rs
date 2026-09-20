//! **Membership adoption over D8** (multi-region spec M2a).
//!
//! Read-side only. No socket, no bring-up, no write path. This is the policy
//! layer a bring-up calls — the piece D8 has never had.
//!
//! ## What was actually missing, measured
//!
//! The spec called D8 *"0 consumers and 0 tests"*. Half of that is wrong and
//! the correction matters, because it changes what this module should be:
//!
//! | claim | measured in this tree |
//! | :-- | :-- |
//! | 0 tests | **12 running tests** — 10 inline in `runtime.rs`, 2 in `tests/runtime.rs`. Every surface but `cold_load` has call sites |
//! | 0 consumers | **0 reachable consumers.** Exactly two references exist outside `runtime.rs`: `ehdb-gossip/src/origin.rs`, which declares itself INERT, and a re-export in `lib.rs`. Control: `D1EventLog` appears in 35 files |
//!
//! So D8 is *tested* and *unreached*. The gap is the consumer, not coverage —
//! and writing more tests would have felt like progress while changing
//! nothing. This module is the consumer.
//!
//! ## Liveness is wall-clock-free, and that is load-bearing
//!
//! D8 decides liveness by a **monotone per-worker heartbeat counter**, not a
//! timestamp ([`RuntimeStore::list_live_since`]). A node is live if its latest
//! op is not a deregister and its heartbeat is at or above a watermark the
//! caller advances. No clock comparison, so no skew, and an eviction decision
//! is reproducible from the log alone.
//!
//! ⚠ That means the **watermark is the whole policy**. A caller that never
//! advances it evicts nobody, and the system looks permanently healthy — the
//! inert-gate shape. [`MembershipPolicy::advance`] exists so that advancing is
//! a call someone makes, and [`MembershipView::stalled`] makes a policy that
//! has stopped advancing *visible* rather than silently permissive.

use std::collections::BTreeMap;

/// How much membership machinery is switched on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MembershipMode {
    /// **Default.** Nothing reads or writes D8. Today.
    #[default]
    Off,
    /// Register / heartbeat / evict through D8. No network.
    D8,
    /// D8 plus the SWIM gossip transport bound to a socket.
    ///
    /// ⚠ Kind only. `ehdb-gossip` declares itself INERT and
    /// `GossipOrigin` is deliberately unconstructible without a verifier —
    /// do not weaken that to get a bring-up working.
    Gossip,
}

/// Env var selecting the mode.
pub const MEMBERSHIP_MODE_ENV: &str = "NOETL_EHDB_MEMBERSHIP";

impl MembershipMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::D8 => "d8",
            Self::Gossip => "gossip",
        }
    }

    /// ⚠ An unrecognised value is `Off`, matching the fail-safe precedent in
    /// `EventLogMode::from_env` ("an unknown driver never mirrors"). A typo
    /// must not switch on a subsystem.
    pub fn parse(raw: Option<&str>) -> Self {
        match raw.map(|v| v.trim().to_ascii_lowercase()).as_deref() {
            Some("d8") => Self::D8,
            Some("gossip") => Self::Gossip,
            _ => Self::Off,
        }
    }

    pub fn from_env() -> Self {
        Self::parse(std::env::var(MEMBERSHIP_MODE_ENV).ok().as_deref())
    }

    /// Whether D8 is read/written at all.
    pub fn touches_d8(&self) -> bool {
        !matches!(self, Self::Off)
    }
}

/// Kinds of membership transition, for the metric.
///
/// ⭐ [`ALL`](TransitionKind::ALL) exists so a metric can pin **every** label
/// value at 0 unconditionally. `Registry::gather` prunes metric families with
/// no children, so a labelled counter is *absent* until it first fires — and
/// absent reads identically to a broken exporter, the wrong port, or a pod
/// that predates the metric. Pinning is the only way absence becomes
/// distinguishable from zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TransitionKind {
    Join,
    Leave,
    Suspect,
    Evict,
}

impl TransitionKind {
    /// Every value. Closed set, so pinning is exhaustive by construction — a
    /// new variant fails to compile here rather than silently going unpinned.
    pub const ALL: [TransitionKind; 4] = [
        TransitionKind::Join,
        TransitionKind::Leave,
        TransitionKind::Suspect,
        TransitionKind::Evict,
    ];

    pub fn label(&self) -> &'static str {
        match self {
            Self::Join => "join",
            Self::Leave => "leave",
            Self::Suspect => "suspect",
            Self::Evict => "evict",
        }
    }
}

/// Transition counts, with every label present from construction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransitionCounts(BTreeMap<&'static str, u64>);

impl Default for TransitionCounts {
    /// ⭐ Every label at 0, **unconditionally** — not inside a mode branch.
    /// A pin placed inside `if mode != Off` is not a pin: it leaves the
    /// counters absent on exactly the configuration whose value someone would
    /// be reading.
    fn default() -> Self {
        Self(
            TransitionKind::ALL
                .iter()
                .map(|k| (k.label(), 0u64))
                .collect(),
        )
    }
}

impl TransitionCounts {
    pub fn incr(&mut self, kind: TransitionKind) {
        *self.0.entry(kind.label()).or_insert(0) += 1;
    }

    pub fn get(&self, kind: TransitionKind) -> u64 {
        self.0.get(kind.label()).copied().unwrap_or(0)
    }

    /// Exported pairs, always containing every label.
    pub fn exported(&self) -> Vec<(&'static str, u64)> {
        self.0.iter().map(|(k, v)| (*k, *v)).collect()
    }
}

/// The eviction watermark and how it moves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MembershipPolicy {
    mode: MembershipMode,
    /// Heartbeats at or above this count as live.
    watermark: u64,
    /// How many ticks have passed without the watermark advancing.
    ticks_since_advance: u64,
    /// After this many stalled ticks the view reports itself untrustworthy.
    stall_limit: u64,
}

/// Default stalled-tick tolerance before a view stops claiming authority.
pub const DEFAULT_STALL_LIMIT: u64 = 3;

impl MembershipPolicy {
    pub fn new(mode: MembershipMode) -> Self {
        Self {
            mode,
            watermark: 0,
            ticks_since_advance: 0,
            stall_limit: DEFAULT_STALL_LIMIT,
        }
    }

    pub fn with_stall_limit(mut self, limit: u64) -> Self {
        self.stall_limit = limit;
        self
    }

    pub fn mode(&self) -> MembershipMode {
        self.mode
    }

    pub fn watermark(&self) -> u64 {
        self.watermark
    }

    /// Advance the watermark. Monotone: a lower value is ignored rather than
    /// applied, because moving a watermark backwards resurrects evicted nodes.
    pub fn advance(&mut self, to: u64) -> bool {
        if to > self.watermark {
            self.watermark = to;
            self.ticks_since_advance = 0;
            true
        } else {
            self.ticks_since_advance += 1;
            false
        }
    }

    /// A tick that did not advance the watermark.
    pub fn tick_without_advance(&mut self) {
        self.ticks_since_advance += 1;
    }

    /// ⚠ Whether this policy has stopped advancing. A stalled watermark
    /// evicts nobody, so every node reads live and the subsystem looks
    /// healthy precisely when it has stopped working.
    pub fn stalled(&self) -> bool {
        self.ticks_since_advance >= self.stall_limit
    }
}

/// One node as the view sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberEntry {
    pub worker_id: String,
    pub heartbeat: u64,
}

/// The membership answer, plus whether it can be trusted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MembershipView {
    pub live: Vec<MemberEntry>,
    pub evicted: Vec<MemberEntry>,
    /// ⚠ `true` when the policy has stalled. A consumer MUST NOT act on
    /// `live` while this is set: the list is "everyone we ever saw", not
    /// "everyone who is up".
    pub stalled: bool,
    pub watermark: u64,
}

impl MembershipView {
    /// Whether this view is safe to route on.
    pub fn is_authoritative(&self) -> bool {
        !self.stalled
    }
}

/// Partition members into live and evicted against the policy's watermark.
///
/// Pure: takes the members D8 reported and the policy, returns the view. The
/// D8 read itself is the caller's, so this is testable without a store.
pub fn view_for(
    members: &[MemberEntry],
    policy: &MembershipPolicy,
    counts: &mut TransitionCounts,
) -> MembershipView {
    let mut live = Vec::new();
    let mut evicted = Vec::new();
    for m in members {
        if m.heartbeat >= policy.watermark() {
            live.push(m.clone());
        } else {
            counts.incr(TransitionKind::Evict);
            evicted.push(m.clone());
        }
    }
    MembershipView {
        live,
        evicted,
        stalled: policy.stalled(),
        watermark: policy.watermark(),
    }
}
