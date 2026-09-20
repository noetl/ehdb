//! The M2 clock flag — `NOETL_EHDB_HLC`.
//!
//! Separate from the clock itself (`ehdb_core::hlc`) because the clock is a
//! mechanism and this is a policy, and because a pure parse is testable without
//! the process env: `cargo test` does **not** serialise tests, so a test that
//! drove the variable through `set_var` would race every other test in the
//! binary.

/// `NOETL_EHDB_HLC` — `off` (default) | `shadow` | `on`.
pub const HLC_ENV: &str = "NOETL_EHDB_HLC";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HlcMode {
    /// No clock, no stamp. Records serialise byte-identically to today.
    #[default]
    Off,
    /// Stamp every append; **nothing reads it**.
    Shadow,
    /// Reserved for M3, when a reader appears. Behaves as `Shadow` today —
    /// deliberately, because a mode that silently did MORE than shadow before
    /// its reader exists would be a phase-ordering violation wearing a flag.
    On,
}

impl HlcMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Shadow => "shadow",
            Self::On => "on",
        }
    }

    /// Whether appends carry a commit HLC.
    pub fn stamps(self) -> bool {
        !matches!(self, Self::Off)
    }

    /// Pure parse. An unrecognised value — including a typo — is `Off`, the
    /// fail-safe precedent from `EventLogMode::from_env` ("an unknown driver
    /// never mirrors"). A typo must never start writing a new column.
    pub fn parse(raw: Option<&str>) -> Self {
        match raw.map(|s| s.trim().to_ascii_lowercase()).as_deref() {
            Some("shadow") => Self::Shadow,
            Some("on") => Self::On,
            _ => Self::Off,
        }
    }

    pub fn from_env() -> Self {
        Self::parse(std::env::var(HLC_ENV).ok().as_deref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unset_and_typos_are_off() {
        assert_eq!(HlcMode::parse(None), HlcMode::Off);
        for bad in ["", "shadw", "true", "1", "yes", "enabled"] {
            assert_eq!(HlcMode::parse(Some(bad)), HlcMode::Off, "{bad:?}");
        }
    }

    #[test]
    fn only_off_skips_the_stamp() {
        assert!(!HlcMode::Off.stamps());
        assert!(HlcMode::Shadow.stamps());
        assert!(HlcMode::On.stamps());
        // POSITIVE CONTROL: `stamps()` really can be false, so the assertions
        // above are a decision rather than a constant.
        assert_ne!(HlcMode::Off.stamps(), HlcMode::Shadow.stamps());
    }
}
