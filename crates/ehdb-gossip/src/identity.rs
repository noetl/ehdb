//! The gossip identity — what a NoETL instance announces about itself.
//!
//! foca's `Identity` is pluggable precisely so the payload can carry more than
//! an address; its own docs give *"shard id, deployment version"* as the
//! example. That is exactly what routing needs: the membership message names
//! **which shard** the announcing instance owns, so D8's `contract` field is
//! populated straight from gossip rather than inferred.

use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

/// A NoETL instance as its peers see it.
///
/// ⚠ `incarnation` is what makes rejoin work. When foca learns this instance was
/// declared Down, it calls [`Identity::renew`]; returning a bumped incarnation
/// lets the instance re-announce itself as demonstrably newer than the corpse
/// its peers are holding. Without it, a node wrongly evicted stays evicted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardIdentity {
    /// Where to reach this instance. The `Addr` for conflict resolution.
    pub addr: SocketAddr,
    /// **The partition this instance owns.** The reason a custom identity exists.
    pub shard: u32,
    /// Stable instance name — the D8 `worker_id`, and the thing an operator reads.
    pub instance: String,
    /// Bumped on renew; higher wins an address conflict.
    pub incarnation: u64,
}

impl ShardIdentity {
    pub fn new(addr: SocketAddr, shard: u32, instance: impl Into<String>) -> Self {
        Self {
            addr,
            shard,
            instance: instance.into(),
            incarnation: 0,
        }
    }

    /// The D8 `contract` this identity implies — what routing reads back out.
    ///
    /// Deliberately a small, parseable string rather than a nested structure:
    /// `contract` is validated for length at append, and a compact form keeps a
    /// gossip-sourced op cheap.
    pub fn contract(&self) -> String {
        format!("shard={};addr={}", self.shard, self.addr)
    }
}

impl foca::Identity for ShardIdentity {
    type Addr = SocketAddr;

    fn renew(&self) -> Option<Self> {
        Some(Self {
            incarnation: self.incarnation.wrapping_add(1),
            ..self.clone()
        })
    }

    fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// ⚠ Higher incarnation wins. Getting this backwards means a *stale*
    /// identity beats a fresh one, so a rejoining node is permanently rejected
    /// by peers holding its old record — the failure looks like "the node cannot
    /// come back" rather than like a comparison bug.
    fn win_addr_conflict(&self, adversary: &Self) -> bool {
        self.incarnation > adversary.incarnation
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use foca::Identity as _;

    fn id(port: u16, shard: u32, inc: u64) -> ShardIdentity {
        let mut i = ShardIdentity::new(
            format!("127.0.0.1:{port}").parse().unwrap(),
            shard,
            format!("noetl-server-{shard}"),
        );
        i.incarnation = inc;
        i
    }

    #[test]
    fn the_contract_carries_the_shard_and_address() {
        let c = id(8080, 3, 0).contract();
        assert!(
            c.contains("shard=3"),
            "routing reads the shard back out of this: {c}"
        );
        assert!(c.contains("127.0.0.1:8080"), "and the address: {c}");
    }

    #[test]
    fn renew_bumps_the_incarnation_and_keeps_everything_else() {
        let a = id(8080, 3, 7);
        let b = a
            .renew()
            .expect("renew must yield an identity, or a wrongly-evicted node stays evicted");
        assert_eq!(b.incarnation, 8);
        assert_eq!(
            (b.shard, b.addr, &b.instance),
            (a.shard, a.addr, &a.instance)
        );
    }

    /// ⚠ Directionality. Inverting this makes a stale identity beat a fresh one,
    /// so a rejoining node is permanently rejected by peers holding its corpse.
    #[test]
    fn a_higher_incarnation_wins_the_address_conflict() {
        let fresh = id(8080, 3, 9);
        let stale = id(8080, 3, 2);
        assert!(
            fresh.win_addr_conflict(&stale),
            "the newer identity must win"
        );
        assert!(!stale.win_addr_conflict(&fresh), "the older must not");
        assert!(!fresh.win_addr_conflict(&fresh), "equal is not a win");
    }
}
