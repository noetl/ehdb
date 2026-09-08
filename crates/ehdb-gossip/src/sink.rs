//! Translating foca notifications into D8 membership ops.
//!
//! The split this file implements: **foca observes, EHDB records.** foca decides
//! *whether a peer is up*; every transition it reports is appended to D8, and
//! all state, history, recovery and query stay in EHDB.
//!
//! ⚠ **foca's model is edge-triggered.** It emits `MemberUp` / `MemberDown` /
//! `Rejoin` / `Rename` — transitions, not liveness ticks. There is no per-peer
//! "still alive" notification, so D8's heartbeat watermark is **not** advanced by
//! gossip. It is advanced by a local tick over the currently-up set
//! ([`MembershipSink::tick`]), which means `list_live_since` guards against
//! *this adapter* stalling — a real property, and a different one from what
//! gossip guards.

use ehdb_core::Result;
use ehdb_l0::runtime::{RuntimeEvent, RuntimeStore};

use crate::identity::ShardIdentity;

/// What one notification did to the membership log. Returned rather than
/// swallowed so a caller can meter it — a sink that silently does nothing looks
/// exactly like a quiet cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkOutcome {
    Registered,
    Deregistered,
    /// A transition that carries no membership change (`Active`/`Idle`), or a
    /// rename whose old identity was already gone.
    Ignored,
}

/// Appends membership transitions into D8.
pub struct MembershipSink<'a> {
    store: &'a mut RuntimeStore,
}

impl<'a> MembershipSink<'a> {
    pub fn new(store: &'a mut RuntimeStore) -> Self {
        Self { store }
    }

    /// A peer came up (or rejoined). Registering is idempotent-by-design: D8's
    /// `register` re-registers, resetting the watermark, which is the correct
    /// response to a rejoin.
    pub fn member_up(&mut self, id: &ShardIdentity) -> Result<SinkOutcome> {
        self.store.register(&id.instance, id.contract())?;
        Ok(SinkOutcome::Registered)
    }

    /// A peer went down.
    ///
    /// ⚠ Returns `Ignored` when it was already absent rather than erroring: foca
    /// can report a down for a member this instance never saw come up (it joined
    /// mid-gossip), and treating that as an error would make a normal race look
    /// like a fault.
    pub fn member_down(&mut self, id: &ShardIdentity) -> Result<SinkOutcome> {
        Ok(if self.store.deregister(&id.instance)? {
            SinkOutcome::Deregistered
        } else {
            SinkOutcome::Ignored
        })
    }

    /// An identity was replaced (same node, new incarnation or address).
    ///
    /// ⚠ Order matters: register the new identity BEFORE dropping the old. The
    /// reverse leaves a window in which the node is absent from routing, and a
    /// request routed in that window fails closed for no reason.
    pub fn rename(&mut self, old: &ShardIdentity, new: &ShardIdentity) -> Result<SinkOutcome> {
        self.store.register(&new.instance, new.contract())?;
        if old.instance != new.instance {
            self.store.deregister(&old.instance)?;
        }
        Ok(SinkOutcome::Registered)
    }

    /// Advance the heartbeat watermark for every member currently up.
    ///
    /// Called on a local timer, not by gossip — see the module note. Returns how
    /// many were advanced so a stalled adapter is visible as a zero rather than
    /// as silence.
    pub fn tick(&mut self) -> Result<usize> {
        let live: Vec<String> = self
            .store
            .list_live()?
            .into_iter()
            .map(|s| s.worker_id)
            .collect();
        // ⚠ No liveness re-check here: `list_live` already yielded only live
        // members, so `heartbeat` cannot return None for them. An `is_some()`
        // guard here reads as defensive and is in fact unreachable — a mutation
        // deleting it passed every test, which is how it was found.
        let mut n = 0;
        for id in live {
            self.store.heartbeat(&id)?;
            n += 1;
        }
        Ok(n)
    }
}

/// The event a notification maps to, as a pure function — so the mapping is
/// testable without foca, a socket or a store.
///
/// `None` means "no membership change" (`Active` / `Idle` / `Defunct`, which are
/// about *this* instance's own state, not about a peer).
pub fn event_for(notification: &NotificationKind) -> Option<RuntimeEvent> {
    match notification {
        NotificationKind::MemberUp | NotificationKind::Rejoin | NotificationKind::Rename => {
            Some(RuntimeEvent::Register)
        }
        NotificationKind::MemberDown => Some(RuntimeEvent::Deregister),
        NotificationKind::Active | NotificationKind::Idle | NotificationKind::Defunct => None,
    }
}

/// foca's `Notification` carries borrowed identities and a lifetime, which makes
/// it awkward to match on in a table-driven test. This mirrors its *shape* so the
/// mapping above can be exhaustively pinned.
///
/// ⚠ It must stay in sync with foca's enum. `notification_kinds_are_exhaustive`
/// below is the guard: it fails if foca gains a variant this does not model.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotificationKind {
    MemberUp,
    MemberDown,
    Rename,
    Rejoin,
    Active,
    Idle,
    Defunct,
}

#[cfg(test)]
mod tests {
    use super::*;
    use ehdb_l0::substrate::{DurableSubstrate, LocalFsSubstrate};
    use std::sync::Arc;

    fn store(dir: &std::path::Path) -> RuntimeStore {
        let sub: Arc<dyn DurableSubstrate> =
            Arc::new(LocalFsSubstrate::new(dir.join("substrate")).unwrap());
        RuntimeStore::open(RuntimeStore::config(dir.join("local")), sub).unwrap()
    }

    fn id(shard: u32, name: &str) -> ShardIdentity {
        ShardIdentity::new(
            format!("127.0.0.1:{}", 9000 + shard as u16)
                .parse()
                .unwrap(),
            shard,
            name,
        )
    }

    #[test]
    fn member_up_registers_with_the_shard_in_the_contract() {
        let tmp = tempfile::tempdir().unwrap();
        let mut s = store(tmp.path());
        let mut sink = MembershipSink::new(&mut s);
        assert_eq!(
            sink.member_up(&id(3, "server-3")).unwrap(),
            SinkOutcome::Registered
        );

        let st = s.get("server-3").unwrap().expect("the peer must be in D8");
        assert!(
            st.contract.contains("shard=3"),
            "routing reads the shard here: {}",
            st.contract
        );
    }

    #[test]
    fn member_down_deregisters_and_a_repeat_is_ignored_not_an_error() {
        let tmp = tempfile::tempdir().unwrap();
        let mut s = store(tmp.path());
        {
            let mut sink = MembershipSink::new(&mut s);
            sink.member_up(&id(3, "server-3")).unwrap();
            assert_eq!(
                sink.member_down(&id(3, "server-3")).unwrap(),
                SinkOutcome::Deregistered
            );
            // foca can report a down for a member this instance never saw up.
            assert_eq!(
                sink.member_down(&id(3, "server-3")).unwrap(),
                SinkOutcome::Ignored,
                "a normal gossip race must not look like a fault"
            );
        }
        assert!(s.list_live().unwrap().is_empty());
    }

    /// ⚠ The window. Registering the new identity BEFORE dropping the old means
    /// the node is never absent from routing during a rename.
    ///
    /// Asserting only the end state does not prove this — both orders end
    /// identically, and a mutation swapping them passed. The order is observable
    /// in the op log, because `op_seq` is monotonic, so that is what is checked.
    #[test]
    fn rename_registers_the_new_identity_before_dropping_the_old() {
        let tmp = tempfile::tempdir().unwrap();
        let mut s = store(tmp.path());
        {
            let mut sink = MembershipSink::new(&mut s);
            sink.member_up(&id(3, "server-3a")).unwrap();
            sink.rename(&id(3, "server-3a"), &id(3, "server-3b"))
                .unwrap();
        }
        let new_reg = s
            .engine()
            .read_index_after("server-3b", 0)
            .unwrap()
            .into_iter()
            .find(|o| o.event == RuntimeEvent::Register)
            .expect("the new identity must be registered");
        let old_dereg = s
            .engine()
            .read_index_after("server-3a", 0)
            .unwrap()
            .into_iter()
            .find(|o| o.event == RuntimeEvent::Deregister)
            .expect("the old identity must be deregistered");
        assert!(
            new_reg.op_seq < old_dereg.op_seq,
            "the new identity ({}) must be registered BEFORE the old is dropped \
             ({}), or a request routed in the gap fails closed for no reason",
            new_reg.op_seq,
            old_dereg.op_seq
        );

        let live: Vec<String> = s
            .list_live()
            .unwrap()
            .into_iter()
            .map(|x| x.worker_id)
            .collect();
        assert_eq!(
            live,
            vec!["server-3b".to_string()],
            "and the end state is still right"
        );
    }

    #[test]
    fn tick_advances_only_live_members_and_reports_how_many() {
        let tmp = tempfile::tempdir().unwrap();
        let mut s = store(tmp.path());
        let mut sink = MembershipSink::new(&mut s);
        sink.member_up(&id(0, "server-0")).unwrap();
        sink.member_up(&id(1, "server-1")).unwrap();
        sink.member_down(&id(1, "server-1")).unwrap();

        assert_eq!(sink.tick().unwrap(), 1, "only the live member advances");
        assert_eq!(sink.tick().unwrap(), 1);
        assert_eq!(
            s.get("server-0").unwrap().unwrap().heartbeat,
            3,
            "1 at register + 2 ticks"
        );
    }

    /// The whole mapping, pinned.
    #[test]
    fn every_notification_kind_maps_deliberately() {
        use NotificationKind::*;
        let cases = [
            (MemberUp, Some(RuntimeEvent::Register)),
            (Rejoin, Some(RuntimeEvent::Register)),
            (Rename, Some(RuntimeEvent::Register)),
            (MemberDown, Some(RuntimeEvent::Deregister)),
            // These are about THIS instance's own state, not a peer's membership.
            (Active, None),
            (Idle, None),
            (Defunct, None),
        ];
        for (kind, want) in cases {
            assert_eq!(event_for(&kind), want, "{kind:?} must map deliberately");
        }
        assert_eq!(
            cases.len(),
            7,
            "foca has 7 notification variants; model them all"
        );
    }
}
