//! The network-trust half, plugged into D8's validation seam
//! (noetl/ai-meta#332).
//!
//! `ehdb-l0`'s [`validate_op`] answers *"is this op well-formed"*. It cannot
//! answer *"was the sender entitled to write it"* — that needs a signed gossip
//! message and a cluster key. [`OpOrigin`] is the seam where that plugs in, and
//! this module is the gossip-side implementation.
//!
//! # Why there is no permissive default here
//!
//! `ehdb-l0` ships [`TrustedLocalOrigin`], which accepts every structurally
//! valid op. That is correct while every caller is in-process — and **wrong the
//! moment gossip feeds the store from the network**: a peer could append a
//! `Register` claiming to be another shard's instance, and because membership is
//! *persisted*, the poisoned entry does not vanish when the attacker leaves.
//!
//! So [`GossipOrigin`] **cannot be constructed without a verifier**. There is no
//! `Default`, no `new()` that accepts everything, and no feature flag that
//! relaxes it. Wiring gossip therefore forces a deliberate decision about who is
//! trusted, rather than inheriting a default that was safe in a different
//! context.
//!
//! [`validate_op`]: ehdb_l0::runtime::validate_op
//! [`OpOrigin`]: ehdb_l0::runtime::OpOrigin
//! [`TrustedLocalOrigin`]: ehdb_l0::runtime::TrustedLocalOrigin

use ehdb_core::{EhdbError, Result};
use ehdb_l0::runtime::{OpOrigin, RuntimeOp};

/// Decides whether a membership op genuinely came from the node it names.
///
/// Implementations verify a signature over the op against the sending
/// identity's key. ⚠ An implementation that returns `Ok(())` unconditionally
/// re-opens the routing-poisoning path; if that is ever wanted for a test, name
/// the type so it says so.
pub trait MembershipVerifier: Send + Sync {
    fn verify(&self, op: &RuntimeOp) -> Result<()>;
}

/// Authorises membership ops arriving from gossip.
///
/// Construct with [`GossipOrigin::new`], which requires a verifier.
pub struct GossipOrigin<V: MembershipVerifier> {
    verifier: V,
}

impl<V: MembershipVerifier> GossipOrigin<V> {
    /// The only constructor. Takes a verifier by value — there is no path that
    /// produces a `GossipOrigin` without one.
    pub fn new(verifier: V) -> Self {
        Self { verifier }
    }
}

impl<V: MembershipVerifier> OpOrigin for GossipOrigin<V> {
    fn authorize(&self, op: &RuntimeOp) -> Result<()> {
        self.verifier.verify(op).map_err(|e| {
            // Keep the identity in the message: an unauthorised append is a
            // security event, and "which node claimed what" is the first thing
            // anyone reading the log will want.
            EhdbError::InvalidState(format!(
                "membership op for {:?} rejected by the gossip verifier: {e}",
                op.worker_id
            ))
        })
    }
}

/// A verifier that refuses everything.
///
/// Not a placeholder to be replaced by a permissive one — it is the correct
/// default posture for an unconfigured cluster: **no signing key configured
/// means no remote membership is trusted**, which fails closed rather than open.
#[derive(Debug, Clone, Copy, Default)]
pub struct RejectUnsigned;

impl MembershipVerifier for RejectUnsigned {
    fn verify(&self, _op: &RuntimeOp) -> Result<()> {
        Err(EhdbError::InvalidState(
            "no gossip signing key is configured; remote membership is not trusted \
             (noetl/ai-meta#332)"
                .into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ehdb_l0::runtime::RuntimeEvent;

    fn op() -> RuntimeOp {
        RuntimeOp {
            op_seq: 1,
            worker_id: "server-3".into(),
            event: RuntimeEvent::Register,
            heartbeat: 1,
            contract: "shard=3;addr=127.0.0.1:9003".into(),
        }
    }

    /// ⚠ The default posture is CLOSED. An unconfigured cluster trusting remote
    /// membership is the routing-poisoning path, and persistence makes it durable.
    #[test]
    fn an_unconfigured_cluster_trusts_no_remote_membership() {
        let origin = GossipOrigin::new(RejectUnsigned);
        let err = origin
            .authorize(&op())
            .expect_err("unsigned membership must be refused");
        assert!(
            err.to_string().contains("server-3"),
            "the rejection must name the claimed identity — an unauthorised append \
             is a security event: {err}"
        );
    }

    /// Negative control: a verifier that accepts lets the op through, so the
    /// refusal above is the verifier's decision and not the seam being inert.
    #[test]
    fn a_verifier_that_accepts_lets_the_op_through() {
        struct AcceptForTest;
        impl MembershipVerifier for AcceptForTest {
            fn verify(&self, _op: &RuntimeOp) -> Result<()> {
                Ok(())
            }
        }
        assert!(GossipOrigin::new(AcceptForTest).authorize(&op()).is_ok());
    }

    /// ⚠⚠ The structural guarantee: there is no way to build a `GossipOrigin`
    /// without supplying a verifier.
    ///
    /// A source guard, because the property is about the *absence* of an API — no
    /// runtime test can observe a constructor that does not exist, and that is
    /// precisely the kind of thing a later "convenience" PR adds back.
    #[test]
    fn there_is_no_permissive_constructor() {
        let src = include_str!("origin.rs");
        let body = src.split("#[cfg(test)]").next().unwrap();
        let ctors = body.matches("pub fn new").count();
        assert_eq!(
            ctors, 1,
            "expected exactly one constructor; found {ctors} — the extraction may \
             be wrong, or a permissive one was added"
        );
        for forbidden in [
            "impl Default for GossipOrigin",
            "pub fn permissive",
            "pub fn insecure",
        ] {
            assert!(
                !body.contains(forbidden),
                "`{forbidden}` would let gossip be wired without deciding who is \
                 trusted (noetl/ai-meta#332)"
            );
        }
    }
}
