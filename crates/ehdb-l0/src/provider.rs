//! **D10 — provider-facts** (infra-as-playbook desired/observed state, the
//! `provider_state` fold behind `noetl provider {plan,drift,orphans,adopt}`),
//! RFC §0.1.
//!
//! The reconciliation substrate for the provider track (noetl/ai-meta#189): a
//! resource's **desired** state (from a plan) and its **observed** state (from a
//! refresh) folded to a current fact, keyed by a `(stack, provider_urn)` pair —
//! over immutable parts. Modeled as an append-only log of `ProviderFactOp`;
//! desired and observed arrive on separate ticks (plan vs refresh), so each op
//! carries the just-set field and **carries forward** the other from the latest
//! prior op (the CQRS fold). A resource's current fact is its latest op (since
//! [`read_index_after`] returns a key's ops in order, "the last one").
//! **Drift** = a present resource whose `desired != observed`.
//!
//! The fold is **credential-free**: desired/observed are payload-derived state
//! descriptors, never secrets (secrets stay in the keychain per the platform
//! invariant). Same discipline as the eventlog-tier fold in noetl-tools.
//!
//! Fixed shape: **sort key** = `op_seq`; **partition** + **index dim** = the
//! encoded `(stack, provider_urn)` key.
//!
//! [`read_index_after`]: crate::engine::L0Engine::read_index_after

use std::collections::BTreeMap;
use std::sync::Arc;

use ehdb_core::Result;
use serde::{Deserialize, Serialize};

use crate::dataset::{shard_for_execution, Dataset};
use crate::engine::{L0Config, L0Engine, ReplicaTarget};
use crate::substrate::DurableSubstrate;

/// The D10 dataset id.
pub const DATASET_D10_PROVIDER: &str = "d10_provider";

/// Unit-separator joining `(stack, provider_urn)` into the index key. Chosen
/// because it does not appear in stack names or provider URNs.
const SEP: char = '\u{1f}';

/// Encode a `(stack, provider_urn)` pair into its index key.
pub fn provider_key(stack: &str, provider_urn: &str) -> String {
    format!("{stack}{SEP}{provider_urn}")
}

/// One provider-fact transition in the log (the D10 record schema).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderFactOp {
    /// Op-log position (the fixed sort key).
    pub op_seq: u64,
    /// The encoded `(stack, provider_urn)` key (partition + index dim).
    pub key: String,
    /// The stack this resource belongs to.
    pub stack: String,
    /// The provider resource URN.
    pub provider_urn: String,
    /// The desired-state descriptor (credential-free; empty = unknown).
    pub desired: String,
    /// The last observed-state descriptor (credential-free; empty = unknown).
    pub observed: String,
    /// `false` = the resource is forgotten / no longer managed (an explicit
    /// tombstone; drift-scan ignores it, orphan-scan may still care).
    pub present: bool,
}

/// **D10 provider-facts dataset.** Sort key `op_seq`; partition + index dim =
/// the encoded `(stack, provider_urn)` key.
#[derive(Debug, Clone, Copy)]
pub struct ProviderDataset;

impl Dataset for ProviderDataset {
    type Record = ProviderFactOp;
    const NAME: &'static str = DATASET_D10_PROVIDER;

    fn sort_key(r: &ProviderFactOp) -> u64 {
        r.op_seq
    }
    fn partition(r: &ProviderFactOp, shard_count: u32) -> u32 {
        shard_for_execution(&r.key, shard_count)
    }
    fn index_key(r: &ProviderFactOp) -> &str {
        &r.key
    }
    fn read_partition(key: &str, shard_count: u32) -> u32 {
        shard_for_execution(key, shard_count)
    }
}

/// A resource's current fact (the fold result).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderFact {
    pub stack: String,
    pub provider_urn: String,
    pub desired: String,
    pub observed: String,
    pub present: bool,
}

impl ProviderFact {
    /// Whether this present resource is in drift (desired diverges from
    /// observed). A forgotten resource is never in drift.
    pub fn in_drift(&self) -> bool {
        self.present && self.desired != self.observed
    }
}

/// **The D10 provider-facts store** — fold desired/observed, get-latest-fact,
/// drift-scan over the generic engine.
pub struct ProviderStore {
    engine: L0Engine<ProviderDataset>,
}

impl ProviderStore {
    /// Config default for D10 rooted at `local_root`.
    pub fn config(local_root: impl Into<std::path::PathBuf>) -> L0Config {
        L0Config::for_dataset(DATASET_D10_PROVIDER, local_root)
    }

    /// Open a single-replica provider-facts store.
    pub fn open(config: L0Config, substrate: Arc<dyn DurableSubstrate>) -> Result<Self> {
        Ok(Self {
            engine: L0Engine::open(config, substrate)?,
        })
    }
    /// Open an N-way replicated provider-facts store.
    pub fn open_replicated(config: L0Config, replicas: Vec<ReplicaTarget>) -> Result<Self> {
        Ok(Self {
            engine: L0Engine::open_replicated(config, replicas)?,
        })
    }
    /// Cold-load a single-replica provider-facts store.
    pub fn cold_load(config: L0Config, substrate: Arc<dyn DurableSubstrate>) -> Result<Self> {
        Ok(Self {
            engine: L0Engine::cold_load(config, substrate)?,
        })
    }
    /// Cold-load an N-way replicated provider-facts store.
    pub fn cold_load_replicated(config: L0Config, replicas: Vec<ReplicaTarget>) -> Result<Self> {
        Ok(Self {
            engine: L0Engine::cold_load_replicated(config, replicas)?,
        })
    }

    /// The latest op for a key (index-pruned read, last wins).
    fn latest(&self, key: &str) -> Result<Option<ProviderFactOp>> {
        Ok(self
            .engine
            .read_index_after(key, 0)?
            .into_iter()
            .next_back())
    }

    /// **set-desired** — record `desired` for `(stack, provider_urn)` (from a
    /// plan). Carries forward the last observed state; marks the resource
    /// present. Returns the op sequence.
    pub fn set_desired(
        &mut self,
        stack: &str,
        provider_urn: &str,
        desired: impl Into<String>,
    ) -> Result<u64> {
        let key = provider_key(stack, provider_urn);
        let observed = self.latest(&key)?.map(|op| op.observed).unwrap_or_default();
        self.append(&key, stack, provider_urn, desired.into(), observed, true)
    }

    /// **set-observed** — record `observed` for `(stack, provider_urn)` (from a
    /// refresh). Carries forward the last desired state; marks the resource
    /// present. Returns the op sequence.
    pub fn set_observed(
        &mut self,
        stack: &str,
        provider_urn: &str,
        observed: impl Into<String>,
    ) -> Result<u64> {
        let key = provider_key(stack, provider_urn);
        let desired = self.latest(&key)?.map(|op| op.desired).unwrap_or_default();
        self.append(&key, stack, provider_urn, desired, observed.into(), true)
    }

    /// **forget** — mark `(stack, provider_urn)` no longer managed (present =
    /// false; drops out of drift-scan and the present list). Returns `false` if
    /// the resource was unknown or already forgotten.
    pub fn forget(&mut self, stack: &str, provider_urn: &str) -> Result<bool> {
        let key = provider_key(stack, provider_urn);
        match self.latest(&key)? {
            Some(op) if op.present => {
                self.append(&key, stack, provider_urn, op.desired, op.observed, false)?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    /// **get-latest-fact** — the folded current fact for `(stack, provider_urn)`,
    /// or `None` if the resource was never recorded. A forgotten resource still
    /// resolves (with `present = false`) so callers can tell "deleted" from
    /// "never seen".
    pub fn get_latest_fact(&self, stack: &str, provider_urn: &str) -> Result<Option<ProviderFact>> {
        let key = provider_key(stack, provider_urn);
        Ok(self.latest(&key)?.map(|op| ProviderFact {
            stack: op.stack,
            provider_urn: op.provider_urn,
            desired: op.desired,
            observed: op.observed,
            present: op.present,
        }))
    }

    /// **drift-scan** — the present resources in `stack` whose desired state
    /// diverges from their observed state, in provider-URN order.
    pub fn drift_scan(&self, stack: &str) -> Result<Vec<ProviderFact>> {
        Ok(self
            .stack_facts(stack)?
            .into_iter()
            .filter(ProviderFact::in_drift)
            .collect())
    }

    /// The present facts in a stack (latest per key, forgotten dropped), in
    /// provider-URN order.
    pub fn list(&self, stack: &str) -> Result<Vec<ProviderFact>> {
        Ok(self
            .stack_facts(stack)?
            .into_iter()
            .filter(|f| f.present)
            .collect())
    }

    /// Fold the whole log to the latest fact per key, keep this stack's, sorted
    /// by provider URN.
    fn stack_facts(&self, stack: &str) -> Result<Vec<ProviderFact>> {
        let all = self.engine.replay_all()?; // sorted by op_seq
        let mut latest: BTreeMap<String, ProviderFactOp> = BTreeMap::new();
        for op in all {
            latest.insert(op.key.clone(), op); // last op_seq wins
        }
        let mut facts: Vec<ProviderFact> = latest
            .into_values()
            .filter(|op| op.stack == stack)
            .map(|op| ProviderFact {
                stack: op.stack,
                provider_urn: op.provider_urn,
                desired: op.desired,
                observed: op.observed,
                present: op.present,
            })
            .collect();
        facts.sort_by(|a, b| a.provider_urn.cmp(&b.provider_urn));
        Ok(facts)
    }

    fn append(
        &mut self,
        key: &str,
        stack: &str,
        provider_urn: &str,
        desired: String,
        observed: String,
        present: bool,
    ) -> Result<u64> {
        let op_seq = self.engine.global_sequence() + 1;
        self.engine.append_record(ProviderFactOp {
            op_seq,
            key: key.to_string(),
            stack: stack.to_string(),
            provider_urn: provider_urn.to_string(),
            desired,
            observed,
            present,
        })?;
        Ok(op_seq)
    }

    /// Flush all sealed parts to the durable replicas.
    pub fn flush_and_wait(&mut self) -> Result<()> {
        self.engine.flush_and_wait_uploads()
    }
    /// Run background merge/compaction over the fact log.
    pub fn run_pending_merges(&mut self) -> Result<usize> {
        self.engine.run_pending_merges()
    }
    /// The underlying generic engine.
    pub fn engine(&self) -> &L0Engine<ProviderDataset> {
        &self.engine
    }
}
