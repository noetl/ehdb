//! `FencedEngine` — the engine chokepoint, where coverage stops being a grep.
//!
//! ## The property
//!
//! Every durable mutation on [`L0Engine`] requires `&mut`. This type is the sole
//! owner of the engine: the field is private, and the **only** method that hands
//! out `&mut L0Engine` is [`FencedEngine::write`], which cannot return without
//! consulting the fence.
//!
//! So "which durable-mutation paths are fenced?" is answered by the borrow
//! checker rather than by a search: **all twelve `&mut self` methods**, because
//! none of them is callable without a `&mut` that only `write()` can produce.
//! A thirteenth added tomorrow is fenced the moment it is written, without
//! anyone remembering to fence it.
//!
//! That is the property the previous design lacked. `FencedSharedBackend`
//! decorated a trait the writer does not use, and the gap was invisible to every
//! check that existed — 624 real appends and 290,440 commits through the live
//! prod writer with `writes_checked` at **0** (noetl/ai-meta#460).
//!
//! ## ⚠ What it does NOT guarantee
//!
//! The [`MutationPath`] a caller passes to `write()` is a **label**. The
//! compiler enforces that *a* check happened; it does not enforce that the label
//! is accurate, and a caller could hold a guard labelled `AppendRecord` and call
//! `seal_aged_parts` on it. The per-path counters therefore make coverage
//! *observable* — which is what the kind validation needs — and are not
//! themselves a proof. Saying otherwise would be the same overclaim as reading
//! `stale_observed 0` off a decorator nothing reached.
//!
//! ## ⚠ Reads are deliberately not fenced
//!
//! [`FencedEngine::read`] takes no token and increments nothing. A superseded
//! writer serving a stale *read* is a different problem with a different answer
//! (lease-based read fencing), and folding it in here would mean a writer that
//! loses its lease immediately stops answering queries it can still answer
//! correctly. `read()` yields `&L0Engine` only — the type makes that boundary
//! structural rather than a convention.

use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex, MutexGuard};

use ehdb_core::fence::{EpochFence, FenceDecision, MutationPath};
use ehdb_core::Result;
use ehdb_l0::{Dataset, L0Engine};

/// Read access: `&L0Engine`, no fence check, no counters.
pub struct EngineRead<'a, D: Dataset> {
    guard: MutexGuard<'a, L0Engine<D>>,
}

impl<'a, D: Dataset> Deref for EngineRead<'a, D> {
    type Target = L0Engine<D>;
    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

/// Write access: `&mut L0Engine`, obtainable only from
/// [`FencedEngine::write`], which has already consulted the fence.
pub struct EngineWrite<'a, D: Dataset> {
    guard: MutexGuard<'a, L0Engine<D>>,
    decision: FenceDecision,
}

impl<'a, D: Dataset> EngineWrite<'a, D> {
    /// What the fence concluded for this guard. `Stale` here means shadow mode
    /// let a superseded write through on purpose.
    pub fn decision(&self) -> FenceDecision {
        self.decision
    }
}

impl<'a, D: Dataset> Deref for EngineWrite<'a, D> {
    type Target = L0Engine<D>;
    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl<'a, D: Dataset> DerefMut for EngineWrite<'a, D> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

/// The engine, behind its fence.
pub struct FencedEngine<D: Dataset> {
    /// ⚠⚠ Private, and it must stay private. A `pub` field, or any accessor
    /// returning `&mut` or the inner `Mutex`, reopens exactly the hole this type
    /// closes — which is what `FeedWriter::engine()` used to be when it handed
    /// out `Arc<Mutex<L0Engine<D>>>` at 59 call sites.
    inner: Mutex<L0Engine<D>>,
    fence: Arc<EpochFence>,
    shard: u32,
}

impl<D: Dataset> FencedEngine<D> {
    pub fn new(engine: L0Engine<D>, fence: Arc<EpochFence>, shard: u32) -> Self {
        Self {
            inner: Mutex::new(engine),
            fence,
            shard,
        }
    }

    /// An engine with no fence — the default, and byte-identical to the
    /// unfenced behaviour.
    pub fn unfenced(engine: L0Engine<D>, shard: u32) -> Self {
        Self::new(engine, EpochFence::off(), shard)
    }

    pub fn fence(&self) -> &Arc<EpochFence> {
        &self.fence
    }

    pub fn shard(&self) -> u32 {
        self.shard
    }

    /// Read the engine. No fence check — see the module note.
    pub fn read(&self) -> EngineRead<'_, D> {
        EngineRead {
            guard: self.lock_inner(),
        }
    }

    /// Mutate the engine, after the fence agrees.
    ///
    /// ⚠ The check happens **before** the lock is taken, so a refused write does
    /// not queue behind the append lock to be told no. The ledger read is a
    /// small file read; under contention it is cheaper than the lock it avoids.
    pub fn write(&self, path: MutationPath) -> Result<EngineWrite<'_, D>> {
        let decision = self.fence.check(self.shard, path)?;
        Ok(EngineWrite {
            guard: self.lock_inner(),
            decision,
        })
    }

    /// Read the engine **without blocking**, or `None` if the lock is held.
    ///
    /// ⚠⚠ This exists because a metrics handler must never park on the append
    /// lock. `Mutex::lock()` blocks, and a `/metrics` scrape that waits for an
    /// in-flight append is the ehdb#345 shape: the endpoint that is supposed to
    /// report the writer's health becomes unavailable exactly when the writer is
    /// busiest. `None` means "contended", which is a reportable fact; a blocked
    /// thread is not.
    pub fn try_read(&self) -> Option<EngineRead<'_, D>> {
        match self.inner.try_lock() {
            Ok(guard) => Some(EngineRead { guard }),
            Err(std::sync::TryLockError::Poisoned(p)) => Some(EngineRead {
                guard: p.into_inner(),
            }),
            Err(std::sync::TryLockError::WouldBlock) => None,
        }
    }

    /// Mutate without blocking, after the fence agrees.
    ///
    /// ⚠ The fence is consulted **first**, so a refused write reports `Err`
    /// rather than `None` — "you are superseded" and "the lock is busy" are
    /// different answers and a caller that conflated them would retry the one it
    /// must not.
    pub fn try_write(&self, path: MutationPath) -> Result<Option<EngineWrite<'_, D>>> {
        let decision = self.fence.check(self.shard, path)?;
        Ok(match self.inner.try_lock() {
            Ok(guard) => Some(EngineWrite { guard, decision }),
            Err(std::sync::TryLockError::Poisoned(p)) => Some(EngineWrite {
                guard: p.into_inner(),
                decision,
            }),
            Err(std::sync::TryLockError::WouldBlock) => None,
        })
    }

    /// ⚠ Poison is recovered rather than propagated. A panic while holding the
    /// engine lock must not convert into every subsequent append failing — the
    /// engine's own invariants are restored on open, and refusing all writes
    /// because one caller panicked is a worse outcome than continuing. This
    /// mirrors what the previous `self.engine.lock().unwrap()` call sites did in
    /// aggregate, except without the unwrap.
    fn lock_inner(&self) -> MutexGuard<'_, L0Engine<D>> {
        match self.inner.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}
