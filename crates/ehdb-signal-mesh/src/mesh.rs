//! The mesh: an append-only log, the tier wiring, and the cascade.

use crate::a2a::{AgentCard, Capabilities, Skill, Task, TaskState};
use crate::event::*;
use crate::fold::{fold, FoldError, Reduction};
use crate::react::{run_cycle, Reasoner};

/// An in-memory stand-in for EHDB's D1 event log.
///
/// ⚠ Append-only and sequence-assigning, which is the only part of D1 the mesh
/// depends on. It is NOT a durability model — see the design doc.
#[derive(Debug, Default)]
pub struct EventLog {
    pub stream: String,
    pub records: Vec<Record>,
    next_seq: u64,
}

impl EventLog {
    pub fn new(stream: &str) -> Self {
        Self {
            stream: stream.to_string(),
            records: Vec::new(),
            next_seq: 1,
        }
    }

    pub fn append(&mut self, payload: MeshEvent) -> u64 {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.records.push(Record {
            seq,
            stream: self.stream.clone(),
            payload,
        });
        seq
    }

    pub fn head(&self) -> u64 {
        self.next_seq.saturating_sub(1)
    }
}

/// One agent in the mesh.
pub struct Agent {
    pub id: String,
    pub tier: u8,
    pub how: Reduction,
    /// Agent ids this agent reduces. Empty at tier 0 (it reduces raw signals).
    pub children: Vec<String>,
    /// Tier-0 only: the signal class this agent owns. `None` means "every
    /// class", which is almost never what a per-signal-class agent wants.
    ///
    /// ⚠ Added after the first demo run showed both tier-0 agents reducing all
    /// six signals to the same number — a per-class agent that does not filter
    /// by class is just a duplicate of its sibling.
    pub signal_class: Option<String>,
}

impl Agent {
    pub fn card(&self) -> AgentCard {
        AgentCard {
            name: self.id.clone(),
            description: format!("tier-{} reducer ({:?})", self.tier, self.how),
            url: format!("http://mesh.local/agents/{}", self.id),
            version: "0.1.0".into(),
            protocol_version: crate::a2a::A2A_PROTOCOL_VERSION.into(),
            capabilities: Capabilities {
                streaming: false,
                push_notifications: true,
                extensions: vec![],
            },
            skills: vec![Skill {
                id: format!("reduce.tier{}", self.tier),
                name: format!("Reduce tier {}", self.tier),
                description: format!(
                    "Reduce {} input(s) to one value using {:?}",
                    if self.children.is_empty() {
                        "raw signal".to_string()
                    } else {
                        format!("{} child", self.children.len())
                    },
                    self.how
                ),
                tags: vec!["mesh".into(), format!("tier{}", self.tier)],
            }],
            security_schemes: vec!["cluster-mtls".into()],
        }
    }
}

/// A tiered mesh.
pub struct Mesh {
    pub log: EventLog,
    pub agents: Vec<Agent>,
    pub threshold: f64,
}

/// What one cascade produced.
#[derive(Debug, Clone, PartialEq)]
pub struct Verdict {
    pub value: f64,
    pub decision: bool,
    pub up_to_seq: u64,
    /// Events appended during this cascade.
    pub events_appended: usize,
}

impl Mesh {
    pub fn new(stream: &str, agents: Vec<Agent>, threshold: f64) -> Self {
        Self {
            log: EventLog::new(stream),
            agents,
            threshold,
        }
    }

    /// Publish every agent's card. In production the card is registered in the
    /// noetl catalog (`POST /api/catalog/register`, `server/src/main.rs:89`) —
    /// the same "catalog as carrier" pattern the SLM F2 decision used for
    /// generated steps. Here we record the digest so topology is replayable.
    pub fn publish_cards(&mut self) {
        let pubs: Vec<MeshEvent> = self
            .agents
            .iter()
            .map(|a| {
                MeshEvent::AgentCardPublished(AgentCardPublished {
                    agent_id: a.id.clone(),
                    tier: a.tier,
                    card_digest: a.card().digest(),
                })
            })
            .collect();
        for p in pubs {
            self.log.append(p);
        }
    }

    /// Ingest one device reading (the collector's job).
    pub fn observe_signal(
        &mut self,
        device_id: &str,
        class: &str,
        value: f64,
        device_seq: u64,
    ) -> u64 {
        self.log.append(MeshEvent::SignalObserved(SignalObserved {
            device_id: device_id.to_string(),
            signal_class: class.to_string(),
            value,
            device_seq,
        }))
    }

    /// Run the cascade bottom-up at a named watermark.
    ///
    /// # The watermark advances per tier, and it has to
    ///
    /// ⚠⚠ The first version of this fixed ONE watermark for the whole cascade.
    /// That is wrong in a way worth recording: tier 0 emits its aggregates at
    /// sequences *above* the entry watermark, so every tier above read a prefix
    /// that structurally could not contain them and reduced zero inputs to
    /// 0.0. A single global watermark makes the upper tiers blind to the very
    /// cascade they are part of.
    ///
    /// So each tier reads at the log head **as of the moment the tier below
    /// finished**, and that per-tier watermark is recorded on every aggregate.
    /// Determinism is preserved because the watermark is still *named* rather
    /// than "latest": replaying the log reproduces the same per-tier bounds.
    ///
    /// Within a tier, all agents share one watermark, so siblings cannot see
    /// each other's output and the tier is order-independent.
    pub fn cascade(
        &mut self,
        up_to_seq: u64,
        reasoner: &dyn Reasoner,
    ) -> Result<Verdict, FoldError> {
        let before = self.log.records.len();
        let mut tiers: Vec<u8> = self.agents.iter().map(|a| a.tier).collect();
        tiers.sort_unstable();
        tiers.dedup();

        let mut last_value = 0.0;
        let mut top_seq = up_to_seq;
        // Watermark for the tier currently executing. Starts at the caller's
        // bound; advances to the head after each tier completes.
        let mut tier_watermark = up_to_seq;

        for tier in tiers {
            let ids: Vec<String> = self
                .agents
                .iter()
                .filter(|a| a.tier == tier)
                .map(|a| a.id.clone())
                .collect();

            for id in ids {
                let (how, children, agent_tier, signal_class) = {
                    let a = self.agents.iter().find(|a| a.id == id).expect("agent");
                    (a.how, a.children.clone(), a.tier, a.signal_class.clone())
                };

                // A2A Task: the tier above asks this agent for a value AS OF the
                // watermark. Recorded so the exchange is auditable.
                let task_id = format!("task-{id}-{tier_watermark}");
                let mut task = Task::submit(&task_id, "dispatcher", &id, tier_watermark);
                self.log
                    .append(MeshEvent::TaskTransitioned(TaskTransitioned {
                        task_id: task_id.clone(),
                        from_agent: "dispatcher".into(),
                        to_agent: id.clone(),
                        state: task.state.as_str().into(),
                    }));
                task.transition(TaskState::Working)
                    .expect("submitted->working");
                self.log
                    .append(MeshEvent::TaskTransitioned(TaskTransitioned {
                        task_id: task_id.clone(),
                        from_agent: "dispatcher".into(),
                        to_agent: id.clone(),
                        state: task.state.as_str().into(),
                    }));

                // Bounded read of everything at or below the watermark, then
                // narrow to this agent's children.
                let stream = self.log.stream.clone();
                let mut ctx = fold(&self.log.records, &stream, tier_watermark)?;
                if !children.is_empty() {
                    // An aggregator reduces its children only, never raw signals.
                    ctx.inputs.retain(|i| children.contains(&i.agent_id));
                    ctx.signals.clear();
                } else {
                    // A tier-0 agent reduces only its own signal class.
                    ctx.inputs.clear();
                    if let Some(class) = &signal_class {
                        ctx.signals = self
                            .log
                            .records
                            .iter()
                            .filter(|r| r.seq <= tier_watermark)
                            .filter_map(|r| match &r.payload {
                                MeshEvent::SignalObserved(sg) if &sg.signal_class == class => {
                                    Some(sg.value)
                                }
                                _ => None,
                            })
                            .collect();
                    }
                }

                let observed: Vec<u64> = self
                    .log
                    .records
                    .iter()
                    .filter(|r| r.seq <= tier_watermark)
                    .map(|r| r.seq)
                    .collect();

                let turns = run_cycle(&id, agent_tier, &ctx, how, reasoner);
                let mut value = 0.0;
                let mut pop = 0u32;
                for t in &turns {
                    self.log.append(MeshEvent::AgentReasoned(AgentReasoned {
                        agent_id: id.clone(),
                        tier: agent_tier,
                        phase: t.phase.as_str().into(),
                        thought: t.thought.clone(),
                        observed_seqs: observed.clone(),
                    }));
                    if let Some(v) = t.value {
                        value = v;
                    }
                    if let Some(p) = t.population {
                        pop = p;
                    }
                }

                let emitted = self
                    .log
                    .append(MeshEvent::AggregateEmitted(AggregateEmitted {
                        agent_id: id.clone(),
                        tier: agent_tier,
                        value,
                        input_count: pop,
                        up_to_seq: tier_watermark,
                    }));
                top_seq = emitted;
                last_value = value;

                task.transition(TaskState::Completed)
                    .expect("working->completed");
                self.log
                    .append(MeshEvent::TaskTransitioned(TaskTransitioned {
                        task_id,
                        from_agent: "dispatcher".into(),
                        to_agent: id.clone(),
                        state: task.state.as_str().into(),
                    }));
            }
            // The tier finished; the next tier may see what it emitted.
            tier_watermark = self.log.head();
        }

        let decision = last_value >= self.threshold;
        self.log
            .append(MeshEvent::VerdictSynthesised(VerdictSynthesised {
                agent_id: "top".into(),
                value: last_value,
                decision,
                threshold: self.threshold,
                up_to_seq: top_seq,
            }));

        Ok(Verdict {
            value: last_value,
            decision,
            up_to_seq: top_seq,
            events_appended: self.log.records.len() - before,
        })
    }
}
