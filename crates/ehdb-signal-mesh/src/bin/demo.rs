//! Runnable demo: one signal cascading through three tiers, end to end.
//!
//!     cargo run -p ehdb-signal-mesh --bin signal-mesh-demo
//!
//! Deterministic: same output every run, no clock, no network, no model.

use ehdb_signal_mesh::event::MeshEvent;
use ehdb_signal_mesh::fold::Reduction;
use ehdb_signal_mesh::mesh::{Agent, Mesh};
use ehdb_signal_mesh::react::DeterministicReasoner;

fn main() {
    // 6 devices -> 2 tier-0 agents (one per signal class) -> 1 tier-1
    // aggregator -> 1 tier-2 synthesiser.
    let agents = vec![
        Agent {
            id: "t0-temp".into(),
            tier: 0,
            how: Reduction::WeightedMean,
            children: vec![],
            signal_class: Some("temp".into()),
        },
        Agent {
            id: "t0-vibe".into(),
            tier: 0,
            how: Reduction::WeightedMean,
            children: vec![],
            signal_class: Some("vibe".into()),
        },
        Agent {
            id: "t1-site".into(),
            tier: 1,
            how: Reduction::WeightedMean,
            children: vec!["t0-temp".into(), "t0-vibe".into()],
            signal_class: None,
        },
        Agent {
            id: "t2-fleet".into(),
            tier: 2,
            how: Reduction::Max,
            children: vec!["t1-site".into()],
            signal_class: None,
        },
    ];
    let mut mesh = Mesh::new("exec-demo-1", agents, 50.0);

    mesh.publish_cards();
    println!("== Agent Cards published (A2A discovery) ==");
    for a in &mesh.agents {
        let c = a.card();
        println!(
            "  tier {} {:<9} digest={} skills={}",
            a.tier,
            a.id,
            c.digest(),
            c.skills.len()
        );
    }
    println!(
        "  well-known path: {}",
        ehdb_signal_mesh::a2a::AGENT_CARD_WELL_KNOWN_PATH
    );

    println!("\n== Collector appends signals ==");
    for (i, (dev, class, val)) in [
        ("dev-01", "temp", 41.0),
        ("dev-02", "temp", 47.0),
        ("dev-03", "temp", 44.0),
        ("dev-04", "vibe", 61.0),
        ("dev-05", "vibe", 59.0),
        ("dev-06", "vibe", 63.0),
    ]
    .iter()
    .enumerate()
    {
        let seq = mesh.observe_signal(dev, class, *val, i as u64 + 1);
        println!("  seq {seq:>3}  {dev} {class}={val}");
    }

    let watermark = mesh.log.head();
    println!("\n== Cascade at watermark seq={watermark} ==");
    let v = mesh
        .cascade(watermark, &DeterministicReasoner)
        .expect("cascade folds");

    println!("\n== Reasoning trace (ReAct turns, replayable) ==");
    for r in mesh.log.records.iter().filter(|r| r.seq > watermark) {
        if let MeshEvent::AgentReasoned(a) = &r.payload {
            println!("  seq {:>3} [{}] {}", r.seq, a.phase, a.thought);
        }
    }

    println!("\n== TOP-TIER VERDICT ==");
    println!("  numeric value : {:.4}", v.value);
    println!(
        "  boolean       : {}   (threshold {:.1})",
        v.decision, mesh.threshold
    );
    println!("  events appended by the cascade: {}", v.events_appended);
    println!("  total log records: {}", mesh.log.records.len());

    // Replay: fold the same prefix again and confirm the same verdict.
    let replay = ehdb_signal_mesh::fold::fold(&mesh.log.records, &mesh.log.stream, watermark)
        .expect("replay folds");
    println!("\n== Replay check ==");
    println!(
        "  re-folded {} signal(s) through seq {} (staleness {})",
        replay.signals.len(),
        replay.folded_through,
        replay.staleness()
    );
    println!("  complete at watermark: {}", replay.is_complete());
}
