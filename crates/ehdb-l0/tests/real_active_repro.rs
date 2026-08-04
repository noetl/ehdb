//! noetl/ai-meta#209 — reproduce the deployed writer's recovery against the
//! REAL bytes taken off a live kind writer's unsealed active part.
//!
//! The engine-level tests pass and the deployed writer reports
//! `recovered_active_records = 0` after a hard kill with a verified 18-frame
//! unsealed part. Something differs between the synthetic fixture and the real
//! one; this test uses the real file so the difference cannot hide.

use std::sync::Arc;

use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{D1EventLog, L0Config, L0Engine, LocalFsSubstrate, ReplicaTarget};

#[test]
fn recovers_a_real_writer_active_part() {
    let bytes = match std::fs::read(concat!(env!("CARGO_MANIFEST_DIR"), "/../../real_active.bin")) {
        Ok(b) => b,
        Err(_) => {
            eprintln!("real_active.bin absent — skipping (this test is a diagnostic)");
            return;
        }
    };
    assert!(!bytes.is_empty(), "the captured part must not be empty");

    let root = std::env::temp_dir().join(format!(
        "ehdb-real-active-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    // Lay the real file down exactly where the writer would have it:
    //   <local_root>/parts/<dataset>/shard-0/part-000000.active
    let part_dir = root.join("parts").join("d1_event_log").join("shard-0");
    std::fs::create_dir_all(&part_dir).unwrap();
    std::fs::write(part_dir.join("part-000000.active"), &bytes).unwrap();

    let substrate: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&root).unwrap());
    let engine = L0Engine::<D1EventLog>::open_replicated(
        L0Config::d1(&root).with_shard_count(1),
        vec![ReplicaTarget::new("replica-0", substrate)],
    )
    .expect("engine must open over a real unsealed part");

    let snap = engine.metrics().snapshot();
    eprintln!(
        "recovered_active_records = {}  (file was {} bytes)",
        snap.recovered_active_records,
        bytes.len()
    );
    assert!(
        snap.recovered_active_records > 0,
        "the real active part holds intact frames, so recovery must count them; got 0"
    );
}
