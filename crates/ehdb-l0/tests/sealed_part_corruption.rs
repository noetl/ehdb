//! noetl/ehdb#426: a damaged length field inside a SEALED part must surface as
//! an error. The torn-tail rule (stop, keep the prefix) is for an active part a
//! crash interrupted; applied to a sealed part it silently drops every record
//! after the damage.

use std::sync::Arc;

use ehdb_l0::dataset::{D1EventLog, EventRecord};
use ehdb_l0::engine::{L0Config, L0Engine};
use ehdb_l0::substrate::{DurableSubstrate, LocalFsSubstrate};

#[test]
fn a_corrupt_length_in_a_sealed_part_fails_the_read_instead_of_shortening_it() {
    let root = std::env::temp_dir().join(format!("ehdb-426-sealed-corrupt-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let store: Arc<dyn DurableSubstrate> =
        Arc::new(LocalFsSubstrate::new(root.join("obj")).unwrap());
    let cfg = L0Config::d1(root.join("local"))
        .with_shard_count(1)
        .with_seal_max_records(4);
    let mut e: L0Engine<D1EventLog> = L0Engine::open(cfg, store).unwrap();
    for s in 1..=4 {
        e.append_record(EventRecord::new(s, "exec-1", "t", "p").with_event_id(format!("e{s}")))
            .unwrap();
    }
    e.flush_and_wait_uploads().unwrap();
    assert_eq!(e.read_index_after("exec-1", 0).unwrap().len(), 4);

    // Damage the second frame's length field in the sealed local part.
    let shard = root.join("local/parts/d1_event_log/shard-0");
    let part = std::fs::read_dir(&shard)
        .unwrap()
        .filter_map(|d| d.ok().map(|d| d.path()))
        .find(|p| p.extension().is_some_and(|x| x == "eslog"))
        .expect("a sealed part");
    let mut bytes = std::fs::read(&part).unwrap();
    let first_len = 12 + u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
    // Large, under the frame cap, and past the end of the part.
    let bogus = bytes.len() as u32;
    bytes[first_len + 4..first_len + 8].copy_from_slice(&bogus.to_le_bytes());
    std::fs::write(&part, &bytes).unwrap();

    match e.read_index_after("exec-1", 0) {
        Err(_) => {}
        Ok(recs) => panic!(
            "read returned {} of 4 records instead of failing",
            recs.len()
        ),
    }
    assert!(
        e.replay_all().is_err(),
        "replay_all silently shortened a sealed part"
    );
    let _ = std::fs::remove_dir_all(&root);
}
