//! **In-flight depth, exposed** (noetl/ai-meta#455 C6).
//!
//! `ShardConsumerGroup::inflight_len` and `SubjectConsumerGroup::inflight_len` have
//! existed since consumer groups landed, and **no `render_*` function emitted either**.
//! The accessors' existence made the capability look present; a scrape could not see
//! consumer backlog depth at all.
//!
//! # Why it is not derivable from lag
//!
//! `ehdb_feed_shard_lag` is the whole backlog — **undelivered plus unacked**. So polling a
//! record without acking it moves nothing: it leaves `pending` and enters `inflight`, and
//! the sum is unchanged. That is the property this file pins:
//!
//! > **There exist two states with identical `lag` and different `inflight`**, and they are
//! > opposite operational conditions — a stalled consumer and a busy one.
//!
//! An alert on lag alone pages for both and can distinguish neither. That is the same
//! defect shape as a metric whose absence and whose healthy zero look the same.

use std::sync::Arc;

use ehdb_feed::{render_snapshot, LagSnapshot, ShardConsumerGroup, ShardLag};
use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{D1EventLog, L0Config, L0Engine, LocalFsSubstrate};

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("ehdb-inflight-{tag}-{}-{n}", std::process::id()))
}

fn open(dir: &std::path::Path, obj: &std::path::Path) -> L0Engine<D1EventLog> {
    std::fs::create_dir_all(dir).unwrap();
    std::fs::create_dir_all(obj).unwrap();
    let store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(obj).unwrap());
    L0Engine::<D1EventLog>::open(L0Config::d1(dir).with_shard_count(4), store).unwrap()
}

/// Parse `name{labels} value` lines into a map keyed by the whole `name{labels}` token.
fn parse(text: &str) -> std::collections::BTreeMap<String, f64> {
    text.lines()
        .filter(|l| !l.starts_with('#') && !l.trim().is_empty())
        .filter_map(|l| {
            let (k, v) = l.rsplit_once(' ')?;
            Some((k.to_string(), v.parse().ok()?))
        })
        .collect()
}

#[test]
fn inflight_is_present_at_zero_not_absent() {
    let text = render_snapshot(&LagSnapshot::shards_only(vec![
        ShardLag {
            shard: 0,
            committed: 0,
            lag: 0,
            inflight: 0,
        },
        ShardLag {
            shard: 1,
            committed: 0,
            lag: 0,
            inflight: 0,
        },
    ]));
    let m = parse(&text);
    assert_eq!(
        m.get("ehdb_feed_shard_inflight{shard=\"0\"}"),
        Some(&0.0),
        "the inflight series must be PRESENT at 0, not absent — an absent series and a \
         healthy zero are indistinguishable to every alert:\n{text}"
    );
    assert_eq!(
        m.get("ehdb_feed_shard_inflight{shard=\"1\"}"),
        Some(&0.0),
        "every shard must be emitted"
    );
    assert_eq!(
        m.get("ehdb_feed_total_inflight"),
        Some(&0.0),
        "the aggregate must be present at 0 too"
    );
    assert!(
        text.contains("# TYPE ehdb_feed_shard_inflight gauge"),
        "the TYPE header must be declared"
    );
}

#[test]
fn the_existing_lag_lines_are_byte_identical_after_adding_the_family() {
    // ⚠ KEDA's metrics-api scaler in `format: prometheus` matches `valueLocation`
    // as a prefix of the whole `name{labels}` token and takes the FIRST matching
    // line — it has no label selector. So a change to the spacing or label set of
    // the lag lines breaks every ScaledObject pointing at them (noetl/ai-meta#194).
    // The new family is appended as its own block; these exact bytes must survive.
    let text = render_snapshot(&LagSnapshot::shards_only(vec![ShardLag {
        shard: 0,
        committed: 7,
        lag: 12,
        inflight: 3,
    }]));
    assert!(
        text.contains("ehdb_feed_shard_lag{shard=\"0\"} 12\n"),
        "the lag line's bytes changed:\n{text}"
    );
    assert!(
        text.contains("ehdb_feed_shard_committed{shard=\"0\"} 7\n"),
        "the committed line's bytes changed:\n{text}"
    );
    assert!(
        text.contains("ehdb_feed_total_lag 12\n"),
        "the total_lag line's bytes changed:\n{text}"
    );
}

#[test]
fn lag_and_inflight_distinguish_a_stalled_consumer_from_a_busy_one() {
    let (obj, local) = (unique_dir("obj"), unique_dir("local"));
    let mut engine = open(&local, &obj);

    // 10 records into shard 0's key space.
    let shard0: Vec<String> = (0..)
        .map(|i| format!("k{i}"))
        .filter(|k| engine.shard_for(k) == 0)
        .take(10)
        .collect();
    for k in &shard0 {
        engine.append(k, "t", "cmd").unwrap();
    }

    let mut group = ShardConsumerGroup::<D1EventLog>::new(0, 100, 0);

    // --- State A: records exist, nobody is polling. A STALLED consumer. ---
    let lag_a = group.lag(&engine).unwrap();
    let inflight_a = group.inflight_len() as u64;
    assert_eq!(inflight_a, 0, "nothing polled yet");
    assert!(lag_a > 0, "there is a backlog; got {lag_a}");

    // --- State B: poll 4 without acking. A BUSY consumer, same backlog. ---
    for i in 0..4 {
        let d = group
            .poll_assign(&engine, 1, 0)
            .unwrap()
            .unwrap_or_else(|| panic!("poll {i} returned nothing with a backlog of {lag_a}"));
        // Deliberately NOT acked.
        let _ = d;
    }
    let lag_b = group.lag(&engine).unwrap();
    let inflight_b = group.inflight_len() as u64;

    println!(
        "state A (stalled): lag={lag_a} inflight={inflight_a}\n\
         state B (busy):    lag={lag_b} inflight={inflight_b}"
    );

    // THE POINT. Same lag, different inflight — so lag cannot tell the two apart.
    assert_eq!(
        lag_a, lag_b,
        "lag must be unchanged by polling without acking (it counts undelivered PLUS \
         unacked). If these differ, the premise of this gauge is wrong and the whole \
         argument for it needs re-checking."
    );
    assert_eq!(inflight_b, 4, "4 records were assigned and none acked");
    assert_ne!(
        inflight_a, inflight_b,
        "inflight must separate the two states that lag renders identically"
    );

    // And it comes back down on ack — a gauge that only rises is a counter.
    let mut acked = 0;
    for sk in 1..=10u64 {
        if group.ack(sk) {
            acked += 1;
        }
    }
    let inflight_c = group.inflight_len() as u64;
    let lag_c = group.lag(&engine).unwrap();
    println!("state C (acked {acked}): lag={lag_c} inflight={inflight_c}");
    assert_eq!(
        inflight_c, 0,
        "inflight must fall to 0 once everything assigned is acked"
    );
    assert!(
        lag_c < lag_a,
        "acking the prefix must reduce lag ({lag_a} -> {lag_c})"
    );

    // Finally: the rendered exposition carries what the accessor says.
    let text = render_snapshot(&LagSnapshot::shards_only(vec![ShardLag {
        shard: 0,
        committed: group.committed_cursor(),
        lag: lag_c,
        inflight: inflight_c,
    }]));
    let m = parse(&text);
    assert_eq!(
        m.get("ehdb_feed_shard_inflight{shard=\"0\"}"),
        Some(&(inflight_c as f64)),
        "the rendered value must equal the accessor it is derived from"
    );
}
