//! **The D1 durability window exposition** (noetl/ehdb#328, F4).
//!
//! The window is the interval between an acknowledged append and that record
//! being durable on the substrate. Before this, nothing measured it: the only
//! metric naming lag (`upload_lag_micros_total`) is accumulated from the
//! **seal**, so a record waiting in an unsealed active part contributes zero —
//! and on a quiet shard that is the dominant term.
//!
//! These tests pin the exposition contract, including the two properties that
//! make the metric readable rather than merely present: a shard with nothing
//! pending must render `0` (not vanish), and the family must be
//! distinguishable from the consumer-backlog families it sits beside.

use ehdb_feed::{render_snapshot, render_unreplicated, LagSnapshot, ShardLag};
use ehdb_l0::ShardUnreplicated;

fn row(shard: u32, age_ms: u64, records: u64) -> ShardUnreplicated {
    ShardUnreplicated {
        shard,
        oldest_age_millis: age_ms,
        records,
    }
}

#[test]
fn an_idle_shard_renders_zero_rather_than_disappearing() {
    // Prometheus prunes empty metric families, so an unpinned labelled gauge is
    // absent until it first fires — making "nothing pending" and "this binary
    // has no such metric" identical on a scrape. A pinned 0 separates them.
    let text = render_unreplicated(&[row(0, 0, 0), row(1, 0, 0)]);
    assert!(text.contains("ehdb_l0_unreplicated_age_seconds{shard=\"0\"} 0.000\n"));
    assert!(text.contains("ehdb_l0_unreplicated_age_seconds{shard=\"1\"} 0.000\n"));
    assert!(text.contains("ehdb_l0_unreplicated_records{shard=\"0\"} 0\n"));
}

#[test]
fn the_headers_are_emitted_even_with_no_rows_at_all() {
    let text = render_unreplicated(&[]);
    assert!(text.contains("# TYPE ehdb_l0_unreplicated_age_seconds gauge\n"));
    assert!(text.contains("# TYPE ehdb_l0_unreplicated_records gauge\n"));
}

#[test]
fn the_age_is_rendered_in_seconds() {
    let text = render_unreplicated(&[row(0, 1_500, 3)]);
    assert!(
        text.contains("ehdb_l0_unreplicated_age_seconds{shard=\"0\"} 1.500\n"),
        "1500 ms must render as 1.500 s, got:\n{text}"
    );
    assert!(text.contains("ehdb_l0_unreplicated_records{shard=\"0\"} 3\n"));
}

#[test]
fn rows_render_in_shard_order_regardless_of_input_order() {
    let text = render_unreplicated(&[row(2, 0, 0), row(0, 0, 0), row(1, 0, 0)]);
    let idx = |s: &str| text.find(s).expect("series present");
    assert!(
        idx("age_seconds{shard=\"0\"}") < idx("age_seconds{shard=\"1\"}")
            && idx("age_seconds{shard=\"1\"}") < idx("age_seconds{shard=\"2\"}"),
        "scrapes must be byte-stable across samples"
    );
}

#[test]
fn durability_is_not_consumer_backlog() {
    // ⚠ The failure this guards: `ehdb_feed_shard_lag` is the name an alert
    // author reaches for first, and it measures how far a READER is behind —
    // nothing about replication. The two families must not be confusable.
    let backlog = render_snapshot(&LagSnapshot::shards_only(vec![ShardLag {
        shard: 0,
        committed: 10,
        lag: 500,
    }]));
    let durability = render_unreplicated(&[row(0, 0, 0)]);

    assert!(backlog.contains("ehdb_feed_shard_lag{shard=\"0\"} 500\n"));
    assert!(
        !backlog.contains("unreplicated"),
        "the backlog exposition must not carry durability series"
    );
    assert!(
        !durability.contains("ehdb_feed_"),
        "the durability exposition must not carry backlog series"
    );
    // A large consumer backlog says nothing about durability, and vice versa.
    assert!(durability.contains("ehdb_l0_unreplicated_age_seconds{shard=\"0\"} 0.000\n"));
}

#[test]
fn the_replicated_lag_histogram_is_well_formed_and_starts_empty() {
    let metrics = ehdb_l0::L0Metrics::new();
    let text = ehdb_feed::render_replicated_lag(&metrics);
    assert!(text.contains("# TYPE ehdb_l0_replicated_lag_seconds histogram\n"));
    // Present-and-zero before anything replicates, for the same absent≠zero
    // reason as the gauges.
    assert!(text.contains("ehdb_l0_replicated_lag_seconds_count 0\n"));
    assert!(text.contains("ehdb_l0_replicated_lag_seconds_bucket{le=\"+Inf\"} 0\n"));
    for b in ehdb_l0::metrics::REPLICATED_LAG_BUCKETS_SECONDS {
        assert!(
            text.contains(&format!("_bucket{{le=\"{b}\"}} 0\n")),
            "bucket {b} missing from:\n{text}"
        );
    }
}
