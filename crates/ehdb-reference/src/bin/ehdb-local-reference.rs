use std::{collections::HashMap, env, path::PathBuf, process};

use ehdb_core::EhdbError;
use ehdb_reference::compare_projection_parity;
use ehdb_reference::{
    ack_local_reference_event_consumer_json, append_local_reference_domain_record_json,
    bind_local_reference_system_channel_json, compare_shadow_parity,
    consume_local_reference_event_records_json, exercise_affinity_single_writer,
    exercise_durable_recovery, exercise_primary_serve,
    ingest_local_reference_retrieval_document_json, publish_local_reference_system_module_json,
    read_local_reference_domain_records_json, resolve_local_reference_system_module_json,
    retrieve_local_reference_context, summarize_local_reference_json, AckEventConsumerRequest,
    AffinityRoutedEventLog, AppendDomainRecordRequest, AuthoritativeExecutionState,
    BindSystemChannelRequest, ConsumeEventRecordsRequest, EventLogAckRequest,
    EventLogAppendRequest, EventLogDriver, EventLogPrimaryEvent, EventLogReadExecutionRequest,
    EventLogScanRequest, EventLogTailRequest, IngestChunkInput, IngestRetrievalDocumentRequest,
    KvPrimaryInput, LocalReferenceEventLogDriver, LocalReferenceKvStateDriver,
    LocalReferenceObjectBlobDriver, LocalReferenceProjectionEngine, LocalReferenceVectorDriver,
    ObjectPrimaryInput, ProjectionApplyRequest, ProjectionDriver, ProjectionEventInput,
    ProjectionPrimaryInput, PublishSystemModuleRequest, ReadDomainRecordsRequest,
    ResolveSystemModuleRequest, RetrievalOutcome, RetrieveContextRequest, Routed, ServedBy,
    ShardOwnership, VectorPrimaryInput, DEFAULT_LOCAL_REFERENCE_NAMESPACE,
    DEFAULT_LOCAL_REFERENCE_TENANT,
};

fn main() {
    match run(env::args().skip(1).collect()) {
        Ok((output, code)) => {
            println!("{output}");
            process::exit(code);
        }
        Err(err) => {
            eprintln!("{err}");
            process::exit(2);
        }
    }
}

fn run(args: Vec<String>) -> Result<(String, i32), String> {
    match args.split_first() {
        None => Err(usage().to_string()),
        Some((command, rest)) if command == "--help" || command == "-h" => {
            if rest.is_empty() {
                Ok((usage().to_string(), 0))
            } else {
                Err(usage().to_string())
            }
        }
        Some((command, rest)) if command == "summary" => run_summary(rest).map(ok0),
        Some((command, rest)) if command == "append" => run_append(rest).map(ok0),
        Some((command, rest)) if command == "read" => run_read(rest).map(ok0),
        Some((command, rest)) if command == "consume" => run_consume(rest).map(ok0),
        Some((command, rest)) if command == "ack" => run_ack(rest).map(ok0),
        Some((command, rest)) if command == "publish-system" => run_publish_system(rest).map(ok0),
        Some((command, rest)) if command == "bind-system" => run_bind_system(rest).map(ok0),
        Some((command, rest)) if command == "resolve-system" => run_resolve_system(rest).map(ok0),
        Some((command, rest)) if command == "ingest-doc" => run_ingest(rest).map(ok0),
        Some((command, rest)) if command == "retrieve" => run_retrieve(rest),
        Some((command, rest)) if command == "eventlog-append" => run_eventlog_append(rest),
        Some((command, rest)) if command == "eventlog-scan" => run_eventlog_scan(rest),
        Some((command, rest)) if command == "eventlog-read-exec" => run_eventlog_read_exec(rest),
        Some((command, rest)) if command == "eventlog-tail" => run_eventlog_tail(rest),
        Some((command, rest)) if command == "eventlog-ack" => run_eventlog_ack(rest),
        Some((command, rest)) if command == "eventlog-suite" => run_eventlog_suite(rest),
        Some((command, rest)) if command == "eventlog-primary-serve" => {
            run_eventlog_primary_serve(rest)
        }
        Some((command, rest)) if command == "durable-eventlog-recovery" => {
            run_durable_eventlog_recovery(rest)
        }
        Some((command, rest)) if command == "durable-eventlog-affinity" => {
            run_durable_eventlog_affinity(rest)
        }
        Some((command, rest)) if command == "durable-eventlog-affinity-append" => {
            run_durable_eventlog_affinity_append(rest)
        }
        Some((command, rest)) if command == "durable-eventlog-affinity-read" => {
            run_durable_eventlog_affinity_read(rest)
        }
        Some((command, rest)) if command == "projection-apply" => run_projection_apply(rest),
        Some((command, rest)) if command == "projection-read-exec" => {
            run_projection_read_exec(rest)
        }
        Some((command, rest)) if command == "projection-read-event" => {
            run_projection_read_event(rest)
        }
        Some((command, rest)) if command == "projection-list" => run_projection_list(rest),
        Some((command, rest)) if command == "projection-checkpoint" => {
            run_projection_checkpoint(rest)
        }
        Some((command, rest)) if command == "projection-from-eventlog" => {
            run_projection_from_eventlog(rest)
        }
        Some((command, rest)) if command == "projection-suite" => run_projection_suite(rest),
        Some((command, rest)) if command == "projection-primary-serve" => {
            run_projection_primary_serve(rest)
        }
        Some((command, rest)) if command == "kv-primary-serve" => run_kv_primary_serve(rest),
        Some((command, rest)) if command == "object-primary-serve" => {
            run_object_primary_serve(rest)
        }
        Some((command, rest)) if command == "vector-primary-serve" => {
            run_vector_primary_serve(rest)
        }
        _ => Err(usage().to_string()),
    }
}

/// Classify an engine error into the selfcheck exit-code contract:
/// 3 = rejected (bound/state), 4 = invalid (bad identifier), 5 = unavailable.
fn eventlog_exit_code(err: &EhdbError) -> i32 {
    match err {
        EhdbError::InvalidIdentifier(_) => 4,
        EhdbError::InvalidState(_) => 3,
        _ => 5,
    }
}

/// Build the event-log driver from the shared `--log` / `--tenant` / `--namespace`
/// flags (defaults match the rest of the local-reference verbs).
fn eventlog_driver(
    flags: &mut HashMap<String, String>,
) -> Result<LocalReferenceEventLogDriver, String> {
    let log = take_required(flags, "log")?;
    let tenant = flags
        .remove("tenant")
        .unwrap_or_else(|| DEFAULT_LOCAL_REFERENCE_TENANT.to_string());
    let namespace = flags
        .remove("namespace")
        .unwrap_or_else(|| DEFAULT_LOCAL_REFERENCE_NAMESPACE.to_string());
    Ok(LocalReferenceEventLogDriver::new(
        PathBuf::from(log),
        tenant,
        namespace,
    ))
}

fn run_eventlog_append(args: &[String]) -> Result<(String, i32), String> {
    let mut flags = parse_flags(args)?;
    let driver = eventlog_driver(&mut flags)?;
    let execution_id = take_required(&mut flags, "execution-id")?;
    let transaction_id = take_required(&mut flags, "transaction-id")?;
    let payload = take_required(&mut flags, "payload")?;
    ensure_no_unknown_flags(&flags)?;
    match driver.append(&EventLogAppendRequest {
        execution_id,
        transaction_id,
        payload,
    }) {
        Ok(outcome) => Ok((json(&outcome)?, 0)),
        Err(err) => Ok((json_error(&err)?, eventlog_exit_code(&err))),
    }
}

fn run_eventlog_scan(args: &[String]) -> Result<(String, i32), String> {
    let mut flags = parse_flags(args)?;
    let driver = eventlog_driver(&mut flags)?;
    let limit = parse_limit(&mut flags, 100)?;
    let after = parse_after(&mut flags)?;
    ensure_no_unknown_flags(&flags)?;
    match driver.scan_global(&EventLogScanRequest { after, limit }) {
        Ok(outcome) => Ok((json(&outcome)?, 0)),
        Err(err) => Ok((json_error(&err)?, eventlog_exit_code(&err))),
    }
}

fn run_eventlog_read_exec(args: &[String]) -> Result<(String, i32), String> {
    let mut flags = parse_flags(args)?;
    let driver = eventlog_driver(&mut flags)?;
    let execution_id = take_required(&mut flags, "execution-id")?;
    let limit = parse_limit(&mut flags, 100)?;
    let after = parse_after(&mut flags)?;
    ensure_no_unknown_flags(&flags)?;
    match driver.read_execution(&EventLogReadExecutionRequest {
        execution_id,
        after,
        limit,
    }) {
        Ok(outcome) => Ok((json(&outcome)?, 0)),
        Err(err) => Ok((json_error(&err)?, eventlog_exit_code(&err))),
    }
}

fn run_eventlog_tail(args: &[String]) -> Result<(String, i32), String> {
    let mut flags = parse_flags(args)?;
    let driver = eventlog_driver(&mut flags)?;
    let consumer = take_required(&mut flags, "consumer")?;
    let transaction_id = take_required(&mut flags, "transaction-id")?;
    let limit = parse_limit(&mut flags, 100)?;
    ensure_no_unknown_flags(&flags)?;
    match driver.tail(&EventLogTailRequest {
        consumer,
        transaction_id,
        limit,
    }) {
        Ok(outcome) => Ok((json(&outcome)?, 0)),
        Err(err) => Ok((json_error(&err)?, eventlog_exit_code(&err))),
    }
}

fn run_eventlog_ack(args: &[String]) -> Result<(String, i32), String> {
    let mut flags = parse_flags(args)?;
    let driver = eventlog_driver(&mut flags)?;
    let consumer = take_required(&mut flags, "consumer")?;
    let transaction_id = take_required(&mut flags, "transaction-id")?;
    let sequence = take_required(&mut flags, "sequence")?
        .parse::<u64>()
        .map_err(|_| "invalid --sequence value".to_string())?;
    ensure_no_unknown_flags(&flags)?;
    match driver.ack(&EventLogAckRequest {
        consumer,
        transaction_id,
        sequence,
    }) {
        Ok(outcome) => Ok((json(&outcome)?, 0)),
        Err(err) => Ok((json_error(&err)?, eventlog_exit_code(&err))),
    }
}

/// Deterministic one-process drive of the whole engine surface:
/// append(exec A)×2 → append(exec B) → scan(global order) → read-exec(A scoped)
/// → tail(create+pending) → ack → tail(cursor advanced) → shadow-parity check.
/// Exit 0 only when every expected outcome AND the parity report hold.
fn run_eventlog_suite(args: &[String]) -> Result<(String, i32), String> {
    let mut flags = parse_flags(args)?;
    let driver = eventlog_driver(&mut flags)?;
    ensure_no_unknown_flags(&flags)?;

    let mut steps = Vec::new();
    let mut ok = true;

    // Append two events for exec 100 and one for exec 200 (global seq 1,2,3).
    let mut prev_seq = 0u64;
    let mut mirrored = 0usize;
    let mut parity_ok = true;
    for (exec, auth_seq) in [("100", 1u64), ("100", 2), ("200", 3)] {
        let a = driver
            .append(&EventLogAppendRequest {
                execution_id: exec.to_string(),
                transaction_id: format!("suite-{exec}-{auth_seq}"),
                payload: format!("{{\"exec\":\"{exec}\",\"seq\":{auth_seq}}}"),
            })
            .map_err(|err| err.to_string())?;
        mirrored += 1;
        let report = compare_shadow_parity(Some(auth_seq), &a, prev_seq, mirrored);
        parity_ok &= report.holds();
        prev_seq = a.global_sequence;
        steps.push(serde_json::json!({
            "step": "append", "execution_id": exec,
            "global_sequence": a.global_sequence, "parity_holds": report.holds(),
        }));
    }
    ok &= parity_ok;

    let scan = driver
        .scan_global(&EventLogScanRequest {
            after: None,
            limit: 100,
        })
        .map_err(|err| err.to_string())?;
    let global_order: Vec<u64> = scan.records.iter().map(|r| r.global_sequence).collect();
    ok &= global_order == vec![1, 2, 3];
    steps.push(serde_json::json!({"step": "scan_global", "order": global_order}));

    let ex = driver
        .read_execution(&EventLogReadExecutionRequest {
            execution_id: "100".to_string(),
            after: None,
            limit: 100,
        })
        .map_err(|err| err.to_string())?;
    // Exec 100 scoped read returns its two events (global seq 1 and 2).
    ok &= ex.returned == 2 && ex.records.iter().all(|r| r.execution_id == "100");
    steps.push(serde_json::json!({"step": "read_execution", "returned": ex.returned}));

    let t1 = driver
        .tail(&EventLogTailRequest {
            consumer: "suite-projector".to_string(),
            transaction_id: "suite-c1".to_string(),
            limit: 100,
        })
        .map_err(|err| err.to_string())?;
    ok &= t1.created_consumer && t1.pending_count == 3;
    steps.push(serde_json::json!({"step": "tail_initial", "pending": t1.pending_count}));

    driver
        .ack(&EventLogAckRequest {
            consumer: "suite-projector".to_string(),
            transaction_id: "suite-ack1".to_string(),
            sequence: 1,
        })
        .map_err(|err| err.to_string())?;

    let t2 = driver
        .tail(&EventLogTailRequest {
            consumer: "suite-projector".to_string(),
            transaction_id: "suite-c2".to_string(),
            limit: 100,
        })
        .map_err(|err| err.to_string())?;
    ok &= t2.pending_count == 2 && t2.acked_sequence == Some(1);
    steps.push(serde_json::json!({"step": "tail_after_ack", "pending": t2.pending_count}));

    let report = serde_json::json!({
        "suite": "ehdb-eventlog", "driver": driver.driver_name(),
        "ok": ok, "parity_ok": parity_ok, "steps": steps,
    });
    let output = serde_json::to_string(&report).map_err(|err| err.to_string())?;
    Ok((output, if ok { 0 } else { 1 }))
}

/// Authoritative primary-serve cycle (completion program Phase 9, tier 1):
/// drive append + global scan + per-execution read + durable tail + ack +
/// fresh-driver replay through the EHDB engine and emit the served-by-EHDB
/// proof (with dual-run parity against a 1-based incumbent sequence).  Exit 0
/// only when [`EventLogPrimaryServeReport::served_by_ehdb`] holds.
fn run_eventlog_primary_serve(args: &[String]) -> Result<(String, i32), String> {
    let mut flags = parse_flags(args)?;
    let driver = eventlog_driver(&mut flags)?;
    let consumer = flags
        .remove("consumer")
        .unwrap_or_else(|| "primary-serve-projector".to_string());
    ensure_no_unknown_flags(&flags)?;

    // Deterministic drive: two executions interleaved with known 1-based
    // authoritative sequences so the dual-run parity check is exact.
    let events = [("100", 1u64), ("200", 2), ("100", 3)]
        .into_iter()
        .map(|(exec, seq)| EventLogPrimaryEvent {
            execution_id: exec.to_string(),
            transaction_id: format!("primary-{exec}-{seq}"),
            payload: format!("{{\"exec\":\"{exec}\",\"seq\":{seq}}}"),
            authoritative_sequence: Some(seq),
        })
        .collect::<Vec<_>>();

    match exercise_primary_serve(&driver, &events, &consumer, "primary-ack") {
        Ok(report) => {
            let served = report.served_by_ehdb();
            let output = serde_json::to_string(&serde_json::json!({
                "suite": "ehdb-eventlog-primary-serve",
                "driver": report.driver_name,
                "served_by_ehdb": served,
                "report": report,
            }))
            .map_err(|err| err.to_string())?;
            Ok((output, if served { 0 } else { 1 }))
        }
        Err(err) => Ok((json_error(&err)?, eventlog_exit_code(&err))),
    }
}

/// Durable, segmented event-log crash-recovery drive (Phase 9 primary-serve
/// prerequisite): append a deterministic event set + ack a durable-consumer
/// cursor through the durable segment backend, then **reopen a fresh driver
/// over the same root** (simulated pod restart) and prove the reopened store
/// serves the identical record set with zero loss, gapless ordering,
/// per-execution scope, payload fidelity, and durable-cursor survival —
/// replay-is-truth from the durable segments the runbook §C durability gate
/// requires beyond `local_reference`.  `--root` is the store directory (not a
/// single JSONL file).  Exit 0 only when [`DurableRecoveryReport::recovered`]
/// holds.
fn run_durable_eventlog_recovery(args: &[String]) -> Result<(String, i32), String> {
    let mut flags = parse_flags(args)?;
    let root = take_required(&mut flags, "root")?;
    let consumer = flags
        .remove("consumer")
        .unwrap_or_else(|| "durable-recovery-projector".to_string());
    ensure_no_unknown_flags(&flags)?;

    // Deterministic drive: two executions interleaved so the reopened scope +
    // ordering + cursor checks are exact.
    let events: Vec<EventLogAppendRequest> = [("100", 1u64), ("200", 2), ("100", 3)]
        .into_iter()
        .map(|(exec, seq)| EventLogAppendRequest {
            execution_id: exec.to_string(),
            transaction_id: format!("durable-{exec}-{seq}"),
            payload: format!("{{\"exec\":\"{exec}\",\"seq\":{seq}}}"),
        })
        .collect();

    match exercise_durable_recovery(PathBuf::from(root), &events, &consumer) {
        Ok(report) => {
            let recovered = report.recovered();
            let output = serde_json::to_string(&serde_json::json!({
                "suite": "ehdb-durable-eventlog-recovery",
                "driver": report.driver_name,
                "recovered": recovered,
                "report": report,
            }))
            .map_err(|err| err.to_string())?;
            Ok((output, if recovered { 0 } else { 1 }))
        }
        Err(err) => Ok((json_error(&err)?, eventlog_exit_code(&err))),
    }
}

/// Distinct exit code for a routed write refused because this replica does not
/// own the target shard (route it to the owner). Kept apart from the engine
/// error codes (3 rejected / 4 invalid / 5 unavailable) so a shell / kind-soak
/// harness can assert "refused as non-owner" distinctly from a real failure.
const EXIT_NOT_OWNER: i32 = 6;

/// Build a single replica's affinity-routed durable event log from the shared
/// `--root` / `--shard-index` / `--shard-count` flags.
fn affinity_routed_log(
    flags: &mut HashMap<String, String>,
) -> Result<AffinityRoutedEventLog, String> {
    let root = take_required(flags, "root")?;
    let shard_count = match flags.remove("shard-count") {
        Some(raw) => raw
            .parse::<u32>()
            .map_err(|_| format!("invalid --shard-count value: {raw}"))?,
        None => 1,
    };
    let shard_index = match flags.remove("shard-index") {
        Some(raw) => raw
            .parse::<u32>()
            .map_err(|_| format!("invalid --shard-index value: {raw}"))?,
        None => 0,
    };
    let ownership = ShardOwnership::new(shard_index, shard_count).map_err(|err| err.to_string())?;
    AffinityRoutedEventLog::open(PathBuf::from(root), ownership).map_err(|err| err.to_string())
}

/// Execution-affinity single-writer drive (completion program, durable
/// event-log backend slice 2): spin up a `--shard-count`-replica pool over one
/// `--root`, partition a deterministic execution set, and prove owner-writes /
/// non-owner-refused / single-writer-invariant / non-owner-cold-load-read /
/// crash-recovery-under-ownership.  Exit 0 only when
/// [`AffinitySingleWriterReport::holds`] holds.
fn run_durable_eventlog_affinity(args: &[String]) -> Result<(String, i32), String> {
    let mut flags = parse_flags(args)?;
    let root = take_required(&mut flags, "root")?;
    let shard_count = match flags.remove("shard-count") {
        Some(raw) => raw
            .parse::<u32>()
            .map_err(|_| format!("invalid --shard-count value: {raw}"))?,
        None => 2,
    };
    ensure_no_unknown_flags(&flags)?;

    match exercise_affinity_single_writer(PathBuf::from(root), shard_count) {
        Ok(report) => {
            let holds = report.holds();
            let output = serde_json::to_string(&serde_json::json!({
                "suite": "ehdb-durable-eventlog-affinity",
                "shard_count": report.shard_count,
                "single_writer_holds": holds,
                "report": report,
            }))
            .map_err(|err| err.to_string())?;
            Ok((output, if holds { 0 } else { 1 }))
        }
        Err(err) => Ok((json_error(&err)?, eventlog_exit_code(&err))),
    }
}

/// One affinity-routed append: the owner writes (exit 0), a non-owner is refused
/// with no side effect (exit [`EXIT_NOT_OWNER`], distinct from engine errors) —
/// the routed decision a shell / kind-soak harness asserts.
fn run_durable_eventlog_affinity_append(args: &[String]) -> Result<(String, i32), String> {
    let mut flags = parse_flags(args)?;
    let log = affinity_routed_log(&mut flags)?;
    let execution_id = take_required(&mut flags, "execution-id")?;
    let transaction_id = take_required(&mut flags, "transaction-id")?;
    let payload = take_required(&mut flags, "payload")?;
    ensure_no_unknown_flags(&flags)?;

    match log.append(&EventLogAppendRequest {
        execution_id: execution_id.clone(),
        transaction_id,
        payload,
    }) {
        Ok(Routed::Served(outcome)) => {
            let output = serde_json::to_string(&serde_json::json!({
                "routed": "served",
                "shard_index": log.ownership().shard_index(),
                "shard_count": log.ownership().shard_count(),
                "shard": log.shard_of(&execution_id),
                "outcome": outcome,
            }))
            .map_err(|err| err.to_string())?;
            Ok((output, 0))
        }
        Ok(Routed::NotOwner { owner_shard }) => {
            let output = serde_json::to_string(&serde_json::json!({
                "routed": "not_owner",
                "shard_index": log.ownership().shard_index(),
                "shard_count": log.ownership().shard_count(),
                "owner_shard": owner_shard,
                "execution_id": execution_id,
            }))
            .map_err(|err| err.to_string())?;
            Ok((output, EXIT_NOT_OWNER))
        }
        Err(err) => Ok((json_error(&err)?, eventlog_exit_code(&err))),
    }
}

/// One affinity-routed per-execution read: the owner serves it resident, a
/// non-owner cold-loads the durable segments read-only.  Exit 0 (reads always
/// succeed); the JSON `served_by` shows which path served it.
fn run_durable_eventlog_affinity_read(args: &[String]) -> Result<(String, i32), String> {
    let mut flags = parse_flags(args)?;
    let log = affinity_routed_log(&mut flags)?;
    let execution_id = take_required(&mut flags, "execution-id")?;
    let limit = parse_limit(&mut flags, 100)?;
    let after = parse_after(&mut flags)?;
    ensure_no_unknown_flags(&flags)?;

    match log.read_execution(&EventLogReadExecutionRequest {
        execution_id: execution_id.clone(),
        after,
        limit,
    }) {
        Ok(read) => {
            let served_by = match read.served_by {
                ServedBy::OwnerResident => "owner_resident",
                ServedBy::NonOwnerColdLoad => "non_owner_cold_load",
            };
            let output = serde_json::to_string(&serde_json::json!({
                "served_by": served_by,
                "shard_index": log.ownership().shard_index(),
                "shard_count": log.ownership().shard_count(),
                "shard": log.shard_of(&execution_id),
                "outcome": read.outcome,
            }))
            .map_err(|err| err.to_string())?;
            Ok((output, 0))
        }
        Err(err) => Ok((json_error(&err)?, eventlog_exit_code(&err))),
    }
}

// ---------------------------------------------------------------------------
// Projection / read-model engine verbs (completion program Phase 7)
// ---------------------------------------------------------------------------

/// Build the projection engine from the shared `--log` / `--tenant` /
/// `--namespace` flags (defaults match the rest of the local-reference verbs).
fn projection_engine(
    flags: &mut HashMap<String, String>,
) -> Result<LocalReferenceProjectionEngine, String> {
    let log = take_required(flags, "log")?;
    let tenant = flags
        .remove("tenant")
        .unwrap_or_else(|| DEFAULT_LOCAL_REFERENCE_TENANT.to_string());
    let namespace = flags
        .remove("namespace")
        .unwrap_or_else(|| DEFAULT_LOCAL_REFERENCE_NAMESPACE.to_string());
    Ok(LocalReferenceProjectionEngine::new(
        PathBuf::from(log),
        tenant,
        namespace,
    ))
}

fn run_projection_apply(args: &[String]) -> Result<(String, i32), String> {
    let mut flags = parse_flags(args)?;
    let engine = projection_engine(&mut flags)?;
    let consumer = flags
        .remove("consumer")
        .unwrap_or_else(|| "projector".to_string());
    let transaction_id = take_required(&mut flags, "transaction-id")?;
    let events_json = take_required(&mut flags, "events-json")?;
    ensure_no_unknown_flags(&flags)?;
    let events: Vec<ProjectionEventInput> = serde_json::from_str(&events_json)
        .map_err(|e| format!("invalid --events-json (expected [ProjectionEventInput]): {e}"))?;
    match engine.apply(&ProjectionApplyRequest {
        consumer,
        transaction_id,
        events,
    }) {
        Ok(outcome) => Ok((json(&outcome)?, 0)),
        Err(err) => Ok((json_error(&err)?, eventlog_exit_code(&err))),
    }
}

fn run_projection_read_exec(args: &[String]) -> Result<(String, i32), String> {
    let mut flags = parse_flags(args)?;
    let engine = projection_engine(&mut flags)?;
    let execution_id = take_required(&mut flags, "execution-id")?;
    ensure_no_unknown_flags(&flags)?;
    match engine.read_execution_state(&execution_id) {
        Ok(outcome) => Ok((json(&outcome)?, 0)),
        Err(err) => Ok((json_error(&err)?, eventlog_exit_code(&err))),
    }
}

fn run_projection_read_event(args: &[String]) -> Result<(String, i32), String> {
    let mut flags = parse_flags(args)?;
    let engine = projection_engine(&mut flags)?;
    let event_id = take_required(&mut flags, "event-id")?
        .parse::<i64>()
        .map_err(|_| "invalid --event-id value".to_string())?;
    ensure_no_unknown_flags(&flags)?;
    match engine.read_event(event_id) {
        Ok(outcome) => Ok((json(&outcome)?, 0)),
        Err(err) => Ok((json_error(&err)?, eventlog_exit_code(&err))),
    }
}

fn run_projection_list(args: &[String]) -> Result<(String, i32), String> {
    let mut flags = parse_flags(args)?;
    let engine = projection_engine(&mut flags)?;
    let limit = parse_limit(&mut flags, 100)?;
    ensure_no_unknown_flags(&flags)?;
    match engine.list_executions(limit) {
        Ok(outcome) => Ok((json(&outcome)?, 0)),
        Err(err) => Ok((json_error(&err)?, eventlog_exit_code(&err))),
    }
}

fn run_projection_checkpoint(args: &[String]) -> Result<(String, i32), String> {
    let mut flags = parse_flags(args)?;
    let engine = projection_engine(&mut flags)?;
    let consumer = flags
        .remove("consumer")
        .unwrap_or_else(|| "projector".to_string());
    ensure_no_unknown_flags(&flags)?;
    match engine.checkpoint(&consumer) {
        Ok(outcome) => Ok((json(&outcome)?, 0)),
        Err(err) => Ok((json_error(&err)?, eventlog_exit_code(&err))),
    }
}

/// Integration bridge: scan an existing Phase-6 event log and materialize its
/// tail into a projection store, proving the eventlog-tail → projection-apply
/// path end to end.  `--eventlog-log` is the event-log JSONL; `--log` is the
/// (distinct) projection JSONL.
fn run_projection_from_eventlog(args: &[String]) -> Result<(String, i32), String> {
    let mut flags = parse_flags(args)?;
    // The event-log driver (source) and the projection engine (sink) use
    // distinct log paths so the projection store never contends the log.
    let eventlog_path = take_required(&mut flags, "eventlog-log")?;
    let engine = projection_engine(&mut flags)?;
    let consumer = flags
        .remove("consumer")
        .unwrap_or_else(|| "projector".to_string());
    let transaction_id = flags
        .remove("transaction-id")
        .unwrap_or_else(|| "projection-from-eventlog".to_string());
    let limit = parse_limit(&mut flags, 1000)?;
    ensure_no_unknown_flags(&flags)?;

    let source = LocalReferenceEventLogDriver::new(
        PathBuf::from(eventlog_path),
        DEFAULT_LOCAL_REFERENCE_TENANT,
        DEFAULT_LOCAL_REFERENCE_NAMESPACE,
    );
    let scan = source
        .scan_global(&EventLogScanRequest { after: None, limit })
        .map_err(|err| err.to_string())?;
    let events: Vec<ProjectionEventInput> = scan
        .records
        .iter()
        .filter_map(ProjectionEventInput::from_event_log_record)
        .collect();
    match engine.apply(&ProjectionApplyRequest {
        consumer,
        transaction_id,
        events,
    }) {
        Ok(outcome) => Ok((json(&outcome)?, 0)),
        Err(err) => Ok((json_error(&err)?, eventlog_exit_code(&err))),
    }
}

/// Deterministic one-process drive of the whole projection surface:
/// apply(3 events, exec 100 → terminal) → read-execution (folded completed) →
/// read-event → list → re-apply (idempotent no-op) → checkpoint → parity-compare
/// vs a matching authoritative snapshot.  Exit 0 only when every expected
/// outcome AND the parity report hold.
fn run_projection_suite(args: &[String]) -> Result<(String, i32), String> {
    let mut flags = parse_flags(args)?;
    let engine = projection_engine(&mut flags)?;
    ensure_no_unknown_flags(&flags)?;

    let mut steps = Vec::new();
    let mut ok = true;

    let events = vec![
        ProjectionEventInput {
            global_sequence: 1,
            event_id: 10,
            execution_id: "100".to_string(),
            event_type: "playbook_started".to_string(),
            node_name: Some("start".to_string()),
            status: Some("running".to_string()),
            prev_event_id: None,
        },
        ProjectionEventInput {
            global_sequence: 2,
            event_id: 11,
            execution_id: "100".to_string(),
            event_type: "command.completed".to_string(),
            node_name: Some("load".to_string()),
            status: Some("completed".to_string()),
            prev_event_id: Some(10),
        },
        ProjectionEventInput {
            global_sequence: 3,
            event_id: 12,
            execution_id: "100".to_string(),
            event_type: "playbook.completed".to_string(),
            node_name: Some("finish".to_string()),
            status: Some("completed".to_string()),
            prev_event_id: Some(11),
        },
    ];

    let apply = engine
        .apply(&ProjectionApplyRequest {
            consumer: "suite-projector".to_string(),
            transaction_id: "suite-t1".to_string(),
            events: events.clone(),
        })
        .map_err(|err| err.to_string())?;
    ok &= apply.applied == 3 && apply.checkpoint.applied_through_sequence == 3;
    steps.push(serde_json::json!({"step": "apply", "applied": apply.applied}));

    let read = engine
        .read_execution_state("100")
        .map_err(|err| err.to_string())?;
    let state = read.state.clone().ok_or("missing execution state")?;
    ok &= state.status == "completed" && state.terminal && state.event_count == 3;
    steps.push(serde_json::json!({"step": "read_execution", "status": state.status}));

    let ev = engine.read_event(11).map_err(|err| err.to_string())?;
    ok &= ev.exists;
    steps.push(serde_json::json!({"step": "read_event", "exists": ev.exists}));

    let list = engine.list_executions(100).map_err(|err| err.to_string())?;
    ok &= list.total == 1;
    steps.push(serde_json::json!({"step": "list", "total": list.total}));

    // Re-apply the same batch → idempotent no-op (exactly-once on global seq).
    let replay = engine
        .apply(&ProjectionApplyRequest {
            consumer: "suite-projector".to_string(),
            transaction_id: "suite-t2".to_string(),
            events,
        })
        .map_err(|err| err.to_string())?;
    ok &= replay.applied == 0 && replay.skipped_below_checkpoint == 3;
    steps.push(serde_json::json!({"step": "reapply_idempotent", "applied": replay.applied}));

    let checkpoint = engine
        .checkpoint("suite-projector")
        .map_err(|err| err.to_string())?;
    ok &= checkpoint.applied_through_sequence == 3 && checkpoint.applied_count == 3;
    steps.push(
        serde_json::json!({"step": "checkpoint", "through": checkpoint.applied_through_sequence}),
    );

    // Parity vs a matching authoritative (Postgres-materializer) snapshot.
    let authoritative = vec![AuthoritativeExecutionState {
        execution_id: "100".to_string(),
        status: "completed".to_string(),
        event_count: 3,
        terminal: true,
    }];
    let parity = compare_projection_parity(
        &list.states,
        &authoritative,
        checkpoint.applied_through_sequence,
        Some(3),
    );
    let parity_ok = parity.holds();
    ok &= parity_ok;
    steps.push(serde_json::json!({"step": "parity", "holds": parity_ok}));

    let report = serde_json::json!({
        "suite": "ehdb-projection", "driver": engine.driver_name(),
        "ok": ok, "parity_ok": parity_ok, "steps": steps,
    });
    let output = serde_json::to_string(&report).map_err(|err| err.to_string())?;
    Ok((output, if ok { 0 } else { 1 }))
}

/// Authoritative projection primary-serve cycle (completion program Phase 9,
/// tier 2): drive apply (materialize) + the three read-model query contracts
/// (list / per-execution read / event lookup) + durable checkpoint + idempotent
/// re-apply + fresh-engine replay through the EHDB engine and emit the
/// served-by-EHDB proof (with dual-run parity against a matching incumbent
/// materializer snapshot).  Exit 0 only when
/// [`ProjectionPrimaryServeReport::served_by_ehdb`] holds.
fn run_projection_primary_serve(args: &[String]) -> Result<(String, i32), String> {
    let mut flags = parse_flags(args)?;
    let engine = projection_engine(&mut flags)?;
    let consumer = flags
        .remove("consumer")
        .unwrap_or_else(|| "primary-serve-projector".to_string());
    ensure_no_unknown_flags(&flags)?;

    // Deterministic drive: exec "100" runs to a terminal completed (2 events),
    // exec "200" one running event — a scope + fold + parity ground truth with a
    // matching authoritative snapshot so the dual-run parity check is exact.
    let events = vec![
        ProjectionEventInput {
            global_sequence: 1,
            event_id: 10,
            execution_id: "100".to_string(),
            event_type: "playbook_started".to_string(),
            node_name: Some("start".to_string()),
            status: Some("running".to_string()),
            prev_event_id: None,
        },
        ProjectionEventInput {
            global_sequence: 2,
            event_id: 20,
            execution_id: "200".to_string(),
            event_type: "playbook_started".to_string(),
            node_name: Some("start".to_string()),
            status: Some("running".to_string()),
            prev_event_id: None,
        },
        ProjectionEventInput {
            global_sequence: 3,
            event_id: 11,
            execution_id: "100".to_string(),
            event_type: "playbook.completed".to_string(),
            node_name: Some("finish".to_string()),
            status: Some("completed".to_string()),
            prev_event_id: Some(10),
        },
    ];
    let authoritative = vec![
        AuthoritativeExecutionState {
            execution_id: "100".to_string(),
            status: "completed".to_string(),
            event_count: 2,
            terminal: true,
        },
        AuthoritativeExecutionState {
            execution_id: "200".to_string(),
            status: "running".to_string(),
            event_count: 1,
            terminal: false,
        },
    ];
    let input = ProjectionPrimaryInput {
        events,
        authoritative,
        authoritative_offset: Some(3),
    };

    match ehdb_reference::projection::exercise_primary_serve(
        &engine,
        &input,
        &consumer,
        "primary-t1",
    ) {
        Ok(report) => {
            let served = report.served_by_ehdb();
            let output = serde_json::to_string(&serde_json::json!({
                "suite": "ehdb-projection-primary-serve",
                "driver": report.driver_name,
                "served_by_ehdb": served,
                "report": report,
            }))
            .map_err(|err| err.to_string())?;
            Ok((output, if served { 0 } else { 1 }))
        }
        Err(err) => Ok((json_error(&err)?, eventlog_exit_code(&err))),
    }
}

/// Authoritative KV / platform-state primary-serve cycle (completion program
/// Phase 9, tier 3): drive put + per-key served get + bucket scan + optimistic
/// CAS (versioned swap + create-only conflict) + tombstone delete + absolute-TTL
/// lease + fresh-driver replay through the EHDB engine and emit the
/// served-by-EHDB proof (with per-read dual-run parity against a NATS-KV mirror
/// applied in lockstep).  Exit 0 only when
/// [`KvPrimaryServeReport::served_by_ehdb`] holds.
fn run_kv_primary_serve(args: &[String]) -> Result<(String, i32), String> {
    let mut flags = parse_flags(args)?;
    let log = take_required(&mut flags, "log")?;
    let tenant = flags
        .remove("tenant")
        .unwrap_or_else(|| DEFAULT_LOCAL_REFERENCE_TENANT.to_string());
    let namespace = flags
        .remove("namespace")
        .unwrap_or_else(|| DEFAULT_LOCAL_REFERENCE_NAMESPACE.to_string());
    let bucket = flags
        .remove("bucket")
        .unwrap_or_else(|| "noetl_kv_primary_serve".to_string());
    ensure_no_unknown_flags(&flags)?;

    let driver = LocalReferenceKvStateDriver::new(PathBuf::from(log), tenant, namespace);
    // Deterministic drive: three distinct circuit keys (CAS on the first, delete
    // on the last), a TTL lease at clock 1000 — a scope + CAS + delete + TTL
    // ground truth with an in-lockstep NATS-KV mirror so the dual-run parity is
    // exact.
    let input = KvPrimaryInput {
        bucket,
        entries: vec![
            (
                "circuit.1".to_string(),
                "{\"phase\":\"closed\"}".to_string(),
            ),
            ("circuit.2".to_string(), "{\"phase\":\"open\"}".to_string()),
            ("circuit.3".to_string(), "{\"phase\":\"half\"}".to_string()),
        ],
        now_ms: 1_000,
    };

    match ehdb_reference::kv::exercise_primary_serve(&driver, &input, "primary-t3") {
        Ok(report) => {
            let served = report.served_by_ehdb();
            let output = serde_json::to_string(&serde_json::json!({
                "suite": "ehdb-kv-primary-serve",
                "driver": report.driver_name,
                "served_by_ehdb": served,
                "report": report,
            }))
            .map_err(|err| err.to_string())?;
            Ok((output, if served { 0 } else { 1 }))
        }
        Err(err) => Ok((json_error(&err)?, eventlog_exit_code(&err))),
    }
}

/// Authoritative object / blob primary-serve cycle (completion program Phase 9,
/// tier 4): drive put + per-key digest-verified served get + prefix list +
/// in-cluster locate + tombstone delete + fresh-driver replay through the EHDB
/// object engine and emit the served-by-EHDB proof (with per-read dual-run
/// digest-parity against an external-store mirror applied in lockstep).  Exit 0
/// only when [`ObjectPrimaryServeReport::served_by_ehdb`] holds.
fn run_object_primary_serve(args: &[String]) -> Result<(String, i32), String> {
    let mut flags = parse_flags(args)?;
    let log = take_required(&mut flags, "log")?;
    let tenant = flags
        .remove("tenant")
        .unwrap_or_else(|| DEFAULT_LOCAL_REFERENCE_TENANT.to_string());
    let namespace = flags
        .remove("namespace")
        .unwrap_or_else(|| DEFAULT_LOCAL_REFERENCE_NAMESPACE.to_string());
    ensure_no_unknown_flags(&flags)?;

    // The content-addressed blob store rides a sibling directory of the registry
    // log, so the bytes + registry share a parent and clean up together.
    let log_path = PathBuf::from(&log);
    let object_root = log_path
        .parent()
        .map(|p| p.join("ehdb_object_store"))
        .unwrap_or_else(|| PathBuf::from("ehdb_object_store"));
    let driver = LocalReferenceObjectBlobDriver::new(log_path, object_root, tenant, namespace);

    // Deterministic drive: three distinct platform-artifact keys under one
    // execution prefix (delete on the last state shard), each with distinct bytes
    // — a scope + list + locate + delete ground truth with an in-lockstep
    // external-store mirror so the dual-run digest-parity is exact.
    let input = ObjectPrimaryInput {
        prefix: Some("noetl/env=primary/".to_string()),
        entries: vec![
            (
                "noetl/env=primary/execution=exec-p/state/open.feather".to_string(),
                b"arrow-ipc-state-open".to_vec(),
            ),
            (
                "noetl/env=primary/execution=exec-p/results/s/f/r/a.feather".to_string(),
                b"arrow-ipc-result-frame".to_vec(),
            ),
            (
                "noetl/env=primary/execution=exec-p/state/sealed.feather".to_string(),
                b"arrow-ipc-state-sealed".to_vec(),
            ),
        ],
    };

    match ehdb_reference::object::exercise_primary_serve(&driver, &input, "primary-t4") {
        Ok(report) => {
            let served = report.served_by_ehdb();
            let output = serde_json::to_string(&serde_json::json!({
                "suite": "ehdb-object-primary-serve",
                "driver": report.driver_name,
                "served_by_ehdb": served,
                "report": report,
            }))
            .map_err(|err| err.to_string())?;
            Ok((output, if served { 0 } else { 1 }))
        }
        Err(err) => Ok((json_error(&err)?, eventlog_exit_code(&err))),
    }
}

/// Authoritative vector primary-serve cycle (completion program Phase 9, tier 5 —
/// the final tier): drive upsert + served cosine top-k query + tombstone delete +
/// fresh-driver replay through the EHDB vector engine and emit the served-by-EHDB
/// proof (with per-query dual-run parity — id set + rank order + score
/// monotonicity — against a Qdrant mirror ranked in lockstep).  Exit 0 only when
/// [`VectorPrimaryServeReport::served_by_ehdb`] holds.
fn run_vector_primary_serve(args: &[String]) -> Result<(String, i32), String> {
    let mut flags = parse_flags(args)?;
    let log = take_required(&mut flags, "log")?;
    let tenant = flags
        .remove("tenant")
        .unwrap_or_else(|| DEFAULT_LOCAL_REFERENCE_TENANT.to_string());
    let namespace = flags
        .remove("namespace")
        .unwrap_or_else(|| DEFAULT_LOCAL_REFERENCE_NAMESPACE.to_string());
    ensure_no_unknown_flags(&flags)?;

    let driver = LocalReferenceVectorDriver::new(PathBuf::from(log), tenant, namespace);

    // Deterministic drive: three distinct platform-RAG points under one collection
    // (delete on the last), each a distinct embedding, queried [1,0,0] so the
    // ranking is a > b > c — a scope + rank + delete ground truth with an
    // in-lockstep Qdrant mirror so the dual-run top-k parity is exact.
    let input = VectorPrimaryInput {
        collection: "playbook-surface".to_string(),
        model_id: "text-embedding-3-small".to_string(),
        entries: vec![
            (
                "noetl/playbook/weather.example/chunk.0".to_string(),
                vec![1.0, 0.0, 0.0],
            ),
            (
                "noetl/playbook/weather.example/chunk.1".to_string(),
                vec![0.9, 0.1, 0.0],
            ),
            (
                "noetl/catalog/embeddings/tool.http".to_string(),
                vec![0.0, 1.0, 0.0],
            ),
        ],
        query: vec![1.0, 0.0, 0.0],
        top_k: 10,
    };

    match ehdb_reference::vector::exercise_primary_serve(&driver, &input, "primary-t5") {
        Ok(report) => {
            let served = report.served_by_ehdb();
            let output = serde_json::to_string(&serde_json::json!({
                "suite": "ehdb-vector-primary-serve",
                "driver": report.driver_name,
                "served_by_ehdb": served,
                "report": report,
            }))
            .map_err(|err| err.to_string())?;
            Ok((output, if served { 0 } else { 1 }))
        }
        Err(err) => Ok((json_error(&err)?, eventlog_exit_code(&err))),
    }
}

fn parse_limit(flags: &mut HashMap<String, String>, default: usize) -> Result<usize, String> {
    match flags.remove("limit") {
        Some(raw) => raw
            .parse::<usize>()
            .map_err(|_| format!("invalid --limit value: {raw}")),
        None => Ok(default),
    }
}

fn parse_after(flags: &mut HashMap<String, String>) -> Result<Option<u64>, String> {
    match flags.remove("after") {
        Some(raw) => Ok(Some(
            raw.parse::<u64>()
                .map_err(|_| format!("invalid --after value: {raw}"))?,
        )),
        None => Ok(None),
    }
}

fn json<T: serde::Serialize>(value: &T) -> Result<String, String> {
    serde_json::to_string(value).map_err(|err| err.to_string())
}

fn json_error(err: &EhdbError) -> Result<String, String> {
    serde_json::to_string(&serde_json::json!({"error": err.to_string()})).map_err(|e| e.to_string())
}

/// Wrap an always-exit-0 verb output as `(output, code)`.
fn ok0(output: String) -> (String, i32) {
    (output, 0)
}

fn run_summary(args: &[String]) -> Result<String, String> {
    let mut flags = parse_flags(args)?;
    let log = take_required(&mut flags, "log")?;
    ensure_no_unknown_flags(&flags)?;
    summarize_local_reference_json(PathBuf::from(log)).map_err(|err| err.to_string())
}

fn run_append(args: &[String]) -> Result<String, String> {
    let mut flags = parse_flags(args)?;
    let log = take_required(&mut flags, "log")?;
    let stream = take_required(&mut flags, "stream")?;
    let subject = take_required(&mut flags, "subject")?;
    let transaction_id = take_required(&mut flags, "transaction-id")?;
    let payload = take_required(&mut flags, "payload")?;
    let tenant = flags
        .remove("tenant")
        .unwrap_or_else(|| DEFAULT_LOCAL_REFERENCE_TENANT.to_string());
    let namespace = flags
        .remove("namespace")
        .unwrap_or_else(|| DEFAULT_LOCAL_REFERENCE_NAMESPACE.to_string());
    ensure_no_unknown_flags(&flags)?;

    append_local_reference_domain_record_json(AppendDomainRecordRequest {
        log_path: PathBuf::from(log),
        tenant,
        namespace,
        stream,
        subject,
        transaction_id,
        payload,
    })
    .map_err(|err| err.to_string())
}

fn run_read(args: &[String]) -> Result<String, String> {
    let mut flags = parse_flags(args)?;
    let log = take_required(&mut flags, "log")?;
    let stream = take_required(&mut flags, "stream")?;
    let tenant = flags
        .remove("tenant")
        .unwrap_or_else(|| DEFAULT_LOCAL_REFERENCE_TENANT.to_string());
    let namespace = flags
        .remove("namespace")
        .unwrap_or_else(|| DEFAULT_LOCAL_REFERENCE_NAMESPACE.to_string());
    let limit = match flags.remove("limit") {
        Some(raw) => raw
            .parse::<usize>()
            .map_err(|_| format!("invalid --limit value: {raw}"))?,
        None => 100,
    };
    let after = match flags.remove("after") {
        Some(raw) => Some(
            raw.parse::<u64>()
                .map_err(|_| format!("invalid --after value: {raw}"))?,
        ),
        None => None,
    };
    ensure_no_unknown_flags(&flags)?;

    read_local_reference_domain_records_json(ReadDomainRecordsRequest {
        log_path: PathBuf::from(log),
        tenant,
        namespace,
        stream,
        after,
        limit,
    })
    .map_err(|err| err.to_string())
}

fn run_consume(args: &[String]) -> Result<String, String> {
    let mut flags = parse_flags(args)?;
    let log = take_required(&mut flags, "log")?;
    let stream = take_required(&mut flags, "stream")?;
    let consumer = take_required(&mut flags, "consumer")?;
    let transaction_id = take_required(&mut flags, "transaction-id")?;
    let tenant = flags
        .remove("tenant")
        .unwrap_or_else(|| DEFAULT_LOCAL_REFERENCE_TENANT.to_string());
    let namespace = flags
        .remove("namespace")
        .unwrap_or_else(|| DEFAULT_LOCAL_REFERENCE_NAMESPACE.to_string());
    let limit = match flags.remove("limit") {
        Some(raw) => raw
            .parse::<usize>()
            .map_err(|_| format!("invalid --limit value: {raw}"))?,
        None => 100,
    };
    ensure_no_unknown_flags(&flags)?;

    consume_local_reference_event_records_json(ConsumeEventRecordsRequest {
        log_path: PathBuf::from(log),
        tenant,
        namespace,
        stream,
        consumer,
        transaction_id,
        limit,
    })
    .map_err(|err| err.to_string())
}

fn run_ack(args: &[String]) -> Result<String, String> {
    let mut flags = parse_flags(args)?;
    let log = take_required(&mut flags, "log")?;
    let stream = take_required(&mut flags, "stream")?;
    let consumer = take_required(&mut flags, "consumer")?;
    let transaction_id = take_required(&mut flags, "transaction-id")?;
    let sequence_raw = take_required(&mut flags, "sequence")?;
    let sequence = sequence_raw
        .parse::<u64>()
        .map_err(|_| format!("invalid --sequence value: {sequence_raw}"))?;
    let tenant = flags
        .remove("tenant")
        .unwrap_or_else(|| DEFAULT_LOCAL_REFERENCE_TENANT.to_string());
    let namespace = flags
        .remove("namespace")
        .unwrap_or_else(|| DEFAULT_LOCAL_REFERENCE_NAMESPACE.to_string());
    ensure_no_unknown_flags(&flags)?;

    ack_local_reference_event_consumer_json(AckEventConsumerRequest {
        log_path: PathBuf::from(log),
        tenant,
        namespace,
        stream,
        consumer,
        transaction_id,
        sequence,
    })
    .map_err(|err| err.to_string())
}

fn run_publish_system(args: &[String]) -> Result<String, String> {
    let mut flags = parse_flags(args)?;
    let log = take_required(&mut flags, "log")?;
    let path = take_required(&mut flags, "path")?;
    let revision_raw = take_required(&mut flags, "revision")?;
    let revision = revision_raw
        .parse::<u32>()
        .map_err(|_| format!("invalid --revision value: {revision_raw}"))?;
    let digest = take_required(&mut flags, "digest")?;
    let entry = take_required(&mut flags, "entry")?;
    let target = take_required(&mut flags, "target")?;
    let object_path = take_required(&mut flags, "object-path")?;
    let byte_len_raw = take_required(&mut flags, "byte-len")?;
    let byte_len = byte_len_raw
        .parse::<u64>()
        .map_err(|_| format!("invalid --byte-len value: {byte_len_raw}"))?;
    let capabilities = take_required(&mut flags, "capabilities")?
        .split(',')
        .map(|c| c.trim().to_string())
        .filter(|c| !c.is_empty())
        .collect();
    let transaction_id = take_required(&mut flags, "transaction-id")?;
    let tenant = flags
        .remove("tenant")
        .unwrap_or_else(|| DEFAULT_LOCAL_REFERENCE_TENANT.to_string());
    let namespace = flags
        .remove("namespace")
        .unwrap_or_else(|| DEFAULT_LOCAL_REFERENCE_NAMESPACE.to_string());
    ensure_no_unknown_flags(&flags)?;

    publish_local_reference_system_module_json(PublishSystemModuleRequest {
        log_path: PathBuf::from(log),
        tenant,
        namespace,
        path,
        revision,
        digest,
        entry,
        target,
        object_path,
        byte_len,
        capabilities,
        transaction_id,
    })
    .map_err(|err| err.to_string())
}

fn run_bind_system(args: &[String]) -> Result<String, String> {
    let mut flags = parse_flags(args)?;
    let log = take_required(&mut flags, "log")?;
    let environment = take_required(&mut flags, "environment")?;
    let channel = take_required(&mut flags, "channel")?;
    let path = take_required(&mut flags, "path")?;
    let revision_raw = take_required(&mut flags, "revision")?;
    let revision = revision_raw
        .parse::<u32>()
        .map_err(|_| format!("invalid --revision value: {revision_raw}"))?;
    let digest = take_required(&mut flags, "digest")?;
    let transaction_id = take_required(&mut flags, "transaction-id")?;
    let tenant = flags
        .remove("tenant")
        .unwrap_or_else(|| DEFAULT_LOCAL_REFERENCE_TENANT.to_string());
    let namespace = flags
        .remove("namespace")
        .unwrap_or_else(|| DEFAULT_LOCAL_REFERENCE_NAMESPACE.to_string());
    ensure_no_unknown_flags(&flags)?;

    bind_local_reference_system_channel_json(BindSystemChannelRequest {
        log_path: PathBuf::from(log),
        tenant,
        namespace,
        environment,
        channel,
        path,
        revision,
        digest,
        transaction_id,
    })
    .map_err(|err| err.to_string())
}

fn run_resolve_system(args: &[String]) -> Result<String, String> {
    let mut flags = parse_flags(args)?;
    let log = take_required(&mut flags, "log")?;
    let environment = take_required(&mut flags, "environment")?;
    let channel = take_required(&mut flags, "channel")?;
    let path = take_required(&mut flags, "path")?;
    let tenant = flags
        .remove("tenant")
        .unwrap_or_else(|| DEFAULT_LOCAL_REFERENCE_TENANT.to_string());
    let namespace = flags
        .remove("namespace")
        .unwrap_or_else(|| DEFAULT_LOCAL_REFERENCE_NAMESPACE.to_string());
    ensure_no_unknown_flags(&flags)?;

    resolve_local_reference_system_module_json(ResolveSystemModuleRequest {
        log_path: PathBuf::from(log),
        tenant,
        namespace,
        environment,
        channel,
        path,
    })
    .map_err(|err| err.to_string())
}

/// Ingest one retrieval document.  Chunks are supplied as a single `||`-joined
/// string; ordinals + chunk ids are assigned positionally.
fn run_ingest(args: &[String]) -> Result<String, String> {
    let mut flags = parse_flags(args)?;
    let log = take_required(&mut flags, "log")?;
    let document_id = take_required(&mut flags, "document-id")?;
    let chunks_raw = take_required(&mut flags, "chunks")?;
    let source_uri = flags
        .remove("source-uri")
        .unwrap_or_else(|| format!("artifact://{document_id}/source.md"));
    let content_type = flags
        .remove("content-type")
        .unwrap_or_else(|| "text/plain".to_string());
    let transaction_id = flags
        .remove("transaction-id")
        .unwrap_or_else(|| format!("txn-{document_id}"));
    let tenant = flags
        .remove("tenant")
        .unwrap_or_else(|| DEFAULT_LOCAL_REFERENCE_TENANT.to_string());
    let namespace = flags
        .remove("namespace")
        .unwrap_or_else(|| DEFAULT_LOCAL_REFERENCE_NAMESPACE.to_string());
    ensure_no_unknown_flags(&flags)?;

    let chunks: Vec<IngestChunkInput> = chunks_raw
        .split("||")
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .enumerate()
        .map(|(index, text)| IngestChunkInput {
            chunk_id: format!("{document_id}-{index}"),
            ordinal: index as u32,
            text: text.to_string(),
            checksum: format!("len-{}", text.len()),
        })
        .collect();

    ingest_local_reference_retrieval_document_json(IngestRetrievalDocumentRequest {
        log_path: PathBuf::from(log),
        tenant,
        namespace,
        document_id,
        source_uri,
        content_type,
        transaction_id,
        chunks,
    })
    .map_err(|err| err.to_string())
}

/// Run a bounded retrieval.  Exit code conveys the outcome so a shell harness can
/// assert: 0 = hit/empty, 3 = rejected (bound), 4 = invalid.
fn run_retrieve(args: &[String]) -> Result<(String, i32), String> {
    let mut flags = parse_flags(args)?;
    let log = take_required(&mut flags, "log")?;
    let query = take_required(&mut flags, "query")?;
    let tenant = flags
        .remove("tenant")
        .unwrap_or_else(|| DEFAULT_LOCAL_REFERENCE_TENANT.to_string());
    let namespace = flags
        .remove("namespace")
        .unwrap_or_else(|| DEFAULT_LOCAL_REFERENCE_NAMESPACE.to_string());
    let top_k = parse_opt_usize(&mut flags, "top-k")?.unwrap_or(0);
    let max_chunk_bytes = parse_opt_usize(&mut flags, "max-chunk-bytes")?.unwrap_or(0);
    let time_budget_ms = match flags.remove("time-budget-ms") {
        Some(raw) => raw
            .parse::<u64>()
            .map_err(|_| format!("invalid --time-budget-ms value: {raw}"))?,
        None => 0,
    };
    ensure_no_unknown_flags(&flags)?;

    let outcome = retrieve_local_reference_context(RetrieveContextRequest {
        log_path: PathBuf::from(log),
        tenant,
        namespace,
        query,
        top_k,
        max_chunk_bytes,
        time_budget_ms,
    })
    .map_err(|err| err.to_string())?;

    let code = match outcome.outcome {
        RetrievalOutcome::Hit | RetrievalOutcome::Empty => 0,
        RetrievalOutcome::Rejected => 3,
        RetrievalOutcome::Invalid => 4,
    };
    let json = serde_json::to_string(&outcome).map_err(|err| err.to_string())?;
    Ok((json, code))
}

fn parse_opt_usize(
    flags: &mut HashMap<String, String>,
    key: &str,
) -> Result<Option<usize>, String> {
    match flags.remove(key) {
        Some(raw) => raw
            .parse::<usize>()
            .map(Some)
            .map_err(|_| format!("invalid --{key} value: {raw}")),
        None => Ok(None),
    }
}

/// Parse `--key value` pairs into a map.  Rejects positional args and flags
/// without a value so a malformed invocation fails loudly rather than being
/// silently ignored.
fn parse_flags(args: &[String]) -> Result<HashMap<String, String>, String> {
    let mut flags = HashMap::new();
    let mut iter = args.iter();
    while let Some(token) = iter.next() {
        let key = token
            .strip_prefix("--")
            .ok_or_else(|| format!("unexpected argument: {token}\n{}", usage()))?;
        if key.is_empty() {
            return Err(format!("empty flag name\n{}", usage()));
        }
        let value = iter
            .next()
            .ok_or_else(|| format!("flag --{key} is missing a value\n{}", usage()))?;
        if flags.insert(key.to_string(), value.clone()).is_some() {
            return Err(format!("duplicate flag --{key}\n{}", usage()));
        }
    }
    Ok(flags)
}

fn take_required(flags: &mut HashMap<String, String>, key: &str) -> Result<String, String> {
    flags
        .remove(key)
        .ok_or_else(|| format!("missing required flag --{key}\n{}", usage()))
}

fn ensure_no_unknown_flags(flags: &HashMap<String, String>) -> Result<(), String> {
    if flags.is_empty() {
        return Ok(());
    }
    let mut unknown: Vec<&String> = flags.keys().collect();
    unknown.sort();
    let rendered = unknown
        .iter()
        .map(|key| format!("--{key}"))
        .collect::<Vec<_>>()
        .join(", ");
    Err(format!("unknown flag(s): {rendered}\n{}", usage()))
}

fn usage() -> &'static str {
    "usage:\n  ehdb-local-reference summary --log <path>\n  ehdb-local-reference append --log <path> --stream <name> --subject <subject> --transaction-id <id> --payload <text> [--tenant <t>] [--namespace <n>]\n  ehdb-local-reference read --log <path> --stream <name> [--tenant <t>] [--namespace <n>] [--limit <n>] [--after <sequence>]\n  ehdb-local-reference consume --log <path> --stream <name> --consumer <name> --transaction-id <id> [--tenant <t>] [--namespace <n>] [--limit <n>]\n  ehdb-local-reference ack --log <path> --stream <name> --consumer <name> --transaction-id <id> --sequence <sequence> [--tenant <t>] [--namespace <n>]\n  ehdb-local-reference publish-system --log <path> --path <lib> --revision <n> --digest <sha256:...> --entry <export> --target <wasm32-unknown-unknown|wasm32-wasi-preview1> --object-path <path> --byte-len <n> --capabilities <c1,c2,...> --transaction-id <id> [--tenant <t>] [--namespace <n>]\n  ehdb-local-reference bind-system --log <path> --environment <env> --channel <chan> --path <lib> --revision <n> --digest <sha256:...> --transaction-id <id> [--tenant <t>] [--namespace <n>]\n  ehdb-local-reference resolve-system --log <path> --environment <env> --channel <chan> --path <lib> [--tenant <t>] [--namespace <n>]\n  ehdb-local-reference ingest-doc --log <path> --document-id <id> --chunks <text1||text2||...> [--source-uri <uri>] [--content-type <ct>] [--transaction-id <id>] [--tenant <t>] [--namespace <n>]\n  ehdb-local-reference retrieve --log <path> --query <text> [--top-k <n>] [--max-chunk-bytes <n>] [--time-budget-ms <n>] [--tenant <t>] [--namespace <n>]\n  ehdb-local-reference eventlog-append --log <path> --execution-id <id> --transaction-id <id> --payload <text> [--tenant <t>] [--namespace <n>]\n  ehdb-local-reference eventlog-scan --log <path> [--after <sequence>] [--limit <n>] [--tenant <t>] [--namespace <n>]\n  ehdb-local-reference eventlog-read-exec --log <path> --execution-id <id> [--after <sequence>] [--limit <n>] [--tenant <t>] [--namespace <n>]\n  ehdb-local-reference eventlog-tail --log <path> --consumer <name> --transaction-id <id> [--limit <n>] [--tenant <t>] [--namespace <n>]\n  ehdb-local-reference eventlog-ack --log <path> --consumer <name> --transaction-id <id> --sequence <sequence> [--tenant <t>] [--namespace <n>]\n  ehdb-local-reference eventlog-suite --log <path> [--tenant <t>] [--namespace <n>]\n  ehdb-local-reference eventlog-primary-serve --log <path> [--consumer <name>] [--tenant <t>] [--namespace <n>]\n  ehdb-local-reference durable-eventlog-recovery --root <dir> [--consumer <name>]\n  ehdb-local-reference durable-eventlog-affinity --root <dir> [--shard-count <n>]\n  ehdb-local-reference durable-eventlog-affinity-append --root <dir> --shard-index <n> --shard-count <n> --execution-id <id> --transaction-id <id> --payload <text>\n  ehdb-local-reference durable-eventlog-affinity-read --root <dir> --shard-index <n> --shard-count <n> --execution-id <id> [--after <sequence>] [--limit <n>]\n  ehdb-local-reference projection-apply --log <path> --transaction-id <id> --events-json <json-array> [--consumer <name>] [--tenant <t>] [--namespace <n>]\n  ehdb-local-reference projection-read-exec --log <path> --execution-id <id> [--tenant <t>] [--namespace <n>]\n  ehdb-local-reference projection-read-event --log <path> --event-id <id> [--tenant <t>] [--namespace <n>]\n  ehdb-local-reference projection-list --log <path> [--limit <n>] [--tenant <t>] [--namespace <n>]\n  ehdb-local-reference projection-checkpoint --log <path> [--consumer <name>] [--tenant <t>] [--namespace <n>]\n  ehdb-local-reference projection-from-eventlog --eventlog-log <path> --log <path> [--consumer <name>] [--transaction-id <id>] [--limit <n>] [--tenant <t>] [--namespace <n>]\n  ehdb-local-reference projection-suite --log <path> [--tenant <t>] [--namespace <n>]\n  ehdb-local-reference projection-primary-serve --log <path> [--consumer <name>] [--tenant <t>] [--namespace <n>]\n  ehdb-local-reference kv-primary-serve --log <path> [--bucket <name>] [--tenant <t>] [--namespace <n>]\n  ehdb-local-reference object-primary-serve --log <path> [--tenant <t>] [--namespace <n>]\n  ehdb-local-reference vector-primary-serve --log <path> [--tenant <t>] [--namespace <n>]"
}
