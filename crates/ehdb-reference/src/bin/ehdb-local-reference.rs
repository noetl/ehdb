use std::{collections::HashMap, env, path::PathBuf, process};

use ehdb_reference::{
    ack_local_reference_event_consumer_json, append_local_reference_domain_record_json,
    bind_local_reference_system_channel_json, consume_local_reference_event_records_json,
    ingest_local_reference_retrieval_document_json, publish_local_reference_system_module_json,
    read_local_reference_domain_records_json, resolve_local_reference_system_module_json,
    retrieve_local_reference_context, summarize_local_reference_json, AckEventConsumerRequest,
    AppendDomainRecordRequest, BindSystemChannelRequest, ConsumeEventRecordsRequest,
    IngestChunkInput, IngestRetrievalDocumentRequest, PublishSystemModuleRequest,
    ReadDomainRecordsRequest, ResolveSystemModuleRequest, RetrievalOutcome, RetrieveContextRequest,
    DEFAULT_LOCAL_REFERENCE_NAMESPACE, DEFAULT_LOCAL_REFERENCE_TENANT,
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
        _ => Err(usage().to_string()),
    }
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
    "usage:\n  ehdb-local-reference summary --log <path>\n  ehdb-local-reference append --log <path> --stream <name> --subject <subject> --transaction-id <id> --payload <text> [--tenant <t>] [--namespace <n>]\n  ehdb-local-reference read --log <path> --stream <name> [--tenant <t>] [--namespace <n>] [--limit <n>] [--after <sequence>]\n  ehdb-local-reference consume --log <path> --stream <name> --consumer <name> --transaction-id <id> [--tenant <t>] [--namespace <n>] [--limit <n>]\n  ehdb-local-reference ack --log <path> --stream <name> --consumer <name> --transaction-id <id> --sequence <sequence> [--tenant <t>] [--namespace <n>]\n  ehdb-local-reference publish-system --log <path> --path <lib> --revision <n> --digest <sha256:...> --entry <export> --target <wasm32-unknown-unknown|wasm32-wasi-preview1> --object-path <path> --byte-len <n> --capabilities <c1,c2,...> --transaction-id <id> [--tenant <t>] [--namespace <n>]\n  ehdb-local-reference bind-system --log <path> --environment <env> --channel <chan> --path <lib> --revision <n> --digest <sha256:...> --transaction-id <id> [--tenant <t>] [--namespace <n>]\n  ehdb-local-reference resolve-system --log <path> --environment <env> --channel <chan> --path <lib> [--tenant <t>] [--namespace <n>]\n  ehdb-local-reference ingest-doc --log <path> --document-id <id> --chunks <text1||text2||...> [--source-uri <uri>] [--content-type <ct>] [--transaction-id <id>] [--tenant <t>] [--namespace <n>]\n  ehdb-local-reference retrieve --log <path> --query <text> [--top-k <n>] [--max-chunk-bytes <n>] [--time-budget-ms <n>] [--tenant <t>] [--namespace <n>]"
}
