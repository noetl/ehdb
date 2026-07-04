use std::{collections::HashMap, env, path::PathBuf, process};

use ehdb_reference::{
    append_local_reference_domain_record_json, read_local_reference_domain_records_json,
    summarize_local_reference_json, AppendDomainRecordRequest, ReadDomainRecordsRequest,
    DEFAULT_LOCAL_REFERENCE_NAMESPACE, DEFAULT_LOCAL_REFERENCE_TENANT,
};

fn main() {
    match run(env::args().skip(1).collect()) {
        Ok(output) => println!("{output}"),
        Err(err) => {
            eprintln!("{err}");
            process::exit(2);
        }
    }
}

fn run(args: Vec<String>) -> Result<String, String> {
    match args.split_first() {
        None => Err(usage().to_string()),
        Some((command, rest)) if command == "--help" || command == "-h" => {
            if rest.is_empty() {
                Ok(usage().to_string())
            } else {
                Err(usage().to_string())
            }
        }
        Some((command, rest)) if command == "summary" => run_summary(rest),
        Some((command, rest)) if command == "append" => run_append(rest),
        Some((command, rest)) if command == "read" => run_read(rest),
        _ => Err(usage().to_string()),
    }
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
    "usage:\n  ehdb-local-reference summary --log <path>\n  ehdb-local-reference append --log <path> --stream <name> --subject <subject> --transaction-id <id> --payload <text> [--tenant <t>] [--namespace <n>]\n  ehdb-local-reference read --log <path> --stream <name> [--tenant <t>] [--namespace <n>] [--limit <n>] [--after <sequence>]"
}
