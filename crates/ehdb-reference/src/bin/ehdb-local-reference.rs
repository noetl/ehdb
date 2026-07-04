use std::{env, path::PathBuf, process};

use ehdb_reference::summarize_local_reference_json;

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
    match args.as_slice() {
        [flag] if flag == "--help" || flag == "-h" => Ok(usage().to_string()),
        [command, flag, path] if command == "summary" && flag == "--log" => {
            summarize_local_reference_json(PathBuf::from(path)).map_err(|err| err.to_string())
        }
        _ => Err(usage().to_string()),
    }
}

fn usage() -> &'static str {
    "usage: ehdb-local-reference summary --log <path>"
}
