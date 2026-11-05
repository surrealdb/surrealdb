use anyhow::Result;
use clap::Args;
use glob::glob;
use serde_json::{Map, Value, json};
use surrealdb_core::syn;

use crate::cli::json;

#[derive(Args, Debug)]
pub struct ValidateCommandArguments {
	#[arg(help = "Glob pattern for the files to validate")]
	#[arg(default_value = "**/*.surql")]
	patterns: Vec<String>,
	#[arg(long, help = "Read query from standard input")]
	#[arg(conflicts_with = "patterns")]
	stdin: bool,
	#[command(flatten)]
	json: json::JsonArg,
}

/// What `--json` reports for one file: whether it parsed, and if not, why.
fn file_result(path: &str, error: Option<String>) -> Value {
	match error {
		None => json!({ "path": path, "valid": true }),
		Some(error) => json!({ "path": path, "valid": false, "error": error }),
	}
}

/// The `--json` failure of `kind` when some file did not validate, carrying
/// every file's result under `files`.
fn failed_files(kind: json::Kind, message: &str, files: Vec<Value>) -> anyhow::Error {
	let mut extra = Map::new();
	extra.insert("files".into(), Value::Array(files));
	json::Failure {
		kind,
		message: message.to_owned(),
		extra,
	}
	.into()
}

pub async fn init(args: ValidateCommandArguments) -> Result<()> {
	let ValidateCommandArguments {
		patterns,
		stdin,
		json: json::JsonArg {
			json,
		},
	} = args;

	if stdin {
		use tokio::io::AsyncReadExt;
		let mut input = String::new();

		if let Err(error) = tokio::io::stdin().read_to_string(&mut input).await {
			return Err(anyhow::anyhow!("Failed to read from stdin: {error}"));
		}

		match syn::parse(&input) {
			Ok(_) if json => {
				let files = vec![file_result("<stdin>", None)];
				return json::print(&json!({ "valid": true, "files": files }));
			}
			Ok(_) => println!("<stdin>: OK"),
			Err(error) if json => {
				let files = vec![file_result("<stdin>", Some(error.to_string()))];
				return Err(failed_files(
					json::Kind::Invalid,
					"The query failed to validate",
					files,
				));
			}
			Err(error) => {
				println!("<stdin>: FAIL");
				println!("{error}");
				return Err(anyhow::anyhow!("The query failed to validate"));
			}
		}

		return Ok(());
	}

	let mut entries = vec![];

	for pattern in patterns {
		let pattern_entries = match glob(&pattern) {
			Ok(entries) => entries,
			Err(error) => {
				let message = format!("Error parsing glob pattern '{pattern}': {error}");
				return Err(json::Failure::new(json::Kind::Usage, message).into());
			}
		};

		entries.extend(pattern_entries.flatten());
	}

	if entries.is_empty() {
		return Err(anyhow::anyhow!("No files found"));
	}

	let mut failed = false;
	// Under `--json`, a file that could not be read decides the failure's kind:
	// the first such file's, `not_found` when it does not exist and `unknown`
	// for any other error. When every file was read, the kind is `invalid`.
	let mut unreadable = None;
	let mut files = vec![];

	for entry in entries {
		let path = entry.display().to_string();
		let error = match tokio::fs::read_to_string(&entry).await {
			Ok(content) => syn::parse(&content).err().map(|error| error.to_string()),
			Err(error) => {
				unreadable.get_or_insert(if error.kind() == std::io::ErrorKind::NotFound {
					json::Kind::NotFound
				} else {
					json::Kind::Unknown
				});
				Some(format!("Failed to read file '{path}': {error}"))
			}
		};
		if !json {
			match &error {
				None => println!("{path}: OK\n"),
				Some(error) => println!("{path}: FAIL\n{error}"),
			}
		}
		failed |= error.is_some();
		files.push(file_result(&path, error));
	}

	if failed {
		let message = "Some queries failed to validate";
		return Err(if json {
			failed_files(unreadable.unwrap_or(json::Kind::Invalid), message, files)
		} else {
			anyhow::anyhow!(message)
		});
	}

	if json {
		json::print(&json!({ "valid": true, "files": files }))?;
	}

	Ok(())
}
