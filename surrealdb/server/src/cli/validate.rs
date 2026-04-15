use anyhow::Result;
use clap::Args;
use glob::glob;
use surrealdb_core::syn;

#[derive(Args, Debug)]
pub struct ValidateCommandArguments {
	#[arg(help = "Glob pattern for the files to validate")]
	#[arg(default_value = "**/*.surql")]
	patterns: Vec<String>,
	#[arg(long, help = "Read query from standard input")]
	#[arg(conflicts_with = "patterns")]
	stdin: bool,
}

pub async fn init(args: ValidateCommandArguments) -> Result<()> {
	let ValidateCommandArguments {
		patterns,
		stdin,
	} = args;

	if stdin {
		use tokio::io::AsyncReadExt;
		let mut input = String::new();

		if let Err(error) = tokio::io::stdin().read_to_string(&mut input).await {
			return Err(anyhow::anyhow!("Failed to read from stdin: {error}"));
		}

		match syn::parse(&input) {
			Ok(_) => println!("<stdin>: OK"),
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
				return Err(anyhow::anyhow!("Error parsing glob pattern '{pattern}': {error}"));
			}
		};

		entries.extend(pattern_entries.flatten());
	}

	if entries.is_empty() {
		return Err(anyhow::anyhow!("No files found"));
	}

	let mut failed = false;

	for entry in entries {
		let file_content = match tokio::fs::read_to_string(entry.clone()).await {
			Ok(content) => content,
			Err(error) => {
				println!("{}: FAIL", entry.display());
				println!("Failed to read file '{}': {error}", entry.display());
				failed = true;
				continue;
			}
		};

		match syn::parse(&file_content) {
			Ok(_) => {
				println!("{}: OK", entry.display());
				println!();
			}
			Err(error) => {
				println!("{}: FAIL", entry.display());
				println!("{error}");
				failed = true;
			}
		}
	}

	if failed {
		return Err(anyhow::anyhow!("Some queries failed to validate"));
	}

	Ok(())
}
