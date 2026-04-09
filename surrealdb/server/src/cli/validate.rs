use anyhow::{Result, bail};
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

		tokio::io::stdin().read_to_string(&mut input).await?;

		match syn::parse(&input) {
			Ok(_) => println!("<stdin>: OK"),
			Err(error) => {
				println!("<stdin>: KO");
				eprintln!("{error}");
				bail!(error)
			}
		}
		return Ok(());
	}

	let mut entries = vec![];

	for pattern in patterns {
		let pattern_entries = match glob(&pattern) {
			Ok(entries) => entries,
			Err(error) => {
				eprintln!("Error parsing glob pattern {pattern}: {error}");

				return Err(anyhow::Error::new(error)
					.context(format!("Error parsing glob pattern '{pattern}'")));
			}
		};

		entries.extend(pattern_entries.flatten());
	}

	let mut has_entries = false;

	for entry in entries {
		let file_content = tokio::fs::read_to_string(entry.clone()).await?;
		let parse_result = syn::parse(&file_content);

		match parse_result {
			Ok(_) => {
				println!("{}: OK", entry.display());
			}
			Err(error) => {
				println!("{}: KO", entry.display());
				eprintln!("{error}");

				bail!(error)
			}
		}

		has_entries = true;
	}

	if !has_entries {
		eprintln!("No files found");
		bail!("No filed found");
	}

	Ok(())
}
