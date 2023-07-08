use crate::err::Error;
use clap::Args;
use glob::glob;
use std::io::{Error as IoError, ErrorKind};
use surrealdb::sql::parse;

#[derive(Args, Debug)]
pub struct ValidateCommandArguments {
	#[arg(help = "Glob pattern for the files to validate")]
	#[arg(default_value = "**/*.surql")]
	pattern: String,
}

pub async fn init(args: ValidateCommandArguments) -> Result<(), Error> {
	let ValidateCommandArguments {
		pattern,
	} = args;

	let entries = match glob(&pattern) {
		Ok(entries) => entries,
		Err(error) => {
			eprintln!("Error parsing glob pattern {pattern}: {error}");

			return Err(Error::Io(IoError::new(
				ErrorKind::Other,
				format!("Error parsing glob pattern {pattern}: {error}"),
			)));
		}
	};

	let mut has_entries = false;

	for entry in entries.flatten() {
		let file_content = tokio::fs::read_to_string(entry.clone()).await?;
		let parse_result = parse(&file_content);

		match parse_result {
			Ok(_) => {
				println!("{}: OK", entry.display());
			}
			Err(error) => {
				println!("{}: KO", entry.display());
				eprintln!("{error}");

				return Err(crate::err::Error::from(error));
			}
		}

		has_entries = true;
	}

	if !has_entries {
		eprintln!("No files found for pattern {pattern}");

		return Err(Error::Io(IoError::new(
			ErrorKind::NotFound,
			format!("No files found for pattern {pattern}"),
		)));
	}

	Ok(())
}
