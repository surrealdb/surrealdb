use anyhow::{Result, bail};
use clap::Args;
use surrealdb_core::syn;
use tokio::fs;
use tokio::io::{self, AsyncReadExt};

#[derive(Args, Debug)]
pub struct FormatCommandArguments {
	#[arg(help = "Path to the SurrealQL file to format. Use dash - to read from stdin.")]
	#[arg(index = 1)]
	file: String,

	#[arg(short, long)]
	#[arg(help = "Overwrite the file with the formatted output instead of printing to stdout.")]
	write: bool,
}

pub async fn init(
	FormatCommandArguments {
		file,
		write,
	}: FormatCommandArguments,
) -> Result<()> {
	let is_stdin = file == "-";

	if write && is_stdin {
		bail!("The --write flag cannot be used when reading from stdin.");
	}

	let content = if is_stdin {
		let mut content = String::new();

		io::stdin().read_to_string(&mut content).await?;

		content
	} else {
		fs::read_to_string(&file).await?
	};

	let formatted = syn::format(content.as_bytes())?;

	if write {
		fs::write(file, formatted).await?
	} else {
		println!("{formatted}");
	}

	Ok(())
}
