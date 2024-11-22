use anyhow::{bail, Context, Result};
use arbitrary::Unstructured;
use clap::ArgMatches;
use surrealdb_core::sql::Query;
use tokio::fs;

pub async fn run(matches: &ArgMatches) -> Result<()> {
	match matches.subcommand() {
		Some(("fmt", matches)) => fmt(matches).await,
		Some(("import", _)) => {
			bail!("Not yet implemented");
		}
		_ => bail!("invalid fuzz subcommand"),
	}
}

pub async fn fmt(matches: &ArgMatches) -> Result<()> {
	let input = matches.get_one::<String>("INPUT").unwrap();

	let file = fs::read(input).await.context("Failed to read fuzz fmt input file")?;

	let mut unstructured = Unstructured::new(&file);
	let query = unstructured.arbitrary::<Query>().context("Failed to structure fuzzing bytes")?;

	println!("{:#?}", query);
	println!("{}", query);

	Ok(())
}
