use crate::dbs;
use anyhow::Result;
use clap::Args;
use surrealdb::Surreal;

#[derive(Args, Debug)]
pub struct FixCommandArguments {
	#[arg(help = "Database path used for storing data")]
	#[arg(env = "SURREAL_PATH", index = 1)]
	#[arg(default_value = "memory")]
	#[arg(value_parser = super::validator::path_valid)]
	path: String,
}

pub async fn init(
	FixCommandArguments {
		path,
	}: FixCommandArguments,
) -> Result<()> {
	// Clean the path
	let client = Surreal::connect(path, 1024).await?;

	// Fix the datastore, if applicable
	Err(anyhow::anyhow!("Not implemented"))
}
