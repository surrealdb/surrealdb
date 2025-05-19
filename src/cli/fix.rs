use crate::dbs;
use anyhow::Result;
use clap::Args;
use surrealdb::engine::any;

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
	let endpoint = any::__into_endpoint(path)?;
	let path = if endpoint.path.is_empty() {
		endpoint.url.to_string()
	} else {
		endpoint.path
	};
	// Fix the datastore, if applicable
	dbs::fix(path).await?;
	// All ok
	Ok(())
}
