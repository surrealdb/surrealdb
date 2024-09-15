use crate::cli::validator::parser::env_filter::CustomEnvFilter;
use crate::cli::validator::parser::env_filter::CustomEnvFilterParser;
use crate::dbs;
use crate::err::Error;
use clap::Args;
use surrealdb::engine::any::IntoEndpoint;

#[derive(Args, Debug)]
pub struct FixCommandArguments {
	#[arg(help = "Database path used for storing data")]
	#[arg(env = "SURREAL_PATH", index = 1)]
	#[arg(default_value = "memory")]
	#[arg(value_parser = super::validator::path_valid)]
	path: String,
	#[arg(help = "The logging level for the database server")]
	#[arg(env = "SURREAL_LOG", short = 'l', long = "log")]
	#[arg(default_value = "info")]
	#[arg(value_parser = CustomEnvFilterParser::new())]
	log: CustomEnvFilter,
}

pub async fn init(
	FixCommandArguments {
		path,
		log,
	}: FixCommandArguments,
) -> Result<(), Error> {
	// Initialize opentelemetry and logging
	crate::telemetry::builder().with_filter(log).init()?;
	// Clean the path
	let endpoint = path.into_endpoint()?;
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
