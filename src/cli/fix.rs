use anyhow::Result;
use clap::Args;
use surrealdb_core::kvs::TransactionBuilderFactory;

#[derive(Args, Debug)]
pub struct FixCommandArguments {
	#[arg(help = "Database path used for storing data")]
	#[arg(env = "SURREAL_PATH", index = 1)]
	#[arg(default_value = "memory")]
	path: String,
}

/// Validate the datastore path for the `fix` subcommand.
///
/// Only the `TransactionBuilderFactory` bound is required here because this
/// command does not need to start the HTTP server or build routes.
pub async fn init<F: TransactionBuilderFactory>(args: FixCommandArguments) -> Result<()> {
	// All ok
	F::path_valid(&args.path)?;
	Err(anyhow::anyhow!("Fix is not implemented"))
}
