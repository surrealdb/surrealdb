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

pub async fn init<F: TransactionBuilderFactory>(args: FixCommandArguments) -> Result<()> {
	// All ok
	F::path_valid(&args.path)?;
	Err(anyhow::anyhow!("Fix is not implemented"))
}
