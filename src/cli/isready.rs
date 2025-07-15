use crate::cli::abstraction::DatabaseConnectionArguments;
use anyhow::Result;
use clap::Args;
use surrealdb::Surreal;

#[derive(Args, Debug)]
pub struct IsReadyCommandArguments {
	#[command(flatten)]
	conn: DatabaseConnectionArguments,
}

pub async fn init(
	IsReadyCommandArguments {
		conn: DatabaseConnectionArguments {
			endpoint,
		},
	}: IsReadyCommandArguments,
) -> Result<()> {
	// Connect to the database engine
	Surreal::connect(endpoint).await?;
	// Log output ok
	println!("OK");
	// All ok
	Ok(())
}
