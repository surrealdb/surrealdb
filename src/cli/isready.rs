use crate::cli::abstraction::DatabaseConnectionArguments;
use crate::err::Error;
use clap::Args;
use surrealdb::engine::any::connect;

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
) -> Result<(), Error> {
	// Connect to the database engine
	connect(endpoint).await?;
	// Log output ok
	println!("OK");
	// All ok
	Ok(())
}
