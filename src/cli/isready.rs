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
			connection_url: endpoint,
		},
	}: IsReadyCommandArguments,
) -> Result<(), Error> {
	// Initialize opentelemetry and logging
	crate::o11y::builder().with_log_level("error").init();
	// Connect to the database engine
	let client = connect(endpoint).await?;
	// Check if the database engine is healthy
	client.health().await?;
	println!("OK");
	Ok(())
}
