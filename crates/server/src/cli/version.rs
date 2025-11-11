use anyhow::Result;
use clap::Args;
use surrealdb::engine::any::connect;

use crate::cli::abstraction::OptionalDatabaseConnectionArguments;
use crate::env::RELEASE;

#[derive(Args, Debug)]
pub struct VersionCommandArguments {
	#[command(flatten)]
	conn: OptionalDatabaseConnectionArguments,
}

pub async fn init(
	VersionCommandArguments {
		conn: OptionalDatabaseConnectionArguments {
			endpoint,
		},
	}: VersionCommandArguments,
) -> Result<()> {
	// Print server version if endpoint supplied else CLI version
	if let Some(e) = endpoint {
		// Print remote server version
		println!("{}", get_server_version_string(e).await?);
	} else {
		// Print local CLI version
		println!("{}", *RELEASE);
	}
	// All ok
	Ok(())
}

async fn get_server_version_string(endpoint: String) -> Result<String> {
	// Connect to the database engine
	let client = connect(endpoint).await?;
	// Query database version info
	let server_version = client.version().await?;
	// Convert version info to formatted string
	Ok(server_version.to_string())
}
