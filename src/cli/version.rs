use crate::cli::abstraction::OptionalDatabaseConnectionArguments;
use crate::env::release;
use crate::err::Error;
use clap::Args;
use surrealdb::engine::any::connect;

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
) -> Result<(), Error> {
	// Initialize opentelemetry and logging
	crate::o11y::builder().with_log_level("error").init();
	// Print server version if endpoint supplied else CLI version
	if let Some(e) = endpoint {
		// Print remote server version
		println!("{}", get_server_version_string(e).await?);
	} else {
		// Print local CLI version
		println!("{}", release());
	}
	Ok(())
}

async fn get_server_version_string(endpoint: String) -> Result<String, Error> {
	// Connect to the database engine
	let client = connect(endpoint).await?;
	// Query database version info
	let server_version = client.version().await?;
	// Convert version info to formatted string
	Ok(server_version.to_string())
}
