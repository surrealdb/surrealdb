use crate::cli::LOG;
use crate::err::Error;
use clap::Args;
use surrealdb::engine::any::connect;
use surrealdb::error::Api as ApiError;
use surrealdb::opt::auth::Root;
use surrealdb::Error as SurrealError;
use crate::cli::abstraction::{AuthArguments, DatabaseConnectionArguments, DatabaseSelectionArguments};

#[derive(Args, Debug)]
pub struct ImportCommandArguments {
	#[arg(help = "Path to the sql file to import")]
	#[arg(index = 1)]
	file: String,
	#[command(flatten)]
	conn: DatabaseConnectionArguments,
	#[command(flatten)]
	auth: AuthArguments,
	#[command(flatten)]
	sel: DatabaseSelectionArguments,
}

pub async fn init(
	ImportCommandArguments {
		file,
		conn: DatabaseConnectionArguments {
			connection_url: endpoint,
		},
		auth: AuthArguments {
			username,
			password,
		},
		sel: DatabaseSelectionArguments {
			namespace: ns,
			database: db,
		},
	}: ImportCommandArguments,
) -> Result<(), Error> {
	// Initialize opentelemetry and logging
	crate::o11y::builder().with_log_level("error").init();

	// Connect to the database engine
	let client = connect(endpoint).await?;
	// Sign in to the server if the specified database engine supports it
	let root = Root {
		username: &username,
		password: &password,
	};
	if let Err(error) = client.signin(root).await {
		match error {
			// Authentication not supported by this engine, we can safely continue
			SurrealError::Api(ApiError::AuthNotSupported) => {}
			error => {
				return Err(error.into());
			}
		}
	}
	// Use the specified namespace / database
	client.use_ns(ns).use_db(db).await?;
	// Import the data into the database
	client.import(file).await?;
	info!(target: LOG, "The SQL file was imported successfully");
	// Everything OK
	Ok(())
}
