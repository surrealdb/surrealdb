use crate::cli::LOG;
use crate::err::Error;
use surrealdb::engine::any::connect;
use surrealdb::error::Api as ApiError;
use surrealdb::opt::auth::Root;
use surrealdb::Error as SurrealError;

#[tokio::main]
pub async fn init(matches: &clap::ArgMatches) -> Result<(), Error> {
	// Initialize opentelemetry and logging
	crate::o11y::builder().with_log_level("error").init();
	// Try to parse the file argument
	let file = matches.get_one::<String>("file").unwrap();
	// Parse all other cli arguments
	let username = matches.get_one::<String>("user").unwrap();
	let password = matches.get_one::<String>("pass").unwrap();
	let endpoint = matches.get_one::<String>("conn").unwrap();
	let ns = matches.get_one::<String>("ns").unwrap();
	let db = matches.get_one::<String>("db").unwrap();
	// Connect to the database engine
	let client = connect(endpoint).await?;
	// Sign in to the server if the specified database engine supports it
	let root = Root {
		username,
		password,
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
	// Export the data from the database
	client.export(file).await?;
	info!(target: LOG, "The SQL file was exported successfully");
	// Everything OK
	Ok(())
}
