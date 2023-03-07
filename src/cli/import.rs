use crate::cli::LOG;
use crate::err::Error;
use surrealdb::engine::any::connect;
use surrealdb::error::Api as ApiError;
use surrealdb::opt::auth::Root;
use surrealdb::Error as SurrealError;

#[tokio::main]
pub async fn init(matches: &clap::ArgMatches) -> Result<(), Error> {
	// Set the default logging level
	crate::cli::log::init(1);
	// Try to parse the file argument
	let file = matches.value_of("file").unwrap();
	// Parse all other cli arguments
	let username = matches.value_of("user").unwrap();
	let password = matches.value_of("pass").unwrap();
	let endpoint = matches.value_of("conn").unwrap();
	let ns = matches.value_of("ns").unwrap();
	let db = matches.value_of("db").unwrap();
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
	// Import the data into the database
	client.import(file).await?;
	info!(target: LOG, "The SQL file was imported successfully");
	// Everything OK
	Ok(())
}
