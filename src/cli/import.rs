use crate::cli::abstraction::{
	AuthArguments, DatabaseConnectionArguments, DatabaseSelectionArguments,
};
use crate::err::Error;
use clap::Args;
use surrealdb::engine::any::connect;
use surrealdb::opt::auth::Root;

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
			endpoint,
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

	let root = Root {
		username: &username,
		password: &password,
	};
	// Connect to the database engine
	#[cfg(feature = "has-storage")]
	let address = (endpoint, root);
	#[cfg(not(feature = "has-storage"))]
	let address = endpoint;
	let client = connect(address).await?;
	// Sign in to the server
	client.signin(root).await?;
	// Use the specified namespace / database
	client.use_ns(ns).use_db(db).await?;
	// Import the data into the database
	client.import(file).await?;
	info!("The SQL file was imported successfully");
	// Everything OK
	Ok(())
}
