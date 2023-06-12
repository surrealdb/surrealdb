use crate::cli::abstraction::{
	AuthArguments, DatabaseConnectionArguments, DatabaseSelectionArguments,
};
use crate::cli::LOG;
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

	let client = if username.is_none() {
		connect(endpoint.to_owned()).await?
	} else {
		let root = Root {
			username: &username.unwrap(),
			password: &password.expect("Password is required when username is provided"),
		};

		// Connect to the database engine with authentication
		//
		// NOTE: Why do we need to do this? This code is used to connect to local and remote engines.
		// * For local engines, here we enable authentication and in the signin below we actually authenticate.
		// * For remote engines, it's not really necessary, because auth is already configured by the server.
		// It was decided to do it this way to keep the same code in both scenarios.
		let client = connect((endpoint, root)).await?;

		// Sign in to the server
		client.signin(root).await?;
		client
	};

	// Use the specified namespace / database
	client.use_ns(ns).use_db(db).await?;
	// Import the data into the database
	client.import(file).await?;
	info!(target: LOG, "The SQL file was imported successfully");
	// Everything OK
	Ok(())
}
