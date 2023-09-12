use crate::cli::abstraction::{
	AuthArguments, DatabaseConnectionArguments, DatabaseSelectionArguments,
};
use crate::err::Error;
use clap::Args;
use surrealdb::dbs::Capabilities;
use surrealdb::engine::any::connect;
use surrealdb::opt::auth::Root;
use surrealdb::opt::Config;

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
	crate::telemetry::builder().with_log_level("info").init();
	// Default datastore configuration for local engines
	let config = Config::new().capabilities(Capabilities::all());

	let client = if let Some((username, password)) = username.zip(password) {
		let root = Root {
			username: &username,
			password: &password,
		};

		// Connect to the database engine with authentication
		//
		// * For local engines, here we enable authentication and in the signin below we actually authenticate.
		// * For remote engines, we connect to the endpoint and then signin.
		#[cfg(feature = "has-storage")]
		let address = (endpoint, config.user(root));
		#[cfg(not(feature = "has-storage"))]
		let address = endpoint;
		let client = connect(address).await?;

		// Sign in to the server
		client.signin(root).await?;
		client
	} else {
		connect((endpoint, config)).await?
	};

	// Use the specified namespace / database
	client.use_ns(ns).use_db(db).await?;
	// Import the data into the database
	client.import(file).await?;
	info!("The SQL file was imported successfully");
	// Everything OK
	Ok(())
}
