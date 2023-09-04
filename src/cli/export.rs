use crate::cli::abstraction::{
	AuthArguments, DatabaseConnectionArguments, DatabaseSelectionArguments,
};
use crate::err::Error;
use clap::Args;
use futures_util::StreamExt;
use surrealdb::engine::any::connect;
use surrealdb::opt::auth::Root;
use surrealdb::opt::Config;
use tokio::io::{self, AsyncWriteExt};

#[derive(Args, Debug)]
pub struct ExportCommandArguments {
	#[arg(help = "Path to the sql file to export. Use dash - to write into stdout.")]
	#[arg(default_value = "-")]
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
	ExportCommandArguments {
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
	}: ExportCommandArguments,
) -> Result<(), Error> {
	// Initialize opentelemetry and logging
	crate::telemetry::builder().with_log_level("error").init();

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
		let address = (endpoint, Config::new().user(root));
		#[cfg(not(feature = "has-storage"))]
		let address = endpoint;
		let client = connect(address).await?;

		// Sign in to the server
		client.signin(root).await?;
		client
	} else {
		connect(endpoint).await?
	};

	// Use the specified namespace / database
	client.use_ns(ns).use_db(db).await?;
	// Export the data from the database
	if file == "-" {
		// Prepare the backup
		let mut backup = client.export(()).await?;
		// Get a handle to standard output
		let mut stdout = io::stdout();
		// Write the backup to standard output
		while let Some(bytes) = backup.next().await {
			stdout.write_all(&bytes?).await?;
		}
	} else {
		client.export(file).await?;
	}
	info!("The SQL file was exported successfully");
	// Everything OK
	Ok(())
}
