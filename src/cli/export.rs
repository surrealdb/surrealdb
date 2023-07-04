use crate::cli::abstraction::{
	AuthArguments, DatabaseConnectionArguments, DatabaseSelectionArguments,
};
use crate::err::Error;
use clap::Args;
use surrealdb::engine::any::connect;
use surrealdb::opt::auth::Root;

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
	// Export the data from the database
	client.export(file).await?;
	info!("The SQL file was exported successfully");
	// Everything OK
	Ok(())
}
