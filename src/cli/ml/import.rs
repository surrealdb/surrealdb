use anyhow::Result;
use clap::Args;
use surrealdb::engine::any::{self, connect};
use surrealdb::opt::Config;
use surrealdb::opt::capabilities::Capabilities;

use crate::cli::abstraction::auth::{CredentialsBuilder, CredentialsLevel};
use crate::cli::abstraction::{
	AuthArguments, DatabaseConnectionArguments, DatabaseSelectionArguments,
};

#[derive(Args, Debug)]
pub struct ImportCommandArguments {
	#[arg(help = "Path to the SurrealML file to import")]
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
			token,
			auth_level,
		},
		sel: DatabaseSelectionArguments {
			namespace,
			database,
		},
	}: ImportCommandArguments,
) -> Result<()> {
	// Default datastore configuration for local engines
	let config = Config::new().capabilities(Capabilities::all());
	let is_local = any::__into_endpoint(&endpoint)?.parse_kind()?.is_local();
	// If username and password are specified, and we are connecting to a remote
	// SurrealDB server, then we need to authenticate. If we are connecting
	// directly to a datastore (i.e. surrealkv://local.skv or tikv://...), then we
	// don't need to authenticate because we use an embedded (local) SurrealDB
	// instance with auth disabled.
	let client = if username.is_some() && password.is_some() && !is_local {
		debug!("Connecting to the database engine with authentication");
		let creds = CredentialsBuilder::default()
			.with_username(username.as_deref())
			.with_password(password.as_deref())
			.with_namespace(namespace.as_str())
			.with_database(database.as_str());

		let client = connect(endpoint).await?;

		debug!("Signing in to the database engine at '{:?}' level", auth_level);
		match auth_level {
			CredentialsLevel::Root => client.signin(creds.root()?).await?,
			CredentialsLevel::Namespace => client.signin(creds.namespace()?).await?,
			CredentialsLevel::Database => client.signin(creds.database()?).await?,
		};

		client
	} else if token.is_some() && !is_local {
		let client = connect(endpoint).await?;
		client.authenticate(token.unwrap()).await?;

		client
	} else {
		debug!("Connecting to the database engine without authentication");
		connect((endpoint, config)).await?
	};

	// Use the specified namespace / database
	client.use_ns(namespace).use_db(database).await?;
	// Import the data into the database
	client.import(file).ml().await?;
	info!("The SurrealML file was imported successfully");
	// All ok
	Ok(())
}
