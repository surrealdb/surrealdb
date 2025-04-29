use crate::cli::abstraction::auth::{CredentialsBuilder, CredentialsLevel};
use crate::cli::abstraction::{AuthArguments, DatabaseSelectionArguments};
use crate::err::Error;
use clap::Args;
use surrealdb::engine::any::{connect, IntoEndpoint};
use surrealdb::opt::{capabilities::Capabilities, Config};

#[derive(Args, Debug)]
pub struct DatabaseConnectionArguments {
	#[arg(help = "Database endpoint to import to")]
	#[arg(short = 'e', long = "endpoint", visible_aliases = ["conn"])]
	#[arg(default_value = "http://localhost:8000")]
	#[arg(value_parser = super::validator::endpoint_valid)]
	pub(crate) endpoint: String,
}

#[derive(Args, Debug)]
pub struct ImportCommandArguments {
	#[arg(help = "Path to the SurrealQL file to import")]
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
) -> Result<(), Error> {
	// Default datastore configuration for local engines
	let config = Config::new().capabilities(Capabilities::all());
	// If username and password are specified, and we are connecting to a remote SurrealDB server, then we need to authenticate.
	// If we are connecting directly to a datastore (i.e. surrealkv://local.skv or tikv://...), then we don't need to authenticate because we use an embedded (local) SurrealDB instance with auth disabled.
	let client = if username.is_some()
		&& password.is_some()
		&& !endpoint.clone().into_endpoint()?.parse_kind()?.is_local()
	{
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
	} else if token.is_some() && !endpoint.clone().into_endpoint()?.parse_kind()?.is_local() {
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
	client.import(file).await.inspect_err(|_| {
		error!("Surreal import failed, import might only be partially completed or have failed entirely.")
	})?;
	info!("The SurrealQL file was imported successfully");
	// All ok
	Ok(())
}
