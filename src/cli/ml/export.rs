use crate::cli::abstraction::auth::{CredentialsBuilder, CredentialsLevel};
use crate::cli::abstraction::{
	AuthArguments, DatabaseConnectionArguments, DatabaseSelectionArguments,
};
use crate::err::Error;
use clap::Args;
use futures_util::StreamExt;
use surrealdb::engine::any::{connect, IntoEndpoint};
use tokio::io::{self, AsyncWriteExt};

#[derive(Args, Debug)]
pub struct ModelArguments {
	#[arg(help = "The name of the model")]
	#[arg(env = "SURREAL_NAME", long = "name")]
	pub(crate) name: String,
	#[arg(help = "The version of the model")]
	#[arg(env = "SURREAL_VERSION", long = "version")]
	pub(crate) version: String,
}

#[derive(Args, Debug)]
pub struct ExportCommandArguments {
	#[arg(help = "Path to the SurrealML file to export. Use dash - to write into stdout.")]
	#[arg(default_value = "-")]
	#[arg(index = 1)]
	file: String,

	#[command(flatten)]
	model: ModelArguments,
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
		model: ModelArguments {
			name,
			version,
		},
		conn: DatabaseConnectionArguments {
			endpoint,
		},
		auth: AuthArguments {
			username,
			password,
			auth_level,
		},
		sel: DatabaseSelectionArguments {
			namespace,
			database,
		},
	}: ExportCommandArguments,
) -> Result<(), Error> {
	// Initialize opentelemetry and logging
	crate::telemetry::builder().with_log_level("error").init();

	// If username and password are specified, and we are connecting to a remote SurrealDB server, then we need to authenticate.
	// If we are connecting directly to a datastore (i.e. file://local.db or tikv://...), then we don't need to authenticate because we use an embedded (local) SurrealDB instance with auth disabled.
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
	} else {
		debug!("Connecting to the database engine without authentication");
		connect(endpoint).await?
	};

	// Parse model version
	let version = match version.parse() {
		Ok(version) => version,
		Err(_) => {
			return Err(Error::Other(format!("`{version}` is not a valid semantic version")));
		}
	};

	// Use the specified namespace / database
	client.use_ns(namespace).use_db(database).await?;
	// Export the data from the database
	debug!("Exporting data from the database");
	if file == "-" {
		// Prepare the backup
		let mut backup = client.export(()).ml(&name, version).await?;
		// Get a handle to standard output
		let mut stdout = io::stdout();
		// Write the backup to standard output
		while let Some(bytes) = backup.next().await {
			stdout.write_all(&bytes?).await?;
		}
	} else {
		client.export(file).ml(&name, version).await?;
	}
	info!("The SurrealML file was exported successfully");
	// Everything OK
	Ok(())
}
