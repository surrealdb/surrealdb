use anyhow::Result;
use clap::Args;
use futures_util::StreamExt;
use surrealdb::Connection;
use surrealdb::engine::any::{self, connect};
use surrealdb::method::{Export, ExportConfig};
use surrealdb_core::kvs::export::TableConfig;
use tokio::io::{self, AsyncWriteExt};

use crate::cli::abstraction::auth::{CredentialsBuilder, CredentialsLevel};
use crate::cli::abstraction::{AuthArguments, DatabaseSelectionArguments};

#[derive(Args, Debug)]
pub struct DatabaseConnectionArguments {
	#[arg(help = "Database endpoint to export from")]
	#[arg(short = 'e', long = "endpoint")]
	#[arg(default_value = "http://localhost:8000")]
	#[arg(value_parser = super::validator::endpoint_valid)]
	pub(crate) endpoint: String,
}

#[derive(Args, Debug)]
struct ExportConfigArguments {
	/// Whether only specific resources should be exported
	#[arg(long)]
	only: bool,
	/// Whether users should be exported
	#[arg(long, num_args = 0..=1, default_missing_value = "true")]
	users: Option<bool>,
	/// Whether access methods should be exported
	#[arg(long, num_args = 0..=1, default_missing_value = "true")]
	accesses: Option<bool>,
	/// Whether params should be exported
	#[arg(long, num_args = 0..=1, default_missing_value = "true")]
	params: Option<bool>,
	/// Whether functions should be exported
	#[arg(long, num_args = 0..=1, default_missing_value = "true")]
	functions: Option<bool>,
	/// Whether analyzers should be exported
	#[arg(long, num_args = 0..=1, default_missing_value = "true")]
	analyzers: Option<bool>,
	/// Whether tables should be exported, optionally providing a list of tables
	#[arg(long, num_args = 0..=1, default_missing_value = "true", value_parser = super::validator::export_tables)]
	tables: Option<TableConfig>,
	/// Comma-separated list of tables to exclude from the export
	#[arg(long, value_parser = super::validator::export_tables_exclude)]
	#[arg(conflicts_with = "tables")]
	tables_exclude: Option<TableConfig>,
	/// Whether versions should be exported
	#[arg(long, num_args = 0..=1, default_missing_value = "true")]
	versions: Option<bool>,
	/// Whether records should be exported
	#[arg(long, num_args = 0..=1, default_missing_value = "true")]
	records: Option<bool>,
	/// Whether apis should be exported
	#[arg(long, num_args = 0..=1, default_missing_value = "true")]
	apis: Option<bool>,
	/// Whether buckets should be exported
	#[arg(long, num_args = 0..=1, default_missing_value = "true")]
	buckets: Option<bool>,
	/// Whether modules should be exported
	#[arg(long, num_args = 0..=1, default_missing_value = "true")]
	modules: Option<bool>,
	/// Whether configs should be exported
	#[arg(long, num_args = 0..=1, default_missing_value = "true")]
	configs: Option<bool>,
}

#[derive(Args, Debug)]
pub struct ExportCommandArguments {
	#[arg(help = "Path to the SurrealQL file to export. Use dash - to write into stdout.")]
	#[arg(default_value = "-")]
	#[arg(index = 1)]
	file: String,
	#[command(flatten)]
	conn: DatabaseConnectionArguments,
	#[command(flatten)]
	auth: AuthArguments,
	#[command(flatten)]
	sel: DatabaseSelectionArguments,
	#[command(flatten)]
	config: ExportConfigArguments,
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
			token,
			auth_level,
		},
		sel: DatabaseSelectionArguments {
			namespace,
			database,
		},
		config,
	}: ExportCommandArguments,
) -> Result<()> {
	let is_local = any::__into_endpoint(&endpoint)?.parse_kind()?.is_local();
	// If username and password are specified, and we are connecting to a remote
	// SurrealDB server, then we need to authenticate. If we are connecting
	// directly to a datastore (i.e. surrealkv://local.skv or tikv://...), then we
	// don't need to authenticate because we use an embedded (local) SurrealDB
	// instance with auth disabled.
	let client = if username.is_some() && password.is_some() && !is_local {
		debug!("Connecting to the database engine with authentication");
		let creds = CredentialsBuilder::default()
			.with_username(username.clone())
			.with_password(password.clone())
			.with_namespace(namespace.clone())
			.with_database(database.clone());

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
		if let Some(t) = token {
			client.authenticate(t).await?;
		}
		client
	} else {
		debug!("Connecting to the database engine without authentication");
		connect(endpoint).await?
	};

	// Use the specified namespace / database
	client.use_ns(namespace).use_db(database).await?;

	// Export the data from the database
	debug!("Exporting data from the database");
	if file == "-" {
		// Prepare the backup
		let mut backup = apply_config(config, client.export(())).await?;
		// Get a handle to standard output
		let mut stdout = io::stdout();
		// Write the backup to standard output
		while let Some(bytes) = backup.next().await {
			stdout.write_all(&bytes?).await?;
		}
	} else {
		apply_config(config, client.export(file)).await?;
	}
	info!("The SurrealQL file was exported successfully");
	// Everything OK
	Ok(())
}

fn apply_config<C: Connection, R>(
	config: ExportConfigArguments,
	export: Export<C, R>,
) -> Export<C, R, ExportConfig> {
	let mut export = export.with_config();

	if config.only {
		export = export
			.users(false)
			.accesses(false)
			.params(false)
			.functions(false)
			.analyzers(false)
			.tables(false)
			.versions(false)
			.records(false);
	}

	if let Some(value) = config.users {
		export = export.users(value);
	}

	if let Some(value) = config.accesses {
		export = export.accesses(value);
	}

	if let Some(value) = config.params {
		export = export.params(value);
	}

	if let Some(value) = config.functions {
		export = export.functions(value);
	}

	if let Some(value) = config.analyzers {
		export = export.analyzers(value);
	}

	match (config.tables, config.tables_exclude) {
		(Some(_), Some(_)) => unreachable!("Cannot specify both --tables and --tables-exclude"),
		(Some(tables), None) => export = export.tables(tables),
		(None, Some(tables_exclude)) => export = export.tables(tables_exclude),
		(None, None) => {}
	}

	if let Some(value) = config.versions {
		export = export.versions(value);
	}

	if let Some(value) = config.records {
		export = export.records(value);
	}

	if let Some(value) = config.apis {
		export = export.apis(value);
	}

	if let Some(value) = config.buckets {
		export = export.buckets(value);
	}

	if let Some(value) = config.modules {
		export = export.modules(value);
	}

	if let Some(value) = config.configs {
		export = export.configs(value);
	}

	export
}
