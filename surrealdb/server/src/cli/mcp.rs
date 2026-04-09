//! `surreal mcp` CLI subcommand.
//!
//! Starts the MCP server over stdio, suitable for IDE integration with
//! Cursor, VS Code, Claude Desktop, etc.

use std::sync::Arc;

use anyhow::Result;
use clap::Args;
use rustls::crypto::CryptoProvider;
use surrealdb::engine::any;
use surrealdb_core::buc::BucketStoreProvider;
use surrealdb_core::kvs::TransactionBuilderFactory;
use surrealdb_core::options::EngineOptions;
use surrealdb_mcp::McpService;
use tokio_util::sync::CancellationToken;

use super::config::ConfigCheck;
use crate::dbs;
use crate::dbs::StartCommandDbsOptions;
use crate::ntw::RouterFactory;

#[derive(Args, Debug)]
pub struct McpCommandArguments {
	#[arg(help = "Database path used for storing data")]
	#[arg(env = "SURREAL_PATH", index = 1)]
	#[arg(default_value = "memory")]
	path: String,
	#[arg(help = "The initial namespace to use")]
	#[arg(env = "SURREAL_MCP_NS", long = "ns")]
	namespace: Option<String>,
	#[arg(help = "The initial database to use")]
	#[arg(env = "SURREAL_MCP_DB", long = "db")]
	database: Option<String>,
	#[arg(
		help = "The username for the initial database root user. Only if no other root user exists",
		help_heading = "Authentication"
	)]
	#[arg(
		env = "SURREAL_USER",
		short = 'u',
		long = "username",
		visible_alias = "user",
		requires = "password"
	)]
	username: Option<String>,
	#[arg(
		help = "The password for the initial database root user. Only if no other root user exists",
		help_heading = "Authentication"
	)]
	#[arg(
		env = "SURREAL_PASS",
		short = 'p',
		long = "password",
		visible_alias = "pass",
		requires = "username"
	)]
	password: Option<String>,
	#[command(flatten)]
	#[command(next_help_heading = "Database")]
	dbs: StartCommandDbsOptions,
}

/// Start the MCP server over stdio.
pub async fn init<
	C: TransactionBuilderFactory + RouterFactory + ConfigCheck + BucketStoreProvider,
>(
	composer: C,
	McpCommandArguments {
		path,
		namespace,
		database,
		username: user,
		password: pass,
		dbs: dbs_opts,
	}: McpCommandArguments,
) -> Result<()> {
	let _ = CryptoProvider::install_default(rustls::crypto::aws_lc_rs::default_provider());

	C::path_valid(&path)?;

	let endpoint = any::__into_endpoint(path)?;
	let path = if endpoint.path.is_empty() {
		endpoint.url.to_string()
	} else {
		endpoint.path
	};

	let engine = EngineOptions::default();

	let config = super::config::Config {
		bind: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
		client_ip: crate::ntw::client_ip::ClientIp::Socket,
		path,
		user,
		pass,
		no_identification_headers: true,
		allow_origin: Vec::new(),
		engine,
		crt: None,
		key: None,
	};

	crate::env::init()?;

	let canceller = CancellationToken::new();
	let datastore = Arc::new(dbs::init::<C>(composer, &config, canceller.clone(), dbs_opts).await?);

	let service = McpService::new(datastore.clone(), namespace, database);

	tracing::info!(target: "surrealdb::mcp", "Starting MCP server over stdio");

	surrealdb_mcp::service::serve_stdio(service).await?;

	canceller.cancel();
	datastore.shutdown().await?;

	Ok(())
}
