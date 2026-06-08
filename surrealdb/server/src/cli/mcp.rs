//! `surreal mcp` CLI subcommand.
//!
//! Starts the MCP server over stdio, suitable for IDE integration with
//! Cursor, VS Code, Claude Desktop, etc.

use std::sync::Arc;

use anyhow::Result;
use clap::Args;
use surrealdb::engine::any;
use surrealdb_core::buc::BucketStoreProvider;
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::TransactionBuilderFactory;
use surrealdb_core::options::EngineOptions;
use surrealdb_mcp::McpService;
use tokio_util::sync::CancellationToken;

use super::config::ConfigCheck;
use crate::dbs;
use crate::dbs::StartCommandDbsOptions;
use crate::ntw::RouterFactory;
use crate::observe::ObservabilityRuntime;

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
	C: TransactionBuilderFactory
		+ RouterFactory
		+ ConfigCheck
		+ BucketStoreProvider
		+ crate::observe::ObservabilityProvider,
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
	runtime: ObservabilityRuntime,
) -> Result<()> {
	// Install the rustls process-default crypto provider before any TLS
	// operations occur. Under `feature = "fips"` this asserts FIPS mode is
	// active and aborts startup otherwise.
	crate::tls::install_default_crypto_provider()?;

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

	// MCP stdio has no `/metrics` HTTP endpoint, so we skip building a
	// `MetricsObserver`. Composer-contributed observers (audit,
	// slow-query, ...) still wire through `create_observer_with_runtime`
	// so they reach the runtime's audit logger / meter providers,
	// mirroring the `METRICS_ENABLED=false` branch in
	// `start::build_observability`.
	let observer = <C as crate::observe::ObservabilityProvider>::create_observer_with_runtime(
		&composer, &runtime,
	);

	let canceller = CancellationToken::new();
	let (datastore, _recv, _router_state) =
		dbs::init::<C>(composer, &config, canceller.clone(), observer, dbs_opts).await?;
	let datastore = Arc::new(datastore);

	// The STDIO transport is a locally-trusted, in-process connection: the
	// operator who launched `surreal mcp` is the one driving the MCP client,
	// and there is no network layer between them. Use an owner-level session
	// so the MCP server works regardless of whether the datastore has auth
	// enabled or guest access disabled.
	let base_session = Session::owner();
	let mut service = McpService::new(Arc::clone(&datastore), namespace, database, base_session)
		.with_transport_label("stdio");
	// When telemetry has produced a meter provider (Prometheus disabled but
	// OTLP enabled, or just the no-op default), build a `MetricsObserver`
	// so MCP tool dispatch is recorded against `surrealdb.mcp.tool.*`.
	// Building the observer when no readers are attached is harmless: the
	// OTel SDK short-circuits no-op meters internally.
	if let Ok(observer) = crate::observe::MetricsObserver::new(&runtime) {
		let recorder: Arc<dyn surrealdb_mcp::metrics::McpMetricsRecorder> =
			Arc::new(crate::observe::McpRecorderAdapter::new(Arc::new(observer)));
		service = service.with_metrics_recorder(recorder);
	}

	tracing::info!(target: "surrealdb::mcp", "Starting MCP server over stdio");

	surrealdb_mcp::service::serve_stdio(service).await?;

	canceller.cancel();
	datastore.shutdown().await?;

	Ok(())
}
