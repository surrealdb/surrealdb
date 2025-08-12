use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

#[cfg(feature = "ml")]
use anyhow::Context;
use anyhow::Result;
use clap::Args;
use surrealdb::engine::{any, tasks};
use tokio_util::sync::CancellationToken;

use super::config::{CF, Config};
use crate::cnf::LOGO;
#[cfg(feature = "ml")]
use crate::core::ml::execution::session::set_environment;
use crate::core::options::EngineOptions;
use crate::dbs::StartCommandDbsOptions;
use crate::net::client_ip::ClientIp;
use crate::{dbs, env, net};

#[derive(Args, Debug)]
pub struct StartCommandArguments {
	#[arg(help = "Database path used for storing data")]
	#[arg(env = "SURREAL_PATH", index = 1)]
	#[arg(default_value = "memory")]
	#[arg(value_parser = super::validator::path_valid)]
	path: String,
	#[arg(help = "Whether to hide the startup banner")]
	#[arg(env = "SURREAL_NO_BANNER", long)]
	#[arg(default_value_t = false)]
	no_banner: bool,
	#[arg(help = "Encryption key to use for on-disk encryption")]
	#[arg(env = "SURREAL_KEY", short = 'k', long = "key")]
	#[arg(value_parser = super::validator::key_valid)]
	#[arg(hide = true)] // Not currently in use
	key: Option<String>,
	//
	// Tasks
	#[arg(
		help = "The interval at which to refresh node registration information",
		help_heading = "Database"
	)]
	#[arg(env = "SURREAL_NODE_MEMBERSHIP_REFRESH_INTERVAL", long = "node-membership-refresh-interval", value_parser = super::validator::duration)]
	#[arg(default_value = "3s")]
	node_membership_refresh_interval: Duration,
	#[arg(
		help = "The interval at which process and archive inactive nodes",
		help_heading = "Database"
	)]
	#[arg(env = "SURREAL_NODE_MEMBERSHIP_CHECK_INTERVAL", long = "node-membership-check-interval", value_parser = super::validator::duration)]
	#[arg(default_value = "15s")]
	node_membership_check_interval: Duration,
	#[arg(
		help = "The interval at which to process and cleanup archived nodes",
		help_heading = "Database"
	)]
	#[arg(env = "SURREAL_NODE_MEMBERSHIP_CLEANUP_INTERVAL", long = "node-membership-cleanup-interval", value_parser = super::validator::duration)]
	#[arg(default_value = "300s")]
	node_membership_cleanup_interval: Duration,
	#[arg(
		help = "The interval at which to perform changefeed garbage collection",
		help_heading = "Database"
	)]
	#[arg(env = "SURREAL_CHANGEFEED_GC_INTERVAL", long = "changefeed-gc-interval", value_parser = super::validator::duration)]
	#[arg(default_value = "10s")]
	changefeed_gc_interval: Duration,
	#[arg(env = "SURREAL_INDEX_COMPACTION_INTERVAL", long = "index-compaction-interval", value_parser = super::validator::duration)]
	#[arg(default_value = "5s")]
	index_compaction_interval: Duration,
	//
	// Authentication
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
	//
	// Datastore connection
	#[command(next_help_heading = "Datastore connection")]
	#[command(flatten)]
	kvs: Option<StartCommandRemoteTlsOptions>,
	//
	// HTTP Server
	#[command(next_help_heading = "HTTP server")]
	#[command(flatten)]
	web: Option<StartCommandWebTlsOptions>,
	#[arg(help = "The method of detecting the client's IP address")]
	#[arg(env = "SURREAL_CLIENT_IP", long)]
	#[arg(default_value = "socket", value_enum)]
	client_ip: ClientIp,
	#[arg(help = "The hostname or IP address to listen for connections on")]
	#[arg(env = "SURREAL_BIND", short = 'b', long = "bind")]
	#[arg(default_value = "127.0.0.1:8000")]
	listen_addresses: Vec<SocketAddr>,
	#[arg(help = "Whether to suppress the server name and version headers")]
	#[arg(env = "SURREAL_NO_IDENTIFICATION_HEADERS", long)]
	#[arg(default_value_t = false)]
	no_identification_headers: bool,
	//
	// Database options
	#[command(flatten)]
	#[command(next_help_heading = "Database")]
	dbs: StartCommandDbsOptions,
}

#[derive(Args, Debug)]
#[group(requires_all = ["kvs_ca", "kvs_crt", "kvs_key"], multiple = true)]
struct StartCommandRemoteTlsOptions {
	#[arg(help = "Path to the CA file used when connecting to the remote KV store")]
	#[arg(env = "SURREAL_KVS_CA", long = "kvs-ca", value_parser = super::validator::file_exists)]
	kvs_ca: Option<PathBuf>,
	#[arg(help = "Path to the certificate file used when connecting to the remote KV store")]
	#[arg(env = "SURREAL_KVS_CRT", long = "kvs-crt", value_parser = super::validator::file_exists)]
	kvs_crt: Option<PathBuf>,
	#[arg(help = "Path to the private key file used when connecting to the remote KV store")]
	#[arg(env = "SURREAL_KVS_KEY", long = "kvs-key", value_parser = super::validator::file_exists)]
	kvs_key: Option<PathBuf>,
}

#[derive(Args, Debug)]
#[group(requires_all = ["web_crt", "web_key"], multiple = true)]
struct StartCommandWebTlsOptions {
	#[arg(help = "Path to the certificate file for encrypted client connections")]
	#[arg(env = "SURREAL_WEB_CRT", long = "web-crt", value_parser = super::validator::file_exists)]
	web_crt: Option<PathBuf>,
	#[arg(help = "Path to the private key file for encrypted client connections")]
	#[arg(env = "SURREAL_WEB_KEY", long = "web-key", value_parser = super::validator::file_exists)]
	web_key: Option<PathBuf>,
}

pub async fn init(
	StartCommandArguments {
		path,
		username: user,
		password: pass,
		client_ip,
		listen_addresses,
		dbs,
		web,
		node_membership_refresh_interval,
		node_membership_check_interval,
		node_membership_cleanup_interval,
		changefeed_gc_interval,
		index_compaction_interval,
		no_banner,
		no_identification_headers,
		..
	}: StartCommandArguments,
) -> Result<()> {
	// Check if we should output a banner
	if !no_banner {
		println!("{LOGO}");
	}
	// Clean the path
	let endpoint = any::__into_endpoint(path)?;
	let path = if endpoint.path.is_empty() {
		endpoint.url.to_string()
	} else {
		endpoint.path
	};
	// Extract the certificate and key
	let (crt, key) = if let Some(val) = web {
		(val.web_crt, val.web_key)
	} else {
		(None, None)
	};
	// Configure the engine
	let engine = EngineOptions::default()
		.with_node_membership_refresh_interval(node_membership_refresh_interval)
		.with_node_membership_check_interval(node_membership_check_interval)
		.with_node_membership_cleanup_interval(node_membership_cleanup_interval)
		.with_changefeed_gc_interval(changefeed_gc_interval)
		.with_index_compaction_interval(index_compaction_interval);
	// Configure the config
	let config = Config {
		bind: listen_addresses.first().copied().unwrap(),
		client_ip,
		path,
		user,
		pass,
		no_identification_headers,
		engine,
		crt,
		key,
	};
	// Setup the command-line options
	let _ = CF.set(config);
	// Initiate environment
	env::init()?;

	// if ML feature is enabled load the ONNX runtime lib that is embedded
	#[cfg(feature = "ml")]
	set_environment().context("Failed to initialize ML library")?;

	// Create a token to cancel tasks
	let canceller = CancellationToken::new();
	// Start the datastore
	let datastore = Arc::new(dbs::init(dbs).await?);
	// Start the node agent
	let nodetasks = tasks::init(datastore.clone(), canceller.clone(), &CF.get().unwrap().engine);
	// Start the web server
	net::init(datastore.clone(), canceller.clone()).await?;
	// Shutdown and stop closed tasks
	canceller.cancel();
	// Wait for background tasks to finish
	nodetasks.resolve().await?;
	// Shutdown the datastore
	datastore.shutdown().await?;
	// All ok
	Ok(())
}
