use super::config;
use super::config::Config;
use crate::cli::validator::parser::env_filter::CustomEnvFilter;
use crate::cli::validator::parser::env_filter::CustomEnvFilterParser;
use crate::cnf::LOGO;
use crate::dbs;
use crate::dbs::{StartCommandDbsOptions, DB};
use crate::env;
use crate::err::Error;
use crate::net::{self, client_ip::ClientIp};
use clap::Args;
use opentelemetry::Context as TelemetryContext;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;
use surrealdb::engine::any::IntoEndpoint;
use surrealdb::engine::tasks::start_tasks;
use tokio_util::sync::CancellationToken;

#[derive(Args, Debug)]
pub struct StartCommandArguments {
	#[arg(help = "Database path used for storing data")]
	#[arg(env = "SURREAL_PATH", index = 1)]
	#[arg(default_value = "memory")]
	#[arg(value_parser = super::validator::path_valid)]
	path: String,
	#[arg(help = "The logging level for the database server")]
	#[arg(env = "SURREAL_LOG", short = 'l', long = "log")]
	#[arg(default_value = "info")]
	#[arg(value_parser = CustomEnvFilterParser::new())]
	log: CustomEnvFilter,
	#[arg(help = "Whether to hide the startup banner")]
	#[arg(env = "SURREAL_NO_BANNER", long)]
	#[arg(default_value_t = false)]
	no_banner: bool,
	#[arg(help = "Encryption key to use for on-disk encryption")]
	#[arg(env = "SURREAL_KEY", short = 'k', long = "key")]
	#[arg(value_parser = super::validator::key_valid)]
	#[arg(hide = true)] // Not currently in use
	key: Option<String>,

	#[arg(
		help = "The interval at which to run node agent tick (including garbage collection)",
		help_heading = "Database"
	)]
	#[arg(env = "SURREAL_TICK_INTERVAL", long = "tick-interval", value_parser = super::validator::duration)]
	#[arg(default_value = "10s")]
	tick_interval: Duration,

	//
	// Authentication
	//
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
	//
	#[command(next_help_heading = "Datastore connection")]
	#[command(flatten)]
	kvs: Option<StartCommandRemoteTlsOptions>,

	//
	// HTTP Server
	//
	#[command(next_help_heading = "HTTP server")]
	#[command(flatten)]
	web: Option<StartCommandWebTlsOptions>,
	#[arg(help = "The method of detecting the client's IP address")]
	#[arg(env = "SURREAL_CLIENT_IP", long)]
	#[arg(default_value = "socket", value_enum)]
	client_ip: ClientIp,
	#[arg(help = "The hostname or ip address to listen for connections on")]
	#[arg(env = "SURREAL_BIND", short = 'b', long = "bind")]
	#[arg(default_value = "0.0.0.0:8000")]
	listen_addresses: Vec<SocketAddr>,

	//
	// Database options
	//
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
		log,
		tick_interval,
		no_banner,
		..
	}: StartCommandArguments,
) -> Result<(), Error> {
	// Initialize opentelemetry and logging
	crate::telemetry::builder().with_filter(log).init();
	// Start metrics subsystem
	crate::telemetry::metrics::init(&TelemetryContext::current())
		.expect("failed to initialize metrics");

	// Check if a banner should be outputted
	if !no_banner {
		// Output SurrealDB logo
		println!("{LOGO}");
	}
	// Clean the path
	let endpoint = path.into_endpoint()?;
	let path = if endpoint.path.is_empty() {
		endpoint.url.to_string()
	} else {
		endpoint.path
	};
	// Setup the cli options
	let _ = config::CF.set(Config {
		bind: listen_addresses.first().cloned().unwrap(),
		client_ip,
		path,
		user,
		pass,
		tick_interval,
		crt: web.as_ref().and_then(|x| x.web_crt.clone()),
		key: web.as_ref().and_then(|x| x.web_key.clone()),
		engine: None,
	});
	// This is the cancellation token propagated down to
	// all the async functions that needs to be stopped gracefully.
	let ct = CancellationToken::new();
	// Initiate environment
	env::init().await?;
	// Start the kvs server
	dbs::init(dbs).await?;
	// Start the node agent
	let (tasks, task_chans) = start_tasks(
		&config::CF.get().unwrap().engine.unwrap_or_default(),
		DB.get().unwrap().clone(),
	);
	// Start the web server
	net::init(ct.clone()).await?;
	// Shutdown and stop closed tasks
	task_chans.into_iter().for_each(|chan| {
		if let Err(e) = chan.send(()) {
			error!("Failed to send shutdown signal to task: {}", e);
		}
	});
	ct.cancel();
	tasks.resolve().await?;
	// All ok
	Ok(())
}
