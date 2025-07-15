use super::config::{CF, Config};
use crate::cnf::LOGO;
use crate::dbs;
use crate::dbs::StartCommandDbsOptions;
use crate::env;
use crate::net::{self, client_ip::ClientIp};
use anyhow::Result;
use clap::Args;
use std::net::SocketAddr;
use std::path::PathBuf;
use tokio_util::sync::CancellationToken;

#[cfg(feature = "ml")]
use anyhow::Context;
#[cfg(feature = "ml")]
use surrealdb_core::ml::execution::session::set_environment;

#[derive(Args, Debug)]
pub struct StartCommandArguments {
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
		client_ip,
		listen_addresses,
		dbs: dbs_opts,
		web,
		no_banner,
		no_identification_headers,
		..
	}: StartCommandArguments,
) -> Result<()> {
	// Check if we should output a banner
	if !no_banner {
		println!("{LOGO}");
	}
	// use anyhow::Context;
	// let client =
	// 	Surreal::connect(path.as_str()).await.context("Failed to connect to database")?;
	// Extract the certificate and key
	let (crt, key) = if let Some(val) = web {
		(val.web_crt, val.web_key)
	} else {
		(None, None)
	};
	// Configure the config
	let config = Config {
		bind: listen_addresses.first().copied().unwrap(),
		client_ip,
		no_identification_headers,
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
	let cancellation_token = CancellationToken::new();
	// Start the datastore
	let surrealdb = dbs::init(dbs_opts, cancellation_token.clone()).await?;
	// Start the web server
	let api_datastore = surrealdb.kvs();
	let api_canceller = cancellation_token.clone();

	tokio::select! {
		result = tokio::spawn(async move { net::init(api_datastore, api_canceller).await }) => {
			if let Err(e) = result {
				error!("Failed to start web server: {:?}", e);
				cancellation_token.cancel();
			}
		}
		result = surrealdb.into_future() => {
			if let Err(e) = result {
				error!("Failed to start datastore: {:?}", e);
				cancellation_token.cancel();
			}
		}
	}

	// All ok
	Ok(())
}
