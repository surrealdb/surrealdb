use super::config;
use super::config::Config;
use crate::cli::validator::parser::env_filter::CustomEnvFilter;
use crate::cli::validator::parser::env_filter::CustomEnvFilterParser;
use crate::cnf::LOGO;
use crate::dbs;
use crate::dbs::StartCommandDbsOptions;
use crate::env;
use crate::err::Error;
use crate::iam;
use crate::net::{self, client_ip::ClientIp};
use clap::Args;
use ipnet::IpNet;
use std::net::SocketAddr;
use std::path::PathBuf;

#[derive(Args, Debug)]
pub struct StartCommandArguments {
	#[arg(help = "Database path used for storing data")]
	#[arg(env = "SURREAL_PATH", index = 1)]
	#[arg(default_value = "memory")]
	#[arg(value_parser = super::validator::path_valid)]
	path: String,
	#[arg(help = "The master username for the database")]
	#[arg(env = "SURREAL_USER", short = 'u', long = "username", visible_alias = "user")]
	#[arg(default_value = "root")]
	username: String,
	#[arg(help = "The master password for the database")]
	#[arg(env = "SURREAL_PASS", short = 'p', long = "password", visible_alias = "pass")]
	password: Option<String>,
	#[arg(help = "The allowed networks for master authentication")]
	#[arg(env = "SURREAL_ADDR", long = "addr")]
	#[arg(default_value = "127.0.0.1/32")]
	allowed_networks: Vec<IpNet>,
	#[arg(help = "The method of detecting the client's IP address")]
	#[arg(env = "SURREAL_CLIENT_IP", long)]
	#[arg(default_value = "socket", value_enum)]
	client_ip: ClientIp,
	#[arg(help = "The hostname or ip address to listen for connections on")]
	#[arg(env = "SURREAL_BIND", short = 'b', long = "bind")]
	#[arg(default_value = "0.0.0.0:8000")]
	listen_addresses: Vec<SocketAddr>,
	#[command(flatten)]
	dbs: StartCommandDbsOptions,
	#[arg(help = "Encryption key to use for on-disk encryption")]
	#[arg(env = "SURREAL_KEY", short = 'k', long = "key")]
	#[arg(value_parser = super::validator::key_valid)]
	key: Option<String>,
	#[command(flatten)]
	kvs: Option<StartCommandRemoteTlsOptions>,
	#[command(flatten)]
	web: Option<StartCommandWebTlsOptions>,
	#[arg(help = "Whether strict mode is enabled on this database instance")]
	#[arg(env = "SURREAL_STRICT", short = 's', long = "strict")]
	#[arg(default_value_t = false)]
	strict: bool,
	#[arg(help = "The logging level for the database server")]
	#[arg(env = "SURREAL_LOG", short = 'l', long = "log")]
	#[arg(default_value = "info")]
	#[arg(value_parser = CustomEnvFilterParser::new())]
	log: CustomEnvFilter,
	#[arg(help = "Whether to hide the startup banner")]
	#[arg(env = "SURREAL_NO_BANNER", long)]
	#[arg(default_value_t = false)]
	no_banner: bool,
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
		strict,
		log: CustomEnvFilter(log),
		no_banner,
		..
	}: StartCommandArguments,
) -> Result<(), Error> {
	// Initialize opentelemetry and logging
	crate::o11y::builder().with_filter(log).init();

	// Check if a banner should be outputted
	if !no_banner {
		// Output SurrealDB logo
		println!("{LOGO}");
	}
	// Setup the cli options
	let _ = config::CF.set(Config {
		strict,
		bind: listen_addresses.first().cloned().unwrap(),
		client_ip,
		path,
		user,
		pass,
		crt: web.as_ref().and_then(|x| x.web_crt.clone()),
		key: web.as_ref().and_then(|x| x.web_key.clone()),
	});
	// Initiate environment
	env::init().await?;
	// Initiate master auth
	iam::init().await?;
	// Start the kvs server
	dbs::init(dbs).await?;
	// Start the web server
	net::init().await?;
	// All ok
	Ok(())
}
