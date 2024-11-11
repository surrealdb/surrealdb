use crate::net::client_ip::ClientIp;
use std::sync::OnceLock;
use std::{net::SocketAddr, path::PathBuf};
use surrealdb::options::EngineOptions;

pub static CF: OnceLock<Config> = OnceLock::new();

#[derive(Clone, Debug)]
pub struct Config {
	pub bind: SocketAddr,
	pub path: String,
	pub client_ip: ClientIp,
	pub user: Option<String>,
	pub pass: Option<String>,
	pub crt: Option<PathBuf>,
	pub key: Option<PathBuf>,
	pub engine: EngineOptions,
	pub no_identification_headers: bool,
}
