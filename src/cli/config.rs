use crate::net::client_ip::ClientIp;
use std::sync::OnceLock;
use std::{net::SocketAddr, path::PathBuf};

pub static CF: OnceLock<Config> = OnceLock::new();

use std::time::Duration;
use surrealdb::options::EngineOptions;

#[derive(Clone, Debug)]
pub struct Config {
	pub bind: SocketAddr,
	pub path: String,
	pub client_ip: ClientIp,
	pub user: Option<String>,
	pub pass: Option<String>,
	pub crt: Option<PathBuf>,
	pub key: Option<PathBuf>,
	pub tick_interval: Duration,
	pub engine: Option<EngineOptions>,
}
