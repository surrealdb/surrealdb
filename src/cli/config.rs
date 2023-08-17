#[cfg(feature = "has-storage")]
use crate::net::client_ip::ClientIp;
#[cfg(feature = "has-storage")]
use std::sync::OnceLock;
use std::{net::SocketAddr, path::PathBuf};

#[cfg(feature = "has-storage")]
pub static CF: OnceLock<Config> = OnceLock::new();

use std::time::Duration;

#[derive(Clone, Debug)]
pub struct Config {
	pub bind: SocketAddr,
	pub path: String,
	#[cfg(feature = "has-storage")]
	pub client_ip: ClientIp,
	pub user: Option<String>,
	pub pass: Option<String>,
	pub crt: Option<PathBuf>,
	pub key: Option<PathBuf>,
	pub tick_interval: Duration,
}
