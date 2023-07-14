#[cfg(feature = "has-storage")]
use crate::net::client_ip::ClientIp;
#[cfg(feature = "has-storage")]
use once_cell::sync::OnceCell;
use std::{net::SocketAddr, path::PathBuf};

#[cfg(feature = "has-storage")]
pub static CF: OnceCell<Config> = OnceCell::new();

#[derive(Clone, Debug)]
pub struct Config {
	pub bind: SocketAddr,
	pub path: String,
	#[cfg(feature = "has-storage")]
	pub client_ip: ClientIp,
	pub user: String,
	pub pass: Option<String>,
	pub crt: Option<PathBuf>,
	pub key: Option<PathBuf>,
}
