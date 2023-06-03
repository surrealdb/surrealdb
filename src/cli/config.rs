use crate::net::client_ip::ClientIp;
use once_cell::sync::OnceCell;
use std::{net::SocketAddr, path::PathBuf};

pub static CF: OnceCell<Config> = OnceCell::new();

#[derive(Clone, Debug)]
pub struct Config {
	pub strict: bool,
	pub bind: SocketAddr,
	pub path: String,
	pub client_ip: ClientIp,
	pub user: String,
	pub pass: Option<String>,
	pub crt: Option<PathBuf>,
	pub key: Option<PathBuf>,
}
