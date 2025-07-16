use crate::net::client_ip::ClientIp;
use std::sync::OnceLock;
use std::{net::SocketAddr, path::PathBuf};

pub static CF: OnceLock<Config> = OnceLock::new();

#[derive(Clone, Debug)]
pub struct Config {
	pub bind: SocketAddr,
	pub grpc_address: SocketAddr,
	pub client_ip: ClientIp,
	pub crt: Option<PathBuf>,
	pub key: Option<PathBuf>,
	pub no_identification_headers: bool,
}
