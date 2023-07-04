#[cfg(any(
	feature = "storage-mem",
	feature = "storage-tikv",
	feature = "storage-rocksdb",
	feature = "storage-speedb",
	feature = "storage-fdb",
))]
use crate::net::client_ip::ClientIp;
#[cfg(any(
	feature = "storage-mem",
	feature = "storage-tikv",
	feature = "storage-rocksdb",
	feature = "storage-speedb",
	feature = "storage-fdb",
))]
use once_cell::sync::OnceCell;
use std::{net::SocketAddr, path::PathBuf};

#[cfg(any(
	feature = "storage-mem",
	feature = "storage-tikv",
	feature = "storage-rocksdb",
	feature = "storage-speedb",
	feature = "storage-fdb",
))]
pub static CF: OnceCell<Config> = OnceCell::new();

#[derive(Clone, Debug)]
pub struct Config {
	pub strict: bool,
	pub bind: SocketAddr,
	pub path: String,
	#[cfg(any(
		feature = "storage-mem",
		feature = "storage-tikv",
		feature = "storage-rocksdb",
		feature = "storage-speedb",
		feature = "storage-fdb",
	))]
	pub client_ip: ClientIp,
	pub user: String,
	pub pass: Option<String>,
	pub crt: Option<PathBuf>,
	pub key: Option<PathBuf>,
}
