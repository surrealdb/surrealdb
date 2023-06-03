use once_cell::sync::OnceCell;
use surrealdb::kvs::DsOpts;
use std::{net::SocketAddr, path::PathBuf};

pub static CF: OnceCell<Config> = OnceCell::new();

#[derive(Clone, Debug)]
pub struct Config {
	pub ds_opts: DsOpts,
	pub bind: SocketAddr,
	pub path: String,
	pub user: String,
	pub pass: Option<String>,
	pub crt: Option<PathBuf>,
	pub key: Option<PathBuf>,
}
