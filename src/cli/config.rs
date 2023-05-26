use once_cell::sync::OnceCell;
use std::{net::SocketAddr, path::PathBuf};

pub static CF: OnceCell<Config> = OnceCell::new();

#[derive(Clone, Debug)]
pub struct Config {
	pub strict: bool,
	pub bind: SocketAddr,
	pub path: String,
	pub auth: bool,
	pub crt: Option<PathBuf>,
	pub key: Option<PathBuf>,
}
