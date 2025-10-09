use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::Result;
use surrealdb_core::CommunityComposer;

use crate::core::options::EngineOptions;
use crate::net::client_ip::ClientIp;

pub trait ConfigCheck {
	fn check_config(&mut self, _cfg: &Config) -> Result<()>;
}

impl ConfigCheck for CommunityComposer {
	fn check_config(&mut self, _cfg: &Config) -> Result<()> {
		Ok(())
	}
}

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
