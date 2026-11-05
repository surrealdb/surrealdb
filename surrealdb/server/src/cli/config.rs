use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::Result;
use surrealdb_core::CommunityComposer;
use surrealdb_core::options::EngineOptions;

use crate::ntw::client_ip::ClientIp;

/// Trait for validating configuration before system initialization.
///
/// This trait is part of the composer pattern and allows composers to perform
/// validation checks on the configuration before the datastore and network
/// components are initialized. Implementations can verify that the configuration
/// is valid for the specific backend and features being used.
#[async_trait::async_trait]
pub trait ConfigCheck: Send + Sync + 'static {
	/// Validates the provided configuration.
	///
	/// # Parameters
	/// - `cfg`: The configuration to validate
	///
	/// # Returns
	/// - `Ok(())` if the configuration is valid
	/// - `Err` if the configuration is invalid or incompatible
	async fn check_config(&mut self, _cfg: &Config) -> Result<()>;
}

#[async_trait::async_trait]
impl ConfigCheck for CommunityComposer {
	async fn check_config(&mut self, _cfg: &Config) -> Result<()> {
		Ok(())
	}
}

#[derive(Clone, Debug)]
pub struct Config {
	pub bind: SocketAddr,
	pub postgres_bind: Option<SocketAddr>,
	pub mysql_bind: Option<SocketAddr>,
	pub path: String,
	pub client_ip: ClientIp,
	pub user: Option<String>,
	pub pass: Option<String>,
	pub crt: Option<PathBuf>,
	pub key: Option<PathBuf>,
	pub engine: EngineOptions,
	pub no_identification_headers: bool,
	pub allow_origin: Vec<String>,
	/// How long an attached HTTP RPC session's stored copy lives without
	/// being used.
	pub durable_session_ttl: std::time::Duration,
	/// Whether the HTTP server issues and accepts the `HttpOnly` session cookie.
	pub cookie_auth: bool,
}
