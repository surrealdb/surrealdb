use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::Result;
use surrealdb_core::CommunityComposer;

use crate::core::options::EngineOptions;
use crate::net::client_ip::ClientIp;

/// Trait for validating configuration before system initialization.
///
/// This trait is part of the composer pattern and allows composers to perform
/// validation checks on the configuration before the datastore and network
/// components are initialized. Implementations can verify that the configuration
/// is valid for the specific backend and features being used.
pub trait ConfigCheck {
	/// Validates the provided configuration.
	///
	/// # Parameters
	/// - `cfg`: The configuration to validate
	///
	/// # Returns
	/// - `Ok(())` if the configuration is valid
	/// - `Err` if the configuration is invalid or incompatible
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
