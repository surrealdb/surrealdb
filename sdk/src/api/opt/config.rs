use crate::opt::capabilities::Capabilities;
#[cfg(storage)]
use std::path::PathBuf;
use std::time::Duration;
use surrealdb_core::{dbs::Capabilities as CoreCapabilities, iam::Level};

/// Configuration for server connection, including: strictness, notifications, query_timeout, transaction_timeout
#[derive(Debug, Clone, Default)]
pub struct Config {
	pub(crate) strict: bool,
	pub(crate) notifications: bool,
	pub(crate) query_timeout: Option<Duration>,
	pub(crate) transaction_timeout: Option<Duration>,
	#[cfg(any(feature = "native-tls", feature = "rustls"))]
	pub(crate) tls_config: Option<super::Tls>,
	// Only used by the local engines
	// `Level::No` in this context means no authentication information was configured
	pub(crate) auth: Level,
	pub(crate) username: String,
	pub(crate) password: String,
	pub(crate) tick_interval: Option<Duration>,
	pub(crate) capabilities: CoreCapabilities,
	#[cfg(storage)]
	pub(crate) temporary_directory: Option<PathBuf>,
}

impl Config {
	/// Create a default config that can be modified to configure a connection
	pub fn new() -> Self {
		Default::default()
	}

	/// Set the strict value of the config to the supplied value
	pub fn set_strict(mut self, strict: bool) -> Self {
		self.strict = strict;
		self
	}

	/// Enables `strict` server mode
	pub fn strict(mut self) -> Self {
		self.strict = true;
		self
	}

	/// Set the notifications value of the config to the supplied value
	#[deprecated(
		since = "1.1.0",
		note = "Moved to `Capabilities::with_live_query_notifications()`"
	)]
	pub fn set_notifications(mut self, notifications: bool) -> Self {
		self.notifications = notifications;
		self
	}

	/// Set the config to use notifications
	#[deprecated(
		since = "1.1.0",
		note = "Moved to `Capabilities::with_live_query_notifications()`"
	)]
	pub fn notifications(mut self) -> Self {
		self.notifications = true;
		self
	}

	/// Set the query timeout of the config
	pub fn query_timeout(mut self, timeout: impl Into<Option<Duration>>) -> Self {
		self.query_timeout = timeout.into();
		self
	}

	/// Set the transaction timeout of the config
	pub fn transaction_timeout(mut self, timeout: impl Into<Option<Duration>>) -> Self {
		self.transaction_timeout = timeout.into();
		self
	}

	/// Set the default user
	pub fn user(mut self, user: crate::opt::auth::Root<'_>) -> Self {
		self.auth = Level::Root;
		user.username.clone_into(&mut self.username);
		user.password.clone_into(&mut self.password);
		self
	}

	/// Use Rustls to configure TLS connections
	#[cfg(feature = "rustls")]
	#[cfg_attr(docsrs, doc(cfg(feature = "rustls")))]
	pub fn rustls(mut self, config: rustls::ClientConfig) -> Self {
		self.tls_config = Some(super::Tls::Rust(config));
		self
	}

	/// Use native TLS to configure TLS connections
	#[cfg(feature = "native-tls")]
	#[cfg_attr(docsrs, doc(cfg(feature = "native-tls")))]
	pub fn native_tls(mut self, config: native_tls::TlsConnector) -> Self {
		self.tls_config = Some(super::Tls::Native(config));
		self
	}

	/// Set the interval at which the database should run node maintenance tasks
	pub fn tick_interval(mut self, interval: impl Into<Option<Duration>>) -> Self {
		self.tick_interval = interval.into().filter(|x| !x.is_zero());
		self
	}

	/// Set the capabilities for the database
	pub fn capabilities(mut self, capabilities: Capabilities) -> Self {
		self.capabilities = capabilities.build();
		self
	}

	#[cfg(storage)]
	pub fn temporary_directory(mut self, path: Option<PathBuf>) -> Self {
		self.temporary_directory = path;
		self
	}
}
