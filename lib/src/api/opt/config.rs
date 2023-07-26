use std::time::Duration;

/// Configuration for server connection, including: strictness, notifications, query_timeout, transaction_timeout
#[cfg(any(
	feature = "kv-mem",
	feature = "kv-tikv",
	feature = "kv-rocksdb",
	feature = "kv-speedb",
	feature = "kv-fdb",
	feature = "kv-indxdb",
))]
#[derive(Debug)]
pub struct Config {
	pub(crate) strict: bool,
	pub(crate) notifications: bool,
	pub(crate) query_timeout: Option<Duration>,
	pub(crate) transaction_timeout: Option<Duration>,
}

impl Config {
	pub fn new() -> Self {
		Default::default()
	}

	pub fn set_strict(mut self, strict: bool) -> Self {
		self.strict = strict;
		self
	}

	pub fn strict(mut self) -> Self {
		self.strict = true;
		self
	}

	pub fn set_notifications(mut self, notifications: bool) -> Self {
		self.notifications = notifications;
		self
	}

	pub fn notifications(mut self) -> Self {
		self.notifications = true;
		self
	}

	pub fn query_timeout(mut self, timeout: impl Into<Option<Duration>>) -> Self {
		self.query_timeout = timeout.into();
		self
	}

	pub fn transaction_timeout(mut self, timeout: impl Into<Option<Duration>>) -> Self {
		self.transaction_timeout = timeout.into();
		self
	}
}

impl Default for Config {
	fn default() -> Self {
		Self {
			strict: false,
			notifications: false,
			query_timeout: None,
			transaction_timeout: None,
		}
	}
}
