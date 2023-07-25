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
