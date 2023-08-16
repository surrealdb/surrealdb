use std::time::Duration;

/// Configuration for server connection, including: strictness, notifications, query_timeout, transaction_timeout
#[derive(Debug, Default)]
pub struct Config {
	pub(crate) strict: bool,
	pub(crate) notifications: bool,
	pub(crate) query_timeout: Option<Duration>,
	pub(crate) transaction_timeout: Option<Duration>,
}

impl Config {
	///Create a default config that can be modified to configure a connection
	pub fn new() -> Self {
		Default::default()
	}

	///Set the strict value of the config to the supplied value
	pub fn set_strict(mut self, strict: bool) -> Self {
		self.strict = strict;
		self
	}

	///Set the config to use strict mode
	pub fn strict(mut self) -> Self {
		self.strict = true;
		self
	}

	///Set the notifications value of the config to the supplied value
	pub fn set_notifications(mut self, notifications: bool) -> Self {
		self.notifications = notifications;
		self
	}

	///Set the config to use notifications
	pub fn notifications(mut self) -> Self {
		self.notifications = true;
		self
	}

	///Set the query timeout of the config
	pub fn query_timeout(mut self, timeout: impl Into<Option<Duration>>) -> Self {
		self.query_timeout = timeout.into();
		self
	}

	///Set the transaction timeout of the config
	pub fn transaction_timeout(mut self, timeout: impl Into<Option<Duration>>) -> Self {
		self.transaction_timeout = timeout.into();
		self
	}
}
