//! Knobs of the outbound HTTP clients.
//!
//! These govern SurrealDB acting as an HTTP client: how many redirect hops a
//! request may follow, how long connecting may take, how long an idle
//! connection stays pooled, and which `User-Agent` outgoing requests carry.
//! This layer owns them because it builds the capability-filtered client that
//! every outbound request from a query goes through.
//!
//! The JWKS fetch client in [`iam::jwks`](crate::iam) builds its own
//! `reqwest::Client` — that path is reachable without the `http` feature — and
//! reads the redirect budget and the user agent from here, so both clients
//! present the same limits to a remote host.
//!
//! Every value is read once, while a client is being built, and is baked into
//! it: changing one affects only clients created afterwards.

use surrealdb_cnf as cnf;

/// Limits applied to the outbound HTTP clients.
#[derive(Clone, Debug)]
pub(crate) struct HttpConfig {
	/// The maximum number of HTTP redirects allowed within http functions (default:
	/// 10)
	pub max_http_redirects: usize,
	/// The maximum number of idle HTTP connections to maintain per host (default: 128)
	pub max_http_idle_connections_per_host: usize,
	/// The timeout for idle HTTP connections before closing (default: 90 seconds)
	pub http_idle_timeout_secs: u64,
	/// The timeout for connecting to HTTP endpoints (default: 30 seconds)
	pub http_connect_timeout_secs: u64,
	/// Specify the USER-AGENT string used by HTTP requests
	pub surrealdb_user_agent: String,
}

impl Default for HttpConfig {
	fn default() -> Self {
		Self {
			max_http_redirects: 10,
			max_http_idle_connections_per_host: 128,
			http_idle_timeout_secs: 90,
			http_connect_timeout_secs: 30,
			surrealdb_user_agent: "SurrealDB".to_string(),
		}
	}
}

impl cnf::Config for HttpConfig {
	fn parse(&mut self, map: &cnf::ConfigMap) {
		map.parse_key("max_http_redirects", &mut self.max_http_redirects)
			.parse_key(
				"max_http_idle_connections_per_host",
				&mut self.max_http_idle_connections_per_host,
			)
			.parse_key("http_idle_timeout_secs", &mut self.http_idle_timeout_secs)
			.parse_key("http_connect_timeout_secs", &mut self.http_connect_timeout_secs)
			.parse_key("surrealdb_user_agent", &mut self.surrealdb_user_agent);
	}
}
