#[cfg(feature = "protocol-http")]
mod http;
#[cfg(feature = "protocol-ws")]
mod ws;

#[cfg(feature = "kv-indxdb")]
mod indxdb;
#[cfg(feature = "kv-mem")]
mod mem;
#[cfg(feature = "kv-rocksdb")]
mod rocksdb;
#[cfg(feature = "kv-surrealkv")]
mod surrealkv;
#[cfg(feature = "kv-tikv")]
mod tikv;

#[cfg(any(feature = "kv-mem", feature = "kv-surrealkv", feature = "kv-rocksdb"))]
mod local;

use url::Url;

use super::Config;
use crate::Error;
use crate::{Connection, Result};

/// A server address used to connect to the server
#[derive(Debug, Clone)]
pub struct Endpoint {
	#[doc(hidden)]
	pub url: Url,
	#[doc(hidden)]
	pub path: String,
	pub(crate) config: Config,
}

impl Endpoint {
	pub(crate) fn new(url: Url) -> Self {
		Self {
			url,
			path: String::new(),
			config: Default::default(),
		}
	}

	#[doc(hidden)]
	pub fn parse_kind(&self) -> Result<EndpointKind> {
		match EndpointKind::from(self.url.scheme()) {
			EndpointKind::Unsupported(s) => Err(Error::configuration(
				format!("Unsupported scheme: {s}"),
				None,
			)),
			kind => Ok(kind),
		}
	}

	/// Append a query parameter to the endpoint path string.
	/// Only used when a local engine (e.g. `kv-mem`, `kv-rocksdb`) is enabled.
	#[cfg_attr(
		not(any(feature = "kv-mem", feature = "kv-surrealkv", feature = "kv-rocksdb")),
		allow(dead_code)
	)]
	pub(crate) fn append_query_param(&mut self, key: &str, value: &str) {
		if self.path.contains('?') {
			self.path = format!("{}&{key}={value}", self.path);
		} else {
			self.path = format!("{}?{key}={value}", self.path);
		}
	}
}

/// A trait for converting inputs to a server address object
pub trait IntoEndpoint<Scheme>: into_endpoint::Sealed<Scheme> {}

pub(crate) mod into_endpoint {
	pub trait Sealed<Scheme> {
		/// The client implied by this scheme and address combination
		type Client: super::Connection;
		/// Converts an input into a server address object
		fn into_endpoint(self) -> super::Result<super::Endpoint>;
	}
}

fn replace_tilde(path: &str) -> String {
	if path.starts_with("~/") {
		let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_owned());
		path.replacen("~/", &format!("{home}/"), 1)
	} else if path.starts_with("~\\") {
		let home = std::env::var("HOMEPATH").unwrap_or_else(|_| ".".to_owned());
		path.replacen("~\\", &format!("{home}\\"), 1)
	} else {
		path.to_owned()
	}
}

pub(crate) fn path_to_string(protocol: &str, path: impl AsRef<std::path::Path>) -> String {
	use std::path::Path;

	use path_clean::PathClean;

	let path = path.as_ref().display().to_string();
	// Split query parameters from the path before cleaning to avoid
	// path normalization corrupting query strings (e.g. `?` is valid
	// in Unix filenames but has special meaning in URLs).
	let (path_part, query_part) = match path.split_once('?') {
		Some((p, q)) => (p.to_string(), Some(q.to_string())),
		None => (path, None),
	};
	let expanded = replace_tilde(&path_part);
	let cleaned = Path::new(&expanded).clean();
	match query_part {
		Some(q) => format!("{protocol}{}?{q}", cleaned.display()),
		None => format!("{protocol}{}", cleaned.display()),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_path_to_string() {
		let paths = [
			// Unix-like paths
			"path/to/db",
			"/path/to/db",
			// Windows paths
			"path\\to\\db",
			"\\path\\to\\db",
			"c:path\\to\\db",
			"c:\\path\\to\\db",
		];

		let scheme = "scheme://";

		for path in paths {
			let expanded = replace_tilde(path);
			assert_eq!(expanded, path, "failed to replace `{path}`");

			let converted = path_to_string(scheme, path);
			assert_eq!(converted, format!("{scheme}{path}"), "failed to convert `{path}`");
		}
	}
}

#[derive(Debug)]
#[doc(hidden)]
pub enum EndpointKind {
	Http,
	Https,
	Ws,
	Wss,
	#[cfg(target_family = "wasm")]
	IndxDb,
	Memory,
	RocksDb,
	TiKv,
	Unsupported(String),
	SurrealKv,
}

impl From<&str> for EndpointKind {
	fn from(s: &str) -> Self {
		match s {
			"http" => Self::Http,
			"https" => Self::Https,
			"ws" => Self::Ws,
			"wss" => Self::Wss,
			#[cfg(target_family = "wasm")]
			"indxdb" => Self::IndxDb,
			"mem" => Self::Memory,
			"rocksdb" => Self::RocksDb,
			"tikv" => Self::TiKv,
			"surrealkv" => Self::SurrealKv,
			_ => Self::Unsupported(s.to_owned()),
		}
	}
}

#[doc(hidden)]
impl EndpointKind {
	pub fn is_remote(&self) -> bool {
		matches!(
			self,
			EndpointKind::Http | EndpointKind::Https | EndpointKind::Ws | EndpointKind::Wss
		)
	}

	pub fn is_local(&self) -> bool {
		!self.is_remote()
	}
}
