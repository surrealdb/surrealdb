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
use crate::{Connection, Error, Result};

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
			EndpointKind::Unsupported(s) => {
				Err(Error::configuration(format!("Unsupported scheme: {s}"), None))
			}
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
	use percent_encoding::percent_decode_str;

	// Percent-decode the path so that URL-encoded characters (e.g. %20 for spaces)
	// are converted to their literal equivalents before being used as filesystem paths.
	let path =
		percent_decode_str(&path.as_ref().display().to_string()).decode_utf8_lossy().into_owned();
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

	#[test]
	fn test_path_to_string_percent_decoding() {
		let scheme = "surrealkv://";

		// %20 should be decoded to spaces
		let result = path_to_string(scheme, "/tmp/surrealdb%20path%20test/db");
		assert_eq!(result, "surrealkv:///tmp/surrealdb path test/db");

		// Multiple encoded characters
		let result = path_to_string(scheme, "/path%20with%20multiple%20spaces/db");
		assert_eq!(result, "surrealkv:///path with multiple spaces/db");

		// Path without encoding should be unchanged
		let result = path_to_string(scheme, "/tmp/normal-path/db");
		assert_eq!(result, "surrealkv:///tmp/normal-path/db");

		// Other percent-encoded characters (e.g. %23 = #)
		let result = path_to_string(scheme, "/tmp/path%23with%23hashes/db");
		assert_eq!(result, "surrealkv:///tmp/path#with#hashes/db");
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
