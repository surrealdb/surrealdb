#[cfg(feature = "protocol-http")]
mod http;
#[cfg(feature = "protocol-ws")]
mod ws;

#[cfg(feature = "kv-fdb")]
mod fdb;
#[cfg(feature = "kv-indxdb")]
mod indxdb;
#[cfg(feature = "kv-mem")]
mod mem;
#[cfg(feature = "kv-rocksdb")]
mod rocksdb;
#[cfg(feature = "kv-speedb")]
mod speedb;
#[cfg(feature = "kv-tikv")]
mod tikv;

use crate::api::Connection;
use crate::api::Result;
use url::Url;

use super::Config;

/// A server address used to connect to the server
#[derive(Debug, Clone)]
#[allow(dead_code)] // used by the embedded and remote connections
pub struct Endpoint {
	#[doc(hidden)]
	pub url: Url,
	#[doc(hidden)]
	pub path: String,
	pub(crate) config: Config,
}

/// A trait for converting inputs to a server address object
pub trait IntoEndpoint<Scheme> {
	/// The client implied by this scheme and address combination
	type Client: Connection;
	/// Converts an input into a server address object
	fn into_endpoint(self) -> Result<Endpoint>;
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

#[allow(dead_code)]
pub(crate) fn path_to_string(protocol: &str, path: impl AsRef<std::path::Path>) -> String {
	use path_clean::PathClean;
	use std::path::Path;

	let path = path.as_ref().display().to_string();
	let expanded = replace_tilde(&path);
	let cleaned = Path::new(&expanded).clean();
	format!("{protocol}{}", cleaned.display())
}

// Macro for implementing the identity implementation of IntoEndpoint for Endpoint for various schemes
macro_rules! identity {
	(Client = $client:ty, $($scheme:ty),*) => {
		$(
			impl IntoEndpoint<$scheme> for Endpoint {
				type Client = $client;

				fn into_endpoint(self) -> Result<Endpoint> {
					Ok(self)
				}
			}
		)*
	}
}

pub(crate) use identity;

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
