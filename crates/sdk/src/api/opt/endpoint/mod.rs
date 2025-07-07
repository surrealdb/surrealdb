#[cfg(feature = "protocol-ws")]
mod grpc;

#[cfg(kv_fdb)]
mod fdb;
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

use std::str::FromStr;

use crate::api::Result;
use crate::api::err::Error;
use url::Url;

use super::Config;

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
	pub fn parse_kind(&self) -> std::result::Result<EndpointKind, Error> {
		self.url.scheme().parse::<EndpointKind>()
	}
}

impl FromStr for Endpoint {
	type Err = anyhow::Error;

	fn from_str(s: &str) -> anyhow::Result<Self> {
		let url = if s == "memory" {
			Url::parse("memory://memory")?
		} else {
			Url::parse(s)?
		};

		Ok(Self::new(url))
	}
}

impl TryFrom<&str> for Endpoint {
	type Error = anyhow::Error;

	fn try_from(s: &str) -> anyhow::Result<Self> {
		Self::from_str(s)
	}
}

impl TryFrom<String> for Endpoint {
	type Error = anyhow::Error;

	fn try_from(s: String) -> anyhow::Result<Self> {
		Self::from_str(&s)
	}
}

/// A trait for converting inputs to a server address object
pub trait IntoEndpoint<Scheme>: into_endpoint::Sealed<Scheme> {}

pub(crate) mod into_endpoint {
	pub trait Sealed<Scheme> {
		/// The client implied by this scheme and address combination
		type Client: tonic::client::GrpcService<tonic::body::BoxBody> + 'static;
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
	use path_clean::PathClean;
	use std::path::Path;

	let path = path.as_ref().display().to_string();
	let expanded = replace_tilde(&path);
	let cleaned = Path::new(&expanded).clean();
	format!("{protocol}{}", cleaned.display())
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
	Grpc,
	Grpcs,
	FoundationDb,
	#[cfg(target_family = "wasm")]
	IndxDb,
	Memory,
	RocksDb,
	File,
	TiKv,
	SurrealKv,
	SurrealKvVersioned,
}

impl FromStr for EndpointKind {
	type Err = Error;

	fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
		Ok(match s {
			"http" => Self::Http,
			"https" => Self::Https,
			"ws" => Self::Ws,
			"wss" => Self::Wss,
			"grpc" => Self::Grpc,
			"grpcs" => Self::Grpcs,
			"fdb" => Self::FoundationDb,
			#[cfg(target_family = "wasm")]
			"indxdb" => Self::IndxDb,
			"mem" | "memory" => Self::Memory,
			"file" => Self::File,
			"rocksdb" => Self::RocksDb,
			"tikv" => Self::TiKv,
			"surrealkv" => Self::SurrealKv,
			"surrealkv+versioned" => Self::SurrealKvVersioned,
			unexpected => return Err(Error::Scheme(unexpected.to_string())),
		})
	}
}

#[doc(hidden)]
impl EndpointKind {
	pub fn is_remote(&self) -> bool {
		matches!(
			self,
			EndpointKind::Http
				| EndpointKind::Https
				| EndpointKind::Ws
				| EndpointKind::Wss
				| EndpointKind::Grpc
				| EndpointKind::Grpcs
		)
	}

	pub fn is_local(&self) -> bool {
		!self.is_remote()
	}
}
