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

use crate::api::err::Error;
use crate::api::Connection;
use crate::api::Result;
use url::Url;

use super::Config;

/// A server address used to connect to the server
#[derive(Debug)]
#[allow(dead_code)] // used by the embedded and remote connections
pub struct Endpoint {
	pub(crate) endpoint: Url,
	pub(crate) config: Config,
}

impl Endpoint {
	pub fn parse_kind(&self) -> Result<EndpointKind> {
		match EndpointKind::from(self.endpoint.scheme()) {
			EndpointKind::Unsupported(s) => Err(Error::Scheme(s).into()),
			kind => Ok(kind),
		}
	}
}

/// A trait for converting inputs to a server address object
pub trait IntoEndpoint<Scheme> {
	/// The client implied by this scheme and address combination
	type Client: Connection;
	/// Converts an input into a server address object
	fn into_endpoint(self) -> Result<Endpoint>;
}

#[derive(Debug)]
pub enum EndpointKind {
	Http,
	Https,
	Ws,
	Wss,
	FoundationDb,
	#[cfg(target_arch = "wasm32")]
	IndxDb,
	Memory,
	RocksDb,
	File,
	SpeeDb,
	TiKv,
	Unsupported(String),
}

impl From<&str> for EndpointKind {
	fn from(s: &str) -> Self {
		match s {
			"http" => Self::Http,
			"https" => Self::Https,
			"ws" => Self::Ws,
			"wss" => Self::Wss,
			"fdb" => Self::FoundationDb,
			#[cfg(target_arch = "wasm32")]
			"indxdb" => Self::IndxDb,
			"mem" => Self::Memory,
			"file" => Self::File,
			"rocksdb" => Self::RocksDb,
			"speedb" => Self::SpeeDb,
			"tikv" => Self::TiKv,
			_ => Self::Unsupported(s.to_owned()),
		}
	}
}

impl EndpointKind {
	pub fn is_local(&self) -> bool {
		!matches!(
			self,
			EndpointKind::Http | EndpointKind::Https | EndpointKind::Ws | EndpointKind::Wss
		)
	}
}

#[cfg(any(feature = "kv-fdb", feature = "kv-rocksdb", feature = "kv-speedb"))]
fn make_url(scheme: &str, path: impl AsRef<std::path::Path>) -> String {
	format!("{scheme}://{}", path.as_ref().display())
}
