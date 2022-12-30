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
#[cfg(feature = "kv-tikv")]
mod tikv;

use crate::api::Connection;
use crate::api::Result;
use url::Url;

/// A server address used to connect to the server
#[derive(Debug)]
#[allow(dead_code)] // used by the embedded and remote connections
pub struct Endpoint {
	pub(crate) endpoint: Url,
	#[allow(dead_code)] // used by the embedded database
	pub(crate) strict: bool,
	#[cfg(any(feature = "native-tls", feature = "rustls"))]
	pub(crate) tls_config: Option<super::Tls>,
}

/// A trait for converting inputs to a server address object
pub trait IntoEndpoint<Scheme> {
	/// The client implied by this scheme and address combination
	type Client: Connection;
	/// Converts an input into a server address object
	fn into_endpoint(self) -> Result<Endpoint>;
}
