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

/// TLS Configuration
#[cfg(any(feature = "native-tls", feature = "rustls"))]
#[cfg_attr(docsrs, doc(cfg(any(feature = "native-tls", feature = "rustls"))))]
#[derive(Debug)]
pub enum Tls {
	/// Native TLS configuration
	#[cfg(feature = "native-tls")]
	#[cfg_attr(docsrs, doc(cfg(feature = "native-tls")))]
	Native(native_tls::TlsConnector),
	/// Rustls configuration
	#[cfg(feature = "rustls")]
	#[cfg_attr(docsrs, doc(cfg(feature = "rustls")))]
	Rust(rustls::ClientConfig),
}

/// Enables `strict` server mode
#[cfg(any(
	feature = "kv-mem",
	feature = "kv-tikv",
	feature = "kv-rocksdb",
	feature = "kv-fdb",
	feature = "kv-indxdb",
))]
#[derive(Debug)]
pub struct Strict;

/// A server address used to connect to the server
#[derive(Debug)]
#[allow(dead_code)] // used by the embedded and remote connections
pub struct ServerAddrs {
	pub(crate) endpoint: Url,
	#[allow(dead_code)] // used by the embedded database
	pub(crate) strict: bool,
	#[cfg(any(feature = "native-tls", feature = "rustls"))]
	pub(crate) tls_config: Option<Tls>,
}

/// A trait for converting inputs to a server address object
pub trait ToServerAddrs<Scheme> {
	/// The client implied by this scheme and address combination
	type Client: Connection;

	/// Converts an input into a server address object
	fn to_server_addrs(self) -> Result<ServerAddrs>;
}
