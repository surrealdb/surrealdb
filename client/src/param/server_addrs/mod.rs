#[cfg(feature = "http")]
mod http;
#[cfg(feature = "ws")]
mod ws;

use crate::Connection;
use crate::Result;
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

/// A server address used to connect to the server
#[derive(Debug)]
pub struct ServerAddrs {
	pub(crate) endpoint: Url,
	#[cfg(any(feature = "native-tls", feature = "rustls"))]
	pub(crate) tls_config: Option<Tls>,
}

/// A trait for converting inputs to a server address object
pub trait ToServerAddrs<T> {
	/// The client implied by this address and protocol combination
	type Client: Connection;

	/// Converts an input into a server address object
	fn to_server_addrs(self) -> Result<ServerAddrs>;
}
