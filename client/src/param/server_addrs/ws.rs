use crate::net::WsClient;
use crate::param::ServerAddrs;
#[cfg(any(feature = "native-tls", feature = "rustls"))]
use crate::param::Tls;
use crate::param::ToServerAddrs;
use crate::protocol::Ws;
use crate::protocol::Wss;
use crate::Result;
use std::net::SocketAddr;
use url::Url;

impl ToServerAddrs<Ws> for &str {
	type Client = WsClient;

	fn to_server_addrs(self) -> Result<ServerAddrs> {
		Ok(ServerAddrs {
			endpoint: Url::parse(&format!("ws://{self}"))?,
			#[cfg(any(feature = "native-tls", feature = "rustls"))]
			tls_config: None,
		})
	}
}

impl ToServerAddrs<Ws> for SocketAddr {
	type Client = WsClient;

	fn to_server_addrs(self) -> Result<ServerAddrs> {
		Ok(ServerAddrs {
			endpoint: Url::parse(&format!("ws://{self}"))?,
			#[cfg(any(feature = "native-tls", feature = "rustls"))]
			tls_config: None,
		})
	}
}

impl ToServerAddrs<Ws> for String {
	type Client = WsClient;

	fn to_server_addrs(self) -> Result<ServerAddrs> {
		Ok(ServerAddrs {
			endpoint: Url::parse(&format!("ws://{self}"))?,
			#[cfg(any(feature = "native-tls", feature = "rustls"))]
			tls_config: None,
		})
	}
}

impl ToServerAddrs<Wss> for &str {
	type Client = WsClient;

	fn to_server_addrs(self) -> Result<ServerAddrs> {
		Ok(ServerAddrs {
			endpoint: Url::parse(&format!("wss://{self}"))?,
			#[cfg(any(feature = "native-tls", feature = "rustls"))]
			tls_config: None,
		})
	}
}

impl ToServerAddrs<Wss> for SocketAddr {
	type Client = WsClient;

	fn to_server_addrs(self) -> Result<ServerAddrs> {
		Ok(ServerAddrs {
			endpoint: Url::parse(&format!("wss://{self}"))?,
			#[cfg(any(feature = "native-tls", feature = "rustls"))]
			tls_config: None,
		})
	}
}

impl ToServerAddrs<Wss> for String {
	type Client = WsClient;

	fn to_server_addrs(self) -> Result<ServerAddrs> {
		Ok(ServerAddrs {
			endpoint: Url::parse(&format!("wss://{self}"))?,
			#[cfg(any(feature = "native-tls", feature = "rustls"))]
			tls_config: None,
		})
	}
}

#[cfg(feature = "native-tls")]
#[cfg_attr(docsrs, doc(cfg(feature = "native-tls")))]
impl<T> ToServerAddrs<Wss> for (T, native_tls::TlsConnector)
where
	T: ToServerAddrs<Wss>,
{
	type Client = WsClient;

	fn to_server_addrs(self) -> Result<ServerAddrs> {
		let (address, config) = self;
		let mut address = address.to_server_addrs()?;
		address.tls_config = Some(Tls::Native(config));
		Ok(address)
	}
}

#[cfg(feature = "rustls")]
#[cfg_attr(docsrs, doc(cfg(feature = "rustls")))]
impl<T> ToServerAddrs<Wss> for (T, rustls::ClientConfig)
where
	T: ToServerAddrs<Wss>,
{
	type Client = WsClient;

	fn to_server_addrs(self) -> Result<ServerAddrs> {
		let (address, config) = self;
		let mut address = address.to_server_addrs()?;
		address.tls_config = Some(Tls::Rust(config));
		Ok(address)
	}
}
