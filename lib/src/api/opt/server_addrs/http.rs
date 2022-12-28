use crate::api::engines::remote::http::Client;
use crate::api::engines::remote::http::Http;
use crate::api::engines::remote::http::Https;
use crate::api::err::Error;
#[cfg(any(feature = "native-tls", feature = "rustls"))]
use crate::api::opt::Tls;
use crate::api::opt::ToServerAddrs;
use crate::api::Result;
use crate::api::ServerAddrs;
use std::net::SocketAddr;
use url::Url;

impl ToServerAddrs<Http> for &str {
	type Client = Client;

	fn to_server_addrs(self) -> Result<ServerAddrs> {
		let url = format!("http://{self}");
		Ok(ServerAddrs {
			endpoint: Url::parse(&url).map_err(|_| Error::InvalidUrl(url))?,
			strict: false,
			#[cfg(any(feature = "native-tls", feature = "rustls"))]
			tls_config: None,
		})
	}
}

impl ToServerAddrs<Http> for SocketAddr {
	type Client = Client;

	fn to_server_addrs(self) -> Result<ServerAddrs> {
		let url = format!("http://{self}");
		Ok(ServerAddrs {
			endpoint: Url::parse(&url).map_err(|_| Error::InvalidUrl(url))?,
			strict: false,
			#[cfg(any(feature = "native-tls", feature = "rustls"))]
			tls_config: None,
		})
	}
}

impl ToServerAddrs<Http> for String {
	type Client = Client;

	fn to_server_addrs(self) -> Result<ServerAddrs> {
		let url = format!("http://{self}");
		Ok(ServerAddrs {
			endpoint: Url::parse(&url).map_err(|_| Error::InvalidUrl(url))?,
			strict: false,
			#[cfg(any(feature = "native-tls", feature = "rustls"))]
			tls_config: None,
		})
	}
}

impl ToServerAddrs<Https> for &str {
	type Client = Client;

	fn to_server_addrs(self) -> Result<ServerAddrs> {
		let url = format!("https://{self}");
		Ok(ServerAddrs {
			endpoint: Url::parse(&url).map_err(|_| Error::InvalidUrl(url))?,
			strict: false,
			#[cfg(any(feature = "native-tls", feature = "rustls"))]
			tls_config: None,
		})
	}
}

impl ToServerAddrs<Https> for SocketAddr {
	type Client = Client;

	fn to_server_addrs(self) -> Result<ServerAddrs> {
		let url = format!("https://{self}");
		Ok(ServerAddrs {
			endpoint: Url::parse(&url).map_err(|_| Error::InvalidUrl(url))?,
			strict: false,
			#[cfg(any(feature = "native-tls", feature = "rustls"))]
			tls_config: None,
		})
	}
}

impl ToServerAddrs<Https> for String {
	type Client = Client;

	fn to_server_addrs(self) -> Result<ServerAddrs> {
		let url = format!("https://{self}");
		Ok(ServerAddrs {
			endpoint: Url::parse(&url).map_err(|_| Error::InvalidUrl(url))?,
			strict: false,
			#[cfg(any(feature = "native-tls", feature = "rustls"))]
			tls_config: None,
		})
	}
}

#[cfg(feature = "native-tls")]
#[cfg_attr(docsrs, doc(cfg(feature = "native-tls")))]
impl<T> ToServerAddrs<Https> for (T, native_tls::TlsConnector)
where
	T: ToServerAddrs<Https>,
{
	type Client = Client;

	fn to_server_addrs(self) -> Result<ServerAddrs> {
		let (address, config) = self;
		let mut address = address.to_server_addrs()?;
		address.tls_config = Some(Tls::Native(config));
		Ok(address)
	}
}

#[cfg(feature = "rustls")]
#[cfg_attr(docsrs, doc(cfg(feature = "rustls")))]
impl<T> ToServerAddrs<Https> for (T, rustls::ClientConfig)
where
	T: ToServerAddrs<Https>,
{
	type Client = Client;

	fn to_server_addrs(self) -> Result<ServerAddrs> {
		let (address, config) = self;
		let mut address = address.to_server_addrs()?;
		address.tls_config = Some(Tls::Rust(config));
		Ok(address)
	}
}
