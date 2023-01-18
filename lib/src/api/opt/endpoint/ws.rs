use crate::api::engine::remote::ws::Client;
use crate::api::engine::remote::ws::Ws;
use crate::api::engine::remote::ws::Wss;
use crate::api::err::Error;
use crate::api::opt::Endpoint;
use crate::api::opt::IntoEndpoint;
#[cfg(any(feature = "native-tls", feature = "rustls"))]
use crate::api::opt::Tls;
use crate::api::Result;
use std::net::SocketAddr;
use url::Url;

impl IntoEndpoint<Ws> for &str {
	type Client = Client;

	fn into_endpoint(self) -> Result<Endpoint> {
		let url = format!("ws://{self}");
		Ok(Endpoint {
			endpoint: Url::parse(&url).map_err(|_| Error::InvalidUrl(url))?,
			strict: false,
			#[cfg(any(feature = "native-tls", feature = "rustls"))]
			tls_config: None,
		})
	}
}

impl IntoEndpoint<Ws> for SocketAddr {
	type Client = Client;

	fn into_endpoint(self) -> Result<Endpoint> {
		let url = format!("ws://{self}");
		Ok(Endpoint {
			endpoint: Url::parse(&url).map_err(|_| Error::InvalidUrl(url))?,
			strict: false,
			#[cfg(any(feature = "native-tls", feature = "rustls"))]
			tls_config: None,
		})
	}
}

impl IntoEndpoint<Ws> for String {
	type Client = Client;

	fn into_endpoint(self) -> Result<Endpoint> {
		let url = format!("ws://{self}");
		Ok(Endpoint {
			endpoint: Url::parse(&url).map_err(|_| Error::InvalidUrl(url))?,
			strict: false,
			#[cfg(any(feature = "native-tls", feature = "rustls"))]
			tls_config: None,
		})
	}
}

impl IntoEndpoint<Wss> for &str {
	type Client = Client;

	fn into_endpoint(self) -> Result<Endpoint> {
		let url = format!("wss://{self}");
		Ok(Endpoint {
			endpoint: Url::parse(&url).map_err(|_| Error::InvalidUrl(url))?,
			strict: false,
			#[cfg(any(feature = "native-tls", feature = "rustls"))]
			tls_config: None,
		})
	}
}

impl IntoEndpoint<Wss> for SocketAddr {
	type Client = Client;

	fn into_endpoint(self) -> Result<Endpoint> {
		let url = format!("wss://{self}");
		Ok(Endpoint {
			endpoint: Url::parse(&url).map_err(|_| Error::InvalidUrl(url))?,
			strict: false,
			#[cfg(any(feature = "native-tls", feature = "rustls"))]
			tls_config: None,
		})
	}
}

impl IntoEndpoint<Wss> for String {
	type Client = Client;

	fn into_endpoint(self) -> Result<Endpoint> {
		let url = format!("wss://{self}");
		Ok(Endpoint {
			endpoint: Url::parse(&url).map_err(|_| Error::InvalidUrl(url))?,
			strict: false,
			#[cfg(any(feature = "native-tls", feature = "rustls"))]
			tls_config: None,
		})
	}
}

#[cfg(feature = "native-tls")]
#[cfg_attr(docsrs, doc(cfg(feature = "native-tls")))]
impl<T> IntoEndpoint<Wss> for (T, native_tls::TlsConnector)
where
	T: IntoEndpoint<Wss>,
{
	type Client = Client;

	fn into_endpoint(self) -> Result<Endpoint> {
		let (address, config) = self;
		let mut address = address.into_endpoint()?;
		address.tls_config = Some(Tls::Native(config));
		Ok(address)
	}
}

#[cfg(feature = "rustls")]
#[cfg_attr(docsrs, doc(cfg(feature = "rustls")))]
impl<T> IntoEndpoint<Wss> for (T, rustls::ClientConfig)
where
	T: IntoEndpoint<Wss>,
{
	type Client = Client;

	fn into_endpoint(self) -> Result<Endpoint> {
		let (address, config) = self;
		let mut address = address.into_endpoint()?;
		address.tls_config = Some(Tls::Rust(config));
		Ok(address)
	}
}
