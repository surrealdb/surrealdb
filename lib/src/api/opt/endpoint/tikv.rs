use crate::api::engine::local::Db;
use crate::api::engine::local::TiKv;
use crate::api::err::Error;
use crate::api::opt::Endpoint;
use crate::api::opt::IntoEndpoint;
use crate::api::opt::Strict;
use crate::api::Result;
use std::net::SocketAddr;
use url::Url;

impl IntoEndpoint<TiKv> for &str {
	type Client = Db;

	fn into_endpoint(self) -> Result<Endpoint> {
		let url = format!("tikv://{self}");
		Ok(Endpoint {
			endpoint: Url::parse(&url).map_err(|_| Error::InvalidUrl(url))?,
			strict: false,
			#[cfg(any(feature = "native-tls", feature = "rustls"))]
			tls_config: None,
		})
	}
}

impl IntoEndpoint<TiKv> for SocketAddr {
	type Client = Db;

	fn into_endpoint(self) -> Result<Endpoint> {
		let url = format!("tikv://{self}");
		Ok(Endpoint {
			endpoint: Url::parse(&url).map_err(|_| Error::InvalidUrl(url))?,
			strict: false,
			#[cfg(any(feature = "native-tls", feature = "rustls"))]
			tls_config: None,
		})
	}
}

impl IntoEndpoint<TiKv> for String {
	type Client = Db;

	fn into_endpoint(self) -> Result<Endpoint> {
		let url = format!("tikv://{self}");
		Ok(Endpoint {
			endpoint: Url::parse(&url).map_err(|_| Error::InvalidUrl(url))?,
			strict: false,
			#[cfg(any(feature = "native-tls", feature = "rustls"))]
			tls_config: None,
		})
	}
}

impl<T> IntoEndpoint<TiKv> for (T, Strict)
where
	T: IntoEndpoint<TiKv>,
{
	type Client = Db;

	fn into_endpoint(self) -> Result<Endpoint> {
		let mut address = self.0.into_endpoint()?;
		address.strict = true;
		Ok(address)
	}
}
