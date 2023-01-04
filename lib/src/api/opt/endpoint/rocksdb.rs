use crate::api::engines::local::Db;
use crate::api::engines::local::File;
use crate::api::engines::local::RocksDb;
use crate::api::err::Error;
use crate::api::opt::Endpoint;
use crate::api::opt::IntoEndpoint;
use crate::api::opt::Strict;
use crate::api::Result;
use std::path::Path;
use url::Url;

impl IntoEndpoint<RocksDb> for &str {
	type Client = Db;

	fn into_endpoint(self) -> Result<Endpoint> {
		let url = format!("rocksdb://{self}");
		Ok(Endpoint {
			endpoint: Url::parse(&url).map_err(|_| Error::InvalidUrl(url))?,
			strict: false,
			#[cfg(any(feature = "native-tls", feature = "rustls"))]
			tls_config: None,
		})
	}
}

impl IntoEndpoint<RocksDb> for &Path {
	type Client = Db;

	fn into_endpoint(self) -> Result<Endpoint> {
		let url = format!("rocksdb://{}", self.display());
		Ok(Endpoint {
			endpoint: Url::parse(&url).map_err(|_| Error::InvalidUrl(url))?,
			strict: false,
			#[cfg(any(feature = "native-tls", feature = "rustls"))]
			tls_config: None,
		})
	}
}

impl<T> IntoEndpoint<RocksDb> for (T, Strict)
where
	T: AsRef<Path>,
{
	type Client = Db;

	fn into_endpoint(self) -> Result<Endpoint> {
		let url = format!("rocksdb://{}", self.0.as_ref().display());
		Ok(Endpoint {
			endpoint: Url::parse(&url).map_err(|_| Error::InvalidUrl(url))?,
			strict: true,
			#[cfg(any(feature = "native-tls", feature = "rustls"))]
			tls_config: None,
		})
	}
}

impl IntoEndpoint<File> for &str {
	type Client = Db;

	fn into_endpoint(self) -> Result<Endpoint> {
		let url = format!("file://{self}");
		Ok(Endpoint {
			endpoint: Url::parse(&url).map_err(|_| Error::InvalidUrl(url))?,
			strict: false,
			#[cfg(any(feature = "native-tls", feature = "rustls"))]
			tls_config: None,
		})
	}
}

impl IntoEndpoint<File> for &Path {
	type Client = Db;

	fn into_endpoint(self) -> Result<Endpoint> {
		let url = format!("file://{}", self.display());
		Ok(Endpoint {
			endpoint: Url::parse(&url).map_err(|_| Error::InvalidUrl(url))?,
			strict: false,
			#[cfg(any(feature = "native-tls", feature = "rustls"))]
			tls_config: None,
		})
	}
}

impl<T> IntoEndpoint<File> for (T, Strict)
where
	T: AsRef<Path>,
{
	type Client = Db;

	fn into_endpoint(self) -> Result<Endpoint> {
		let url = format!("file://{}", self.0.as_ref().display());
		Ok(Endpoint {
			endpoint: Url::parse(&url).map_err(|_| Error::InvalidUrl(url))?,
			strict: true,
			#[cfg(any(feature = "native-tls", feature = "rustls"))]
			tls_config: None,
		})
	}
}
