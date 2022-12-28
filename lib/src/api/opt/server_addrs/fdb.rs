use crate::api::engines::local::Db;
use crate::api::engines::local::FDb;
use crate::api::err::Error;
use crate::api::opt::ServerAddrs;
use crate::api::opt::Strict;
use crate::api::opt::ToServerAddrs;
use crate::api::Result;
use std::path::Path;
use url::Url;

impl ToServerAddrs<FDb> for &str {
	type Client = Db;

	fn to_server_addrs(self) -> Result<ServerAddrs> {
		let url = format!("fdb://{self}");
		Ok(ServerAddrs {
			endpoint: Url::parse(&url).map_err(|_| Error::InvalidUrl(url))?,
			strict: false,
			#[cfg(any(feature = "native-tls", feature = "rustls"))]
			tls_config: None,
		})
	}
}

impl ToServerAddrs<FDb> for &Path {
	type Client = Db;

	fn to_server_addrs(self) -> Result<ServerAddrs> {
		let url = format!("fdb://{}", self.display());
		Ok(ServerAddrs {
			endpoint: Url::parse(&url).map_err(|_| Error::InvalidUrl(url))?,
			strict: false,
			#[cfg(any(feature = "native-tls", feature = "rustls"))]
			tls_config: None,
		})
	}
}

impl<T> ToServerAddrs<FDb> for (T, Strict)
where
	T: AsRef<Path>,
{
	type Client = Db;

	fn to_server_addrs(self) -> Result<ServerAddrs> {
		let url = format!("fdb://{}", self.0.as_ref().display());
		Ok(ServerAddrs {
			endpoint: Url::parse(&url).map_err(|_| Error::InvalidUrl(url))?,
			strict: true,
			#[cfg(any(feature = "native-tls", feature = "rustls"))]
			tls_config: None,
		})
	}
}
