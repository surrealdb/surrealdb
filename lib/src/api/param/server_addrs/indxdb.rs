use crate::api::embedded::Db;
use crate::api::err::Error;
use crate::api::param::ServerAddrs;
use crate::api::param::Strict;
use crate::api::param::ToServerAddrs;
use crate::api::storage::IndxDb;
use crate::api::Result;
use url::Url;

impl ToServerAddrs<IndxDb> for &str {
	type Client = Db;

	fn to_server_addrs(self) -> Result<ServerAddrs> {
		let url = format!("indxdb://{self}");
		Ok(ServerAddrs {
			endpoint: Url::parse(&url).map_err(|_| Error::InvalidUrl(url))?,
			strict: false,
			#[cfg(any(feature = "native-tls", feature = "rustls"))]
			tls_config: None,
		})
	}
}

impl ToServerAddrs<IndxDb> for (&str, Strict) {
	type Client = Db;

	fn to_server_addrs(self) -> Result<ServerAddrs> {
		let mut address = ToServerAddrs::<IndxDb>::to_server_addrs(self.0)?;
		address.strict = true;
		Ok(address)
	}
}
