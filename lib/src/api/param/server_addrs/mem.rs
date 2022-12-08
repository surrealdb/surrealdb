use crate::api::embedded::Db;
use crate::api::param::ServerAddrs;
use crate::api::param::Strict;
use crate::api::param::ToServerAddrs;
use crate::api::storage::Mem;
use crate::api::Result;
use url::Url;

impl ToServerAddrs<Mem> for () {
	type Client = Db;

	fn to_server_addrs(self) -> Result<ServerAddrs> {
		Ok(ServerAddrs {
			endpoint: Url::parse("mem://").unwrap(),
			strict: false,
			#[cfg(any(feature = "native-tls", feature = "rustls"))]
			tls_config: None,
		})
	}
}

impl ToServerAddrs<Mem> for Strict {
	type Client = Db;

	fn to_server_addrs(self) -> Result<ServerAddrs> {
		let mut address = ToServerAddrs::<Mem>::to_server_addrs(())?;
		address.strict = true;
		Ok(address)
	}
}
