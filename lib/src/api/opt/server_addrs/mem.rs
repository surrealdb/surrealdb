use crate::api::engines::local::Db;
use crate::api::engines::local::Mem;
use crate::api::opt::ServerAddrs;
use crate::api::opt::Strict;
use crate::api::opt::ToServerAddrs;
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
