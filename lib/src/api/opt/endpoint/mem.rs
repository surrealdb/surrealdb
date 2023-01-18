use crate::api::engine::local::Db;
use crate::api::engine::local::Mem;
use crate::api::opt::Endpoint;
use crate::api::opt::IntoEndpoint;
use crate::api::opt::Strict;
use crate::api::Result;
use url::Url;

impl IntoEndpoint<Mem> for () {
	type Client = Db;

	fn into_endpoint(self) -> Result<Endpoint> {
		Ok(Endpoint {
			endpoint: Url::parse("mem://").unwrap(),
			strict: false,
			#[cfg(any(feature = "native-tls", feature = "rustls"))]
			tls_config: None,
		})
	}
}

impl IntoEndpoint<Mem> for Strict {
	type Client = Db;

	fn into_endpoint(self) -> Result<Endpoint> {
		let mut address = IntoEndpoint::<Mem>::into_endpoint(())?;
		address.strict = true;
		Ok(address)
	}
}
