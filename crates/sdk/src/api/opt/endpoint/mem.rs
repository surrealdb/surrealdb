use url::Url;

use crate::api::Result;
use crate::api::engine::local::{Db, Mem};
use crate::api::opt::endpoint::into_endpoint;
use crate::api::opt::{Endpoint, IntoEndpoint};
use crate::opt::Config;

impl IntoEndpoint<Mem> for () {}
impl into_endpoint::Sealed<Mem> for () {
	type Client = Db;

	fn into_endpoint(self) -> Result<Endpoint> {
		let protocol = "mem://";
		let url = Url::parse(protocol)
			.unwrap_or_else(|_| unreachable!("`{protocol}` should be static and valid"));
		let mut endpoint = Endpoint::new(url);
		"memory".clone_into(&mut endpoint.path);
		Ok(endpoint)
	}
}

impl IntoEndpoint<Mem> for Config {}
impl into_endpoint::Sealed<Mem> for Config {
	type Client = Db;

	fn into_endpoint(self) -> Result<Endpoint> {
		let mut endpoint = into_endpoint::Sealed::<Mem>::into_endpoint(())?;
		endpoint.config = self;
		Ok(endpoint)
	}
}
