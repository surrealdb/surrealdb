use crate::api::Result;
use crate::api::engine::local::Db;
use crate::api::engine::local::Mem;
use crate::api::opt::Endpoint;
use crate::api::opt::IntoEndpoint;
use crate::api::opt::endpoint::into_endpoint;
use crate::opt::Config;
use url::Url;

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
