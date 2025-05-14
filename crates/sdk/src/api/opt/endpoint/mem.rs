use crate::api::engine::local::Db;
use crate::api::engine::local::Mem;
use crate::api::opt::Endpoint;
use crate::api::opt::IntoEndpoint;
use crate::api::Result;
use crate::opt::Config;
use url::Url;

impl IntoEndpoint<Mem> for () {
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

impl IntoEndpoint<Mem> for Config {
	type Client = Db;

	fn into_endpoint(self) -> Result<Endpoint> {
		let mut endpoint = IntoEndpoint::<Mem>::into_endpoint(())?;
		endpoint.config = self;
		Ok(endpoint)
	}
}
