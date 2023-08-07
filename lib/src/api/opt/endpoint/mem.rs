use crate::api::engine::local::Db;
use crate::api::engine::local::Mem;
use crate::api::opt::auth::Root;
use crate::api::opt::Endpoint;
use crate::api::opt::IntoEndpoint;
use crate::api::opt::Strict;
use crate::api::Result;
use crate::iam::Level;
use crate::opt::Config;
use url::Url;

impl IntoEndpoint<Mem> for () {
	type Client = Db;

	fn into_endpoint(self) -> Result<Endpoint> {
		Ok(Endpoint {
			endpoint: Url::parse("mem://").unwrap(),
			config: Default::default(),
			#[cfg(any(feature = "native-tls", feature = "rustls"))]
			tls_config: None,
			auth: Level::No,
			username: String::new(),
			password: String::new(),
		})
	}
}

impl IntoEndpoint<Mem> for Strict {
	type Client = Db;

	fn into_endpoint(self) -> Result<Endpoint> {
		let mut endpoint = IntoEndpoint::<Mem>::into_endpoint(())?;
		endpoint.config.strict = true;
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

impl IntoEndpoint<Mem> for Root<'_> {
	type Client = Db;

	fn into_endpoint(self) -> Result<Endpoint> {
		let mut endpoint = IntoEndpoint::<Mem>::into_endpoint(())?;
		endpoint.auth = Level::Root;
		endpoint.username = self.username.to_owned();
		endpoint.password = self.password.to_owned();
		Ok(endpoint)
	}
}

impl IntoEndpoint<Mem> for (Strict, Root<'_>) {
	type Client = Db;

	fn into_endpoint(self) -> Result<Endpoint> {
		let (_, root) = self;
		let mut endpoint = IntoEndpoint::<Mem>::into_endpoint(root)?;
		endpoint.config.strict = true;
		Ok(endpoint)
	}
}

impl IntoEndpoint<Mem> for (Config, Root<'_>) {
	type Client = Db;

	fn into_endpoint(self) -> Result<Endpoint> {
		let (config, root) = self;
		let mut endpoint = IntoEndpoint::<Mem>::into_endpoint(root)?;
		endpoint.config = config;
		Ok(endpoint)
	}
}
