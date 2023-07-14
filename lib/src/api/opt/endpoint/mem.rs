use crate::api::engine::local::Db;
use crate::api::engine::local::Mem;
use crate::api::opt::auth::Root;
use crate::api::opt::Endpoint;
use crate::api::opt::IntoEndpoint;
use crate::api::opt::Strict;
use crate::api::Result;
use crate::dbs::Level;
use url::Url;

impl IntoEndpoint<Mem> for () {
	type Client = Db;

	fn into_endpoint(self) -> Result<Endpoint> {
		Ok(Endpoint {
			endpoint: Url::parse("mem://").unwrap(),
			strict: false,
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
		endpoint.strict = true;
		Ok(endpoint)
	}
}

impl IntoEndpoint<Mem> for Root<'_> {
	type Client = Db;

	fn into_endpoint(self) -> Result<Endpoint> {
		let mut endpoint = IntoEndpoint::<Mem>::into_endpoint(())?;
		endpoint.auth = Level::Kv;
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
		endpoint.strict = true;
		Ok(endpoint)
	}
}
