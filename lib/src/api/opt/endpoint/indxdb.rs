use crate::api::engine::local::Db;
use crate::api::engine::local::IndxDb;
use crate::api::err::Error;
use crate::api::opt::auth::Root;
use crate::api::opt::Endpoint;
use crate::api::opt::IntoEndpoint;
use crate::api::opt::Strict;
use crate::api::Result;
use crate::dbs::Level;
use url::Url;

impl IntoEndpoint<IndxDb> for &str {
	type Client = Db;

	fn into_endpoint(self) -> Result<Endpoint> {
		let url = format!("indxdb://{self}");
		Ok(Endpoint {
			endpoint: Url::parse(&url).map_err(|_| Error::InvalidUrl(url))?,
			strict: false,
			#[cfg(any(feature = "native-tls", feature = "rustls"))]
			tls_config: None,
			auth: Level::No,
			username: String::new(),
			password: String::new(),
		})
	}
}

impl IntoEndpoint<IndxDb> for (&str, Strict) {
	type Client = Db;

	fn into_endpoint(self) -> Result<Endpoint> {
		let mut endpoint = IntoEndpoint::<IndxDb>::into_endpoint(self.0)?;
		endpoint.strict = true;
		Ok(endpoint)
	}
}

impl IntoEndpoint<IndxDb> for (&str, Root<'_>) {
	type Client = Db;

	fn into_endpoint(self) -> Result<Endpoint> {
		let (name, root) = self;
		let mut endpoint = IntoEndpoint::<IndxDb>::into_endpoint(name)?;
		endpoint.auth = Level::Kv;
		endpoint.username = root.username.to_owned();
		endpoint.password = root.password.to_owned();
		Ok(endpoint)
	}
}

impl IntoEndpoint<IndxDb> for (&str, Strict, Root<'_>) {
	type Client = Db;

	fn into_endpoint(self) -> Result<Endpoint> {
		let (name, _, root) = self;
		let mut endpoint = IntoEndpoint::<IndxDb>::into_endpoint((name, root))?;
		endpoint.strict = true;
		Ok(endpoint)
	}
}
