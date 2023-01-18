use crate::api::engine::local::Db;
use crate::api::engine::local::IndxDb;
use crate::api::err::Error;
use crate::api::opt::Endpoint;
use crate::api::opt::IntoEndpoint;
use crate::api::opt::Strict;
use crate::api::Result;
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
		})
	}
}

impl IntoEndpoint<IndxDb> for (&str, Strict) {
	type Client = Db;

	fn into_endpoint(self) -> Result<Endpoint> {
		let mut address = IntoEndpoint::<IndxDb>::into_endpoint(self.0)?;
		address.strict = true;
		Ok(address)
	}
}
