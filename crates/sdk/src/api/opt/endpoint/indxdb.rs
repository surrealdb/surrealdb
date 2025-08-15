use url::Url;

use crate::api::Result;
use crate::api::engine::local::{Db, IndxDb};
use crate::api::opt::endpoint::into_endpoint;
use crate::api::opt::{Config, Endpoint, IntoEndpoint};

macro_rules! endpoints {
	($($name:ty),*) => {
		$(
			impl IntoEndpoint<IndxDb> for $name {}
			impl into_endpoint::Sealed<IndxDb> for $name {
				type Client = Db;

				fn into_endpoint(self) -> Result<Endpoint> {
					let protocol = "indxdb://";
					let url = Url::parse(protocol)
					    .unwrap_or_else(|_| unreachable!("`{protocol}` should be static and valid"));
					let mut endpoint = Endpoint::new(url);
					endpoint.path = super::path_to_string(protocol, self);
					Ok(endpoint)
				}
			}

			impl IntoEndpoint<IndxDb> for ($name, Config) {}
			impl into_endpoint::Sealed<IndxDb> for ($name, Config) {
				type Client = Db;

				fn into_endpoint(self) -> Result<Endpoint> {
					let mut endpoint = into_endpoint::Sealed::<IndxDb>::into_endpoint(self.0)?;
					endpoint.config = self.1;
					Ok(endpoint)
				}
			}
		)*
	};
}

endpoints!(&str, &String, String);
