use crate::api::engine::local::Db;
use crate::api::engine::local::Postgres;
use crate::api::err::Error;
use crate::api::opt::Config;
use crate::api::opt::Endpoint;
use crate::api::opt::IntoEndpoint;
use crate::api::Result;
use std::net::SocketAddr;
use url::Url;

macro_rules! endpoints {
	($($name:ty),*) => {
		$(
			impl IntoEndpoint<Postgres> for $name {
				type Client = Db;

				fn into_endpoint(self) -> Result<Endpoint> {
					let url = format!("postgres://{self}");
					let mut endpoint = Endpoint::new(Url::parse(&url).map_err(|_| Error::InvalidUrl(url.clone()))?);
					endpoint.path = url;
					Ok(endpoint)
				}
			}

			impl IntoEndpoint<Postgres> for ($name, Config) {
				type Client = Db;

				fn into_endpoint(self) -> Result<Endpoint> {
					let mut endpoint = IntoEndpoint::<Postgres>::into_endpoint(self.0)?;
					endpoint.config = self.1;
					Ok(endpoint)
				}
			}
		)*
	}
}

endpoints!(&str, &String, String, SocketAddr);
