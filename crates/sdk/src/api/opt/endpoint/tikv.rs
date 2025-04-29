use std::net::SocketAddr;

use url::Url;

use crate::api::engine::local::{Db, TiKv};
use crate::api::err::Error;
use crate::api::opt::{Config, Endpoint, IntoEndpoint};
use crate::api::Result;

macro_rules! endpoints {
	($($name:ty),*) => {
		$(
			impl IntoEndpoint<TiKv> for $name {
				type Client = Db;

				fn into_endpoint(self) -> Result<Endpoint> {
					let url = format!("tikv://{self}");
					let mut endpoint = Endpoint::new(Url::parse(&url).map_err(|_| Error::InvalidUrl(url.clone()))?);
					endpoint.path = url;
					Ok(endpoint)
				}
			}

			impl IntoEndpoint<TiKv> for ($name, Config) {
				type Client = Db;

				fn into_endpoint(self) -> Result<Endpoint> {
					let mut endpoint = IntoEndpoint::<TiKv>::into_endpoint(self.0)?;
					endpoint.config = self.1;
					Ok(endpoint)
				}
			}
		)*
	}
}

endpoints!(&str, &String, String, SocketAddr);
