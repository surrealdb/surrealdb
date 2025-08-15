use std::net::SocketAddr;

use url::Url;

use crate::api::engine::remote::ws::{Client, Ws, Wss};
use crate::api::err::Error;
use crate::api::opt::IntoEndpoint;
use crate::api::opt::endpoint::into_endpoint;
use crate::api::{Endpoint, Result};
use crate::opt::Config;

macro_rules! endpoints {
	($($name:ty),*) => {
		$(
			impl IntoEndpoint<Ws> for $name {}
			impl into_endpoint::Sealed<Ws> for $name {
				type Client = Client;

				fn into_endpoint(self) -> Result<Endpoint> {
					let url = format!("ws://{self}");
					Ok(Endpoint::new(Url::parse(&url).map_err(|_| Error::InvalidUrl(url))?))
				}
			}

			impl IntoEndpoint<Ws> for ($name, Config) {}
			impl into_endpoint::Sealed<Ws> for ($name, Config) {
				type Client = Client;

				fn into_endpoint(self) -> Result<Endpoint> {
					let mut endpoint = into_endpoint::Sealed::<Ws>::into_endpoint(self.0)?;
					endpoint.config = self.1;
					Ok(endpoint)
				}
			}

			impl IntoEndpoint<Wss> for $name {}
			impl into_endpoint::Sealed<Wss> for $name {
				type Client = Client;

				fn into_endpoint(self) -> Result<Endpoint> {
					let url = format!("wss://{self}");
					Ok(Endpoint::new(Url::parse(&url).map_err(|_| Error::InvalidUrl(url))?))
				}
			}

			impl IntoEndpoint<Wss> for ($name, Config) {}
			impl into_endpoint::Sealed<Wss> for ($name, Config) {
				type Client = Client;

				fn into_endpoint(self) -> Result<Endpoint> {
					let mut endpoint = into_endpoint::Sealed::<Wss>::into_endpoint(self.0)?;
					endpoint.config = self.1;
					Ok(endpoint)
				}
			}
		)*
	}
}

endpoints!(&str, &String, String, SocketAddr);
