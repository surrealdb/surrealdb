use std::net::SocketAddr;

use url::Url;

use crate::engine::remote::ws::{Client, Ws, Wss};
use crate::Error;
use crate::opt::endpoint::into_endpoint;
use crate::opt::{Config, IntoEndpoint};
use crate::{Endpoint, Result};

macro_rules! endpoints {
	($($name:ty),*) => {
		$(
			impl IntoEndpoint<Ws> for $name {}
			impl into_endpoint::Sealed<Ws> for $name {
				type Client = Client;

				fn into_endpoint(self) -> Result<Endpoint> {
					let url = format!("ws://{self}");
					Ok(Endpoint::new(Url::parse(&url).map_err(|_| Error::internal(format!("Invalid URL: {url}")))?))
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
					Ok(Endpoint::new(Url::parse(&url).map_err(|_| Error::internal(format!("Invalid URL: {url}")))?))
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
