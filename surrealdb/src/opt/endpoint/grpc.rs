use std::net::SocketAddr;

use surrealdb_types::ValidationError;
use url::Url;

use crate::engine::remote::grpc::{Client, Grpc, Grpcs};
use crate::opt::endpoint::into_endpoint;
use crate::opt::{Config, IntoEndpoint};
use crate::{Endpoint, Error, Result};

macro_rules! endpoints {
	($($name:ty),*) => {
		$(
			impl IntoEndpoint<Grpc> for $name {}
			impl into_endpoint::Sealed<Grpc> for $name {
				type Client = Client;

				fn into_endpoint(self) -> Result<Endpoint> {
					let url = format!("grpc://{self}");
					Ok(Endpoint::new(Url::parse(&url).map_err(|_| Error::validation(format!("Invalid URL: {url}"), ValidationError::InvalidRequest))?))
				}
			}

			impl IntoEndpoint<Grpc> for ($name, Config) {}
			impl into_endpoint::Sealed<Grpc> for ($name, Config) {
				type Client = Client;

				fn into_endpoint(self) -> Result<Endpoint> {
					let mut endpoint = into_endpoint::Sealed::<Grpc>::into_endpoint(self.0)?;
					endpoint.config = self.1;
					Ok(endpoint)
				}
			}

			impl IntoEndpoint<Grpcs> for $name {}
			impl into_endpoint::Sealed<Grpcs> for $name {
				type Client = Client;

				fn into_endpoint(self) -> Result<Endpoint> {
					let url = format!("grpcs://{self}");
					Ok(Endpoint::new(Url::parse(&url).map_err(|_| Error::validation(format!("Invalid URL: {url}"), ValidationError::InvalidRequest))?))
				}
			}

			impl IntoEndpoint<Grpcs> for ($name, Config) {}
			impl into_endpoint::Sealed<Grpcs> for ($name, Config) {
				type Client = Client;

				fn into_endpoint(self) -> Result<Endpoint> {
					let mut endpoint = into_endpoint::Sealed::<Grpcs>::into_endpoint(self.0)?;
					endpoint.config = self.1;
					Ok(endpoint)
				}
			}
		)*
	}
}

endpoints!(&str, &String, String, SocketAddr);
