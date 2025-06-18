use crate::api::Endpoint;
use crate::api::Result;
use crate::api::engine::remote::http::Client;
use crate::api::engine::remote::http::Http;
use crate::api::engine::remote::http::Https;
use crate::api::err::Error;
use crate::api::opt::IntoEndpoint;
use crate::api::opt::endpoint::into_endpoint;
use crate::opt::Config;
use std::net::SocketAddr;
use url::Url;

macro_rules! endpoints {
	($($name:ty),*) => {
		$(
			impl IntoEndpoint<Http> for $name {}
			impl into_endpoint::Sealed<Http> for $name {
				type Client = Client;

				fn into_endpoint(self) -> Result<Endpoint> {
					let url = format!("http://{self}");
					Ok(Endpoint::new(Url::parse(&url).map_err(|_| Error::InvalidUrl(url))?))
				}
			}

			impl IntoEndpoint<Http> for ($name, Config) {}
			impl into_endpoint::Sealed<Http> for ($name, Config) {
				type Client = Client;

				fn into_endpoint(self) -> Result<Endpoint> {
					let mut endpoint = into_endpoint::Sealed::<Http>::into_endpoint(self.0)?;
					endpoint.config = self.1;
					Ok(endpoint)
				}
			}

			impl IntoEndpoint<Https> for $name {}
			impl into_endpoint::Sealed<Https> for $name {
				type Client = Client;

				fn into_endpoint(self) -> Result<Endpoint> {
					let url = format!("https://{self}");
					Ok(Endpoint::new(Url::parse(&url).map_err(|_| Error::InvalidUrl(url))?))
				}
			}

			impl IntoEndpoint<Https> for ($name, Config) {}
			impl into_endpoint::Sealed<Https> for ($name, Config) {
				type Client = Client;

				fn into_endpoint(self) -> Result<Endpoint> {
					let mut endpoint = into_endpoint::Sealed::<Https>::into_endpoint(self.0)?;
					endpoint.config = self.1;
					Ok(endpoint)
				}
			}
		)*
	}
}

endpoints!(&str, &String, String, SocketAddr);
