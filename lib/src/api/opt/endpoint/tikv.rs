use super::identity;
use crate::api::engine::local::Db;
use crate::api::engine::local::TiKv;
use crate::api::opt::Config;
use crate::api::opt::Endpoint;
use crate::api::opt::IntoEndpoint;
use crate::api::Result;
use std::net::SocketAddr;
use url::Url;

macro_rules! endpoints {
	($($name:ty),*) => {
		$(
			impl IntoEndpoint<TiKv> for $name {
				type Client = Db;

				fn into_endpoint(self) -> Result<Endpoint> {
					let url = format!("tikv://{self}");
					Ok(Endpoint {
						url: Url::parse(&url).unwrap(),
						path: url,
						config: Default::default(),
					})
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
identity!(Client = Db, TiKv);
