use crate::api::engine::local::Db;
use crate::api::engine::local::SurrealKv;
use crate::api::opt::Config;
use crate::api::opt::Endpoint;
use crate::api::opt::IntoEndpoint;
use crate::api::Result;
use std::path::Path;
use std::path::PathBuf;
use url::Url;

macro_rules! endpoints {
	($($name:ty),*) => {
		$(
			impl IntoEndpoint<SurrealKv> for $name {
				type Client = Db;

				fn into_endpoint(self) -> Result<Endpoint> {
					let protocol = "surrealkv://";
					let url = Url::parse(protocol)
					    .unwrap_or_else(|_| unreachable!("`{protocol}` should be static and valid"));
					let mut endpoint = Endpoint::new(url);
					endpoint.path = super::path_to_string(protocol, self);
					Ok(endpoint)
				}
			}

			impl IntoEndpoint<SurrealKv> for ($name, Config) {
				type Client = Db;

				fn into_endpoint(self) -> Result<Endpoint> {
					let mut endpoint = IntoEndpoint::<SurrealKv>::into_endpoint(self.0)?;
					endpoint.config = self.1;
					Ok(endpoint)
				}
			}
		)*
	}
}

endpoints!(&str, &String, String, &Path, PathBuf);
