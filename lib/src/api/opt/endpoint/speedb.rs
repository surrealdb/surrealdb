use crate::api::engine::local::Db;
use crate::api::engine::local::SpeeDb;
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
			impl IntoEndpoint<SpeeDb> for $name {
				type Client = Db;

				fn into_endpoint(self) -> Result<Endpoint> {
					let protocol = "speedb://";
					Ok(Endpoint {
						url: Url::parse(protocol).unwrap(),
						path: super::path_to_string(protocol, self),
						config: Default::default(),
					})
				}
			}

			impl IntoEndpoint<SpeeDb> for ($name, Config) {
				type Client = Db;

				fn into_endpoint(self) -> Result<Endpoint> {
					let mut endpoint = IntoEndpoint::<SpeeDb>::into_endpoint(self.0)?;
					endpoint.config = self.1;
					Ok(endpoint)
				}
			}
		)*
	}
}

endpoints!(&str, &String, String, &Path, PathBuf);
