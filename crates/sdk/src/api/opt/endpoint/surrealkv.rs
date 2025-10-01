use std::path::{Path, PathBuf};

use url::Url;

use crate::Connect;
use crate::api::Result;
use crate::api::engine::local::{Db, SurrealKv};
use crate::api::err::Error;
use crate::api::opt::endpoint::into_endpoint;
use crate::api::opt::{Config, Endpoint, IntoEndpoint};

const VERSIONED_SCHEME: &str = "surrealkv+versioned";

macro_rules! endpoints {
	($($name:ty),*) => {
		$(
			impl IntoEndpoint<SurrealKv> for $name {}
			impl into_endpoint::Sealed<SurrealKv> for $name {
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

			impl IntoEndpoint<SurrealKv> for ($name, Config) {}
			impl into_endpoint::Sealed<SurrealKv> for ($name, Config) {
				type Client = Db;

				fn into_endpoint(self) -> Result<Endpoint> {
					let mut endpoint = into_endpoint::Sealed::<SurrealKv>::into_endpoint(self.0)?;
					endpoint.config = self.1;
					Ok(endpoint)
				}
			}
		)*
	}
}

endpoints!(&str, &String, String, &Path, PathBuf);

impl<R> Connect<Db, R> {
	/// SurrealKV database with versions enabled
	///
	/// # Examples
	///
	/// Instantiating a SurrealKV-backed instance with versions
	///
	/// ```no_run
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// use surrealdb::Surreal;
	/// use surrealdb::engine::local::SurrealKv;
	///
	/// let db = Surreal::new::<SurrealKv>("path/to/database-folder").versioned().await?;
	/// # Ok(())
	/// # }
	/// ```
	///
	/// Instantiating a SurrealKV-backed strict instance with versions
	///
	/// ```no_run
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// use surrealdb::opt::Config;
	/// use surrealdb::Surreal;
	/// use surrealdb::engine::local::SurrealKv;
	///
	/// let config = Config::default().strict();
	/// let db = Surreal::new::<SurrealKv>(("path/to/database-folder", config)).versioned().await?;
	/// # Ok(())
	/// # }
	/// ```
	pub fn versioned(mut self) -> Self {
		let replace_scheme = |mut endpoint: Endpoint| -> Result<Endpoint> {
			match endpoint.url.scheme() {
				// If the engine is an unversioned SurrealKV, we want to switch it to a versioned
				// one
				"surrealkv" => {
					// Replace the scheme in the URL
					endpoint.url.set_scheme(VERSIONED_SCHEME).unwrap_or_else(|_| {
						unreachable!("`{VERSIONED_SCHEME}` should be static and valid")
					});
					// and in the path
					if let Some((_, rest)) = endpoint.path.split_once(':') {
						endpoint.path = format!("{VERSIONED_SCHEME}:{rest}");
					}
					Ok(endpoint)
				}
				// SurrealKV is already versioned, nothing to do here
				self::VERSIONED_SCHEME => Ok(endpoint),
				// This engine doesn't support versions
				scheme => Err(Error::VersionsNotSupported(scheme.to_owned()).into()),
			}
		};
		self.address = self.address.and_then(replace_scheme);
		self
	}
}
