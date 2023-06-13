use crate::api::engine::local::Db;
use crate::api::engine::local::File;
use crate::api::engine::local::RocksDb;
use crate::api::err::Error;
use crate::api::opt::auth::Root;
use crate::api::opt::Endpoint;
use crate::api::opt::IntoEndpoint;
use crate::api::opt::Strict;
use crate::api::Result;
use crate::dbs::Level;
use std::path::Path;
use url::Url;

impl IntoEndpoint<RocksDb> for &str {
	type Client = Db;

	fn into_endpoint(self) -> Result<Endpoint> {
		let url = format!("rocksdb://{self}");
		Ok(Endpoint {
			endpoint: Url::parse(&url).map_err(|_| Error::InvalidUrl(url))?,
			strict: false,
			#[cfg(any(feature = "native-tls", feature = "rustls"))]
			tls_config: None,
			auth: Level::No,
			username: String::new(),
			password: String::new(),
		})
	}
}

impl IntoEndpoint<RocksDb> for &Path {
	type Client = Db;

	fn into_endpoint(self) -> Result<Endpoint> {
		let path = self.display().to_string();
		IntoEndpoint::<RocksDb>::into_endpoint(path.as_str())
	}
}

impl<T> IntoEndpoint<RocksDb> for (T, Strict)
where
	T: AsRef<Path>,
{
	type Client = Db;

	fn into_endpoint(self) -> Result<Endpoint> {
		let (path, _) = self;
		let mut endpoint = IntoEndpoint::<RocksDb>::into_endpoint(path.as_ref())?;
		endpoint.strict = true;
		Ok(endpoint)
	}
}

impl<T> IntoEndpoint<RocksDb> for (T, Root<'_>)
where
	T: AsRef<Path>,
{
	type Client = Db;

	fn into_endpoint(self) -> Result<Endpoint> {
		let (path, root) = self;
		let mut endpoint = IntoEndpoint::<RocksDb>::into_endpoint(path.as_ref())?;
		endpoint.auth = Level::Kv;
		endpoint.username = root.username.to_owned();
		endpoint.password = root.password.to_owned();
		Ok(endpoint)
	}
}

impl<T> IntoEndpoint<RocksDb> for (T, Strict, Root<'_>)
where
	T: AsRef<Path>,
{
	type Client = Db;

	fn into_endpoint(self) -> Result<Endpoint> {
		let (path, _, root) = self;
		let mut endpoint = IntoEndpoint::<RocksDb>::into_endpoint((path.as_ref(), root))?;
		endpoint.strict = true;
		Ok(endpoint)
	}
}

impl IntoEndpoint<File> for &str {
	type Client = Db;

	fn into_endpoint(self) -> Result<Endpoint> {
		let url = format!("file://{self}");
		Ok(Endpoint {
			endpoint: Url::parse(&url).map_err(|_| Error::InvalidUrl(url))?,
			strict: false,
			#[cfg(any(feature = "native-tls", feature = "rustls"))]
			tls_config: None,
			auth: Level::No,
			username: String::new(),
			password: String::new(),
		})
	}
}

impl IntoEndpoint<File> for &Path {
	type Client = Db;

	fn into_endpoint(self) -> Result<Endpoint> {
		let path = self.display().to_string();
		IntoEndpoint::<File>::into_endpoint(path.as_str())
	}
}

impl<T> IntoEndpoint<File> for (T, Strict)
where
	T: AsRef<Path>,
{
	type Client = Db;

	fn into_endpoint(self) -> Result<Endpoint> {
		let (path, _) = self;
		let mut endpoint = IntoEndpoint::<RocksDb>::into_endpoint(path.as_ref())?;
		endpoint.strict = true;
		Ok(endpoint)
	}
}

impl<T> IntoEndpoint<File> for (T, Root<'_>)
where
	T: AsRef<Path>,
{
	type Client = Db;

	fn into_endpoint(self) -> Result<Endpoint> {
		let (path, root) = self;
		let mut endpoint = IntoEndpoint::<File>::into_endpoint(path.as_ref())?;
		endpoint.auth = Level::Kv;
		endpoint.username = root.username.to_owned();
		endpoint.password = root.password.to_owned();
		Ok(endpoint)
	}
}

impl<T> IntoEndpoint<File> for (T, Strict, Root<'_>)
where
	T: AsRef<Path>,
{
	type Client = Db;

	fn into_endpoint(self) -> Result<Endpoint> {
		let (path, _, root) = self;
		let mut endpoint = IntoEndpoint::<File>::into_endpoint((path.as_ref(), root))?;
		endpoint.strict = true;
		Ok(endpoint)
	}
}
