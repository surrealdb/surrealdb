//! Functionality for connecting to local and remote databases

pub mod engines;
pub mod err;
pub mod method;
pub mod opt;

mod conn;

pub use method::query::Response;

use crate::api::conn::DbResponse;
use crate::api::conn::Router;
use crate::api::err::Error;
use crate::api::opt::Endpoint;
use once_cell::sync::OnceCell;
use semver::BuildMetadata;
use semver::VersionReq;
use std::fmt::Debug;
use std::future::Future;
use std::future::IntoFuture;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;

/// A specialized `Result` type
pub type Result<T> = std::result::Result<T, crate::Error>;

const SUPPORTED_VERSIONS: (&str, &str) = (">=1.0.0-beta.8, <2.0.0", "20221030.c12a1cc");
const LOG: &str = "surrealdb::api";

/// Connection trait implemented by supported engines
pub trait Connection: conn::Connection {}

/// The future returned when creating a new SurrealDB instance
#[derive(Debug)]
pub struct Connect<'r, C: Connection, Response> {
	router: Option<&'r OnceCell<Arc<Router<C>>>>,
	address: Result<Endpoint>,
	capacity: usize,
	client: PhantomData<C>,
	response_type: PhantomData<Response>,
}

impl<C, R> Connect<'_, C, R>
where
	C: Connection,
{
	/// Sets the maximum capacity of the connection
	///
	/// This is used to set bounds of the channels used internally
	/// as well set the capacity of the `HashMap` used for routing
	/// responses in case of the WebSocket client.
	///
	/// Setting this capacity to `0` (the default) means that
	/// unbounded channels will be used. If your queries per second
	/// are so high that the client is running out of memory,
	/// it might be helpful to set this to a number that works best
	/// for you.
	///
	/// # Examples
	///
	/// ```no_run
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// use surrealdb::engines::remote::ws::Ws;
	/// use surrealdb::Surreal;
	///
	/// let db = Surreal::new::<Ws>("localhost:8000")
	///     .with_capacity(100_000)
	///     .await?;
	/// # Ok(())
	/// # }
	/// ```
	#[must_use]
	pub const fn with_capacity(mut self, capacity: usize) -> Self {
		self.capacity = capacity;
		self
	}
}

impl<'r, Client> IntoFuture for Connect<'r, Client, Surreal<Client>>
where
	Client: Connection,
{
	type Output = Result<Surreal<Client>>;
	type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'r>>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let client = Client::connect(self.address?, self.capacity).await?;
			client.check_server_version();
			Ok(client)
		})
	}
}

impl<'r, Client> IntoFuture for Connect<'r, Client, ()>
where
	Client: Connection,
{
	type Output = Result<()>;
	type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'r>>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			match self.router {
				Some(router) => {
					let option =
						Client::connect(self.address?, self.capacity).await?.router.into_inner();
					match option {
						Some(client) => {
							let _res = router.set(client);
						}
						None => unreachable!(),
					}
				}
				None => unreachable!(),
			}
			Ok(())
		})
	}
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub(crate) enum ExtraFeatures {
	Auth,
	Backup,
}

/// A database client instance for embedded or remote databases
#[derive(Debug)]
pub struct Surreal<C: Connection> {
	router: OnceCell<Arc<Router<C>>>,
}

impl<C> Surreal<C>
where
	C: Connection,
{
	fn check_server_version(&self) {
		let conn = self.clone();
		tokio::spawn(async move {
			let (versions, build_meta) = SUPPORTED_VERSIONS;
			// invalid version requirements should be caught during development
			let req = VersionReq::parse(versions).expect("valid supported versions");
			let build_meta =
				BuildMetadata::new(build_meta).expect("valid supported build metadata");
			match conn.version().await {
				Ok(version) => {
					let server_build = &version.build;
					if !req.matches(&version) {
						warn!(target: LOG, "server version `{version}` does not match the range supported by the client `{versions}`");
					} else if !server_build.is_empty() && server_build < &build_meta {
						warn!(target: LOG, "server build `{server_build}` is older than the minimum supported build `{build_meta}`");
					}
				}
				Err(error) => {
					trace!(target: LOG, "failed to lookup the server version; {error:?}");
				}
			}
		});
	}
}

impl<C> Clone for Surreal<C>
where
	C: Connection,
{
	fn clone(&self) -> Self {
		Self {
			router: self.router.clone(),
		}
	}
}

trait ExtractRouter<C>
where
	C: Connection,
{
	fn extract(&self) -> Result<&Router<C>>;
}

impl<C> ExtractRouter<C> for OnceCell<Arc<Router<C>>>
where
	C: Connection,
{
	fn extract(&self) -> Result<&Router<C>> {
		let router = self.get().ok_or(Error::ConnectionUninitialised)?;
		Ok(router)
	}
}
