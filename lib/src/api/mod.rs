//! Functionality for connecting to local and remote databases

pub mod engine;
pub mod err;
#[cfg(feature = "protocol-http")]
pub mod headers;
pub mod method;
pub mod opt;

mod conn;

pub use method::query::Response;

use crate::api::conn::DbResponse;
use crate::api::conn::Router;
use crate::api::err::Error;
use crate::api::opt::Endpoint;
use semver::BuildMetadata;
use semver::VersionReq;
use std::fmt::Debug;
use std::future::Future;
use std::future::IntoFuture;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::OnceLock;

/// A specialized `Result` type
pub type Result<T> = std::result::Result<T, crate::Error>;

const SUPPORTED_VERSIONS: (&str, &str) = (">=1.0.0, <2.0.0", "20230701.55918b7c");

/// Connection trait implemented by supported engines
pub trait Connection: conn::Connection {}

/// The future returned when creating a new SurrealDB instance
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Connect<C: Connection, Response> {
	router: Arc<OnceLock<Router<C>>>,
	address: Result<Endpoint>,
	capacity: usize,
	client: PhantomData<C>,
	response_type: PhantomData<Response>,
}

impl<C, R> Connect<C, R>
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
	/// use surrealdb::engine::remote::ws::Ws;
	/// use surrealdb::Surreal;
	///
	/// let db = Surreal::new::<Ws>("localhost:8000")
	///     .with_capacity(100_000)
	///     .await?;
	/// # Ok(())
	/// # }
	/// ```
	pub const fn with_capacity(mut self, capacity: usize) -> Self {
		self.capacity = capacity;
		self
	}
}

impl<Client> IntoFuture for Connect<Client, Surreal<Client>>
where
	Client: Connection,
{
	type Output = Result<Surreal<Client>>;
	type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync>>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let client = Client::connect(self.address?, self.capacity).await?;
			client.check_server_version().await?;
			Ok(client)
		})
	}
}

impl<Client> IntoFuture for Connect<Client, ()>
where
	Client: Connection,
{
	type Output = Result<()>;
	type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync>>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			// Avoid establishing another connection if already connected
			if self.router.get().is_some() {
				return Err(Error::AlreadyConnected.into());
			}
			let arc = Client::connect(self.address?, self.capacity).await?.router;
			let cell = Arc::into_inner(arc).expect("new connection to have no references");
			let router = cell.into_inner().expect("router to be set");
			self.router.set(router).map_err(|_| Error::AlreadyConnected)?;
			let client = Surreal {
				router: self.router,
			};
			client.check_server_version().await?;
			Ok(())
		})
	}
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub(crate) enum ExtraFeatures {
	Backup,
	LiveQueries,
}

/// A database client instance for embedded or remote databases
#[derive(Debug)]
pub struct Surreal<C: Connection> {
	router: Arc<OnceLock<Router<C>>>,
}

impl<C> Surreal<C>
where
	C: Connection,
{
	async fn check_server_version(&self) -> Result<()> {
		let (versions, build_meta) = SUPPORTED_VERSIONS;
		// invalid version requirements should be caught during development
		let req = VersionReq::parse(versions).expect("valid supported versions");
		let build_meta = BuildMetadata::new(build_meta).expect("valid supported build metadata");
		let mut version = self.version().await?;
		// we would like to be able to connect to pre-releases too
		version.pre = Default::default();
		let server_build = &version.build;
		if !req.matches(&version) {
			return Err(Error::VersionMismatch {
				server_version: version,
				supported_versions: versions.to_owned(),
			}
			.into());
		} else if !server_build.is_empty() && server_build < &build_meta {
			return Err(Error::BuildMetadataMismatch {
				server_metadata: server_build.clone(),
				supported_metadata: build_meta,
			}
			.into());
		}
		Ok(())
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

trait OnceLockExt<C>
where
	C: Connection,
{
	fn with_value(value: Router<C>) -> OnceLock<Router<C>> {
		let cell = OnceLock::new();
		match cell.set(value) {
			Ok(()) => cell,
			Err(_) => unreachable!("don't have exclusive access to `cell`"),
		}
	}

	fn extract(&self) -> Result<&Router<C>>;
}

impl<C> OnceLockExt<C> for OnceLock<Router<C>>
where
	C: Connection,
{
	fn extract(&self) -> Result<&Router<C>> {
		let router = self.get().ok_or(Error::ConnectionUninitialised)?;
		Ok(router)
	}
}
