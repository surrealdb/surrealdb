//! Functionality for connecting to local and remote databases

pub mod engine;
pub mod err;
#[cfg(feature = "protocol-http")]
pub mod headers;
pub mod method;
pub mod opt;

mod conn;

pub use method::query::Response;
use semver::Version;
use tokio::sync::watch;

use crate::api::conn::DbResponse;
use crate::api::conn::Router;
use crate::api::err::Error;
use crate::api::opt::Endpoint;
use semver::BuildMetadata;
use semver::VersionReq;
use std::fmt;
use std::fmt::Debug;
use std::future::Future;
use std::future::IntoFuture;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::OnceLock;

use self::opt::EndpointKind;
use self::opt::WaitFor;

/// A specialized `Result` type
pub type Result<T> = std::result::Result<T, crate::Error>;

// Channel for waiters
type Waiter = (watch::Sender<Option<WaitFor>>, watch::Receiver<Option<WaitFor>>);

const SUPPORTED_VERSIONS: (&str, &str) = (">=1.0.0, <3.0.0", "20230701.55918b7c");
const REVISION_SUPPORTED_SERVER_VERSION: Version = Version::new(1, 2, 0);

/// Connection trait implemented by supported engines
pub trait Connection: conn::Connection {}

/// The future returned when creating a new SurrealDB instance
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Connect<C: Connection, Response> {
	router: Arc<OnceLock<Router>>,
	engine: PhantomData<C>,
	address: Result<Endpoint>,
	capacity: usize,
	client: PhantomData<C>,
	waiter: Arc<Waiter>,
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
			let mut endpoint = self.address?;
			let endpoint_kind = EndpointKind::from(endpoint.url.scheme());
			let mut client = Client::connect(endpoint.clone(), self.capacity).await?;
			if endpoint_kind.is_remote() {
				let mut version = client.version().await?;
				// we would like to be able to connect to pre-releases too
				version.pre = Default::default();
				client.check_server_version(&version).await?;
				if version >= REVISION_SUPPORTED_SERVER_VERSION && endpoint_kind.is_ws() {
					// Switch to revision based serialisation
					endpoint.supports_revision = true;
					client = Client::connect(endpoint, self.capacity).await?;
				}
			}
			// Both ends of the channel are still alive at this point
			client.waiter.0.send(Some(WaitFor::Connection)).ok();
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
			let mut endpoint = self.address?;
			let endpoint_kind = EndpointKind::from(endpoint.url.scheme());
			let mut client = Client::connect(endpoint.clone(), self.capacity).await?;
			if endpoint_kind.is_remote() {
				let mut version = client.version().await?;
				// we would like to be able to connect to pre-releases too
				version.pre = Default::default();
				client.check_server_version(&version).await?;
				if version >= REVISION_SUPPORTED_SERVER_VERSION && endpoint_kind.is_ws() {
					// Switch to revision based serialisation
					endpoint.supports_revision = true;
					client = Client::connect(endpoint, self.capacity).await?;
				}
			}
			let cell =
				Arc::into_inner(client.router).expect("new connection to have no references");
			let router = cell.into_inner().expect("router to be set");
			self.router.set(router).map_err(|_| Error::AlreadyConnected)?;
			// Both ends of the channel are still alive at this point
			self.waiter.0.send(Some(WaitFor::Connection)).ok();
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
pub struct Surreal<C: Connection> {
	router: Arc<OnceLock<Router>>,
	waiter: Arc<Waiter>,
	engine: PhantomData<C>,
}

impl<C> Surreal<C>
where
	C: Connection,
{
	async fn check_server_version(&self, version: &Version) -> Result<()> {
		let (versions, build_meta) = SUPPORTED_VERSIONS;
		// invalid version requirements should be caught during development
		let req = VersionReq::parse(versions).expect("valid supported versions");
		let build_meta = BuildMetadata::new(build_meta).expect("valid supported build metadata");
		let server_build = &version.build;
		if !req.matches(version) {
			return Err(Error::VersionMismatch {
				server_version: version.clone(),
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
			waiter: self.waiter.clone(),
			engine: self.engine,
		}
	}
}

impl<C> Debug for Surreal<C>
where
	C: Connection,
{
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("Surreal")
			.field("router", &self.router)
			.field("engine", &self.engine)
			.finish()
	}
}

trait OnceLockExt {
	fn with_value(value: Router) -> OnceLock<Router> {
		let cell = OnceLock::new();
		match cell.set(value) {
			Ok(()) => cell,
			Err(_) => unreachable!("don't have exclusive access to `cell`"),
		}
	}

	fn extract(&self) -> Result<&Router>;
}

impl OnceLockExt for OnceLock<Router> {
	fn extract(&self) -> Result<&Router> {
		let router = self.get().ok_or(Error::ConnectionUninitialised)?;
		Ok(router)
	}
}
