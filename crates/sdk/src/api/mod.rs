//! Functionality for connecting to local and remote databases

use std::fmt;
use std::fmt::Debug;
use std::future::IntoFuture;
use std::marker::PhantomData;
use std::sync::{Arc, OnceLock};

use anyhow::ensure;
use method::BoxFuture;
use semver::{BuildMetadata, Version, VersionReq};
use tokio::sync::watch;

use crate::Result;

macro_rules! transparent_wrapper{
	(
		$(#[$m:meta])*
		$vis:vis struct $name:ident($field_vis:vis $inner:ty)
	) => {
		$(#[$m])*
		#[repr(transparent)]
		$vis struct $name($field_vis $inner);

		#[allow(dead_code)]
		impl $name{
			#[doc(hidden)]
			pub fn from_inner(inner: $inner) -> Self{
				$name(inner)
			}

			#[doc(hidden)]
			pub fn from_inner_ref(inner: &$inner) -> &Self{
				unsafe{
					std::mem::transmute::<&$inner,&$name>(inner)
				}
			}

			#[doc(hidden)]
			pub fn from_inner_mut(inner: &mut $inner) -> &mut Self{
				unsafe{
					std::mem::transmute::<&mut $inner,&mut $name>(inner)
				}
			}

			#[doc(hidden)]
			pub fn into_inner(self) -> $inner{
				self.0
			}

			#[doc(hidden)]
			pub fn into_inner_ref(&self) -> &$inner{
				&self.0
			}

			#[doc(hidden)]
			pub fn into_inner_mut(&mut self) -> &mut $inner{
				&mut self.0
			}
		}

		impl std::fmt::Display for $name{
			fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result{
				self.0.fmt(fmt)
			}
		}
		impl std::fmt::Debug for $name{
			fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result{
				self.0.fmt(fmt)
			}
		}
	};
}

macro_rules! impl_serialize_wrapper {
	($ty:ty) => {
		impl ::revision::Revisioned for $ty {
			fn revision() -> u16 {
				crate::core::val::Value::revision()
			}

			fn serialize_revisioned<W: std::io::Write>(
				&self,
				w: &mut W,
			) -> std::result::Result<(), revision::Error> {
				self.0.serialize_revisioned(w)
			}

			fn deserialize_revisioned<R: std::io::Read>(
				r: &mut R,
			) -> std::result::Result<Self, revision::Error>
			where
				Self: Sized,
			{
				::revision::Revisioned::deserialize_revisioned(r).map(Self::from_inner)
			}
		}

		impl ::serde::Serialize for $ty {
			fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
			where
				S: ::serde::ser::Serializer,
			{
				self.0.serialize(serializer)
			}
		}

		impl<'de> ::serde::de::Deserialize<'de> for $ty {
			fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
			where
				D: ::serde::de::Deserializer<'de>,
			{
				Ok(Self::from_inner(::serde::de::Deserialize::deserialize(deserializer)?))
			}
		}
	};
}

pub mod engine;
pub mod err;
#[cfg(feature = "protocol-http")]
pub mod headers;
pub mod method;
pub mod opt;
pub mod value;

mod conn;

pub use method::query::Response;

use self::conn::Router;
use self::err::Error;
use self::opt::{Endpoint, EndpointKind, WaitFor};

// Channel for waiters
type Waiter = (watch::Sender<Option<WaitFor>>, watch::Receiver<Option<WaitFor>>);

const SUPPORTED_VERSIONS: (&str, &str) = (">=1.2.0, <4.0.0", "20230701.55918b7c");

/// Connection trait implemented by supported engines
pub trait Connection: conn::Sealed {}

/// The future returned when creating a new SurrealDB instance
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Connect<C: Connection, Response> {
	surreal: Surreal<C>,
	address: Result<Endpoint>,
	capacity: usize,
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
	type IntoFuture = BoxFuture<'static, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let endpoint = self.address?;
			let endpoint_kind = EndpointKind::from(endpoint.url.scheme());
			let client = Client::connect(endpoint, self.capacity).await?;
			if endpoint_kind.is_remote() {
				match client.version().await {
					Ok(mut version) => {
						// we would like to be able to connect to pre-releases too
						version.pre = Default::default();
						client.check_server_version(&version).await?;
					}
					// TODO(raphaeldarley) don't error if Method Not allowed
					Err(e) => return Err(e),
				}
			}
			// Both ends of the channel are still alive at this point
			client.inner.waiter.0.send(Some(WaitFor::Connection)).ok();
			Ok(client)
		})
	}
}

impl<Client> IntoFuture for Connect<Client, ()>
where
	Client: Connection,
{
	type Output = Result<()>;
	type IntoFuture = BoxFuture<'static, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			// Avoid establishing another connection if already connected
			ensure!(self.surreal.inner.router.get().is_none(), Error::AlreadyConnected);
			let endpoint = self.address?;
			let endpoint_kind = EndpointKind::from(endpoint.url.scheme());
			let client = Client::connect(endpoint, self.capacity).await?;
			if endpoint_kind.is_remote() {
				match client.version().await {
					Ok(mut version) => {
						// we would like to be able to connect to pre-releases too
						version.pre = Default::default();
						client.check_server_version(&version).await?;
					}
					// TODO(raphaeldarley) don't error if Method Not allowed
					Err(e) => return Err(e),
				}
			}
			let inner =
				Arc::into_inner(client.inner).expect("new connection to have no references");
			let router = inner.router.into_inner().expect("router to be set");
			self.surreal.inner.router.set(router).map_err(|_| Error::AlreadyConnected)?;
			// Both ends of the channel are still alive at this point
			self.surreal.inner.waiter.0.send(Some(WaitFor::Connection)).ok();
			Ok(())
		})
	}
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub(crate) enum ExtraFeatures {
	Backup,
	LiveQueries,
}

#[derive(Debug)]
struct Inner {
	router: OnceLock<Router>,
	waiter: Waiter,
}

/// A database client instance for embedded or remote databases.
///
/// See [Running SurrealDB embedded in
/// Rust](crate#running-surrealdb-embedded-in-rust) for tips on how to optimize
/// performance for the client when working with embedded instances.
pub struct Surreal<C: Connection> {
	inner: Arc<Inner>,
	engine: PhantomData<C>,
}

impl<C> From<(OnceLock<Router>, Waiter)> for Surreal<C>
where
	C: Connection,
{
	fn from((router, waiter): (OnceLock<Router>, Waiter)) -> Self {
		Surreal {
			inner: Arc::new(Inner {
				router,
				waiter,
			}),
			engine: PhantomData,
		}
	}
}

impl<C> From<(Router, Waiter)> for Surreal<C>
where
	C: Connection,
{
	fn from((router, waiter): (Router, Waiter)) -> Self {
		Surreal {
			inner: Arc::new(Inner {
				router: OnceLock::with_value(router),
				waiter,
			}),
			engine: PhantomData,
		}
	}
}

impl<C> From<Arc<Inner>> for Surreal<C>
where
	C: Connection,
{
	fn from(inner: Arc<Inner>) -> Self {
		Surreal {
			inner,
			engine: PhantomData,
		}
	}
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
		ensure!(
			req.matches(version),
			Error::VersionMismatch {
				server_version: version.clone(),
				supported_versions: versions.to_owned(),
			}
		);

		ensure!(
			server_build.is_empty() || server_build >= &build_meta,
			Error::BuildMetadataMismatch {
				server_metadata: server_build.clone(),
				supported_metadata: build_meta,
			}
		);
		Ok(())
	}
}

impl<C> Clone for Surreal<C>
where
	C: Connection,
{
	fn clone(&self) -> Self {
		Self {
			inner: self.inner.clone(),
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
			.field("router", &self.inner.router)
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
