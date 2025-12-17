//! This library provides a low-level database library implementation, a remote
//! client and a query language definition, for [SurrealDB](https://surrealdb.com), the ultimate cloud database for
//! tomorrow's applications. SurrealDB is a scalable, distributed,
//! collaborative, document-graph database for the realtime web.
//!
//! This library can be used to start an [embedded](crate::engine::local)
//! in-memory datastore, an embedded datastore persisted to disk, a
//! browser-based embedded datastore backed by IndexedDB, or for connecting to a distributed [TiKV](https://tikv.org) key-value store.
//!
//! It also enables simple and advanced querying of a
//! [remote](crate::engine::remote) SurrealDB server from server-side or
//! client-side code. All connections to SurrealDB are made over WebSockets by
//! default, and automatically reconnect when the connection is terminated.

#![doc(html_favicon_url = "https://surrealdb.s3.amazonaws.com/favicon.png")]
#![doc(html_logo_url = "https://surrealdb.s3.amazonaws.com/icon.png")]
#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(all(target_family = "wasm", feature = "ml"))]
compile_error!("The `ml` feature is not supported on Wasm.");

#[macro_use]
extern crate tracing;

pub mod engine;
#[doc(hidden)]
#[cfg(feature = "protocol-http")]
pub mod headers;
pub mod method;
pub mod opt;

mod conn;
mod err;
mod notification;

#[doc(hidden)]
/// Channels for receiving a SurrealQL database export
pub mod channel {
	pub use async_channel::{Receiver, Sender, bounded, unbounded};
}

/// Different error types for embedded and remote databases
pub mod error {
	pub use crate::err::Error as Api;
}

pub mod parse {
	pub use surrealdb_core::syn::value;
}

pub use method::query::IndexedResults;
#[doc(inline)]
pub use surrealdb_types as types;

#[doc(inline)]
pub use crate::notification::Notification;

/// A specialized `Result` type
pub type Result<T> = std::result::Result<T, err::Error>;
use std::fmt;
use std::fmt::Debug;
use std::future::IntoFuture;
use std::marker::PhantomData;
use std::sync::{Arc, OnceLock};

use async_channel::{Receiver, Sender};
#[doc(inline)]
pub use err::Error;
// Removed anyhow::ensure - will implement custom ensure macro if needed
use method::BoxFuture;
use semver::{BuildMetadata, Version, VersionReq};
use tokio::sync::watch;
use uuid::Uuid;

use self::conn::Router;
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
			let client = Client::connect(endpoint, self.capacity, None).await?;
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
			if self.surreal.inner.router.get().is_some() {
				return Err(Error::AlreadyConnected);
			}
			let endpoint = self.address?;
			let endpoint_kind = EndpointKind::from(endpoint.url.scheme());
			let session_clone = self.surreal.inner.session_clone.clone();
			let client = Client::connect(endpoint, self.capacity, Some(session_clone)).await?;
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
			let router = client.inner.router.wait().clone();
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
#[allow(dead_code)]
enum SessionId {
	Initial(Uuid),
	Clone {
		old: Uuid,
		new: Uuid,
	},
	Drop(Uuid),
}

#[derive(Debug, Clone)]
struct SessionClone {
	sender: Sender<SessionId>,
	#[allow(dead_code)]
	receiver: Receiver<SessionId>,
}

impl SessionClone {
	fn new() -> Self {
		let (sender, receiver) = async_channel::unbounded();
		Self {
			sender,
			receiver,
		}
	}
}

#[derive(Debug)]
struct Inner {
	router: OnceLock<Router>,
	waiter: Waiter,
	session_clone: SessionClone,
}

impl Inner {
	fn clone_session(&self, old: Uuid, new: Uuid) {
		self.session_clone
			.sender
			.try_send(SessionId::Clone {
				old,
				new,
			})
			.ok();
	}
}

/// A database client instance for embedded or remote databases.
///
/// See [Running SurrealDB embedded in
/// Rust](crate#running-surrealdb-embedded-in-rust) for tips on how to optimize
/// performance for the client when working with embedded instances.
pub struct Surreal<C: Connection> {
	inner: Arc<Inner>,
	session_id: Uuid,
	engine: PhantomData<C>,
}

impl<C> From<Arc<Inner>> for Surreal<C>
where
	C: Connection,
{
	fn from(inner: Arc<Inner>) -> Self {
		let session_id = Uuid::new_v4();
		inner.session_clone.sender.try_send(SessionId::Initial(session_id)).ok();
		Surreal {
			inner,
			session_id,
			engine: PhantomData,
		}
	}
}

impl<C> From<(OnceLock<Router>, Waiter, SessionClone)> for Surreal<C>
where
	C: Connection,
{
	fn from((router, waiter, session_clone): (OnceLock<Router>, Waiter, SessionClone)) -> Self {
		Arc::new(Inner {
			router,
			waiter,
			session_clone,
		})
		.into()
	}
}

impl<C> From<(Router, Waiter, SessionClone)> for Surreal<C>
where
	C: Connection,
{
	fn from((router, waiter, session_clone): (Router, Waiter, SessionClone)) -> Self {
		let oncelock = OnceLock::with_value(router);
		(oncelock, waiter, session_clone).into()
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
		if !req.matches(version) {
			return Err(Error::VersionMismatch {
				server_version: version.clone(),
				supported_versions: versions.to_owned(),
			});
		}

		if !server_build.is_empty() && server_build < &build_meta {
			return Err(Error::BuildMetadataMismatch {
				server_metadata: server_build.clone(),
				supported_metadata: build_meta,
			});
		}
		Ok(())
	}
}

impl<C> Clone for Surreal<C>
where
	C: Connection,
{
	fn clone(&self) -> Self {
		let session_id = Uuid::new_v4();
		self.inner.clone_session(self.session_id, session_id);
		Self {
			inner: self.inner.clone(),
			session_id,
			engine: self.engine,
		}
	}
}

impl<C> Drop for Surreal<C>
where
	C: Connection,
{
	fn drop(&mut self) {
		self.inner.session_clone.sender.try_send(SessionId::Drop(self.session_id)).ok();
	}
}

impl<C> Debug for Surreal<C>
where
	C: Connection,
{
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("Surreal")
			.field("router", &self.inner.router)
			.field("session_id", &self.session_id)
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
