#![recursion_limit = "256"]

//! This library provides a low-level database library implementation, a remote
//! client and a query language definition, for [SurrealDB](https://surrealdb.com), the ultimate cloud database for
//! tomorrow's applications. SurrealDB is a scalable, distributed,
//! collaborative, document-graph database for the realtime web.
//!
//! This library can be used to start an [embedded](crate::engine::local)
//! in-memory datastore, an embedded datastore persisted to disk, a
//! browser-based embedded datastore backed by the origin private file system.
//!
//! It also enables simple and advanced querying of a
//! [remote](crate::engine::remote) SurrealDB server from server-side or
//! client-side code. All connections to SurrealDB are made over WebSockets by
//! default, and automatically reconnect when the connection is terminated.

#![doc(html_favicon_url = "https://surrealdb.s3.amazonaws.com/favicon.png")]
#![doc(html_logo_url = "https://surrealdb.s3.amazonaws.com/icon.png")]
#![cfg_attr(docsrs, feature(doc_cfg))]
// TODO: Remove once this crate no longer needs it; see `absolute_paths` in the workspace lints.
#![allow(clippy::absolute_paths)]

#[macro_use]
extern crate tracing;

pub mod engine;
#[doc(hidden)]
#[cfg(http_engine)]
pub mod headers;
pub mod method;
pub mod opt;

mod conn;
mod notification;

#[doc(hidden)]
/// Channels for receiving a SurrealQL database export
pub mod channel {
	pub use async_channel::{Receiver, Sender, bounded, unbounded};
}

#[cfg(feature = "parse")]
#[cfg_attr(docsrs, doc(cfg(feature = "parse")))]
pub mod parse {
	//! Parse SurrealQL text without connecting to a database.

	/// Parses a SurrealQL value from its textual representation.
	pub fn value(input: &str) -> anyhow::Result<crate::types::Value> {
		surrealdb_expr::parser::value(input).map_err(anyhow::Error::new)
	}
}

#[doc(inline)]
pub use method::Stats;
#[doc(inline)]
pub use method::Stream;
#[doc(inline)]
pub use method::query::IndexedResults;
#[doc(inline)]
pub use surrealdb_types as types;

#[doc(inline)]
pub use crate::notification::Notification;

/// A specialized `Result` type
pub type Result<T> = std::result::Result<T, Error>;
use std::fmt;
use std::fmt::Debug;
use std::future::IntoFuture;
use std::marker::PhantomData;
use std::sync::{Arc, OnceLock};

use async_channel::{Receiver, Sender};
use method::BoxFuture;
use semver::{Version, VersionReq};
use surrealdb_engine_api::SessionId;
#[doc(inline)]
pub use surrealdb_types::Error;
use surrealdb_types::NotAllowedError;
use tokio::sync::watch;
use uuid::Uuid;

use self::conn::Router;
use self::opt::{Endpoint, EndpointKind, WaitFor};

// Channel for waiters
type Waiter = (watch::Sender<Option<WaitFor>>, watch::Receiver<Option<WaitFor>>);

/// The server versions this client connects to. A server outside the range is
/// refused on connect, unless it denies the `version` RPC method. Pre-release
/// tags are stripped before matching, so `4.0.0-beta.1` is judged as `4.0.0`.
const SUPPORTED_VERSIONS: &str = ">=3.0.0-alpha.1, <5.0.0";

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
	/// This applies to the remote engines. An embedded connection queues
	/// nothing between the caller and the datastore — each request is awaited
	/// where it is made — so there is no channel for it to bound, and it is
	/// ignored.
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
				client.verify_server_version().await?;
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
				return Err(Error::connection(
					"Already connected".to_string(),
					Some(crate::types::ConnectionError::AlreadyConnected),
				));
			}
			let endpoint = self.address?;
			let endpoint_kind = EndpointKind::from(endpoint.url.scheme());
			let session_clone = self.surreal.inner.session_clone.clone();
			let client = Client::connect(endpoint, self.capacity, Some(session_clone)).await?;
			if endpoint_kind.is_remote() {
				client.verify_server_version().await?;
			}
			let router = client.inner.router.wait().clone();
			self.surreal.inner.router.set(router).map_err(|_| {
				Error::connection(
					"Already connected".to_string(),
					Some(crate::types::ConnectionError::AlreadyConnected),
				)
			})?;
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

#[derive(Debug, Clone)]
struct SessionClone {
	sender: Sender<SessionId>,
	#[allow(dead_code)]
	receiver: Receiver<SessionId>,
	/// Closes once the router task that was handed [`Self::router_receiver`]
	/// has ended.
	router_stopped: Receiver<()>,
	/// The sender behind [`Self::router_stopped`], until a router takes it.
	router_guard: Arc<std::sync::Mutex<Option<Sender<()>>>>,
}

impl SessionClone {
	fn new() -> Self {
		let (sender, receiver) = async_channel::unbounded();
		let (guard, router_stopped) = async_channel::bounded(1);
		Self {
			sender,
			receiver,
			router_stopped,
			router_guard: Arc::new(std::sync::Mutex::new(Some(guard))),
		}
	}

	/// The session events for a router task that runs apart from the engine
	/// (the HTTP and WebSocket transports). Dropping it, which is what the
	/// router does as it exits, is what [`Self::router_stopped`] reports.
	#[cfg(any(http_engine, ws_engine))]
	fn router_receiver(&self) -> RouterSessions {
		let guard = self.router_guard.lock().ok().and_then(|mut slot| slot.take());
		RouterSessions {
			receiver: self.receiver.clone(),
			_stopped: guard,
		}
	}

	/// Whether a router task was handed [`Self::router_receiver`].
	fn has_router(&self) -> bool {
		self.router_guard.lock().map(|slot| slot.is_none()).unwrap_or(false)
	}
}

/// A router task's end of the session channel.
#[cfg(any(http_engine, ws_engine))]
#[derive(Debug)]
pub(crate) struct RouterSessions {
	receiver: Receiver<SessionId>,
	_stopped: Option<Sender<()>>,
}

#[cfg(any(http_engine, ws_engine))]
impl RouterSessions {
	pub(crate) async fn recv(&self) -> std::result::Result<SessionId, async_channel::RecvError> {
		self.receiver.recv().await
	}

	pub(crate) fn try_recv(&self) -> std::result::Result<SessionId, async_channel::TryRecvError> {
		self.receiver.try_recv()
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
	/// Ends the session when the last handle sharing it is dropped.
	lease: Arc<SessionLease>,
	engine: PhantomData<C>,
}

/// Holds a session open. The session is detached when the last handle that
/// shares it goes away, so a live stream can outlive the client that
/// created it without losing the session its notifications are delivered to.
struct SessionLease {
	inner: Arc<Inner>,
	session_id: Uuid,
}

impl Drop for SessionLease {
	fn drop(&mut self) {
		self.inner.session_clone.sender.try_send(SessionId::Drop(self.session_id)).ok();
	}
}

#[doc(hidden)]
impl<C> From<Arc<Inner>> for Surreal<C>
where
	C: Connection,
{
	fn from(inner: Arc<Inner>) -> Self {
		let session_id = Uuid::new_v4();
		inner.session_clone.sender.try_send(SessionId::Initial(session_id)).ok();
		Surreal {
			lease: Arc::new(SessionLease {
				inner: Arc::clone(&inner),
				session_id,
			}),
			inner,
			session_id,
			engine: PhantomData,
		}
	}
}

#[doc(hidden)]
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

#[doc(hidden)]
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
		if !server_version_supported(version) {
			return Err(Error::internal(format!(
				"server version `{version}` does not match the range supported by the client `{SUPPORTED_VERSIONS}`"
			)));
		}

		Ok(())
	}

	// If the server denies the `version` RPC method via its capabilities, skip
	// the version check rather than failing to connect — the operator has
	// explicitly opted out of exposing the version, and the rest of the
	// connection is still usable.
	async fn verify_server_version(&self) -> Result<()> {
		match self.version().await {
			Ok(version) => self.check_server_version(&version).await,
			Err(e) if matches!(e.not_allowed_details(), Some(NotAllowedError::Method { .. })) => {
				debug!(
					"Skipping server version check; the `version` RPC method is denied by server capabilities"
				);
				Ok(())
			}
			Err(e) => Err(e),
		}
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
			lease: Arc::new(SessionLease {
				inner: Arc::clone(&self.inner),
				session_id,
			}),
			inner: Arc::clone(&self.inner),
			session_id,
			engine: self.engine,
		}
	}
}

impl<C> Surreal<C>
where
	C: Connection,
{
	/// Stops the connection and waits for it to wind down.
	///
	/// Dropping the last handle stops an embedded engine too, but on a task
	/// nothing can await. This ends the sessions of **every** handle onto the
	/// same connection, so they fail afterwards, and resolves once the embedded
	/// datastore has shut down and no background task still holds it. Storage
	/// that keeps its directory locked until the datastore is dropped is released
	/// when the last handle is then dropped. A remote
	/// connection detaches its server sessions. A failure to shut the datastore
	/// down is returned.
	///
	/// # Examples
	///
	/// ```no_run
	/// use surrealdb::Surreal;
	/// use surrealdb::engine::any::Any;
	///
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// let db = Surreal::<Any>::init();
	/// db.connect("surrealkv://path/to/db").await?;
	/// db.shutdown().await?;
	/// drop(db);
	/// // The directory can be opened again here.
	/// # Ok(())
	/// # }
	/// ```
	pub async fn shutdown(&self) -> Result<()> {
		// Not connected yet: there is nothing to stop, and closing the session
		// channel would leave a later `connect` with an engine that stops at
		// once.
		let Some(router) = self.inner.router.get() else {
			return Ok(());
		};
		self.inner.session_clone.sender.close();
		router.engine.shutdown().await?;
		// A transport that runs its own router task is done once that task has
		// detached its sessions and exited.
		if self.inner.session_clone.has_router() {
			self.inner.session_clone.router_stopped.recv().await.ok();
		}
		Ok(())
	}

	/// A handle on this same session, which stays open until every such handle
	/// is dropped. Unlike `clone`, it does not start a new session.
	pub(crate) fn share_session<D: Connection>(&self) -> Surreal<D> {
		Surreal {
			inner: Arc::clone(&self.inner),
			session_id: self.session_id,
			lease: Arc::clone(&self.lease),
			engine: PhantomData,
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
		let router = self.get().ok_or_else(|| {
			Error::connection(
				"Connection uninitialised".to_string(),
				Some(crate::types::ConnectionError::Uninitialised),
			)
		})?;
		Ok(router)
	}
}

/// Whether a server reporting `version` is inside [`SUPPORTED_VERSIONS`].
///
/// semver matches a pre-release only against a bound that names a pre-release
/// of the same release, so `4.0.0-beta.1` or `3.4.0-nightly` alone would fail
/// `>=3.0.0-alpha.1, <5.0.0`. A pre-release the requirement does not match is
/// therefore judged by its release version, so a client accepts the betas,
/// release candidates and nightlies of every release it accepts. The exception
/// is a release a bound names a pre-release of (`3.0.0`): there the pre-release
/// order decides, so `3.0.0-alpha.0` stays below the `3.0.0-alpha.1` floor.
fn server_version_supported(version: &Version) -> bool {
	// invalid version requirements should be caught during development
	let req = VersionReq::parse(SUPPORTED_VERSIONS).expect("valid supported versions");
	if req.matches(version) {
		return true;
	}
	if version.pre.is_empty() {
		return false;
	}
	let release = Version::new(version.major, version.minor, version.patch);
	let bound_names_this_release = req.comparators.iter().any(|bound| {
		!bound.pre.is_empty()
			&& bound.major == release.major
			&& bound.minor == Some(release.minor)
			&& bound.patch == Some(release.patch)
	});
	!bound_names_this_release && req.matches(&release)
}

/// Used by engine code (HTTP, local, any, and WS on wasm) when converting std/io errors.
#[allow(dead_code)]
fn std_error_to_types_error(error: impl std::fmt::Display) -> Error {
	Error::internal(error.to_string())
}

#[cfg(test)]
mod tests {
	use super::*;

	fn supported(version: &str) -> bool {
		server_version_supported(&Version::parse(version).unwrap())
	}

	#[test]
	fn test_supported_versions() {
		assert!(supported("3.0.0-alpha.1"));
		assert!(supported("3.0.0-beta.3"));
		assert!(supported("3.0.0-rc.2"));
		assert!(supported("3.0.0"));
		assert!(supported("3.0.1"));
		assert!(supported("3.4.0-nightly"));
		assert!(supported("3.9.0"));

		assert!(!supported("2.9.0"));
		assert!(!supported("2.6.2"));
		assert!(!supported("3.0.0-alpha.0"));
		assert!(!supported("2.9.0-rc.1"));
	}

	#[test]
	fn test_supported_versions_accepts_4x() {
		assert!(supported("4.0.0-alpha.1"));
		assert!(supported("4.0.0-beta.2"));
		assert!(supported("4.0.0-rc.1"));
		assert!(supported("4.0.0"));
		assert!(supported("4.0.3"));
		assert!(supported("4.9.0"));
	}

	#[test]
	fn test_supported_versions_rejects_5x() {
		assert!(!supported("5.0.0-alpha.1"));
		assert!(!supported("5.0.0"));
		assert!(!supported("6.1.0"));
	}
}

#[cfg(test)]
mod session_clone_tests {
	use super::SessionClone;

	#[cfg(any(http_engine, ws_engine))]
	#[tokio::test]
	async fn router_stopped_closes_when_the_router_drops_its_receiver() {
		let sessions = SessionClone::new();
		assert!(!sessions.has_router());
		let router = sessions.router_receiver();
		assert!(sessions.has_router());
		let stopped = sessions.router_stopped.clone();
		drop(router);
		assert!(stopped.recv().await.is_err(), "the router has stopped");
	}

	#[test]
	fn a_connection_without_a_router_task_has_nothing_to_wait_for() {
		assert!(!SessionClone::new().has_router());
	}
}
