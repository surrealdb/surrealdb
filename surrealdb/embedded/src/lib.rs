//! The shared body of SurrealDB's embedded JavaScript engines.
//!
//! `@surrealdb/node-native` and `@surrealdb/wasm-native` are the same database
//! behind two different FFIs. Everything that is not the FFI lives here:
//! connection options, and the session / transaction / live-query bookkeeping an
//! [`RpcProtocol`] implementation needs. Each shim is left with the parts that
//! genuinely differ — how JavaScript hands over an options object, how values
//! cross the boundary, and the lifetime of the FFI handle.
//!
//! **No wire format is inherent here.** An embedded engine has no wire, so the
//! typed methods ([`EmbeddedEngine::execute`], [`EmbeddedEngine::notifications`])
//! are the real boundary and a shim that can hand JavaScript a value directly
//! pays nothing for encoding. [`EmbeddedEngine::execute_encoded`] and [`wire`]
//! are there for a caller that does speak bytes — today both shims do, because
//! the JavaScript SDK's engine interface is shared with its WebSocket and HTTP
//! engines and those genuinely have a wire.
//!
//! This is deliberately the *server* side of the RPC boundary. The client side
//! is `SurrealEngine` in `surrealdb-engine-api`, which the Rust SDK's router
//! consumes; the JavaScript SDK is the client here, and it speaks the RPC
//! envelope directly.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};
use std::time::Duration;

use anyhow::Result;
use async_channel::Receiver;
use dashmap::DashMap;
use futures::Stream;
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_core::rpc::RpcProtocol;
use surrealdb_datastore::Transaction;
use surrealdb_types::{Action, HashMap, Notification, Value};
use tokio::sync::RwLock;
use uuid::Uuid;

mod options;
mod protocol;
pub mod wire;

pub use surrealdb_core::rpc::Format;
pub use surrealdb_rpc::export::Config as ExportConfig;
pub use surrealdb_rpc::{DbResult, Request};

pub use self::options::{
	CapabilitiesConfig, DefaultsConfig, Options, PlannerStrategy, Targets, TargetsConfig,
};

/// An embedded SurrealDB instance addressed over the RPC protocol.
///
/// Every method takes `&self`: wasm-bindgen cannot express `&mut self` on an
/// async method, and a shim holds this behind its own lock so its handle can be
/// released independently.
pub struct EmbeddedEngine {
	/// The id of this connection's implicit default session, used by every
	/// request that names no session of its own — mirroring how the WebSocket
	/// transport resolves its connection-level session.
	id: Uuid,
	kvs: Arc<Datastore>,
	/// Live query id -> the id of the session that registered it. Shared with
	/// the notification stream, which ends a registration when it yields the
	/// `Killed` notification naming it.
	live_queries: Arc<HashMap<Uuid, Uuid>>,
	/// Open client-managed transactions, each tagged with the session that owns
	/// it so it can be cancelled when that session goes away.
	transactions: DashMap<Uuid, (Uuid, Arc<Transaction>)>,
	sessions: HashMap<Uuid, Arc<RwLock<Session>>>,
	notifications: Receiver<Notification>,
}

impl EmbeddedEngine {
	/// Opens a datastore at `endpoint` and registers the connection's implicit
	/// default session.
	///
	/// `options` arrives already deserialized, so the shim owns the conversion
	/// from its own JavaScript value representation.
	pub async fn connect(endpoint: &str, options: Options) -> Result<Self> {
		// `mem:` is the JavaScript SDK's spelling of the in-memory backend.
		let endpoint = match endpoint {
			e if e.starts_with("mem:") => "memory",
			e => e,
		};

		let Options {
			query_timeout,
			transaction_timeout,
			capabilities,
			defaults,
		} = options;
		let defaults = defaults.unwrap_or_default();

		// Notifications are wired unconditionally: the JavaScript engine
		// subscribes as part of connecting, before it can know whether the
		// capability is on, and an unused channel costs nothing.
		let (notify_tx, notify_rx) =
			async_channel::bounded(surrealdb_cnf::NOTIFICATIONS_CHANNEL_SIZE);

		let capabilities =
			capabilities.map_or_else(|| Ok(Default::default()), TryInto::try_into)?;

		let kvs = Datastore::builder()
			.with_notify(notify_tx)
			.with_capabilities(capabilities)
			.with_transaction_timeout(transaction_timeout.map(Duration::from_secs))
			.with_query_timeout(query_timeout.map(Duration::from_secs))
			.build_with_path(endpoint)
			.await?;

		let (_, is_new) = kvs.check_version().await?;
		// Register this node in the cluster keyspace. Live query rows are keyed
		// by node id, so a datastore that never bootstraps has no node row for
		// its own subscriptions to hang off.
		kvs.bootstrap().await?;

		if is_new && let Some((namespace, database)) = defaults.get_defaults() {
			kvs.initialise_defaults(&namespace, &database).await?;
		}

		let id = Uuid::now_v7();
		let mut session = Session::default().with_rt(true);
		session.id = Some(id);
		let sessions = HashMap::new();
		sessions.insert(id, Arc::new(RwLock::new(session)));

		Ok(Self {
			id,
			kvs,
			live_queries: Arc::new(HashMap::new()),
			transactions: DashMap::new(),
			sessions,
			notifications: notify_rx,
		})
	}

	/// Answers one RPC request.
	///
	/// This is the engine's actual boundary: no encoding is involved, so a shim
	/// that can hand JavaScript a value directly — rather than bytes it has to
	/// decode again — does not pay for a round trip through a wire format.
	/// [`Self::execute_encoded`] is the framed form for a caller that does speak
	/// bytes.
	///
	/// A request naming no session is served by this connection's implicit
	/// default session, mirroring how the WebSocket transport resolves its own
	/// connection-level session.
	pub async fn execute(
		&self,
		request: Request,
	) -> std::result::Result<DbResult, surrealdb_types::Error> {
		let client_session: Option<Uuid> = request.session_id.map(Into::into);
		let session_id = client_session.unwrap_or(self.id);
		RpcProtocol::execute(
			self,
			request.txn.map(Into::into),
			session_id,
			client_session,
			request.method,
			request.params,
		)
		.await
	}

	/// Answers one encoded RPC request with an encoded reply.
	///
	/// A method error is encoded into the reply envelope rather than returned:
	/// the JavaScript SDK reads it off the response like any other transport's.
	/// The `Err` here is reserved for a request that could not be framed at all.
	pub async fn execute_encoded(&self, format: Format, request: &[u8]) -> Result<Vec<u8>> {
		let obj = wire::decode(format, request, self.recursion_limit())?.into_object()?;
		let value = match self.execute(Request::from_object(obj)?).await {
			Ok(result) => Value::from_t(result),
			Err(err) => {
				let mut envelope = surrealdb_types::Object::default();
				envelope.insert("error".to_owned(), Value::from_t(err));
				Value::Object(envelope)
			}
		};
		wire::encode(format, value)
	}

	/// The live-query notifications for this connection.
	///
	/// A stream rather than a channel of its own, so a shim adds no buffer of its
	/// own on top of the datastore's bounded notification channel.
	///
	/// That bounds what this connection holds; it does **not** reach the write
	/// path. `Executor::flush_live_query_notifications` spawns a detached task
	/// per statement batch that drains an unbounded per-batch channel into the
	/// datastore's bounded one, so a stalled consumer fills the bounded channel,
	/// blocks those tasks, and leaves the notifications accumulating in the
	/// unbounded channels behind them. Bounding delivery end to end has to
	/// happen before it is detached, which is not something a shim can do.
	pub fn notifications(&self) -> Notifications {
		Notifications {
			inner: Box::pin(self.notifications.clone()),
			live_queries: Arc::clone(&self.live_queries),
		}
	}

	/// Exports the default session's database as SurrealQL.
	///
	/// The config arrives decoded, so the shim owns its wire format the same way
	/// it does for [`Self::execute`]; [`wire::decode`] plus
	/// [`Value::into_t`](surrealdb_types::Value::into_t) is the path from bytes.
	pub async fn export(&self, config: Option<ExportConfig>) -> Result<String> {
		let (tx, rx) = async_channel::unbounded();
		let session = self.default_session();
		let session = session.read().await;

		match config {
			Some(config) => {
				self.kvs.export_with_config(&session, tx, config).await?.await?;
			}
			None => self.kvs.export(&session, tx).await?.await?,
		}

		let mut buffer = Vec::new();
		while let Ok(item) = rx.try_recv() {
			buffer.push(item);
		}
		Ok(String::from_utf8(buffer.concat())?)
	}

	/// Exports the default session's database from an encoded config.
	///
	/// The framed counterpart to [`Self::export`], as
	/// [`Self::execute_encoded`] is to [`Self::execute`].
	pub async fn export_encoded(&self, format: Format, config: Option<&[u8]>) -> Result<String> {
		let config = match config {
			Some(bytes) => Some(
				wire::decode(format, bytes, self.recursion_limit())?.into_t::<ExportConfig>()?,
			),
			None => None,
		};
		self.export(config).await
	}

	/// Imports SurrealQL into the default session's database.
	pub async fn import(&self, sql: &str) -> Result<()> {
		let session = self.default_session();
		let session = session.read().await;
		self.kvs.import(sql, &session).await?;
		Ok(())
	}

	/// The engine version, as the `version` RPC method reports it.
	pub fn version() -> &'static str {
		surrealdb_core::env::VERSION
	}

	fn default_session(&self) -> Arc<RwLock<Session>> {
		self.sessions.get(&self.id).expect("the default session is registered on connect")
	}

	/// The nesting depth a decoded RPC payload may reach, taken from the same
	/// datastore setting the server's transports use.
	fn recursion_limit(&self) -> usize {
		self.kvs.parser_config().max_object_parsing_depth as usize
	}
}

/// The live-query notifications for one embedded connection.
///
/// Yields the notification itself rather than encoded bytes, so a shim that can
/// hand JavaScript a value directly does not pay for a round trip through a wire
/// format; one that wants bytes encodes with [`wire::encode`].
///
/// Ends when the datastore's notification channel closes. Dropping this stops
/// nothing: subscriptions live in the datastore, and are ended by `KILL`, by the
/// session going away, or by [`EmbeddedEngine`] being dropped.
pub struct Notifications {
	/// Pinned on the heap because [`Receiver`] is not `Unpin`, which keeps
	/// `Notifications` itself `Unpin` for the shims that drive it from a loop.
	/// One allocation per connection.
	inner: Pin<Box<Receiver<Notification>>>,
	/// The registrations to keep in step with what the datastore has ended.
	live_queries: Arc<HashMap<Uuid, Uuid>>,
}

impl Notifications {
	/// Adapts this into encoded frames, for a shim that hands JavaScript bytes.
	pub fn encoded(self, format: Format) -> EncodedNotifications {
		EncodedNotifications {
			inner: self,
			format,
		}
	}
}

/// [`Notifications`] encoded for a shim that carries bytes.
///
/// A notification that fails to encode is dropped rather than ending the stream,
/// which would silently unsubscribe every live query on the connection.
pub struct EncodedNotifications {
	inner: Notifications,
	format: Format,
}

impl Stream for EncodedNotifications {
	type Item = Vec<u8>;

	fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
		loop {
			match Pin::new(&mut self.inner).poll_next(cx) {
				Poll::Ready(Some(notification)) => {
					let format = self.format;
					if let Ok(encoded) = wire::encode(format, Value::from_t(notification)) {
						return Poll::Ready(Some(encoded));
					}
					// Dropped; try the next one rather than stall the stream.
				}
				Poll::Ready(None) => return Poll::Ready(None),
				Poll::Pending => return Poll::Pending,
			}
		}
	}
}

impl Stream for Notifications {
	type Item = Notification;

	fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
		let next = self.inner.as_mut().poll_next(cx);
		if let Poll::Ready(Some(notification)) = &next {
			// A `Killed` notification is the last one its live query id will ever
			// carry, and the only signal naming a subscription the datastore has
			// ended, so the registration ends with it. Nothing else can drop the
			// entry.
			if notification.action == Action::Killed {
				self.live_queries.remove(&notification.id.into_inner());
			}
		}
		next
	}
}

#[cfg(all(test, feature = "kv-mem"))]
mod tests {
	use futures::StreamExt;
	use surrealdb_types::{Array, Object};

	use super::*;

	async fn engine() -> EmbeddedEngine {
		EmbeddedEngine::connect("memory", Options::default()).await.expect("connect")
	}

	fn query(sql: &str) -> Request {
		Request::from_object(query_object(sql)).expect("a query request should parse")
	}

	/// A query request as the wire carries it, so the encoded tests exercise the
	/// same decode path a shim's caller does.
	fn query_object(sql: &str) -> Object {
		let mut obj = Object::default();
		obj.insert("method".to_owned(), Value::String("query".to_owned()));
		obj.insert(
			"params".to_owned(),
			Value::Array(Array::from(vec![Value::String(sql.to_owned())])),
		);
		obj
	}

	/// The typed boundary answers without any encoding at all, which is what
	/// lets a shim hand JavaScript a value rather than bytes to decode again.
	#[tokio::test]
	async fn execute_answers_without_encoding() {
		let engine = engine().await;
		let result = engine.execute(query("RETURN 1 + 1")).await.expect("query");
		let DbResult::Query(mut results) = result else {
			panic!("expected a query result, got {result:?}");
		};
		assert_eq!(results.remove(0).result.expect("statement"), Value::from_t(2i64));
	}

	/// No format is inherent to the engine: the same request answers over each
	/// format the enum carries, so a shim picks by what its caller speaks.
	#[tokio::test]
	async fn execute_encoded_round_trips_every_format() {
		let engine = engine().await;
		for format in [Format::Cbor, Format::Flatbuffers, Format::Json] {
			let request = wire::encode(format, Value::Object(query_object("RETURN 40 + 2")))
				.unwrap_or_else(|e| panic!("{format:?} should encode a request: {e:#}"));
			let reply = engine
				.execute_encoded(format, &request)
				.await
				.unwrap_or_else(|e| panic!("{format:?} should answer: {e:#}"));
			let decoded = wire::decode(format, &reply, 32)
				.unwrap_or_else(|e| panic!("{format:?} should decode its own reply: {e:#}"));
			assert!(
				format!("{decoded:?}").contains("42"),
				"{format:?} lost the result: {decoded:?}"
			);
		}
	}

	/// A LIVE registration delivers through the typed stream, and the stream ends
	/// when the engine goes away rather than hanging its consumer.
	#[tokio::test]
	async fn notifications_deliver_and_then_end() {
		let engine = engine().await;
		let mut stream = engine.notifications();
		engine.execute(query("DEFINE NAMESPACE n")).await.expect("ns");
		engine.execute(query("USE NS n; DEFINE DATABASE d;")).await.expect("db");
		engine
			.execute(query("USE NS n DB d; DEFINE TABLE t; LIVE SELECT * FROM t;"))
			.await
			.expect("live");
		engine.execute(query("USE NS n DB d; CREATE t:1;")).await.expect("create");

		let notification = tokio::time::timeout(Duration::from_secs(10), stream.next())
			.await
			.expect("a live query should deliver within 10s")
			.expect("the stream should yield a notification");
		assert_eq!(notification.action, Action::Create);

		// Dropping the engine closes the datastore's notification channel, which
		// ends the stream instead of leaving a consumer awaiting forever.
		drop(engine);
		assert!(
			tokio::time::timeout(Duration::from_secs(10), stream.next())
				.await
				.expect("the stream should end once the engine is gone")
				.is_none()
		);
	}

	/// An unusable format is reported rather than silently producing nothing.
	#[test]
	fn an_unsupported_format_is_an_error() {
		assert!(wire::encode(Format::Unsupported, Value::None).is_err());
		assert!(wire::decode(Format::Unsupported, &[], 32).is_err());
	}
}
