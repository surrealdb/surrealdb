mod options;

use std::sync::Arc;
use std::time::Duration;

use async_channel::Receiver;
use dashmap::DashMap;
use napi::bindgen_prelude::*;
use napi::tokio::sync::RwLock;
use napi_derive::napi;
use options::Options;
use serde_json::{Value as JsValue, from_value};
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::export::Config;
use surrealdb_core::kvs::{Datastore, Transaction, TransactionType};
use surrealdb_core::rpc::format::cbor;
use surrealdb_core::rpc::{DbResult, Request, RpcProtocol, invalid_params};
use surrealdb_types::{Action, Array, HashMap, Notification, Value};
use uuid::Uuid;

use crate::err::err_map;

/// An embedded SurrealDB instance, addressed over the RPC protocol.
///
/// The connection is taken out of the `Option` by [`SurrealNodeEngine::free`],
/// after which every method reports a closed engine rather than panicking —
/// the addon is built with `panic = "abort"`, so a panic here would take the
/// host process down with it.
#[napi]
pub struct SurrealNodeEngine(RwLock<Option<SurrealNodeConnection>>);

#[napi]
pub struct NotificationReceiver {
	receiver: Receiver<Uint8Array>,
}

#[napi]
impl NotificationReceiver {
	#[napi]
	pub async fn recv(&self) -> std::result::Result<Option<Uint8Array>, Error> {
		match self.receiver.recv().await {
			Ok(data) => Ok(Some(data)),
			Err(_) => Ok(None), // Channel closed
		}
	}
}

#[napi]
impl SurrealNodeEngine {
	#[napi]
	pub async fn execute(&self, data: Uint8Array) -> std::result::Result<Uint8Array, Error> {
		let lock = self.0.read().await;
		let engine = lock.as_ref().ok_or_else(closed)?;
		let obj = cbor::decode(data.to_vec().as_slice(), engine.recursion_limit())
			.map_err(err_map)?
			.into_object()
			.map_err(err_map)?;
		let req = Request::from_object(obj).map_err(err_map)?;
		// A request that names no session is served by this connection's
		// implicit default session, mirroring how the WebSocket transport
		// resolves its own connection-level session.
		let client_session: Option<Uuid> = req.session_id.map(Into::into);
		let session_id = client_session.unwrap_or(engine.id);
		let res = RpcProtocol::execute(
			engine,
			req.txn.map(Into::into),
			session_id,
			client_session,
			req.method,
			req.params,
		)
		.await;

		match res {
			Ok(result) => {
				let value = Value::from_t(result);
				let out = cbor::encode(value).map_err(err_map)?;
				Ok(out.as_slice().into())
			}
			Err(rpc_err) => {
				let mut envelope = surrealdb_types::Object::default();
				envelope.insert("error".to_string(), Value::from_t(rpc_err));
				let out = cbor::encode(Value::Object(envelope)).map_err(err_map)?;
				Ok(out.as_slice().into())
			}
		}
	}

	/// Start forwarding live query notifications, CBOR-encoded, on a channel
	/// the caller drains with [`NotificationReceiver::recv`].
	#[napi]
	pub async fn notifications(&self) -> std::result::Result<NotificationReceiver, Error> {
		let (notifications, live_queries) = {
			let lock = self.0.read().await;
			let engine = lock.as_ref().ok_or_else(closed)?;
			(engine.notifications.clone(), Arc::clone(&engine.live_queries))
		};

		let (tx, rx) = async_channel::unbounded();

		// Spawn a task to process notifications
		napi::tokio::spawn(async move {
			while let Ok(notification) = notifications.recv().await {
				// A `Killed` notification is the last one its live query id
				// will ever carry, and the only signal that names a
				// subscription the datastore has ended, so the registration
				// ends with it. Nothing else can drop the entry.
				if notification.action == Action::Killed {
					live_queries.remove(&notification.id.into_inner());
				}

				let message = Value::from_t(notification);

				if let Ok(out) = cbor::encode(message) {
					let data = out.as_slice().into();
					if tx.send(data).await.is_err() {
						break; // Receiver dropped
					}
				}
			}
		});

		Ok(NotificationReceiver {
			receiver: rx,
		})
	}

	#[napi]
	pub async fn connect(
		endpoint: String,
		#[napi(ts_arg_type = "ConnectionOptions")] opts: Option<JsValue>,
	) -> std::result::Result<SurrealNodeEngine, Error> {
		let endpoint = match &endpoint {
			s if s.starts_with("mem:") => "memory",
			s => s,
		};

		let opts: Option<Options> = from_value::<Option<Options>>(JsValue::from(opts))?;
		let Options {
			query_timeout,
			transaction_timeout,
			capabilities,
			defaults,
		} = opts.unwrap_or_default();
		let defaults = defaults.unwrap_or_default();

		// Notifications are wired unconditionally: the JavaScript engine
		// subscribes as part of connecting, before it can know whether the
		// capability is on, and an unused channel costs nothing.
		let (notify_tx, notify_rx) =
			async_channel::bounded(surrealdb_core::cnf::NOTIFICATIONS_CHANNEL_SIZE);

		let kvs = Datastore::builder()
			.with_notify(notify_tx)
			.with_capabilities(capabilities.map_or(Ok(Default::default()), TryInto::try_into)?)
			.with_transaction_timeout(transaction_timeout.map(Duration::from_secs))
			.with_query_timeout(query_timeout.map(Duration::from_secs))
			.build_with_path(endpoint)
			.await
			.map_err(err_map)?;

		let (_, is_new) = kvs.check_version().await.map_err(err_map)?;
		// Register this node in the cluster keyspace. Live query rows are
		// keyed by node id, so a datastore that never bootstraps has no node
		// row for its own subscriptions to hang off.
		kvs.bootstrap().await.map_err(err_map)?;

		if is_new && let Some(defaults) = defaults.get_defaults() {
			kvs.initialise_defaults(&defaults.0, &defaults.1).await.map_err(err_map)?;
		}

		// The implicit default session, used by every request that names no
		// session id of its own.
		let id = Uuid::now_v7();
		let mut session = Session::default().with_rt(true);
		session.id = Some(id);
		let sessions = HashMap::new();
		sessions.insert(id, Arc::new(RwLock::new(session)));

		let connection = SurrealNodeConnection {
			id,
			kvs: Arc::new(kvs),
			live_queries: Arc::new(HashMap::new()),
			transactions: DashMap::new(),
			sessions,
			notifications: notify_rx,
		};

		Ok(SurrealNodeEngine(RwLock::new(Some(connection))))
	}

	#[napi]
	pub async fn export(&self, config: Option<Uint8Array>) -> std::result::Result<String, Error> {
		let lock = self.0.read().await;
		let engine = lock.as_ref().ok_or_else(closed)?;
		let (tx, rx) = async_channel::unbounded();
		let session_arc = engine.default_session();
		let session_guard = session_arc.read().await;

		match config {
			Some(config) => {
				let in_config = cbor::decode(config.to_vec().as_slice(), engine.recursion_limit())
					.map_err(err_map)?;
				let config = in_config.into_t::<Config>().map_err(err_map)?;
				engine
					.kvs
					.export_with_config(&session_guard, tx, config)
					.await
					.map_err(err_map)?
					.await
					.map_err(err_map)?;
			}
			None => {
				engine
					.kvs
					.export(&session_guard, tx)
					.await
					.map_err(err_map)?
					.await
					.map_err(err_map)?;
			}
		};

		let mut buffer = Vec::new();
		while let Ok(item) = rx.try_recv() {
			buffer.push(item);
		}

		let result = String::from_utf8(buffer.concat()).map_err(err_map)?;

		Ok(result)
	}

	#[napi]
	pub async fn import(&self, input: String) -> std::result::Result<(), Error> {
		let lock = self.0.read().await;
		let engine = lock.as_ref().ok_or_else(closed)?;
		let session_arc = engine.default_session();
		let session_guard = session_arc.read().await;
		engine.kvs.import(&input, &session_guard).await.map_err(err_map)?;

		Ok(())
	}

	#[napi]
	pub fn version() -> std::result::Result<String, Error> {
		Ok(surrealdb_core::env::VERSION.to_owned())
	}

	#[napi]
	pub async fn free(&self) {
		let _inner_opt = self.0.write().await.take();
	}
}

/// The error reported by every method once [`SurrealNodeEngine::free`] has
/// taken the connection.
fn closed() -> Error {
	Error::new(napi::Status::GenericFailure, "The engine has been closed")
}

struct SurrealNodeConnection {
	/// The id of this connection's implicit default session.
	pub id: Uuid,
	pub kvs: Arc<Datastore>,
	/// Live query id -> the id of the session that registered it. Shared with
	/// the notification-forwarding task, which ends a registration when it
	/// forwards the `Killed` notification naming it.
	pub live_queries: Arc<HashMap<Uuid, Uuid>>,
	/// Open client-managed transactions, each tagged with the session that
	/// owns it so it can be cancelled when that session goes away.
	pub transactions: DashMap<Uuid, (Uuid, Arc<Transaction>)>,
	pub sessions: HashMap<Uuid, Arc<RwLock<Session>>>,
	pub notifications: Receiver<Notification>,
}

impl SurrealNodeConnection {
	fn default_session(&self) -> Arc<RwLock<Session>> {
		self.sessions.get(&self.id).expect("the default session is registered on connect")
	}

	/// The nesting depth a decoded RPC payload may reach, taken from the same
	/// datastore setting the server's transports use.
	fn recursion_limit(&self) -> usize {
		self.kvs.parser_config().max_object_parsing_depth as usize
	}
}

type TxError = surrealdb_types::Error;
type TxResult<T> = std::result::Result<T, TxError>;

impl RpcProtocol for SurrealNodeConnection {
	fn kvs(&self) -> &Datastore {
		&self.kvs
	}

	fn kvs_arc(&self) -> Arc<Datastore> {
		Arc::clone(&self.kvs)
	}

	fn version_data(&self) -> DbResult {
		DbResult::Other(Value::String(format!("surrealdb-{}", surrealdb_core::env::VERSION)))
	}

	/// A pointer to all active sessions
	fn session_map(&self) -> &HashMap<Uuid, Arc<RwLock<Session>>> {
		&self.sessions
	}

	const LQ_SUPPORT: bool = true;

	/// Records a LIVE registration against the session that made it.
	///
	/// The namespace and database are snapshotted by the caller off the
	/// session read guard it already holds; this implementation has no use
	/// for them, and must not re-lock the session to obtain them.
	async fn handle_live(
		&self,
		lqid: &Uuid,
		session_id: Uuid,
		_namespace: Option<String>,
		_database: Option<String>,
	) {
		self.live_queries.insert(*lqid, session_id);
	}

	/// Handles the cleanup of live queries for a given session
	async fn cleanup_lqs(&self, session_id: &Uuid) {
		let mut gc = Vec::new();
		self.live_queries.retain(|key, value| {
			if value == session_id {
				gc.push(*key);
				return false;
			}
			true
		});
		let _ = self.kvs.delete_queries(gc).await;
	}

	/// Handles the cleanup of all live queries
	async fn cleanup_all_lqs(&self) {
		let gc: Vec<Uuid> = self.live_queries.to_vec().into_iter().map(|(key, _)| key).collect();
		self.live_queries.clear();
		let _ = self.kvs.delete_queries(gc).await;
	}

	/// Cancels any transactions still open for a session that is being
	/// detached or reset, so abandoning a session cannot leak them.
	async fn cleanup_txns(&self, session_id: &Uuid) {
		// Collect the ids first: the removal below awaits, and a live DashMap
		// iterator held across an await can deadlock the map.
		let doomed: Vec<Uuid> = self
			.transactions
			.iter()
			.filter(|entry| &entry.value().0 == session_id)
			.map(|entry| *entry.key())
			.collect();
		for id in doomed {
			if let Some((_, (_, tx))) = self.transactions.remove(&id) {
				let _ = tx.cancel().await;
			}
		}
	}

	// ------------------------------
	// Transactions
	// ------------------------------

	/// Retrieves a transaction by ID
	async fn get_tx(&self, id: Uuid) -> TxResult<Arc<Transaction>> {
		self.transactions
			.get(&id)
			.map(|entry| Arc::clone(&entry.value().1))
			.ok_or_else(|| invalid_params("Transaction not found"))
	}

	// `set_tx` is deliberately left at its default. A transaction only enters
	// the map through `begin`, which is also what tags it with the session
	// responsible for cleaning it up, so an implementation here could only
	// produce an untracked transaction that no cleanup path finds.

	// ------------------------------
	// Methods for transactions
	// ------------------------------

	/// Begin a new transaction
	async fn begin(&self, _txn: Option<Uuid>, session_id: Uuid) -> TxResult<DbResult> {
		// Reject a `begin` for a session that was never attached, so a caller
		// cannot strand transactions under a session id nothing will ever
		// clean up. The implicit default session is always registered.
		self.get_session(&session_id).await?;
		// Create a new transaction
		let tx = self
			.kvs()
			.transaction(TransactionType::Write)
			.await
			.map_err(surrealdb_core::rpc::types_error_from_anyhow)?;
		// Generate a unique transaction ID
		let id = Uuid::now_v7();
		// Store the transaction in the map, tagged with the owning session
		self.transactions.insert(id, (session_id, Arc::new(tx)));
		// Close the begin/detach race: `del_session` removes the session from
		// the map before draining its transactions, so a detach that ran
		// during the await above would have drained the map before this
		// transaction was published. Re-checking after the insert means one
		// side always observes the other.
		if !self.sessions.contains_key(&session_id) {
			self.cleanup_txns(&session_id).await;
			return Err(surrealdb_core::rpc::session_not_found(session_id));
		}
		// Return the transaction ID to the client
		Ok(DbResult::Other(Value::Uuid(surrealdb_types::Uuid::from(id))))
	}

	/// Commit a transaction
	async fn commit(
		&self,
		_txn: Option<Uuid>,
		_session_id: Uuid,
		params: Array,
	) -> TxResult<DbResult> {
		let (_, tx) = self.take_tx(params)?;
		tx.commit().await.map_err(surrealdb_core::rpc::types_error_from_anyhow)?;
		Ok(DbResult::Other(Value::None))
	}

	/// Cancel a transaction
	async fn cancel(
		&self,
		_txn: Option<Uuid>,
		_session_id: Uuid,
		params: Array,
	) -> TxResult<DbResult> {
		let (_, tx) = self.take_tx(params)?;
		tx.cancel().await.map_err(surrealdb_core::rpc::types_error_from_anyhow)?;
		Ok(DbResult::Other(Value::None))
	}
}

impl SurrealNodeConnection {
	/// Remove the transaction named by the trailing UUID in `params`, as sent
	/// by `commit` and `cancel`.
	fn take_tx(&self, params: Array) -> TxResult<(Uuid, Arc<Transaction>)> {
		let mut params_vec = params.into_vec();
		let Some(Value::Uuid(txn_id)) = params_vec.pop() else {
			return Err(invalid_params("Expected transaction UUID"));
		};
		self.transactions
			.remove(&txn_id.into_inner())
			.map(|(_, entry)| entry)
			.ok_or_else(|| invalid_params("Transaction not found"))
	}
}
