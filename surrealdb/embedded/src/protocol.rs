//! The RPC method semantics for an embedded engine.
//!
//! Only the hooks an embedded engine has to answer for itself are implemented:
//! session and transaction bookkeeping, and live-query registration. Every
//! method's behaviour comes from [`RpcProtocol`]'s provided implementations, so
//! an embedded connection and a WebSocket connection cannot drift.

use std::sync::Arc;

use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_core::rpc::{RpcProtocol, types_error_from_anyhow};
use surrealdb_datastore::Transaction;
use surrealdb_kvs::TransactionType;
use surrealdb_rpc::DbResult;
use surrealdb_rpc::error::{invalid_params, session_not_found};
use surrealdb_types::{Array, HashMap, Value};
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::EmbeddedEngine;

type TxResult<T> = std::result::Result<T, surrealdb_types::Error>;

impl RpcProtocol for EmbeddedEngine {
	const LQ_SUPPORT: bool = true;

	fn kvs(&self) -> &Datastore {
		&self.kvs
	}

	fn kvs_arc(&self) -> Arc<Datastore> {
		Arc::clone(&self.kvs)
	}

	fn version_data(&self) -> DbResult {
		DbResult::Other(Value::String(format!("surrealdb-{}", surrealdb_core::env::VERSION)))
	}

	fn session_map(&self) -> &HashMap<Uuid, Arc<RwLock<Session>>> {
		&self.sessions
	}

	/// Records a LIVE registration against the session that made it.
	///
	/// The namespace and database are snapshotted by the caller off the session
	/// read guard it already holds; this implementation has no use for them, and
	/// must not re-lock the session to obtain them.
	async fn handle_live(
		&self,
		lqid: &Uuid,
		session_id: Uuid,
		_namespace: Option<String>,
		_database: Option<String>,
	) {
		self.live_queries.insert(*lqid, session_id);
	}

	/// Ends the live queries a single session registered.
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

	/// Ends every live query on this connection.
	async fn cleanup_all_lqs(&self) {
		let gc: Vec<Uuid> = self.live_queries.to_vec().into_iter().map(|(key, _)| key).collect();
		self.live_queries.clear();
		let _ = self.kvs.delete_queries(gc).await;
	}

	/// Cancels any transactions still open for a session that is being detached
	/// or reset, so abandoning a session cannot leak them.
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

	async fn begin(&self, _txn: Option<Uuid>, session_id: Uuid) -> TxResult<DbResult> {
		// Reject a `begin` for a session that was never attached, so a caller
		// cannot strand transactions under a session id nothing will ever clean
		// up. The implicit default session is always registered.
		self.get_session(&session_id).await?;
		let tx = self
			.kvs()
			.transaction(TransactionType::Write)
			.await
			.map_err(types_error_from_anyhow)?;
		let id = Uuid::now_v7();
		self.transactions.insert(id, (session_id, Arc::new(tx)));
		// Close the begin/detach race: `del_session` removes the session from
		// the map before draining its transactions, so a detach that ran during
		// the await above would have drained the map before this transaction was
		// published. Re-checking after the insert means one side always observes
		// the other.
		if !self.sessions.contains_key(&session_id) {
			self.cleanup_txns(&session_id).await;
			return Err(session_not_found(session_id));
		}
		Ok(DbResult::Other(Value::Uuid(surrealdb_types::Uuid::from(id))))
	}

	async fn commit(
		&self,
		_txn: Option<Uuid>,
		_session_id: Uuid,
		params: Array,
	) -> TxResult<DbResult> {
		let (_, tx) = self.take_tx(params)?;
		tx.commit().await.map_err(types_error_from_anyhow)?;
		Ok(DbResult::Other(Value::None))
	}

	async fn cancel(
		&self,
		_txn: Option<Uuid>,
		_session_id: Uuid,
		params: Array,
	) -> TxResult<DbResult> {
		let (_, tx) = self.take_tx(params)?;
		tx.cancel().await.map_err(types_error_from_anyhow)?;
		Ok(DbResult::Other(Value::None))
	}
}

impl EmbeddedEngine {
	/// Remove the transaction named by the trailing UUID in `params`, as sent by
	/// `commit` and `cancel`.
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
