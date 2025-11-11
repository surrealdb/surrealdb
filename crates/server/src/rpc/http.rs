use std::sync::Arc;

use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_core::rpc::{DbResult, RpcError, RpcProtocol};
use surrealdb_types::{Array, Value};
use tokio::sync::Semaphore;
use uuid::Uuid;

use crate::cnf::{PKG_NAME, PKG_VERSION};

pub struct Http {
	pub kvs: Arc<Datastore>,
	pub lock: Arc<Semaphore>,
	pub session: Arc<Session>,
}

impl Http {
	pub fn new(kvs: Arc<Datastore>, session: Session) -> Self {
		Self {
			kvs,
			lock: Arc::new(Semaphore::new(1)),
			session: Arc::new(session),
		}
	}
}

impl RpcProtocol for Http {
	/// The datastore for this RPC interface
	fn kvs(&self) -> &Datastore {
		&self.kvs
	}

	/// Retrieves the modification lock for this RPC context
	fn lock(&self) -> Arc<Semaphore> {
		self.lock.clone()
	}

	/// The version information for this RPC context
	fn version_data(&self) -> DbResult {
		let value = Value::String(format!("{PKG_NAME}-{}", *PKG_VERSION));
		DbResult::Other(value)
	}

	// ------------------------------
	// Sessions
	// ------------------------------

	/// The current session for this RPC context
	fn get_session(&self, _id: Option<&Uuid>) -> Arc<Session> {
		self.session.clone()
	}
	/// Mutable access to the current session for this RPC context
	fn set_session(&self, _id: Option<Uuid>, _session: Arc<Session>) {
		// Do nothing as HTTP is stateless
	}
	/// Mutable access to the current session for this RPC context
	fn del_session(&self, _id: &Uuid) {
		// Do nothing as HTTP is stateless
	}
	/// Lists all sessions
	fn list_sessions(&self) -> Vec<Uuid> {
		vec![]
	}

	// ------------------------------
	// Realtime
	// ------------------------------

	/// Live queries are disabled on HTTP
	const LQ_SUPPORT: bool = false;

	/// Handles the cleanup of live queries
	async fn cleanup_lqs(&self, _session_id: Option<&Uuid>) {
		// Do nothing as HTTP is stateless
	}

	/// Handles the cleanup of live queries
	async fn cleanup_all_lqs(&self) {
		// Do nothing as HTTP is stateless
	}

	// ------------------------------
	// Overrides
	// ------------------------------

	/// Parameters can't be set or unset on HTTP RPC context
	async fn set(&self, _session_id: Option<Uuid>, _params: Array) -> Result<DbResult, RpcError> {
		Err(RpcError::MethodNotFound)
	}

	/// Parameters can't be set or unset on HTTP RPC context
	async fn unset(&self, _session_id: Option<Uuid>, _params: Array) -> Result<DbResult, RpcError> {
		Err(RpcError::MethodNotFound)
	}

	/// Transactions are not supported on HTTP RPC context
	async fn begin(
		&self,
		_txn: Option<Uuid>,
		_session_id: Option<Uuid>,
	) -> Result<DbResult, RpcError> {
		Err(RpcError::MethodNotFound)
	}

	/// Transactions are not supported on HTTP RPC context
	async fn commit(
		&self,
		_txn: Option<Uuid>,
		_session_id: Option<Uuid>,
		_params: Array,
	) -> Result<DbResult, RpcError> {
		Err(RpcError::MethodNotFound)
	}

	/// Transactions are not supported on HTTP RPC context
	async fn cancel(
		&self,
		_txn: Option<Uuid>,
		_session_id: Option<Uuid>,
		_params: Array,
	) -> Result<DbResult, RpcError> {
		Err(RpcError::MethodNotFound)
	}
}
