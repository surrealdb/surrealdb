use std::sync::Arc;

use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_core::rpc::{DbResult, Method, RpcProtocol, method_not_found};
use surrealdb_types::{Array, Error as TypesError, HashMap, Value};
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::cnf::{PKG_NAME, PKG_VERSION};

/// HTTP RPC handler with per-request session isolation.
///
/// Sessions are inserted under unique per-request keys by `post_handler`
/// and removed after execution completes. No default session is stored.
pub struct Http {
	pub kvs: Arc<Datastore>,
	pub sessions: HashMap<Uuid, Arc<RwLock<Session>>>,
}

impl Http {
	pub fn new(kvs: Arc<Datastore>) -> Self {
		Self {
			kvs,
			sessions: HashMap::new(),
		}
	}
}

impl RpcProtocol for Http {
	/// The datastore for this RPC interface
	fn kvs(&self) -> &Datastore {
		&self.kvs
	}

	/// The version information for this RPC context
	fn version_data(&self) -> DbResult {
		let value = Value::String(format!("{PKG_NAME}-{}", *PKG_VERSION));
		DbResult::Other(value)
	}

	/// A pointer to all active sessions
	fn session_map(&self) -> &HashMap<Uuid, Arc<RwLock<Session>>> {
		&self.sessions
	}

	// ------------------------------
	// Realtime
	// ------------------------------

	/// Live queries are disabled on HTTP
	const LQ_SUPPORT: bool = false;

	/// Handles the cleanup of live queries
	async fn cleanup_lqs(&self, _session_id: &Uuid) {
		// Do nothing as HTTP is stateless
	}

	/// Handles the cleanup of live queries
	async fn cleanup_all_lqs(&self) {
		// Do nothing as HTTP is stateless
	}

	// ------------------------------
	// Overrides
	// ------------------------------

	/// Transactions are not supported on HTTP RPC context
	async fn begin(&self, _txn: Option<Uuid>, _session_id: Uuid) -> Result<DbResult, TypesError> {
		Err(method_not_found(Method::Begin.to_string()))
	}

	/// Transactions are not supported on HTTP RPC context
	async fn commit(
		&self,
		_txn: Option<Uuid>,
		_session_id: Uuid,
		_params: Array,
	) -> Result<DbResult, TypesError> {
		Err(method_not_found(Method::Commit.to_string()))
	}

	/// Transactions are not supported on HTTP RPC context
	async fn cancel(
		&self,
		_txn: Option<Uuid>,
		_session_id: Uuid,
		_params: Array,
	) -> Result<DbResult, TypesError> {
		Err(method_not_found(Method::Cancel.to_string()))
	}
}
