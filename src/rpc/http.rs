use std::sync::Arc;

use surrealdb::types::{Array, Value};
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_core::rpc::{DbResult, RpcContext, RpcError, RpcProtocolV1};
use tokio::sync::Semaphore;

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

impl RpcContext for Http {
	/// The datastore for this RPC interface
	fn kvs(&self) -> &Datastore {
		&self.kvs
	}
	/// Retrieves the modification lock for this RPC context
	fn lock(&self) -> Arc<Semaphore> {
		self.lock.clone()
	}
	/// The current session for this RPC context
	fn session(&self) -> Arc<Session> {
		self.session.clone()
	}
	/// Mutable access to the current session for this RPC context
	fn set_session(&self, _session: Arc<Session>) {
		// Do nothing as HTTP is stateless
	}
	/// The version information for this RPC context
	fn version_data(&self) -> DbResult {
		let value = Value::String(format!("{PKG_NAME}-{}", *PKG_VERSION));
		DbResult::Other(value)
	}

	// ------------------------------
	// Realtime
	// ------------------------------

	/// Live queries are disabled on HTTP
	const LQ_SUPPORT: bool = false;

	/// Handles the cleanup of live queries
	async fn cleanup_lqs(&self) {
		// Do nothing as HTTP is stateless
	}

	// ------------------------------
	// GraphQL
	// ------------------------------

	// GraphQL queries are enabled on HTTP
	//const GQL_SUPPORT: bool = true;

	/*
	fn graphql_schema_cache(&self) -> &SchemaCache {
		&self.gql_schema
	}
	*/
}

impl RpcProtocolV1 for Http {
	/// Parameters can't be set or unset on HTTP RPC context
	async fn set(&self, _params: Array) -> Result<DbResult, RpcError> {
		Err(RpcError::MethodNotFound)
	}

	/// Parameters can't be set or unset on HTTP RPC context
	async fn unset(&self, _params: Array) -> Result<DbResult, RpcError> {
		Err(RpcError::MethodNotFound)
	}
}
