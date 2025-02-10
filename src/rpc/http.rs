use crate::cnf::{PKG_NAME, PKG_VERSION};
use std::sync::Arc;
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_core::rpc::Data;
use surrealdb_core::rpc::RpcContext;
use surrealdb_core::rpc::RpcError;
use surrealdb_core::rpc::RpcProtocolV1;
use surrealdb_core::rpc::RpcProtocolV2;
use surrealdb_core::sql::Array;
use tokio::sync::Semaphore;

#[cfg(surrealdb_unstable)]
use surrealdb_core::gql::{Pessimistic, SchemaCache};

pub struct Http {
	pub kvs: Arc<Datastore>,
	pub lock: Arc<Semaphore>,
	pub session: Arc<Session>,
	#[cfg(surrealdb_unstable)]
	pub gql_schema: SchemaCache<Pessimistic>,
}

impl Http {
	pub fn new(kvs: &Arc<Datastore>, session: Session) -> Self {
		Self {
			kvs: kvs.clone(),
			lock: Arc::new(Semaphore::new(1)),
			session: Arc::new(session),
			#[cfg(surrealdb_unstable)]
			gql_schema: SchemaCache::new(kvs.clone()),
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
	fn version_data(&self) -> Data {
		format!("{PKG_NAME}-{}", *PKG_VERSION).into()
	}

	// ------------------------------
	// Realtime
	// ------------------------------

	/// Live queries are disabled on HTTP
	const LQ_SUPPORT: bool = false;

	// ------------------------------
	// GraphQL
	// ------------------------------

	/// GraphQL queries are enabled on HTTP
	#[cfg(surrealdb_unstable)]
	const GQL_SUPPORT: bool = true;

	#[cfg(surrealdb_unstable)]
	fn graphql_schema_cache(&self) -> &SchemaCache {
		&self.gql_schema
	}
}

impl RpcProtocolV1 for Http {
	/// Parameters can't be set or unset on HTTP RPC context
	async fn set(&self, _params: Array) -> Result<Data, RpcError> {
		Err(RpcError::MethodNotFound)
	}

	/// Parameters can't be set or unset on HTTP RPC context
	async fn unset(&self, _params: Array) -> Result<Data, RpcError> {
		Err(RpcError::MethodNotFound)
	}
}

impl RpcProtocolV2 for Http {
	/// Parameters can't be set or unset on HTTP RPC context
	async fn set(&self, _params: Array) -> Result<Data, RpcError> {
		Err(RpcError::MethodNotFound)
	}

	/// Parameters can't be set or unset on HTTP RPC context
	async fn unset(&self, _params: Array) -> Result<Data, RpcError> {
		Err(RpcError::MethodNotFound)
	}
}
