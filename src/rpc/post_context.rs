use std::collections::BTreeMap;
use std::sync::Arc;

use crate::cnf::{PKG_NAME, PKG_VERSION};
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_core::rpc::Data;
use surrealdb_core::rpc::RpcContext;
use surrealdb_core::rpc::RpcError;
use surrealdb_core::sql::Array;
use surrealdb_core::sql::Value;

#[cfg(surrealdb_unstable)]
use surrealdb_core::gql::{Pessimistic, SchemaCache};

pub struct PostRpcContext {
	pub kvs: Arc<Datastore>,
	pub session: Session,
	pub vars: BTreeMap<String, Value>,
	#[cfg(surrealdb_unstable)]
	pub gql_schema: SchemaCache<Pessimistic>,
}

impl PostRpcContext {
	pub fn new(kvs: &Arc<Datastore>, session: Session, vars: BTreeMap<String, Value>) -> Self {
		Self {
			kvs: kvs.clone(),
			session,
			vars,
			#[cfg(surrealdb_unstable)]
			gql_schema: SchemaCache::new(kvs.clone()),
		}
	}
}

impl RpcContext for PostRpcContext {
	/// The datastore for this RPC interface
	fn kvs(&self) -> &Datastore {
		&self.kvs
	}
	/// The current session for this RPC context
	fn session(&self) -> &Session {
		&self.session
	}
	/// Mutable access to the current session for this RPC context
	fn session_mut(&mut self) -> &mut Session {
		&mut self.session
	}
	/// The current parameters stored on this RPC context
	fn vars(&self) -> &BTreeMap<String, Value> {
		&self.vars
	}
	/// Mutable access to the current parameters stored on this RPC context
	fn vars_mut(&mut self) -> &mut BTreeMap<String, Value> {
		&mut self.vars
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

	// ------------------------------
	// Overrides
	// ------------------------------

	/// Parameters can't be set or unset on HTTP RPC context
	async fn set(&mut self, _params: Array) -> Result<Data, RpcError> {
		Err(RpcError::MethodNotFound)
	}

	/// Parameters can't be set or unset on HTTP RPC context
	async fn unset(&mut self, _params: Array) -> Result<Data, RpcError> {
		Err(RpcError::MethodNotFound)
	}
}
