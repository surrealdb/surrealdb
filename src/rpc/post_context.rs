use std::collections::BTreeMap;
use std::sync::Arc;

use crate::cnf::{PKG_NAME, PKG_VERSION};
use surrealdb::rpc::Data;
use surrealdb::rpc::RpcContext;
use surrealdb::rpc::RpcError;
use surrealdb_core::dbs::Session;
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_core::kvs::Datastore;
use surrealdb_core::rpc::args::Take;
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
	fn kvs(&self) -> &Datastore {
		&self.kvs
	}

	fn session(&self) -> &Session {
		&self.session
	}

	fn session_mut(&mut self) -> &mut Session {
		&mut self.session
	}

	fn vars(&self) -> &BTreeMap<String, Value> {
		&self.vars
	}

	fn vars_mut(&mut self) -> &mut BTreeMap<String, Value> {
		&mut self.vars
	}

	fn version_data(&self) -> Data {
		Value::from(format!("{PKG_NAME}-{}", *PKG_VERSION)).into()
	}

	#[cfg(surrealdb_unstable)]
	const GQL_SUPPORT: bool = true;
	#[cfg(surrealdb_unstable)]
	fn graphql_schema_cache(&self) -> &SchemaCache {
		&self.gql_schema
	}

	// disable:

	// doesn't do anything so shouldn't be supported
	async fn set(&mut self, _params: Array) -> Result<Data, RpcError> {
		Err(RpcError::MethodNotFound)
	}

	// doesn't do anything so shouldn't be supported
	async fn unset(&mut self, _params: Array) -> Result<Data, RpcError> {
		Err(RpcError::MethodNotFound)
	}
}
