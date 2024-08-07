use std::collections::BTreeMap;
use std::sync::Arc;

use crate::cnf::{PKG_NAME, PKG_VERSION};
use surrealdb::dbs::Session;
use surrealdb::gql::{Pessimistic, SchemaCache};
use surrealdb::kvs::Datastore;
use surrealdb::rpc::Data;
use surrealdb::rpc::RpcContext;
use surrealdb::rpc::RpcError;
use surrealdb::sql::Array;
use surrealdb::sql::Value;

pub struct PostRpcContext {
	pub kvs: Arc<Datastore>,
	pub session: Session,
	pub vars: BTreeMap<String, Value>,
	pub gql_schema: SchemaCache<Pessimistic>,
}

impl<'a> PostRpcContext {
	pub fn new(kvs: &Arc<Datastore>, session: Session, vars: BTreeMap<String, Value>) -> Self {
		Self {
			kvs: kvs.clone(),
			session,
			vars,
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

	fn version_data(&self) -> impl Into<Data> {
		let val: Value = format!("{PKG_NAME}-{}", *PKG_VERSION).into();
		val
	}

	const GQL_SUPPORT: bool = true;
	fn graphql_schema_cache(&self) -> &SchemaCache {
		&self.gql_schema
	}

	// disable:

	// doesn't do anything so shouldn't be supported
	async fn set(&mut self, _params: Array) -> Result<impl Into<Data>, RpcError> {
		let out: Result<Value, RpcError> = Err(RpcError::MethodNotFound);
		out
	}

	// doesn't do anything so shouldn't be supported
	async fn unset(&mut self, _params: Array) -> Result<impl Into<Data>, RpcError> {
		let out: Result<Value, RpcError> = Err(RpcError::MethodNotFound);
		out
	}
}
