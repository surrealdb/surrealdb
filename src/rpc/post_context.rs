use std::collections::BTreeMap;

use crate::cnf::{PKG_NAME, PKG_VERSION};
use surrealdb::dbs::Session;
use surrealdb::kvs::Datastore;
use surrealdb::rpc::args::Take;
use surrealdb::rpc::Data;
use surrealdb::rpc::RpcContext;
use surrealdb::rpc::RpcError;
use surrealdb::sql::Array;
use surrealdb::sql::Value;

pub struct PostRpcContext<'a> {
	pub kvs: &'a Datastore,
	pub session: Session,
	pub vars: BTreeMap<String, Value>,
}

impl<'a> PostRpcContext<'a> {
	pub fn new(kvs: &'a Datastore, session: Session, vars: BTreeMap<String, Value>) -> Self {
		Self {
			kvs,
			session,
			vars,
		}
	}
}

impl RpcContext for PostRpcContext<'_> {
	fn kvs(&self) -> &Datastore {
		self.kvs
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

	// reimplimentaions:

	async fn signup(&mut self, params: Array) -> Result<impl Into<Data>, RpcError> {
		let Ok(Value::Object(v)) = params.needs_one() else {
			return Err(RpcError::InvalidParams);
		};
		let out: Result<Value, RpcError> =
			surrealdb::iam::signup::signup(self.kvs, &mut self.session, v)
				.await
				.map(Into::into)
				.map_err(Into::into);

		out
	}

	async fn signin(&mut self, params: Array) -> Result<impl Into<Data>, RpcError> {
		let Ok(Value::Object(v)) = params.needs_one() else {
			return Err(RpcError::InvalidParams);
		};
		let out: Result<Value, RpcError> =
			surrealdb::iam::signin::signin(self.kvs, &mut self.session, v)
				.await
				.map(Into::into)
				.map_err(Into::into);
		out
	}

	async fn authenticate(&mut self, params: Array) -> Result<impl Into<Data>, RpcError> {
		let Ok(Value::Strand(token)) = params.needs_one() else {
			return Err(RpcError::InvalidParams);
		};
		surrealdb::iam::verify::token(self.kvs, &mut self.session, &token.0).await?;
		Ok(Value::None)
	}
}
