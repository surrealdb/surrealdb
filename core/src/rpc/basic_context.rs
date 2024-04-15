use std::collections::BTreeMap;

use crate::{
	dbs::Session,
	kvs::Datastore,
	rpc::RpcContext,
	sql::{Array, Value},
};

use super::{args::Take, Data, RpcError};

#[non_exhaustive]
pub struct BasicRpcContext<'a> {
	pub kvs: &'a Datastore,
	pub session: Session,
	pub vars: BTreeMap<String, Value>,
	pub version_string: String,
}

impl<'a> BasicRpcContext<'a> {
	pub fn new(
		kvs: &'a Datastore,
		session: Session,
		vars: BTreeMap<String, Value>,
		version_string: String,
	) -> Self {
		Self {
			kvs,
			session,
			vars,
			version_string,
		}
	}
}

impl RpcContext for BasicRpcContext<'_> {
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

	fn version_data(&self) -> impl Into<super::Data> {
		Value::Strand(self.version_string.clone().into())
	}

	// reimplimentaions:

	async fn signup(&mut self, params: Array) -> Result<impl Into<Data>, RpcError> {
		let Ok(Value::Object(v)) = params.needs_one() else {
			return Err(RpcError::InvalidParams);
		};
		let out: Result<Value, RpcError> =
			crate::iam::signup::signup(self.kvs, &mut self.session, v)
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
			crate::iam::signin::signin(self.kvs, &mut self.session, v)
				.await
				.map(Into::into)
				.map_err(Into::into);
		out
	}

	async fn authenticate(&mut self, params: Array) -> Result<impl Into<Data>, RpcError> {
		let Ok(Value::Strand(token)) = params.needs_one() else {
			return Err(RpcError::InvalidParams);
		};
		crate::iam::verify::token(self.kvs, &mut self.session, &token.0).await?;
		Ok(Value::None)
	}
}
