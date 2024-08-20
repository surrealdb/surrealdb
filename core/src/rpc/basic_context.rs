use std::collections::BTreeMap;

use crate::{dbs::Session, kvs::Datastore, rpc::RpcContext, sql::Value};

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

	fn version_data(&self) -> super::Data {
		Value::Strand(self.version_string.clone().into()).into()
	}
}
