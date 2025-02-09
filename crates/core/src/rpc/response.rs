use crate::dbs;
use crate::dbs::Notification;
use crate::sql;
use crate::sql::Value;
use revision::revisioned;
use serde::Serialize;

/// The data returned by the database
// The variants here should be in exactly the same order as `crate::engine::remote::ws::Data`
// In future, they will possibly be merged to avoid having to keep them in sync.
#[revisioned(revision = 1)]
#[derive(Debug, Serialize)]
#[non_exhaustive]
pub enum RpcResponse {
	/// Generally methods return a `sql::Value`
	Other(Value),
	/// The query methods, `query` and `query_with` return a `Vec` of responses
	Query(Vec<dbs::Response>),
	/// Live queries return a notification
	Live(Notification),
	// Add new variants here
}

impl From<Value> for RpcResponse {
	fn from(v: Value) -> Self {
		RpcResponse::Other(v)
	}
}

impl From<String> for RpcResponse {
	fn from(v: String) -> Self {
		RpcResponse::Other(Value::from(v))
	}
}

impl From<Notification> for RpcResponse {
	fn from(n: Notification) -> Self {
		RpcResponse::Live(n)
	}
}

impl From<Vec<dbs::Response>> for RpcResponse {
	fn from(v: Vec<dbs::Response>) -> Self {
		RpcResponse::Query(v)
	}
}

impl From<RpcResponse> for Value {
	fn from(val: RpcResponse) -> Self {
		match val {
			RpcResponse::Query(v) => sql::to_value(v).unwrap(),
			RpcResponse::Live(v) => sql::to_value(v).unwrap(),
			RpcResponse::Other(v) => v,
		}
	}
}
