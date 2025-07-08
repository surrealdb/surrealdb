
use crate::dbs::{Failure, Notification};
use crate::rpc::protocol::v1;
use crate::rpc::protocol::v1::types::{V1QueryResponse, V1Value};
use revision::revisioned;
use serde::Serialize;

#[revisioned(revision = 1)]
#[derive(Debug, Serialize)]
pub struct V1Response {
	pub id: Option<V1Value>,
	pub result: Result<V1Data, Failure>,
}

impl V1Response {
	/// Create a JSON RPC result response
	pub fn success<T: Into<V1Data>>(id: Option<V1Value>, data: T) -> V1Response {
		V1Response {
			id,
			result: Ok(data.into()),
		}
	}

	/// Create a JSON RPC failure response
	pub fn failure(id: Option<V1Value>, err: Failure) -> V1Response {
		V1Response {
			id,
			result: Err(err),
		}
	}
}

impl From<V1Response> for V1Value {
	fn from(resp: V1Response) -> Self {
		let mut value = match resp.result {
			Ok(val) => match V1Value::try_from(val) {
				Ok(v) => map! {"result".to_string() => v},
				Err(e) => map!("error".to_string() => V1Value::from(e.to_string())),
			},
			Err(err) => map! {
				"error".to_string() => V1Value::from(err),
			},
		};
		if let Some(id) = resp.id {
			value.insert("id".to_string(), id);
		}
		value.into()
	}
}

pub trait IntoRpcResponse {
	fn into_response(self, id: Option<V1Value>) -> V1Response;
}

impl<T, E> IntoRpcResponse for Result<T, E>
where
	T: Into<V1Data>,
	E: Into<Failure>,
{
	fn into_response(self, id: Option<V1Value>) -> V1Response {
		match self {
			Ok(v) => V1Response::success(id, v.into()),
			Err(err) => V1Response::failure(id, err.into()),
		}
	}
}

/// The data returned by the database
// The variants here should be in exactly the same order as `crate::engine::remote::ws::Data`
// In future, they will possibly be merged to avoid having to keep them in sync.
#[revisioned(revision = 1)]
#[derive(Debug, Serialize)]
#[non_exhaustive]
pub enum V1Data {
	/// Generally methods return a `expr::Value`
	Other(V1Value),
	/// The query methods, `query` and `query_with` return a `Vec` of responses
	Query(Vec<V1QueryResponse>),
	/// Live queries return a notification
	Live(Notification),
	// Add new variants here
}

impl From<V1Value> for V1Data {
	fn from(v: V1Value) -> Self {
		V1Data::Other(v)
	}
}

impl From<String> for V1Data {
	fn from(v: String) -> Self {
		V1Data::Other(V1Value::from(v))
	}
}

impl From<Notification> for V1Data {
	fn from(n: Notification) -> Self {
		V1Data::Live(n)
	}
}

impl From<Vec<V1QueryResponse>> for V1Data {
	fn from(v: Vec<V1QueryResponse>) -> Self {
		V1Data::Query(v)
	}
}

impl TryFrom<V1Data> for V1Value {
	type Error = anyhow::Error;

	fn try_from(val: V1Data) -> Result<Self, Self::Error> {
		match val {
			V1Data::Query(v) => v1::to_value(v),
			V1Data::Live(v) => v1::to_value(v),
			V1Data::Other(v) => Ok(v),
		}
	}
}

impl TryFrom<Option<&crate::expr::Value>> for V1Data {
	type Error = anyhow::Error;

	#[inline]
	fn try_from(val: Option<&crate::expr::Value>) -> Result<Self, Self::Error> {
		match val {
			Some(v) => Ok(V1Data::Other(v.clone().try_into()?)),
			None => Ok(V1Data::Other(V1Value::None)),
		}
	}
}
