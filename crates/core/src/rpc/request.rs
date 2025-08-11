use uuid::Uuid;

use crate::rpc::{Method, RpcError};
use crate::val::{Array, Number, Object, Value};

pub static ID: &str = "id";
pub static METHOD: &str = "method";
pub static PARAMS: &str = "params";
pub static VERSION: &str = "version";
pub static TXN: &str = "txn";

#[derive(Debug)]
pub struct Request {
	pub id: Option<Value>,
	pub version: Option<u8>,
	pub txn: Option<Uuid>,
	pub method: Method,
	pub params: Array,
}

impl Request {
	/// Create a request by extracting the request fields from an surealql
	/// object.
	pub fn from_object(mut obj: Object) -> Result<Self, RpcError> {
		// Fetch the 'id' argument

		let id = obj.remove("id");
		let id = match id {
			None | Some(Value::None) => None,
			Some(
				Value::Null
				| Value::Uuid(_)
				| Value::Number(_)
				| Value::Strand(_)
				| Value::Datetime(_),
			) => id,
			_ => return Err(RpcError::InvalidRequest),
		};

		// Fetch the 'version' argument
		let version = match obj.remove(VERSION) {
			None | Some(Value::None | Value::Null) => None,
			Some(Value::Number(v)) => match v {
				Number::Int(1) => Some(1),
				Number::Int(2) => Some(2),
				_ => return Err(RpcError::InvalidRequest),
			},
			_ => return Err(RpcError::InvalidRequest),
		};

		// Fetch the 'txn' argument
		let txn = match obj.remove(TXN) {
			None | Some(Value::None | Value::Null) => None,
			Some(Value::Uuid(x)) => Some(x.0),
			Some(Value::Strand(x)) => {
				Some(Uuid::try_parse(x.as_str()).map_err(|_| RpcError::InvalidRequest)?)
			}
			_ => return Err(RpcError::InvalidRequest),
		};

		// Fetch the 'method' argument
		let method = match obj.remove(METHOD) {
			Some(Value::Strand(v)) => v.into_string(),
			_ => return Err(RpcError::InvalidRequest),
		};
		// Fetch the 'params' argument
		let params = match obj.remove(PARAMS) {
			Some(Value::Array(v)) => v,
			_ => Array::new(),
		};
		// Parse the specified method
		let method = Method::parse_case_sensitive(method);
		// Return the parsed request
		Ok(Request {
			id,
			method,
			params,
			version,
			txn,
		})
	}
}
