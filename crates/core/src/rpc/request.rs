use uuid::Uuid;

use crate::rpc::format::cbor::Cbor;
use crate::rpc::{Method, RpcError};
use crate::val::{Array, Number, Value};

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

impl TryFrom<Cbor> for Request {
	type Error = RpcError;
	fn try_from(val: Cbor) -> Result<Self, RpcError> {
		Value::try_from(val).map_err(|_| RpcError::InvalidRequest)?.try_into()
	}
}

impl TryFrom<Value> for Request {
	type Error = RpcError;
	fn try_from(val: Value) -> Result<Self, RpcError> {
		// Fetch the 'id' argument
		let id = match val.get_field_value("id") {
			v if v.is_none() => None,
			v if v.is_null() => Some(v),
			v if v.is_uuid() => Some(v),
			v if v.is_number() => Some(v),
			v if v.is_strand() => Some(v),
			v if v.is_datetime() => Some(v),
			_ => return Err(RpcError::InvalidRequest),
		};

		// Fetch the 'version' argument
		let version = match val.get_field_value(VERSION) {
			v if v.is_none() => None,
			v if v.is_null() => None,
			Value::Number(v) => match v {
				Number::Int(1) => Some(1),
				Number::Int(2) => Some(2),
				_ => return Err(RpcError::InvalidRequest),
			},
			_ => return Err(RpcError::InvalidRequest),
		};
		// Fetch the 'txn' argument
		let txn = match val.get_field_value(TXN) {
			Value::None => None,
			Value::Null => None,
			Value::Uuid(x) => Some(x.0),
			Value::Strand(x) => Some(Uuid::try_parse(&x.0).map_err(|_| RpcError::InvalidRequest)?),
			_ => return Err(RpcError::InvalidRequest),
		};
		// Fetch the 'method' argument
		let method = match val.get_field_value(METHOD) {
			Value::Strand(v) => v.to_raw(),
			_ => return Err(RpcError::InvalidRequest),
		};
		// Fetch the 'params' argument
		let params = match val.get_field_value(PARAMS) {
			Value::Array(v) => v,
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
