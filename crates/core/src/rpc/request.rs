use crate::rpc::format::cbor::Cbor;
use crate::rpc::format::msgpack::Pack;
use crate::rpc::Method;
use crate::rpc::RpcError;
use crate::sql::Array;
use crate::sql::Number;
use crate::sql::Part;
use crate::sql::Value;
use std::sync::LazyLock;

pub static ID: LazyLock<[Part; 1]> = LazyLock::new(|| [Part::from("id")]);
pub static METHOD: LazyLock<[Part; 1]> = LazyLock::new(|| [Part::from("method")]);
pub static PARAMS: LazyLock<[Part; 1]> = LazyLock::new(|| [Part::from("params")]);
pub static VERSION: LazyLock<[Part; 1]> = LazyLock::new(|| [Part::from("version")]);

#[derive(Debug)]
pub struct Request {
	pub id: Option<Value>,
	pub version: Option<u8>,
	pub method: Method,
	pub params: Array,
}

impl TryFrom<Cbor> for Request {
	type Error = RpcError;
	fn try_from(val: Cbor) -> Result<Self, RpcError> {
		<Cbor as TryInto<Value>>::try_into(val).map_err(|_| RpcError::InvalidRequest)?.try_into()
	}
}

impl TryFrom<Pack> for Request {
	type Error = RpcError;
	fn try_from(val: Pack) -> Result<Self, RpcError> {
		<Pack as TryInto<Value>>::try_into(val).map_err(|_| RpcError::InvalidRequest)?.try_into()
	}
}

impl TryFrom<Value> for Request {
	type Error = RpcError;
	fn try_from(val: Value) -> Result<Self, RpcError> {
		// Fetch the 'id' argument
		let id = match val.pick(&*ID) {
			v if v.is_none() => None,
			v if v.is_null() => Some(v),
			v if v.is_uuid() => Some(v),
			v if v.is_number() => Some(v),
			v if v.is_strand() => Some(v),
			v if v.is_datetime() => Some(v),
			_ => return Err(RpcError::InvalidRequest),
		};
		// Fetch the 'version' argument
		let version = match val.pick(&*VERSION) {
			v if v.is_none() => None,
			v if v.is_null() => None,
			Value::Number(v) => match v {
				Number::Int(1) => Some(1),
				Number::Int(2) => Some(2),
				_ => return Err(RpcError::InvalidRequest),
			},
			_ => return Err(RpcError::InvalidRequest),
		};
		// Fetch the 'method' argument
		let method = match val.pick(&*METHOD) {
			Value::Strand(v) => v.to_raw(),
			_ => return Err(RpcError::InvalidRequest),
		};
		// Fetch the 'params' argument
		let params = match val.pick(&*PARAMS) {
			Value::Array(v) => v,
			_ => Array::new(),
		};
		// Parse the specified method
		let method = Method::parse_case_sensitive(&method);
		// Return the parsed request
		Ok(Request {
			id,
			method,
			params,
			version,
		})
	}
}
