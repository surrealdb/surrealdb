use std::str::FromStr;

use crate::rpc::{Method, RpcError};
use crate::types::{PublicArray, PublicNumber, PublicObject, PublicUuid, PublicValue};

pub static ID: &str = "id";
pub static METHOD: &str = "method";
pub static PARAMS: &str = "params";
pub static VERSION: &str = "version";
pub static TXN: &str = "txn";
pub static SESSION_ID: &str = "session";

#[derive(Debug)]
pub struct Request {
	pub id: Option<PublicValue>,
	pub version: Option<u8>,
	pub session_id: Option<PublicUuid>,
	pub txn: Option<PublicUuid>,
	pub method: Method,
	pub params: PublicArray,
}

impl Request {
	/// Create a request by extracting the request fields from an surealql
	/// object.
	pub fn from_object(mut obj: PublicObject) -> Result<Self, RpcError> {
		// Fetch the 'id' argument

		let id = obj.remove("id");
		let id = match id {
			None | Some(PublicValue::None) => None,
			Some(
				PublicValue::Null
				| PublicValue::Uuid(_)
				| PublicValue::Number(_)
				| PublicValue::String(_)
				| PublicValue::Datetime(_),
			) => id,
			_ => return Err(RpcError::InvalidRequest),
		};

		// Fetch the 'version' argument
		let version = match obj.remove(VERSION) {
			None | Some(PublicValue::None | PublicValue::Null) => None,
			Some(PublicValue::Number(v)) => match v {
				PublicNumber::Int(1) => Some(1),
				PublicNumber::Int(2) => Some(2),
				_ => return Err(RpcError::InvalidRequest),
			},
			_ => return Err(RpcError::InvalidRequest),
		};

		// Fetch the 'txn' argument
		let session_id = match obj.remove(SESSION_ID) {
			None | Some(PublicValue::None | PublicValue::Null) => None,
			Some(PublicValue::Uuid(x)) => Some(x),
			Some(PublicValue::String(x)) => {
				Some(PublicUuid::from_str(x.as_str()).map_err(|_| RpcError::InvalidRequest)?)
			}
			_ => return Err(RpcError::InvalidRequest),
		};

		// Fetch the 'txn' argument
		let txn = match obj.remove(TXN) {
			None | Some(PublicValue::None | PublicValue::Null) => None,
			Some(PublicValue::Uuid(x)) => Some(x),
			Some(PublicValue::String(x)) => {
				Some(PublicUuid::from_str(x.as_str()).map_err(|_| RpcError::InvalidRequest)?)
			}
			_ => return Err(RpcError::InvalidRequest),
		};

		// Fetch the 'method' argument
		let method = match obj.remove(METHOD) {
			Some(PublicValue::String(v)) => v,
			_ => return Err(RpcError::InvalidRequest),
		};
		// Fetch the 'params' argument
		let params = match obj.remove(PARAMS) {
			Some(PublicValue::Array(v)) => v,
			_ => PublicArray::new(),
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
			session_id,
		})
	}
}
