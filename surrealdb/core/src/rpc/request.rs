use std::str::FromStr;

use surrealdb_types::Error as TypesError;

use crate::rpc::{Method, invalid_request};
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
	pub fn from_object(mut obj: PublicObject) -> Result<Self, TypesError> {
		// Fetch the 'version' argument
		let version = match obj.remove(VERSION) {
			None | Some(PublicValue::None | PublicValue::Null) => None,
			Some(PublicValue::Number(v)) => match v {
				PublicNumber::Int(1) => Some(1),
				PublicNumber::Int(2) => Some(2),
				_ => return Err(invalid_request()),
			},
			_ => return Err(invalid_request()),
		};

		// Fetch the 'txn' argument
		let txn = match obj.remove(TXN) {
			None | Some(PublicValue::None | PublicValue::Null) => None,
			Some(PublicValue::Uuid(x)) => Some(x),
			Some(PublicValue::String(x)) => {
				Some(PublicUuid::from_str(x.as_str()).map_err(|_| invalid_request())?)
			}
			_ => return Err(invalid_request()),
		};

		// Fetch the 'method' argument
		let method = match obj.remove(METHOD) {
			Some(PublicValue::String(v)) => v,
			_ => return Err(invalid_request()),
		};
		// Fetch the 'params' argument
		let params = match obj.remove(PARAMS) {
			Some(PublicValue::Array(v)) => v,
			_ => PublicArray::new(),
		};
		// Parse the specified method
		let method = Method::parse_case_sensitive(method);

		let id = Request::extract_id(&mut obj)?;
		let session_id = Request::extract_session(&mut obj)?;

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

	pub fn extract_id(obj: &mut PublicObject) -> Result<Option<PublicValue>, TypesError> {
		let id = obj.remove(ID);
		match id {
			None | Some(PublicValue::None) => Ok(None),
			Some(
				PublicValue::Null
				| PublicValue::Uuid(_)
				| PublicValue::Number(_)
				| PublicValue::String(_)
				| PublicValue::Datetime(_),
			) => Ok(id),
			_ => Err(invalid_request()),
		}
	}

	pub fn extract_session(obj: &mut PublicObject) -> Result<Option<PublicUuid>, TypesError> {
		match obj.remove(SESSION_ID) {
			None | Some(PublicValue::None | PublicValue::Null) => Ok(None),
			Some(PublicValue::Uuid(x)) => Ok(Some(x)),
			Some(PublicValue::String(x)) => {
				Ok(Some(PublicUuid::from_str(x.as_str()).map_err(|_| invalid_request())?))
			}
			_ => Err(invalid_request()),
		}
	}
}
