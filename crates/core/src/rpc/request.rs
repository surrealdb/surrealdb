// use crate::rpc::Method;
// use crate::rpc::RpcError;
// use crate::rpc::format::cbor::Cbor;
// use crate::sql::Array;
// use crate::sql::Number;
// use crate::sql::SqlValue;

// pub static ID: &str = "id";
// pub static METHOD: &str = "method";
// pub static PARAMS: &str = "params";
// pub static VERSION: &str = "version";



// pub use crate::protocol::surrealdb::rpc::Request;

pub use crate::protocol::flatbuffers::surreal_db::protocol::rpc::Request;

// #[derive(Debug)]
// pub struct Request {
// 	pub id: Option<SqlValue>,
// 	pub version: Option<u8>,
// 	pub method: Method,
// 	pub params: Array,
// }

// impl TryFrom<Cbor> for Request {
// 	type Error = RpcError;
// 	fn try_from(val: Cbor) -> Result<Self, RpcError> {
// 		SqlValue::try_from(val).map_err(|_| RpcError::InvalidRequest)?.try_into()
// 	}
// }

// impl TryFrom<SqlValue> for Request {
// 	type Error = RpcError;
// 	fn try_from(val: SqlValue) -> Result<Self, RpcError> {
// 		// Fetch the 'id' argument
// 		let id = match val.get_field_value("id") {
// 			v if v.is_none() => None,
// 			v if v.is_null() => Some(v),
// 			v if v.is_uuid() => Some(v),
// 			v if v.is_number() => Some(v),
// 			v if v.is_strand() => Some(v),
// 			v if v.is_datetime() => Some(v),
// 			_ => return Err(RpcError::InvalidRequest),
// 		};

// 		// Fetch the 'version' argument
// 		let version = match val.get_field_value(VERSION) {
// 			v if v.is_none() => None,
// 			v if v.is_null() => None,
// 			SqlValue::Number(v) => match v {
// 				Number::Int(1) => Some(1),
// 				Number::Int(2) => Some(2),
// 				_ => return Err(RpcError::InvalidRequest),
// 			},
// 			_ => return Err(RpcError::InvalidRequest),
// 		};
// 		// Fetch the 'method' argument
// 		let method = match val.get_field_value(METHOD) {
// 			SqlValue::Strand(v) => v.to_raw(),
// 			_ => return Err(RpcError::InvalidRequest),
// 		};
// 		// Fetch the 'params' argument
// 		let params = match val.get_field_value(PARAMS) {
// 			SqlValue::Array(v) => v,
// 			_ => Array::new(),
// 		};
// 		// Parse the specified method
// 		let method = Method::parse_case_sensitive(method);
// 		// Return the parsed request
// 		Ok(Request {
// 			id,
// 			method,
// 			params,
// 			version,
// 		})
// 	}
// }
