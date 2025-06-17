use crate::expr::serde::{deserialize, serialize};
use crate::rpc::RpcError;
use crate::rpc::request::Request;
use crate::sql::SqlValue;
use crate::protocol::surrealdb::value::Value as ValueProto;
use prost::Message;

pub fn parse_value(val: &[u8]) -> Result<SqlValue, RpcError> {
	deserialize::<SqlValue>(val).map_err(|_| RpcError::ParseError)
}

pub fn req(val: &[u8]) -> Result<Request, RpcError> {
	Request::decode(val)
		.map_err(|_| RpcError::InvalidRequest)
}

pub fn res(res: ValueProto) -> Result<Vec<u8>, RpcError> {
	// Serialize the response with full internal type information
	let value: ValueProto = res.into();
	Ok(serialize(&value).unwrap())
}
