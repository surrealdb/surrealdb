use prost::Message;

use crate::expr::serde::{deserialize, serialize};
use crate::rpc::RpcError;
use crate::rpc::request::Request;
use crate::sql::SqlValue;
use crate::protocol::surrealdb::ast::SqlValue as SqlValueProto;
use crate::protocol::surrealdb::value::Value as ValueProto;

pub fn parse_value(val: &[u8]) -> Result<SqlValue, RpcError> {
	SqlValueProto::decode(val)
		.map_err(|_| RpcError::ParseError)
		.and_then(|v| SqlValue::try_from(v).map_err(|err| RpcError::Thrown(err.to_string())))
}

pub fn req(val: &[u8]) -> Result<Request, RpcError> {
	Request::decode(val)
		.map_err(|_| RpcError::InvalidRequest)
		
}

pub fn res(res: ValueProto) -> Result<Vec<u8>, RpcError> {
	// Serialize the response with full internal type information
	Ok(res.encode_to_vec())
}
