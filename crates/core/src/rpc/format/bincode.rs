use crate::rpc::RpcError;
use crate::rpc::format::ResTrait;
use crate::rpc::request::Request;
use crate::sql::Value;
use crate::sql::serde::{deserialize, serialize};

pub fn parse_value(val: &[u8]) -> Result<Value, RpcError> {
	deserialize::<Value>(val).map_err(|_| RpcError::ParseError)
}

pub fn req(val: &[u8]) -> Result<Request, RpcError> {
	parse_value(val)?.try_into()
}

pub fn res(res: impl ResTrait) -> Result<Vec<u8>, RpcError> {
	// Serialize the response with full internal type information
	Ok(serialize(&res).unwrap())
}
