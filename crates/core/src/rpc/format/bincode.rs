use bytes::Bytes;

use crate::expr::Value;
use crate::expr::serde::{deserialize, serialize};
use crate::rpc::RpcError;
use crate::rpc::format::ResTrait;
use crate::rpc::request::Request;

pub fn parse_value(val: &Bytes) -> Result<Value, RpcError> {
	deserialize::<Value>(val).map_err(|_| RpcError::ParseError)
}

pub fn req(val: &Bytes) -> Result<Request, RpcError> {
	deserialize::<Request>(val).map_err(|_| RpcError::ParseError)
}

pub fn res(res: impl ResTrait) -> Result<Vec<u8>, RpcError> {
	// Serialize the response with full internal type information
	Ok(serialize(&res).unwrap())
}
