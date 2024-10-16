use crate::rpc::format::ResTrait;
use crate::rpc::request::Request;
use crate::rpc::RpcError;
use crate::sql::serde::{deserialize, serialize};
use crate::sql::Value;

pub fn req(val: &[u8]) -> Result<Request, RpcError> {
	deserialize::<Value>(val).map_err(|_| RpcError::ParseError)?.try_into()
}

pub fn res(res: impl ResTrait) -> Result<Vec<u8>, RpcError> {
	// Serialize the response with full internal type information
	Ok(serialize(&res).unwrap())
}
