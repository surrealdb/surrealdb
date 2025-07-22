use crate::expr::serde::{deserialize, serialize};
use crate::rpc::RpcError;
use crate::rpc::format::ResTrait;
use crate::rpc::protocol::v1::types::V1Value;
use crate::rpc::request::V1Request;

pub fn parse_value(val: &[u8]) -> Result<V1Value, RpcError> {
	let val = deserialize::<V1Value>(val).map_err(|err| RpcError::ParseError(err.to_string()))?;
	Ok(val)
}

pub fn req(val: &[u8]) -> Result<V1Request, RpcError> {
	parse_value(val)?.try_into()
}

pub fn res(res: impl ResTrait) -> Result<Vec<u8>, RpcError> {
	// Serialize the response with full internal type information
	Ok(serialize(&res).unwrap())
}
