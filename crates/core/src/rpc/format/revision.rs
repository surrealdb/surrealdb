use crate::rpc::RpcError;
use crate::val::Value;

pub fn decode(val: &[u8]) -> Result<Value, RpcError> {
	revision::from_slice(val).map_err(|_| RpcError::ParseError)
}

pub fn encode(val: &Value) -> Result<Vec<u8>, RpcError> {
	revision::to_vec(val).map_err(|_| RpcError::ParseError)
}
