use crate::rpc::RpcError;
use crate::val::Value;

pub fn encode(value: &Value) -> Result<Vec<u8>, RpcError> {
	let mut res = Vec::new();
	bincode::serialize_into(&mut res, value).map_err(|e| RpcError::Serialize(e.to_string()))?;
	Ok(res)
}

pub fn decode(value: &[u8]) -> Result<Value, RpcError> {
	bincode::deserialize_from(value).map_err(|e| RpcError::Deserialize(e.to_string()))
}
