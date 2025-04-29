use crate::rpc::format::ResTrait;
use crate::rpc::request::Request;
use crate::rpc::RpcError;
use crate::sql::Value;
use revision::Revisioned;

pub fn parse_value(val: Vec<u8>) -> Result<Value, RpcError> {
	Value::deserialize_revisioned(&mut val.as_slice()).map_err(|_| RpcError::ParseError)
}

pub fn req(val: Vec<u8>) -> Result<Request, RpcError> {
	parse_value(val)?.try_into()
}

pub fn res(res: impl ResTrait) -> Result<Vec<u8>, RpcError> {
	// Serialize the response with full internal type information
	let mut buf = Vec::new();
	res.serialize_revisioned(&mut buf).unwrap();
	Ok(buf)
}
