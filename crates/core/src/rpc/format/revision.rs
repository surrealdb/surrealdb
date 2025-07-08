use crate::rpc::RpcError;
use crate::rpc::format::ResTrait;
use crate::rpc::protocol::v1::types::V1Value;
use crate::rpc::request::V1Request;
use crate::sql::SqlValue;
use revision::Revisioned;

pub fn parse_value(val: Vec<u8>) -> Result<V1Value, RpcError> {
	V1Value::deserialize_revisioned(&mut val.as_slice()).map_err(|_| RpcError::ParseError)
}

pub fn req(val: Vec<u8>) -> Result<V1Request, RpcError> {
	parse_value(val)?.try_into()
}

pub fn res(res: impl ResTrait) -> Result<Vec<u8>, RpcError> {
	// Serialize the response with full internal type information
	let mut buf = Vec::new();
	res.serialize_revisioned(&mut buf).unwrap();
	Ok(buf)
}
