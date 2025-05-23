use std::io::Cursor;

use crate::expr::Value;
use crate::rpc::RpcError;
use crate::rpc::format::ResTrait;
use crate::rpc::request::Request;
use bytes::Bytes;
use revision::Revisioned;

pub fn parse_value(val: &Bytes) -> Result<Value, RpcError> {
	let mut cursor = Cursor::new(val);
	Value::deserialize_revisioned(&mut cursor).map_err(|_| RpcError::ParseError)
}

pub fn req(val: &Bytes) -> Result<Request, RpcError> {
	let mut cursor = Cursor::new(val);
	Request::deserialize_revisioned(&mut cursor).map_err(|_| RpcError::ParseError)
}

pub fn res(res: impl ResTrait) -> Result<Vec<u8>, RpcError> {
	// Serialize the response with full internal type information
	let mut buf = Vec::new();
	res.serialize_revisioned(&mut buf).unwrap();
	Ok(buf)
}
