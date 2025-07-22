use crate::rpc::RpcError;
use crate::rpc::format::ResTrait;
use crate::rpc::request::Request;
use crate::val::Value;
use revision::Revisioned;

pub fn decode(val: &[u8]) -> Result<Value, RpcError> {
	revision::from_slice(val).map_err(|_| RpcError::ParseError)
}

pub fn encode(val: &Value) -> Result<Vec<u8>, RpcError> {
	revision::to_vec(val).map_err(|_| RpcError::ParseError)
}

#[deprecated]
pub fn parse_value(val: Vec<u8>) -> Result<Value, RpcError> {
	todo!()
}

#[deprecated]
pub fn req(val: Vec<u8>) -> Result<Request, RpcError> {
	todo!()
	//parse_value(val)?.try_into()
}

#[deprecated]
pub fn res(res: impl ResTrait) -> Result<Vec<u8>, RpcError> {
	todo!()
	/*
	// Serialize the response with full internal type information
	let mut buf = Vec::new();
	res.serialize_revisioned(&mut buf).unwrap();
	Ok(buf)
		*/
}
