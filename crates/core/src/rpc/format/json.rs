use bytes::Bytes;

use crate::expr::Value;
use crate::rpc::RpcError;
use crate::rpc::request::Request;
use crate::syn;

use super::ResTrait;

pub fn parse_value(val: &Bytes) -> Result<Value, RpcError> {
	syn::value_legacy_strand(std::str::from_utf8(val).or(Err(RpcError::ParseError))?)
		.or(Err(RpcError::ParseError))
		.map(Into::into)
}

pub fn req(val: &Bytes) -> Result<Request, RpcError> {
	let request: Request = serde_json::from_slice(val).map_err(|_| RpcError::ParseError)?;

	Ok(request)
}

pub fn res(res: impl ResTrait) -> Result<Vec<u8>, RpcError> {
	// Convert the response into simplified JSON
	let val: Value = res.into();
	let val = val.into_json();
	// Serialize the response with simplified type information
	let res = serde_json::to_string(&val).unwrap();
	// Return the message length, and message as binary
	Ok(res.into())
}
