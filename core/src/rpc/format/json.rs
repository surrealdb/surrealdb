use crate::rpc::request::Request;
use crate::rpc::RpcError;
use crate::sql::Value;
use crate::syn;

use super::ResTrait;

pub fn req(val: &[u8]) -> Result<Request, RpcError> {
	syn::value_legacy_strand(std::str::from_utf8(val).or(Err(RpcError::ParseError))?)
		.or(Err(RpcError::ParseError))?
		.try_into()
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
