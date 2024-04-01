use crate::rpc::request::Request;
use crate::rpc::RpcError;
use crate::sql;
use crate::sql::Value;

pub fn req(val: &[u8]) -> Result<Request, RpcError> {
	sql::value(std::str::from_utf8(val).or(Err(RpcError::ParseError))?)
		.or(Err(RpcError::ParseError))?
		.try_into()
}

pub fn res(res: Value) -> Result<Vec<u8>, RpcError> {
	// Convert the response into simplified JSON
	let val = res.into_json();
	// Serialize the response with simplified type information
	let res = serde_json::to_string(&val).unwrap();
	// Return the message length, and message as binary
	Ok(res.into())
}
