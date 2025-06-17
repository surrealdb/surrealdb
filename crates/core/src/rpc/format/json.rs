use crate::expr::Value;
use crate::rpc::RpcError;
use crate::rpc::request::Request;
use crate::sql::SqlValue;
use crate::syn;
use crate::proto::surrealdb::value::Value as ValueProto;

pub fn parse_value(val: &[u8]) -> Result<SqlValue, RpcError> {
	// syn::value_legacy_strand(std::str::from_utf8(val).or(Err(RpcError::ParseError))?)
	// 	.or(Err(RpcError::ParseError))
	serde_json::from_slice(val)
		.map_err(|_| RpcError::ParseError)
		
}

pub fn req(val: &[u8]) -> Result<Request, RpcError> {
	serde_json::from_slice(val)
		.map_err(|_| RpcError::InvalidRequest)
		
}

pub fn res(val: ValueProto) -> Result<Vec<u8>, RpcError> {
	// Serialize the response with simplified type information
	let res = serde_json::to_vec(&val).unwrap();
	// Return the message length, and message as binary
	Ok(res)
}
