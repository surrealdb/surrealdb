use crate::rpc::RpcError;
use crate::rpc::protocol::v1::types::V1Value;
use crate::rpc::request::V1Request;
use crate::syn;

mod convert;

use super::ResTrait;

pub fn parse_value(val: &[u8]) -> Result<V1Value, RpcError> {
	let value = syn::value_legacy_strand(
		std::str::from_utf8(val).map_err(|err| RpcError::ParseError(err.to_string()))?,
	)
	.map_err(|err| RpcError::ParseError(err.to_string()))?;

	V1Value::try_from(value).map_err(|v: anyhow::Error| RpcError::Thrown(v.to_string()))
}

pub fn req(val: &[u8]) -> Result<V1Request, RpcError> {
	parse_value(val)?.try_into()
}

pub fn res(res: impl ResTrait) -> Result<Vec<u8>, RpcError> {
	// Convert the response into simplified JSON
	let val: V1Value = res.into();
	let val = val.into_json();
	// Serialize the response with simplified type information
	let res = serde_json::to_string(&val).unwrap();
	// Return the message length, and message as binary
	Ok(res.into())
}
