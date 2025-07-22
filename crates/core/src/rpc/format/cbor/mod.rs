mod convert;

use crate::rpc::RpcError;
use crate::rpc::protocol::v1::types::V1Value;
use crate::rpc::request::V1Request;
pub use ciborium::Value as CborValue;

use super::ResTrait;

pub fn parse_value(val: Vec<u8>) -> Result<V1Value, RpcError> {
	let cbor = ciborium::from_reader::<CborValue, _>(&mut val.as_slice())
		.map_err(|err| RpcError::ParseError(err.to_string()))?;

	V1Value::from_cbor(cbor).map_err(|v| RpcError::Thrown(v.to_string()))
}

pub fn req(val: Vec<u8>) -> Result<V1Request, RpcError> {
	parse_value(val)?.try_into()
}

pub fn res(res: impl ResTrait) -> Result<Vec<u8>, RpcError> {
	// Convert the response into a value
	let val: V1Value = res.into();
	let val = val.into_cbor()?;
	// Create a new vector for encoding output
	let mut res = Vec::new();
	// Serialize the value into CBOR binary data
	ciborium::into_writer(&val, &mut res).unwrap();
	// Return the message length, and message as binary
	Ok(res)
}
