mod convert;

use std::io::Cursor;

use bytes::Bytes;

use crate::expr::Value;
use crate::rpc::RpcError;
use crate::rpc::request::Request;
pub use ciborium::Value as CborValue;

use super::ResTrait;

pub fn parse_value(val: &Bytes) -> Result<Value, RpcError> {
	let mut cursor = Cursor::new(val);
	let cbor =
		ciborium::from_reader::<CborValue, _>(&mut cursor).map_err(|_| RpcError::ParseError)?;

	Value::try_from(cbor).map_err(|v| RpcError::Thrown(v.to_string()))
}

pub fn parse_request(val: &Bytes) -> Result<Request, RpcError> {
	let mut cursor = Cursor::new(val);
	let cbor =
		ciborium::from_reader::<CborValue, _>(&mut cursor).map_err(|_| RpcError::ParseError)?;

	Request::try_from(cbor).map_err(|v| RpcError::Thrown(v.to_string()))
}

pub fn res(res: impl ResTrait) -> Result<Vec<u8>, RpcError> {
	// Convert the response into a value
	let val: Value = res.into();
	let val: CborValue = val.try_into()?;
	// Create a new vector for encoding output
	let mut res = Vec::new();
	// Serialize the value into CBOR binary data
	ciborium::into_writer(&val, &mut res).unwrap();
	// Return the message length, and message as binary
	Ok(res)
}
