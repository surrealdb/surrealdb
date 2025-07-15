mod convert;

pub use convert::Cbor;

use crate::rpc::RpcError;
use crate::rpc::protocol::v1::types::V1Value;
use crate::rpc::request::V1Request;
use ciborium::Value as Data;

use super::ResTrait;

pub fn parse_value(val: Vec<u8>) -> Result<V1Value, RpcError> {
	let cbor = ciborium::from_reader::<Data, _>(&mut val.as_slice())
		.map_err(|_| RpcError::ParseError)
		.map(Cbor)?;

	V1Value::try_from(cbor).map_err(|v: &str| RpcError::Thrown(v.into()))
}

pub fn req(val: Vec<u8>) -> Result<V1Request, RpcError> {
	parse_value(val)?.try_into()
}

pub fn res(res: impl ResTrait) -> Result<Vec<u8>, RpcError> {
	// Convert the response into a value
	let val: V1Value = res.into();
	let val: Cbor = val.try_into()?;
	// Create a new vector for encoding output
	let mut res = Vec::new();
	// Serialize the value into CBOR binary data
	ciborium::into_writer(&val.0, &mut res).unwrap();
	// Return the message length, and message as binary
	Ok(res)
}
