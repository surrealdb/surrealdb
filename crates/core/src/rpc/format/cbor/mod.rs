mod convert;

use crate::expr::Value;
use crate::rpc::RpcError;
use crate::rpc::request::Request;
use crate::sql::SqlValue;
use ciborium::Value as Data;
pub use convert::Cbor;

use super::ResTrait;

pub fn parse_value(val: Vec<u8>) -> Result<SqlValue, RpcError> {
	let cbor = ciborium::from_reader::<Data, _>(&mut val.as_slice())
		.map_err(|_| RpcError::ParseError)
		.map(Cbor)?;

	SqlValue::try_from(cbor).map_err(|v: &str| RpcError::Thrown(v.into()))
}

pub fn req(val: Vec<u8>) -> Result<Request, RpcError> {
	parse_value(val)?.try_into()
}

pub fn res(res: impl ResTrait) -> Result<Vec<u8>, RpcError> {
	// Convert the response into a value
	let val: Value = res.into();
	let val: Cbor = val.try_into()?;
	// Create a new vector for encoding output
	let mut res = Vec::new();
	// Serialize the value into CBOR binary data
	ciborium::into_writer(&val.0, &mut res).unwrap();
	// Return the message length, and message as binary
	Ok(res)
}
