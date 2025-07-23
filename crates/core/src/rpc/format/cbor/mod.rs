mod convert;

use crate::rpc::RpcError;
use crate::rpc::request::Request;
use crate::val::Value;

pub fn encode(v: Value) -> Result<Vec<u8>, RpcError> {
	let encoding = convert::from_value(v);
	let mut res = Vec::new();
	//TODO: Check if this can ever panic.
	ciborium::into_writer(&encoding, &mut res).unwrap();
	Ok(res)
}

pub fn decode(bytes: &[u8]) -> Result<Value, RpcError> {
	let encoding = ciborium::from_reader(bytes).map_err(|_| RpcError::ParseError)?;
	convert::to_value(encoding).map_err(|x| RpcError::Thrown(x.to_owned()))
}
