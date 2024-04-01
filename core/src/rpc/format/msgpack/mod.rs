mod convert;

use crate::rpc::RpcError;
pub use convert::Pack;

use crate::rpc::request::Request;
use crate::sql::Value;

pub fn req(val: Vec<u8>) -> Result<Request, RpcError> {
	rmpv::decode::read_value(&mut val.as_slice())
		.map_err(|_| RpcError::ParseError)
		.map(Pack)?
		.try_into()
}

pub fn res(res: Value) -> Result<Vec<u8>, RpcError> {
	// Convert the response into a value
	let val: Pack = res.try_into()?;
	// Create a new vector for encoding output
	let mut res = Vec::new();
	// Serialize the value into MsgPack binary data
	rmpv::encode::write_value(&mut res, &val.0).unwrap();
	Ok(res)
}
