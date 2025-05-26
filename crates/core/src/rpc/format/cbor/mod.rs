pub mod decode;
pub mod either;
pub mod encode;
pub mod err;
pub mod major;
pub mod reader;
pub mod simple;
pub mod tags;
pub mod types;
pub mod writer;

use decode::Decoder;
use encode::Encode;
use writer::Writer;

use crate::expr::Value;
use crate::rpc::RpcError;
use crate::rpc::request::Request;
use crate::sql::SqlValue;

use super::ResTrait;

pub fn parse_value(val: Vec<u8>) -> Result<SqlValue, RpcError> {
	let mut dec = Decoder::from(val.as_slice());
	dec.decode().map_err(|_| RpcError::ParseError)
}

pub fn req(val: Vec<u8>) -> Result<Request, RpcError> {
	parse_value(val)?.try_into()
}

pub fn res(res: impl ResTrait) -> Result<Vec<u8>, RpcError> {
	// Convert the response into a value
	let val: Value = res.into();
	let val: SqlValue = val.into();
	let mut writer = Writer::default();
	val.encode(&mut writer).map_err(|e| RpcError::InternalError(e.into()))?;
	// Return the message length, and message as binary
	Ok(writer.into_inner())
}
