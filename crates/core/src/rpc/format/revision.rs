use crate::rpc::RpcError;
use crate::rpc::request::Request;
use crate::sql::SqlValue;
use revision::Revisioned;
use crate::proto::surrealdb::value::Value as ValueProto;

pub fn parse_value(val: Vec<u8>) -> Result<SqlValue, RpcError> {
	SqlValue::deserialize_revisioned(&mut val.as_slice()).map_err(|_| RpcError::ParseError)
}

pub fn req(val: Vec<u8>) -> Result<Request, RpcError> {
	todo!("DELETE")
	// parse_value(val)?.try_into()
}

pub fn res(res: ValueProto) -> Result<Vec<u8>, RpcError> {
	// Serialize the response with full internal type information
	let mut buf = Vec::new();
	todo!("DELETE");
	// res.serialize_revisioned(&mut buf).unwrap();
	Ok(buf)
}
