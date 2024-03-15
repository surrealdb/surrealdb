use crate::rpc::request::Request;
use crate::rpc::response::Response;
use axum::extract::ws::Message;
use revision::Revisioned;
use surrealdb::rpc::RpcError;
use surrealdb::sql::Value;

pub fn req_ws(msg: Message) -> Result<Request, RpcError> {
	match msg {
		Message::Binary(val) => Value::deserialize_revisioned(&mut val.as_slice())
			.map_err(|_| RpcError::ParseError)?
			.try_into(),
		_ => Err(RpcError::InvalidRequest),
	}
}

pub fn res_ws(res: Response) -> Result<(usize, Message), RpcError> {
	// Serialize the response with full internal type information
	let mut buf = Vec::new();
	res.serialize_revisioned(&mut buf).unwrap();
	// Return the message length, and message as binary
	Ok((buf.len(), Message::Binary(buf)))
}
