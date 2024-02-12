use crate::rpc::failure::Failure;
use crate::rpc::request::Request;
use crate::rpc::response::Response;
use axum::extract::ws::Message;
use revision::Revisioned;
use surrealdb::sql::Value;

pub fn req(msg: Message) -> Result<Request, Failure> {
	match msg {
		Message::Binary(val) => Value::deserialize_revisioned(&mut val.as_slice())
			.map_err(|_| Failure::PARSE_ERROR)?
			.try_into(),
		_ => Err(Failure::INVALID_REQUEST),
	}
}

pub fn res(res: Response) -> Result<(usize, Message), Failure> {
	// Serialize the response with full internal type information
	let mut buf = Vec::new();
	res.serialize_revisioned(&mut buf).unwrap();
	// Return the message length, and message as binary
	Ok((buf.len(), Message::Binary(buf)))
}
