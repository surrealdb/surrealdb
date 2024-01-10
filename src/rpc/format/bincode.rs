use crate::rpc::failure::Failure;
use crate::rpc::request::Request;
use crate::rpc::response::Response;
use axum::extract::ws::Message;
use surrealdb::sql::serde::deserialize;
use surrealdb::sql::Value;

pub fn req(msg: Message) -> Result<Request, Failure> {
	match msg {
		Message::Binary(val) => {
			deserialize::<Value>(&val).map_err(|_| Failure::PARSE_ERROR)?.try_into()
		}
		_ => Err(Failure::INVALID_REQUEST),
	}
}

pub fn res(res: Response) -> Result<(usize, Message), Failure> {
	// Serialize the response with full internal type information
	let res = surrealdb::sql::serde::serialize(&res).unwrap();
	// Return the message length, and message as binary
	Ok((res.len(), Message::Binary(res)))
}
