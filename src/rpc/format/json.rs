use crate::rpc::failure::Failure;
use crate::rpc::request::Request;
use crate::rpc::response::Response;
use axum::extract::ws::Message;
use bytes::Bytes;
use surrealdb::sql;

pub fn req_ws(msg: Message) -> Result<Request, Failure> {
	match msg {
		Message::Text(val) => {
			surrealdb::sql::value(&val).map_err(|_| Failure::PARSE_ERROR)?.try_into()
		}
		_ => Err(Failure::INVALID_REQUEST),
	}
}

pub fn res_ws(res: Response) -> Result<(usize, Message), Failure> {
	// Convert the response into simplified JSON
	let val = res.into_json();
	// Serialize the response with simplified type information
	let res = serde_json::to_string(&val).unwrap();
	// Return the message length, and message as binary
	Ok((res.len(), Message::Text(res)))
}

pub fn req_http(val: &Bytes) -> Result<Request, Failure> {
	sql::value(std::str::from_utf8(&val).or(Err(Failure::PARSE_ERROR))?)
		.or(Err(Failure::PARSE_ERROR))?
		.try_into()
}
