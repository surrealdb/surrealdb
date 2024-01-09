use crate::rpc::failure::Failure;
use crate::rpc::request::Request;
use crate::rpc::response::Response;
use axum::extract::ws::Message;

pub fn req(msg: Message) -> Result<Request, Failure> {
	match msg {
		Message::Text(val) => {
			surrealdb::sql::value(&val).map_err(|_| Failure::PARSE_ERROR)?.try_into()
		}
		_ => Err(Failure::INVALID_REQUEST),
	}
}

pub fn res(res: Response) -> Result<(usize, Message), Failure> {
	// Convert the response into simplified JSON
	let val = res.into_json();
	// Serialize the value into MsgPack binary data
	let res = serde_pack::to_vec(&val).unwrap();
	// Return the message length, and message as binary
	Ok((res.len(), Message::Binary(res)))
}
