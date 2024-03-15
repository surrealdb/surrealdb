use crate::rpc::request::Request;
use crate::rpc::response::Response;
use axum::extract::ws::Message;
use surrealdb::rpc::RpcError;
use surrealdb::sql::serde::deserialize;
use surrealdb::sql::Value;

pub fn req_ws(msg: Message) -> Result<Request, RpcError> {
	match msg {
		Message::Binary(val) => {
			deserialize::<Value>(&val).map_err(|_| RpcError::ParseError)?.try_into()
		}
		_ => Err(RpcError::InvalidRequest),
	}
}

pub fn res_ws(res: Response) -> Result<(usize, Message), RpcError> {
	// Serialize the response with full internal type information
	let res = surrealdb::sql::serde::serialize(&res).unwrap();
	// Return the message length, and message as binary
	Ok((res.len(), Message::Binary(res)))
}
