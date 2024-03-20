use crate::net::headers::ContentType;
use crate::rpc::request::Request;
use crate::rpc::response::Response;
use axum::extract::ws::Message;
use axum::response::IntoResponse;
use axum::response::Response as AxumResponse;
use bytes::Bytes;
use http::header::CONTENT_TYPE;
use http::HeaderValue;
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

pub fn req_http(val: &Bytes) -> Result<Request, RpcError> {
	deserialize::<Value>(val).map_err(|_| RpcError::ParseError)?.try_into()
}

pub fn res_http(res: Response) -> Result<AxumResponse, RpcError> {
	// Serialize the response with full internal type information
	let res = surrealdb::sql::serde::serialize(&res).unwrap();
	// Return the message length, and message as binary
	// TODO: Check what this header should be, I'm being consistent with /sql
	Ok(([(CONTENT_TYPE, HeaderValue::from(ContentType::Surrealdb))], res).into_response())
}
