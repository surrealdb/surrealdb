use crate::net::headers::ContentType;
use crate::rpc::request::Request;
use crate::rpc::response::Response;
use axum::extract::ws::Message;
use axum::response::{IntoResponse, Response as AxumResponse};
use bytes::Bytes;
use http::header::CONTENT_TYPE;
use http::HeaderValue;
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

pub fn req_http(body: Bytes) -> Result<Request, RpcError> {
	let val: Vec<u8> = body.into();
	Value::deserialize_revisioned(&mut val.as_slice()).map_err(|_| RpcError::ParseError)?.try_into()
}

pub fn res_http(res: Response) -> Result<AxumResponse, RpcError> {
	// Serialize the response with full internal type information
	let mut buf = Vec::new();
	res.serialize_revisioned(&mut buf).unwrap();
	// Return the message length, and message as binary
	// TODO: Check what this header should be, new header needed for revisioned
	Ok(([(CONTENT_TYPE, HeaderValue::from(ContentType::Surrealdb))], buf).into_response())
}
