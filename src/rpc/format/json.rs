use crate::rpc::request::Request;
use crate::rpc::response::Response;
use axum::extract::ws::Message;
use axum::response::IntoResponse;
use axum::response::Response as AxumResponse;
use bytes::Bytes;
use http::StatusCode;
use surrealdb::rpc::RpcError;
use surrealdb::sql;

pub fn req_ws(msg: Message) -> Result<Request, RpcError> {
	match msg {
		Message::Text(val) => {
			surrealdb::syn::value_legacy_strand(&val).map_err(|_| RpcError::ParseError)?.try_into()
		}
		_ => Err(RpcError::InvalidRequest),
	}
}

pub fn res_ws(res: Response) -> Result<(usize, Message), RpcError> {
	// Convert the response into simplified JSON
	let val = res.into_json();
	// Serialize the response with simplified type information
	let res = serde_json::to_string(&val).unwrap();
	// Return the message length, and message as binary
	Ok((res.len(), Message::Text(res)))
}

pub fn req_http(val: &Bytes) -> Result<Request, RpcError> {
	sql::value(std::str::from_utf8(val).or(Err(RpcError::ParseError))?)
		.or(Err(RpcError::ParseError))?
		.try_into()
}

pub fn res_http(res: Response) -> Result<AxumResponse, RpcError> {
	// Convert the response into simplified JSON
	let val = res.into_json();
	// Serialize the response with simplified type information
	let res = serde_json::to_string(&val).unwrap();
	// Return the message length, and message as binary
	Ok((StatusCode::OK, res).into_response())
}
