mod convert;

use bytes::Bytes;
pub use convert::Cbor;
use http::header::CONTENT_TYPE;
use http::HeaderValue;
use surrealdb::rpc::RpcError;

use crate::net::headers::ContentType;
use crate::rpc::request::Request;
use crate::rpc::response::Response;
use axum::extract::ws::Message;
use axum::response::{IntoResponse, Response as AxumResponse};
use ciborium::Value as Data;

pub fn req_ws(msg: Message) -> Result<Request, RpcError> {
	match msg {
		Message::Text(val) => {
			surrealdb::sql::value(&val).map_err(|_| RpcError::ParseError)?.try_into()
		}
		Message::Binary(val) => ciborium::from_reader::<Data, _>(&mut val.as_slice())
			.map_err(|_| RpcError::ParseError)
			.map(Cbor)?
			.try_into(),
		_ => Err(RpcError::InvalidRequest),
	}
}

pub fn res_ws(res: Response) -> Result<(usize, Message), RpcError> {
	// Convert the response into a value
	let val: Cbor = res.into_value().try_into()?;
	// Create a new vector for encoding output
	let mut res = Vec::new();
	// Serialize the value into CBOR binary data
	ciborium::into_writer(&val.0, &mut res).unwrap();
	// Return the message length, and message as binary
	Ok((res.len(), Message::Binary(res)))
}

pub fn req_http(body: Bytes) -> Result<Request, RpcError> {
	let val: Vec<u8> = body.into();
	ciborium::from_reader::<Data, _>(&mut val.as_slice())
		.map_err(|_| RpcError::ParseError)
		.map(Cbor)?
		.try_into()
}

pub fn res_http(res: Response) -> Result<AxumResponse, RpcError> {
	// Convert the response into a value
	let val: Cbor = res.into_value().try_into()?;
	// Create a new vector for encoding output
	let mut res = Vec::new();
	// Serialize the value into CBOR binary data
	ciborium::into_writer(&val.0, &mut res).unwrap();
	// Return the message length, and message as binary
	Ok(([(CONTENT_TYPE, HeaderValue::from(ContentType::ApplicationCbor))], res).into_response())
}
