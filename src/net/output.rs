use super::headers::Accept;
use anyhow::Result;
use axum::response::{IntoResponse, Response};
use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderValue};
use serde::Serialize;
use serde_json::Value as Json;
use surrealdb::expr;

pub enum Output {
	None,
	Fail,
	Text(String),
	Json(Vec<u8>), // JSON
	Cbor(Vec<u8>), // CBOR
	Full(Vec<u8>), // Full type serialization
}
impl Output {
	pub fn json<T>(val: &T) -> Output
	where
		T: Serialize,
	{
		match serde_json::to_vec(val) {
			Ok(v) => Output::Json(v),
			Err(_) => Output::Fail,
		}
	}

	pub fn cbor<T>(val: &T) -> Output
	where
		T: Serialize,
	{
		let mut out = Vec::new();
		match ciborium::into_writer(&val, &mut out) {
			Ok(_) => Output::Cbor(out),
			Err(_) => Output::Fail,
		}
	}

	pub fn full<T>(val: &T) -> Output
	where
		T: Serialize,
	{
		match surrealdb::sql::serde::serialize(val) {
			Ok(v) => Output::Full(v),
			Err(_) => Output::Fail,
		}
	}
}

/// Convert and simplify the value into JSON
pub fn simplify<T: Serialize + 'static>(v: T) -> Result<Json> {
	Ok(expr::to_value(v)?.into())
}

impl IntoResponse for Output {
	fn into_response(self) -> Response {
		match self {
			Output::Text(v) => {
				([(CONTENT_TYPE, HeaderValue::from(Accept::TextPlain))], v).into_response()
			}
			Output::Json(v) => {
				([(CONTENT_TYPE, HeaderValue::from(Accept::ApplicationJson))], v).into_response()
			}
			Output::Cbor(v) => {
				([(CONTENT_TYPE, HeaderValue::from(Accept::ApplicationCbor))], v).into_response()
			}
			Output::Full(v) => {
				([(CONTENT_TYPE, HeaderValue::from(Accept::Surrealdb))], v).into_response()
			}
			Output::None => StatusCode::OK.into_response(),
			Output::Fail => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
		}
	}
}
