use axum::response::{IntoResponse, Response};
use bincode::Options;
use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderValue};
use serde::Serialize;

use super::headers::Accept;
use crate::core::val;

pub enum Output {
	None,
	Fail,
	Text(String),
	Json(Vec<u8>), // JSON
	Cbor(Vec<u8>), // CBOR
	Bincode(Vec<u8>), /* Full type serialization, don't know why this is called 'full'
	                * serialization, this is just bincode. Should not be used. */
}
impl Output {
	// All these methods should not be used.
	//
	// They handle serialization differently then how serialization is handled in
	// core. We need to force a single way to serialize values or end up with
	// subtle bugs and format differences.
	#[deprecated]
	pub fn json_value(val: &val::Value) -> Output {
		match crate::core::rpc::format::json::encode(val.clone()) {
			Ok(v) => Output::Json(v),
			Err(_) => Output::Fail,
		}
	}

	#[deprecated]
	pub fn json_other<T>(val: &T) -> Output
	where
		T: Serialize,
	{
		match serde_json::to_vec(val) {
			Ok(v) => Output::Json(v),
			Err(_) => Output::Fail,
		}
	}

	#[deprecated]
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

	#[deprecated]
	pub fn bincode<T>(val: &T) -> Output
	where
		T: Serialize,
	{
		let val = bincode::options()
			.with_no_limit()
			.with_little_endian()
			.with_varint_encoding()
			.serialize(val);
		match val {
			Ok(v) => Output::Bincode(v),
			Err(_) => Output::Fail,
		}
	}
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
			Output::Bincode(v) => {
				([(CONTENT_TYPE, HeaderValue::from(Accept::Surrealdb))], v).into_response()
			}
			Output::None => StatusCode::OK.into_response(),
			Output::Fail => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
		}
	}
}
