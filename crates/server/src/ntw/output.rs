use axum::response::{IntoResponse, Response};
use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderValue};
use serde::Serialize;
use surrealdb_types::Value;

use super::headers::Accept;

pub enum Output {
	None,
	Fail,
	Text(String),
	Json(Vec<u8>),
	Cbor(Vec<u8>),
	Flatbuffers(Vec<u8>),
}
impl Output {
	// All these methods should not be used.
	//
	// They handle serialization differently then how serialization is handled in
	// core. We need to force a single way to serialize values or end up with
	// subtle bugs and format differences.
	#[deprecated]
	pub fn json_value(val: &Value) -> Output {
		match surrealdb_core::rpc::format::json::encode(val.clone()) {
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
	pub fn cbor(val: Value) -> Output {
		match surrealdb_core::rpc::format::cbor::encode(val) {
			Ok(bytes) => Output::Cbor(bytes),
			Err(_) => Output::Fail,
		}
	}

	#[deprecated]
	pub fn flatbuffers(val: &Value) -> Output {
		let val = surrealdb_core::rpc::format::flatbuffers::encode(val);
		match val {
			Ok(v) => Output::Flatbuffers(v),
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
			Output::Flatbuffers(v) => {
				([(CONTENT_TYPE, HeaderValue::from(Accept::ApplicationFlatbuffers))], v)
					.into_response()
			}
			Output::None => StatusCode::OK.into_response(),
			Output::Fail => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
		}
	}
}
