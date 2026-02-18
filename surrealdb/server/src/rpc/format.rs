use axum::extract::ws::Message;
use axum::response::Response as AxumResponse;
use bytes::Bytes;
use http::header::CONTENT_TYPE;
use surrealdb_core::rpc::format::Format;
use surrealdb_core::rpc::request::Request;
use surrealdb_core::rpc::{DbResponse, invalid_request, parse_error};
use surrealdb_types::{Error as TypesError, SurrealValue, Value};

use crate::ntw::headers::{Accept, ContentType};

impl From<&Accept> for Format {
	fn from(value: &Accept) -> Self {
		match value {
			Accept::TextPlain => Format::Unsupported,
			Accept::ApplicationJson => Format::Json,
			Accept::ApplicationCbor => Format::Cbor,
			Accept::ApplicationOctetStream => Format::Unsupported,
			Accept::ApplicationFlatbuffers => Format::Flatbuffers,
		}
	}
}

impl From<&ContentType> for Format {
	fn from(value: &ContentType) -> Self {
		match value {
			ContentType::TextPlain => Format::Unsupported,
			ContentType::ApplicationJson => Format::Json,
			ContentType::ApplicationCbor => Format::Cbor,
			ContentType::ApplicationOctetStream => Format::Unsupported,
			ContentType::ApplicationSurrealDBFlatbuffers => Format::Flatbuffers,
		}
	}
}

impl From<&Format> for ContentType {
	fn from(format: &Format) -> Self {
		match format {
			Format::Json => ContentType::ApplicationJson,
			Format::Cbor => ContentType::ApplicationCbor,
			Format::Flatbuffers => ContentType::ApplicationSurrealDBFlatbuffers,
			Format::Unsupported => ContentType::ApplicationOctetStream,
		}
	}
}

pub trait WsFormat {
	/// Process a WebSocket RPC request
	fn req_ws(&self, msg: Message) -> Result<Request, TypesError>;
	/// Process a WebSocket RPC response
	fn res_ws(&self, res: DbResponse) -> Result<(usize, Message), TypesError>;
}

impl WsFormat for Format {
	/// Process a WebSocket RPC request
	fn req_ws(&self, msg: Message) -> Result<Request, TypesError> {
		let val = msg.into_data();
		match self {
			Format::Json => {
				let val =
					surrealdb_core::rpc::format::json::decode(&val).map_err(|_| parse_error())?;
				if let Value::Object(obj) = val {
					Ok(Request::from_object(obj)?)
				} else {
					Err(parse_error())
				}
			}
			Format::Cbor => {
				let val =
					surrealdb_core::rpc::format::cbor::decode(&val).map_err(|_| parse_error())?;
				if let Value::Object(obj) = val {
					Ok(Request::from_object(obj)?)
				} else {
					Err(parse_error())
				}
			}
			Format::Flatbuffers => {
				let val = surrealdb_core::rpc::format::flatbuffers::decode(&val)
					.map_err(|_| parse_error())?;
				if let Value::Object(obj) = val {
					Ok(Request::from_object(obj)?)
				} else {
					Err(parse_error())
				}
			}
			Format::Unsupported => Err(invalid_request()),
		}
	}

	/// Process a WebSocket RPC response
	fn res_ws(&self, res: DbResponse) -> Result<(usize, Message), TypesError> {
		match self {
			Format::Json => {
				let val = surrealdb_core::rpc::format::json::encode_str(res.into_value())
					.map_err(|_| parse_error())?;
				Ok((val.len(), Message::Text(val.into())))
			}
			Format::Cbor => {
				let val = surrealdb_core::rpc::format::cbor::encode(res.into_value())
					.map_err(|_| parse_error())?;
				Ok((val.len(), Message::Binary(val.into())))
			}
			Format::Flatbuffers => {
				let res_value = res.into_value();
				let val = surrealdb_core::rpc::format::flatbuffers::encode(&res_value)
					.map_err(|_| parse_error())?;
				Ok((val.len(), Message::Binary(val.into())))
			}
			Format::Unsupported => Err(invalid_request()),
		}
	}
}

pub trait HttpFormat {
	/// Process a HTTP RPC request
	fn req_http(&self, body: Bytes) -> Result<Request, TypesError>;
	/// Process a HTTP RPC response
	fn res_http(&self, res: DbResponse) -> Result<AxumResponse, TypesError>;
}

impl HttpFormat for Format {
	/// Process a HTTP RPC request
	fn req_http(&self, body: Bytes) -> Result<Request, TypesError> {
		match self {
			Format::Json => {
				let val =
					surrealdb_core::rpc::format::json::decode(&body).map_err(|_| parse_error())?;
				if let Value::Object(obj) = val {
					Ok(Request::from_object(obj)?)
				} else {
					Err(parse_error())
				}
			}
			Format::Cbor => {
				let val =
					surrealdb_core::rpc::format::cbor::decode(&body).map_err(|_| parse_error())?;
				if let Value::Object(obj) = val {
					Ok(Request::from_object(obj)?)
				} else {
					Err(parse_error())
				}
			}
			Format::Flatbuffers => {
				let val = surrealdb_core::rpc::format::flatbuffers::decode(&body)
					.map_err(|_| parse_error())?;
				if let Value::Object(obj) = val {
					Ok(Request::from_object(obj)?)
				} else {
					Err(parse_error())
				}
			}
			Format::Unsupported => Err(invalid_request()),
		}
	}

	/// Process a HTTP RPC response
	fn res_http(&self, res: DbResponse) -> Result<AxumResponse, TypesError> {
		let val = match self {
			Format::Json => surrealdb_core::rpc::format::json::encode_str(res.into_value())
				.map_err(|_| parse_error())?
				.into_bytes(),
			Format::Cbor => surrealdb_core::rpc::format::cbor::encode(res.into_value())
				.map_err(|_| parse_error())?,
			Format::Flatbuffers => {
				let res_value = res.into_value();
				surrealdb_core::rpc::format::flatbuffers::encode(&res_value)
					.map_err(|_| parse_error())?
			}
			Format::Unsupported => return Err(invalid_request()),
		};

		AxumResponse::builder()
			.header(CONTENT_TYPE, ContentType::from(self))
			.body(val.into())
			.map_err(|_| parse_error())
	}
}
