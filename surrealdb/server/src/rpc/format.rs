use axum::extract::ws::Message;
use axum::response::Response as AxumResponse;
use bytes::Bytes;
use http::header::CONTENT_TYPE;
use surrealdb_core::rpc::format::Format;
use surrealdb_rpc::error::{invalid_request, parse_error};
use surrealdb_rpc::request::Request;
use surrealdb_rpc::{DbResponse, EncodedRows, RowFormat};
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

/// Encode `res` in `format`, splicing rows the executor already encoded.
fn encode_response(format: RowFormat, res: DbResponse) -> Result<Vec<u8>, TypesError> {
	surrealdb_core::rpc::format::encoded::encode_response(format, res).map_err(|_| parse_error())
}

pub trait WsFormat {
	/// Process a WebSocket RPC request
	fn req_ws(&self, msg: Message, recursion_limit: usize) -> Result<Request, TypesError>;
	/// Process a WebSocket RPC response
	fn res_ws(&self, res: DbResponse) -> Result<(usize, Message), TypesError>;
	/// The `query_stream` `rows` frame of statement `index`, carrying `rows`,
	/// as the successful response to request `id`, written around the rows
	/// as they are encoded; `None` when they are not encoded in this format.
	fn rows_ws(
		&self,
		id: Option<Value>,
		session_id: Option<uuid::Uuid>,
		index: usize,
		rows: &EncodedRows,
	) -> Option<Result<(usize, Message), TypesError>>;
}

impl WsFormat for Format {
	/// Process a WebSocket RPC request
	fn req_ws(&self, msg: Message, recursion_limit: usize) -> Result<Request, TypesError> {
		let val = msg.into_data();
		match self {
			Format::Json => {
				let val = surrealdb_core::rpc::format::json::decode(&val, recursion_limit)
					.map_err(|_| parse_error())?;
				if let Value::Object(obj) = val {
					Ok(Request::from_object(obj)?)
				} else {
					Err(parse_error())
				}
			}
			Format::Cbor => {
				let val = surrealdb_core::rpc::format::cbor::decode(&val, recursion_limit)
					.map_err(|_| parse_error())?;
				if let Value::Object(obj) = val {
					Ok(Request::from_object(obj)?)
				} else {
					Err(parse_error())
				}
			}
			Format::Flatbuffers => {
				//FIXME: Flatbuffers should probably also implement a recursion_limit.
				let val = surrealdb_types::decode(&val).map_err(|_| parse_error())?;
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
	fn rows_ws(
		&self,
		id: Option<Value>,
		session_id: Option<uuid::Uuid>,
		index: usize,
		rows: &EncodedRows,
	) -> Option<Result<(usize, Message), TypesError>> {
		let format = self.row_format().filter(|format| *format == rows.format())?;
		let encoded = surrealdb_core::rpc::format::encoded::encode_stream_rows(
			format, id, session_id, index, rows,
		)
		.map_err(|_| parse_error());
		Some(encoded.and_then(|bytes| match format {
			RowFormat::Json => {
				let text = String::from_utf8(bytes).map_err(|_| parse_error())?;
				Ok((text.len(), Message::Text(text.into())))
			}
			RowFormat::Cbor => Ok((bytes.len(), Message::Binary(bytes.into()))),
		}))
	}

	fn res_ws(&self, res: DbResponse) -> Result<(usize, Message), TypesError> {
		match self {
			Format::Json => {
				let val = encode_response(RowFormat::Json, res)?;
				let val = String::from_utf8(val).map_err(|_| parse_error())?;
				Ok((val.len(), Message::Text(val.into())))
			}
			Format::Cbor => {
				let val = encode_response(RowFormat::Cbor, res)?;
				Ok((val.len(), Message::Binary(val.into())))
			}
			Format::Flatbuffers => {
				let res_value = res.into_value();
				let val = surrealdb_types::encode(&res_value).map_err(|_| parse_error())?;
				Ok((val.len(), Message::Binary(val.into())))
			}
			Format::Unsupported => Err(invalid_request()),
		}
	}
}

pub trait HttpFormat {
	/// Process a HTTP RPC request
	fn req_http(&self, body: Bytes, recursion_limit: usize) -> Result<Request, TypesError>;
	/// Process a HTTP RPC response
	fn res_http(&self, res: DbResponse) -> Result<AxumResponse, TypesError>;
}

impl HttpFormat for Format {
	/// Process a HTTP RPC request
	fn req_http(&self, body: Bytes, recursion_limit: usize) -> Result<Request, TypesError> {
		match self {
			Format::Json => {
				let val = surrealdb_core::rpc::format::json::decode(&body, recursion_limit)
					.map_err(|_| parse_error())?;
				if let Value::Object(obj) = val {
					Ok(Request::from_object(obj)?)
				} else {
					Err(parse_error())
				}
			}
			Format::Cbor => {
				let val = surrealdb_core::rpc::format::cbor::decode(&body, recursion_limit)
					.map_err(|_| parse_error())?;
				if let Value::Object(obj) = val {
					Ok(Request::from_object(obj)?)
				} else {
					Err(parse_error())
				}
			}
			Format::Flatbuffers => {
				let val = surrealdb_types::decode(&body).map_err(|_| parse_error())?;
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
			Format::Json => encode_response(RowFormat::Json, res)?,
			Format::Cbor => encode_response(RowFormat::Cbor, res)?,
			Format::Flatbuffers => {
				let res_value = res.into_value();
				surrealdb_types::encode(&res_value).map_err(|_| parse_error())?
			}
			Format::Unsupported => return Err(invalid_request()),
		};

		AxumResponse::builder()
			.header(CONTENT_TYPE, ContentType::from(self))
			.body(val.into())
			.map_err(|_| parse_error())
	}
}
