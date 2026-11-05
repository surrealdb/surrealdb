use axum::extract::ws::Message;
use axum::response::Response as AxumResponse;
use bytes::Bytes;
use http::header::CONTENT_TYPE;
use surrealdb_core::rpc::format::Format;
use surrealdb_rpc::error::{decode_error, invalid_request, parse_error};
use surrealdb_rpc::request::Request;
use surrealdb_rpc::{DbResponse, EncodedRows, RowFormat};
use surrealdb_types::shaped::ShapeScope;
use surrealdb_types::{Error as TypesError, SurrealValue, Value};

use crate::ntw::headers::{Accept, ContentType};

impl From<&Accept> for Format {
	fn from(value: &Accept) -> Self {
		match value {
			Accept::TextPlain => Format::Unsupported,
			Accept::ApplicationJson => Format::Json,
			Accept::ApplicationCbor => Format::Cbor,
			Accept::ApplicationCbor2 => Format::Cbor2,
			Accept::ApplicationOctetStream => Format::Unsupported,
			Accept::ApplicationFlatbuffers => Format::Flatbuffers,
			Accept::ApplicationShaped => Format::shaped(),
		}
	}
}

impl From<&ContentType> for Format {
	fn from(value: &ContentType) -> Self {
		match value {
			ContentType::TextPlain => Format::Unsupported,
			ContentType::ApplicationJson => Format::Json,
			ContentType::ApplicationCbor => Format::Cbor,
			ContentType::ApplicationCbor2 => Format::Cbor2,
			ContentType::ApplicationOctetStream => Format::Unsupported,
			ContentType::ApplicationSurrealDBFlatbuffers => Format::Flatbuffers,
			ContentType::ApplicationSurrealDBShaped => Format::shaped(),
		}
	}
}

impl From<&Format> for ContentType {
	fn from(format: &Format) -> Self {
		match format {
			Format::Json => ContentType::ApplicationJson,
			Format::Cbor => ContentType::ApplicationCbor,
			Format::Cbor2 => ContentType::ApplicationCbor2,
			Format::Flatbuffers => ContentType::ApplicationSurrealDBFlatbuffers,
			Format::Shaped => ContentType::ApplicationSurrealDBShaped,
			Format::Unsupported => ContentType::ApplicationOctetStream,
		}
	}
}

/// Encode `res` in `format`, splicing rows the executor already encoded.
fn encode_response(format: RowFormat, res: DbResponse) -> Result<Vec<u8>, TypesError> {
	surrealdb_core::rpc::format::encoded::encode_response(format, res).map_err(|_| parse_error())
}

/// [`encode_response`] as shared bytes, written around CBOR rows where they lie.
fn encode_response_bytes(format: RowFormat, res: DbResponse) -> Result<Bytes, TypesError> {
	surrealdb_core::rpc::format::encoded::encode_response_bytes(format, res)
		.map_err(|_| parse_error())
}

pub trait WsFormat {
	/// Process a WebSocket RPC request
	fn req_ws(&self, msg: Message, recursion_limit: usize) -> Result<Request, TypesError>;
	/// The `id` of a WebSocket RPC request that [`WsFormat::req_ws`] refused, so
	/// that the refusal is answered to the call that caused it. A client matches
	/// a response to its request by this `id`; one without it never settles the
	/// pending call.
	fn req_id_ws(&self, msg: &Message, recursion_limit: usize) -> Option<Value>;
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
		trace: Option<Box<surrealdb_rpc::Trace>>,
		scope: &mut ShapeScope,
	) -> Option<Result<(usize, Message), TypesError>>;
}

impl WsFormat for Format {
	fn req_id_ws(&self, msg: &Message, recursion_limit: usize) -> Option<Value> {
		let data = match msg {
			Message::Text(text) => text.as_bytes(),
			Message::Binary(bytes) => bytes.as_ref(),
			Message::Ping(_) | Message::Pong(_) | Message::Close(_) => return None,
		};
		match self {
			Format::Json => surrealdb_core::rpc::format::json::request_id(data),
			// A shaped connection's requests are CBOR.
			Format::Cbor | Format::Cbor2 | Format::Shaped => {
				surrealdb_core::rpc::format::cbor::request_id(data, recursion_limit)
			}
			Format::Flatbuffers | Format::Unsupported => None,
		}
	}

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
				let val = surrealdb_core::rpc::format::cbor::decode_request(&val, recursion_limit)
					.map_err(|e| decode_error(e.to_string()))?;
				if let Value::Object(obj) = val {
					Ok(Request::from_object(obj)?)
				} else {
					Err(parse_error())
				}
			}
			// A shaped connection's requests are CBOR v2.
			Format::Cbor2 | Format::Shaped => {
				let val =
					surrealdb_core::rpc::format::cbor::decode_request_v2(&val, recursion_limit)
						.map_err(|e| decode_error(e.to_string()))?;
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
		trace: Option<Box<surrealdb_rpc::Trace>>,
		scope: &mut ShapeScope,
	) -> Option<Result<(usize, Message), TypesError>> {
		let format = self.row_format().filter(|format| *format == rows.format())?;
		let encoded = surrealdb_core::rpc::format::encoded::encode_stream_rows(
			format, id, session_id, index, rows, trace, scope,
		)
		.map_err(|_| parse_error());
		Some(encoded.and_then(|bytes| match format {
			RowFormat::Json => {
				let text = String::from_utf8(bytes).map_err(|_| parse_error())?;
				Ok((text.len(), Message::Text(text.into())))
			}
			RowFormat::Cbor | RowFormat::Cbor2 | RowFormat::Shaped => {
				Ok((bytes.len(), Message::Binary(bytes.into())))
			}
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
				let val = encode_response_bytes(RowFormat::Cbor, res)?;
				Ok((val.len(), Message::Binary(val)))
			}
			Format::Cbor2 => {
				let val = encode_response_bytes(RowFormat::Cbor2, res)?;
				Ok((val.len(), Message::Binary(val)))
			}
			Format::Shaped => {
				let val = encode_response(RowFormat::Shaped, res)?;
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
				let val = surrealdb_core::rpc::format::cbor::decode_request(&body, recursion_limit)
					.map_err(|e| decode_error(e.to_string()))?;
				if let Value::Object(obj) = val {
					Ok(Request::from_object(obj)?)
				} else {
					Err(parse_error())
				}
			}
			// A shaped request is CBOR v2.
			Format::Cbor2 | Format::Shaped => {
				let val =
					surrealdb_core::rpc::format::cbor::decode_request_v2(&body, recursion_limit)
						.map_err(|e| decode_error(e.to_string()))?;
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
		let val: Bytes = match self {
			Format::Json => encode_response(RowFormat::Json, res)?.into(),
			Format::Cbor => encode_response_bytes(RowFormat::Cbor, res)?,
			Format::Cbor2 => encode_response_bytes(RowFormat::Cbor2, res)?,
			Format::Shaped => encode_response(RowFormat::Shaped, res)?.into(),
			Format::Flatbuffers => {
				let res_value = res.into_value();
				surrealdb_types::encode(&res_value).map_err(|_| parse_error())?.into()
			}
			Format::Unsupported => return Err(invalid_request()),
		};

		AxumResponse::builder()
			.header(CONTENT_TYPE, ContentType::from(self))
			.body(val.into())
			.map_err(|_| parse_error())
	}
}

#[cfg(test)]
mod tests {
	use axum_extra::headers::Header as _;
	use http::HeaderValue;
	use surrealdb_core::rpc::format::{Format, PROTOCOLS};

	use crate::ntw::headers::{Accept, ContentType};

	/// `shaped` is negotiated as every format is: a WebSocket subprotocol, an `Accept` and a
	/// `Content-Type`; its responses are binary CBOR and its requests are read as CBOR.
	#[test]
	fn shaped_is_negotiated_as_the_other_formats_are() {
		assert!(PROTOCOLS.contains(&"shaped"));
		assert_eq!(Format::from("shaped"), Format::Shaped);
		let value = HeaderValue::from_static(surrealdb_rpc::format::SHAPED);
		let accept = Accept::decode(&mut std::iter::once(&value)).expect("decodes");
		assert_eq!(Format::from(&accept), Format::Shaped);
		let content_type = ContentType::decode(&mut std::iter::once(&value)).expect("decodes");
		assert_eq!(Format::from(&content_type), Format::Shaped);
		assert_eq!(
			HeaderValue::from(ContentType::from(&Format::Shaped)),
			HeaderValue::from_static(surrealdb_rpc::format::SHAPED)
		);
		assert_eq!(Format::Shaped.row_format(), Some(surrealdb_rpc::RowFormat::Shaped));
		assert_eq!(Format::Cbor.row_format(), Some(surrealdb_rpc::RowFormat::Cbor));
		assert_eq!(Format::Cbor2.row_format(), Some(surrealdb_rpc::RowFormat::Cbor2));
	}

	#[test]
	fn cbor2_is_negotiated_as_the_other_formats_are() {
		assert!(PROTOCOLS.contains(&"cbor.2"));
		assert_eq!(Format::from("cbor.2"), Format::Cbor2);
		let value = HeaderValue::from_static(surrealdb_rpc::format::CBOR2);
		let accept = Accept::decode(&mut std::iter::once(&value)).expect("decodes");
		assert_eq!(Format::from(&accept), Format::Cbor2);
		let content_type = ContentType::decode(&mut std::iter::once(&value)).expect("decodes");
		assert_eq!(Format::from(&content_type), Format::Cbor2);
		assert_eq!(
			HeaderValue::from(ContentType::from(&Format::Cbor2)),
			HeaderValue::from_static(surrealdb_rpc::format::CBOR2)
		);
		assert_eq!(Format::Cbor2.row_format(), Some(surrealdb_rpc::RowFormat::Cbor2));
	}

	/// A CBOR request the decoder refuses fails over HTTP `/rpc` as a parse error carrying the
	/// decoder's message: tag 0 around a datetime as text, refused naming tag 12.
	#[test]
	fn a_refused_cbor_request_over_http_carries_the_decoder_message() {
		use surrealdb_types::ValidationError;

		use super::HttpFormat as _;

		// {"id": "1", "method": "query", "params": ["RETURN $d", {"d": 0("x")}]}
		let mut body = vec![0xa3, 0x62, b'i', b'd', 0x61, b'1', 0x66];
		body.extend_from_slice(b"method");
		body.push(0x65);
		body.extend_from_slice(b"query");
		body.push(0x66);
		body.extend_from_slice(b"params");
		body.extend_from_slice(&[0x82, 0x69]);
		body.extend_from_slice(b"RETURN $d");
		body.extend_from_slice(&[0xa1, 0x61, b'd', 0xc0, 0x61, b'x']);
		for format in [Format::Cbor, Format::Cbor2] {
			let err = format.req_http(body.clone().into(), 64).expect_err("refused");
			assert_eq!(err.validation_details(), Some(&ValidationError::Parse));
			assert!(err.message().contains("send tag 12"), "{format:?}: {}", err.message());
		}
	}
}
