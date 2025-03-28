use crate::net::headers::{Accept, ContentType};
use crate::rpc::failure::Failure;
use crate::rpc::response::Response;
use axum::extract::ws::Message;
use axum::response::IntoResponse;
use axum::response::Response as AxumResponse;
use bytes::Bytes;
use http::header::{HeaderValue, CONTENT_TYPE};
use surrealdb::rpc::format::Format;
use surrealdb::rpc::request::Request;
use surrealdb::rpc::RpcError;

impl From<&Accept> for Format {
	fn from(value: &Accept) -> Self {
		match value {
			Accept::TextPlain => Format::Unsupported,
			Accept::ApplicationJson => Format::Json,
			Accept::ApplicationCbor => Format::Cbor,
			Accept::ApplicationPack => Format::Msgpack,
			Accept::ApplicationOctetStream => Format::Unsupported,
			Accept::Surrealdb => Format::Bincode,
		}
	}
}

impl From<&ContentType> for Format {
	fn from(value: &ContentType) -> Self {
		match value {
			ContentType::TextPlain => Format::Unsupported,
			ContentType::ApplicationJson => Format::Json,
			ContentType::ApplicationCbor => Format::Cbor,
			ContentType::ApplicationPack => Format::Msgpack,
			ContentType::ApplicationOctetStream => Format::Unsupported,
			ContentType::Surrealdb => Format::Bincode,
		}
	}
}

impl From<&Format> for ContentType {
	fn from(format: &Format) -> Self {
		match format {
			Format::Json => ContentType::ApplicationJson,
			Format::Cbor => ContentType::ApplicationCbor,
			Format::Msgpack => ContentType::ApplicationPack,
			Format::Unsupported => ContentType::ApplicationOctetStream,
			Format::Bincode => ContentType::Surrealdb,
			_ => ContentType::TextPlain,
		}
	}
}

pub trait WsFormat {
	/// Process a WebSocket RPC request
	fn req_ws(&self, msg: Message) -> Result<Request, Failure>;
	/// Process a WebSocket RPC response
	fn res_ws(&self, res: Response) -> Result<(usize, Message), Failure>;
}

impl WsFormat for Format {
	/// Process a WebSocket RPC request
	fn req_ws(&self, msg: Message) -> Result<Request, Failure> {
		let val = msg.into_data();
		self.req(val).map_err(Into::into)
	}
	/// Process a WebSocket RPC response
	fn res_ws(&self, res: Response) -> Result<(usize, Message), Failure> {
		let res = self.res(res).map_err(Failure::from)?;
		if matches!(self, Format::Json) {
			// If this has significant performance overhead it could be
			// replaced with unsafe { String::from_utf8_unchecked(res) }
			// This would still be completel safe as in the case of JSON
			// ressult come from a call to Into::<Vec<u8>> for String.
			Ok((res.len(), Message::Text(String::from_utf8(res).unwrap())))
		} else {
			Ok((res.len(), Message::Binary(res)))
		}
	}
}

pub trait HttpFormat {
	/// Process a HTTP RPC request
	fn req_http(&self, body: Bytes) -> Result<Request, RpcError>;
	/// Process a HTTP RPC response
	fn res_http(&self, res: Response) -> Result<AxumResponse, RpcError>;
}

impl HttpFormat for Format {
	/// Process a HTTP RPC request
	fn req_http(&self, body: Bytes) -> Result<Request, RpcError> {
		self.req(body)
	}
	/// Process a HTTP RPC response
	fn res_http(&self, res: Response) -> Result<AxumResponse, RpcError> {
		let res = self.res(res)?;
		if matches!(self, Format::Json) {
			// If this has significant performance overhead it could be
			// replaced with unsafe { String::from_utf8_unchecked(res) }
			// This would still be completel safe as in the case of JSON
			// ressult come from a call to Into::<Vec<u8>> for String.
			Ok((
				[(CONTENT_TYPE, HeaderValue::from(ContentType::ApplicationJson))],
				String::from_utf8(res).unwrap(),
			)
				.into_response())
		} else {
			Ok(([(CONTENT_TYPE, HeaderValue::from(ContentType::from(self)))], res).into_response())
		}
	}
}
