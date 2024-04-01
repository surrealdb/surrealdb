use crate::net::headers::{Accept, ContentType};
use crate::rpc::failure::Failure;
use crate::rpc::response::Response;
use axum::extract::ws::Message;
use axum::response::Response as AxumResponse;
use bytes::Bytes;
use surrealdb::rpc::format::Format;
use surrealdb::rpc::request::Request;
use surrealdb::rpc::RpcError;

impl From<&Accept> for Format {
	fn from(value: &Accept) -> Self {
		match value {
			Accept::TextPlain => Format::None,
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
			ContentType::TextPlain => Format::None,
			ContentType::ApplicationJson => Format::Json,
			ContentType::ApplicationCbor => Format::Cbor,
			ContentType::ApplicationPack => Format::Msgpack,
			ContentType::ApplicationOctetStream => Format::Unsupported,
			ContentType::Surrealdb => Format::Bincode,
		}
	}
}

pub trait WsFormat {
	fn req_ws(&self, msg: Message) -> Result<Request, Failure>;
	fn res_ws(&self, res: Response) -> Result<(usize, Message), Failure>;
}

impl WsFormat for Format {
	fn req_ws(&self, msg: Message) -> Result<Request, Failure> {
		let val = msg.into_data();
		self.req(val).map_err(Into::into)
	}

	fn res_ws(&self, res: Response) -> Result<(usize, Message), Failure> {
		let val = res.into_value();
		let res = self.res(val).map_err(Into::into)?;
		if matches!(self, Format::Json) {
			// If this has significant performance overhead it could be replaced with unsafe { String::from_utf8_unchecked(res) }
			// This would be safe as in the case of JSON res come from a call to Into::<Vec<u8>> for String
			Ok((res.len(), Message::Text(String::from_utf8(res))))
		} else {
			Ok((res.len(), Message::Binary(res)))
		}
	}
}

pub trait HttpFormat {
	fn req_http(&self, body: Bytes) -> Result<Request, RpcError>;
	fn res_http(&self, res: Response) -> Result<AxumResponse, RpcError>;
}

impl HttpFormat for Format {
	fn req_http(&self, body: Bytes) -> Result<Request, RpcError> {
		self.req(body).map_err(Into::into)
	}

	fn res_http(&self, res: Response) -> Result<AxumResponse, RpcError> {
		let val = res.into_value();
		let res = self.res(val).map_err(Into::into)?;
		if matches!(self, Format::Json) {
			// If this has significant performance overhead it could be replaced with unsafe { String::from_utf8_unchecked(res) }
			// This would be safe as in the case of JSON res come from a call to Into::<Vec<u8>> for String
			Ok((200, String::from_utf8(res)).into_response())
		} else {
			Ok((200, res).into_response())
		}
	}
}

// impl Format {
// 	/// Check if this format has been set
// 	pub fn is_none(&self) -> bool {
// 		matches!(self, Format::None)
// 	}
// 	/// Process a request using the specified format
// 	pub fn req_ws(&self, msg: Message) -> Result<Request, Failure> {
// 		match self {
// 			Self::None => unreachable!(), // We should never arrive at this code
// 			Self::Unsupported => unreachable!(), // We should never arrive at this code
// 			Self::Json => json::req_ws(msg),
// 			Self::Cbor => cbor::req_ws(msg),
// 			Self::Msgpack => msgpack::req_ws(msg),
// 			Self::Bincode => bincode::req_ws(msg),
// 			Self::Revision => revision::req_ws(msg),
// 		}
// 		.map_err(Into::into)
// 	}
// 	/// Process a response using the specified format
// 	pub fn res_ws(&self, res: Response) -> Result<(usize, Message), Failure> {
// 		match self {
// 			Self::None => unreachable!(), // We should never arrive at this code
// 			Self::Unsupported => unreachable!(), // We should never arrive at this code
// 			Self::Json => json::res_ws(res),
// 			Self::Cbor => cbor::res_ws(res),
// 			Self::Msgpack => msgpack::res_ws(res),
// 			Self::Bincode => bincode::res_ws(res),
// 			Self::Revision => revision::res_ws(res),
// 		}
// 		.map_err(Into::into)
// 	}
// 	/// Process a request using the specified format
// 	pub fn req_http(&self, body: Bytes) -> Result<Request, RpcError> {
// 		match self {
// 			Self::None => unreachable!(), // We should never arrive at this code
// 			Self::Unsupported => unreachable!(), // We should never arrive at this code
// 			Self::Json => json::req_http(&body),
// 			Self::Cbor => cbor::req_http(body),
// 			Self::Msgpack => msgpack::req_http(body),
// 			Self::Bincode => bincode::req_http(&body),
// 			Self::Revision => revision::req_http(body),
// 		}
// 	}
// 	/// Process a response using the specified format
// 	pub fn res_http(&self, res: Response) -> Result<AxumResponse, RpcError> {
// 		match self {
// 			Self::None => unreachable!(), // We should never arrive at this code
// 			Self::Unsupported => unreachable!(), // We should never arrive at this code
// 			Self::Json => json::res_http(res),
// 			Self::Cbor => cbor::res_http(res),
// 			Self::Msgpack => msgpack::res_http(res),
// 			Self::Bincode => bincode::res_http(res),
// 			Self::Revision => revision::res_http(res),
// 		}
// 	}
// }
