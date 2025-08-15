use axum::extract::ws::Message;
use axum::response::Response as AxumResponse;
use bytes::Bytes;
use http::header::CONTENT_TYPE;

use crate::core::rpc::RpcError;
use crate::core::rpc::format::Format;
use crate::core::rpc::request::Request;
use crate::core::val;
use crate::net::headers::{Accept, ContentType};
use crate::rpc::failure::Failure;
use crate::rpc::response::Response;

impl From<&Accept> for Format {
	fn from(value: &Accept) -> Self {
		match value {
			Accept::TextPlain => Format::Unsupported,
			Accept::ApplicationJson => Format::Json,
			Accept::ApplicationCbor => Format::Cbor,
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
		match self {
			Format::Json => {
				let val = crate::core::rpc::format::json::decode(&val)
					.map_err(|_| RpcError::ParseError)?;
				if let val::Value::Object(obj) = val {
					Ok(Request::from_object(obj)?)
				} else {
					Err(Failure::from(RpcError::ParseError))
				}
			}
			Format::Cbor => {
				let val = crate::core::rpc::format::cbor::decode(&val)
					.map_err(|_| RpcError::ParseError)?;
				if let val::Value::Object(obj) = val {
					Ok(Request::from_object(obj)?)
				} else {
					Err(Failure::from(RpcError::ParseError))
				}
			}
			Format::Bincode => {
				let val = crate::core::rpc::format::bincode::decode(&val)
					.map_err(|_| RpcError::ParseError)?;
				if let val::Value::Object(obj) = val {
					Ok(Request::from_object(obj)?)
				} else {
					Err(Failure::from(RpcError::ParseError))
				}
			}
			Format::Revision => {
				let val = crate::core::rpc::format::revision::decode(&val)
					.map_err(|_| RpcError::ParseError)?;
				if let val::Value::Object(obj) = val {
					Ok(Request::from_object(obj)?)
				} else {
					Err(Failure::from(RpcError::ParseError))
				}
			}
			Format::Unsupported => Err(Failure::from(RpcError::InvalidRequest)),
		}
	}

	/// Process a WebSocket RPC response
	fn res_ws(&self, res: Response) -> Result<(usize, Message), Failure> {
		match self {
			Format::Json => {
				let val = crate::core::rpc::format::json::encode_str(res.into_value())
					.map_err(|_| RpcError::ParseError)?;
				Ok((val.len(), Message::Text(val)))
			}
			Format::Cbor => {
				let val = crate::core::rpc::format::cbor::encode(res.into_value())
					.map_err(|_| RpcError::ParseError)?;
				Ok((val.len(), Message::Binary(val)))
			}
			Format::Bincode => {
				let val = crate::core::rpc::format::bincode::encode(&res)
					.map_err(|_| RpcError::ParseError)?;
				Ok((val.len(), Message::Binary(val)))
			}
			Format::Revision => {
				let val = crate::core::rpc::format::revision::encode(&res)
					.map_err(|_| RpcError::ParseError)?;
				Ok((val.len(), Message::Binary(val)))
			}
			Format::Unsupported => Err(Failure::from(RpcError::InvalidRequest)),
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
		match self {
			Format::Json => {
				let val = crate::core::rpc::format::json::decode(&body)
					.map_err(|_| RpcError::ParseError)?;
				if let val::Value::Object(obj) = val {
					Ok(Request::from_object(obj)?)
				} else {
					Err(RpcError::ParseError)
				}
			}
			Format::Cbor => {
				let val = crate::core::rpc::format::cbor::decode(&body)
					.map_err(|_| RpcError::ParseError)?;
				if let val::Value::Object(obj) = val {
					Ok(Request::from_object(obj)?)
				} else {
					Err(RpcError::ParseError)
				}
			}
			Format::Bincode => {
				let val = crate::core::rpc::format::bincode::decode(&body)
					.map_err(|_| RpcError::ParseError)?;
				if let val::Value::Object(obj) = val {
					Ok(Request::from_object(obj)?)
				} else {
					Err(RpcError::ParseError)
				}
			}
			Format::Revision => {
				let val = crate::core::rpc::format::revision::decode(&body)
					.map_err(|_| RpcError::ParseError)?;
				if let val::Value::Object(obj) = val {
					Ok(Request::from_object(obj)?)
				} else {
					Err(RpcError::ParseError)
				}
			}
			Format::Unsupported => Err(RpcError::InvalidRequest),
		}
	}
	/// Process a HTTP RPC response
	fn res_http(&self, res: Response) -> Result<AxumResponse, RpcError> {
		match self {
			Format::Json => {
				let val = crate::core::rpc::format::json::encode_str(res.into_value())
					.map_err(|_| RpcError::ParseError)?;
				Ok(AxumResponse::builder()
					.header(CONTENT_TYPE, ContentType::ApplicationJson)
					.body(val.into())
					.unwrap())
			}
			Format::Cbor => {
				let val = crate::core::rpc::format::cbor::encode(res.into_value())
					.map_err(|_| RpcError::ParseError)?;
				Ok(AxumResponse::builder()
					.header(CONTENT_TYPE, ContentType::from(self))
					.body(val.into())
					.unwrap())
			}
			Format::Bincode => {
				let val = crate::core::rpc::format::bincode::encode(&res)
					.map_err(|_| RpcError::ParseError)?;
				Ok(AxumResponse::builder()
					.header(CONTENT_TYPE, ContentType::from(self))
					.body(val.into())
					.unwrap())
			}
			Format::Revision => {
				let val = crate::core::rpc::format::revision::encode(&res)
					.map_err(|_| RpcError::ParseError)?;
				Ok(AxumResponse::builder()
					.header(CONTENT_TYPE, ContentType::from(self))
					.body(val.into())
					.unwrap())
			}
			Format::Unsupported => Err(RpcError::InvalidRequest),
		}
	}
}
