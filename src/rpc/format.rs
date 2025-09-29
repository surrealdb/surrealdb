use axum::extract::ws::Message;
use axum::response::Response as AxumResponse;
use bytes::Bytes;
use http::header::CONTENT_TYPE;
use surrealdb::types::{SurrealValue, Value};
use surrealdb_core::rpc::format::Format;
use surrealdb_core::rpc::request::Request;
use surrealdb_core::rpc::{DbResponse, DbResultError, RpcError};

use crate::net::headers::{Accept, ContentType};

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
	fn req_ws(&self, msg: Message) -> Result<Request, DbResultError>;
	/// Process a WebSocket RPC response
	fn res_ws(&self, res: DbResponse) -> Result<(usize, Message), DbResultError>;
}

impl WsFormat for Format {
	/// Process a WebSocket RPC request
	fn req_ws(&self, msg: Message) -> Result<Request, DbResultError> {
		let val = msg.into_data();
		match self {
			Format::Json => {
				let val = surrealdb_core::rpc::format::json::decode(&val)
					.map_err(|_| RpcError::ParseError)?;
				if let Value::Object(obj) = val {
					Ok(Request::from_object(obj)?)
				} else {
					Err(DbResultError::from(RpcError::ParseError))
				}
			}
			Format::Cbor => {
				let val = surrealdb_core::rpc::format::cbor::decode(&val)
					.map_err(|_| RpcError::ParseError)?;
				if let Value::Object(obj) = val {
					Ok(Request::from_object(obj)?)
				} else {
					Err(DbResultError::from(RpcError::ParseError))
				}
			}
			Format::Bincode => {
				let val = surrealdb_core::rpc::format::bincode::decode(&val)
					.map_err(|_| RpcError::ParseError)?;
				if let Value::Object(obj) = val {
					Ok(Request::from_object(obj)?)
				} else {
					Err(DbResultError::from(RpcError::ParseError))
				}
			}
			Format::Revision => {
				let val = surrealdb_core::rpc::format::revision::decode(&val)
					.map_err(|_| RpcError::ParseError)?;
				if let Value::Object(obj) = val {
					Ok(Request::from_object(obj)?)
				} else {
					Err(DbResultError::from(RpcError::ParseError))
				}
			}
			Format::Unsupported => Err(DbResultError::from(RpcError::InvalidRequest)),
		}
	}

	/// Process a WebSocket RPC response
	fn res_ws(&self, res: DbResponse) -> Result<(usize, Message), DbResultError> {
		match self {
			Format::Json => {
				let val = surrealdb_core::rpc::format::json::encode_str(res.into_value())
					.map_err(|_| RpcError::ParseError)?;
				Ok((val.len(), Message::Text(val)))
			}
			Format::Cbor => {
				let val = surrealdb_core::rpc::format::cbor::encode(res.into_value())
					.map_err(|_| RpcError::ParseError)?;
				Ok((val.len(), Message::Binary(val)))
			}
			Format::Bincode => {
				let val = surrealdb_core::rpc::format::bincode::encode(&res)
					.map_err(|_| RpcError::ParseError)?;
				Ok((val.len(), Message::Binary(val)))
			}
			Format::Revision => {
				let val = surrealdb_core::rpc::format::revision::encode(&res)
					.map_err(|_| RpcError::ParseError)?;
				Ok((val.len(), Message::Binary(val)))
			}
			Format::Unsupported => Err(DbResultError::from(RpcError::InvalidRequest)),
		}
	}
}

pub trait HttpFormat {
	/// Process a HTTP RPC request
	fn req_http(&self, body: Bytes) -> Result<Request, RpcError>;
	/// Process a HTTP RPC response
	fn res_http(&self, res: DbResponse) -> Result<AxumResponse, RpcError>;
}

impl HttpFormat for Format {
	/// Process a HTTP RPC request
	fn req_http(&self, body: Bytes) -> Result<Request, RpcError> {
		match self {
			Format::Json => {
				let val = surrealdb_core::rpc::format::json::decode(&body)
					.map_err(|_| RpcError::ParseError)?;
				if let Value::Object(obj) = val {
					Ok(Request::from_object(obj)?)
				} else {
					Err(RpcError::ParseError)
				}
			}
			Format::Cbor => {
				let val = surrealdb_core::rpc::format::cbor::decode(&body)
					.map_err(|_| RpcError::ParseError)?;
				if let Value::Object(obj) = val {
					Ok(Request::from_object(obj)?)
				} else {
					Err(RpcError::ParseError)
				}
			}
			Format::Bincode => {
				let val = surrealdb_core::rpc::format::bincode::decode(&body)
					.map_err(|_| RpcError::ParseError)?;
				if let Value::Object(obj) = val {
					Ok(Request::from_object(obj)?)
				} else {
					Err(RpcError::ParseError)
				}
			}
			Format::Revision => {
				let val = surrealdb_core::rpc::format::revision::decode(&body)
					.map_err(|_| RpcError::ParseError)?;
				if let Value::Object(obj) = val {
					Ok(Request::from_object(obj)?)
				} else {
					Err(RpcError::ParseError)
				}
			}
			Format::Unsupported => Err(RpcError::InvalidRequest),
		}
	}
	/// Process a HTTP RPC response
	fn res_http(&self, res: DbResponse) -> Result<AxumResponse, RpcError> {
		match self {
			Format::Json => {
				let val = surrealdb_core::rpc::format::json::encode_str(res.into_value())
					.map_err(|_| RpcError::ParseError)?;
				Ok(AxumResponse::builder()
					.header(CONTENT_TYPE, ContentType::ApplicationJson)
					.body(val.into())
					.unwrap())
			}
			Format::Cbor => {
				let val = surrealdb_core::rpc::format::cbor::encode(res.into_value())
					.map_err(|_| RpcError::ParseError)?;
				Ok(AxumResponse::builder()
					.header(CONTENT_TYPE, ContentType::from(self))
					.body(val.into())
					.unwrap())
			}
			Format::Bincode => {
				let val = surrealdb_core::rpc::format::bincode::encode(&res)
					.map_err(|_| RpcError::ParseError)?;
				Ok(AxumResponse::builder()
					.header(CONTENT_TYPE, ContentType::from(self))
					.body(val.into())
					.unwrap())
			}
			Format::Revision => {
				let val = surrealdb_core::rpc::format::revision::encode(&res)
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
