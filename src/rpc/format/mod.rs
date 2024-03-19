mod bincode;
pub mod cbor;
mod json;
pub mod msgpack;
mod revision;

use crate::net::headers::{Accept, ContentType};
use crate::rpc::failure::Failure;
use crate::rpc::request::Request;
use crate::rpc::response::Response;
use axum::extract::ws::Message;
use axum::response::Response as AxumResponse;
use bytes::Bytes;
use surrealdb::rpc::RpcError;

pub const PROTOCOLS: [&str; 5] = [
	"json",     // For basic JSON serialisation
	"cbor",     // For basic CBOR serialisation
	"msgpack",  // For basic Msgpack serialisation
	"bincode",  // For full internal serialisation
	"revision", // For full versioned serialisation
];

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Format {
	None,        // No format is specified yet
	Json,        // For basic JSON serialisation
	Cbor,        // For basic CBOR serialisation
	Msgpack,     // For basic Msgpack serialisation
	Bincode,     // For full internal serialisation
	Revision,    // For full versioned serialisation
	Unsupported, // Unsupported format
}

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

impl From<&str> for Format {
	fn from(v: &str) -> Self {
		match v {
			s if s == PROTOCOLS[0] => Format::Json,
			s if s == PROTOCOLS[1] => Format::Cbor,
			s if s == PROTOCOLS[2] => Format::Msgpack,
			s if s == PROTOCOLS[3] => Format::Bincode,
			s if s == PROTOCOLS[4] => Format::Revision,
			_ => Format::None,
		}
	}
}

impl Format {
	/// Check if this format has been set
	pub fn is_none(&self) -> bool {
		matches!(self, Format::None)
	}
	/// Process a request using the specified format
	pub fn req_ws(&self, msg: Message) -> Result<Request, Failure> {
		match self {
			Self::None => unreachable!(), // We should never arrive at this code
			Self::Unsupported => unreachable!(), // We should never arrive at this code
			Self::Json => json::req_ws(msg),
			Self::Cbor => cbor::req_ws(msg),
			Self::Msgpack => msgpack::req_ws(msg),
			Self::Bincode => bincode::req_ws(msg),
			Self::Revision => revision::req_ws(msg),
		}
		.map_err(Into::into)
	}
	/// Process a response using the specified format
	pub fn res_ws(&self, res: Response) -> Result<(usize, Message), Failure> {
		match self {
			Self::None => unreachable!(), // We should never arrive at this code
			Self::Unsupported => unreachable!(), // We should never arrive at this code
			Self::Json => json::res_ws(res),
			Self::Cbor => cbor::res_ws(res),
			Self::Msgpack => msgpack::res_ws(res),
			Self::Bincode => bincode::res_ws(res),
			Self::Revision => revision::res_ws(res),
		}
		.map_err(Into::into)
	}
	/// Process a request using the specified format
	pub fn req_http(&self, body: Bytes) -> Result<Request, RpcError> {
		match self {
			Self::None => unreachable!(), // We should never arrive at this code
			Self::Unsupported => unreachable!(), // We should never arrive at this code
			Self::Json => json::req_http(&body),
			Self::Cbor => cbor::req_http(body),
			Self::Msgpack => msgpack::req_http(body),
			Self::Bincode => bincode::req_http(&body),
			Self::Revision => revision::req_http(body),
		}
	}
	/// Process a response using the specified format
	pub fn res_http(&self, res: Response) -> Result<AxumResponse, RpcError> {
		match self {
			Self::None => unreachable!(), // We should never arrive at this code
			Self::Unsupported => unreachable!(), // We should never arrive at this code
			Self::Json => json::res_http(res),
			Self::Cbor => cbor::res_http(res),
			Self::Msgpack => msgpack::res_http(res),
			Self::Bincode => bincode::res_http(res),
			Self::Revision => revision::res_http(res),
		}
	}
}
