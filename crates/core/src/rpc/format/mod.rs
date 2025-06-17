pub mod bincode;
pub mod cbor;
pub mod json;
pub mod revision;
pub mod proto;

use ::revision::Revisioned;
use serde::Serialize;

use super::{RpcError, request::Request};
use crate::{expr::Value, sql::SqlValue};
use crate::proto::surrealdb::ast::SqlValue as SqlValueProto;
use crate::proto::surrealdb::value::Value as ValueProto;

pub const PROTOCOLS: [&str; 3] = [
	"json",     // For basic JSON serialisation
	"cbor",     // For basic CBOR serialisation
	"protobuf",  // For full internal serialisation
];

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Format {
	Json,        // Self describing JSON serialisation
	Cbor,        // Self describing CBOR serialisation
	Protobuf,	// For full protobuf serialisation
	Unsupported, // Unsupported format
}

impl From<&str> for Format {
	fn from(v: &str) -> Self {
		match v {
			s if s == PROTOCOLS[0] => Format::Json,
			s if s == PROTOCOLS[1] => Format::Cbor,
			s if s == PROTOCOLS[2] => Format::Protobuf,
			_ => Format::Unsupported,
		}
	}
}

impl Format {
	/// Process a request using the specified format
	pub fn req(&self, val: impl Into<Vec<u8>>) -> Result<Request, RpcError> {
		let val = val.into();
		match self {
			Self::Json => json::req(&val),
			Self::Cbor => cbor::req(val),
			Self::Protobuf => proto::req(&val),
			Self::Unsupported => Err(RpcError::InvalidRequest),
		}
	}

	/// Process a response using the specified format
	pub fn res(&self, val: ValueProto) -> Result<Vec<u8>, RpcError> {
		match self {
			Self::Json => json::res(val),
			Self::Cbor => cbor::res(val),
			Self::Protobuf => proto::res(val),
			Self::Unsupported => Err(RpcError::InvalidRequest),
		}
	}

	/// Process a request using the specified format
	pub fn parse_value(&self, val: impl Into<Vec<u8>>) -> Result<SqlValue, RpcError> {
		let val = val.into();
		match self {
			Self::Json => json::parse_value(&val),
			Self::Cbor => cbor::parse_value(val),
			Self::Protobuf => proto::parse_value(&val),
			Self::Unsupported => Err(RpcError::InvalidRequest),
		}
	}
}
