pub mod bincode;
pub mod cbor;
pub mod json;
pub mod revision;

use ::revision::Revisioned;
use bytes::Bytes;
use serde::Serialize;

use super::{RpcError, request::Request};
use crate::expr::Value;

pub const PROTOCOLS: [&str; 4] = [
	"json",     // For basic JSON serialisation
	"cbor",     // For basic CBOR serialisation
	"bincode",  // For full internal serialisation
	"revision", // For full versioned serialisation
];

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[non_exhaustive]
pub enum Format {
	Json,        // For basic JSON serialisation
	Cbor,        // For basic CBOR serialisation
	Bincode,     // For full internal serialisation
	Revision,    // For full versioned serialisation
	Unsupported, // Unsupported format
}

pub trait ResTrait: Serialize + Into<Value> + Revisioned {}

impl<T: Serialize + Into<Value> + Revisioned> ResTrait for T {}

impl From<&str> for Format {
	fn from(v: &str) -> Self {
		match v {
			s if s == PROTOCOLS[0] => Format::Json,
			s if s == PROTOCOLS[1] => Format::Cbor,
			s if s == PROTOCOLS[2] => Format::Bincode,
			s if s == PROTOCOLS[3] => Format::Revision,
			_ => Format::Unsupported,
		}
	}
}

impl Format {
	/// Process a request using the specified format
	pub fn req(&self, val: Bytes) -> Result<Request, RpcError> {
		match self {
			Self::Json => json::req(&val),
			Self::Cbor => cbor::parse_request(&val),
			Self::Bincode => bincode::req(&val),
			Self::Revision => revision::req(&val),
			Self::Unsupported => Err(RpcError::InvalidRequest),
		}
	}

	/// Process a response using the specified format
	pub fn res(&self, val: impl ResTrait) -> Result<Vec<u8>, RpcError> {
		match self {
			Self::Json => json::res(val),
			Self::Cbor => cbor::res(val),
			Self::Bincode => bincode::res(val),
			Self::Revision => revision::res(val),
			Self::Unsupported => Err(RpcError::InvalidRequest),
		}
	}

	/// Process a request using the specified format
	pub fn parse_value(&self, val: Bytes) -> Result<Value, RpcError> {
		match self {
			Self::Json => json::parse_value(&val),
			Self::Cbor => cbor::parse_value(&val),
			Self::Bincode => bincode::parse_value(&val),
			Self::Revision => revision::parse_value(&val),
			Self::Unsupported => Err(RpcError::InvalidRequest),
		}
	}
}
