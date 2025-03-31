pub mod bincode;
pub mod cbor;
pub mod json;
pub mod msgpack;
pub mod revision;

use ::revision::Revisioned;
use serde::Serialize;

use super::{request::Request, RpcError};
use crate::sql::Value;

pub const PROTOCOLS: [&str; 5] = [
	"json",     // For basic JSON serialisation
	"cbor",     // For basic CBOR serialisation
	"msgpack",  // For basic Msgpack serialisation
	"bincode",  // For full internal serialisation
	"revision", // For full versioned serialisation
];

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[non_exhaustive]
pub enum Format {
	Json,        // For basic JSON serialisation
	Cbor,        // For basic CBOR serialisation
	Msgpack,     // For basic Msgpack serialisation
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
			s if s == PROTOCOLS[2] => Format::Msgpack,
			s if s == PROTOCOLS[3] => Format::Bincode,
			s if s == PROTOCOLS[4] => Format::Revision,
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
			Self::Msgpack => msgpack::req(val),
			Self::Bincode => bincode::req(&val),
			Self::Revision => revision::req(val),
			Self::Unsupported => Err(RpcError::InvalidRequest),
		}
	}

	/// Process a response using the specified format
	pub fn res(&self, val: impl ResTrait) -> Result<Vec<u8>, RpcError> {
		match self {
			Self::Json => json::res(val),
			Self::Cbor => cbor::res(val),
			Self::Msgpack => msgpack::res(val),
			Self::Bincode => bincode::res(val),
			Self::Revision => revision::res(val),
			Self::Unsupported => Err(RpcError::InvalidRequest),
		}
	}

	/// Process a request using the specified format
	pub fn parse_value(&self, val: impl Into<Vec<u8>>) -> Result<Value, RpcError> {
		let val = val.into();
		match self {
			Self::Json => json::parse_value(&val),
			Self::Cbor => cbor::parse_value(val),
			Self::Msgpack => msgpack::parse_value(val),
			Self::Bincode => bincode::parse_value(&val),
			Self::Revision => revision::parse_value(val),
			Self::Unsupported => Err(RpcError::InvalidRequest),
		}
	}
}
