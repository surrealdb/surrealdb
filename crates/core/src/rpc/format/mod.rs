pub mod bincode;
pub mod cbor;
pub mod json;
pub mod revision;

use ::revision::Revisioned;
use serde::Serialize;

use super::RpcError;
use super::request::Request;
use crate::val::Value;

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

impl From<&str> for Format {
	fn from(v: &str) -> Self {
		match v {
			"json" => Format::Json,
			"cbor" => Format::Cbor,
			"bincode" => Format::Bincode,
			"revision" => Format::Revision,
			_ => Format::Unsupported,
		}
	}
}

impl Format {
	/// Process a request using the specified format
	pub fn req(&self, val: &[u8]) -> Result<Request, RpcError> {
		let val = self.parse_value(val)?;
		let object = val.into_object().ok_or(RpcError::InvalidRequest)?;
		Request::from_object(object)
	}

	/// Process a response using the specified format
	pub fn res(&self, val: Value) -> Result<Vec<u8>, RpcError> {
		match self {
			Self::Json => json::encode(val),
			Self::Cbor => cbor::encode(val),
			Self::Bincode => bincode::encode(&val),
			Self::Revision => revision::encode(&val),
			Self::Unsupported => Err(RpcError::InvalidRequest),
		}
	}

	/// Process a request using the specified format
	pub fn parse_value(&self, val: &[u8]) -> Result<Value, RpcError> {
		let val = val.into();
		match self {
			Self::Json => json::decode(val),
			Self::Cbor => cbor::decode(val),
			Self::Bincode => bincode::decode(val),
			Self::Revision => revision::decode(val),
			Self::Unsupported => Err(RpcError::InvalidRequest),
		}
	}
}
