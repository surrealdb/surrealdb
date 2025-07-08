pub mod bincode;
pub mod cbor;
pub mod json;
pub mod revision;

use ::revision::Revisioned;
use serde::Serialize;

use super::{RpcError, request::V1Request};
use crate::{expr::Value, rpc::protocol::v1::types::V1Value, sql::SqlValue};

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

impl std::str::FromStr for Format {
	type Err = RpcError;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		Ok(s.into())
	}
}

pub trait ResTrait: Serialize + Into<V1Value> + Revisioned {}

impl<T: Serialize + Into<V1Value> + Revisioned> ResTrait for T {}

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
	pub fn req(&self, val: impl Into<Vec<u8>>) -> Result<V1Request, RpcError> {
		let val = val.into();
		match self {
			Self::Json => json::req(&val),
			Self::Cbor => cbor::req(val),
			Self::Bincode => bincode::req(&val),
			Self::Revision => revision::req(val),
			Self::Unsupported => Err(RpcError::InvalidRequest("Unsupported format".into())),
		}
	}

	/// Process a response using the specified format
	pub fn res(&self, val: impl ResTrait) -> Result<Vec<u8>, RpcError> {
		match self {
			Self::Json => json::res(val),
			Self::Cbor => cbor::res(val),
			Self::Bincode => bincode::res(val),
			Self::Revision => revision::res(val),
			Self::Unsupported => Err(RpcError::InvalidRequest("Unsupported format".into())),
		}
	}

	/// Process a request using the specified format
	pub fn parse_value(&self, val: impl Into<Vec<u8>>) -> Result<V1Value, RpcError> {
		let val = val.into();
		match self {
			Self::Json => json::parse_value(&val),
			Self::Cbor => cbor::parse_value(val),
			Self::Bincode => bincode::parse_value(&val),
			Self::Revision => revision::parse_value(val),
			Self::Unsupported => Err(RpcError::InvalidRequest("Unsupported format".into())),
		}
	}
}

pub fn parse_expr_value_from_content_type(
	content_type: Option<&str>,
	bytes: Vec<u8>,
) -> Result<Value, RpcError> {
	let parsed = match content_type {
		Some("application/json") => json::parse_value(&bytes),
		Some("application/cbor") => cbor::parse_value(bytes),
		Some("application/surrealdb") => revision::parse_value(bytes),
		_ => return Ok(Value::Bytes(crate::expr::Bytes(bytes))),
	};

	let v1_network_value =
		parsed.map_err(|_| RpcError::InvalidRequest("Invalid request".into()))?;
	let value = v1_network_value
		.try_into()
		.map_err(|_| RpcError::InvalidRequest("Invalid request".into()))?;

	Ok(value)
}
