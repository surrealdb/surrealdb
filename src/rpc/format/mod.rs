mod bincode;
pub mod cbor;
mod json;
pub mod msgpack;
mod revision;

use crate::rpc::failure::Failure;
use crate::rpc::request::Request;
use crate::rpc::response::Response;
use axum::extract::ws::Message;

pub const PROTOCOLS: [&str; 5] = [
	"json",     // For basic JSON serialisation
	"cbor",     // For basic CBOR serialisation
	"msgpack",  // For basic Msgpack serialisation
	"bincode",  // For full internal serialisation
	"revision", // For full versioned serialisation
];

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Format {
	None,     // No format is specified yet
	Json,     // For basic JSON serialisation
	Cbor,     // For basic CBOR serialisation
	Msgpack,  // For basic Msgpack serialisation
	Bincode,  // For full internal serialisation
	Revision, // For full versioned serialisation
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
	pub fn req(&self, msg: Message) -> Result<Request, Failure> {
		match self {
			Self::None => unreachable!(), // We should never arrive at this code
			Self::Json => json::req(msg),
			Self::Cbor => cbor::req(msg),
			Self::Msgpack => msgpack::req(msg),
			Self::Bincode => bincode::req(msg),
			Self::Revision => revision::req(msg),
		}
	}
	/// Process a response using the specified format
	pub fn res(&self, res: Response) -> Result<(usize, Message), Failure> {
		match self {
			Self::None => unreachable!(), // We should never arrive at this code
			Self::Json => json::res(res),
			Self::Cbor => cbor::res(res),
			Self::Msgpack => msgpack::res(res),
			Self::Bincode => bincode::res(res),
			Self::Revision => revision::res(res),
		}
	}
}
