use std::str::FromStr;

use super::{RpcError, request::Request};
use crate::protocol::FromFlatbuffers;
use crate::{
	dbs::ResponseData,
	expr::Value,
	protocol::{
		ToFlatbuffers,
	},
	rpc::Response,
};
use surrealdb_protocol::proto::v1::Value as ValueProto;



const FLATBUFFERS_PROTOCOL: &str = "flatbuffers";
const JSON_PROTOCOL: &str = "json";

pub const PROTOCOLS: [&str; 2] = [FLATBUFFERS_PROTOCOL, JSON_PROTOCOL];

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Format {
	Json,       // Self describing JSON serialisation
	Protobuf, // For full flatbuffer serialisation
}

impl FromStr for Format {
	type Err = RpcError;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			JSON_PROTOCOL => Ok(Format::Json),
			FLATBUFFERS_PROTOCOL => Ok(Format::Protobuf),
			unsupported => {
				Err(RpcError::InvalidRequest(format!("Unsupported format: {unsupported}")))
			}
		}
	}
}

impl Format {
	/// Process a request using the specified format
	pub fn req(&self, bytes: &[u8]) -> Result<Request, RpcError> {
		match self {
			Format::Json => {
				let req: Request = serde_json::from_slice(bytes)
					.map_err(|e| RpcError::InvalidRequest(e.to_string()))?;
				Ok(req)
			}
			Format::Protobuf => {
				todo!("STU: Remove protobuf from here and add back CBOR")
			}
		}
	}

	/// Process a response using the specified format
	pub fn res(&self, response: Response) -> Result<Vec<u8>, RpcError> {
		match self {
			Format::Json => {
				let json =
					serde_json::to_vec(&response).map_err(|e| RpcError::Thrown(e.to_string()))?;
				Ok(json)
			}
			Format::Protobuf => {
				todo!("STU: Remove protobuf from here and add back CBOR")
			}
		}
	}

	/// Process a request using the specified format
	pub fn parse_value(&self, val: &[u8]) -> Result<Value, RpcError> {
		match self {
			Format::Json => {
				serde_json::from_slice(val).map_err(|e| RpcError::InvalidRequest(e.to_string()))
			}
			Format::Protobuf => {
				todo!("STU: Remove protobuf from here and add back CBOR")
			}
		}
	}
}
