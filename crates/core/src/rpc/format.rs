use std::str::FromStr;

use super::{RpcError, request::Request};
use crate::protocol::FromFlatbuffers;
use crate::{
	dbs::ResponseData,
	expr::Value,
	protocol::{
		ToFlatbuffers,
		flatbuffers::surreal_db::protocol::{expr::Value as ValueFb, rpc as rpc_fb},
	},
	rpc::Response,
};

const FLATBUFFERS_PROTOCOL: &str = "flatbuffers";
const JSON_PROTOCOL: &str = "json";

pub const PROTOCOLS: [&str; 2] = [FLATBUFFERS_PROTOCOL, JSON_PROTOCOL];

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Format {
	Json,       // Self describing JSON serialisation
	Flatbuffer, // For full flatbuffer serialisation
}

impl FromStr for Format {
	type Err = RpcError;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			JSON_PROTOCOL => Ok(Format::Json),
			FLATBUFFERS_PROTOCOL => Ok(Format::Flatbuffer),
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
			Format::Flatbuffer => {
				let req = flatbuffers::root::<rpc_fb::Request>(bytes)
					.map_err(|e| RpcError::InvalidRequest(e.to_string()))?;

				Request::from_fb(req).map_err(|_| {
					RpcError::InvalidRequest("Failed to convert Flatbuffer request".into())
				})
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
			Format::Flatbuffer => {
				let mut builder = flatbuffers::FlatBufferBuilder::new();
				let response_fb = response.to_fb(&mut builder);
				builder.finish_minimal(response_fb);
				Ok(builder.finished_data().to_vec())
			}
		}
	}

	/// Process a request using the specified format
	pub fn parse_value(&self, val: &[u8]) -> Result<Value, RpcError> {
		match self {
			Format::Json => {
				serde_json::from_slice(val).map_err(|e| RpcError::InvalidRequest(e.to_string()))
			}
			Format::Flatbuffer => {
				let fb_value = flatbuffers::root::<ValueFb>(val)
					.map_err(|e| RpcError::InvalidRequest(e.to_string()))?;
				Value::from_fb(fb_value).map_err(|_| {
					RpcError::InvalidRequest("Failed to convert Flatbuffer value".into())
				})
			}
		}
	}
}
