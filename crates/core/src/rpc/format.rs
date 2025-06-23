use ::revision::Revisioned;
use serde::Serialize;

use super::{RpcError, request::Request};
use crate::{
	expr::Value,
	protocol::{
		ToFlatbuffers,
		flatbuffers::surreal_db::protocol::{
			expr::Value as ValueFb,
			rpc::{QueryResult, QueryResultArgs, Response, ResponseArgs},
		},
	},
	sql::SqlValue,
};

// pub const PROTOCOLS: [&str; 2] = [
// 	"json",        // For basic JSON serialisation
// 	"flatbuffers", // For basic Flatbuffers serialisation
// ];

// #[derive(Debug, Clone, Copy, Eq, PartialEq)]
// pub enum Format {
// 	Json,        // Self describing JSON serialisation
// 	Flatbuffer,  // For full flatbuffer serialisation
// 	Unsupported, // Unsupported format
// }

// impl From<&str> for Format {
// 	fn from(v: &str) -> Self {
// 		match v {
// 			s if s == PROTOCOLS[0] => Format::Json,
// 			s if s == PROTOCOLS[1] => Format::Flatbuffer,
// 			_ => Format::Unsupported,
// 		}
// 	}
// }

// impl Format {
// 	/// Process a request using the specified format
// 	pub fn req(&self, bytes: &[u8]) -> Result<Request<'_>, RpcError> {
// 		let req = flatbuffers::root::<Request>(bytes)
// 			.map_err(|e| RpcError::InvalidRequest)?;
// 	}

// 	/// Process a response using the specified format
// 	pub fn res(&self, val: Value) -> Result<Vec<u8>, RpcError> {
// 		let mut fbb = flatbuffers::FlatBufferBuilder::new();

// 		let fb_value = val.to_fb(&mut fbb);

// 		let query_response = QueryResponse::create(
// 			&mut fbb,
// 			&QueryResponseArgs {
// 				index: 0,
// 				stats: None,
// 				result: Some(&fb_value),
// 			},
// 		);

// 		let results = fbb.create_vector(&[query_response]);

// 		let response = Response::create(
// 			&mut fbb,
// 			&ResponseArgs {
// 				id: None,
// 				results: Some(&results),
// 			},
// 		);
// 	}

// 	/// Process a request using the specified format
// 	pub fn parse_value(&self, val: &[u8]) -> Result<Value, RpcError> {
// 		match self {
// 			Format::Json => serde_json::from_slice(val).map_err(|e| RpcError::InvalidRequest),
// 			Format::Flatbuffer => {
// 				let fb_value = flatbuffers::root::<ValueFb>(val)
// 					.map_err(|e| RpcError::InvalidRequest(e.to_string()))?;
// 				Value::try_from(fb_value).map_err(|_| {
// 					RpcError::InvalidRequest("Failed to convert Flatbuffer value".into())
// 				})
// 			}
// 			Format::Unsupported => Err(RpcError::UnsupportedFormat),
// 		}
// 	}
// }
