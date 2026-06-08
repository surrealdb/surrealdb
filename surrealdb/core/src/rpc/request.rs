use std::collections::HashMap;
use std::str::FromStr;

use surrealdb_types::Error as TypesError;

use crate::rpc::{Method, invalid_request};
use crate::types::{PublicArray, PublicNumber, PublicObject, PublicUuid, PublicValue};

pub static ID: &str = "id";
pub static METHOD: &str = "method";
pub static PARAMS: &str = "params";
pub static VERSION: &str = "version";
pub static TXN: &str = "txn";
pub static SESSION_ID: &str = "session";
pub static TRACE_CONTEXT: &str = "trace_context";

#[derive(Debug)]
pub struct Request {
	pub id: Option<PublicValue>,
	pub version: Option<u8>,
	pub session_id: Option<PublicUuid>,
	pub txn: Option<PublicUuid>,
	pub method: Method,
	pub params: PublicArray,
	/// Optional W3C Trace Context propagation headers carried in the
	/// RPC envelope itself, used by transports that don't have their
	/// own header layer (notably WebSocket). When present, the server
	/// layer uses this map as the OTel parent for the per-message
	/// span. Keys are propagation header names (e.g. `traceparent`,
	/// `tracestate`); values are the corresponding header values. The
	/// wire-level field is named `trace_context` to match the W3C
	/// spec title ("W3C Trace Context") and to make clear the field
	/// carries the full propagation context, not just a trace id.
	pub trace_context: Option<HashMap<String, String>>,
}

impl Request {
	/// Create a request by extracting the request fields from an surealql
	/// object.
	pub fn from_object(mut obj: PublicObject) -> Result<Self, TypesError> {
		// Fetch the 'id' argument

		let id = obj.remove("id");
		let id = match id {
			None | Some(PublicValue::None) => None,
			Some(
				PublicValue::Null
				| PublicValue::Uuid(_)
				| PublicValue::Number(_)
				| PublicValue::String(_)
				| PublicValue::Datetime(_),
			) => id,
			_ => return Err(invalid_request()),
		};

		// Fetch the 'version' argument
		let version = match obj.remove(VERSION) {
			None | Some(PublicValue::None | PublicValue::Null) => None,
			Some(PublicValue::Number(v)) => match v {
				PublicNumber::Int(1) => Some(1),
				PublicNumber::Int(2) => Some(2),
				_ => return Err(invalid_request()),
			},
			_ => return Err(invalid_request()),
		};

		// Fetch the 'txn' argument
		let session_id = match obj.remove(SESSION_ID) {
			None | Some(PublicValue::None | PublicValue::Null) => None,
			Some(PublicValue::Uuid(x)) => Some(x),
			Some(PublicValue::String(x)) => {
				Some(PublicUuid::from_str(x.as_str()).map_err(|_| invalid_request())?)
			}
			_ => return Err(invalid_request()),
		};

		// Fetch the 'txn' argument
		let txn = match obj.remove(TXN) {
			None | Some(PublicValue::None | PublicValue::Null) => None,
			Some(PublicValue::Uuid(x)) => Some(x),
			Some(PublicValue::String(x)) => {
				Some(PublicUuid::from_str(x.as_str()).map_err(|_| invalid_request())?)
			}
			_ => return Err(invalid_request()),
		};

		// Fetch the 'method' argument
		let method = match obj.remove(METHOD) {
			Some(PublicValue::String(v)) => v,
			_ => return Err(invalid_request()),
		};
		// Fetch the 'params' argument
		let params = match obj.remove(PARAMS) {
			Some(PublicValue::Array(v)) => v,
			_ => PublicArray::new(),
		};
		// Fetch the optional 'trace_context' argument carrying W3C Trace
		// Context propagation headers. Accept either a bare string
		// (treated as the `traceparent` value, for convenience) or an
		// object whose entries are propagation header names → values.
		// Non-string entries inside the object are skipped silently so a
		// stray `tracestate: null` doesn't fail the whole request.
		let trace_context = match obj.remove(TRACE_CONTEXT) {
			None | Some(PublicValue::None | PublicValue::Null) => None,
			Some(PublicValue::String(v)) => {
				let mut map = HashMap::with_capacity(1);
				map.insert("traceparent".to_string(), v);
				Some(map)
			}
			Some(PublicValue::Object(o)) => {
				let map: HashMap<String, String> = o
					.into_inner()
					.into_iter()
					.filter_map(|(k, v)| match v {
						PublicValue::String(s) => Some((k, s)),
						_ => None,
					})
					.collect();
				if map.is_empty() {
					None
				} else {
					Some(map)
				}
			}
			_ => return Err(invalid_request()),
		};
		// Parse the specified method
		let method = Method::parse_case_sensitive(method);
		// Return the parsed request
		Ok(Request {
			id,
			method,
			params,
			version,
			txn,
			session_id,
			trace_context,
		})
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_types::object;

	use super::*;

	fn ping_with_trace_context(trace_context: PublicValue) -> PublicObject {
		object! {
			id: 1,
			method: "ping",
			trace_context: trace_context,
		}
	}

	#[test]
	fn trace_context_absent_is_none() {
		let obj = object! { id: 1, method: "ping" };
		let req = Request::from_object(obj).unwrap();
		assert!(req.trace_context.is_none());
	}

	#[test]
	fn trace_context_null_is_none() {
		let req = Request::from_object(ping_with_trace_context(PublicValue::Null)).unwrap();
		assert!(req.trace_context.is_none());
	}

	#[test]
	fn trace_context_none_is_none() {
		let req = Request::from_object(ping_with_trace_context(PublicValue::None)).unwrap();
		assert!(req.trace_context.is_none());
	}

	#[test]
	fn trace_context_string_becomes_traceparent() {
		let traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
		let req = Request::from_object(ping_with_trace_context(PublicValue::String(
			traceparent.to_string(),
		)))
		.unwrap();
		let trace_context = req.trace_context.expect("trace_context populated");
		assert_eq!(trace_context.len(), 1);
		assert_eq!(trace_context.get("traceparent").map(String::as_str), Some(traceparent));
	}

	#[test]
	fn trace_context_object_keeps_string_entries() {
		let obj = object! {
			id: 1,
			method: "ping",
			trace_context: object! {
				traceparent: "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
				tracestate: "vendor=foo",
			},
		};
		let req = Request::from_object(obj).unwrap();
		let trace_context = req.trace_context.expect("trace_context populated");
		assert_eq!(trace_context.len(), 2);
		assert_eq!(
			trace_context.get("traceparent").map(String::as_str),
			Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"),
		);
		assert_eq!(trace_context.get("tracestate").map(String::as_str), Some("vendor=foo"));
	}

	#[test]
	fn trace_context_object_drops_non_string_entries() {
		let obj = object! {
			id: 1,
			method: "ping",
			trace_context: object! {
				traceparent: "00-aaaa-bbbb-01",
				answer: 42_i64,
			},
		};
		let req = Request::from_object(obj).unwrap();
		let trace_context = req.trace_context.expect("trace_context populated");
		assert_eq!(trace_context.len(), 1);
		assert!(trace_context.contains_key("traceparent"));
	}

	#[test]
	fn trace_context_object_with_only_non_string_entries_is_none() {
		let obj = object! {
			id: 1,
			method: "ping",
			trace_context: object! { answer: 42_i64 },
		};
		let req = Request::from_object(obj).unwrap();
		assert!(req.trace_context.is_none());
	}

	#[test]
	fn trace_context_with_unsupported_kind_is_invalid_request() {
		let obj = object! {
			id: 1,
			method: "ping",
			trace_context: 42_i64,
		};
		assert!(Request::from_object(obj).is_err());
	}
}
