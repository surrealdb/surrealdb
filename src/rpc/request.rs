use axum::extract::ws::Message;
use surrealdb::sql::{serde::deserialize, Array, Value};

use once_cell::sync::Lazy;
use surrealdb::sql::Part;

use super::response::{Failure, OutputFormat};

pub static ID: Lazy<[Part; 1]> = Lazy::new(|| [Part::from("id")]);
pub static METHOD: Lazy<[Part; 1]> = Lazy::new(|| [Part::from("method")]);
pub static PARAMS: Lazy<[Part; 1]> = Lazy::new(|| [Part::from("params")]);

pub struct Request {
	pub id: Option<Value>,
	pub method: String,
	pub params: Array,
	pub size: usize,
	pub out_fmt: Option<OutputFormat>,
}

/// Parse the RPC request
pub async fn parse_request(msg: Message) -> Result<Request, Failure> {
	let mut out_fmt = None;
	let (req, size) = match msg {
		// This is a binary message
		Message::Binary(val) => {
			// Use binary output
			out_fmt = Some(OutputFormat::Full);

			match deserialize(&val) {
				Ok(v) => (v, val.len()),
				Err(_) => {
					debug!("Error when trying to deserialize the request");
					return Err(Failure::PARSE_ERROR);
				}
			}
		}
		// This is a text message
		Message::Text(ref val) => {
			// Parse the SurrealQL object
			match surrealdb::sql::value(val) {
				// The SurrealQL message parsed ok
				Ok(v) => (v, val.len()),
				// The SurrealQL message failed to parse
				_ => return Err(Failure::PARSE_ERROR),
			}
		}
		// Unsupported message type
		_ => {
			debug!("Unsupported message type: {:?}", msg);
			return Err(Failure::custom("Unsupported message type"));
		}
	};

	// Fetch the 'id' argument
	let id = match req.pick(&*ID) {
		v if v.is_none() => None,
		v if v.is_null() => Some(v),
		v if v.is_uuid() => Some(v),
		v if v.is_number() => Some(v),
		v if v.is_strand() => Some(v),
		v if v.is_datetime() => Some(v),
		_ => return Err(Failure::INVALID_REQUEST),
	};
	// Fetch the 'method' argument
	let method = match req.pick(&*METHOD) {
		Value::Strand(v) => v.to_raw(),
		_ => return Err(Failure::INVALID_REQUEST),
	};

	// Fetch the 'params' argument
	let params = match req.pick(&*PARAMS) {
		Value::Array(v) => v,
		_ => Array::new(),
	};

	Ok(Request {
		id,
		method,
		params,
		size,
		out_fmt,
	})
}
