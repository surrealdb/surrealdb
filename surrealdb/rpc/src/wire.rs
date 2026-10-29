//! The wire format the Rust SDK speaks to a server over WebSocket and HTTP: CBOR.
//!
//! The SDK offers `cbor.2` and falls back to `cbor`, so it works with a server that predates
//! `cbor.2`. It writes requests in plain `cbor` whichever the server selects: a `cbor.2` decoder
//! reads plain `cbor`, and a request carries no sized number to lose, since the SDK's requests
//! widen them. It reads responses with the `cbor.2` decoder, which also reads plain `cbor`, so a
//! server that selected `cbor.2` answers with sized numbers kept and one that did not answers
//! with them widened.
//!
//! A server writes record ids as tag 8 around an array, never as text, so a decode here takes no
//! SurrealQL parser and refuses a record id written as text.

use surrealdb_types::Value;

use crate::format;

/// The WebSocket subprotocols the SDK offers, most preferred first.
pub const PROTOCOLS: [&str; 2] = ["cbor.2", "cbor"];

/// [`PROTOCOLS`] as one `Sec-WebSocket-Protocol` header value.
pub const PROTOCOLS_HEADER: &str = "cbor.2, cbor";

/// The HTTP `Accept` the SDK sends: a server that cannot answer `cbor.2` answers `cbor`.
pub const ACCEPT: &str = format::CBOR2;

/// The HTTP `Content-Type` of a request body, which [`encode`] writes.
pub const CONTENT_TYPE: &str = format::CBOR;

/// How deeply a decoded value may nest.
const RECURSION_LIMIT: usize = 512;

/// `value` as a request body, in plain `cbor`.
pub fn encode(value: Value) -> anyhow::Result<Vec<u8>> {
	surrealdb_types::cbor::encode(value)
}

/// The value a response carries, in `cbor.2` or plain `cbor`.
pub fn decode(bytes: &[u8]) -> anyhow::Result<Value> {
	surrealdb_types::cbor::decode_v2(bytes, RECURSION_LIMIT, None)
}

/// Where a response is addressed: the `id` and `session` of its envelope, read without decoding
/// its result. `None` where the bytes are not an envelope this reads, which [`decode`] then
/// reads as it reads any response.
pub fn response_route(bytes: &[u8]) -> Option<ResponseRoute> {
	let (entries, _) = surrealdb_types::cbor::map_entries(bytes, RECURSION_LIMIT)?;
	let mut route = ResponseRoute::default();
	for (key, value) in entries {
		match key {
			"id" => route.id = Some(decode(value).ok()?),
			key if key == crate::request::SESSION_ID => {
				let Value::Uuid(id) = decode(value).ok()? else {
					return None;
				};
				route.session_id = Some(id.into());
			}
			_ => {}
		}
	}
	Some(route)
}

/// The `id` and `session` of a response's envelope ([`response_route`]).
#[derive(Debug, Default, PartialEq)]
pub struct ResponseRoute {
	pub id: Option<Value>,
	pub session_id: Option<uuid::Uuid>,
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn a_route_is_the_envelope_a_whole_decode_reads() {
		use surrealdb_types::SurrealValue;

		use crate::{DbResponse, DbResult};
		let session = uuid::Uuid::from_u128(9);
		for (id, session_id) in [
			(Some(Value::from_t(3i64)), Some(session)),
			(None, Some(session)),
			(Some(Value::from_t(4i64)), None),
		] {
			let result = DbResult::Other(Value::from_t("a value"));
			let bytes =
				encode(DbResponse::success(id.clone(), session_id, result).into_value()).unwrap();
			let decoded = crate::db_response_from_bytes(&bytes).unwrap();
			let route = response_route(&bytes).expect("an envelope");
			assert_eq!(route.id, decoded.id);
			assert_eq!(route.session_id, decoded.session_id);
			assert_eq!(
				route,
				ResponseRoute {
					id,
					session_id
				}
			);
		}
		assert_eq!(response_route(&encode(Value::from_t(1i64)).unwrap()), None);
	}

	#[test]
	fn the_header_offers_the_protocols() {
		assert_eq!(PROTOCOLS_HEADER, PROTOCOLS.join(", "));
	}
}
