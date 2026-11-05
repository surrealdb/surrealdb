//! The wire format the Rust SDK speaks to a server over WebSocket and HTTP: CBOR.
//!
//! The SDK offers `cbor.2` and falls back to `cbor`, so it works with a server that predates
//! `cbor.2`. It writes requests in plain `cbor` whichever the server selects: a `cbor.2` decoder
//! reads plain `cbor`, and a request carries no sized number to lose, since the SDK's requests
//! widen them. A request holding a value plain `cbor` has no form for (a regex) is written in
//! `cbor.2` instead, over HTTP with the `cbor.2` content type; a server that reads only plain
//! `cbor` refuses it as a tag it does not accept. It reads responses with the `cbor.2` decoder,
//! which also reads plain `cbor`, so a server that selected `cbor.2` answers with sized numbers
//! kept and one that did not answers with them widened.
//!
//! A server writes record ids as tag 8 around an array, never as text, so a decode here takes no
//! SurrealQL parser and refuses a record id written as text.

use std::ops::Bound;

use surrealdb_types::{RecordIdKey, Value};

use crate::format;

/// The WebSocket subprotocols the SDK offers, most preferred first.
pub const PROTOCOLS: [&str; 2] = ["cbor.2", "cbor"];

/// [`PROTOCOLS`] as one `Sec-WebSocket-Protocol` header value.
pub const PROTOCOLS_HEADER: &str = "cbor.2, cbor";

/// The HTTP `Accept` the SDK sends: a server that cannot answer `cbor.2` answers `cbor`.
pub const ACCEPT: &str = format::CBOR2;

/// The HTTP `Content-Type` of a request body in plain `cbor`, which [`encode`] writes for any
/// request that does not need `cbor.2` ([`RequestBody::content_type`]).
pub const CONTENT_TYPE: &str = format::CBOR;

/// How deeply a decoded value may nest.
const RECURSION_LIMIT: usize = 512;

/// `value` as a request body, in plain `cbor` unless it holds a value only `cbor.2` carries.
pub fn encode(value: Value) -> anyhow::Result<Vec<u8>> {
	encode_body(value).map(|body| body.bytes)
}

/// A request body and the format it is written in.
pub struct RequestBody {
	pub bytes: Vec<u8>,
	/// The HTTP `Content-Type` naming the body's format.
	pub content_type: &'static str,
}

/// `value` as a request body: plain `cbor`, or `cbor.2` when it holds a value plain `cbor` has
/// no form for.
pub fn encode_body(value: Value) -> anyhow::Result<RequestBody> {
	if needs_v2(&value) {
		Ok(RequestBody {
			bytes: surrealdb_types::cbor::encode_v2(value)?,
			content_type: format::CBOR2,
		})
	} else {
		Ok(RequestBody {
			bytes: surrealdb_types::cbor::encode(value)?,
			content_type: CONTENT_TYPE,
		})
	}
}

/// Whether `value` holds a regex, which plain `cbor` cannot encode.
fn needs_v2(value: &Value) -> bool {
	fn in_key(key: &RecordIdKey) -> bool {
		match key {
			RecordIdKey::Number(_) | RecordIdKey::String(_) | RecordIdKey::Uuid(_) => false,
			RecordIdKey::Array(v) => v.iter().any(needs_v2),
			RecordIdKey::Object(v) => v.values().any(needs_v2),
			RecordIdKey::Range(r) => bound(r.start(), in_key) || bound(r.end(), in_key),
		}
	}
	fn bound<T>(b: Bound<&T>, f: impl Fn(&T) -> bool) -> bool {
		match b {
			Bound::Included(v) | Bound::Excluded(v) => f(v),
			Bound::Unbounded => false,
		}
	}
	match value {
		Value::Regex(_) => true,
		Value::Array(v) => v.iter().any(needs_v2),
		Value::Object(v) => v.values().any(needs_v2),
		Value::Set(v) => v.iter().any(needs_v2),
		Value::Range(r) => bound(r.start(), needs_v2) || bound(r.end(), needs_v2),
		Value::RecordId(r) => in_key(&r.key),
		Value::None
		| Value::Null
		| Value::Bool(_)
		| Value::Number(_)
		| Value::String(_)
		| Value::Bytes(_)
		| Value::Duration(_)
		| Value::Datetime(_)
		| Value::Uuid(_)
		| Value::Geometry(_)
		| Value::Table(_)
		| Value::File(_)
		| Value::Date(_)
		| Value::Time(_)
		| Value::Timestamp(_)
		| Value::Interval(_) => false,
	}
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

	/// A request plain `cbor` can carry is written in it, byte for byte; one holding a regex
	/// anywhere is written in `cbor.2`, which a server reads back as the same value.
	#[test]
	fn a_request_is_plain_cbor_unless_it_holds_a_regex() {
		use std::collections::{BTreeMap, BTreeSet};

		use surrealdb_types::{
			Array, Number, Object, Range, RecordId, RecordIdKeyRange, Regex, Set, Table,
		};

		let plain = Value::Array(Array::from(vec![
			Value::String("RETURN $r".into()),
			Value::Number(Number::I32(7)),
			Value::Object(Object::from(BTreeMap::from([(
				"r".to_owned(),
				Value::String("/abc/".into()),
			)]))),
		]));
		let body = encode_body(plain.clone()).unwrap();
		assert_eq!(body.content_type, CONTENT_TYPE);
		assert_eq!(body.bytes, surrealdb_types::cbor::encode(plain).unwrap());

		let regex = || Value::Regex("^a/b[0-9]+$".parse::<Regex>().unwrap());
		let record = |key| {
			Value::RecordId(RecordId {
				table: Table::new("t"),
				key,
			})
		};
		for value in [
			regex(),
			Value::Array(Array::from(vec![Value::Null, regex()])),
			Value::Object(Object::from(BTreeMap::from([("r".to_owned(), regex())]))),
			Value::Set(Set::from(BTreeSet::from([regex()]))),
			Value::Range(Box::new(Range::new(Bound::Unbounded, Bound::Excluded(regex())))),
			record(RecordIdKey::Array(Array::from(vec![regex()]))),
			record(RecordIdKey::Object(Object::from(BTreeMap::from([("r".to_owned(), regex())])))),
			record(RecordIdKey::Range(Box::new(RecordIdKeyRange {
				start: Bound::Included(RecordIdKey::Array(Array::from(vec![regex()]))),
				end: Bound::Unbounded,
			}))),
		] {
			let body = encode_body(value.clone()).unwrap();
			assert_eq!(body.content_type, format::CBOR2, "{value:?}");
			assert_eq!(encode(value.clone()).unwrap(), body.bytes);
			let back = surrealdb_types::cbor::decode_v2(&body.bytes, RECURSION_LIMIT, None);
			assert_eq!(back.unwrap(), value);
		}
	}

	#[test]
	fn the_header_offers_the_protocols() {
		assert_eq!(PROTOCOLS_HEADER, PROTOCOLS.join(", "));
	}
}
