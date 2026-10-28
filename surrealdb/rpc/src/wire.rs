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

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn the_header_offers_the_protocols() {
		assert_eq!(PROTOCOLS_HEADER, PROTOCOLS.join(", "));
	}
}
