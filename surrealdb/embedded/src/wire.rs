//! Encoding for the bytes an FFI shim carries.
//!
//! An embedded engine has no wire, so no format is inherent to it — the choice
//! belongs to whatever the shim's caller speaks. This dispatches over the same
//! [`Format`] the server's transports negotiate, so an embedded connection and a
//! WebSocket connection cannot end up encoding the same value differently.
//!
//! `Format::Json` is lossy for SurrealQL values (a record id and a string are
//! both JSON strings) and exists for human-facing output, not for a round trip.
//! It is accepted here because the enum has the variant, and rejected as an
//! input format for the same reason the server rejects it on ingest paths where
//! fidelity matters.

use anyhow::{Result, bail};
use surrealdb_core::rpc::Format;
use surrealdb_core::rpc::format::{cbor, json};
use surrealdb_types::Value;

/// Decodes a value the shim's caller sent.
///
/// `recursion_limit` bounds nesting depth on the formats that can express
/// unbounded nesting, and comes from the datastore's parser configuration so it
/// matches what the server's transports apply.
pub fn decode(format: Format, bytes: &[u8], recursion_limit: usize) -> Result<Value> {
	match format {
		Format::Cbor => cbor::decode(bytes, recursion_limit),
		// Flatbuffers is length-prefixed and self-describing, so nesting is
		// bounded by the buffer rather than by a configured depth.
		Format::Flatbuffers => surrealdb_types::decode(bytes),
		Format::Json => json::decode(bytes, recursion_limit),
		Format::Unsupported => bail!("unsupported wire format"),
	}
}

/// Encodes a value for the shim's caller.
pub fn encode(format: Format, value: Value) -> Result<Vec<u8>> {
	match format {
		Format::Cbor => cbor::encode(value),
		Format::Flatbuffers => surrealdb_types::encode(&value),
		Format::Json => json::encode(value),
		Format::Unsupported => bail!("unsupported wire format"),
	}
}
