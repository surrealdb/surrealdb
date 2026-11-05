//! The CBOR wire format: [`Value`] to and from CBOR bytes, as the RPC layer and the SDK speak it.
//!
//! `cbor` writes and reads the tags of the `cbor` protocol, `*_v2` those of `cbor.2`, which adds
//! the typed numeric tags. A decode turns a record id written as text into a [`RecordId`] only
//! through the [`RecordIdParser`] it is given, since parsing one needs the SurrealQL parser.

mod convert;
mod read;

pub use convert::{
	Ctx, RequestBlocks, TAG_BIGDECIMAL, TAG_DATE, TAG_F16, TAG_F32, TAG_I8, TAG_I16, TAG_I32,
	TAG_INTERVAL, TAG_REGEX, TAG_TIME, TAG_TIMESTAMP, TAG_U8, TAG_U16, TAG_U32, TAG_U64,
	from_value, from_value_v2, to_request_value, to_value, to_value_v2,
};
pub use read::{MapEntries, map_entries};

use crate::{RecordId, Value};

/// Parses a record id written as text, such as `person:tobie`.
pub type RecordIdParser = fn(&str) -> anyhow::Result<RecordId>;

/// `v` as `cbor` bytes.
pub fn encode(v: Value) -> anyhow::Result<Vec<u8>> {
	let encoding = from_value(v)?;
	let mut res = Vec::new();
	// `Vec<u8>` never returns an I/O error from `Write`, so the `Io` variant
	// of `ciborium::ser::Error` is unreachable here. The `Value` variant is
	// only constructed by ciborium's tag serializer, which our `CborValue`
	// representation never trips. Propagate as an error rather than panic
	// so any future ciborium change cannot crash the caller.
	ciborium::into_writer(&encoding, &mut res)
		.map_err(|e| anyhow::anyhow!("failed to encode CBOR: {e}"))?;
	Ok(res)
}

/// `v` as `cbor.2` bytes.
pub fn encode_v2(v: Value) -> anyhow::Result<Vec<u8>> {
	let encoding = from_value_v2(v)?;
	let mut res = Vec::new();
	ciborium::into_writer(&encoding, &mut res)
		.map_err(|e| anyhow::anyhow!("failed to encode CBOR: {e}"))?;
	Ok(res)
}

/// The `cbor` item at the start of `bytes`; bytes after it are ignored.
pub fn decode(
	bytes: &[u8],
	recursion_limit: usize,
	record_id: Option<RecordIdParser>,
) -> anyhow::Result<Value> {
	match read::read(bytes, recursion_limit, false, record_id) {
		Ok((value, _)) => Ok(value),
		Err(_) => decode_tree(bytes, recursion_limit, false, record_id, false),
	}
}

/// The `cbor.2` item at the start of `bytes`; bytes after it are ignored.
pub fn decode_v2(
	bytes: &[u8],
	recursion_limit: usize,
	record_id: Option<RecordIdParser>,
) -> anyhow::Result<Value> {
	match read::read(bytes, recursion_limit, true, record_id) {
		Ok((value, _)) => Ok(value),
		Err(_) => decode_tree(bytes, recursion_limit, true, record_id, false),
	}
}

/// `bytes` as exactly one `cbor` item: bytes after the item are an error.
pub fn decode_exact(
	bytes: &[u8],
	recursion_limit: usize,
	record_id: Option<RecordIdParser>,
) -> anyhow::Result<Value> {
	match read::read(bytes, recursion_limit, false, record_id) {
		Ok((value, read)) if read == bytes.len() => Ok(value),
		_ => decode_tree(bytes, recursion_limit, false, record_id, true),
	}
}

/// `bytes` as exactly one `cbor.2` item: bytes after the item are an error.
pub fn decode_exact_v2(
	bytes: &[u8],
	recursion_limit: usize,
	record_id: Option<RecordIdParser>,
) -> anyhow::Result<Value> {
	match read::read(bytes, recursion_limit, true, record_id) {
		Ok((value, read)) if read == bytes.len() => Ok(value),
		_ => decode_tree(bytes, recursion_limit, true, record_id, true),
	}
}

/// The item at the start of `bytes` decoded as a `ciborium` tree and then converted: the
/// definition [`read::read`] answers to, and the route that words the refusal of an item it
/// cannot read. With `exact`, bytes after the item are an error.
fn decode_tree(
	bytes: &[u8],
	recursion_limit: usize,
	v2: bool,
	record_id: Option<RecordIdParser>,
	exact: bool,
) -> anyhow::Result<Value> {
	let mut reader = bytes;
	let encoding = ciborium::de::from_reader_with_recursion_limit(&mut reader, recursion_limit)
		.map_err(|e| anyhow::anyhow!(e.to_string()))?;
	anyhow::ensure!(!exact || reader.is_empty(), "bytes after the CBOR item");
	match v2 {
		true => to_value_v2(encoding, record_id),
		false => to_value(encoding, record_id),
	}
}
