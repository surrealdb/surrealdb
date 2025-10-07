use surrealdb_types::{FromFlatbuffers, SurrealValue, ToFlatbuffers};

use crate::types::PublicValue;

/// Encode a public value to a flatbuffers vector.
pub fn encode(value: &PublicValue) -> anyhow::Result<Vec<u8>> {
	let mut fbb = flatbuffers::FlatBufferBuilder::new();
	let value = value.to_fb(&mut fbb)?;
	fbb.finish(value, None);
	Ok(fbb.finished_data().to_vec())
}

/// Decode a flatbuffers vector to a public value.
pub fn decode<T: SurrealValue>(value: &[u8]) -> anyhow::Result<T> {
	let value_fb = flatbuffers::root::<surrealdb_protocol::fb::v1::Value>(value)?;
	let value = surrealdb_types::Value::from_fb(value_fb)?;

	T::from_value(value)
}
