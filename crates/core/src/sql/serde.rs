use bincode::Options;
use bincode::Result;
use serde::{Deserialize, Serialize};

pub fn serialize<T>(value: &T) -> Result<Vec<u8>>
where
	T: ?Sized + Serialize,
{
	bincode::options()
		// Don't specify a byte limit
		.with_no_limit()
		// Use little-endian data ordering
		.with_little_endian()
		// Use variable-sized integer encoding
		.with_varint_encoding()
		// Serialize the value
		.serialize(value)
}

pub fn deserialize<'a, T>(bytes: &'a [u8]) -> Result<T>
where
	T: Deserialize<'a>,
{
	bincode::options()
		// Don't specify a byte limit
		.with_no_limit()
		// Use little-endian data ordering
		.with_little_endian()
		// Use variable-sized integer encoding
		.with_varint_encoding()
		// Allow any remaining unused data
		.allow_trailing_bytes()
		// Deserialize the value
		.deserialize(bytes)
}
