use bincode::Options;
use bincode::Result;
use serde::{Deserialize, Serialize};

pub fn serialize<T>(value: &T) -> Result<Vec<u8>>
where
	T: ?Sized + Serialize,
{
	bincode::options()
		.with_no_limit()
		.with_little_endian()
		.with_varint_encoding()
		.reject_trailing_bytes()
		.serialize(value)
}

pub fn deserialize<'a, T>(bytes: &'a [u8]) -> Result<T>
where
	T: Deserialize<'a>,
{
	bincode::options()
		.with_no_limit()
		.with_little_endian()
		.with_varint_encoding()
		// Ignore extra fields so we can pull out the ID only from responses that fail to deserialise
		.allow_trailing_bytes()
		.deserialize(bytes)
}
