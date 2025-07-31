use bincode::Options;
use serde::Serialize;
use serde::de::DeserializeOwned;

pub fn encode<S: Serialize>(value: &S) -> Result<Vec<u8>, String> {
	bincode::options()
		.with_no_limit()
		.with_little_endian()
		.with_varint_encoding()
		.serialize(value)
		.map_err(|e| e.to_string())
}

pub fn decode<D: DeserializeOwned>(value: &[u8]) -> Result<D, String> {
	bincode::options()
		.with_no_limit()
		.with_little_endian()
		.with_varint_encoding()
		.deserialize_from(value)
		.map_err(|e| e.to_string())
}
