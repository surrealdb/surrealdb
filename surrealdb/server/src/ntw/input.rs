use anyhow::Result;
use bytes::Bytes;

pub(crate) fn bytes_to_utf8(bytes: &Bytes) -> Result<&str> {
	Ok(std::str::from_utf8(bytes)?)
}
