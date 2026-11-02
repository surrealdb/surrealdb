//! Wire codecs for the PostgreSQL protocol: pure functions over plain values, one module per
//! type family, so the column typing and the connection code that call them stay separate from
//! the byte layouts.

pub mod bits;
pub mod geometry;
pub mod lsn;
pub mod mac;
pub mod money;
pub mod network;
pub mod oid;
pub mod range;

/// A binary value that does not match its type's format.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CodecError {
	pub reason: String,
}

impl std::fmt::Display for CodecError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.write_str(&self.reason)
	}
}

impl std::error::Error for CodecError {}

/// A refusal with its reason.
fn codec_error(reason: impl Into<String>) -> CodecError {
	CodecError {
		reason: reason.into(),
	}
}
