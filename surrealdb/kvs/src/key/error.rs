//! Failures raised while encoding and decoding storage keys.
//!
//! Both are structural rather than user-facing: a key that cannot be encoded
//! or that reads back with the wrong shape means the engine and the store
//! disagree, not that the query was wrong.

// The mapper below is the only place this layer's failures become public.
// A new variant must make that decision explicitly rather than inheriting
// whatever the last arm happened to be.
#![deny(clippy::wildcard_enum_match_arm)]

use common::{LeafError, internal_todo};
use surrealdb_types::Error as TypesError;

/// A failure in the storage key layer.
#[derive(Debug, thiserror::Error)]
pub enum Error {
	/// A value was asked to take part in a key but has no key encoding.
	#[error("Tried to serialize a value which cannot be serialized.")]
	Unencodable,

	/// A key read back from the store did not have the shape its type requires.
	#[error("Encountered KV store corruption: {0}")]
	Corrupted(&'static str),
}

impl LeafError for Error {
	fn map_kind(self, message: String) -> TypesError {
		match self {
			Error::Unencodable => TypesError::serialization(message, None),
			Error::Corrupted(_) => internal_todo(message),
		}
	}
}
