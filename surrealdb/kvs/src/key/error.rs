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

	/// An object taking part in a key, as a record id's key or an indexed
	/// value, has a field named by the empty string. The key encoding writes
	/// such a name as the end of the object, so the key could not be read back.
	#[error(
		"Found {value} in a record id or an indexed value, but an object there cannot have an empty field name"
	)]
	EmptyFieldName {
		value: String,
	},

	/// A key read back from the store did not have the shape its type requires.
	#[error("Encountered KV store corruption: {0}")]
	Corrupted(&'static str),
}

impl Error {
	/// The failure for a value that refused its key encoding with `reason`, the
	/// payload of a `storekey::EncodeError::Custom`: the refusal itself when it
	/// is one of this layer's errors, else [`Error::Unencodable`].
	pub fn from_refusal(reason: Box<dyn std::error::Error + Send + Sync>) -> Self {
		match reason.downcast::<Error>() {
			Ok(error) => *error,
			Err(_) => Error::Unencodable,
		}
	}
}

impl LeafError for Error {
	fn map_kind(self, message: String) -> TypesError {
		match self {
			Error::Unencodable => TypesError::serialization(message, None),
			// The caller's value has a shape no key can hold.
			Error::EmptyFieldName {
				..
			} => TypesError::validation(message, None),
			Error::Corrupted(_) => internal_todo(message),
		}
	}
}
