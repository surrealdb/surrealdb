//! Failures raised while ordering a result set.
//!
//! A sort that does not fit the configured memory budget spills its rows to a
//! temporary file and merge-sorts them from disk. That path is the only reason
//! ordering can fail at all, and it fails in exactly three ways: the spill file
//! misbehaves, a row does not survive the round trip through the spill
//! encoding, or the sort itself does not complete.
//!
//! None of these are the query's fault. They say the engine could not carry out
//! an ordering it had already accepted, so a client can retry the same query
//! unchanged.
//!
//! Two collectors share this: the iterator's [`FileCollector`] and the
//! execution engine's external sort operators. They write the same on-disk
//! format, so they raise the same failures.
//!
//! [`FileCollector`]: super::file

// The mapper below is the only place this layer's failures become public.
// A new variant must make that decision explicitly rather than inheriting
// whatever the last arm happened to be.
#![deny(clippy::wildcard_enum_match_arm)]

use std::io::Error as IoError;

use common::{LeafError, internal_todo};
use revision::Error as RevisionError;
use surrealdb_types::Error as TypesError;

/// A failure while ordering a result set.
#[derive(Debug, thiserror::Error)]
#[cfg_attr(
	any(not(storage), target_family = "wasm"),
	allow(dead_code, reason = "the spill path needs a storage backend and a blocking thread pool")
)]
pub(crate) enum SortError {
	/// Reading or writing the spill file failed.
	#[error("I/O error: {0}")]
	Io(#[from] IoError),

	/// A row could not be encoded into, or decoded out of, the spill file.
	#[error("Versioned error: {0}")]
	Revision(#[from] RevisionError),

	/// The sort could not be carried out: the merge-sort library gave up, the
	/// blocking task running it did not return, or the collector was asked for
	/// a result it does not hold.
	#[error("Error while ordering a result: {0}.")]
	OrderingError(String),
}

impl LeafError for SortError {
	fn map_kind(self, message: String) -> TypesError {
		match self {
			// Engine-side: nothing a client supplied caused these and nothing a
			// client can change avoids them.
			SortError::Io(_) | SortError::OrderingError(_) => internal_todo(message),
			SortError::Revision(_) => TypesError::serialization(message, None),
		}
	}
}

#[cfg(storage)]
impl<S, D, I> From<ext_sort::SortError<S, D, I>> for SortError
where
	S: std::error::Error,
	D: std::error::Error,
	I: std::error::Error,
{
	fn from(e: ext_sort::SortError<S, D, I>) -> SortError {
		SortError::OrderingError(e.to_string())
	}
}
