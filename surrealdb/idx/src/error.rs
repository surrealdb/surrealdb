//! Failures raised by the index layer.
//!
//! These cover the whole of indexing: the vector shapes a vector index will
//! accept, the uniqueness a UNIQUE index enforces, the analyzers, tokenizers
//! and highlighters behind full-text search, the allowlist a mapper file has to
//! resolve inside, and the planner's search for an index able to support a
//! given expression.

// The mapper below is the only place this layer's failures become public.
// A new variant must make that decision explicitly rather than inheriting
// whatever the last arm happened to be.
#![deny(clippy::wildcard_enum_match_arm)]

use common::{LeafError, internal_todo};
use fst::Error as FstError;
use surrealdb_types::{Error as TypesError, QueryError, ToSql};

use crate::ft::MatchRef;
use crate::val::RecordId;

/// A failure in the index layer.
#[derive(Debug, thiserror::Error)]
#[allow(clippy::enum_variant_names)]
pub enum Error {
	/// The size of the vector is incorrect
	#[error("Incorrect vector dimension ({current}). Expected a vector of {expected} dimension.")]
	InvalidVectorDimension {
		current: usize,
		expected: usize,
	},

	/// The size of the vector is incorrect
	#[error("The value cannot be converted to a vector: {0}")]
	InvalidVectorValue(String),

	/// A database index entry for the specified record already exists
	#[error("Database index `{index}` already contains {value}, with record `{record}`", record = record.to_sql())]
	IndexExists {
		record: RecordId,
		index: String,
		value: String,
	},

	/// The query planner did not find an index able to support the given
	/// expression
	#[error("There was no suitable index supporting the expression: {exp}")]
	NoIndexFoundForMatch {
		exp: String,
	},

	/// Represents an error when analyzing a value
	#[error("A value can't be analyzed: {0}")]
	AnalyzerError(String),

	/// Represents an error when trying to highlight a value
	#[error("A value can't be highlighted: {0}")]
	HighlightError(String),

	/// Represents an underlying error with FST
	#[error("FstError error: {0}")]
	FstError(#[from] FstError),

	/// Duplicated match references are not allowed
	#[error("Duplicated Match reference: {mr}")]
	DuplicatedMatchRef {
		mr: MatchRef,
	},

	/// A mapper path outside the configured allowlist was requested
	#[error("File access denied: {0}")]
	FileAccessDenied(String),

	/// An ANN graph's in-memory state names an entry-point element whose
	/// vector the reading transaction cannot see.
	///
	/// The graph state (entry point, layer versions) is held per process and
	/// reconciled with the store by a state check before a search runs; the
	/// element vectors are read from the transaction. A search that starts
	/// from state which is ahead of its own snapshot — the writes that
	/// introduced the entry point were rolled back, or the state check was
	/// skipped — cannot reach the graph at all. Reloading the state, which a
	/// retry does, resolves it.
	#[error(
		"Vector index {index_id} on table `{table}` cannot be searched: entry point element {element} has no vector in this transaction"
	)]
	AnnEntryPointUnreadable {
		table: String,
		index_id: u32,
		element: u64,
	},
}

impl LeafError for Error {
	fn map_kind(self, message: String) -> TypesError {
		match self {
			Error::DuplicatedMatchRef {
				..
			} => TypesError::validation(message, None),
			Error::NoIndexFoundForMatch {
				..
			}
			| Error::AnalyzerError(_)
			| Error::HighlightError(_)
			| Error::FstError(_) => TypesError::internal(message),
			// The graph state this search started from disagrees with its own
			// snapshot, which is what a retry resolves: the next search
			// reconciles the state before reading. Reported as a conflict so a
			// client retries rather than surfacing it as a failed query.
			Error::AnnEntryPointUnreadable {
				..
			} => TypesError::query(message, QueryError::TransactionConflict),
			Error::InvalidVectorDimension {
				..
			}
			| Error::InvalidVectorValue(_)
			| Error::IndexExists {
				..
			}
			| Error::FileAccessDenied(_) => internal_todo(message),
		}
	}
}

/// The record a unique-index conflict names, if `error` is one.
///
/// UPSERT and INSERT turn a duplicate index entry into a retry against that
/// record, but only when the statement did not name a record id; otherwise they
/// re-raise the error untouched. Inspecting by reference keeps both outcomes
/// open, so deciding which applies can never consume the error.
pub fn index_exists_record(error: &anyhow::Error) -> Option<RecordId> {
	let Some(Error::IndexExists {
		record,
		..
	}) = error.downcast_ref::<Error>()
	else {
		return None;
	};
	Some(record.clone())
}
