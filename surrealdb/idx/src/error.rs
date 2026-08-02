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
use surrealdb_types::{Error as TypesError, ToSql};

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
