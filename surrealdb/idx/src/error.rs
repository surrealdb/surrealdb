//! Failures raised by the index layer.
//!
//! These cover the whole of indexing: the vector shapes a vector index will
//! accept, the uniqueness a UNIQUE index enforces, the analyzers, tokenizers
//! and highlighters behind full-text search, the allowlist a mapper file has to
//! resolve inside, the transactions an index can be built in while its
//! definition waits, and the planner's search for an index able to support a
//! given expression.

// The mapper below is the only place this layer's failures become public.
// A new variant must make that decision explicitly rather than inheriting
// whatever the last arm happened to be.
#![deny(clippy::wildcard_enum_match_arm)]

use common::{LeafError, internal_todo};
use fst::Error as FstError;
use surrealdb_types::{Error as TypesError, QueryError, ToSql, ValidationError};

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

	/// The same, with the conflicting value and the record that holds it
	/// withheld from a caller whose SELECT is permission checked: naming either
	/// would disclose a row or field value the caller cannot read. The conflict
	/// itself is still reported, as a uniqueness constraint cannot be enforced
	/// without it.
	#[error("Database index `{index}` already contains a conflicting value in another record")]
	IndexExistsHidden {
		index: String,
	},

	/// A UNIQUE index whose build failed cannot enforce uniqueness, so a write
	/// that adds a key to it is refused until the index is rebuilt or removed.
	#[error(
		"Database index `{index}` on table `{table}` failed to build, so it cannot enforce UNIQUE for this write: fix the duplicate values and run REBUILD INDEX, or REMOVE INDEX"
	)]
	IndexBuildFailed {
		table: String,
		index: String,
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

	/// The build left out the cargo feature an index kernel needs.
	///
	/// The definition still parses and stored state still decodes; building,
	/// loading or querying the structure fails with this error.
	#[error("{what} are not available: this build was compiled without the `{feature}` feature")]
	FeatureDisabled {
		feature: &'static str,
		what: &'static str,
	},

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

	/// An index whose build its defining statement waits for, defined in a
	/// transaction that has written records of the index's table. The build
	/// reads the table as last committed, which holds none of those writes.
	#[error(
		"Cannot define index `{index}` on `{table}`: this transaction has written records of `{table}`, which a build without CONCURRENTLY cannot see. Define the index CONCURRENTLY, or commit those writes first."
	)]
	BlockingBuildAfterWrites {
		index: String,
		table: String,
	},

	/// A transaction changed a record that an index it defined holds an entry
	/// for, written by the build the definition ran.
	///
	/// A blocking build reads and writes in transactions of its own, so the
	/// transaction that defined the index cannot see that entry, and writing
	/// the key fails the transaction's commit against the build's own write
	/// whatever it is retried with. The change is refused where it is made,
	/// not at the commit.
	#[error(
		"Cannot change record `{record}`: index `{index}` was defined by this transaction, and the build that ran for it wrote an entry for the record that this transaction cannot see. Define the index CONCURRENTLY, or commit the definition before changing the record.",
		record = record.to_sql()
	)]
	EntryOfUnseenBuild {
		index: String,
		record: RecordId,
	},
}

impl LeafError for Error {
	fn map_kind(self, message: String) -> TypesError {
		match self {
			Error::DuplicatedMatchRef {
				..
			}
			| Error::EntryOfUnseenBuild {
				..
			} => TypesError::validation(message, None),
			Error::NoIndexFoundForMatch {
				..
			}
			| Error::AnalyzerError(_)
			| Error::HighlightError(_)
			| Error::FstError(_) => TypesError::internal(message),
			Error::FeatureDisabled {
				..
			} => TypesError::configuration(message, None),
			// The graph state this search started from disagrees with its own
			// snapshot, which is what a retry resolves: the next search
			// reconciles the state before reading. Reported as a conflict so a
			// client retries rather than surfacing it as a failed query.
			Error::AnnEntryPointUnreadable {
				..
			} => TypesError::query(message, QueryError::TransactionConflict),
			// The statement asks for something its transaction cannot give, and
			// the caller can change it: define the index CONCURRENTLY, or after
			// the writes are committed.
			Error::BlockingBuildAfterWrites {
				..
			} => TypesError::validation(message, ValidationError::InvalidRequest),
			Error::InvalidVectorDimension {
				..
			}
			| Error::InvalidVectorValue(_)
			| Error::IndexExists {
				..
			}
			| Error::FileAccessDenied(_) => internal_todo(message),
			// A UNIQUE-index conflict with the stored record and its indexed value
			// redacted for a permission-checked caller. The write is refused by a
			// uniqueness constraint the caller can resolve by choosing another
			// value, so it is a validation fault rather than an engine one. The
			// released sibling `IndexExists` stays untyped above: a shipped error
			// keeps its public kind, so only this new variant is typed.
			Error::IndexExistsHidden {
				..
			} => TypesError::validation(message, ValidationError::InvalidRequest),
			Error::IndexBuildFailed {
				..
			} => TypesError::query(message, None),
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
