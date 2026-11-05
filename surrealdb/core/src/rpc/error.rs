//! RPC layer error constructors that depend on core-internal types.
//!
//! The pure constructors (using only [`surrealdb_types`]) live in
//! [`surrealdb_rpc::error`]. The two constructors below stay in core because
//! they reach into core-only types (`crate::val::Duration`, `crate::err`).

use surrealdb_types::Error as TypesError;

/// Build the error returned when a call trips the wall-clock query-timeout
/// guard. Mirrors the deadline-based query timeout (`err::Error::QueryTimedout`)
/// in both message and structured [`surrealdb_types::QueryError::TimedOut`]
/// detail, so a timeout surfaces identically regardless of which guard
/// (deadline or wall-clock) fired or which transport (RPC / HTTP) reported it.
///
/// Shared by the RPC dispatch guard and the HTTP transport helpers so the two
/// never drift apart.
pub fn query_timeout_error(duration: std::time::Duration) -> TypesError {
	TypesError::query(
		format!(
			"The query was not executed because it exceeded the timeout: {}",
			crate::val::Duration::from(duration)
		),
		surrealdb_types::QueryError::TimedOut {
			duration,
		},
	)
}

/// Convert an anyhow error to a wire error, downcasting to preserve structured
/// error information where possible.
///
/// Tries, in order:
/// Delegates to [`crate::err::anyhow_to_types_error`], which owns the list of
/// SurrealDB error types that can appear inside an `anyhow::Error`.
pub fn types_error_from_anyhow(error: anyhow::Error) -> TypesError {
	crate::err::anyhow_to_types_error(error)
}

#[cfg(test)]
mod tests {
	use surrealdb_types::{Error as TypesError, QueryError};

	use super::types_error_from_anyhow;
	use crate::err;
	use crate::kvs::Error as KvsError;

	#[test]
	fn bare_kvs_conflict_maps_to_transaction_conflict() {
		// A transaction conflict bailed directly from the transactor is a
		// bare `kvs::Error` inside anyhow (not wrapped in `err::Error::Kvs`).
		// It must still surface the structured TransactionConflict kind so
		// SDK `.retry()` fires, rather than collapsing to a generic error.
		let err = anyhow::Error::new(KvsError::TransactionConflict("write conflict".into()));
		let te = types_error_from_anyhow(err);
		assert_eq!(te.query_details(), Some(&QueryError::TransactionConflict));
	}

	#[test]
	fn wrapped_kvs_conflict_still_maps_to_transaction_conflict() {
		// Regression: the already-wrapped form must keep working.
		let err = anyhow::Error::new(err::Error::Kvs(KvsError::TransactionConflict(
			"write conflict".into(),
		)));
		let te = types_error_from_anyhow(err);
		assert_eq!(te.query_details(), Some(&QueryError::TransactionConflict));
	}

	#[test]
	fn existing_types_error_is_returned_verbatim() {
		// A ready-made TypesError passes through unchanged (kind preserved).
		let original = TypesError::query("boom".to_string(), QueryError::NotExecuted);
		let te = types_error_from_anyhow(anyhow::Error::new(original));
		assert_eq!(te.query_details(), Some(&QueryError::NotExecuted));
	}
}
