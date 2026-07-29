//! Failures any layer of the engine can raise.
//!
//! Each layer owns an error type describing what can go wrong *there*. These
//! four are the exception: a broken invariant, an unclassified internal
//! failure, and the two query-lifecycle signals are raised from the storage
//! layer up to the executor alike. Duplicating them per layer would mean the
//! same concept mapping to the wire in a dozen places, so the engine names
//! them once, here.
//!
//! They belong to the engine, not to the shared-utility layer: the crates
//! below the engine (the AST, the parser, the token stream) never execute a
//! query and so can never raise any of them.

use std::time::Duration;

use common::LeafError;
use common::fmt::SqlDuration;
use surrealdb_types::{Error as TypesError, QueryError, ToSql};

/// A failure that any layer of the engine can raise.
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
	/// An invariant the code relies on did not hold.
	///
	/// Build these with [`EngineError::unreachable`] or the `fail!` macro,
	/// which record the source location.
	#[error("The database encountered unreachable logic: {0}")]
	Unreachable(String),

	/// A failure with no more specific classification.
	#[error("Internal database error: {0}")]
	Internal(String),

	/// The query stopped because its transaction was cancelled.
	#[error("The query was not executed due to a cancelled transaction")]
	QueryCancelled,

	/// The query stopped because it ran past its timeout.
	#[error("The query was not executed because it exceeded the timeout: {}", SqlDuration(*.0).to_sql())]
	QueryTimedout(Duration),
}

impl EngineError {
	/// Report a broken invariant, tagged with the caller's source location.
	///
	/// The location is baked into the message because these are read in logs
	/// and bug reports, where the site that noticed is the only useful lead.
	#[cold]
	#[track_caller]
	pub fn unreachable<T: std::fmt::Display>(message: T) -> EngineError {
		let location = std::panic::Location::caller();
		EngineError::Unreachable(format!("{}:{}: {}", location.file(), location.line(), message))
	}
}

impl LeafError for EngineError {
	fn map_kind(self, message: String) -> TypesError {
		match self {
			EngineError::Unreachable(_) | EngineError::Internal(_) => TypesError::internal(message),
			EngineError::QueryCancelled => TypesError::query(message, QueryError::Cancelled),
			EngineError::QueryTimedout(duration) => TypesError::query(
				message,
				QueryError::TimedOut {
					duration,
				},
			),
		}
	}
}
