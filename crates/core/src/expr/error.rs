//! Module implementing error used mostly by the executor.

use crate::err;
use std::fmt;

use super::Value;

/// Error originating form the executor.
///
/// These are errors which can be the result of normal SurQL execution. Think invalid function
/// arguments, type errors, no database, etc.
#[derive(Debug)]
pub enum SqlError {
	/// Error explicitly throw from by the `THROW` statement.
	/// Should not be used for other purposes.
	Thrown(String),
	/// A timeout happened while executing the query.
	Timeout,
	/// The query was explicitly canceled
	Canceled,
	/// The query did execute but was then rolled back when it failed.
	RolledBack {
		message: Option<String>,
	},
	/// Tried to execute invalid SurQL query.
	///
	/// This error is for queries which are always invalid regardless of context.
	Invalid {
		message: String,
	},
	/// Returned for syntatic productions which are possible but not supported by the
	/// current executor.
	Unsupported {
		/// What the operations was.
		what: String,
		/// Why the operation is not supported.
		reason: Option<String>,
	},

	InvalidArgument {
		function: String,
		message: String,
	},
}

impl SqlError {
	pub fn invalid_fetch(v: &Value) -> Self {
		SqlError::Invalid {
			message: format!(
				"Found {v} on FETCH CLAUSE, but FETCH expects an idiom, a string or fields"
			),
		}
	}
}

impl std::error::Error for SqlError {}
impl fmt::Display for SqlError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			SqlError::Thrown(message) => {
				write!(f, "An error was raised by a THROW statement: {message}")
			}
			SqlError::Timeout => write!(
				f,
				"The query was not executed because it exceeded it execeeded it's timeout deadline"
			),
			SqlError::Canceled => {
				write!(f, "The query was not executed because it's transaction was canceled.")
			}
			SqlError::RolledBack {
				message,
			} => {
				if let Some(m) = message {
					write!(f, "The query was rolled back after it's transaction was canceled: {m}.")
				} else {
					write!(f, "The query was rolled back after it's transaction was canceled")
				}
			}
			SqlError::Unsupported {
				what,
				reason,
			} => {
				write!(f, "The query tried to do an operation which is not supported by the current executor, {what}")?;
				if let Some(reason) = reason {
					write!(f, ": {reason}")?;
				}
				Ok(())
			}
			SqlError::Invalid {
				message,
			} => {
				write!(f, "Invalid surrealql query: {message}")
			}
			SqlError::InvalidArgument {
				function,
				message,
			} => {
				write!(f, "Invalid surrealql method call to `{function}`: {message}")
			}
		}
	}
}

impl err::HasErrorCode for SqlError {
	fn error_code(&self) -> err::ErrorCode {
		let variant = match self {
			SqlError::Thrown(_) => 0,
			SqlError::Timeout => 1,
			SqlError::Canceled => 2,
			SqlError::RolledBack {
				..
			} => 3,
			SqlError::Invalid {
				..
			} => 4,
			SqlError::InvalidArgument {
				..
			} => 5,

			SqlError::Unsupported {
				..
			} => 6,
		};

		err::ErrorCode::new(err::SubSystem::Query, variant)
	}
}
