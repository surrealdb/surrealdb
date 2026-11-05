//! The contract a SurrealDB error type implements to reach the wire.
//!
//! Each layer of the engine owns its own error enum, but they all have to
//! arrive at the client as one [`surrealdb_types::Error`]. [`LeafError`] is
//! that conversion, stated once so every layer answers the same question the
//! same way.

use surrealdb_types::Error as TypesError;

/// Converts an error into its public form.
///
/// Implement [`map_kind`](LeafError::map_kind) and nothing else.
///
/// # Nesting
///
/// When one error wraps another, the outer arm must call the inner error's
/// `map_kind`, **not** its `to_types_error`. `to_types_error` attaches the
/// cause, so calling it inward attaches one at each level and the outer
/// attachment silently replaces the inner one. `map_kind` classifies without
/// touching the cause, which is why it is the only method to write and
/// `to_types_error` is provided.
pub trait LeafError: std::error::Error + Sized + Send + Sync + 'static {
	/// Classify this error: kind, details and code.
	///
	/// `message` is the error's own `Display` output, computed once by
	/// [`to_types_error`](LeafError::to_types_error) and passed in so an
	/// implementation can move payload fields out of `self` rather than
	/// cloning them to build its message.
	///
	/// Do not attach a cause here.
	fn map_kind(self, message: String) -> TypesError;

	/// Produce the public error.
	///
	/// Frames [`map_kind`](LeafError::map_kind) with the error's message and
	/// one level of its source chain. Deeper levels are deliberately dropped:
	/// they routinely name storage internals and raw field values, which are
	/// not for clients.
	fn to_types_error(self) -> TypesError {
		let message = self.to_string();
		let cause = std::error::Error::source(&self).map(|s| TypesError::internal(s.to_string()));
		let mapped = self.map_kind(message);
		match cause {
			Some(cause) => mapped.with_cause(cause),
			None => mapped,
		}
	}
}

/// A variant that reaches clients as an untyped internal error only because it
/// always has.
///
/// Behaviourally identical to [`TypesError::internal`]. It exists to keep those
/// variants greppable and countable while they are worked off, so that a
/// deliberate `internal` and an unclassified one do not look alike.
#[inline]
pub fn internal_todo(message: String) -> TypesError {
	TypesError::internal(message)
}
