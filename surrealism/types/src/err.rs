//! Error handling utilities for adding context to errors.
//!
//! This module provides the [`PrefixError`](crate::err::PrefixError) trait, which extends [`Result`] and [`Option`]
//! with a method to add contextual prefixes to error messages.

use anyhow::Result;

/// Extension trait for adding contextual prefixes to errors.
///
/// This trait provides a convenient way to add context to errors without losing the
/// underlying error information. It's similar to `context()` from `anyhow`, but uses
/// a lazy closure to construct the prefix (avoiding allocation when not needed).
///
/// # Example
///
/// ```rust,ignore
/// use surrealism_types::err::PrefixError;
///
/// fn parse_value(s: &str) -> Result<i64> {
///     s.parse()
///         .prefix_err(|| format!("Failed to parse '{}' as i64", s))
/// }
///
/// // Error message will be: "Failed to parse 'abc' as i64: invalid digit found in string"
/// let result = parse_value("abc");
/// ```
pub trait PrefixError<T> {
	/// Add a contextual prefix to an error message.
	///
	/// # Parameters
	///
	/// - `prefix`: A closure that constructs the prefix message (only called on error)
	///
	/// # Returns
	///
	/// - `Ok(T)` if the result/option was successful
	/// - `Err` with the prefixed message if there was an error
	///
	/// # Example
	///
	/// ```rust,ignore
	/// let result: Result<i64> = some_operation()
	///     .prefix_err(|| "Operation failed");
	/// ```
	fn prefix_err<F, S>(self, prefix: F) -> Result<T>
	where
		F: FnOnce() -> S,
		S: std::fmt::Display;
}

impl<T, E> PrefixError<T> for std::result::Result<T, E>
where
	E: std::fmt::Display + Send + Sync + 'static,
{
	fn prefix_err<F, S>(self, prefix: F) -> Result<T>
	where
		F: FnOnce() -> S,
		S: std::fmt::Display,
	{
		self.map_err(|e| anyhow::anyhow!(format!("{}: {}", prefix(), e)))
	}
}

impl<T> PrefixError<T> for Option<T> {
	fn prefix_err<F, S>(self, prefix: F) -> Result<T>
	where
		F: FnOnce() -> S,
		S: std::fmt::Display,
	{
		self.ok_or_else(|| anyhow::anyhow!(format!("{}: None", prefix())))
	}
}
