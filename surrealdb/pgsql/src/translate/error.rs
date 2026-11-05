//! Translation error types for PostgreSQL to SurrealQL translation.

use thiserror::Error;

/// An error encountered during AST translation from PostgreSQL to SurrealQL.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TranslationError {
	/// The statement or expression contains a feature not supported by the translator.
	#[error("Unsupported PostgreSQL feature: {0}")]
	Unsupported(String),
	/// The statement or expression is structurally invalid for translation.
	#[error("Invalid statement structure: {0}")]
	Invalid(String),
	/// Failed to format translated output.
	#[error("Format error: {0}")]
	Format(String),
}

impl From<std::fmt::Error> for TranslationError {
	fn from(e: std::fmt::Error) -> Self {
		Self::Format(e.to_string())
	}
}
