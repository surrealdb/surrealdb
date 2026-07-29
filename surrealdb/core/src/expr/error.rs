//! Failures raised by the value algebra.
//!
//! These are the ways an expression can fail to produce a value: an operand
//! pair no operator accepts, a conversion the target type refuses, a document
//! shape a clause cannot consume. They describe the data, not the machinery
//! evaluating it, which is why a subset of them may be resolved to `NONE`
//! rather than aborting the query - see [`Error::is_ignorable`].

// The mapper below is the only place this layer's failures become public.
// A new variant must make that decision explicitly rather than inheriting
// whatever the last arm happened to be.
#![deny(clippy::wildcard_enum_match_arm)]

use common::{LeafError, internal_todo};
use surrealdb_types::{Error as TypesError, SerializationError, ToSql, ValidationError};

use crate::expr::operation::PatchError;
use crate::val::{CastError, CoerceError, Value};

/// A failure in the value algebra.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
	/// There was an error with the SQL query
	#[error("Cannot use {} in a CONTENT clause", value.to_sql())]
	InvalidContent {
		value: Value,
	},

	/// There was an error with the SQL query
	#[error("Cannot use {} in a MERGE clause", value.to_sql())]
	InvalidMerge {
		value: Value,
	},

	/// There was an error with the provided JSON Patch
	#[error("The JSON Patch contains invalid operations. {0}")]
	InvalidPatch(PatchError),

	/// Given test operation failed for JSON Patch
	#[error(
		"Given test operation failed for JSON Patch. Expected `{expected}`, but got `{got}` instead."
	)]
	PatchTest {
		expected: String,
		got: String,
	},

	/// The wrong quantity or magnitude of arguments was given for the specified
	/// function
	#[error("Incorrect arguments for function {name}(). {message}")]
	InvalidFunctionArguments {
		name: String,
		message: String,
	},

	/// Invalid regular expression
	#[error("Invalid regular expression: {0:?}")]
	InvalidRegex(String),

	/// Found a record id for the record but this is not a valid id
	#[error("Found {value} for the Record ID but this is not a valid id")]
	IdInvalid {
		value: String,
	},

	/// Found a table name for the record but this is not a valid table
	#[error("Found {value} for the Record ID but this is not a valid table name")]
	TbInvalid {
		value: String,
	},

	/// A destructuring variant was used in a context where it is not supported
	#[error("{variant} destructuring method is not supported here")]
	UnsupportedDestructure {
		variant: String,
	},

	/// Unable to coerce to a value to another value
	#[error("{0}")]
	Coerce(#[from] CoerceError),

	/// Unable to convert a value to another value
	#[error("{0}")]
	Cast(#[from] CastError),

	/// Cannot perform addition
	#[error("Cannot perform addition with '{0}' and '{1}'")]
	TryAdd(String, String),

	/// Cannot perform subtraction
	#[error("Cannot perform subtraction with '{0}' and '{1}'")]
	TrySub(String, String),

	/// Cannot perform multiplication
	#[error("Cannot perform multiplication with '{0}' and '{1}'")]
	TryMul(String, String),

	/// Cannot perform division
	#[error("Cannot perform division with '{0}' and '{1}'")]
	TryDiv(String, String),

	/// Cannot perform remainder
	#[error("Cannot perform remainder with '{0}' and '{1}'")]
	TryRem(String, String),

	/// Cannot perform power
	#[error("Cannot raise the value '{0}' with '{1}'")]
	TryPow(String, String),

	/// Cannot perform negation
	#[error("Cannot negate the value '{0}'")]
	TryNeg(String),

	/// Cannot extend a non-array value
	#[error("Cannot extend '{0}' as it is not an array")]
	TryExtend(String),

	/// It's is not possible to convert between the two types
	#[error("Cannot convert from '{0}' to '{1}'")]
	TryFrom(String, &'static str),

	/// Represents a failure in timestamp arithmetic related to database
	/// internals
	#[error("Timestamp arithmetic error: {0}")]
	TimestampOverflow(String),

	/// The supplied type could not be serialized into `expr::Value`
	#[error("Serialization error: {0}")]
	Serialization(String),

	/// Represents an arithmetic result that does not fit the value's type
	#[error("Failed to compute: \"{0}\", as the operation results in an arithmetic overflow.")]
	ArithmeticOverflow(String),

	/// Represents a negative value for a type that must be zero or positive
	#[error("Failed to compute: \"{0}\", as the operation results in a negative value.")]
	ArithmeticNegativeOverflow(String),

	#[error("The string could not be parsed into a bytesize")]
	InvalidBytesize,
}

impl LeafError for Error {
	fn map_kind(self, message: String) -> TypesError {
		match self {
			// Validation: the value handed to the operation had the wrong shape.
			Error::InvalidContent {
				value,
			} => TypesError::validation(
				message,
				ValidationError::InvalidContent {
					value: value.to_sql(),
				},
			),
			Error::InvalidMerge {
				value,
			} => TypesError::validation(
				message,
				ValidationError::InvalidMerge {
					value: value.to_sql(),
				},
			),
			Error::InvalidPatch(_) => TypesError::validation(message, None),
			Error::Coerce(_) => TypesError::validation(message, None),
			Error::Cast(_) => TypesError::validation(message, None),
			Error::TryAdd(..)
			| Error::TrySub(..)
			| Error::TryMul(..)
			| Error::TryDiv(..)
			| Error::TryRem(..)
			| Error::TryPow(..)
			| Error::TryNeg(_)
			| Error::TryExtend(_) => TypesError::validation(message, None),
			Error::TryFrom(..) => TypesError::validation(message, None),

			// Serialization
			Error::Serialization(..) => {
				TypesError::serialization(message, SerializationError::Serialization)
			}

			// A versionstamp that will not fit is an engine-internals fault, not
			// something the caller phrased wrongly.
			Error::TimestampOverflow(..) => TypesError::internal(message),

			// Reach clients as an untyped internal error only because they always
			// have. Each is a candidate for a real kind, and giving one a kind
			// moves the wire snapshot, so it is a deliberate change and not a
			// tidy-up.
			Error::PatchTest {
				..
			}
			| Error::InvalidFunctionArguments {
				..
			}
			| Error::InvalidRegex(_)
			| Error::IdInvalid {
				..
			}
			| Error::TbInvalid {
				..
			}
			| Error::UnsupportedDestructure {
				..
			}
			| Error::ArithmeticOverflow(_)
			| Error::ArithmeticNegativeOverflow(_)
			| Error::InvalidBytesize => internal_todo(message),
		}
	}
}

impl Error {
	/// Returns true if this error represents a data-shape problem (type
	/// mismatch, coercion failure, bad operand pair) that expression evaluation
	/// may resolve to `NONE` instead of propagating.
	///
	/// Returns false for everything else, including the arithmetic failures
	/// that are not listed. Membership is decided per variant and is not a
	/// family rule: `TryDiv` and `TryPow` are ignorable, `TryRem` and `TryFrom`
	/// are not. Do not widen the list to make it look tidy.
	pub(crate) fn is_ignorable(&self) -> bool {
		matches!(
			self,
			Error::Coerce(_)
				| Error::Cast(_)
				| Error::InvalidFunctionArguments { .. }
				| Error::TryAdd(..)
				| Error::TrySub(..)
				| Error::TryMul(..)
				| Error::TryDiv(..)
				| Error::TryPow(..)
				| Error::TryNeg(..)
				| Error::TryExtend(..)
		)
	}
}

impl From<regex::Error> for Error {
	fn from(error: regex::Error) -> Self {
		Error::InvalidRegex(error.to_string())
	}
}
