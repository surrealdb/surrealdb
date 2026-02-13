use std::fmt;

use anyhow::Context;
use serde::{Deserialize, Serialize};

use crate::{Kind, Object, SurrealValue, ToSql, Value};

// -----------------------------------------------------------------------------
// Public API error type (wire-friendly, non-lossy, supports chaining)
// -----------------------------------------------------------------------------

/// Curated error kind for public APIs.
///
/// Maps the full set of internal/database errors into a smaller set of
/// categories. Serializes as a snake_case string on the wire (e.g. `"validation"`).
/// The enum is non-exhaustive so new variants can be added later. The server
/// should map any unmappable error to [`Internal`](ErrorKind::Internal) before sending.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ErrorKind {
	/// Invalid input: parse error, invalid request or params.
	Validation,
	/// RPC/protocol: method not found or not allowed.
	Method,
	/// Feature or config not supported (e.g. live query, GraphQL config).
	Configuration,
	/// Authentication or authorisation failure.
	Auth,
	/// User-thrown error (e.g. from THROW in SurrealQL).
	Thrown,
	/// Query execution failure (not executed, timeout, cancelled).
	Query,
	/// Serialisation or deserialisation error.
	Serialization,
	/// Resource not found (e.g. table, record, namespace).
	NotFound,
	/// Resource already exists (e.g. table, record).
	AlreadyExists,
	/// Internal or unexpected error (server or client).
	Internal,
}

impl fmt::Display for ErrorKind {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			ErrorKind::Validation => f.write_str("validation"),
			ErrorKind::Method => f.write_str("method"),
			ErrorKind::Configuration => f.write_str("configuration"),
			ErrorKind::Auth => f.write_str("auth"),
			ErrorKind::Thrown => f.write_str("thrown"),
			ErrorKind::Query => f.write_str("query"),
			ErrorKind::Serialization => f.write_str("serialization"),
			ErrorKind::NotFound => f.write_str("not_found"),
			ErrorKind::AlreadyExists => f.write_str("already_exists"),
			ErrorKind::Internal => f.write_str("internal"),
		}
	}
}

/// Public error type for SurrealDB APIs.
///
/// Designed to be returned from public APIs (including over the wire). It is
/// wire-friendly and non-lossy: serialization preserves `kind`, `message`,
/// `details`, and the cause chain. Use this type whenever an error crosses
/// an API boundary (e.g. server response, SDK method return).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Error {
	/// Machine-readable error kind.
	pub kind: ErrorKind,
	/// Human-readable error message.
	pub message: String,
	/// Optional structured details (e.g. `{ "name": "users" }` for table not found).
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub details: Option<Object>,
	/// The underlying cause of this error, if any. Semantically: "this error was caused by that
	/// one".
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub cause: Option<Box<Error>>,
}

impl Error {
	/// Creates a new error with the given `kind` and `message`.
	pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
		Self {
			kind,
			message: message.into(),
			details: None,
			cause: None,
		}
	}

	/// Adds optional structured details to this error.
	#[must_use]
	pub fn with_details(mut self, details: Object) -> Self {
		self.details = Some(details);
		self
	}

	/// Sets the cause of this error (the error that led to this one).
	#[must_use]
	pub fn with_cause(mut self, cause: Error) -> Self {
		self.cause = Some(Box::new(cause));
		self
	}

	/// Returns the underlying cause of this error, if any.
	pub fn cause(&self) -> Option<&Error> {
		self.cause.as_deref()
	}

	/// Returns an iterator over the full cause chain (this error, then its cause, then the cause's
	/// cause, etc.).
	pub fn chain(&self) -> Chain<'_> {
		Chain {
			current: Some(self),
		}
	}

	/// Returns structured auth error details when this error's kind is [`ErrorKind::Auth`] and
	/// `details` is present. Use this instead of matching on the error message string.
	pub fn auth_details(&self) -> Option<AuthError> {
		if self.kind != ErrorKind::Auth {
			return None;
		}
		let details = self.details.as_ref()?;
		Some(AuthError::from_details(details))
	}
}

// -----------------------------------------------------------------------------
// Structured error details (wire format in Error.details)
// -----------------------------------------------------------------------------

/// Structured details for [`ErrorKind::Auth`] errors.
///
/// Serialized as an object in `Error.details` (e.g. `{ "token_expired": true }`) so clients can
/// detect auth failure reasons without parsing the message string.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AuthError {
	/// The token used for authentication has expired.
	pub token_expired: bool,
	/// The session has expired.
	pub session_expired: bool,
}

impl AuthError {
	/// Build details object for use with `Error::with_details`.
	/// Fields with default values are omitted from the serialized object.
	pub fn into_details(self) -> Object {
		let mut o = Object::new();
		if self.token_expired {
			o.insert("token_expired", true);
		}
		if self.session_expired {
			o.insert("session_expired", true);
		}
		o
	}

	/// Parse from `Error.details`; missing or invalid fields default to `false`.
	pub fn from_details(details: &Object) -> Self {
		let token_expired = details
			.get("token_expired")
			.and_then(|v| <bool as SurrealValue>::from_value(v.clone()).ok())
			.unwrap_or(false);
		let session_expired = details
			.get("session_expired")
			.and_then(|v| <bool as SurrealValue>::from_value(v.clone()).ok())
			.unwrap_or(false);
		Self {
			token_expired,
			session_expired,
		}
	}
}

/// Iterator over an error and its cause chain.
#[derive(Debug)]
pub struct Chain<'a> {
	current: Option<&'a Error>,
}

impl<'a> Iterator for Chain<'a> {
	type Item = &'a Error;

	fn next(&mut self) -> Option<Self::Item> {
		let err = self.current?;
		self.current = err.cause.as_deref();
		Some(err)
	}
}

impl fmt::Display for Error {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{}", self.message)?;
		if let Some(cause) = &self.cause {
			write!(f, ": {}", cause)?;
		}
		Ok(())
	}
}

impl std::error::Error for Error {
	fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
		self.cause.as_deref().map(|e| e as &(dyn std::error::Error + 'static))
	}
}

impl SurrealValue for Error {
	fn kind_of() -> Kind {
		Kind::Object
	}

	fn is_value(value: &Value) -> bool {
		matches!(value, Value::Object(obj) if obj.contains_key("kind") && obj.contains_key("message"))
	}

	fn into_value(self) -> Value {
		let mut obj = Object::new();
		let kind_str = serde_json::to_string(&self.kind).expect("ErrorKind serializes to a string");
		obj.insert("kind", kind_str.trim_matches('"').to_string());
		obj.insert("message", self.message);
		if let Some(details) = self.details {
			obj.insert("details", details);
		}
		if let Some(cause) = self.cause {
			obj.insert("cause", cause.into_value());
		}
		Value::Object(obj)
	}

	fn from_value(value: Value) -> anyhow::Result<Self> {
		let Value::Object(mut obj) = value else {
			anyhow::bail!("expected object for Error");
		};
		let kind_str = obj.remove("kind").context("missing 'kind'")?.into_string()?;
		let kind = serde_json::from_str(&format!("\"{}\"", kind_str)).context("invalid 'kind'")?;
		let message = obj.remove("message").context("missing 'message'")?.into_string()?;
		let details = obj.remove("details").map(Object::from_value).transpose()?;
		let cause = obj.remove("cause").map(Error::from_value).transpose()?.map(Box::new);
		Ok(Self {
			kind,
			message,
			details,
			cause,
		})
	}
}

// -----------------------------------------------------------------------------
// Type conversion errors (internal to the types layer)
// -----------------------------------------------------------------------------

/// Errors that can occur when working with SurrealDB types
#[derive(Debug, Clone)]
pub enum TypeError {
	/// Failed to convert between types
	Conversion(ConversionError),
	/// Value is out of range for the target type
	OutOfRange(OutOfRangeError),
	/// Array or tuple length mismatch
	LengthMismatch(LengthMismatchError),
	/// Invalid format or structure
	Invalid(String),
}

/// Error when converting between types
#[derive(Debug, Clone)]
pub struct ConversionError {
	/// The expected kind
	pub expected: Kind,
	/// The actual kind that was received
	pub actual: Kind,
	/// Optional context about what was being converted
	pub context: Option<String>,
}

/// Error when a value is out of range for the target type
#[derive(Debug, Clone)]
pub struct OutOfRangeError {
	/// The value that was out of range
	pub value: String,
	/// The target type name
	pub target_type: String,
	/// Optional additional context
	pub context: Option<String>,
}

/// Error when an array or tuple has the wrong length
#[derive(Debug, Clone)]
pub struct LengthMismatchError {
	/// The expected length
	pub expected: usize,
	/// The actual length received
	pub actual: usize,
	/// The target type name
	pub target_type: String,
}

impl ConversionError {
	/// Create a new conversion error
	pub fn new(expected: Kind, actual: Kind) -> Self {
		Self {
			expected,
			actual,
			context: None,
		}
	}

	/// Create a conversion error from a value
	pub fn from_value(expected: Kind, value: &Value) -> Self {
		Self {
			expected,
			actual: value.kind(),
			context: None,
		}
	}

	/// Add context to the error
	pub fn with_context(mut self, context: impl Into<String>) -> Self {
		self.context = Some(context.into());
		self
	}
}

impl OutOfRangeError {
	/// Create a new out of range error
	pub fn new(value: impl fmt::Display, target_type: impl Into<String>) -> Self {
		Self {
			value: value.to_string(),
			target_type: target_type.into(),
			context: None,
		}
	}

	/// Add context to the error
	pub fn with_context(mut self, context: impl Into<String>) -> Self {
		self.context = Some(context.into());
		self
	}
}

impl LengthMismatchError {
	/// Create a new length mismatch error
	pub fn new(expected: usize, actual: usize, target_type: impl Into<String>) -> Self {
		Self {
			expected,
			actual,
			target_type: target_type.into(),
		}
	}
}

impl fmt::Display for TypeError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			TypeError::Conversion(e) => write!(f, "{}", e),
			TypeError::OutOfRange(e) => write!(f, "{}", e),
			TypeError::LengthMismatch(e) => write!(f, "{}", e),
			TypeError::Invalid(s) => write!(f, "Invalid: {}", s),
		}
	}
}

impl fmt::Display for ConversionError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "Expected {}, got {}", self.expected.to_sql(), self.actual.to_sql())?;
		if let Some(context) = &self.context {
			write!(f, " ({})", context)?;
		}
		Ok(())
	}
}

impl fmt::Display for OutOfRangeError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "Value {} is out of range for type {}", self.value, self.target_type)?;
		if let Some(context) = &self.context {
			write!(f, " ({})", context)?;
		}
		Ok(())
	}
}

impl fmt::Display for LengthMismatchError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(
			f,
			"Length mismatch for {}: expected {}, got {}",
			self.target_type, self.expected, self.actual
		)
	}
}

impl std::error::Error for TypeError {}
impl std::error::Error for ConversionError {}
impl std::error::Error for OutOfRangeError {}
impl std::error::Error for LengthMismatchError {}

// Note: anyhow::Error automatically implements From for all types that implement std::error::Error,
// so we don't need manual From implementations here.

/// Helper function to create a conversion error
pub fn conversion_error(expected: Kind, value: impl Into<Value>) -> anyhow::Error {
	let value = value.into();
	ConversionError::from_value(expected, &value).into()
}

/// Helper function to create an out of range error
pub fn out_of_range_error(
	value: impl fmt::Display,
	target_type: impl Into<String>,
) -> anyhow::Error {
	OutOfRangeError::new(value, target_type).into()
}

/// Helper function to create a length mismatch error
pub fn length_mismatch_error(
	expected: usize,
	actual: usize,
	target_type: impl Into<String>,
) -> anyhow::Error {
	LengthMismatchError::new(expected, actual, target_type).into()
}

/// Helper function to create a conversion error for union types (Either)
/// where the value doesn't match any of the possible types
pub fn union_conversion_error(expected: Kind, value: impl Into<Value>) -> anyhow::Error {
	let value = value.into();
	ConversionError::from_value(expected, &value)
		.with_context("Value does not match any variant in union type")
		.into()
}
