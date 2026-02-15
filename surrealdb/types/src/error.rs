use std::fmt;

use serde::{Deserialize, Serialize};

use crate::{Kind, SurrealValue, ToSql, Value};

// -----------------------------------------------------------------------------
// JSON-RPC 2.0 and SurrealDB-specific error codes (wire backwards compatibility)
// -----------------------------------------------------------------------------

/// Numeric error codes used on the wire for RPC. Kept for backwards compatibility.
#[allow(missing_docs)]
mod code {
	pub const PARSE_ERROR: i64 = -32700;
	pub const INVALID_REQUEST: i64 = -32600;
	pub const METHOD_NOT_FOUND: i64 = -32601;
	pub const METHOD_NOT_ALLOWED: i64 = -32602;
	pub const INVALID_PARAMS: i64 = -32603;
	pub const LIVE_QUERY_NOT_SUPPORTED: i64 = -32604;
	pub const BAD_LIVE_QUERY_CONFIG: i64 = -32605;
	pub const BAD_GRAPHQL_CONFIG: i64 = -32606;
	pub const INTERNAL_ERROR: i64 = -32000;
	pub const CLIENT_SIDE_ERROR: i64 = -32001;
	pub const INVALID_AUTH: i64 = -32002;
	pub const QUERY_NOT_EXECUTED: i64 = -32003;
	pub const QUERY_TIMEDOUT: i64 = -32004;
	pub const QUERY_CANCELLED: i64 = -32005;
	pub const THROWN: i64 = -32006;
	pub const SERIALIZATION_ERROR: i64 = -32007;
	pub const DESERIALIZATION_ERROR: i64 = -32008;
}

/// Default wire code when none is specified (e.g. for deserialization of older wire format).
fn default_code() -> i64 {
	code::INTERNAL_ERROR
}

// -----------------------------------------------------------------------------
// Public API error type (wire-friendly, non-lossy, supports chaining)
// -----------------------------------------------------------------------------

/// Curated error kind for public APIs.
///
/// Maps the full set of internal/database errors into a smaller set of
/// categories. Serializes as a snake_case string on the wire (e.g. `"validation"`).
/// The enum is non-exhaustive so new variants can be added later. The server
/// should map any unmappable error to [`Internal`](ErrorKind::Internal) before sending.
#[derive(Clone, Debug, Default, PartialEq, Eq, SurrealValue, Serialize, Deserialize)]
#[surreal(crate = "crate")]
#[surreal(untagged)]
#[non_exhaustive]
pub enum ErrorKind {
	/// Invalid input: parse error, invalid request or params.
	Validation,
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
	/// Operation or feature not allowed (e.g. RPC method, scripting, function, net target).
	NotAllowed,
	/// Resource not found (e.g. table, record, namespace).
	NotFound,
	/// Resource already exists (e.g. table, record).
	AlreadyExists,
	/// Connection error (e.g. uninitialised, already connected). Used in the SDK.
	Connection,
	/// Internal or unexpected error (server or client).
	#[default]
	Internal,
}

/// Public error type for SurrealDB APIs
///
/// Designed to be returned from public APIs (including over the wire). It is
/// wire-friendly and non-lossy: serialization preserves `kind`, `message`,
/// `details`, and the cause chain. Use this type whenever an error crosses
/// an API boundary (e.g. server response, SDK method return).
///
/// When deserialising via [`SurrealValue::from_value`], missing fields (e.g. `kind` from older
/// clients) are filled from [`Default`]; use `#[surreal(default)]` for backwards compatibility.
#[derive(Clone, Debug, PartialEq, Eq, SurrealValue, Serialize, Deserialize)]
#[surreal(crate = "crate")]
pub struct Error {
	/// Wire-only error code for RPC backwards compatibility. Not part of the public API; may be
	/// removed in the next major release. Always present on the wire; defaults to
	/// [`code::INTERNAL_ERROR`] when not otherwise set.
	#[serde(default = "default_code")]
	#[surreal(default = "default_code")]
	code: i64,
	/// Machine-readable error kind. Defaults to [`Internal`](ErrorKind::Internal) when not
	/// present (e.g. when deserialising errors from older clients that did not send `kind`).
	#[serde(default)]
	#[surreal(default)]
	kind: ErrorKind,
	/// Human-readable error message.
	message: String,
	/// Optional structured details (e.g. `{ "name": "users" }` for table not found).
	#[serde(skip_serializing_if = "Option::is_none")]
	#[surreal(skip_serializing_if = "Option::is_none")]
	details: Option<Value>,
	/// The underlying cause of this error, if any. Semantically: "this error was caused by that
	/// one".
	#[serde(skip_serializing_if = "Option::is_none")]
	#[surreal(skip_serializing_if = "Option::is_none")]
	cause: Option<Box<Error>>,
}

impl Error {
	/// Validation error (parse error, invalid request or params), with optional structured details.
	/// When `details` is provided, the wire code is set from the variant (e.g. `Parse` →
	/// `PARSE_ERROR`).
	pub fn validation(message: String, details: Option<ValidationError>) -> Self {
		let code = details
			.as_ref()
			.map(|d| match d {
				ValidationError::Parse => code::PARSE_ERROR,
				ValidationError::InvalidRequest => code::INVALID_REQUEST,
				ValidationError::InvalidParams => code::INVALID_PARAMS,
			})
			.unwrap_or(code::INTERNAL_ERROR);
		Self {
			kind: ErrorKind::Validation,
			message,
			code,
			details: details.map(ValidationError::into_value),
			cause: None,
		}
	}

	/// Not-allowed error (e.g. method, scripting, function, net target), with optional
	/// structured details. When `details` is provided, the wire code is set from the variant.
	pub fn not_allowed(message: String, details: Option<NotAllowedError>) -> Self {
		let code = details
			.as_ref()
			.map(|d| match d {
				NotAllowedError::Method => code::METHOD_NOT_ALLOWED,
			})
			.unwrap_or(code::INTERNAL_ERROR);
		Self {
			kind: ErrorKind::NotAllowed,
			message,
			code,
			details: details.map(NotAllowedError::into_value),
			cause: None,
		}
	}

	/// Configuration error (feature or config not supported), with optional structured details.
	/// When `details` is provided, the wire code is set from the variant.
	pub fn configuration(message: String, details: Option<ConfigurationError>) -> Self {
		let code = details
			.as_ref()
			.map(|d| match d {
				ConfigurationError::LiveQueryNotSupported => code::LIVE_QUERY_NOT_SUPPORTED,
				ConfigurationError::BadLiveQueryConfig => code::BAD_LIVE_QUERY_CONFIG,
				ConfigurationError::BadGraphqlConfig => code::BAD_GRAPHQL_CONFIG,
			})
			.unwrap_or(code::INTERNAL_ERROR);
		Self {
			kind: ErrorKind::Configuration,
			message,
			code,
			details: details.map(ConfigurationError::into_value),
			cause: None,
		}
	}

	/// Authentication or authorisation error, with optional structured details.
	/// When `details` is provided, the wire code is set from the variant.
	pub fn auth(message: String, details: Option<AuthError>) -> Self {
		let code = details
			.as_ref()
			.map(|d| match d {
				AuthError::TokenExpired => code::INVALID_AUTH,
				AuthError::SessionExpired => code::INTERNAL_ERROR,
				AuthError::InvalidAuth
				| AuthError::UnexpectedAuth
				| AuthError::MissingUserOrPass
				| AuthError::NoSigninTarget
				| AuthError::InvalidPass
				| AuthError::TokenMakingFailed
				| AuthError::InvalidRole(_)
				| AuthError::NotAllowed {
					..
				}
				| AuthError::InvalidSignup => code::INVALID_AUTH,
			})
			.unwrap_or(code::INTERNAL_ERROR);
		Self {
			kind: ErrorKind::Auth,
			message,
			code,
			details: details.map(AuthError::into_value),
			cause: None,
		}
	}

	/// User-thrown error (e.g. from THROW in SurrealQL). Sets wire code for RPC.
	pub fn thrown(message: String) -> Self {
		Self {
			kind: ErrorKind::Thrown,
			message,
			code: code::THROWN,
			details: None,
			cause: None,
		}
	}

	/// Query execution error (not executed, timeout, cancelled), with optional structured details.
	/// When `details` is provided, the wire code is set from the variant.
	pub fn query(message: String, details: Option<QueryError>) -> Self {
		let code = details
			.as_ref()
			.map(|d| match d {
				QueryError::NotExecuted => code::QUERY_NOT_EXECUTED,
				QueryError::Timedout => code::QUERY_TIMEDOUT,
				QueryError::Cancelled => code::QUERY_CANCELLED,
			})
			.unwrap_or(code::INTERNAL_ERROR);
		Self {
			kind: ErrorKind::Query,
			message,
			code,
			details: details.map(QueryError::into_value),
			cause: None,
		}
	}

	/// Serialisation or deserialisation error, with optional structured details.
	/// When `details` is provided, the wire code is set from the variant.
	pub fn serialization(message: String, details: Option<SerializationError>) -> Self {
		let code = details
			.as_ref()
			.map(|d| match d {
				SerializationError::Serialization => code::SERIALIZATION_ERROR,
				SerializationError::Deserialization => code::DESERIALIZATION_ERROR,
			})
			.unwrap_or(code::INTERNAL_ERROR);
		Self {
			kind: ErrorKind::Serialization,
			message,
			code,
			details: details.map(SerializationError::into_value),
			cause: None,
		}
	}

	/// Resource not found (e.g. table, record, namespace, RPC method), with optional
	/// structured details. When `details` is `NotFoundError::Method`, the wire code is set to
	/// `METHOD_NOT_FOUND` for RPC backwards compatibility.
	pub fn not_found(message: String, details: Option<NotFoundError>) -> Self {
		let code = details
			.as_ref()
			.and_then(|d| match d {
				NotFoundError::Method => Some(code::METHOD_NOT_FOUND),
				_ => None,
			})
			.unwrap_or(code::INTERNAL_ERROR);
		Self {
			kind: ErrorKind::NotFound,
			message,
			code,
			details: details.map(NotFoundError::into_value),
			cause: None,
		}
	}

	/// Resource already exists (e.g. table, record), with optional structured details.
	pub fn already_exists(message: String, details: Option<AlreadyExistsError>) -> Self {
		Self {
			kind: ErrorKind::AlreadyExists,
			message,
			code: code::INTERNAL_ERROR,
			details: details.map(AlreadyExistsError::into_value),
			cause: None,
		}
	}

	/// Connection error (e.g. uninitialised, already connected), with optional structured details.
	/// Used in the SDK for client-side connection state errors.
	pub fn connection(message: String, details: Option<ConnectionError>) -> Self {
		Self {
			kind: ErrorKind::Connection,
			message,
			code: code::CLIENT_SIDE_ERROR,
			details: details.map(ConnectionError::into_value),
			cause: None,
		}
	}

	/// Internal or unexpected error (server or client). Sets wire code for RPC.
	pub fn internal(message: String) -> Self {
		Self {
			kind: ErrorKind::Internal,
			message,
			code: code::INTERNAL_ERROR,
			details: None,
			cause: None,
		}
	}

	/// Adds optional structured details to this error.
	pub fn with_details(mut self, details: impl SurrealValue) -> Self {
		self.details = Some(details.into_value());
		self
	}

	/// Sets the cause of this error (the error that led to this one).
	pub fn with_cause(mut self, cause: Error) -> Self {
		self.cause = Some(Box::new(cause));
		self
	}

	/// Returns the machine-readable error kind.
	pub fn kind(&self) -> &ErrorKind {
		&self.kind
	}

	/// Returns the human-readable error message.
	pub fn message(&self) -> &str {
		&self.message
	}

	/// Returns optional structured details, if any.
	pub fn details(&self) -> Option<&Value> {
		self.details.as_ref()
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
		if self.kind() != &ErrorKind::Auth {
			return None;
		}
		let details = self.details()?;
		AuthError::from_value(details.clone()).ok()
	}

	/// Returns structured validation error details when this error's kind is
	/// [`ErrorKind::Validation`] and `details` is present. Use this instead of matching on the
	/// error message string.
	pub fn validation_details(&self) -> Option<ValidationError> {
		if self.kind() != &ErrorKind::Validation {
			return None;
		}
		let details = self.details()?;
		ValidationError::from_value(details.clone()).ok()
	}

	/// Returns structured not-allowed error details when this error's kind is
	/// [`ErrorKind::NotAllowed`] and `details` is present.
	pub fn not_allowed_details(&self) -> Option<NotAllowedError> {
		if self.kind() != &ErrorKind::NotAllowed {
			return None;
		}
		let details = self.details()?;
		NotAllowedError::from_value(details.clone()).ok()
	}

	/// Returns structured configuration error details when this error's kind is
	/// [`ErrorKind::Configuration`] and `details` is present.
	pub fn configuration_details(&self) -> Option<ConfigurationError> {
		if self.kind() != &ErrorKind::Configuration {
			return None;
		}
		let details = self.details()?;
		ConfigurationError::from_value(details.clone()).ok()
	}

	/// Returns structured serialization error details when this error's kind is
	/// [`ErrorKind::Serialization`] and `details` is present.
	pub fn serialization_details(&self) -> Option<SerializationError> {
		if self.kind() != &ErrorKind::Serialization {
			return None;
		}
		let details = self.details()?;
		SerializationError::from_value(details.clone()).ok()
	}

	/// Returns structured not-found error details when this error's kind is
	/// [`ErrorKind::NotFound`] and `details` is present.
	pub fn not_found_details(&self) -> Option<NotFoundError> {
		if self.kind() != &ErrorKind::NotFound {
			return None;
		}
		let details = self.details()?;
		NotFoundError::from_value(details.clone()).ok()
	}

	/// Returns structured query error details when this error's kind is [`ErrorKind::Query`] and
	/// `details` is present.
	pub fn query_details(&self) -> Option<QueryError> {
		if self.kind() != &ErrorKind::Query {
			return None;
		}
		let details = self.details()?;
		QueryError::from_value(details.clone()).ok()
	}

	/// Returns structured already-exists error details when this error's kind is
	/// [`ErrorKind::AlreadyExists`] and `details` is present.
	pub fn already_exists_details(&self) -> Option<AlreadyExistsError> {
		if self.kind() != &ErrorKind::AlreadyExists {
			return None;
		}
		let details = self.details()?;
		AlreadyExistsError::from_value(details.clone()).ok()
	}

	/// Returns structured connection error details when this error's kind is
	/// [`ErrorKind::Connection`] and `details` is present.
	pub fn connection_details(&self) -> Option<ConnectionError> {
		if self.kind() != &ErrorKind::Connection {
			return None;
		}
		let details = self.details()?;
		ConnectionError::from_value(details.clone()).ok()
	}
}

// -----------------------------------------------------------------------------
// Structured error details (wire format in Error.details)
// -----------------------------------------------------------------------------

/// Auth failure reason for [`ErrorKind::Auth`] errors.
///
/// Serialized as a string in `Error.details` (e.g. `"SessionExpired"`) so clients can detect
/// auth failure reasons without parsing the message string.
#[derive(Clone, Debug, PartialEq, Eq, SurrealValue)]
#[surreal(crate = "crate")]
#[surreal(untagged)]
pub enum AuthError {
	/// The token used for authentication has expired.
	TokenExpired,
	/// The session has expired.
	SessionExpired,
	/// Authentication failed (invalid credentials or similar).
	InvalidAuth,
	/// Unexpected error while performing authentication.
	UnexpectedAuth,
	/// Username or password was not provided.
	MissingUserOrPass,
	/// No signin target (SC, DB, NS, or KV) specified.
	NoSigninTarget,
	/// The password did not verify.
	InvalidPass,
	/// Failed to create the authentication token.
	TokenMakingFailed,
	/// Signup failed.
	InvalidSignup,
	/// Invalid role (IAM). Carries the role name.
	InvalidRole(String),
	/// Not enough permissions to perform the action (IAM). Carries actor, action, resource.
	NotAllowed {
		/// Actor that attempted the action.
		actor: String,
		/// Action that was attempted.
		action: String,
		/// Resource the action was attempted on.
		resource: String,
	},
}

/// Validation failure reason for [`ErrorKind::Validation`] errors.
///
/// Serialized as a string in `Error.details` (e.g. `"InvalidParams"`) so clients can detect
/// validation failure reasons without parsing the message string.
#[derive(Clone, Debug, PartialEq, Eq, SurrealValue)]
#[surreal(crate = "crate")]
#[surreal(untagged)]
pub enum ValidationError {
	/// Parse error (invalid message or request format).
	Parse,
	/// Invalid request structure.
	InvalidRequest,
	/// Invalid parameters.
	InvalidParams,
}

/// Not-allowed reason for [`ErrorKind::NotAllowed`] errors.
///
/// Serialized as a string in `Error.details` so clients can detect the reason without parsing
/// the message string.
#[derive(Clone, Debug, PartialEq, Eq, SurrealValue)]
#[surreal(crate = "crate")]
#[surreal(untagged)]
pub enum NotAllowedError {
	/// RPC method, scripting, function, or net target not allowed.
	Method,
}

/// Configuration failure reason for [`ErrorKind::Configuration`] errors.
///
/// Serialized as a string in `Error.details` so clients can detect the reason without parsing
/// the message string.
#[derive(Clone, Debug, PartialEq, Eq, SurrealValue)]
#[surreal(crate = "crate")]
#[surreal(untagged)]
pub enum ConfigurationError {
	/// Live query not supported.
	LiveQueryNotSupported,
	/// Bad live query config.
	BadLiveQueryConfig,
	/// Bad GraphQL config.
	BadGraphqlConfig,
}

/// Serialisation failure reason for [`ErrorKind::Serialization`] errors.
///
/// Serialized as a string in `Error.details` so clients can detect the reason without parsing
/// the message string.
#[derive(Clone, Debug, PartialEq, Eq, SurrealValue)]
#[surreal(crate = "crate")]
#[surreal(untagged)]
pub enum SerializationError {
	/// Serialisation error.
	Serialization,
	/// Deserialisation error.
	Deserialization,
}

/// Not-found reason for [`ErrorKind::NotFound`] errors.
///
/// Serialized as a string in `Error.details` (e.g. `"Session"`) so clients can detect
/// what was not found without parsing the message string.
#[derive(Clone, Debug, PartialEq, Eq, SurrealValue)]
#[surreal(crate = "crate")]
#[surreal(untagged)]
pub enum NotFoundError {
	/// RPC method not found.
	Method,
	/// Session not found.
	Session,
	/// Table not found.
	Table,
	/// Record not found.
	Record,
	/// Namespace not found.
	Namespace,
	/// Database not found.
	Database,
	/// Transaction not found.
	Transaction,
}

/// Query failure reason for [`ErrorKind::Query`] errors.
///
/// Serialized as a string in `Error.details` (e.g. `"Timedout"`) so clients can detect
/// the reason without parsing the message string.
#[derive(Clone, Debug, PartialEq, Eq, SurrealValue)]
#[surreal(crate = "crate")]
#[surreal(untagged)]
pub enum QueryError {
	/// Query was not executed.
	NotExecuted,
	/// Query timed out.
	Timedout,
	/// Query was cancelled.
	Cancelled,
}

/// Already-exists reason for [`ErrorKind::AlreadyExists`] errors.
///
/// Serialized as a string in `Error.details` (e.g. `"Session"`) so clients can detect
/// what already exists without parsing the message string.
#[derive(Clone, Debug, PartialEq, Eq, SurrealValue)]
#[surreal(crate = "crate")]
#[surreal(untagged)]
pub enum AlreadyExistsError {
	/// Session already exists.
	Session,
	/// Table already exists.
	Table,
	/// Record already exists.
	Record,
	/// Namespace already exists.
	Namespace,
	/// Database already exists.
	Database,
}

/// Connection failure reason for [`ErrorKind::Connection`] errors.
///
/// Serialized as a string in `Error.details` (e.g. `"Uninitialised"`) so clients can detect
/// the reason without parsing the message string. Used in the SDK for client-side connection
/// state errors.
#[derive(Clone, Debug, PartialEq, Eq, SurrealValue)]
#[surreal(crate = "crate")]
#[surreal(untagged)]
pub enum ConnectionError {
	/// Connection was used before being initialised.
	Uninitialised,
	/// Connect was called on an instance that is already connected.
	AlreadyConnected,
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
			write!(f, ": {}", cause.message)?;
		}
		Ok(())
	}
}

impl std::error::Error for Error {
	fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
		self.cause.as_deref().map(|e| e as &(dyn std::error::Error + 'static))
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

impl From<ConversionError> for Error {
	fn from(e: ConversionError) -> Self {
		Error::internal(e.to_string())
	}
}

impl From<OutOfRangeError> for Error {
	fn from(e: OutOfRangeError) -> Self {
		Error::internal(e.to_string())
	}
}

impl From<LengthMismatchError> for Error {
	fn from(e: LengthMismatchError) -> Self {
		Error::internal(e.to_string())
	}
}

impl From<TypeError> for Error {
	fn from(e: TypeError) -> Self {
		Error::internal(e.to_string())
	}
}

/// Helper function to create a conversion error
pub fn conversion_error(expected: Kind, value: impl Into<Value>) -> Error {
	let value = value.into();
	ConversionError::from_value(expected, &value).into()
}

/// Helper function to create an out of range error
pub fn out_of_range_error(value: impl fmt::Display, target_type: impl Into<String>) -> Error {
	OutOfRangeError::new(value, target_type).into()
}

/// Helper function to create a length mismatch error
pub fn length_mismatch_error(
	expected: usize,
	actual: usize,
	target_type: impl Into<String>,
) -> Error {
	LengthMismatchError::new(expected, actual, target_type).into()
}

/// Helper function to create a conversion error for union types (Either)
/// where the value doesn't match any of the possible types
pub fn union_conversion_error(expected: Kind, value: impl Into<Value>) -> Error {
	let value = value.into();
	ConversionError::from_value(expected, &value)
		.with_context("Value does not match any variant in union type")
		.into()
}
