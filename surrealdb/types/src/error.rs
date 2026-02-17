use std::fmt;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::{Kind, Object, SurrealValue, ToSql, Value};

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

/// A SurrealDB error kind
#[derive(Clone, Debug, Default, PartialEq, Eq, SurrealValue, Serialize, Deserialize)]
#[surreal(crate = "crate")]
#[surreal(untagged)]
#[non_exhaustive]
pub enum ErrorKind {
	/// Invalid input: parse error, invalid request or params.
	/// Used for validation failures.
	Validation,
	/// Feature or config not supported (e.g. live query, GraphQL config).
	/// Used for configuration errors.
	Configuration,
	/// User-thrown error (e.g. from THROW in SurrealQL).
	/// Used for errors thrown by user code.
	Thrown,
	/// Query execution failure (not executed, timeout, cancelled).
	/// Used for query errors.
	Query,
	/// Serialisation or deserialisation error.
	/// Used for serialization errors.
	Serialization,
	/// Operation or feature not allowed (e.g. RPC method, scripting, function, net target).
	/// Used for permission or method errors.
	NotAllowed,
	/// Resource not found (e.g. table, record, namespace).
	/// Used for missing resources.
	NotFound,
	/// Resource already exists (e.g. table, record).
	/// Used for duplicate resources.
	AlreadyExists,
	/// Connection error (e.g. uninitialised, already connected). Used in the SDK.
	/// Used for client connection errors.
	Connection,
	/// Internal or unexpected error (server or client).
	/// Used for unexpected failures.
	///
	/// Also used as the fallback for unknown error kinds from the wire (forward compat).
	#[default]
	#[serde(other)]
	#[surreal(other)]
	Internal,
}

/// Represents an error in SurrealDB
///
/// Designed to be returned from public APIs (including over the wire). It is
/// wire-friendly and non-lossy: serialization preserves `kind`, `message`,
/// `details`, and the cause chain. Use this type whenever an error crosses
/// an API boundary (e.g. server response, SDK method return).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Error {
	/// Wire-only error code for RPC backwards compatibility.
	#[serde(default = "default_code")]
	code: i64,
	/// The kind of error (validation, configuration, thrown, query, serialization, not allowed,
	/// not found, already exists, connection, internal).
	#[serde(default)]
	kind: ErrorKind,
	/// Human-readable error message describing the error.
	message: String,
	/// Optional typed structured details for the error. The variant matches the `kind` field.
	#[serde(skip_serializing_if = "Option::is_none")]
	details: Option<ErrorDetails>,
}

impl Error {
	/// Validation error (parse error, invalid request or params), with optional structured details.
	/// When `details` is provided, the wire code is set from the variant (e.g. `Parse` →
	/// `PARSE_ERROR`).
	pub fn validation(message: String, details: impl Into<Option<ValidationError>>) -> Self {
		let details = details.into();
		let code = details
			.as_ref()
			.map(|d| match d {
				ValidationError::Parse => code::PARSE_ERROR,
				ValidationError::InvalidRequest => code::INVALID_REQUEST,
				ValidationError::InvalidParams => code::INVALID_PARAMS,
				ValidationError::NamespaceEmpty
				| ValidationError::DatabaseEmpty
				| ValidationError::InvalidParameter {
					..
				}
				| ValidationError::InvalidContent {
					..
				}
				| ValidationError::InvalidMerge {
					..
				} => code::INVALID_REQUEST,
			})
			.unwrap_or(code::INTERNAL_ERROR);
		Self {
			kind: ErrorKind::Validation,
			message,
			code,
			details: details.map(ErrorDetails::Validation),
		}
	}

	/// Not-allowed error (e.g. method, scripting, function, net target), with optional
	/// structured details. When `details` is provided, the wire code is set from the variant.
	pub fn not_allowed(message: String, details: impl Into<Option<NotAllowedError>>) -> Self {
		let details = details.into();
		let code = details
			.as_ref()
			.map(|d| match d {
				NotAllowedError::Auth(auth_error) => match auth_error {
					AuthError::TokenExpired => code::INVALID_AUTH,
					AuthError::SessionExpired => code::INTERNAL_ERROR,
					AuthError::InvalidAuth
					| AuthError::UnexpectedAuth
					| AuthError::MissingUserOrPass
					| AuthError::NoSigninTarget
					| AuthError::InvalidPass
					| AuthError::TokenMakingFailed
					| AuthError::InvalidRole {
						..
					}
					| AuthError::NotAllowed {
						..
					}
					| AuthError::InvalidSignup => code::INVALID_AUTH,
				},
				NotAllowedError::Method {
					..
				}
				| NotAllowedError::Scripting
				| NotAllowedError::Function {
					..
				}
				| NotAllowedError::Target {
					..
				} => code::METHOD_NOT_ALLOWED,
			})
			.unwrap_or(code::INTERNAL_ERROR);
		Self {
			kind: ErrorKind::NotAllowed,
			message,
			code,
			details: details.map(ErrorDetails::NotAllowed),
		}
	}

	/// Configuration error (feature or config not supported), with optional structured details.
	/// When `details` is provided, the wire code is set from the variant.
	pub fn configuration(message: String, details: impl Into<Option<ConfigurationError>>) -> Self {
		let details = details.into();
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
			details: details.map(ErrorDetails::Configuration),
		}
	}

	/// User-thrown error (e.g. from THROW in SurrealQL). Sets wire code for RPC.
	pub fn thrown(message: String) -> Self {
		Self {
			kind: ErrorKind::Thrown,
			message,
			code: code::THROWN,
			details: None,
		}
	}

	/// Query execution error (not executed, timeout, cancelled), with optional structured details.
	/// When `details` is provided, the wire code is set from the variant.
	pub fn query(message: String, details: impl Into<Option<QueryError>>) -> Self {
		let details = details.into();
		let code = details
			.as_ref()
			.map(|d| match d {
				QueryError::NotExecuted => code::QUERY_NOT_EXECUTED,
				QueryError::TimedOut {
					..
				} => code::QUERY_TIMEDOUT,
				QueryError::Cancelled => code::QUERY_CANCELLED,
			})
			.unwrap_or(code::INTERNAL_ERROR);
		Self {
			kind: ErrorKind::Query,
			message,
			code,
			details: details.map(ErrorDetails::Query),
		}
	}

	/// Serialisation or deserialisation error, with optional structured details.
	/// When `details` is provided, the wire code is set from the variant.
	pub fn serialization(message: String, details: impl Into<Option<SerializationError>>) -> Self {
		let details = details.into();
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
			details: details.map(ErrorDetails::Serialization),
		}
	}

	/// Resource not found (e.g. table, record, namespace, RPC method), with optional
	/// structured details. When `details` is `NotFoundError::Method`, the wire code is set to
	/// `METHOD_NOT_FOUND` for RPC backwards compatibility.
	pub fn not_found(message: String, details: impl Into<Option<NotFoundError>>) -> Self {
		let details = details.into();
		let code = details
			.as_ref()
			.and_then(|d| match d {
				NotFoundError::Method {
					..
				} => Some(code::METHOD_NOT_FOUND),
				_ => None,
			})
			.unwrap_or(code::INTERNAL_ERROR);
		Self {
			kind: ErrorKind::NotFound,
			message,
			code,
			details: details.map(ErrorDetails::NotFound),
		}
	}

	/// Resource already exists (e.g. table, record), with optional structured details.
	pub fn already_exists(message: String, details: impl Into<Option<AlreadyExistsError>>) -> Self {
		let details = details.into();
		Self {
			kind: ErrorKind::AlreadyExists,
			message,
			code: code::INTERNAL_ERROR,
			details: details.map(ErrorDetails::AlreadyExists),
		}
	}

	/// Connection error (e.g. uninitialised, already connected), with optional structured details.
	/// Used in the SDK for client-side connection state errors.
	pub fn connection(message: String, details: impl Into<Option<ConnectionError>>) -> Self {
		let details = details.into();
		Self {
			kind: ErrorKind::Connection,
			message,
			code: code::CLIENT_SIDE_ERROR,
			details: details.map(ErrorDetails::Connection),
		}
	}

	/// Internal or unexpected error (server or client). Sets wire code for RPC.
	pub fn internal(message: String) -> Self {
		Self {
			kind: ErrorKind::Internal,
			message,
			code: code::INTERNAL_ERROR,
			details: None,
		}
	}

	/// Build an error from the query-result wire shape (message, optional kind, details).
	/// Used when deserialising query result error payloads that do not include `code`. Uses
	/// [`default_code`] and defaults `kind` to [`Internal`](ErrorKind::Internal) when not present.
	#[doc(hidden)]
	pub fn from_parts(
		message: String,
		kind: Option<ErrorKind>,
		details: Option<Value>,
	) -> Self {
		let kind = kind.unwrap_or_default();
		let typed_details =
			details.and_then(|v| ErrorDetails::from_value_with_kind(&kind, v).ok());
		Self {
			code: default_code(),
			kind,
			message,
			details: typed_details,
		}
	}

	/// Returns the machine-readable error kind.
	pub fn kind(&self) -> &ErrorKind {
		&self.kind
	}

	/// Returns the human-readable error message.
	pub fn message(&self) -> &str {
		&self.message
	}

	/// Returns optional typed structured details, if any.
	pub fn details(&self) -> Option<&ErrorDetails> {
		self.details.as_ref()
	}

	/// Returns structured validation error details, if this is a validation error with details.
	pub fn validation_details(&self) -> Option<&ValidationError> {
		match &self.details {
			Some(ErrorDetails::Validation(d)) => Some(d),
			_ => None,
		}
	}

	/// Returns structured not-allowed error details, if this is a not-allowed error with details.
	pub fn not_allowed_details(&self) -> Option<&NotAllowedError> {
		match &self.details {
			Some(ErrorDetails::NotAllowed(d)) => Some(d),
			_ => None,
		}
	}

	/// Returns structured configuration error details, if this is a configuration error with details.
	pub fn configuration_details(&self) -> Option<&ConfigurationError> {
		match &self.details {
			Some(ErrorDetails::Configuration(d)) => Some(d),
			_ => None,
		}
	}

	/// Returns structured serialization error details, if this is a serialization error with details.
	pub fn serialization_details(&self) -> Option<&SerializationError> {
		match &self.details {
			Some(ErrorDetails::Serialization(d)) => Some(d),
			_ => None,
		}
	}

	/// Returns structured not-found error details, if this is a not-found error with details.
	pub fn not_found_details(&self) -> Option<&NotFoundError> {
		match &self.details {
			Some(ErrorDetails::NotFound(d)) => Some(d),
			_ => None,
		}
	}

	/// Returns structured query error details, if this is a query error with details.
	pub fn query_details(&self) -> Option<&QueryError> {
		match &self.details {
			Some(ErrorDetails::Query(d)) => Some(d),
			_ => None,
		}
	}

	/// Returns structured already-exists error details, if this is an already-exists error with details.
	pub fn already_exists_details(&self) -> Option<&AlreadyExistsError> {
		match &self.details {
			Some(ErrorDetails::AlreadyExists(d)) => Some(d),
			_ => None,
		}
	}

	/// Returns structured connection error details, if this is a connection error with details.
	pub fn connection_details(&self) -> Option<&ConnectionError> {
		match &self.details {
			Some(ErrorDetails::Connection(d)) => Some(d),
			_ => None,
		}
	}
}

// -----------------------------------------------------------------------------
// ErrorDetails enum (typed wrapper for all detail variants)
// -----------------------------------------------------------------------------

/// Typed error details. Each variant wraps the detail enum for a specific
/// [`ErrorKind`]. This allows Rust users to pattern-match on details directly
/// instead of working with raw `Value`.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorDetails {
	/// Validation error details.
	Validation(ValidationError),
	/// Configuration error details.
	Configuration(ConfigurationError),
	/// Query error details.
	Query(QueryError),
	/// Serialization error details.
	Serialization(SerializationError),
	/// Not-allowed error details.
	NotAllowed(NotAllowedError),
	/// Not-found error details.
	NotFound(NotFoundError),
	/// Already-exists error details.
	AlreadyExists(AlreadyExistsError),
	/// Connection error details.
	Connection(ConnectionError),
}

impl ErrorDetails {
	/// Deserialize details using the error kind to select the right variant.
	/// O(1) dispatch -- no trial-and-error parsing.
	pub(crate) fn from_value_with_kind(kind: &ErrorKind, value: Value) -> Result<Self, Error> {
		match kind {
			ErrorKind::Validation => {
				ValidationError::from_value(value).map(ErrorDetails::Validation)
			}
			ErrorKind::Configuration => {
				ConfigurationError::from_value(value).map(ErrorDetails::Configuration)
			}
			ErrorKind::Query => QueryError::from_value(value).map(ErrorDetails::Query),
			ErrorKind::Serialization => {
				SerializationError::from_value(value).map(ErrorDetails::Serialization)
			}
			ErrorKind::NotAllowed => {
				NotAllowedError::from_value(value).map(ErrorDetails::NotAllowed)
			}
			ErrorKind::NotFound => NotFoundError::from_value(value).map(ErrorDetails::NotFound),
			ErrorKind::AlreadyExists => {
				AlreadyExistsError::from_value(value).map(ErrorDetails::AlreadyExists)
			}
			ErrorKind::Connection => {
				ConnectionError::from_value(value).map(ErrorDetails::Connection)
			}
			_ => Err(Error::internal("No details type for this error kind".to_string())),
		}
	}
}

impl SurrealValue for ErrorDetails {
	fn kind_of() -> Kind {
		Kind::Object
	}

	fn is_value(value: &Value) -> bool {
		matches!(value, Value::Object(_))
	}

	fn into_value(self) -> Value {
		match self {
			Self::Validation(d) => d.into_value(),
			Self::Configuration(d) => d.into_value(),
			Self::Query(d) => d.into_value(),
			Self::Serialization(d) => d.into_value(),
			Self::NotAllowed(d) => d.into_value(),
			Self::NotFound(d) => d.into_value(),
			Self::AlreadyExists(d) => d.into_value(),
			Self::Connection(d) => d.into_value(),
		}
	}

	fn from_value(value: Value) -> Result<Self, Error> {
		// Fallback slow path: try each variant in order.
		// Prefer ErrorDetails::from_value_with_kind when ErrorKind is available.
		if let Ok(v) = ValidationError::from_value(value.clone()) {
			return Ok(Self::Validation(v));
		}
		if let Ok(v) = ConfigurationError::from_value(value.clone()) {
			return Ok(Self::Configuration(v));
		}
		if let Ok(v) = QueryError::from_value(value.clone()) {
			return Ok(Self::Query(v));
		}
		if let Ok(v) = SerializationError::from_value(value.clone()) {
			return Ok(Self::Serialization(v));
		}
		if let Ok(v) = NotAllowedError::from_value(value.clone()) {
			return Ok(Self::NotAllowed(v));
		}
		if let Ok(v) = NotFoundError::from_value(value.clone()) {
			return Ok(Self::NotFound(v));
		}
		if let Ok(v) = AlreadyExistsError::from_value(value.clone()) {
			return Ok(Self::AlreadyExists(v));
		}
		if let Ok(v) = ConnectionError::from_value(value) {
			return Ok(Self::Connection(v));
		}
		Err(Error::internal("Failed to decode ErrorDetails".to_string()))
	}
}

// -----------------------------------------------------------------------------
// Structured error details (wire format in Error.details)
// -----------------------------------------------------------------------------

/// Auth failure reason for [`ErrorKind::NotAllowed`] errors.
#[derive(Clone, Debug, PartialEq, Eq, SurrealValue)]
#[surreal(crate = "crate")]
#[surreal(tag = "kind", content = "details", skip_content_if = "Value::is_empty")]
#[non_exhaustive]
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
	InvalidRole {
		/// Name of the invalid role.
		name: String,
	},
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

impl From<AuthError> for Option<NotAllowedError> {
	fn from(auth_error: AuthError) -> Self {
		Some(NotAllowedError::Auth(auth_error))
	}
}

/// Validation failure reason for [`ErrorKind::Validation`] errors.
#[derive(Clone, Debug, PartialEq, Eq, SurrealValue)]
#[surreal(crate = "crate")]
#[surreal(tag = "kind", content = "details", skip_content_if = "Value::is_empty")]
#[non_exhaustive]
pub enum ValidationError {
	/// Parse error (invalid message or request format).
	Parse,
	/// Invalid request structure.
	InvalidRequest,
	/// Invalid parameters.
	InvalidParams,
	/// Namespace is empty.
	NamespaceEmpty,
	/// Database is empty.
	DatabaseEmpty,
	/// Invalid parameter with name.
	InvalidParameter {
		/// Name of the invalid parameter.
		name: String,
	},
	/// Invalid content value.
	InvalidContent {
		/// The invalid value.
		value: String,
	},
	/// Invalid merge value.
	InvalidMerge {
		/// The invalid value.
		value: String,
	},
}

/// Not-allowed reason for [`ErrorKind::NotAllowed`] errors.
#[derive(Clone, Debug, PartialEq, Eq, SurrealValue)]
#[surreal(crate = "crate")]
#[surreal(tag = "kind", content = "details", skip_content_if = "Value::is_empty")]
#[non_exhaustive]
pub enum NotAllowedError {
	/// Scripting not allowed.
	Scripting,
	/// Authentication or authorisation failure.
	Auth(AuthError),
	/// RPC method not allowed.
	Method {
		/// Name of the method.
		name: String,
	},
	/// Function not allowed.
	Function {
		/// Name of the function.
		name: String,
	},
	/// Net target not allowed.
	Target {
		/// Name of the net target.
		name: String,
	},
}

/// Configuration failure reason for [`ErrorKind::Configuration`] errors.
#[derive(Clone, Debug, PartialEq, Eq, SurrealValue)]
#[surreal(crate = "crate")]
#[surreal(tag = "kind", content = "details", skip_content_if = "Value::is_empty")]
#[non_exhaustive]
pub enum ConfigurationError {
	/// Live query not supported.
	LiveQueryNotSupported,
	/// Bad live query config.
	BadLiveQueryConfig,
	/// Bad GraphQL config.
	BadGraphqlConfig,
}

/// Serialisation failure reason for [`ErrorKind::Serialization`] errors.
#[derive(Clone, Debug, PartialEq, Eq, SurrealValue)]
#[surreal(crate = "crate")]
#[surreal(tag = "kind", content = "details", skip_content_if = "Value::is_empty")]
#[non_exhaustive]
pub enum SerializationError {
	/// Serialisation error.
	Serialization,
	/// Deserialisation error.
	Deserialization,
}

/// Not-found reason for [`ErrorKind::NotFound`] errors.
#[derive(Clone, Debug, PartialEq, Eq, SurrealValue)]
#[surreal(crate = "crate")]
#[surreal(tag = "kind", content = "details", skip_content_if = "Value::is_empty")]
#[non_exhaustive]
pub enum NotFoundError {
	/// RPC method not found.
	Method {
		/// Name of the method.
		name: String,
	},
	/// Session not found.
	Session {
		/// Optional session ID that was not found.
		id: Option<String>,
	},
	/// Table not found.
	Table {
		/// Name of the table.
		name: String,
	},
	/// Record not found.
	Record {
		/// ID of the record.
		id: String,
	},
	/// Namespace not found.
	Namespace {
		/// Name of the namespace.
		name: String,
	},
	/// Database not found.
	Database {
		/// Name of the database.
		name: String,
	},
	/// Transaction not found.
	Transaction,
}

/// Query failure reason for [`ErrorKind::Query`] errors.
#[derive(Clone, Debug, PartialEq, Eq, SurrealValue)]
#[surreal(crate = "crate")]
#[surreal(tag = "kind", content = "details", skip_content_if = "Value::is_empty")]
#[non_exhaustive]
pub enum QueryError {
	/// Query was not executed.
	NotExecuted,
	/// Query timed out.
	TimedOut {
		/// Duration after which the query timed out.
		duration: Duration,
	},
	/// Query was cancelled.
	Cancelled,
}

/// Already-exists reason for [`ErrorKind::AlreadyExists`] errors.
#[derive(Clone, Debug, PartialEq, Eq, SurrealValue)]
#[surreal(crate = "crate")]
#[surreal(tag = "kind", content = "details", skip_content_if = "Value::is_empty")]
#[non_exhaustive]
pub enum AlreadyExistsError {
	/// Session already exists.
	Session {
		/// Optional session ID that already exists.
		id: String,
	},
	/// Table already exists.
	Table {
		/// Name of the table.
		name: String,
	},
	/// Record already exists.
	Record {
		/// ID of the record.
		id: String,
	},
	/// Namespace already exists.
	Namespace {
		/// Name of the namespace.
		name: String,
	},
	/// Database already exists.
	Database {
		/// Name of the database.
		name: String,
	},
}

/// Connection failure reason for [`ErrorKind::Connection`] errors.
/// Used in the SDK for client-side connection state errors.
#[derive(Clone, Debug, PartialEq, Eq, SurrealValue)]
#[surreal(crate = "crate")]
#[surreal(tag = "kind", content = "details", skip_content_if = "Value::is_empty")]
#[non_exhaustive]
pub enum ConnectionError {
	/// Connection was used before being initialised.
	Uninitialised,
	/// Connect was called on an instance that is already connected.
	AlreadyConnected,
}

impl fmt::Display for Error {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{}", self.message)
	}
}

impl std::error::Error for Error {}

impl SurrealValue for Error {
	fn kind_of() -> Kind {
		Kind::Object
	}

	fn is_value(value: &Value) -> bool {
		matches!(value, Value::Object(_))
	}

	fn into_value(self) -> Value {
		let mut obj = Object::new();
		obj.insert("code", Value::from(self.code));
		obj.insert("kind", self.kind.into_value());
		obj.insert("message", Value::from(self.message));
		if let Some(details) = self.details {
			obj.insert("details", details.into_value());
		}
		Value::Object(obj)
	}

	fn from_value(value: Value) -> Result<Self, Error> {
		let Value::Object(mut map) = value else {
			return Err(Error::internal("Expected object for Error".to_string()));
		};
		let code = map
			.remove("code")
			.map(i64::from_value)
			.transpose()
			.ok()
			.flatten()
			.unwrap_or(default_code());
		let kind = map
			.remove("kind")
			.map(ErrorKind::from_value)
			.transpose()
			.ok()
			.flatten()
			.unwrap_or_default();
		let message = map
			.remove("message")
			.map(String::from_value)
			.transpose()
			.ok()
			.flatten()
			.unwrap_or_default();
		let details = map
			.remove("details")
			.and_then(|v| ErrorDetails::from_value_with_kind(&kind, v).ok());
		Ok(Self {
			code,
			kind,
			message,
			details,
		})
	}
}

// -----------------------------------------------------------------------------
// Type conversion errors (internal to the types layer)
// -----------------------------------------------------------------------------

/// Errors that can occur when working with SurrealDB types
#[derive(Debug, Clone)]
#[non_exhaustive]
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
#[non_exhaustive]
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
#[non_exhaustive]
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
#[non_exhaustive]
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
			TypeError::Conversion(e) => write!(f, "{e}"),
			TypeError::OutOfRange(e) => write!(f, "{e}"),
			TypeError::LengthMismatch(e) => write!(f, "{e}"),
			TypeError::Invalid(e) => write!(f, "Invalid: {e}"),
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
