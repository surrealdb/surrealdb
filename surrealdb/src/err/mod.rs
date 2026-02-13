use std::io;
use std::path::PathBuf;

use serde::Serialize;
use thiserror::Error;

use crate::IndexedResults;
use crate::types::Value;

/// A specialized `Result` type
#[allow(dead_code)]
pub type Result<T> = std::result::Result<T, Error>;

/// An error originating from a remote or local SurrealDB database.
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum Error {
	/// There was an error processing the query
	#[error("{0}")]
	Query(String),

	/// There was an error processing an HTTP request
	#[error("There was an error processing an HTTP request: {0}")]
	Http(String),

	/// There was an error processing a WebSocket request
	#[error("There was an error processing a WebSocket request: {0}")]
	Ws(String),

	/// The specified scheme does not match any supported protocol or storage
	/// engine
	#[error("Unsupported protocol or storage engine, `{0}`")]
	Scheme(String),

	/// Tried to run database queries without initialising the connection first
	#[error("Connection uninitialised")]
	ConnectionUninitialised,

	/// Tried to call `connect` on an instance already connected
	#[error("Already connected")]
	AlreadyConnected,

	/// Tried to use a range query on a record ID
	#[error("Tried to add a range to an record-id resource")]
	RangeOnRecordId,

	/// Tried to use a range query on an object
	#[error("Tried to add a range to an object resource")]
	RangeOnObject,

	/// Tried to use a range query on an array
	#[error("Tried to add a range to an array resource")]
	RangeOnArray,

	/// Tried to use a range query on an existing range
	#[error("Tried to add a range to a resource which was already a range")]
	RangeOnRange,

	/// Tried to use `table:id` syntax as a method parameter when `(table, id)`
	/// should be used instead
	#[error(
		"Table name `{table}` contained a colon (:), this is dissallowed to avoid confusion with record-id's try `Table(\"{table}\")` instead."
	)]
	TableColonId {
		table: String,
	},

	/// Duplicate request ID
	#[error("Duplicate request ID: {0}")]
	DuplicateRequestId(i64),

	/// Invalid request
	#[error("Invalid request: {0}")]
	InvalidRequest(String),

	/// Invalid params
	#[error("Invalid params: {0}")]
	InvalidParams(String),

	/// Internal server error
	#[error("Internal error: {0}")]
	InternalError(String),

	/// Parse error
	#[error("Parse error: {0}")]
	ParseError(String),

	/// Invalid semantic version
	#[error("Invalid semantic version: {0}")]
	InvalidSemanticVersion(String),

	/// Invalid URL
	#[error("Invalid URL: {0}")]
	InvalidUrl(String),

	/// Failed to convert a `sql::Value` to `T`
	#[error("Failed to convert `{value:?}` to `T`: {error}")]
	FromValue {
		value: Value,
		error: String,
	},

	/// Invalid namespace name
	#[error("Invalid namespace name: {0:?}")]
	InvalidNsName(String),

	/// Invalid database name
	#[error("Invalid database name: {0:?}")]
	InvalidDbName(String),

	/// File open error
	#[error("Failed to open `{path}`: {error}")]
	FileOpen {
		path: PathBuf,
		error: io::Error,
	},

	/// File read error
	#[error("Failed to read `{path}`: {error}")]
	FileRead {
		path: PathBuf,
		error: io::Error,
	},

	/// Tried to take only a single result when the query returned multiple
	/// records
	#[error("Tried to take only a single result from a query that contains multiple")]
	LossyTake(Box<IndexedResults>),

	/// The protocol or storage engine being used does not support backups on
	/// the architecture it's running on
	#[error("The protocol or storage engine does not support backups on this architecture")]
	BackupsNotSupported,

	/// The version of the server is not compatible with the versions supported
	/// by this SDK
	#[error(
		"server version `{server_version}` does not match the range supported by the client `{supported_versions}`"
	)]
	VersionMismatch {
		server_version: semver::Version,
		supported_versions: String,
	},

	/// The build metadata of the server is older than the minimum supported by
	/// this SDK
	#[error(
		"server build `{server_metadata}` is older than the minimum supported build `{supported_metadata}`"
	)]
	BuildMetadataMismatch {
		server_metadata: semver::BuildMetadata,
		supported_metadata: semver::BuildMetadata,
	},

	/// The protocol or storage engine being used does not support live queries
	/// on the architecture it's running on
	#[error("The protocol or storage engine does not support live queries on this architecture")]
	LiveQueriesNotSupported,

	/// Tried to use a range query on an object
	#[error("Live queries on objects not supported")]
	LiveOnObject,

	/// Tried to use a range query on an array
	#[error("Live queries on arrays not supported")]
	LiveOnArray,

	/// Tried to access a query statement as a live query when it isn't a live
	/// query
	#[error("Query statement {0} is not a live query")]
	NotLiveQuery(usize),

	/// Tried to access a query statement falling outside the bounds of the
	/// statements supplied
	#[error("Query statement {0} is out of bounds")]
	QueryIndexOutOfBounds(usize),

	/// Called `Response::take` or `Response::stream` on a query response more
	/// than once
	#[error("Tried to take a query response that has already been taken")]
	ResponseAlreadyTaken,

	/// Tried to insert on an object
	#[error("Insert queries on objects are not supported")]
	InsertOnObject,

	/// Tried to insert on an array
	#[error("Insert queries on arrays are not supported")]
	InsertOnArray,

	/// Tried to insert on an edge or edges
	#[error("Insert queries on ranges are not supported")]
	InsertOnRange,

	#[error("failed to serialize Value: {0}")]
	SerializeValue(String),
	#[error("failed to deserialize Value: {0}")]
	DeSerializeValue(String),

	#[error("The server returned an unexpected response: {0}")]
	InvalidResponse(String),

	#[error("Tried to send a value which could not be serialized: {0}")]
	UnserializableValue(String),

	/// Method not found
	#[error("Method not found: {0}")]
	MethodNotFound(String),

	/// Method not allowed
	#[error("Method not allowed: {0}")]
	MethodNotAllowed(String),

	/// Bad live query configuration
	#[error("Bad live query configuration: {0}")]
	BadLiveQueryConfig(String),

	/// Bad GraphQL configuration
	#[error("Bad GraphQL configuration: {0}")]
	BadGraphQLConfig(String),

	/// A thrown error from the database
	#[error("Thrown error: {0}")]
	Thrown(String),
	/// The message is too long
	#[error("The message is too long: {0}")]
	MessageTooLong(usize),

	/// The write buffer size is too small
	#[error("The write buffer size is too small")]
	MaxWriteBufferSizeTooSmall,

	/// Tried to refresh a token without a refresh token
	#[error("Missing refresh token")]
	MissingRefreshToken,

	/// Error from the database (wire format: kind, message, details, cause).
	/// Use this variant when the server returned a structured error; it preserves
	/// [`surrealdb_types::Error`] for inspection of `kind`, `message`, `details`, and `cause`.
	#[error("{0}")]
	Database(surrealdb_types::Error),
}

impl serde::ser::Error for Error {
	fn custom<T>(msg: T) -> Self
	where
		T: std::fmt::Display,
	{
		Error::SerializeValue(msg.to_string())
	}
}

impl serde::de::Error for Error {
	fn custom<T>(msg: T) -> Self
	where
		T: std::fmt::Display,
	{
		Error::DeSerializeValue(msg.to_string())
	}
}

impl Serialize for Error {
	fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		serializer.serialize_str(self.to_string().as_str())
	}
}

/// Wire-friendly database errors preserve kind, message, details, and cause.
impl From<surrealdb_types::Error> for Error {
	fn from(err: surrealdb_types::Error) -> Self {
		Error::Database(err)
	}
}

// Allow conversion from anyhow::Error (from crate::types) to our Error
impl From<crate::types::anyhow::Error> for Error {
	fn from(error: crate::types::anyhow::Error) -> Self {
		Error::InternalError(error.to_string())
	}
}

// Allow conversion from async_channel::RecvError
impl From<async_channel::RecvError> for Error {
	fn from(error: async_channel::RecvError) -> Self {
		Error::InternalError(format!("Channel receive error: {error}"))
	}
}

// Allow conversion from std::io::Error
impl From<std::io::Error> for Error {
	fn from(error: std::io::Error) -> Self {
		Error::InternalError(format!("I/O error: {error}"))
	}
}

// Allow conversion from reqwest::Error
#[cfg(feature = "protocol-http")]
impl From<reqwest::Error> for Error {
	fn from(error: reqwest::Error) -> Self {
		Error::Http(error.to_string())
	}
}

// Allow conversion from url::ParseError
impl From<url::ParseError> for Error {
	fn from(error: url::ParseError) -> Self {
		Error::InvalidUrl(error.to_string())
	}
}

// Allow conversion from semver::Error
impl From<semver::Error> for Error {
	fn from(error: semver::Error) -> Self {
		Error::InvalidSemanticVersion(error.to_string())
	}
}

// Convert SDK Error to wire-friendly type (for sending client-side errors on the channel).
impl From<Error> for surrealdb_types::Error {
	fn from(e: Error) -> Self {
		match &e {
			Error::Query(msg) => surrealdb_types::Error::query(msg.clone(), None),
			Error::Http(msg) => surrealdb_types::Error::internal(format!("HTTP error: {msg}")),
			Error::Ws(msg) => surrealdb_types::Error::internal(format!("WebSocket error: {msg}")),
			Error::Scheme(msg) => {
				surrealdb_types::Error::configuration(format!("Unsupported scheme: {msg}"), None)
			}
			Error::ConnectionUninitialised => surrealdb_types::Error::connection(
				"Connection uninitialised".to_string(),
				Some(surrealdb_types::ConnectionError::Uninitialised),
			),
			Error::AlreadyConnected => surrealdb_types::Error::connection(
				"Already connected".to_string(),
				Some(surrealdb_types::ConnectionError::AlreadyConnected),
			),
			Error::Database(inner) => return inner.clone(),
			_ => surrealdb_types::Error::internal(e.to_string()),
		}
	}
}
