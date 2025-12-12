use std::io;
use std::path::PathBuf;

use serde::Serialize;
use surrealdb_core::dbs::capabilities::{ParseFuncTargetError, ParseNetTargetError};
use surrealdb_core::rpc::DbResultError;
use surrealdb_types::Value;
use thiserror::Error;

use crate::IndexedResults;

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

	/// `Query::bind` not called with an object nor a key/value tuple
	#[error("Invalid bindings: {0:?}")]
	InvalidBindings(Value),

	/// Tried to use a range query on a record ID
	#[error("Tried to add a range to an record-id resource")]
	RangeOnRecordId,

	/// Tried to use a range query on an object
	#[error("Tried to add a range to an object resource")]
	RangeOnObject,

	/// Tried to use a range query on an array
	#[error("Tried to add a range to an array resource")]
	RangeOnArray,

	/// Tried to use a range query on an edge or edges
	#[error("Tried to add a range to an edge resource")]
	RangeOnEdges,

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

	/// Failed to deserialize a binary response
	#[error("Failed to deserialize a binary response: {error}")]
	ResponseFromBinary {
		binary: Vec<u8>,
		error: bincode::Error,
	},

	/// Failed to serialize `sql::Value` to JSON string
	#[error("Failed to serialize `{value:?}` to JSON string: {error}")]
	ToJsonString {
		value: Value,
		error: String,
	},

	/// Failed to deserialize from JSON string to `sql::Value`
	#[error("Failed to deserialize `{string}` to sql::Value: {error}")]
	FromJsonString {
		string: String,
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

	/// Tried to use a range query on an edge or edges
	#[error("Live queries on edges not supported")]
	LiveOnEdges,

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
	#[error("Insert queries on edges are not supported")]
	InsertOnEdges,

	/// Tried to insert on an edge or edges
	#[error("Insert queries on ranges are not supported")]
	InsertOnRange,

	#[error("Crendentials for signin and signup should be an object")]
	CrendentialsNotObject,

	#[error("{0}")]
	InvalidNetTarget(#[from] ParseNetTargetError),

	#[error("{0}")]
	InvalidFuncTarget(#[from] ParseFuncTargetError),

	#[error("failed to serialize Value: {0}")]
	SerializeValue(String),
	#[error("failed to deserialize Value: {0}")]
	DeSerializeValue(String),

	#[error("failed to serialize to a Value: {0}")]
	Serializer(String),
	#[error("failed to deserialize from a Value: {0}")]
	Deserializer(String),

	#[error("The server returned an unexpected response: {0}")]
	InvalidResponse(String),

	#[error("Tried to send a value which could not be serialized: {0}")]
	UnserializableValue(String),

	/// Tried to convert an value which contained something like for example a
	/// query or future.
	#[error(
		"tried to convert from a value which contained non-primitive values to a value which only allows primitive values."
	)]
	ReceivedInvalidValue,

	/// The engine used does not support data versioning
	#[error("The '{0}' engine does not support data versioning")]
	VersionsNotSupported(String),

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

// There is a 1:1 mapping between DbResultError and Error
impl From<DbResultError> for Error {
	fn from(err: DbResultError) -> Self {
		match err {
			DbResultError::ParseError(message) => Error::ParseError(message),
			DbResultError::InvalidRequest(message) => Error::InvalidRequest(message),
			DbResultError::MethodNotFound(message) => Error::MethodNotFound(message),
			DbResultError::MethodNotAllowed(message) => Error::MethodNotAllowed(message),
			DbResultError::InvalidParams(message) => Error::InvalidParams(message),
			DbResultError::LiveQueryNotSupported => Error::LiveQueriesNotSupported,
			DbResultError::BadLiveQueryConfig(message) => Error::BadLiveQueryConfig(message),
			DbResultError::BadGraphQLConfig(message) => Error::BadGraphQLConfig(message),
			DbResultError::InternalError(message) => Error::InternalError(message),
			DbResultError::Thrown(message) => Error::Thrown(message),
			DbResultError::SerializationError(message) => Error::SerializeValue(message),
			DbResultError::DeserializationError(message) => Error::DeSerializeValue(message),
			DbResultError::ClientSideError(message) => Error::Query(message),
			DbResultError::InvalidAuth(message) => Error::Query(message),
			DbResultError::QueryNotExecuted(message) => Error::Query(message),
			DbResultError::QueryTimedout(message) => Error::Query(message),
			DbResultError::QueryCancelled => Error::Query(
				"The query was not executed due to a cancelled transaction".to_string(),
			),
		}
	}
}

// Allow conversion from anyhow::Error (from surrealdb_types) to our Error
impl From<surrealdb_types::anyhow::Error> for Error {
	fn from(error: surrealdb_types::anyhow::Error) -> Self {
		Error::InternalError(error.to_string())
	}
}

// Allow conversion from async_channel::RecvError
impl From<async_channel::RecvError> for Error {
	fn from(error: async_channel::RecvError) -> Self {
		Error::InternalError(format!("Channel receive error: {}", error))
	}
}

// Allow conversion from std::io::Error
impl From<std::io::Error> for Error {
	fn from(error: std::io::Error) -> Self {
		Error::InternalError(format!("I/O error: {}", error))
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

// Allow conversion from RpcError from core
impl From<surrealdb_core::rpc::RpcError> for Error {
	fn from(error: surrealdb_core::rpc::RpcError) -> Self {
		// Convert to DbResultError first, then to our Error
		DbResultError::from(error).into()
	}
}

// Allow conversion from SDK Error to DbResultError (for sending errors over the wire)
impl From<Error> for DbResultError {
	fn from(error: Error) -> Self {
		match error {
			Error::Query(msg) => DbResultError::Thrown(msg),
			Error::Http(msg) => DbResultError::InternalError(format!("HTTP error: {}", msg)),
			Error::Ws(msg) => DbResultError::InternalError(format!("WebSocket error: {}", msg)),
			Error::Scheme(msg) => {
				DbResultError::InvalidRequest(format!("Unsupported scheme: {}", msg))
			}
			Error::ConnectionUninitialised => {
				DbResultError::InternalError("Connection uninitialised".to_string())
			}
			Error::AlreadyConnected => {
				DbResultError::InternalError("Already connected".to_string())
			}
			Error::InvalidBindings(_) => {
				DbResultError::InvalidParams("Invalid bindings".to_string())
			}
			Error::RangeOnRecordId => {
				DbResultError::InvalidParams("Range on record ID not supported".to_string())
			}
			Error::RangeOnObject => {
				DbResultError::InvalidParams("Range on object not supported".to_string())
			}
			Error::RangeOnArray => {
				DbResultError::InvalidParams("Range on array not supported".to_string())
			}
			Error::RangeOnEdges => {
				DbResultError::InvalidParams("Range on edges not supported".to_string())
			}
			Error::RangeOnRange => {
				DbResultError::InvalidParams("Range on range not supported".to_string())
			}
			Error::TableColonId {
				table,
			} => DbResultError::InvalidParams(format!("Table name '{}' contains colon", table)),
			Error::DuplicateRequestId(id) => {
				DbResultError::InternalError(format!("Duplicate request ID: {}", id))
			}
			Error::InvalidRequest(msg) => DbResultError::InvalidRequest(msg),
			Error::InvalidParams(msg) => DbResultError::InvalidParams(msg),
			Error::InternalError(msg) => DbResultError::InternalError(msg),
			Error::ParseError(msg) => DbResultError::ParseError(msg),
			Error::InvalidSemanticVersion(msg) => {
				DbResultError::InvalidParams(format!("Invalid semantic version: {}", msg))
			}
			Error::InvalidUrl(msg) => {
				DbResultError::InvalidRequest(format!("Invalid URL: {}", msg))
			}
			Error::FromValue {
				value: _,
				error,
			} => DbResultError::InvalidParams(format!("Value conversion error: {}", error)),
			Error::ResponseFromBinary {
				error: _,
				..
			} => DbResultError::DeserializationError(
				"Binary response deserialization error".to_string(),
			),
			Error::ToJsonString {
				value: _,
				error,
			} => DbResultError::SerializationError(format!("JSON serialization error: {}", error)),
			Error::FromJsonString {
				string: _,
				error,
			} => DbResultError::DeserializationError(format!(
				"JSON deserialization error: {}",
				error
			)),
			Error::InvalidNsName(name) => {
				DbResultError::InvalidParams(format!("Invalid namespace name: {:?}", name))
			}
			Error::InvalidDbName(name) => {
				DbResultError::InvalidParams(format!("Invalid database name: {:?}", name))
			}
			Error::FileOpen {
				path,
				error,
			} => DbResultError::InternalError(format!("Failed to open file {:?}: {}", path, error)),
			Error::FileRead {
				path,
				error,
			} => DbResultError::InternalError(format!("Failed to read file {:?}: {}", path, error)),
			Error::LossyTake(_) => DbResultError::InvalidParams("Lossy take operation".to_string()),
			Error::BackupsNotSupported => {
				DbResultError::MethodNotAllowed("Backups not supported".to_string())
			}
			Error::VersionMismatch {
				server_version,
				supported_versions,
			} => DbResultError::InvalidRequest(format!(
				"Version mismatch: server {} vs supported {}",
				server_version, supported_versions
			)),
			Error::BuildMetadataMismatch {
				server_metadata,
				supported_metadata,
			} => DbResultError::InvalidRequest(format!(
				"Build metadata mismatch: server {} vs supported {}",
				server_metadata, supported_metadata
			)),
			Error::LiveQueriesNotSupported => DbResultError::LiveQueryNotSupported,
			Error::LiveOnObject => DbResultError::BadLiveQueryConfig(
				"Live queries on objects not supported".to_string(),
			),
			Error::LiveOnArray => DbResultError::BadLiveQueryConfig(
				"Live queries on arrays not supported".to_string(),
			),
			Error::LiveOnEdges => {
				DbResultError::BadLiveQueryConfig("Live queries on edges not supported".to_string())
			}
			Error::NotLiveQuery(idx) => DbResultError::BadLiveQueryConfig(format!(
				"Query statement {} is not a live query",
				idx
			)),
			Error::QueryIndexOutOfBounds(idx) => {
				DbResultError::InvalidParams(format!("Query statement {} is out of bounds", idx))
			}
			Error::ResponseAlreadyTaken => {
				DbResultError::InternalError("Response already taken".to_string())
			}
			Error::InsertOnObject => DbResultError::InvalidParams(
				"Insert queries on objects are not supported".to_string(),
			),
			Error::InsertOnArray => DbResultError::InvalidParams(
				"Insert queries on arrays are not supported".to_string(),
			),
			Error::InsertOnEdges => DbResultError::InvalidParams(
				"Insert queries on edges are not supported".to_string(),
			),
			Error::InsertOnRange => DbResultError::InvalidParams(
				"Insert queries on ranges are not supported".to_string(),
			),
			Error::CrendentialsNotObject => DbResultError::InvalidParams(
				"Credentials for signin and signup should be an object".to_string(),
			),
			Error::InvalidNetTarget(err) => {
				DbResultError::InvalidParams(format!("Invalid network target: {}", err))
			}
			Error::InvalidFuncTarget(err) => {
				DbResultError::InvalidParams(format!("Invalid function target: {}", err))
			}
			Error::SerializeValue(msg) => DbResultError::SerializationError(msg),
			Error::DeSerializeValue(msg) => DbResultError::DeserializationError(msg),
			Error::Serializer(msg) => DbResultError::SerializationError(msg),
			Error::Deserializer(msg) => DbResultError::DeserializationError(msg),
			Error::InvalidResponse(msg) => {
				DbResultError::InternalError(format!("Invalid response: {}", msg))
			}
			Error::UnserializableValue(msg) => DbResultError::SerializationError(msg),
			Error::ReceivedInvalidValue => {
				DbResultError::InvalidParams("Received invalid value".to_string())
			}
			Error::VersionsNotSupported(engine) => DbResultError::MethodNotAllowed(format!(
				"The '{}' engine does not support data versioning",
				engine
			)),
			Error::MethodNotFound(msg) => DbResultError::MethodNotFound(msg),
			Error::MethodNotAllowed(msg) => DbResultError::MethodNotAllowed(msg),
			Error::BadLiveQueryConfig(msg) => DbResultError::BadLiveQueryConfig(msg),
			Error::BadGraphQLConfig(msg) => DbResultError::BadGraphQLConfig(msg),
			Error::Thrown(msg) => DbResultError::Thrown(msg),
			Error::MessageTooLong(len) => {
				DbResultError::InternalError(format!("Message too long: {}", len))
			}
			Error::MaxWriteBufferSizeTooSmall => {
				DbResultError::InternalError("Write buffer size too small".to_string())
			}
			Error::MissingRefreshToken => {
				DbResultError::InvalidParams("Missing refresh token".to_string())
			}
		}
	}
}
