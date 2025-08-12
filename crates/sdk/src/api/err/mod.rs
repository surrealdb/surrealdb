use std::io;
use std::path::PathBuf;

use serde::Serialize;
use thiserror::Error;

use crate::Value;
use crate::api::Response;
use crate::core::dbs::capabilities::{ParseFuncTargetError, ParseNetTargetError};

/// An error originating from a remote SurrealDB database
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum Error {
	/// There was an error processing the query
	#[error("{0}")]
	Query(String),

	/// There was an error processing a remote HTTP request
	#[error("There was an error processing a remote HTTP request: {0}")]
	Http(String),

	/// There was an error processing a remote WS request
	#[error("There was an error processing a remote WS request: {0}")]
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
	#[error("Invalid bindings: {0}")]
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

	/// Tried to use a range query on an unspecified resource
	#[error("Tried to add a range to an unspecified resource")]
	RangeOnUnspecified,

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
	#[error("Failed to convert `{value}` to `T`: {error}")]
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
	#[error("Failed to serialize `{value}` to JSON string: {error}")]
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
	LossyTake(Response),

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

	/// Tried to use a range query on an unspecified resource
	#[error("Live queries on unspecified resource not supported")]
	LiveOnUnspecified,

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

	/// Tried to insert on an unspecified resource with no data
	#[error("Insert queries on unspecified resource with no data are not supported")]
	InsertOnUnspecified,

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
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		serializer.serialize_str(self.to_string().as_str())
	}
}
