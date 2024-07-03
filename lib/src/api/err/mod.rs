use crate::api::Response;
use crate::sql::Array;
use crate::sql::Edges;
use crate::sql::FromValueError;
use crate::sql::Object;
use crate::sql::Thing;
use crate::Value;
use serde::Serialize;
use std::io;
use std::path::PathBuf;
use surrealdb_core::dbs::capabilities::{ParseFuncTargetError, ParseNetTargetError};
use thiserror::Error;

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

	/// The specified scheme does not match any supported protocol or storage engine
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
	#[error("Range on record IDs not supported: {0}")]
	RangeOnRecordId(Thing),

	/// Tried to use a range query on an object
	#[error("Range on objects not supported: {0}")]
	RangeOnObject(Object),

	/// Tried to use a range query on an array
	#[error("Range on arrays not supported: {0}")]
	RangeOnArray(Array),

	/// Tried to use a range query on an edge or edges
	#[error("Range on edges not supported: {0}")]
	RangeOnEdges(Edges),

	/// Tried to use `table:id` syntax as a method parameter when `(table, id)` should be used instead
	#[error("`{table}:{id}` is not allowed as a method parameter; try `({table}, {id})`")]
	TableColonId {
		table: String,
		id: String,
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

	/// Tried to take only a single result when the query returned multiple records
	#[error("Tried to take only a single result from a query that contains multiple")]
	LossyTake(Response),

	/// The protocol or storage engine being used does not support backups on the architecture
	/// it's running on
	#[error("The protocol or storage engine does not support backups on this architecture")]
	BackupsNotSupported,

	/// The version of the server is not compatible with the versions supported by this SDK
	#[error("server version `{server_version}` does not match the range supported by the client `{supported_versions}`")]
	VersionMismatch {
		server_version: semver::Version,
		supported_versions: String,
	},

	/// The build metadata of the server is older than the minimum supported by this SDK
	#[error("server build `{server_metadata}` is older than the minimum supported build `{supported_metadata}`")]
	BuildMetadataMismatch {
		server_metadata: semver::BuildMetadata,
		supported_metadata: semver::BuildMetadata,
	},

	/// The protocol or storage engine being used does not support live queries on the architecture
	/// it's running on
	#[error("The protocol or storage engine does not support live queries on this architecture")]
	LiveQueriesNotSupported,

	/// Tried to use a range query on an object
	#[error("Live queries on objects not supported: {0}")]
	LiveOnObject(Object),

	/// Tried to use a range query on an array
	#[error("Live queries on arrays not supported: {0}")]
	LiveOnArray(Array),

	/// Tried to use a range query on an edge or edges
	#[error("Live queries on edges not supported: {0}")]
	LiveOnEdges(Edges),

	/// Tried to access a query statement as a live query when it isn't a live query
	#[error("Query statement {0} is not a live query")]
	NotLiveQuery(usize),

	/// Tried to access a query statement falling outside the bounds of the statements supplied
	#[error("Query statement {0} is out of bounds")]
	QueryIndexOutOfBounds(usize),

	/// Called `Response::take` or `Response::stream` on a query response more than once
	#[error("Tried to take a query response that has already been taken")]
	ResponseAlreadyTaken,

	/// Tried to insert on an object
	#[error("Insert queries on objects not supported: {0}")]
	InsertOnObject(Object),

	/// Tried to insert on an array
	#[error("Insert queries on arrays not supported: {0}")]
	InsertOnArray(Array),

	/// Tried to insert on an edge or edges
	#[error("Insert queries on edges not supported: {0}")]
	InsertOnEdges(Edges),

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
	#[error("recieved an invalid value")]
	RecievedInvalidValue,
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

impl From<ParseNetTargetError> for crate::Error {
	fn from(e: ParseNetTargetError) -> Self {
		Self::Api(Error::from(e))
	}
}

impl From<ParseFuncTargetError> for crate::Error {
	fn from(e: ParseFuncTargetError) -> Self {
		Self::Api(Error::from(e))
	}
}

#[cfg(feature = "protocol-http")]
impl From<reqwest::Error> for crate::Error {
	fn from(e: reqwest::Error) -> Self {
		Self::Api(Error::Http(e.to_string()))
	}
}

#[cfg(all(feature = "protocol-ws", not(target_arch = "wasm32")))]
#[cfg_attr(docsrs, doc(cfg(all(feature = "protocol-ws", not(target_arch = "wasm32")))))]
impl From<tokio_tungstenite::tungstenite::Error> for crate::Error {
	fn from(error: tokio_tungstenite::tungstenite::Error) -> Self {
		Self::Api(Error::Ws(error.to_string()))
	}
}

impl<T> From<flume::SendError<T>> for crate::Error {
	fn from(error: flume::SendError<T>) -> Self {
		Self::Api(Error::InternalError(error.to_string()))
	}
}

impl From<flume::RecvError> for crate::Error {
	fn from(error: flume::RecvError) -> Self {
		Self::Api(Error::InternalError(error.to_string()))
	}
}

impl From<url::ParseError> for crate::Error {
	fn from(error: url::ParseError) -> Self {
		Self::Api(Error::InternalError(error.to_string()))
	}
}

#[cfg(all(feature = "protocol-ws", target_arch = "wasm32"))]
#[cfg_attr(docsrs, doc(cfg(all(feature = "protocol-ws", target_arch = "wasm32"))))]
impl From<ws_stream_wasm::WsErr> for crate::Error {
	fn from(error: ws_stream_wasm::WsErr) -> Self {
		Self::Api(Error::Ws(error.to_string()))
	}
}

#[cfg(all(feature = "protocol-ws", target_arch = "wasm32"))]
#[cfg_attr(docsrs, doc(cfg(all(feature = "protocol-ws", target_arch = "wasm32"))))]
impl From<pharos::PharErr> for crate::Error {
	fn from(error: pharos::PharErr) -> Self {
		Self::Api(Error::Ws(error.to_string()))
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

impl From<FromValueError> for crate::Error {
	fn from(error: FromValueError) -> Self {
		Self::Api(Error::FromValue {
			value: error.value,
			error: error.error,
		})
	}
}
