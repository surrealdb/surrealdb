use crate::api::Response;
use crate::sql::Array;
use crate::sql::Edges;
use crate::sql::Object;
use crate::sql::Thing;
use crate::sql::Value;
use std::io;
use std::path::PathBuf;
use thiserror::Error;

/// An error originating from a remote SurrealDB database.
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum Error {
	/// There was an error processing the query
	#[error("{0}")]
	Query(String),

	/// There was an error processing a remote HTTP request
	#[error("There was an error processing a remote HTTP request")]
	Http(String),

	/// There was an error processing a remote WS request
	#[error("There was an error processing a remote WS request")]
	Ws(String),

	/// The specified scheme does not match any supported protocol or storage engine
	#[error("Unsupported protocol or storage engine, `{0}`")]
	Scheme(String),

	/// Tried to run database queries without initialising the connection first
	#[error("Connection uninitialised")]
	ConnectionUninitialised,

	/// `Query::bind` not called with an object nor a key/value tuple
	#[error("Invalid bindings: {0}")]
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
	#[error("Failed to convert `{value}` to `T`: {error}")]
	FromValue {
		value: Value,
		error: String,
	},

	/// Failed to deserialize a binary response
	#[error("Failed to deserialize a binary response: {error}")]
	ResponseFromBinary {
		binary: Vec<u8>,
		error: bung::decode::Error,
	},

	/// Failed to serialize `sql::Value` to JSON string
	#[error("Failed to serialize `{value}` to JSON string: {error}")]
	ToJsonString {
		value: Value,
		error: String,
	},

	/// Failed to serialize `sql::Value` to JSON string
	#[error("Failed to serialize `{string}` to JSON string: {error}")]
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

	/// The protocol or storage engine being used does not support authentication on the
	/// architecture it's running on
	#[error("The protocol or storage engine does not support authentication on this architecture")]
	AuthNotSupported,
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
