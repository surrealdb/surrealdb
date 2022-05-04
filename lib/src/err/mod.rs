use crate::sql::idiom::Idiom;
use crate::sql::thing::Thing;
use crate::sql::value::Value;
use msgpack::encode::Error as SerdeError;
use serde::Serialize;
use std::time::Duration;
use storekey::decode::Error as DecodeError;
use storekey::encode::Error as EncodeError;
use thiserror::Error;

#[cfg(feature = "kv-tikv")]
use tikv::Error as TiKVError;

#[cfg(feature = "kv-echodb")]
use echodb::err::Error as EchoDBError;

#[cfg(feature = "kv-indxdb")]
use indxdb::err::Error as IndxDBError;

#[cfg(feature = "parallel")]
use tokio::sync::mpsc::error::SendError as TokioError;

/// An error originating from the SurrealDB client library.
#[derive(Error, Debug)]
pub enum Error {
	/// This error is used for ignoring a document when processing a query
	#[doc(hidden)]
	#[error("Conditional clause is not truthy")]
	Ignore,

	/// There was an error when connecting to the underlying datastore
	#[error("Couldn't setup connection to underlying datastore")]
	Ds,

	/// There was an error when starting a new transaction
	#[error("Couldn't create a database transaction")]
	Tx,

	/// The transaction was already cancelled or committed
	#[error("Couldn't update a finished transaction")]
	TxFinished,

	/// The current transaction was created as read-only
	#[error("Couldn't write to a read only transaction")]
	TxReadonly,

	/// The conditional value in the request was not equal
	#[error("Value being checked was not correct")]
	TxConditionNotMet,

	/// No namespace has been selected
	#[error("Specify a namespace to use")]
	NsEmpty,

	/// No database has been selected
	#[error("Specify a database to use")]
	DbEmpty,

	/// No SQL query has been specified
	#[error("Specify some SQL code to execute")]
	QueryEmpty,

	/// There was an error with the SQL query
	#[error("Parse error on line {line} at character {char} when parsing '{sql}'")]
	InvalidQuery {
		line: usize,
		char: usize,
		sql: String,
	},

	/// There was an error with the provided JSON Patch
	#[error("The JSON Patch contains invalid operations. {message}")]
	InvalidPatch {
		message: String,
	},

	/// There was an error with the provided JavaScript code
	#[error("Problem with embedded script function. {message}")]
	InvalidScript {
		message: String,
	},

	/// The wrong number of arguments was given for the specified function
	#[error("Incorrect arguments for function {name}(). {message}")]
	InvalidArguments {
		name: String,
		message: String,
	},

	/// The query timedout
	#[error("Query timeout of {timer:?} exceeded")]
	QueryTimeout {
		timer: Duration,
	},

	/// The query did not execute, because the transaction was cancelled
	#[error("Query not executed due to cancelled transaction")]
	QueryCancelled,

	/// The query did not execute, because the transaction has failed
	#[error("Query not executed due to failed transaction")]
	QueryNotExecuted,

	/// The permissions do not allow for performing the specified query
	#[error("You don't have permission to perform this query type")]
	QueryPermissions,

	/// The permissions do not allow for changing to the specified namespace
	#[error("You don't have permission to change to the {ns} namespace")]
	NsNotAllowed {
		ns: String,
	},

	/// The permissions do not allow for changing to the specified database
	#[error("You don't have permission to change to the {db} database")]
	DbNotAllowed {
		db: String,
	},

	/// The requested namespace does not exist
	#[error("The namespace does not exist")]
	NsNotFound,

	/// The requested namespace token does not exist
	#[error("The namespace token does not exist")]
	NtNotFound,

	/// The requested namespace login does not exist
	#[error("The namespace login does not exist")]
	NlNotFound,

	/// The requested database does not exist
	#[error("The database does not exist")]
	DbNotFound,

	/// The requested database token does not exist
	#[error("The database token does not exist")]
	DtNotFound,

	/// The requested database login does not exist
	#[error("The database login does not exist")]
	DlNotFound,

	/// The requested scope does not exist
	#[error("The scope does not exist")]
	ScNotFound,

	/// The requested scope token does not exist
	#[error("The scope token does not exist")]
	StNotFound,

	/// The requested table does not exist
	#[error("The table does not exist")]
	TbNotFound,

	/// Too many recursive subqueries have been processed
	#[error("Too many recursive subqueries have been processed")]
	TooManySubqueries {
		limit: usize,
	},

	/// Can not execute CREATE query using the specified value
	#[error("Can not execute CREATE query using value '{value}'")]
	CreateStatement {
		value: Value,
	},

	/// Can not execute UPDATE query using the specified value
	#[error("Can not execute UPDATE query using value '{value}'")]
	UpdateStatement {
		value: Value,
	},

	/// Can not execute RELATE query using the specified value
	#[error("Can not execute RELATE query using value '{value}'")]
	RelateStatement {
		value: Value,
	},

	/// Can not execute DELETE query using the specified value
	#[error("Can not execute DELETE query using value '{value}'")]
	DeleteStatement {
		value: Value,
	},

	/// Can not execute INSERT query using the specified value
	#[error("Can not execute INSERT query using value '{value}'")]
	InsertStatement {
		value: Value,
	},

	/// The permissions do not allow this query to be run on this table
	#[error("You don't have permission to run the `{query}` query on the `{table}` table")]
	TablePermissions {
		query: String,
		table: String,
	},

	/// The specified table can not be written as it is setup as a foreign table view
	#[error("Unable to write to the `{table}` table while setup as a view")]
	TableIsView {
		table: String,
	},

	/// A database entry for the specified record already exists
	#[error("Database record `{thing}` already exists")]
	RecordExists {
		thing: Thing,
	},

	/// A database index entry for the specified record already exists
	#[error("Database index `{index}` already contains `{thing}`")]
	IndexExists {
		index: String,
		thing: Thing,
	},

	/// The specified field did not conform to the field ASSERT clause
	#[error("Found '{value}' for field '{field}' but field must conform to: {check}")]
	FieldValue {
		value: Value,
		field: Idiom,
		check: Value,
	},

	/// Represents an underlying error with Serde encoding / decoding
	#[error("Serde error: {0}")]
	Serde(#[from] SerdeError),

	/// Represents an error when encoding a key-value entry
	#[error("Key encoding error: {0}")]
	Encode(#[from] EncodeError),

	/// Represents an error when decoding a key-value entry
	#[error("Key decoding error: {0}")]
	Decode(#[from] DecodeError),

	/// Represents an underlying error from the EchoDB instance
	#[cfg(feature = "kv-echodb")]
	#[error("Datastore error: {0}")]
	EchoDB(#[from] EchoDBError),

	/// Represents an underlying error from the IndxDB instance
	#[cfg(feature = "kv-indxdb")]
	#[error("Datastore error: {0}")]
	IndxDB(#[from] IndxDBError),

	/// Represents an underlying error from the TiKV instance
	#[cfg(feature = "kv-tikv")]
	#[error("Datastore error: {0}")]
	TiKV(#[from] TiKVError),

	/// Represents an underlying error with Tokio message channels
	#[cfg(feature = "parallel")]
	#[error("Tokio Error: {0}")]
	Tokio(#[from] TokioError<(Option<Thing>, Value)>),
}

impl Serialize for Error {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		serializer.serialize_str(self.to_string().as_str())
	}
}
