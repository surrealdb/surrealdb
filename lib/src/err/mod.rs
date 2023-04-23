use crate::sql::idiom::Idiom;
use crate::sql::value::Value;
use bung::encode::Error as SerdeError;
use serde::Serialize;
use std::borrow::Cow;
use storekey::decode::Error as DecodeError;
use storekey::encode::Error as EncodeError;
use thiserror::Error;

/// An error originating from an embedded SurrealDB database.
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum Error {
	/// This error is used for ignoring a document when processing a query
	#[doc(hidden)]
	#[error("Conditional clause is not truthy")]
	Ignore,

	/// There was a problem with the underlying datastore
	#[error("There was a problem with the underlying datastore: {0}")]
	Ds(String),

	/// There was a problem with a datastore transaction
	#[error("There was a problem with a datastore transaction: {0}")]
	Tx(String),

	/// There was an error when starting a new datastore transaction
	#[error("There was an error when starting a new datastore transaction")]
	TxFailure,

	/// The transaction was already cancelled or committed
	#[error("Couldn't update a finished transaction")]
	TxFinished,

	/// The current transaction was created as read-only
	#[error("Couldn't write to a read only transaction")]
	TxReadonly,

	/// The conditional value in the request was not equal
	#[error("Value being checked was not correct")]
	TxConditionNotMet,

	/// The key being inserted in the transaction already exists
	#[error("The key being inserted already exists")]
	TxKeyAlreadyExists,

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
	#[error("The SQL query was not parsed fully")]
	QueryRemaining,

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

	/// Remote HTTP request functions are not enabled
	#[error("Remote HTTP request functions are not enabled")]
	HttpDisabled,

	/// it is not possible to set a variable with the specified name
	#[error("Found '{name}' but it is not possible to set a variable with this name")]
	InvalidParam {
		name: String,
	},

	#[error("Found '{field}' in SELECT clause on line {line}, but field is not an aggregate function, and is not present in GROUP BY expression")]
	InvalidField {
		line: usize,
		field: String,
	},

	#[error("Found '{field}' in SPLIT ON clause on line {line}, but field is not present in SELECT expression")]
	InvalidSplit {
		line: usize,
		field: String,
	},

	#[error("Found '{field}' in ORDER BY clause on line {line}, but field is not present in SELECT expression")]
	InvalidOrder {
		line: usize,
		field: String,
	},

	#[error("Found '{field}' in GROUP BY clause on line {line}, but field is not present in SELECT expression")]
	InvalidGroup {
		line: usize,
		field: String,
	},

	/// The LIMIT clause must evaluate to a positive integer
	#[error("Found {value} but the LIMIT clause must evaluate to a positive integer")]
	InvalidLimit {
		value: String,
	},

	/// The START clause must evaluate to a positive integer
	#[error("Found {value} but the START clause must evaluate to a positive integer")]
	InvalidStart {
		value: String,
	},

	/// There was an error with the provided JavaScript code
	#[error("Problem with embedded script function. {message}")]
	InvalidScript {
		message: String,
	},

	/// There was a problem running the specified function
	#[error("There was a problem running the {name}() function. {message}")]
	InvalidFunction {
		name: String,
		message: String,
	},

	/// The wrong quantity or magnitude of arguments was given for the specified function
	#[error("Incorrect arguments for function {name}(). {message}")]
	InvalidArguments {
		name: String,
		message: String,
	},

	/// The query timedout
	#[error("The query was not executed because it exceeded the timeout")]
	QueryTimedout,

	/// The query did not execute, because the transaction was cancelled
	#[error("The query was not executed due to a cancelled transaction")]
	QueryCancelled,

	/// The query did not execute, because the transaction has failed
	#[error("The query was not executed due to a failed transaction")]
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
	#[error("The namespace '{value}' does not exist")]
	NsNotFound {
		value: String,
	},

	/// The requested namespace token does not exist
	#[error("The namespace token '{value}' does not exist")]
	NtNotFound {
		value: String,
	},

	/// The requested namespace login does not exist
	#[error("The namespace login '{value}' does not exist")]
	NlNotFound {
		value: String,
	},

	/// The requested database does not exist
	#[error("The database '{value}' does not exist")]
	DbNotFound {
		value: String,
	},

	/// The requested database token does not exist
	#[error("The database token '{value}' does not exist")]
	DtNotFound {
		value: String,
	},

	/// The requested database login does not exist
	#[error("The database login '{value}' does not exist")]
	DlNotFound {
		value: String,
	},

	/// The requested function does not exist
	#[error("The function 'fn::{value}' does not exist")]
	FcNotFound {
		value: String,
	},

	/// The requested scope does not exist
	#[error("The scope '{value}' does not exist")]
	ScNotFound {
		value: String,
	},

	/// The requested scope token does not exist
	#[error("The scope token '{value}' does not exist")]
	StNotFound {
		value: String,
	},

	/// The requested param does not exist
	#[error("The param '${value}' does not exist")]
	PaNotFound {
		value: String,
	},

	/// The requested table does not exist
	#[error("The table '{value}' does not exist")]
	TbNotFound {
		value: String,
	},

	/// Unable to perform the realtime query
	#[error("Unable to perform the realtime query")]
	RealtimeDisabled,

	/// Reached excessive computation depth due to functions, subqueries, or futures
	#[error("Reached excessive computation depth due to functions, subqueries, or futures")]
	ComputationDepthExceeded,

	/// Can not execute CREATE query using the specified value
	#[error("Can not execute CREATE query using value '{value}'")]
	CreateStatement {
		value: String,
	},

	/// Can not execute UPDATE query using the specified value
	#[error("Can not execute UPDATE query using value '{value}'")]
	UpdateStatement {
		value: String,
	},

	/// Can not execute RELATE query using the specified value
	#[error("Can not execute RELATE query using value '{value}'")]
	RelateStatement {
		value: String,
	},

	/// Can not execute DELETE query using the specified value
	#[error("Can not execute DELETE query using value '{value}'")]
	DeleteStatement {
		value: String,
	},

	/// Can not execute INSERT query using the specified value
	#[error("Can not execute INSERT query using value '{value}'")]
	InsertStatement {
		value: String,
	},

	/// Can not execute LIVE query using the specified value
	#[error("Can not execute LIVE query using value '{value}'")]
	LiveStatement {
		value: String,
	},

	/// Can not execute KILL query using the specified id
	#[error("Can not execute KILL query using id '{value}'")]
	KillStatement {
		value: String,
	},

	/// The permissions do not allow this query to be run on this table
	#[error("You don't have permission to run this query on the `{table}` table")]
	TablePermissions {
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
		thing: String,
	},

	/// A database index entry for the specified record already exists
	#[error("Database index `{index}` already contains {value}, with record `{thing}`")]
	IndexExists {
		thing: String,
		index: String,
		value: String,
	},

	/// The specified field did not conform to the field type check
	#[error("Found {value} for field `{field}`, with record `{thing}`, but expected a {check}")]
	FieldCheck {
		thing: String,
		value: String,
		field: Idiom,
		check: String,
	},

	/// The specified field did not conform to the field ASSERT clause
	#[error("Found {value} for field `{field}`, with record `{thing}`, but field must conform to: {check}")]
	FieldValue {
		thing: String,
		value: String,
		field: Idiom,
		check: String,
	},

	/// Found a record id for the record but this is not a valid id
	#[error("Found '{value}' for the record ID but this is not a valid id")]
	IdInvalid {
		value: String,
	},

	/// The requested function does not exist
	#[error("Expected a {into} but failed to convert {from} into a {into}")]
	ConvertTo {
		from: Value,
		into: Cow<'static, str>,
	},

	/// The requested function does not exist
	#[error("Cannot perform addition with '{0}' and '{1}'")]
	TryAdd(String, String),

	/// The requested function does not exist
	#[error("Cannot perform subtraction with '{0}' and '{1}'")]
	TrySub(String, String),

	/// The requested function does not exist
	#[error("Cannot perform multiplication with '{0}' and '{1}'")]
	TryMul(String, String),

	/// The requested function does not exist
	#[error("Cannot perform division with '{0}' and '{1}'")]
	TryDiv(String, String),

	/// The requested function does not exist
	#[error("Cannot raise the value '{0}' with '{1}'")]
	TryPow(String, String),

	/// It's is not possible to convert between the two types
	#[error("Cannot convert from '{0}' to '{1}'")]
	TryFrom(String, &'static str),

	/// There was an error processing a remote HTTP request
	#[error("There was an error processing a remote HTTP request")]
	Http(String),

	/// There was an error processing a value in parallel
	#[error("There was an error processing a value in parallel")]
	Channel(String),

	/// Represents an underlying error with Serde encoding / decoding
	#[error("Serde error: {0}")]
	Serde(#[from] SerdeError),

	/// Represents an error when encoding a key-value entry
	#[error("Key encoding error: {0}")]
	Encode(#[from] EncodeError),

	/// Represents an error when decoding a key-value entry
	#[error("Key decoding error: {0}")]
	Decode(#[from] DecodeError),
}

impl From<Error> for String {
	fn from(e: Error) -> String {
		e.to_string()
	}
}

#[cfg(feature = "kv-mem")]
impl From<echodb::err::Error> for Error {
	fn from(e: echodb::err::Error) -> Error {
		match e {
			echodb::err::Error::KeyAlreadyExists => Error::TxKeyAlreadyExists,
			_ => Error::Tx(e.to_string()),
		}
	}
}

#[cfg(feature = "kv-indxdb")]
impl From<indxdb::err::Error> for Error {
	fn from(e: indxdb::err::Error) -> Error {
		match e {
			indxdb::err::Error::KeyAlreadyExists => Error::TxKeyAlreadyExists,
			_ => Error::Tx(e.to_string()),
		}
	}
}

#[cfg(feature = "kv-tikv")]
impl From<tikv::Error> for Error {
	fn from(e: tikv::Error) -> Error {
		match e {
			tikv::Error::DuplicateKeyInsertion => Error::TxKeyAlreadyExists,
			_ => Error::Tx(e.to_string()),
		}
	}
}

#[cfg(feature = "kv-rocksdb")]
impl From<rocksdb::Error> for Error {
	fn from(e: rocksdb::Error) -> Error {
		Error::Tx(e.to_string())
	}
}

impl From<channel::RecvError> for Error {
	fn from(e: channel::RecvError) -> Error {
		Error::Channel(e.to_string())
	}
}

impl<T> From<channel::SendError<T>> for Error {
	fn from(e: channel::SendError<T>) -> Error {
		Error::Channel(e.to_string())
	}
}

#[cfg(feature = "http")]
impl From<reqwest::Error> for Error {
	fn from(e: reqwest::Error) -> Error {
		Error::Http(e.to_string())
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
