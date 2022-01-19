use crate::key::bytes::decode::Error as DecodeError;
use crate::key::bytes::encode::Error as EncodeError;
use crate::sql::thing::Thing;
use crate::sql::value::Value;
use echodb::err::Error as EchoDBError;
use http::Error as HttpError;
use serde_cbor::error::Error as CborError;
use serde_json::error::Error as JsonError;
use std::time::Duration;
use thiserror::Error;
use tikv::Error as TiKVError;

#[derive(Error, Debug)]
pub enum Error {
	#[error("Couldn't setup connection to underlying datastore")]
	DsError,

	#[error("Couldn't create a database transaction")]
	TxError,

	#[error("Couldn't update a finished transaction")]
	TxFinishedError,

	#[error("Couldn't write to a read only transaction")]
	TxReadonlyError,

	#[error("Specify a namespace to use")]
	NsError,

	#[error("Specify a database to use")]
	DbError,

	#[error("Specify some SQL code to execute")]
	EmptyError,

	#[error("The request body contains invalid data")]
	RequestError,

	#[error("The query failed to complete in time")]
	TimeoutError,

	#[error("The query was cancelled before completion")]
	CancelledError,

	#[error("Parse error on line {line} at character {char} when parsing '{sql}'")]
	ParseError {
		line: usize,
		char: usize,
		sql: String,
	},

	#[error("Problem with embedded script function. {message}")]
	LanguageError {
		message: String,
	},

	#[error("Incorrect arguments for function {name}(). {message}")]
	ArgumentsError {
		name: String,
		message: String,
	},

	#[error("Query timeout of {timer:?} exceeded")]
	QueryTimeoutError {
		timer: Duration,
	},

	#[error("You don't have permission to perform this query type")]
	QueryPermissionsError,

	#[error("You don't have permission to change to the {ns} namespace")]
	NsAuthenticationError {
		ns: String,
	},

	#[error("You don't have permission to change to the {db} database")]
	DbAuthenticationError {
		db: String,
	},

	#[error("Too many recursive subqueries have been set")]
	RecursiveSubqueryError {
		limit: usize,
	},

	#[error("Can not execute CREATE query using value '{value}'")]
	CreateStatementError {
		value: Value,
	},

	#[error("Can not execute UPDATE query using value '{value}'")]
	UpdateStatementError {
		value: Value,
	},

	#[error("Can not execute RELATE query using value '{value}'")]
	RelateStatementError {
		value: Value,
	},

	#[error("Can not execute DELETE query using value '{value}'")]
	DeleteStatementError {
		value: Value,
	},

	#[error("Can not execute INSERT query using value '{value}'")]
	InsertStatementError {
		value: Value,
	},

	#[error("You don't have permission to run the `{query}` query on the `{table}` table")]
	TablePermissionsError {
		query: String,
		table: String,
	},

	#[error("Unable to write to the `{table}` table while setup as a view")]
	TableViewError {
		table: String,
	},

	#[error("Database record `{thing}` already exists")]
	RecordExistsError {
		thing: Thing,
	},

	#[error("Database index `{index}` already contains `{thing}`")]
	RecordIndexError {
		index: String,
		thing: Thing,
	},

	#[error("Key encoding error: {0}")]
	EncodeError(#[from] EncodeError),

	#[error("Key decoding error: {0}")]
	DecodeError(#[from] DecodeError),

	#[error("Datastore error: {0}")]
	EchoDBError(#[from] EchoDBError),

	#[error("Datastore error: {0}")]
	TiKVError(#[from] TiKVError),

	#[error("HTTP Error: {0}")]
	HttpError(#[from] HttpError),

	#[error("JSON Error: {0}")]
	JsonError(#[from] JsonError),

	#[error("CBOR Error: {0}")]
	CborError(#[from] CborError),
}

impl warp::reject::Reject for Error {}
