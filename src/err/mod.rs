use crate::sql::duration::Duration;
use crate::sql::thing::Thing;
use serde_cbor::error::Error as CborError;
use serde_json::error::Error as JsonError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
	#[error("Specify a namespace to use")]
	NsError,

	#[error("Specify a database to use")]
	DbError,

	#[error("Specify some SQL code to execute")]
	EmptyError,

	#[error("Parse error at position {pos} when parsing '{sql}'")]
	ParseError {
		pos: usize,
		sql: String,
	},

	#[error("Wrong number of arguments at position {pos} when parsing '{sql}'")]
	CountError {
		pos: usize,
		sql: String,
	},

	#[error("Query timeout of {timer} exceeded")]
	TimerError {
		timer: Duration,
	},

	#[error("Database record `{thing}` already exists")]
	ExistError {
		thing: Thing,
	},

	#[error("Database index `{index}` already contains `{thing}`")]
	IndexError {
		index: String,
		thing: Thing,
	},

	#[error("You don't have permission to perform the query `{query}`")]
	PermsError {
		query: String,
	},

	#[error("Unable to write to the `{table}` table while setup as a view")]
	WriteError {
		table: String,
	},

	#[error("You don't have permission to perform this query on the `{table}` table")]
	TableError {
		table: String,
	},

	#[error("JSON Error: {0}")]
	JsonError(JsonError),

	#[error("CBOR Error: {0}")]
	CborError(CborError),
}

impl From<JsonError> for Error {
	fn from(err: JsonError) -> Error {
		Error::JsonError(err)
	}
}

impl From<CborError> for Error {
	fn from(err: CborError) -> Error {
		Error::CborError(err)
	}
}
