use crate::sql::duration::Duration;
use crate::sql::thing::Thing;
use serde::{Deserialize, Serialize};
use serde_cbor::error::Error as CborError;
use serde_json::Error as JsonError;

#[derive(Fail, Debug)]
pub enum Error {
	#[fail(display = "Specify a namespace to use")]
	NSError,

	#[fail(display = "Specify a database to use")]
	DBError,

	#[fail(display = "Specify some SQL code to execute")]
	EmptyError,

	#[fail(display = "Parse error at position {} when parsing '{}'", pos, sql)]
	ParseError { pos: usize, sql: String },

	#[fail(display = "Query timeout of {} exceeded", timer)]
	TimerError { timer: Duration },

	#[fail(display = "Database record `{}` already exists", thing)]
	ExistError { thing: Thing },

	#[fail(display = "Database index `{}` already contains `{}`", index, thing)]
	IndexError { index: String, thing: Thing },

	#[fail(display = "You don't have permission to perform the query `{}`", query)]
	PermsError { query: String },

	#[fail(
		display = "Unable to write to the `{}` table while setup as a view",
		table
	)]
	WriteError { table: String },

	#[fail(
		display = "You don't have permission to perform this query on the `{}` table",
		table
	)]
	TableError { table: String },

	#[fail(display = "JSON Error: {}", _0)]
	JsonError(#[cause] JsonError),

	#[fail(display = "CBOR Error: {}", _0)]
	CborError(#[cause] CborError),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct Message {
	pub code: usize,
	pub info: String,
}

impl Error {
	pub fn build(&self) -> Message {
		Message {
			code: 400,
			info: format!("{}", self),
		}
	}
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
