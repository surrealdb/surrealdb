use crate::sql::thing::Thing;
use crate::sql::value::Value;
use msgpack::encode::Error as SerdeError;
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

#[derive(Error, Debug)]
pub enum Error {
	#[error("Conditional clause is not truthy")]
	Ignore,

	#[error("Couldn't setup connection to underlying datastore")]
	Ds,

	#[error("Couldn't create a database transaction")]
	Tx,

	#[error("Couldn't update a finished transaction")]
	TxFinished,

	#[error("Couldn't write to a read only transaction")]
	TxReadonly,

	#[error("Specify a namespace to use")]
	NsEmpty,

	#[error("Specify a database to use")]
	DbEmpty,

	#[error("Specify some SQL code to execute")]
	QueryEmpty,

	#[error("Parse error on line {line} at character {char} when parsing '{sql}'")]
	InvalidQuery {
		line: usize,
		char: usize,
		sql: String,
	},

	#[error("The JSON Patch contains invalid operations. {message}")]
	InvalidPatch {
		message: String,
	},

	#[error("Problem with embedded script function. {message}")]
	InvalidScript {
		message: String,
	},

	#[error("Incorrect arguments for function {name}(). {message}")]
	InvalidArguments {
		name: String,
		message: String,
	},

	#[error("Query timeout of {timer:?} exceeded")]
	QueryTimeout {
		timer: Duration,
	},

	#[error("Query not executed due to cancelled transaction")]
	QueryCancelled,

	#[error("Query not executed due to failed transaction")]
	QueryNotExecuted,

	#[error("You don't have permission to perform this query type")]
	QueryPermissions,

	#[error("You don't have permission to change to the {ns} namespace")]
	NsNotAllowed {
		ns: String,
	},

	#[error("You don't have permission to change to the {db} database")]
	DbNotAllowed {
		db: String,
	},

	#[error("The namespace does not exist")]
	NsNotFound,

	#[error("The namespace token does not exist")]
	NtNotFound,

	#[error("The namespace login does not exist")]
	NlNotFound,

	#[error("The database does not exist")]
	DbNotFound,

	#[error("The database token does not exist")]
	DtNotFound,

	#[error("The database login does not exist")]
	DlNotFound,

	#[error("The scope does not exist")]
	ScNotFound,

	#[error("The scope token does not exist")]
	StNotFound,

	#[error("The table does not exist")]
	TbNotFound,

	#[error("Too many recursive subqueries have been set")]
	TooManySubqueries {
		limit: usize,
	},

	#[error("Can not execute CREATE query using value '{value}'")]
	CreateStatement {
		value: Value,
	},

	#[error("Can not execute UPDATE query using value '{value}'")]
	UpdateStatement {
		value: Value,
	},

	#[error("Can not execute RELATE query using value '{value}'")]
	RelateStatement {
		value: Value,
	},

	#[error("Can not execute DELETE query using value '{value}'")]
	DeleteStatement {
		value: Value,
	},

	#[error("Can not execute INSERT query using value '{value}'")]
	InsertStatement {
		value: Value,
	},

	#[error("You don't have permission to run the `{query}` query on the `{table}` table")]
	TablePermissionsError {
		query: String,
		table: String,
	},

	#[error("Unable to write to the `{table}` table while setup as a view")]
	TableIsView {
		table: String,
	},

	#[error("Database record `{thing}` already exists")]
	RecordExists {
		thing: Thing,
	},

	#[error("Database index `{index}` already contains `{thing}`")]
	RecordIndex {
		index: String,
		thing: Thing,
	},

	#[error("Serde error: {0}")]
	Serde(#[from] SerdeError),

	#[error("Key encoding error: {0}")]
	Encode(#[from] EncodeError),

	#[error("Key decoding error: {0}")]
	Decode(#[from] DecodeError),

	#[cfg(feature = "kv-echodb")]
	#[error("Datastore error: {0}")]
	EchoDB(#[from] EchoDBError),

	#[cfg(feature = "kv-indxdb")]
	#[error("Datastore error: {0}")]
	IndxDB(#[from] IndxDBError),

	#[cfg(feature = "kv-tikv")]
	#[error("Datastore error: {0}")]
	TiKV(#[from] TiKVError),

	#[cfg(feature = "parallel")]
	#[error("Tokio Error: {0}")]
	Tokio(#[from] TokioError<(Option<Thing>, Value)>),
}
