use core::fmt;
use std::fmt::Display;
use std::io::Error as IoError;
use std::string::FromUtf8Error;

use base64::DecodeError as Base64Error;
use bincode::Error as BincodeError;
#[cfg(storage)]
use ext_sort::SortError;
use fst::Error as FstError;
use http::header::{InvalidHeaderName, InvalidHeaderValue, ToStrError};
use jsonwebtoken::errors::Error as JWTError;
use object_store::Error as ObjectStoreError;
use revision::Error as RevisionError;
use serde::Serialize;
use storekey::DecodeError;
use thiserror::Error;

use crate::api::err::ApiError;
use crate::buc::BucketOperation;
use crate::expr::operation::PatchError;
use crate::expr::{Expr, Idiom};
use crate::iam::Error as IamError;
use crate::idx::ft::MatchRef;
use crate::idx::trees::vector::SharedVector;
use crate::kvs::Error as KvsError;
use crate::syn::error::RenderedError as RenderedParserError;
use crate::val::{CastError, CoerceError, RecordId, Value};

/// An error originating from an embedded SurrealDB database.
#[derive(Error, Debug)]
#[allow(clippy::enum_variant_names)]
#[allow(dead_code, reason = "Some variants are only used by specific KV stores")]
pub(crate) enum Error {
	/// The database encountered unreachable logic
	#[error("The database encountered unreachable logic: {0}")]
	Unreachable(String),

	/// A custom error has been thrown
	#[error("An error occurred: {0}")]
	Thrown(String),

	/// An error originating from the KVS (Key-Value Store) layer
	#[error("There was a problem with the key-value store: {0}")]
	Kvs(#[from] KvsError),

	/// No namespace has been selected
	#[error("Specify a namespace to use")]
	NsEmpty,

	/// No database has been selected
	#[error("Specify a database to use")]
	DbEmpty,

	/// There was an error with the SQL query
	#[error("Parse error: {0}")]
	InvalidQuery(RenderedParserError),

	/// There was an error with the SQL query
	#[error("Cannot use {value} in a CONTENT clause")]
	InvalidContent {
		value: Value,
	},

	/// There was an error with the SQL query
	#[error("Cannot use {value} in a MERGE clause")]
	InvalidMerge {
		value: Value,
	},

	/// There was an error with the provided JSON Patch
	#[error("The JSON Patch contains invalid operations. {0}")]
	InvalidPatch(PatchError),

	#[error("Invalid query: {message}")]
	Query {
		message: String,
	},

	/// Given test operation failed for JSON Patch
	#[error(
		"Given test operation failed for JSON Patch. Expected `{expected}`, but got `{got}` instead."
	)]
	PatchTest {
		expected: String,
		got: String,
	},

	/// Remote HTTP request functions are not enabled
	#[error("Remote HTTP request functions are not enabled")]
	#[allow(dead_code)]
	HttpDisabled,

	/// it is not possible to set a variable with the specified name
	#[error("'{name}' is a protected variable and cannot be set")]
	InvalidParam {
		name: String,
	},

	/// The FETCH clause accepts idioms, strings and fields.
	#[error("Found {value} on FETCH CLAUSE, but FETCH expects an idiom, a string or fields")]
	InvalidFetch {
		value: Expr,
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

	/// There was an error with the provided machine learning model
	#[error("Problem with machine learning computation. {message}")]
	#[allow(dead_code)]
	InvalidModel {
		message: String,
	},

	/// There was a problem running the specified function
	#[error("There was a problem running the {name}() function. {message}")]
	InvalidFunction {
		name: String,
		message: String,
	},

	/// The wrong quantity or magnitude of arguments was given for the specified
	/// function
	#[error("Incorrect arguments for function {name}(). {message}")]
	InvalidArguments {
		name: String,
		message: String,
	},

	/// The wrong quantity or magnitude of arguments was given for the specified
	/// function
	#[error("Invalid aggregation: {message}")]
	InvalidAggregation {
		message: String,
	},

	#[error(
		"Incorrect selector for aggregate selection, expression `{expr}` within in selector cannot be aggregated in a group."
	)]
	InvalidAggregationSelector {
		expr: String,
	},

	/// The URL is invalid
	#[error("The URL `{0}` is invalid")]
	#[cfg_attr(not(feature = "http"), expect(dead_code))]
	InvalidUrl(String),

	/// The size of the vector is incorrect
	#[error("Incorrect vector dimension ({current}). Expected a vector of {expected} dimension.")]
	InvalidVectorDimension {
		current: usize,
		expected: usize,
	},

	/// The size of the vector is incorrect
	#[error(
		"Unable to compute distance.The calculated result is not a valid number: {dist}. Vectors: {left:?} - {right:?}"
	)]
	InvalidVectorDistance {
		left: SharedVector,
		right: SharedVector,
		dist: f64,
	},

	/// The size of the vector is incorrect
	#[error("The value cannot be converted to a vector: {0}")]
	InvalidVectorValue(String),

	/// Invalid regular expression
	#[error("Invalid regular expression: {0:?}")]
	InvalidRegex(String),

	/// Invalid timeout
	#[error("Invalid timeout: {0:?} seconds")]
	InvalidTimeout(u64),

	/// Invalid timeout
	#[error("Invalid control flow statement, break or continue statement found outside of loop.")]
	InvalidControlFlow,

	/// The query timedout
	#[error("The query was not executed because it exceeded the timeout")]
	QueryTimedout,

	/// The query did not execute, because the transaction was cancelled
	#[error("The query was not executed due to a cancelled transaction")]
	QueryCancelled,

	/// The query did not execute, because the memory threshold has been reached
	#[error("The query was not executed due to the memory threshold being reached")]
	QueryBeyondMemoryThreshold,

	/// The query did not execute, because the transaction has failed.
	#[error("The query was not executed due to a failed transaction. {message}")]
	QueryNotExecuted {
		message: String,
	},

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
	#[error("The namespace '{name}' does not exist")]
	NsNotFound {
		name: String,
	},

	/// The requested database does not exist
	#[error("The database '{name}' does not exist")]
	DbNotFound {
		name: String,
	},

	/// The requested event does not exist
	#[error("The event '{name}' does not exist")]
	EvNotFound {
		name: String,
	},

	/// The requested function does not exist
	#[error("The function 'fn::{name}' does not exist")]
	FcNotFound {
		name: String,
	},

	/// The requested function does not exist
	#[error("The module '{name}' does not exist")]
	MdNotFound {
		name: String,
	},

	/// The requested field does not exist
	#[error("The field '{name}' does not exist")]
	FdNotFound {
		name: String,
	},

	/// The requested model does not exist
	#[error("The model 'ml::{name}' does not exist")]
	MlNotFound {
		name: String,
	},

	/// The cluster node does not exist
	#[error("The node '{uuid}' does not exist")]
	NdNotFound {
		uuid: String,
	},

	/// The requested param does not exist
	#[error("The param '${name}' does not exist")]
	PaNotFound {
		name: String,
	},

	/// The requested database does not exist
	#[error("The sequence '{name}' does not exist")]
	SeqNotFound {
		name: String,
	},

	/// The requested config does not exist
	#[error("The config for {name} does not exist")]
	CgNotFound {
		name: String,
	},

	/// The requested table does not exist
	#[error("The table '{name}' does not exist")]
	TbNotFound {
		name: String,
	},

	/// The requested api does not exist
	#[error("The api '/{value}' does not exist")]
	ApNotFound {
		value: String,
	},

	/// The requested analyzer does not exist
	#[error("The analyzer '{name}' does not exist")]
	AzNotFound {
		name: String,
	},

	/// The requested api does not exist
	#[error("The bucket '{name}' does not exist")]
	BuNotFound {
		name: String,
	},

	/// The requested analyzer does not exist
	#[error("The index '{name}' does not exist")]
	IxNotFound {
		name: String,
	},

	/// The requested record does not exist
	#[error("The record '{rid}' does not exist")]
	IdNotFound {
		rid: String,
	},

	/// The requested root user does not exist
	#[error("The root user '{name}' does not exist")]
	UserRootNotFound {
		name: String,
	},

	/// The requested namespace user does not exist
	#[error("The user '{name}' does not exist in the namespace '{ns}'")]
	UserNsNotFound {
		name: String,
		ns: String,
	},

	/// The requested database user does not exist
	#[error("The user '{name}' does not exist in the database '{db}'")]
	UserDbNotFound {
		name: String,
		ns: String,
		db: String,
	},

	/// Unable to perform the realtime query
	#[error("Unable to perform the realtime query")]
	RealtimeDisabled,

	/// Reached excessive computation depth due to functions, subqueries, or
	/// computed values
	#[error("Reached excessive computation depth due to functions, subqueries, or computed values")]
	ComputationDepthExceeded,

	/// Tried to execute a statement that can't be used here
	#[error("Invalid statement: {0}")]
	InvalidStatement(String),

	/// Cannot execute statement using the specified value
	#[error("Cannot execute statement using value: {value}")]
	InvalidStatementTarget {
		value: String,
	},

	/// Cannot execute CREATE statement using the specified value
	#[error("Cannot execute CREATE statement using value: {value}")]
	CreateStatement {
		value: String,
	},

	/// Cannot execute UPSERT statement using the specified value
	#[error("Cannot execute UPSERT statement using value: {value}")]
	UpsertStatement {
		value: String,
	},

	/// Cannot execute UPDATE statement using the specified value
	#[error("Cannot execute UPDATE statement using value: {value}")]
	UpdateStatement {
		value: String,
	},

	/// Cannot execute RELATE statement using the specified value
	#[error("Cannot execute RELATE statement where property 'in' is: {value}")]
	RelateStatementIn {
		value: String,
	},

	/// Cannot execute RELATE statement using the specified value
	#[error("Cannot execute RELATE statement where property 'id' is: {value}")]
	RelateStatementId {
		value: String,
	},

	/// Cannot execute RELATE statement using the specified value
	#[error("Cannot execute RELATE statement where property 'out' is: {value}")]
	RelateStatementOut {
		value: String,
	},

	/// Cannot execute DELETE statement using the specified value
	#[error("Cannot execute DELETE statement using value: {value}")]
	DeleteStatement {
		value: String,
	},

	/// Cannot execute INSERT statement using the specified value
	#[error("Cannot execute INSERT statement using value: {value}")]
	InsertStatement {
		value: String,
	},

	/// Cannot execute INSERT statement using the specified value
	#[error("Cannot execute INSERT statement where property 'in' is: {value}")]
	InsertStatementIn {
		value: String,
	},

	/// Cannot execute INSERT statement using the specified value
	#[error("Cannot execute INSERT statement where property 'id' is: {value}")]
	InsertStatementId {
		value: String,
	},

	/// Cannot execute INSERT statement using the specified value
	#[error("Cannot execute INSERT statement where property 'out' is: {value}")]
	InsertStatementOut {
		value: String,
	},

	/// Cannot execute LIVE statement using the specified value
	#[error("Cannot execute LIVE statement using value: {value}")]
	LiveStatement {
		value: String,
	},

	/// Cannot execute KILL statement using the specified id
	#[error("Cannot execute KILL statement using id: {value}")]
	KillStatement {
		value: String,
	},

	/// Cannot execute CREATE statement using the specified value
	#[error("Expected a single result output when using the ONLY keyword")]
	SingleOnlyOutput,

	/// The permissions do not allow this query to be run on this table
	#[error("You don't have permission to view the ${name} parameter")]
	ParamPermissions {
		name: String,
	},

	/// The permissions do not allow this query to be run on this table
	#[error("You don't have permission to run the fn::{name} function")]
	FunctionPermissions {
		name: String,
	},

	/// The permissions do not allow this query to be run on this table
	#[error("You don't have permission to {op} this file in the `{name}` bucket")]
	BucketPermissions {
		name: String,
		op: BucketOperation,
	},

	/// A database entry for the specified record already exists
	#[error("Database record `{record}` already exists")]
	RecordExists {
		record: RecordId,
	},

	/// A database index entry for the specified record already exists
	#[error("Database index `{index}` already contains {value}, with record `{record}`")]
	IndexExists {
		record: RecordId,
		index: String,
		value: String,
	},

	/// The specified table is not configured for the type of record being added
	#[error("Found record: `{record}` which is {}a relation, but expected a {target_type}", if *relation { "" } else { "not " })]
	TableCheck {
		record: String,
		relation: bool,
		target_type: String,
	},

	/// The specified field did not conform to the field ASSERT clause
	#[error(
		"Found {value} for field `{field}`, with record `{record}`, but field must conform to: {check}"
	)]
	FieldValue {
		record: String,
		value: String,
		field: Idiom,
		check: String,
	},

	/// The specified value did not conform to the LET type check
	#[error("Tried to set `${name}`, but couldn't coerce value: {error}")]
	SetCoerce {
		name: String,
		error: Box<CoerceError>,
	},

	/// The specified value did not conform to the LET type check
	#[error("Couldn't coerce return value from function `{name}`: {error}")]
	ReturnCoerce {
		name: String,
		error: Box<CoerceError>,
	},

	/// The specified value did not conform to the LET type check
	#[error("Couldn't coerce value for field `{field_name}` of `{record}`: {error}")]
	FieldCoerce {
		record: String,
		field_name: String,
		error: Box<CoerceError>,
	},

	/// The specified field did not conform to the field ASSERT clause
	#[error(
		"Found changed value for field `{field}`, with record `{record}`, but field is readonly"
	)]
	FieldReadonly {
		record: String,
		field: Idiom,
	},

	/// The specified field on a SCHEMAFUL table was not defined
	#[error("Found field '{field}', but no such field exists for table '{table}'")]
	FieldUndefined {
		table: String,
		field: Idiom,
	},

	/// Found a record id for the record but this is not a valid id
	#[error("Found {value} for the Record ID but this is not a valid id")]
	IdInvalid {
		value: String,
	},

	/// Found a record id for the record but we are creating a specific record
	#[error("Found {value} for the `id` field, but a specific record has been specified")]
	IdMismatch {
		value: String,
	},

	/// Found a record id for the record but we are creating a specific record
	#[error("Found {value} for the `in` field, which does not match the existing field value")]
	InOverride {
		value: String,
	},

	/// Found a record id for the record but we are creating a specific record
	#[error("Found {value} for the `out` field, which does not match the existing field value")]
	OutOverride {
		value: String,
	},

	/// Unable to coerce to a value to another value
	#[error("{0}")]
	Coerce(#[from] CoerceError),

	/// Unable to convert a value to another value
	#[error("{0}")]
	Cast(#[from] CastError),

	/// Cannot perform addition
	#[error("Cannot perform addition with '{0}' and '{1}'")]
	TryAdd(String, String),

	/// Cannot perform subtraction
	#[error("Cannot perform subtraction with '{0}' and '{1}'")]
	TrySub(String, String),

	/// Cannot perform multiplication
	#[error("Cannot perform multiplication with '{0}' and '{1}'")]
	TryMul(String, String),

	/// Cannot perform division
	#[error("Cannot perform division with '{0}' and '{1}'")]
	TryDiv(String, String),

	/// Cannot perform remainder
	#[error("Cannot perform remainder with '{0}' and '{1}'")]
	TryRem(String, String),

	/// Cannot perform power
	#[error("Cannot raise the value '{0}' with '{1}'")]
	TryPow(String, String),

	/// Cannot perform negation
	#[error("Cannot negate the value '{0}'")]
	TryNeg(String),

	/// It's is not possible to convert between the two types
	#[error("Cannot convert from '{0}' to '{1}'")]
	TryFrom(String, &'static str),

	/// There was an error processing a remote HTTP request
	#[error("There was an error processing a remote HTTP request: {0}")]
	#[cfg_attr(not(feature = "http"), expect(dead_code))]
	Http(String),

	/// There was an error processing a value in parallel
	#[error("There was an error processing a value in parallel: {0}")]
	Channel(String),

	/// Represents an underlying error with IO encoding / decoding
	#[error("I/O error: {0}")]
	Io(#[from] IoError),

	/// Error for when trying to serialize values like Regex and Closure
	#[error("Tried to serialize a value which cannot be serialized.")]
	Unencodable,

	/// Represents an error when decoding a key-value entry
	#[error("Key decoding error: {0}")]
	Decode(#[from] DecodeError),

	/// Represents an underlying error with versioned data encoding / decoding
	#[error("Versioned error: {0}")]
	Revision(#[from] RevisionError),

	/// The index has been found to be inconsistent
	#[error("Index is corrupted: {0}")]
	CorruptedIndex(&'static str),

	/// The query planner did not find an index able to support the given
	/// expression
	#[error("There was no suitable index supporting the expression: {exp}")]
	NoIndexFoundForMatch {
		exp: String,
	},

	/// Represents an error when analyzing a value
	#[error("A value can't be analyzed: {0}")]
	AnalyzerError(String),

	/// Represents an error when trying to highlight a value
	#[error("A value can't be highlighted: {0}")]
	HighlightError(String),

	/// Represents an underlying error with Bincode serializing / deserializing
	#[error("Bincode error: {0}")]
	Bincode(#[from] BincodeError),

	/// Represents an underlying error with FST
	#[error("FstError error: {0}")]
	FstError(#[from] FstError),

	/// Represents an underlying error while reading UTF8 characters
	#[error("Utf8 error: {0}")]
	Utf8Error(#[from] FromUtf8Error),

	/// Represents an underlying error with the Object Store
	#[error("Object Store error: {0}")]
	ObsError(#[from] ObjectStoreError),

	/// Duplicated match references are not allowed
	#[error("Duplicated Match reference: {mr}")]
	DuplicatedMatchRef {
		mr: MatchRef,
	},

	/// Represents a failure in timestamp arithmetic related to database
	/// internals
	#[error("Timestamp arithmetic error: {0}")]
	TimestampOverflow(String),

	/// Internal server error
	/// This should be used extremely sporadically, since we lose the type of
	/// error as a consequence There will be times when it is useful, such as
	/// with unusual type conversion errors
	#[error("Internal database error: {0}")]
	Internal(String),

	/// Unimplemented functionality
	#[error("Unimplemented functionality: {0}")]
	Unimplemented(String),

	/// Represents an underlying IAM error
	#[error("IAM error: {0}")]
	IamError(#[from] IamError),

	//
	// Capabilities
	/// Scripting is not allowed
	#[error("Scripting functions are not allowed")]
	ScriptingNotAllowed,

	/// Function is not allowed
	#[error("Function '{0}' is not allowed to be executed")]
	FunctionNotAllowed(String),

	/// Network target is not allowed
	#[error("Access to network target '{0}' is not allowed")]
	#[cfg_attr(not(feature = "http"), expect(dead_code))]
	NetTargetNotAllowed(String),

	//
	// Authentication / Signup
	#[error("There was an error creating the token")]
	TokenMakingFailed,

	#[error("No record was returned")]
	NoRecordFound,

	#[error("Username or Password was not provided")]
	MissingUserOrPass,

	#[error("No signin target to either SC or DB or NS or KV")]
	NoSigninTarget,

	#[error("The password did not verify")]
	InvalidPass,

	/// There was an error with authentication
	///
	/// This error hides different kinds of errors directly related to
	/// authentication
	#[error("There was a problem with authentication")]
	InvalidAuth,

	/// There was an unexpected error while performing authentication
	///
	/// This error hides different kinds of unexpected errors that may affect
	/// authentication
	#[error("There was an unexpected error while performing authentication")]
	UnexpectedAuth,

	/// There was an error with signing up
	#[error("There was a problem with signing up")]
	InvalidSignup,

	// The cluster node already exists
	#[error("The node '{id}' already exists")]
	ClAlreadyExists {
		id: String,
	},

	/// The requested api already exists
	#[error("The api '/{value}' already exists")]
	ApAlreadyExists {
		value: String,
	},

	/// The requested analyzer already exists
	#[error("The analyzer '{name}' already exists")]
	AzAlreadyExists {
		name: String,
	},

	/// The requested api already exists
	#[error("The bucket '{value}' already exists")]
	BuAlreadyExists {
		value: String,
	},

	/// The requested database already exists
	#[error("The database '{name}' already exists")]
	DbAlreadyExists {
		name: String,
	},

	/// The requested event already exists
	#[error("The event '{name}' already exists")]
	EvAlreadyExists {
		name: String,
	},

	/// The requested field already exists
	#[error("The field '{name}' already exists")]
	FdAlreadyExists {
		name: String,
	},

	/// The requested function already exists
	#[error("The function 'fn::{name}' already exists")]
	FcAlreadyExists {
		name: String,
	},

	/// The requested module already exists
	#[error("The module '{name}' already exists")]
	MdAlreadyExists {
		name: String,
	},

	/// The requested index already exists
	#[error("The index '{name}' already exists")]
	IxAlreadyExists {
		name: String,
	},

	/// The requested model already exists
	#[error("The model '{name}' already exists")]
	MlAlreadyExists {
		name: String,
	},

	/// The requested namespace already exists
	#[error("The namespace '{name}' already exists")]
	NsAlreadyExists {
		name: String,
	},

	/// The requested param already exists
	#[error("The param '${name}' already exists")]
	PaAlreadyExists {
		name: String,
	},

	/// The requested config already exists
	#[error("The config for {name} already exists")]
	CgAlreadyExists {
		name: String,
	},

	/// The requested sequence already exists
	#[error("The sequence '{name}' already exists")]
	SeqAlreadyExists {
		name: String,
	},

	/// The requested table already exists
	#[error("The table '{name}' already exists")]
	TbAlreadyExists {
		name: String,
	},

	/// The requested namespace token already exists
	#[error("The namespace token '{name}' already exists")]
	#[allow(dead_code)]
	NtAlreadyExists {
		name: String,
	},

	/// The requested database token already exists
	#[error("The database token '{name}' already exists")]
	#[allow(dead_code)]
	DtAlreadyExists {
		name: String,
	},

	/// The requested user already exists
	#[error("The root user '{name}' already exists")]
	UserRootAlreadyExists {
		name: String,
	},

	/// The requested namespace user already exists
	#[error("The user '{name}' already exists in the namespace '{ns}'")]
	UserNsAlreadyExists {
		name: String,
		ns: String,
	},

	/// The requested database user already exists
	#[error("The user '{name}' already exists in the database '{db}'")]
	UserDbAlreadyExists {
		name: String,
		ns: String,
		db: String,
	},

	/// A database index entry for the specified table is already building
	#[error("Database index `{name}` is currently building")]
	IndexAlreadyBuilding {
		name: String,
	},

	/// A database index entry for the specified table is already building
	#[error("Index building has been cancelled")]
	IndexingBuildingCancelled,

	/// The token has expired
	#[error("The token has expired")]
	ExpiredToken,

	/// The session has expired
	#[error("The session has expired")]
	ExpiredSession,

	/// The supplied type could not be serialiazed into `expr::Value`
	#[error("Serialization error: {0}")]
	Serialization(String),

	/// The requested root access method already exists
	#[error("The root access method '{ac}' already exists")]
	AccessRootAlreadyExists {
		ac: String,
	},

	/// The requested namespace access method already exists
	#[error("The access method '{ac}' already exists in the namespace '{ns}'")]
	AccessNsAlreadyExists {
		ac: String,
		ns: String,
	},

	/// The requested database access method already exists
	#[error("The access method '{ac}' already exists in the database '{db}'")]
	AccessDbAlreadyExists {
		ac: String,
		ns: String,
		db: String,
	},

	/// The requested root access method does not exist
	#[error("The root access method '{ac}' does not exist")]
	AccessRootNotFound {
		ac: String,
	},

	/// The requested root access grant does not exist
	#[error("The root access grant '{gr}' does not exist for '{ac}'")]
	AccessGrantRootNotFound {
		ac: String,
		gr: String,
	},

	/// The requested namespace access method does not exist
	#[error("The access method '{ac}' does not exist in the namespace '{ns}'")]
	AccessNsNotFound {
		ac: String,
		ns: String,
	},

	/// The requested namespace access grant does not exist
	#[error("The access grant '{gr}' does not exist for '{ac}' in the namespace '{ns}'")]
	AccessGrantNsNotFound {
		ac: String,
		gr: String,
		ns: String,
	},

	/// The requested database access method does not exist
	#[error("The access method '{ac}' does not exist in the database '{db}'")]
	AccessDbNotFound {
		ac: String,
		ns: String,
		db: String,
	},

	/// The requested database access grant does not exist
	#[error("The access grant '{gr}' does not exist for '{ac}' in the database '{db}'")]
	AccessGrantDbNotFound {
		ac: String,
		gr: String,
		ns: String,
		db: String,
	},

	/// The access method cannot be defined on the requested level
	#[error("The access method cannot be defined on the requested level")]
	AccessLevelMismatch,

	#[error("The access method cannot be used in the requested operation")]
	AccessMethodMismatch,

	#[error("The access method does not exist")]
	AccessNotFound,

	#[error("This access method has an invalid duration")]
	AccessInvalidDuration,

	#[error("This access method results in an invalid expiration")]
	AccessInvalidExpiration,

	#[error("The record access signup query failed")]
	AccessRecordSignupQueryFailed,

	#[error("The record access signin query failed")]
	AccessRecordSigninQueryFailed,

	#[error("This record access method does not allow signup")]
	AccessRecordNoSignup,

	#[error("This record access method does not allow signin")]
	AccessRecordNoSignin,

	#[error("This bearer access method requires a key to be provided")]
	AccessBearerMissingKey,

	#[error("This bearer access grant has an invalid format")]
	AccessGrantBearerInvalid,

	#[error("This access grant has an invalid subject")]
	AccessGrantInvalidSubject,

	#[error("This access grant has been revoked")]
	AccessGrantRevoked,

	/// Found a table name for the record but this is not a valid table
	#[error("Found {value} for the Record ID but this is not a valid table name")]
	TbInvalid {
		value: String,
	},

	/// A destructuring variant was used in a context where it is not supported
	#[error("{variant} destructuring method is not supported here")]
	UnsupportedDestructure {
		variant: String,
	},

	#[doc(hidden)]
	#[error("The underlying datastore does not support versioned queries")]
	UnsupportedVersionedQueries,

	/// There was an invalid storage version stored in the database
	#[error("There was an invalid storage version stored in the database")]
	InvalidStorageVersion,

	/// There was an outdated storage version stored in the database
	#[error(
		"The data stored on disk is out-of-date with this version. Please follow the upgrade guides in the documentation"
	)]
	OutdatedStorageVersion,

	#[error("Size of query script exceeded maximum supported size of 4,294,967,295 bytes.")]
	QueryTooLarge,

	/// Represents a failure in timestamp arithmetic related to database
	/// internals
	#[error("Failed to compute: \"{0}\", as the operation results in an arithmetic overflow.")]
	ArithmeticOverflow(String),

	/// Represents a negative value for a type that must be zero or positive
	#[error("Failed to compute: \"{0}\", as the operation results in a negative value.")]
	ArithmeticNegativeOverflow(String),

	#[error("Error while ordering a result: {0}.")]
	#[cfg_attr(target_family = "wasm", expect(dead_code))]
	OrderingError(String),

	/// Found an unexpected value in a range
	#[error("Found {found} for bound but expected {expected}.")]
	InvalidBound {
		found: String,
		expected: String,
	},

	/// Found an unexpected value in a range
	#[error("Exceeded the idiom recursion limit of {limit}.")]
	IdiomRecursionLimitExceeded {
		limit: u32,
	},

	/// Tried to use an idiom RepeatRecurse symbol in a position where it is not
	/// supported
	#[error("Tried to use a `@` repeat recurse symbol in a position where it is not supported")]
	UnsupportedRepeatRecurse,

	/// Tried to use an idiom RepeatRecurse symbol in a position where it is not
	/// supported
	#[error("Cannot construct a recursion plan when an instruction is provided")]
	RecursionInstructionPlanConflict,

	/// The record cannot be deleted as it's still referenced elsewhere
	#[error("Cannot delete `{0}` as it is referenced by `{1}` with an ON DELETE REJECT clause")]
	DeleteRejectedByReference(String, String),

	/// The `REFERENCE` keyword can only be used in combination with a type
	/// referencing a record
	#[error(
		"Cannot use the `REFERENCE` keyword with `TYPE {0}`. Specify only a `record` type, or a type containing only records, instead."
	)]
	ReferenceTypeConflict(String),

	#[error(
		"Cannot use the `REFERENCE` keyword on nested field `{0}`. Specify a referencing field at the root level instead."
	)]
	ReferenceNestedField(String),

	/// Something went wrong while updating references
	#[error("An error occured while updating references for `{0}`: {1}")]
	RefsUpdateFailure(String, String),

	#[error(
		"Cannot set field `{name}` with type `{kind}` as it mismatched with field `{existing_name}` with type `{existing_kind}`"
	)]
	MismatchedFieldTypes {
		name: String,
		kind: String,
		existing_name: String,
		existing_kind: String,
	},

	#[error("An API error occurred: {0}")]
	ApiError(ApiError),

	#[error("The string could not be parsed into a bytesize")]
	InvalidBytesize,

	#[error("The string could not be parsed into a path: {0}")]
	InvalidPath(String),

	#[error("File access denied: {0}")]
	FileAccessDenied(String),

	#[error("No global bucket has been configured")]
	NoGlobalBucket,

	#[error("Bucket `{0}` is unavailable")]
	BucketUnavailable(String),

	#[error("Bucket is unavailable")]
	GlobalBucketEnforced,

	#[error("Bucket url could not be processed: {0}")]
	#[cfg_attr(target_family = "wasm", expect(dead_code))]
	InvalidBucketUrl(String),

	#[error("Bucket backend is not supported")]
	UnsupportedBackend,

	#[error("Write operation is not supported, as bucket `{0}` is in read-only mode")]
	ReadonlyBucket(String),

	#[error("Operation for bucket `{0}` failed: {1}")]
	ObjectStoreFailure(String, String),

	/// The `COMPUTED` clause cannot be used with other clauses altering or
	/// working with the value
	#[error("Cannot use the `{0}` keyword with `COMPUTED`.")]
	ComputedKeywordConflict(String),

	/// The `COMPUTED` clause cannot be used with other nested fields
	#[error("Cannot define field `{0}` as `COMPUTED` since a nested field `{1}` already exists.")]
	ComputedNestedFieldConflict(String, String),

	/// The `COMPUTED` clause cannot be used with other nested fields
	#[error("Cannot define nested field `{0}` as parent field `{1}` is a `COMPUTED` field.")]
	ComputedParentFieldConflict(String, String),

	#[error("Cannot define field `{0}` as `COMPUTED` fields must be top-level.")]
	ComputedNestedField(String),

	/// Cannot use the `{0}` keyword on the `id` field
	#[error("Cannot use the `{0}` keyword on the `id` field.")]
	IdFieldKeywordConflict(String),

	/// Cannot use the `{0}` keyword on the `id` field
	#[error("Cannot use the `{0}` type on the `id` field, as that's not a valid record id key.")]
	IdFieldUnsupportedKind(String),
}

impl Error {
	#[cold]
	#[track_caller]
	pub fn unreachable<T: fmt::Display>(message: T) -> Error {
		let location = std::panic::Location::caller();
		let message = format!("{}:{}: {}", location.file(), location.line(), message);
		Error::Unreachable(message)
	}

	/// Check if this error is related to schema checks
	pub fn is_schema_related(&self) -> bool {
		matches!(
			self,
			Error::FieldCoerce { .. }
				| Error::FieldValue { .. }
				| Error::FieldReadonly { .. }
				| Error::FieldUndefined { .. }
		)
	}
}

impl From<Error> for String {
	fn from(e: Error) -> String {
		e.to_string()
	}
}

impl From<ApiError> for Error {
	fn from(value: ApiError) -> Self {
		Error::ApiError(value)
	}
}

impl From<Base64Error> for Error {
	fn from(_: Base64Error) -> Error {
		Error::InvalidAuth
	}
}

impl From<JWTError> for Error {
	fn from(_: JWTError) -> Error {
		Error::InvalidAuth
	}
}

impl From<regex::Error> for Error {
	fn from(error: regex::Error) -> Self {
		Error::InvalidRegex(error.to_string())
	}
}

impl From<InvalidHeaderName> for Error {
	fn from(error: InvalidHeaderName) -> Self {
		Error::Unreachable(error.to_string())
	}
}

impl From<InvalidHeaderValue> for Error {
	fn from(error: InvalidHeaderValue) -> Self {
		Error::Unreachable(error.to_string())
	}
}

impl From<ToStrError> for Error {
	fn from(error: ToStrError) -> Self {
		Error::Unreachable(error.to_string())
	}
}

impl From<async_channel::RecvError> for Error {
	fn from(e: async_channel::RecvError) -> Error {
		Error::Channel(e.to_string())
	}
}

impl<T> From<async_channel::SendError<T>> for Error {
	fn from(e: async_channel::SendError<T>) -> Error {
		Error::Channel(e.to_string())
	}
}

#[cfg(any(feature = "http", feature = "jwks"))]
impl From<reqwest::Error> for Error {
	fn from(e: reqwest::Error) -> Error {
		Error::Http(e.to_string())
	}
}

#[cfg(storage)]
impl<S, D, I> From<SortError<S, D, I>> for Error
where
	S: std::error::Error,
	D: std::error::Error,
	I: std::error::Error,
{
	fn from(e: SortError<S, D, I>) -> Error {
		Error::Internal(e.to_string())
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

impl serde::ser::Error for Error {
	fn custom<T>(msg: T) -> Self
	where
		T: Display,
	{
		Self::Serialization(msg.to_string())
	}
}
