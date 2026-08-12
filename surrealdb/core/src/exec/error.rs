//! Failures raised while executing a statement.
//!
//! This is the vocabulary of running a query: which statement forms are legal
//! where, whether a clause evaluated to something usable, which permission
//! predicate said no, how deep evaluation may recurse. Both executors - the
//! recursive `compute` path and the streaming planner - speak it, which is why
//! it is one type rather than one per executor.
//!
//! It deliberately names nothing about values, the catalog, keys, indexes,
//! documents, buckets or storage: those layers own their own failures.

// The mapper below is the only place this layer's failures become public.
// A new variant must make that decision explicitly rather than inheriting
// whatever the last arm happened to be.
#![deny(clippy::wildcard_enum_match_arm)]

use common::{LeafError, internal_todo};
use surrealdb_types::{Error as TypesError, ToSql, ValidationError};

use crate::expr::Expr;
use crate::val::CoerceError;

/// A failure in the statement-execution layer.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
	/// A custom error has been thrown
	#[error("An error occurred: {0}")]
	Thrown(String),

	/// No namespace has been selected
	#[error("Specify a namespace to use")]
	NsEmpty,

	/// No database has been selected
	#[error("Specify a database to use")]
	DbEmpty,

	#[error("Invalid query: {message}")]
	Query {
		message: String,
	},

	/// it is not possible to set a variable with the specified name
	#[error("'{name}' is a protected variable and cannot be set")]
	InvalidParam {
		name: String,
	},

	/// The FETCH clause accepts idioms, strings and fields.
	#[error("Found {} on FETCH CLAUSE, but FETCH expects an idiom, a string or fields", value.to_sql())]
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

	/// Invalid timeout
	#[error("Invalid control flow statement, break or continue statement found outside of loop.")]
	InvalidControlFlow,

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
	#[error("You don't have permission to run the {name} function")]
	FunctionPermissions {
		name: String,
	},

	/// A permission predicate attempted to perform a write or other side effect
	/// while being evaluated (GHSA-66r2-5gwj-gxm2). Permission expressions are
	/// evaluated with permission enforcement disabled, so they must be free of
	/// observable side effects.
	#[error("A PERMISSIONS clause cannot contain a statement that modifies data")]
	PermissionPredicateSideEffect,

	/// A `COMPUTED` body reached a data-modifying statement while being
	/// evaluated. The definition-time check ([`Error::ComputedWrite`]) rejects a
	/// mutation written into the body, but treats a call to a user-defined
	/// function as opaque, since the callee is stored separately and can be
	/// redefined afterwards. This is the runtime half of that pair.
	#[error("A COMPUTED clause cannot contain a statement that modifies data")]
	ComputedFieldSideEffect,

	/// A DEFINE/ALTER (or import) supplied a permission clause that directly
	/// contains a data-modifying statement, which is not allowed.
	#[error(
		"Found a non-read-only expression in the PERMISSIONS clause for {kind} `{name}`, but a PERMISSIONS clause must not modify data"
	)]
	PermissionClauseNotReadonly {
		kind: &'static str,
		name: String,
	},

	/// A DEFINE supplied a permission clause that calls a user-defined
	/// function whose stored body reaches a data-modifying statement. The
	/// direct form is [`Error::PermissionClauseNotReadonly`]; this is the
	/// same rule resolved through the function call graph.
	#[error(
		"The PERMISSIONS clause for {kind} `{name}` calls `fn::{function}`, which modifies data, but a PERMISSIONS clause must not modify data"
	)]
	PermissionWriteViaFunction {
		kind: &'static str,
		name: String,
		function: String,
	},

	/// A DEFINE FIELD supplied a COMPUTED body that calls a user-defined
	/// function whose stored body reaches a data-modifying statement. The
	/// direct form is [`Error::ComputedWrite`]; this is the same rule
	/// resolved through the function call graph.
	#[error(
		"Cannot define field `{field}` as `COMPUTED`: the body calls `fn::{function}`, which modifies data, and `COMPUTED` bodies must be read-only"
	)]
	ComputedWriteViaFunction {
		field: String,
		function: String,
	},

	/// A DEFINE/ALTER FUNCTION supplied a body that reaches a data-modifying
	/// statement while COMPUTED fields or permission clauses depend on the
	/// function staying read-only.
	#[error(
		"Cannot define function `fn::{name}`: it modifies data, but it must remain read-only because it is used by {consumers}"
	)]
	FunctionRequiredReadOnly {
		name: String,
		consumers: String,
	},

	/// A DEFINE/ALTER supplied a `PERMISSIONS FOR create/update/delete` clause
	/// that modifies data while the `mutable_permissions` capability is off.
	/// SELECT clauses are never permitted to modify data; these write-triggered
	/// clauses are, but only behind the transitional capability.
	#[error(
		"Cannot define {kind} `{name}`: a create/update/delete PERMISSIONS clause modifies data, which requires the `mutable_permissions` capability to be enabled; otherwise move the side effect to a DEFINE EVENT"
	)]
	MutablePermissionsDisabled {
		kind: &'static str,
		name: String,
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

	/// Internal server error
	/// Unimplemented functionality
	#[error("Unimplemented functionality: {0}")]
	Unimplemented(String),

	/// The planner does not support this statement type (e.g. DML/DDL).
	/// Callers should always fall back to the compute path.
	#[error("Planner unsupported: {0}")]
	PlannerUnsupported(String),

	/// The planner intends to support this but it is not yet implemented.
	/// Callers fall back in BestEffort mode; hard error in AllReadOnlyStatements mode.
	#[error("Planner not yet implemented: {0}")]
	#[allow(
		dead_code,
		reason = "no planner path raises this today; the ladders that read it are already in place"
	)]
	PlannerUnimplemented(String),

	/// The access method cannot be defined on the requested level
	#[error("The access method cannot be defined on the requested level")]
	AccessLevelMismatch,

	#[error(
		"The ES512 algorithm is not currently supported. Please use ES384 or another supported algorithm"
	)]
	AccessUnsupportedAlgorithm,

	#[error(
		"Tokens issued by record access methods can be consumed by third parties and must have an expiration; DURATION FOR TOKEN cannot be NONE on TYPE RECORD access"
	)]
	AccessRecordTokenDurationRequired,

	#[error("This access grant has an invalid subject")]
	AccessGrantInvalidSubject,

	#[error("This access grant has been revoked")]
	AccessGrantRevoked,

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

	/// Encountered a non-record-id value during recursive graph traversal
	#[error("Expected a record ID during recursive graph traversal, but found `{value}`")]
	InvalidRecursionTarget {
		value: String,
	},

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

	#[error(
		"Cannot set field `{name}` with type `{kind}` as it mismatched with field `{existing_name}` with type `{existing_kind}`"
	)]
	MismatchedFieldTypes {
		name: String,
		kind: String,
		existing_name: String,
		existing_kind: String,
	},

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

	/// A `COMPUTED` body is evaluated on every read of the field, inside the
	/// reading statement's transaction, so it must not modify data.
	#[error("Cannot define field `{0}` as `COMPUTED` bodies must be read-only.")]
	ComputedWrite(String),

	/// A field's DEFAULT / VALUE / ASSERT / COMPUTED clauses read each other in
	/// a cycle, so there is no order in which they can all be evaluated
	#[error("Cyclic dependency detected among field clauses: {0}")]
	ComputedFieldCycle(String),

	/// Cannot use the `{0}` keyword on the `id` field
	#[error("Cannot use the `{0}` keyword on the `id` field.")]
	IdFieldKeywordConflict(String),

	/// Cannot use the `{0}` keyword on the `id` field
	#[error("Cannot use the `{0}` type on the `id` field, as that's not a valid record id key.")]
	IdFieldUnsupportedKind(String),

	#[error("Computed fields cannot be indexed. Index: '{index}' - Field: '{field}'")]
	ComputedFieldCannotBeIndexed {
		field: String,
		index: String,
	},
}

impl LeafError for Error {
	fn map_kind(self, message: String) -> TypesError {
		match self {
			// The query named a scope or a parameter it may not use.
			Error::NsEmpty => TypesError::validation(message, ValidationError::NamespaceEmpty),
			Error::DbEmpty => TypesError::validation(message, ValidationError::DatabaseEmpty),
			Error::InvalidParam {
				name,
			} => TypesError::validation(
				message,
				ValidationError::InvalidParameter {
					name,
				},
			),
			Error::AccessUnsupportedAlgorithm => TypesError::validation(message, None),
			// The definition itself is malformed, so the client can act on it.
			// `ComputedFieldSideEffect` is the same fault caught at read time
			// instead of definition time, so it reaches clients the same way;
			// the `ViaFunction`/`RequiredReadOnly` forms are the same rule
			// resolved through the function call graph.
			Error::ComputedWrite(_)
			| Error::ComputedFieldSideEffect
			| Error::ComputedWriteViaFunction {
				..
			}
			| Error::PermissionWriteViaFunction {
				..
			}
			| Error::FunctionRequiredReadOnly {
				..
			}
			| Error::MutablePermissionsDisabled {
				..
			} => TypesError::validation(message, ValidationError::InvalidRequest),

			// A `THROW`, reaching the client verbatim. The only kind in this
			// type the query author chooses.
			Error::Thrown(_) => TypesError::thrown(message),

			// A gap in the engine rather than a fault in the query: there is
			// nothing for a client to correct, so it stays internal.
			Error::Unimplemented(_) => TypesError::internal(message),

			// Reach clients as an untyped internal error only because they always
			// have. Each is a candidate for a real kind, and giving one a kind
			// moves the wire snapshot, so it is a deliberate change and not a
			// tidy-up.
			Error::Query {
				..
			}
			| Error::InvalidFetch {
				..
			}
			| Error::InvalidLimit {
				..
			}
			| Error::InvalidStart {
				..
			}
			| Error::InvalidScript {
				..
			}
			| Error::InvalidModel {
				..
			}
			| Error::InvalidFunction {
				..
			}
			| Error::InvalidControlFlow
			| Error::NsNotAllowed {
				..
			}
			| Error::DbNotAllowed {
				..
			}
			| Error::ComputationDepthExceeded
			| Error::InvalidStatement(_)
			| Error::InvalidStatementTarget {
				..
			}
			| Error::CreateStatement {
				..
			}
			| Error::UpsertStatement {
				..
			}
			| Error::UpdateStatement {
				..
			}
			| Error::RelateStatementIn {
				..
			}
			| Error::RelateStatementId {
				..
			}
			| Error::RelateStatementOut {
				..
			}
			| Error::DeleteStatement {
				..
			}
			| Error::InsertStatement {
				..
			}
			| Error::InsertStatementIn {
				..
			}
			| Error::InsertStatementId {
				..
			}
			| Error::InsertStatementOut {
				..
			}
			| Error::LiveStatement {
				..
			}
			| Error::KillStatement {
				..
			}
			| Error::SingleOnlyOutput
			| Error::ParamPermissions {
				..
			}
			| Error::FunctionPermissions {
				..
			}
			| Error::PermissionPredicateSideEffect
			| Error::PermissionClauseNotReadonly {
				..
			}
			| Error::SetCoerce {
				..
			}
			| Error::ReturnCoerce {
				..
			}
			| Error::PlannerUnsupported(_)
			| Error::PlannerUnimplemented(_)
			| Error::AccessLevelMismatch
			| Error::AccessRecordTokenDurationRequired
			| Error::AccessGrantInvalidSubject
			| Error::AccessGrantRevoked
			| Error::InvalidBound {
				..
			}
			| Error::IdiomRecursionLimitExceeded {
				..
			}
			| Error::UnsupportedRepeatRecurse
			| Error::RecursionInstructionPlanConflict
			| Error::InvalidRecursionTarget {
				..
			}
			| Error::ReferenceTypeConflict(_)
			| Error::ReferenceNestedField(_)
			| Error::MismatchedFieldTypes {
				..
			}
			| Error::ComputedKeywordConflict(_)
			| Error::ComputedNestedFieldConflict(..)
			| Error::ComputedParentFieldConflict(..)
			| Error::ComputedNestedField(_)
			| Error::ComputedFieldCycle(_)
			| Error::IdFieldKeywordConflict(_)
			| Error::IdFieldUnsupportedKind(_)
			| Error::ComputedFieldCannotBeIndexed {
				..
			} => internal_todo(message),
		}
	}
}

/// Raised inside the recursive `compute` path, which signals through
/// [`ControlFlow`](crate::expr::ControlFlow) rather than returning `Err`.
impl From<Error> for crate::expr::ControlFlow {
	fn from(error: Error) -> Self {
		crate::expr::ControlFlow::Err(anyhow::Error::new(error))
	}
}
