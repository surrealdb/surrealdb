//! Failures raised while executing a statement.
//!
//! This is the vocabulary of running a query: which statement forms are legal
//! where, whether a clause evaluated to something usable, which permission
//! predicate said no, how deep evaluation may recurse. Both executors - the
//! recursive `compute` path and the planner - speak it, which is why
//! it is one type rather than one per executor.
//!
//! It deliberately names nothing about values, the catalog, keys, indexes,
//! documents, buckets or storage: those layers own their own failures.

// The mapper below is the only place this layer's failures become public.
// A new variant must make that decision explicitly rather than inheriting
// whatever the last arm happened to be.
#![deny(clippy::wildcard_enum_match_arm)]

use common::{LeafError, internal_todo};
use surrealdb_types::{Error as TypesError, ValidationError};

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

	/// A KNN query vector no distance ranks a record against: one that holds
	/// no number, or a NaN or an infinity. `vector` is the vector as written.
	#[error(
		"Invalid query: The KNN query vector must be a non-empty array of finite numbers: {vector}"
	)]
	InvalidKnnVector {
		vector: String,
	},

	/// A KNN query vector known when the statement is planned that does not
	/// coerce to `array<number>`: `message` is the coercion's error, which a
	/// vector computed when the statement runs raises as it is evaluated.
	#[error("{message}")]
	UncoercibleKnnVector {
		message: String,
	},

	/// it is not possible to set a variable with the specified name
	#[error("'{name}' is a protected variable and cannot be set")]
	InvalidParam {
		name: String,
	},

	/// The FETCH clause accepts idioms, strings and fields.
	#[error("Found {value} on FETCH CLAUSE, but FETCH expects an idiom, a string or fields")]
	InvalidFetch {
		value: String,
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

	/// What an aggregated view's storage holds for one of its groups is not
	/// what the view's maintenance writes.
	///
	/// A group is a row and a state, and a row in the current record format
	/// carries no statistics, so a row without its state cannot be read as an
	/// empty group: the write is refused rather than starting the group over.
	#[error("The view `{table}` is corrupt: {reason}")]
	CorruptView {
		table: String,
		reason: &'static str,
	},

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

	/// A schema clause that must not have effects holds or reaches one.
	///
	/// The clauses are `COMPUTED`, every `PERMISSIONS` clause, `ASSERT`,
	/// `VALUE` and `DEFAULT`; the effects are a write, a schema statement, a
	/// builtin that advances state outside the transaction, `SLEEP`, and a call
	/// whose effects cannot be known when the clause is defined. `clause` names
	/// the clause and `construct` the offending construct and what it does.
	///
	/// Raised where the clause is defined, and where a clause stored before
	/// the rule reaches the effect while it runs (`NoWriteFrame`).
	#[error("The {clause} clause must not have effects: {construct}")]
	SchemaClauseEffect {
		clause: String,
		construct: String,
	},

	/// A Surrealism module function that declared read-only access reached a
	/// data-modifying statement.
	///
	/// The guest builds its query as a string at runtime, so however the guest
	/// language spells the declaration, it cannot prevent this by itself. The
	/// declaration is what the host acts on, and this is where the mismatch is
	/// caught.
	#[error(
		"A read-only Surrealism function cannot modify data; \
		 the function must declare that it requires a writeable transaction"
	)]
	SurrealismReadOnlySideEffect,

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

	/// The engine does not run this statement or expression. The statement
	/// fails with the reason, which names the construct; nothing else
	/// executes it.
	#[error("The engine cannot run this statement: {reason}")]
	PlannerUnsupported {
		reason: String,
	},

	/// The access method cannot be defined on the requested level
	#[error("The access method cannot be defined on the requested level")]
	AccessLevelMismatch,

	#[error(
		"The ES512 algorithm is not currently supported. Please use ES384 or another supported algorithm"
	)]
	AccessUnsupportedAlgorithm,

	/// The access names the redaction placeholder as its key.
	///
	/// Every surface that renders an access substitutes that placeholder for
	/// the real key, export included, so the text of a rendered access is not
	/// a definition that can be replayed — defining from it would sign tokens
	/// with a published string.
	#[error(
		"The access method '{ac}' names the redaction placeholder as its key, which is published wherever an access is rendered; restore it from a definition carrying the real key, or define it again"
	)]
	AccessRedactedKey {
		ac: String,
	},

	#[error(
		"Tokens issued by record access methods can be consumed by third parties and must have an expiration; DURATION FOR TOKEN cannot be NONE on TYPE RECORD access"
	)]
	AccessRecordTokenDurationRequired,

	#[error(
		"A CONTEXT clause is evaluated on every authentication and its result is exposed to the client; it must be read-only"
	)]
	AccessContextNotReadOnly,

	#[error(
		"A CONTEXT clause is evaluated with the authenticated record's own permissions, so it can only be defined on an access method that authenticates a record"
	)]
	AccessContextRequiresRecord,

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

	/// Encountered a non-record-id value during recursive traversal
	#[error("Expected a record ID during recursive traversal, but found `{value}`")]
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

	/// A nested field is held by a field defined in its own right.
	#[error(
		"Cannot define field `{name}`: the field `{parent}` it is nested in is not defined. Define `{parent}` first"
	)]
	FieldParentUndefined {
		name: String,
		parent: String,
	},

	/// A nested field is held by a field whose kind can hold it.
	#[error("Cannot define field `{name}` in field `{parent}` of type `{kind}`: {rule}")]
	FieldParentCannotHold {
		name: String,
		parent: String,
		kind: String,
		rule: String,
	},

	/// A field's kind holds every field already defined in it.
	#[error(
		"Cannot give field `{name}` the type `{kind}`: field `{child}` is defined in it, and {rule}"
	)]
	FieldKindCannotHoldChild {
		name: String,
		kind: String,
		child: String,
		rule: String,
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

	/// A change to the value a field derives (`COMPUTED` defined, redefined or
	/// removed, or its kind changed) that an aggregated view over the table
	/// depends on, by spelling the field or a `COMPUTED` field that reads it,
	/// while the table holds records the view has already grouped by the value
	/// derived before the change.
	#[error(
		"Cannot change the value the computed field `{field}` of `{table}` derives: the aggregated view `{view}` depends on it, and `{table}` holds records the view has already grouped by the value derived before. Remove `{view}`, change the field, then define `{view}` again"
	)]
	ComputedFieldReadByView {
		field: String,
		table: String,
		view: String,
	},

	/// As [`Self::ComputedFieldReadByView`], for a view that reads the record by
	/// names it computes, or hands the record to a body that may read any of
	/// its fields, or spells a `COMPUTED` field that does, so it may depend on
	/// the field.
	#[error(
		"Cannot change the value the computed field `{field}` of `{table}` derives: the aggregated view `{view}` may depend on it, through a read of fields of the record by names that cannot be determined, and `{table}` holds records the view has already grouped by the value derived before. Remove `{view}`, change the field, then define `{view}` again"
	)]
	ComputedFieldReadByViewUnresolved {
		field: String,
		table: String,
		view: String,
	},

	/// A component of a named record id has a type whose stored order a key
	/// range cannot use.
	#[error(
		"The record id component `{component}` of `{table}` cannot have the type `{kind}`: only string, int, datetime, uuid, duration, bool and record components keep their order in storage"
	)]
	IdComponentUnsupportedKind {
		table: String,
		component: String,
		kind: String,
	},

	/// A `DEFINE EXTENSION` outside an import that states neither how its
	/// code is trusted (`SIGNER`) nor that it is not (`UNSIGNED`). An import
	/// stores such a definition as a legacy row.
	#[error("DEFINE EXTENSION needs SIGNER {{…}} or UNSIGNED")]
	ExtensionNeedsTrust,

	/// A `DEFINE BUCKET` outside an import that names no `BACKEND`. An import
	/// stores such a database bucket with none, and it is served from the
	/// global bucket under its namespace, database and name.
	#[error("DEFINE BUCKET needs a BACKEND, such as BACKEND \"global://\" for the global bucket")]
	BucketNeedsBackend,

	/// A component of a named record id is given a name that is not a plain
	/// identifier (`KeyStructure::is_plain_name`): the fields of a record are
	/// keyed by their raw names, which escape any other name.
	#[error(
		"Cannot name the record id component `{component}` of `{table}`: a component name must be a plain identifier of ASCII letters, digits and underscores that does not start with a digit and is not NaN or Infinity"
	)]
	IdComponentNameUnsupported {
		table: String,
		component: String,
	},

	/// The record id of a relation table is its `in`, its `out` and its own
	/// key, so it has no components to name.
	#[error("Cannot name the parts of the record id of `{0}`: it is a relation table")]
	IdComponentsOnRelation(String),

	/// The record id of a view is derived from the view's query.
	#[error("Cannot name the parts of the record id of `{0}`: it is a view")]
	IdComponentsOnView(String),

	/// A table cannot become `TYPE RELATION` while it holds a record that is no
	/// edge: no write could have stored it there, and an export of the table
	/// would write it back as a record the restored table refuses.
	#[error(
		"Cannot make `{table}` TYPE RELATION: it holds {record}, which is not an edge. Delete the record first, or define the table TYPE ANY"
	)]
	TableTypeRelationOverRecord {
		table: String,
		record: String,
	},

	/// A table cannot become `TYPE NORMAL` while it holds an edge: no write
	/// could have stored it there, and an export of the table would write it
	/// back as an edge the restored table refuses.
	#[error(
		"Cannot make `{table}` TYPE NORMAL: it holds the edge {record}. Delete the edge first, or define the table TYPE ANY"
	)]
	TableTypeNormalOverEdge {
		table: String,
		record: String,
	},

	/// A table whose record id names its components cannot become a relation
	/// table or a view. `to` reads "a relation table" or "a view".
	#[error("Cannot make `{table}` {to}: its record id has named components")]
	IdComponentsTableConversion {
		table: String,
		to: String,
	},

	/// A component of a named record id is readable as a field of the table, so
	/// the `id` field takes no permission of its own on `SELECT`.
	#[error(
		"Cannot restrict SELECT on the `id` field of `{0}`: its record id components are readable as fields"
	)]
	IdFieldSelectPermission(String),

	/// A field was defined under the name of a component of the record id.
	#[error(
		"Cannot define the field `{name}` on `{table}`: `{name}` is part of the record id of `{table}`"
	)]
	IdComponentClash {
		name: String,
		table: String,
	},

	/// The record id was given a component name that a field of the table
	/// already holds.
	#[error(
		"Cannot name `{name}` in the record id of `{table}`: `{name}` is already a field of `{table}`"
	)]
	IdComponentShadowsField {
		name: String,
		table: String,
	},

	/// A component name is not a field with a definition of its own, so it can
	/// be neither removed nor altered.
	#[error(
		"`{name}` is part of the record id of `{table}`; change the TYPE of `id` to rename or remove it"
	)]
	IdComponentNotAField {
		name: String,
		table: String,
	},

	/// An existing record has an id the named record id does not admit. `ty` is
	/// the type as written and `reason` says what is wrong with the record id.
	#[error(
		"Cannot give `{table}` the record id {ty}: the existing record {record} does not match it ({reason}). Copy the records into a table with the new record id instead"
	)]
	IdComponentsNonConforming {
		table: String,
		ty: String,
		record: String,
		reason: String,
	},

	/// An existing record stores a field under the name of a component, with a
	/// value other than the component's.
	#[error(
		"Cannot give `{table}` the record id {ty}: the record {record} stores a `{component}` field that differs from its record id. Remove it first, for example with UPDATE {table} UNSET {component}"
	)]
	IdComponentsBodyConflict {
		table: String,
		ty: String,
		record: String,
		component: String,
	},

	/// A name given to a component is read as a stored field by `what` (an
	/// index or a view), whose meaning the name would change.
	#[error(
		"Cannot name `{component}` in the record id of `{table}`: the {what} `{name}` already reads a stored `{component}` field. Remove it, then define it again after changing the record id"
	)]
	IdComponentReadByExisting {
		table: String,
		component: String,
		what: String,
		name: String,
	},

	/// A component name is read by `what` (an index, a view, an event, a field
	/// or a table permission), so the record id cannot drop or rename it.
	#[error(
		"Cannot change the record id of `{table}`: the {what} `{name}` reads its component `{component}`"
	)]
	IdComponentInUse {
		table: String,
		component: String,
		what: String,
		name: String,
	},

	/// `what` (an index, a view, an event, a field or a table permission)
	/// reads the record by names it computes, or hands the record to a body
	/// that may read any of its fields, so it may read the component
	/// `component` the record id would drop or rename.
	#[error(
		"Cannot change the record id of `{table}`: the {what} `{name}` reads fields of the record by names that cannot be determined, so it may read its component `{component}`"
	)]
	IdComponentReadUnresolved {
		table: String,
		component: String,
		what: String,
		name: String,
	},

	/// `what` (an index or a view) reads the record by names it computes, or
	/// hands the record to a body that may read any of its fields, so it may
	/// already read a stored field under the name a component would take.
	#[error(
		"Cannot name `{component}` in the record id of `{table}`: the {what} `{name}` reads fields of the record by names that cannot be determined, so it may already read a stored `{component}` field. Remove it, then define it again after changing the record id"
	)]
	IdComponentReadByExistingUnresolved {
		table: String,
		component: String,
		what: String,
		name: String,
	},

	/// A view over the table selects every field and holds rows, which carry
	/// the components of the record ids they were written under by name; a
	/// change of the names would not rewrite them.
	#[error(
		"Cannot change the record id of `{table}`: the view `{view}` selects every field of `{table}` and holds rows written under the old record id. Remove the view, then define it again after changing the record id"
	)]
	IdComponentsUnderWildcardView {
		table: String,
		view: String,
	},

	/// The table holds records whose keys are stored in an order other than
	/// their record ids', which only the record id structure accounts for, and
	/// the statement would leave the table with no structure. A scan of such a
	/// table is read as delivering `ORDER BY id`, which these keys do not.
	#[error(
		"Cannot remove the names from the record id of `{table}`: it holds {first} and {second}, whose keys are stored in the opposite order to their record ids, which only the names account for. Delete those records, or copy them into a table with the record id you want, first"
	)]
	IdComponentsOutOfOrderWithoutNames {
		table: String,
		first: String,
		second: String,
	},

	/// A write's maintenance reached the view `view` while a row of `view` was
	/// already in flight beneath it: the view reads itself, directly or through
	/// the views of `through`, each of which reads the next and the last of
	/// which reads `view`. Each row maintaining it writes would reach it again,
	/// so the write fails rather than recursing.
	#[error("Cannot maintain the view `{view}`: it reads itself{}", reads_through(.through))]
	ViewReadsItself {
		view: String,
		through: Vec<String>,
	},
}

/// The tail of [`Error::ViewReadsItself`]'s message: nothing for a view that
/// reads itself directly, and otherwise the views it reads itself through, in
/// the order each reads the next.
fn reads_through(through: &[String]) -> String {
	let mut out = String::new();
	for (n, view) in through.iter().enumerate() {
		out.push_str(match n {
			0 => " through `",
			_ => ", which reads `",
		});
		out.push_str(view);
		out.push('`');
	}
	out
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
			// The statement carries a key that cannot be a real one, which the
			// caller can see and correct in the text they sent.
			Error::AccessRedactedKey {
				..
			} => TypesError::validation(message, None),
			// The query names its own vector, which the caller can correct.
			Error::InvalidKnnVector {
				..
			}
			| Error::UncoercibleKnnVector {
				..
			} => TypesError::validation(message, None),
			// The definition itself is malformed, so the client can act on it,
			// whether the fault is caught where the clause is defined or where a
			// clause stored earlier runs. `SurrealismReadOnlySideEffect` is
			// likewise the module author's to fix: the function's declared mode
			// and its query disagree.
			Error::SchemaClauseEffect {
				..
			}
			| Error::SurrealismReadOnlySideEffect => {
				TypesError::validation(message, ValidationError::InvalidRequest)
			}
			// A `CONTEXT` clause the access method may not carry: the author of
			// the DEFINE/ALTER can correct it, so it is a validation fault
			// rather than an engine gap.
			Error::AccessContextNotReadOnly | Error::AccessContextRequiresRecord => {
				TypesError::validation(message, ValidationError::InvalidRequest)
			}
			// A field definition that would nest a field in something that
			// cannot hold it: the author of the statement corrects the schema.
			Error::FieldParentUndefined {
				..
			}
			| Error::FieldParentCannotHold {
				..
			}
			| Error::FieldKindCannotHoldChild {
				..
			} => TypesError::validation(message, ValidationError::InvalidRequest),
			// A table type the records the table holds do not admit: the author
			// of the statement deletes the records or chooses a type that admits
			// them.
			Error::TableTypeRelationOverRecord {
				..
			}
			| Error::TableTypeNormalOverEdge {
				..
			} => TypesError::validation(message, ValidationError::InvalidRequest),
			// A record id whose components are named cannot take the definition:
			// the DEFINE/ALTER/REMOVE or the existing data conflicts with the
			// names, and the author of the statement can correct either.
			Error::IdComponentUnsupportedKind {
				..
			}
			| Error::ExtensionNeedsTrust
			| Error::BucketNeedsBackend
			| Error::IdComponentNameUnsupported {
				..
			}
			| Error::IdComponentsOnRelation(_)
			| Error::IdComponentsOnView(_)
			| Error::IdComponentsTableConversion {
				..
			}
			| Error::IdFieldSelectPermission(_)
			| Error::IdComponentClash {
				..
			}
			| Error::IdComponentShadowsField {
				..
			}
			| Error::IdComponentNotAField {
				..
			}
			| Error::IdComponentsNonConforming {
				..
			}
			| Error::IdComponentsBodyConflict {
				..
			}
			| Error::IdComponentReadByExisting {
				..
			}
			| Error::IdComponentInUse {
				..
			}
			| Error::IdComponentReadUnresolved {
				..
			}
			| Error::IdComponentReadByExistingUnresolved {
				..
			}
			| Error::IdComponentsUnderWildcardView {
				..
			}
			| Error::IdComponentsOutOfOrderWithoutNames {
				..
			} => TypesError::validation(message, ValidationError::InvalidRequest),
			// A view that reads itself is a definition no write can maintain,
			// which its author can remove or define again.
			Error::ViewReadsItself {
				..
			} => TypesError::validation(message, ValidationError::InvalidRequest),

			// The statement would change what a computed field derives under an
			// aggregated view that holds groups keyed by the old values: the
			// author of the statement removes the view first.
			Error::ComputedFieldReadByView {
				..
			}
			| Error::ComputedFieldReadByViewUnresolved {
				..
			} => TypesError::validation(message, ValidationError::InvalidRequest),

			// A `THROW`, reaching the client verbatim. The only kind in this
			// type the query author chooses.
			Error::Thrown(_) => TypesError::thrown(message),

			// A gap in the engine rather than a fault in the query: there is
			// nothing for a client to correct, so it stays internal.
			Error::Unimplemented(_) => TypesError::internal(message),
			// Storage that is not what the maintenance wrote cannot be read as a group.
			Error::CorruptView {
				..
			} => TypesError::serialization(message, None),

			// A cross-tenant namespace/database refusal: the authenticated auth
			// level is not permitted to reach the selected ns/db. This is an
			// authorization denial, so it reaches clients as `NotAllowed` rather
			// than as an internal error.
			Error::NsNotAllowed {
				..
			}
			| Error::DbNotAllowed {
				..
			} => TypesError::not_allowed(message, None),

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
			| Error::SetCoerce {
				..
			}
			| Error::ReturnCoerce {
				..
			}
			| Error::PlannerUnsupported {
				..
			}
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
