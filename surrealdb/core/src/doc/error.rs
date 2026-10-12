//! Failures raised while validating and writing a single document.
//!
//! These are the record-shaped failures of the document pipeline: the record
//! already exists or an edge endpoint does not, a field does not satisfy the
//! schema its table declares, a reference refuses a delete, or a queued async
//! event no longer matches the database it was recorded against.

// The mapper below is the only place this layer's failures become public.
// A new variant must make that decision explicitly rather than inheriting
// whatever the last arm happened to be.
#![deny(clippy::wildcard_enum_match_arm)]

use common::{LeafError, internal_todo};
use surrealdb_types::{
	AlreadyExistsError, Error as TypesError, NotFoundError, ToSql, ValidationError,
};

use crate::expr::Idiom;
use crate::val::{CoerceError, RecordId};

/// A failure in the document layer.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
	/// A database entry for the specified record already exists
	#[error("Database record `{record}` already exists", record = record.to_sql())]
	RecordExists {
		record: RecordId,
	},

	/// The requested record does not exist
	#[error("The record '{rid}' does not exist")]
	IdNotFound {
		rid: String,
	},

	/// Found a record id for the record but we are creating a specific record
	#[error("Found {value} for the `id` field, but a specific record has been specified")]
	IdMismatch {
		value: String,
	},

	/// A record id could not be auto-generated for the table's declared `id` type
	#[error(
		"Cannot generate a record id of type `{kind}` for the `{table}` table; specify an explicit record id, or declare the `id` field as `uuid` or `string` to auto-generate one."
	)]
	IdFieldGenerateUnsupported {
		table: String,
		kind: String,
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

	/// The specified table is not configured for the type of record being added
	#[error("Found record: `{record}` which is {}a relation, but expected a {target_type}", if *relation { "" } else { "not " })]
	TableCheck {
		record: String,
		relation: bool,
		target_type: String,
	},

	/// The specified table is a view (`DEFINE TABLE ... AS SELECT`) and is read-only
	#[error(
		"Cannot write to the `{table}` table, as it is a view (defined with `AS SELECT`); view tables are read-only and their records are computed from the source query"
	)]
	TableIsView {
		table: String,
	},

	/// The specified field did not conform to the field ASSERT clause
	#[error(
		"Found {value} for field `{field}`, with record `{record}`, but field must conform to: {check}",
		field = field.to_sql()
	)]
	FieldValue {
		record: String,
		value: String,
		field: Idiom,
		check: String,
	},

	/// The specified field did not conform to the field ASSERT clause
	#[error(
		"Found changed value for field `{field}`, with record `{record}`, but field is readonly",
		field = field.to_sql()
	)]
	FieldReadonly {
		record: String,
		field: Idiom,
	},

	/// The specified field on a SCHEMAFUL table was not defined
	#[error("Found field '{field}', but no such field exists for table '{table}'", field = field.to_sql())]
	FieldUndefined {
		table: String,
		field: Idiom,
	},

	/// The table's field clauses read each other in a cycle, so there is no
	/// order in which they can all be evaluated
	#[error(
		"Cannot write to table '{table}': the DEFAULT, VALUE, ASSERT or COMPUTED clauses of the fields {fields} depend on each other in a cycle"
	)]
	FieldDependencyCycle {
		table: String,
		fields: String,
	},

	/// The specified value did not conform to the LET type check
	#[error("Couldn't coerce value for field `{field_name}` of `{record}`: {error}")]
	FieldCoerce {
		record: String,
		field_name: String,
		error: Box<CoerceError>,
	},

	/// A `PATCH` pointer reads a field the caller may not select
	#[error(
		"Cannot read `{0}` in a JSON Patch operation: the field's PERMISSIONS FOR select clause does not allow it"
	)]
	PatchPointerNotAllowed(String),

	/// The record cannot be deleted as it's still referenced elsewhere
	#[error("Cannot delete `{0}` as it is referenced by `{1}` with an ON DELETE REJECT clause")]
	DeleteRejectedByReference(String, String),

	/// Something went wrong while updating references
	#[error("An error occurred while updating references for `{0}`: {1}")]
	RefsUpdateFailure(String, String),

	#[error(
		"Error with the event {0}. The ID of the namespace `{1}` does not match the namespace this event has been generated from."
	)]
	EvNamespaceMismatch(String, String),

	#[error(
		"Error with the event {0}. The ID of the database `{1}` does not match the database this event has been generated from."
	)]
	EvDatabaseMismatch(String, String),

	#[error("The event {0} reached the max async event nesting depth: {1}.")]
	EvReachMaxDepth(String, u16),

	/// A record was created in a table whose record id names its components,
	/// and neither the record nor the statement nor the `id` field's `DEFAULT`
	/// supplied one of them
	#[error(
		"Cannot create a record in `{table}` without the record id component `{component}`: set `{component}`, give the record id, or define a DEFAULT on the `id` field"
	)]
	IdComponentMissing {
		table: String,
		component: String,
	},

	/// A write named a field that is a component of the record id, with a value
	/// other than the record id's. `value` is what the caller wrote, and nothing
	/// of the record's own key.
	#[error(
		"Found {value} for the `{component}` field, but it is part of the record id of `{table}` and does not match it"
	)]
	IdComponentMismatch {
		table: String,
		component: String,
		value: String,
	},

	/// An element of the record id is not a value its component can hold.
	/// `error` says why, in the words of the coercion or of the component's
	/// own conformance check.
	#[error(
		"Couldn't coerce value for the record id component `{component}` of `{table}`: {error}"
	)]
	IdComponentCoerce {
		table: String,
		component: String,
		error: String,
	},

	/// The record id of a table whose `id` names its components has another
	/// number of elements. `expected` lists the component names.
	#[error(
		"The record id of `{table}` has the components {expected}, but {found} {} given",
		if *found == 1 { "value was" } else { "values were" }
	)]
	IdComponentArity {
		table: String,
		expected: String,
		found: usize,
	},
}

impl Error {
	/// Check if this error is related to schema checks.
	///
	/// UPSERT's create-then-update fallback treats a schema
	/// failure on the create attempt as recoverable when the statement is
	/// repeatable: it carries the error, retries the document as an update, and
	/// surfaces the carried error only if the update fails too. Any other
	/// failure rolls the create attempt back instead.
	pub(crate) fn is_schema_related(&self) -> bool {
		matches!(
			self,
			Error::FieldCoerce { .. }
				| Error::FieldValue { .. }
				| Error::FieldReadonly { .. }
				| Error::FieldUndefined { .. }
		)
	}
}

impl LeafError for Error {
	fn map_kind(self, message: String) -> TypesError {
		match self {
			Error::RecordExists {
				record,
			} => TypesError::already_exists(
				message,
				AlreadyExistsError::Record {
					id: record.to_sql(),
				},
			),
			// The table's own definitions are mutually inconsistent, so no
			// record can be written until the schema is changed
			Error::FieldDependencyCycle {
				..
			} => TypesError::configuration(message, None),
			Error::IdNotFound {
				rid,
			} => TypesError::not_found(
				message,
				NotFoundError::Record {
					id: rid,
				},
			),
			// A denied read, so it is reported as a permission failure rather
			// than a schema or internal one.
			Error::PatchPointerNotAllowed(..) => TypesError::not_allowed(message, None),
			Error::IdMismatch {
				..
			}
			| Error::IdFieldGenerateUnsupported {
				..
			}
			| Error::InOverride {
				..
			}
			| Error::OutOverride {
				..
			}
			| Error::TableCheck {
				..
			}
			| Error::TableIsView {
				..
			}
			| Error::FieldValue {
				..
			}
			| Error::FieldReadonly {
				..
			}
			| Error::FieldUndefined {
				..
			}
			| Error::FieldCoerce {
				..
			}
			| Error::DeleteRejectedByReference(..)
			| Error::RefsUpdateFailure(..)
			| Error::EvNamespaceMismatch(..)
			| Error::EvDatabaseMismatch(..)
			| Error::EvReachMaxDepth(..) => internal_todo(message),
			// A write that gives a record in a table with named record id
			// components an id that does not fit them, or restates one of them
			// differently. The author of the statement can correct it, so it is
			// a validation fault and not a schema one: `is_schema_related` does
			// not list these, so `UPSERT` never retries one as an update.
			Error::IdComponentMissing {
				..
			}
			| Error::IdComponentMismatch {
				..
			}
			| Error::IdComponentCoerce {
				..
			}
			| Error::IdComponentArity {
				..
			} => TypesError::validation(message, ValidationError::InvalidRequest),
		}
	}
}
