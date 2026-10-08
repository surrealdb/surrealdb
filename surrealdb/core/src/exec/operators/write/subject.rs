//! The table and key one `INSERT` row is written at.
//!
//! `INSERT` is the one mutation whose subject is derived from the row's own
//! payload rather than from an expression the statement wrote, so the rules
//! that read a row's `id` field live here rather than beside the targets.

use anyhow::{Result, bail};
use surrealdb_types::ToSql;

use crate::exec::Error as ExecError;
use crate::val::{RecordIdKey, TableName, Value};

/// The table and record-id key one `INSERT` row is written at, from the row's
/// own `id` field and the statement's `INTO` clause.
///
/// With `INTO` the table is the statement's and the row supplies only the key,
/// coerced from whatever the `id` field holds: a float that round-trips
/// exactly to an `i64`, an integer, a non-empty string, an object, an array, a
/// uuid, or the *key* of a record id — whose table is deliberately discarded,
/// which is what lets `INSERT INTO other (SELECT * FROM test)` migrate records
/// across tables while preserving their keys. A row with no `id` field derives
/// one, and any other value is refused.
///
/// Without `INTO` the `id` field must hold a record id, which supplies both
/// halves.
///
/// A pure coercion over the value: it reads the row's `id` field and nothing
/// else, holds no context, and evaluates no expression.
pub(crate) fn extract_table_and_rid_key(
	record: &Value,
	into: &Option<TableName>,
) -> Result<(TableName, Option<RecordIdKey>)> {
	let Some(tb) = into else {
		let record = record.rid();
		let Value::RecordId(rid) = record else {
			bail!(ExecError::InsertStatementId {
				value: record.to_sql(),
			});
		};
		return Ok((rid.table, Some(rid.key)));
	};

	let rid = match record.rid() {
		// There is a floating point number for the id field. Only accept floats
		// that round-trip exactly to an i64 (finite, no fractional part, within
		// i64 range). Silent truncation, NaN, infinity and other lossy
		// conversions are rejected — see `Number::as_int_lossless`.
		Value::Number(id) if id.is_float() => match id.as_int_lossless() {
			Some(i) => Some(RecordIdKey::Number(i)),
			None => bail!(ExecError::InsertStatementId {
				value: Value::Number(id).to_sql(),
			}),
		},
		// There is an integer number for the id field
		Value::Number(id) if id.is_int() => Some(RecordIdKey::Number(id.as_int())),
		// There is a string for the id field
		Value::String(id) if !id.is_empty() => Some(id.into()),
		// There is an object for the id field
		Value::Object(id) => Some(id.into()),
		// There is an array for the id field
		Value::Array(id) => Some(id.into()),
		// There is a UUID for the id field
		Value::Uuid(id) => Some(id.into()),
		// There is a record id defined
		Value::RecordId(id) => {
			// The RID's table is intentionally discarded in favour of the `INTO`
			// clause: this is what lets `INSERT INTO other (SELECT * FROM test)`
			// migrate records from `test:*` to `other:*` while preserving keys.
			Some(id.key)
		}
		// There is no record id field
		Value::None => None,
		// Any other value cannot be converted to a record id key
		v => {
			bail!(ExecError::InsertStatementId {
				value: v.to_sql(),
			});
		}
	};

	Ok((tb.clone(), rid))
}
