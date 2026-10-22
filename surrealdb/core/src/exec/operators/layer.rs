//! Helpers for the rows of a projection layer.
//!
//! A projection layer is a `Project` of the form `*, e₁ AS n₁, …` whose every
//! column is evaluated in a frame that is not the caller's
//! (`LogicalOp::is_layer`). The analyzer writes a table's `COMPUTED` fields
//! and its field `SELECT` permissions as a stack of such layers directly above
//! the table's `PermissionFilter`; the VM lowers the stack to instructions
//! that set each column on the row under the field's own name.

use surrealdb_types::ToSql;

use crate::val::Value;

/// The record a coercion error names: the row's id, or the row itself when
/// it has none.
pub(crate) fn record_of(row: &Value) -> String {
	if let Value::Object(obj) = row
		&& let Some(Value::RecordId(rid)) = obj.get("id")
	{
		return rid.to_sql();
	}
	row.to_sql()
}
