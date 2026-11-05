//! Reference scanning operator for the engine.
//!
//! This operator scans record references (the `<~` operator) to find records
//! that reference a given source record. Unlike graph edges which are explicit
//! relationships, references are field-level links tracked by the database.

use std::borrow::Cow;
use std::ops::Bound;

use crate::catalog::{DatabaseId, NamespaceId};
use crate::expr::ControlFlow;
use crate::key::TypedRange;
use crate::key::schema::{
	DbRoot, ReferenceForeignFieldPrefix, ReferenceForeignTablePrefix, ReferenceIdPrefix,
};
use crate::val::{RecordId, RecordIdKey, TableName};

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/// [`compute_ref_key_range`] over evaluated bounds `start` and `end` on the
/// referencing record's key.
pub(crate) fn ref_key_range(
	ns_id: NamespaceId,
	db_id: DatabaseId,
	rid: &RecordId,
	referencing_table: Option<&TableName>,
	referencing_field: Option<&str>,
	start: Bound<RecordIdKey>,
	end: Bound<RecordIdKey>,
) -> Result<TypedRange<()>, ControlFlow> {
	let has_range = !matches!(start, Bound::Unbounded) || !matches!(end, Bound::Unbounded);

	let prefix = DbRoot {
		ns: ns_id,
		db: db_id,
	};

	if has_range {
		// Range-bounded scan requires both table and field
		let table = referencing_table.ok_or_else(|| {
			anyhow::anyhow!("Range-bounded reference scans require a referencing table")
		})?;
		let field = referencing_field.ok_or_else(|| {
			anyhow::anyhow!(
				"Cannot scan a specific range of record references without a referencing field"
			)
		})?;

		// The bounds are on the referencing record's key, which is the field the
		// reference key carries after the referring field.
		let owned = |bound: Bound<RecordIdKey>| match bound {
			Bound::Included(key) => Bound::Included(Cow::Owned(key)),
			Bound::Excluded(key) => Bound::Excluded(Cow::Owned(key)),
			Bound::Unbounded => Bound::Unbounded,
		};
		let (start, end) = (owned(start), owned(end));

		Ok(ReferenceForeignFieldPrefix {
			ns: prefix.ns,
			db: prefix.db,
			tb: Cow::Borrowed(&rid.table),
			id: Cow::Borrowed(&rid.key),
			foreign_table: Cow::Borrowed(table),
			foreign_field: Cow::Borrowed(field),
		}
		.range_where((start, end))?)
	} else if let Some(table) = referencing_table {
		if let Some(field) = referencing_field {
			Ok(ReferenceForeignFieldPrefix {
				ns: prefix.ns,
				db: prefix.db,
				tb: Cow::Borrowed(&rid.table),
				id: Cow::Borrowed(&rid.key),
				foreign_table: Cow::Borrowed(table),
				foreign_field: Cow::Borrowed(field),
			}
			.range()?)
		} else {
			Ok(ReferenceForeignTablePrefix {
				ns: prefix.ns,
				db: prefix.db,
				tb: Cow::Borrowed(&rid.table),
				id: Cow::Borrowed(&rid.key),
				foreign_table: Cow::Borrowed(table),
			}
			.range()?)
		}
	} else {
		Ok(ReferenceIdPrefix {
			ns: prefix.ns,
			db: prefix.db,
			tb: Cow::Borrowed(&rid.table),
			id: Cow::Borrowed(&rid.key),
		}
		.range()?)
	}
}
