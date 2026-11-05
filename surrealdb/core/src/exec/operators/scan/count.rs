//! CountScan operator - optimized COUNT() without materializing records.
//!
//! When a query is `SELECT count() FROM table GROUP ALL` (with no WHERE, SPLIT,
//! or meaningful ORDER BY), this operator replaces the full Scan -> Aggregate
//! pipeline.  Instead of streaming, decoding, and aggregating every record it
//! calls `txn.count(beg..end)` on the KV key range and emits the single row
//! the aggregation would have — see [`CountOutput`].
//!
//! The planner emits this operator only when it can statically determine that
//! the query is eligible.  Permissions are resolved at execution time:
//!
//! - **Allow** – proceed with the key-range count.
//! - **Deny**  – count nothing (the table is invisible), and report that count.
//! - **Conditional** – per-record evaluation is required, so the operator falls back to a full scan
//!   + count at runtime.

use std::borrow::Cow;
use std::ops::Bound;

use crate::catalog::{DatabaseId, NamespaceId};
use crate::exec::ValueBatch;
use crate::expr::ControlFlow;
use crate::key::RawRange;
use crate::key::schema::RecordPrefix;
use crate::val::{Number, Object, RecordIdKeyRange, TableName, Value};

/// What a count scan answers with: the row the aggregation *and the projection
/// above it* would have produced, since a count scan stands in for both.
///
/// A shortcut decides which plan runs, never what the plan answers, so the
/// shape is settled once by the matcher that chose the shortcut and carried to
/// every place a count is emitted — including the one where the count is zero
/// because the table is invisible.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CountOutput {
	/// `SELECT VALUE count() … GROUP ALL` — the count itself. `VALUE` asks for
	/// the value rather than for a row holding it, so no key is named and the
	/// alias a statement may write over it names nothing.
	Value,
	/// `SELECT count() [AS c] … GROUP ALL` — one key per counted field, in the
	/// order the statement wrote them, each holding the same count:
	/// `{ "count": N }`, `{ "c": N }`, `{ "a": N, "b": N }`.
	Fields(Vec<String>),
}

impl CountOutput {
	/// The single-row batch this shape answers `count` with.
	pub(crate) fn batch(&self, count: usize) -> ValueBatch {
		let count = Value::Number(Number::Int(count as i64));
		let value = match self {
			Self::Value => count,
			Self::Fields(names) => {
				let mut obj = Object::default();
				for name in names {
					obj.insert(name.clone(), count.clone());
				}
				Value::Object(obj)
			}
		};
		ValueBatch::new(vec![value])
	}

	#[cfg(test)]
	/// Whether the shape has somewhere to put the count. `VALUE` is that place
	/// itself; a field list a matcher accepted always names at least one field.
	pub(crate) fn is_well_formed(&self) -> bool {
		match self {
			Self::Value => true,
			Self::Fields(names) => !names.is_empty(),
		}
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// The record keys a record-id range covers.
pub(crate) fn record_key_range(
	ns_id: NamespaceId,
	db_id: DatabaseId,
	table: &TableName,
	range: &RecordIdKeyRange,
) -> Result<RawRange, ControlFlow> {
	let start = match &range.start {
		Bound::Unbounded => Bound::Unbounded,
		Bound::Included(v) => Bound::Included(Cow::Borrowed(v)),
		Bound::Excluded(v) => Bound::Excluded(Cow::Borrowed(v)),
	};
	let end = match &range.end {
		Bound::Unbounded => Bound::Unbounded,
		Bound::Included(v) => Bound::Included(Cow::Borrowed(v)),
		Bound::Excluded(v) => Bound::Excluded(Cow::Borrowed(v)),
	};
	Ok(RecordPrefix {
		ns: ns_id,
		db: db_id,
		tb: Cow::Borrowed(table),
	}
	.range_where((start, end))?)
}

#[cfg(test)]
mod tests {
	use super::*;

	/// A count scan stands in for the projection above the aggregation, so the
	/// row it emits is the projected one: a named row, or — under `VALUE` —
	/// the count with nothing wrapped around it.
	#[test]
	fn the_output_shape_decides_what_a_count_is_wrapped_in() {
		let batch = |output: CountOutput| output.batch(7).into_values();

		assert_eq!(batch(CountOutput::Value), vec![Value::from(7)]);
		assert_eq!(
			batch(CountOutput::Fields(vec!["count".to_string()])),
			vec![Value::Object(Object::from(map! { "count".to_string() => Value::from(7) }))]
		);
		assert_eq!(
			batch(CountOutput::Fields(vec!["a".to_string(), "b".to_string()])),
			vec![Value::Object(Object::from(map! {
				"a".to_string() => Value::from(7),
				"b".to_string() => Value::from(7),
			}))]
		);
	}

	/// Only a field list can be empty, and a matcher never builds one: the
	/// debug assertion in each scan's constructor rests on that.
	#[test]
	fn a_shape_with_nowhere_to_put_the_count_is_not_well_formed() {
		assert!(CountOutput::Value.is_well_formed());
		assert!(CountOutput::Fields(vec!["count".to_string()]).is_well_formed());
		assert!(!CountOutput::Fields(Vec::new()).is_well_formed());
	}
}
