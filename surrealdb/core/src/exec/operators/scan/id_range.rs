//! The record-key byte range a table scan bounded by record ids reads.
//!
//! A table scan whose predicate bounds `id` below, above or both reads the stretch of the table's
//! keys between the bounds. [`record_id_ranges`] turns the shape the planner chose
//! ([`ScanIdRange`]) and the values of its bounds, evaluated when the scan opens, into that
//! stretch.
//!
//! # Contract
//!
//! - **A superset.** The range holds every record whose id the bounds admit. A bound the scan
//!   cannot use is dropped, which widens its side. The predicate the bounds were planned from is
//!   judged again over every row the scan yields.
//! - **Never an error.** A bound that is not a record id of the scanned table whose key is a
//!   number, a string or a uuid, the keys whose storage order is the order record ids compare in,
//!   leaves its side open. A range that cannot be encoded is the whole table.
//! - **Within the table, in scan order.** The range is clamped to the table's record range by
//!   construction, and an empty or inverted range is no range at all.

use std::ops::Bound;

use super::key_range::ScanSide;
use super::record_key_range;
use crate::catalog::{DatabaseId, NamespaceId};
use crate::key::RawRange;
use crate::val::{RecordIdKey, RecordIdKeyRange, TableName, Value};

/// The shape of a scan bounded by record ids: which bound value limits which side. Bound values
/// are named by their index into the values the scan evaluates when it opens.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ScanIdRange {
	pub(crate) low: Option<ScanSide>,
	pub(crate) high: Option<ScanSide>,
}

/// `value` as a bound on the keys of `table`, or `Unbounded` where it is no record id of `table`
/// whose key storage orders as record ids compare.
fn side(table: &TableName, held: Option<ScanSide>, bounds: &[Value]) -> Bound<RecordIdKey> {
	let Some(held) = held else {
		return Bound::Unbounded;
	};
	let Some(Value::RecordId(record)) = bounds.get(usize::from(held.bound)) else {
		return Bound::Unbounded;
	};
	if record.table != *table
		|| !matches!(
			record.key,
			RecordIdKey::Number(_) | RecordIdKey::String(_) | RecordIdKey::Uuid(_)
		) {
		return Bound::Unbounded;
	}
	let key = record.key.clone();
	if held.inclusive {
		Bound::Included(key)
	} else {
		Bound::Excluded(key)
	}
}

/// The key ranges of the table `name`, whose record range is `table`, that a scan of shape `spec`
/// reads, given `bounds`, the values of its bounds evaluated when the scan opens (`NONE` for a
/// bound whose evaluation failed).
///
/// See the module docs for the contract. The answer is the one range between the bounds, the
/// table's whole record range when neither side is usable, and none when the bounds admit no key.
pub(crate) fn record_id_ranges(
	ns: NamespaceId,
	db: DatabaseId,
	name: &TableName,
	table: &RawRange,
	spec: ScanIdRange,
	bounds: &[Value],
) -> Vec<RawRange> {
	let start = side(name, spec.low, bounds);
	let end = side(name, spec.high, bounds);
	if matches!((&start, &end), (Bound::Unbounded, Bound::Unbounded)) {
		return vec![table.clone()];
	}
	let Ok(range) = record_key_range(
		ns,
		db,
		name,
		&RecordIdKeyRange {
			start,
			end,
		},
	) else {
		return vec![table.clone()];
	};
	let (low, high) = range.bounds();
	if low >= high {
		return Vec::new();
	}
	vec![range]
}

#[cfg(test)]
mod tests {
	use surrealdb_strand::Strand;

	use super::*;
	use crate::key::schema::RecordPrefix;
	use crate::val::{RecordId, Uuid};

	const NS: NamespaceId = NamespaceId(1);
	const DB: DatabaseId = DatabaseId(2);

	fn name(text: &str) -> TableName {
		TableName::new(Strand::new(text))
	}

	fn table_range(table: &TableName) -> RawRange {
		RecordPrefix {
			ns: NS,
			db: DB,
			tb: std::borrow::Cow::Borrowed(table),
		}
		.range()
		.expect("the table's record range")
	}

	fn id(table: &TableName, key: RecordIdKey) -> Value {
		Value::RecordId(RecordId {
			table: table.clone(),
			key,
		})
	}

	fn spec(low: Option<bool>, high: Option<bool>) -> ScanIdRange {
		ScanIdRange {
			low: low.map(|inclusive| ScanSide {
				bound: 0,
				inclusive,
			}),
			high: high.map(|inclusive| ScanSide {
				bound: 1,
				inclusive,
			}),
		}
	}

	fn ranges(table: &TableName, spec: ScanIdRange, bounds: &[Value]) -> Vec<(Vec<u8>, Vec<u8>)> {
		record_id_ranges(NS, DB, table, &table_range(table), spec, bounds)
			.iter()
			.map(|range| {
				let (low, high) = range.bounds();
				(low.to_vec(), high.to_vec())
			})
			.collect()
	}

	fn encoded(table: &TableName, range: &RecordIdKeyRange) -> (Vec<u8>, Vec<u8>) {
		let range = record_key_range(NS, DB, table, range).expect("an encodable range");
		let (low, high) = range.bounds();
		(low.to_vec(), high.to_vec())
	}

	fn key(n: i64) -> RecordIdKey {
		RecordIdKey::Number(n)
	}

	#[test]
	fn the_range_is_what_the_equivalent_record_id_range_encodes_to() {
		let t = name("t");
		let both = [id(&t, key(2)), id(&t, key(9))];
		for (low, high) in [(true, true), (true, false), (false, true), (false, false)] {
			let bound = |inclusive, n| {
				if inclusive {
					Bound::Included(key(n))
				} else {
					Bound::Excluded(key(n))
				}
			};
			assert_eq!(
				ranges(&t, spec(Some(low), Some(high)), &both),
				vec![encoded(
					&t,
					&RecordIdKeyRange {
						start: bound(low, 2),
						end: bound(high, 9),
					}
				)]
			);
		}
		assert_eq!(
			ranges(&t, spec(Some(false), None), &both),
			vec![encoded(
				&t,
				&RecordIdKeyRange {
					start: Bound::Excluded(key(2)),
					end: Bound::Unbounded,
				}
			)]
		);
		assert_eq!(
			ranges(&t, spec(None, Some(true)), &both),
			vec![encoded(
				&t,
				&RecordIdKeyRange {
					start: Bound::Unbounded,
					end: Bound::Included(key(9)),
				}
			)]
		);
	}

	#[test]
	fn string_and_uuid_keys_bound_a_side() {
		let t = name("t");
		let strings = [id(&t, RecordIdKey::String(Strand::new("a"))), Value::None];
		assert_eq!(
			ranges(&t, spec(Some(true), None), &strings),
			vec![encoded(
				&t,
				&RecordIdKeyRange {
					start: Bound::Included(RecordIdKey::String(Strand::new("a"))),
					end: Bound::Unbounded,
				}
			)]
		);
		let uuid = Uuid(uuid::Uuid::nil());
		let uuids = [id(&t, RecordIdKey::Uuid(uuid)), Value::None];
		assert_eq!(
			ranges(&t, spec(Some(false), None), &uuids),
			vec![encoded(
				&t,
				&RecordIdKeyRange {
					start: Bound::Excluded(RecordIdKey::Uuid(uuid)),
					end: Bound::Unbounded,
				}
			)]
		);
	}

	#[test]
	fn a_value_that_bounds_nothing_widens_its_side() {
		let t = name("t");
		let whole = vec![table_range(&t).bounds()]
			.into_iter()
			.map(|(low, high)| (low.to_vec(), high.to_vec()))
			.collect::<Vec<_>>();
		let wrong_table = id(&name("u"), key(1));
		let array_key = id(&t, RecordIdKey::Array(crate::val::Array::default()));
		let object_key = id(&t, RecordIdKey::Object(crate::val::Object::default()));
		for value in [
			wrong_table,
			array_key,
			object_key,
			Value::None,
			Value::Null,
			Value::from(5i64),
			Value::from("t:1"),
		] {
			let bounds = [value.clone(), value];
			assert_eq!(ranges(&t, spec(Some(true), Some(true)), &bounds), whole);
			assert_eq!(ranges(&t, spec(Some(false), None), &bounds), whole);
		}
		// A usable side stands when the other is dropped.
		let bounds = [id(&t, key(2)), Value::None];
		assert_eq!(
			ranges(&t, spec(Some(true), Some(true)), &bounds),
			ranges(&t, spec(Some(true), None), &bounds)
		);
	}

	#[test]
	fn an_empty_or_inverted_range_is_no_range() {
		let t = name("t");
		let inverted = [id(&t, key(5)), id(&t, key(2))];
		assert!(ranges(&t, spec(Some(false), Some(false)), &inverted).is_empty());
		let same = [id(&t, key(5)), id(&t, key(5))];
		assert!(ranges(&t, spec(Some(false), Some(false)), &same).is_empty());
		assert!(ranges(&t, spec(Some(true), Some(false)), &same).is_empty());
		assert!(ranges(&t, spec(Some(false), Some(true)), &same).is_empty());
		assert_eq!(ranges(&t, spec(Some(true), Some(true)), &same).len(), 1);
	}

	#[test]
	fn a_range_lies_within_the_table() {
		let t = name("t");
		let (table_low, table_high) = {
			let range = table_range(&t);
			let (low, high) = range.bounds();
			(low.to_vec(), high.to_vec())
		};
		for n in [i64::MIN, -1, 0, 1, i64::MAX] {
			let bounds = [id(&t, key(n)), id(&t, key(n))];
			for (low, high) in [(Some(true), None), (None, Some(true)), (Some(false), Some(true))] {
				for (start, end) in ranges(&t, spec(low, high), &bounds) {
					assert!(start >= table_low && end <= table_high);
				}
			}
		}
	}
}
