//! Index columns that read a component of the table's record id.
//!
//! A table whose `id` names its components (`DEFINE FIELD id ON reading TYPE
//! [city: string, time: datetime]`) can index a component by its name, as it
//! indexes a field: `DEFINE INDEX by_time ON reading FIELDS time`. A component
//! has no stored value, so what an index holds for it is read off the record
//! id, and two things differ from an index over a stored field.
//!
//! # Evaluation
//!
//! A column that addresses a component is evaluated as `id[i]`, the element of
//! the image's own id ([`canonical_column`]), and a `COUNT` index's guard reads
//! a component the same way ([`canonical_guard`]). Every evaluation of an index
//! entry, the write path's and the build's, canonicalises through these, so a
//! record is indexed alike however it reaches the index.
//!
//! # Datetime order
//!
//! An index stamped below [`DATETIME_ORDER_FORMAT_VERSION`] keys a datetime as
//! its RFC 3339 text, in which a fraction of a second sorts before the whole
//! second it follows, so only the whole second of an entry orders
//! chronologically ([`Datetime::key_second_prefix`]). A range over
//! a datetime component is therefore widened to the whole seconds its bounds lie
//! in ([`widen_access`]); the scan then lists records its predicate rejects, so
//! it answers no conjunct ([`range_is_widened`]) and the predicate is judged
//! again over the records it fetches; and the scan delivers no order across the
//! column ([`IndexRef::orders_coarsely`]). An equality is unchanged: equal
//! datetimes have equal text. The record id an entry ends with is keyed as the
//! record is, so where the id holds a datetime the entries that agree on every
//! column are in no `id` order either ([`id_orders_coarsely`]), whichever
//! column the index holds.
//!
//! An index stamped at [`DATETIME_ORDER_FORMAT_VERSION`] or later keys a
//! datetime as a chronological integer, so its entries order exactly and none
//! of the above applies to it.
//!
//! # Which indexes widen
//!
//! [`derived_datetime_index_needs_widening`] is the one switch, answered from an
//! index's format-version stamp. While it is `true`, [`coarse_column`] marks the
//! datetime components of that index, and every behaviour above follows from the
//! mark. For an index it answers `false` for, no column is marked, and
//! `widen_access`, `range_is_widened` and `IndexRef::orders_coarsely` answer as
//! they do for a column that is not one. No other module reads the switch.

use std::borrow::Cow;
use std::ops::Bound;

use chrono::TimeDelta;

use super::access_path::{BTreeAccess, BoundOperand, ColumnFlags, IndexRef};
use crate::catalog::{ComponentOrder, DATETIME_ORDER_FORMAT_VERSION, KeyOrigin, KeyStructure};
use crate::exec::logical::reads::KeyColumn;
use crate::expr::logical::LogicalExpr;
use crate::expr::{BinaryOperator, Expr, Idiom};
use crate::val::{Datetime, Value};

/// Whether a b-tree index stamped `format_version`, over a datetime component
/// of the record id, must widen the bounds of a range over it.
///
/// `true` while the index entry for a datetime is its RFC 3339 key text, whose
/// order is chronological only down to the whole second: an index stamped below
/// [`DATETIME_ORDER_FORMAT_VERSION`]. This is the one place that says so; every
/// widening, every refusal of an exact answer and every withheld ordering over
/// such a column starts from the mark it gives ([`coarse_column`]).
pub(crate) const fn derived_datetime_index_needs_widening(format_version: u16) -> bool {
	format_version < DATETIME_ORDER_FORMAT_VERSION
}

/// Whether `col` addresses a datetime component of `key`, whose entries in an
/// index stamped `format_version` order only by the whole second.
pub(crate) fn coarse_column(key: &KeyStructure, col: &Idiom, format_version: u16) -> bool {
	derived_datetime_index_needs_widening(format_version)
		&& key.origin == KeyOrigin::IdField
		&& key
			.component_of(col)
			.is_some_and(|position| key.order(position) == ComponentOrder::CoarseSecond)
}

/// For each of `cols`, whether it addresses a datetime component of `key` in
/// an index stamped `format_version` ([`coarse_column`]); `None` when none
/// does.
pub(crate) fn coarse_columns(
	key: &KeyStructure,
	cols: &[Idiom],
	format_version: u16,
) -> Option<ColumnFlags> {
	let flags: ColumnFlags =
		cols.iter().map(|col| coarse_column(key, col, format_version)).collect();
	flags.contains(&true).then_some(flags)
}

/// Whether the entries of an index that ends with the record id list their
/// records out of `id` order. This is so when the id names a component whose key
/// bytes do not order its values exactly ([`KeyStructure::all_exactly_ordered`]),
/// such as a datetime, which orders only by the whole second. Entries that agree
/// on every column of the index then have no `id` order between them.
///
/// This holds whatever [`derived_datetime_index_needs_widening`] answers: the
/// id an entry ends with is encoded as the record key is, under every stamp.
pub(crate) fn id_orders_coarsely(key: &KeyStructure) -> bool {
	key.origin == KeyOrigin::IdField && !key.all_exactly_ordered()
}

/// Which side of a range a bound is.
#[derive(Clone, Copy)]
enum Side {
	Lower,
	Upper,
}

/// The datetime a bound on `side` is widened to for `datetime`, or `None` when
/// no bound reaches every entry that belongs to the predicate.
///
/// An index entry for a datetime within a second sorts, by its text, after the
/// last instant of the second before and no later than the whole second itself:
/// a fraction of a second sorts before the second it follows. A lower bound that
/// reaches every entry of the second `datetime` lies in is therefore the instant
/// just before that second, and an upper bound is the whole second. Both are
/// inclusive, and the predicate decides the entries between.
///
/// A datetime outside the years 0000 to 9999, or a leap second, has no such
/// text, and a lower bound at the first second of year 0000 has no instant
/// before it that does: none of these is widened to a bound.
fn widened(datetime: &Datetime, side: Side) -> Option<Datetime> {
	if !datetime.in_key_range() {
		return None;
	}
	let floor = datetime.floor_second();
	match side {
		Side::Upper => Some(floor),
		Side::Lower => {
			let before = Datetime(floor.checked_sub_signed(TimeDelta::nanoseconds(1))?);
			before.in_key_range().then_some(before)
		}
	}
}

/// `bound` widened as a bound on `side`: a datetime becomes the inclusive bound
/// that reaches its whole second ([`widened`]), and `None` where none does.
/// Anything else, including no bound, is as it was.
///
/// A bound that reads the statement clock has no instant to widen before the
/// scan opens, so it has no widening either.
fn widen_bound(bound: &Bound<BoundOperand>, side: Side) -> Option<Bound<BoundOperand>> {
	match bound {
		Bound::Included(operand) | Bound::Excluded(operand) if operand.is_clock() => None,
		Bound::Included(BoundOperand::Literal(Value::Datetime(datetime)))
		| Bound::Excluded(BoundOperand::Literal(Value::Datetime(datetime))) => widened(datetime, side)
			.map(|datetime| Bound::Included(BoundOperand::Literal(Value::Datetime(datetime)))),
		other @ (Bound::Included(_) | Bound::Excluded(_) | Bound::Unbounded) => Some(other.clone()),
	}
}

/// Widen the datetime bounds of `access` over a column of `index` that orders
/// only by the whole second, so the scan reaches every entry the predicate it
/// came from admits.
///
/// A range on the first column of the index, or the range after the prefix of a
/// compound access, is widened when that column is one
/// ([`IndexRef::orders_coarsely`]). An equality, a prefix and any bound that is
/// not a datetime are as they were. A scan of widened bounds lists records that
/// do not satisfy the predicate, which is judged again over them
/// ([`range_is_widened`]).
///
/// Answers `false`, leaving `access` unchanged, when a datetime bound has no
/// widening ([`widened`]), which includes a bound that reads the statement
/// clock. Dropping such a bound would leave an access that does not read as
/// widened and could be taken as exact, so the caller must discard `access`.
#[must_use]
pub(crate) fn widen_access(index: &IndexRef, access: &mut BTreeAccess) -> bool {
	match access {
		BTreeAccess::Range {
			range,
		} if index.orders_coarsely(0) => {
			let (Some(start), Some(end)) =
				(widen_bound(&range.start, Side::Lower), widen_bound(&range.end, Side::Upper))
			else {
				return false;
			};
			range.start = start;
			range.end = end;
		}
		BTreeAccess::Compound {
			prefix,
			range,
			upper,
		} if index.orders_coarsely(prefix.len()) => {
			// Both bounds are widened before either is written, so a bound with
			// no widening leaves `access` unchanged.
			let mut widened_bounds = [None, None];
			for (slot, bound) in widened_bounds.iter_mut().zip([&*range, &*upper]) {
				let Some((op, operand)) = bound.as_ref() else {
					continue;
				};
				let datetime = match operand {
					BoundOperand::Literal(Value::Datetime(datetime)) => datetime,
					BoundOperand::StatementClock(_) => return false,
					BoundOperand::Literal(_) => continue,
				};
				let (side, op) = match op {
					BinaryOperator::MoreThan | BinaryOperator::MoreThanEqual => {
						(Side::Lower, BinaryOperator::MoreThanEqual)
					}
					BinaryOperator::LessThan | BinaryOperator::LessThanEqual => {
						(Side::Upper, BinaryOperator::LessThanEqual)
					}
					BinaryOperator::Subtract
					| BinaryOperator::Add
					| BinaryOperator::Multiply
					| BinaryOperator::Divide
					| BinaryOperator::Remainder
					| BinaryOperator::Power
					| BinaryOperator::Equal
					| BinaryOperator::ExactEqual
					| BinaryOperator::NotEqual
					| BinaryOperator::AllEqual
					| BinaryOperator::AnyEqual
					| BinaryOperator::Or
					| BinaryOperator::And
					| BinaryOperator::NullCoalescing
					| BinaryOperator::TenaryCondition
					| BinaryOperator::Contain
					| BinaryOperator::NotContain
					| BinaryOperator::ContainAll
					| BinaryOperator::ContainAny
					| BinaryOperator::ContainNone
					| BinaryOperator::Inside
					| BinaryOperator::NotInside
					| BinaryOperator::AllInside
					| BinaryOperator::AnyInside
					| BinaryOperator::NoneInside
					| BinaryOperator::Outside
					| BinaryOperator::Intersects
					| BinaryOperator::Range
					| BinaryOperator::RangeInclusive
					| BinaryOperator::RangeSkip
					| BinaryOperator::RangeSkipInclusive
					| BinaryOperator::Matches(_)
					| BinaryOperator::NearestNeighbor(_) => continue,
				};
				let Some(datetime) = widened(datetime, side) else {
					return false;
				};
				*slot = Some((op, BoundOperand::Literal(Value::Datetime(datetime))));
			}
			for (bound, widened) in [range, upper].into_iter().zip(widened_bounds) {
				if let Some(widened) = widened {
					*bound = Some(widened);
				}
			}
		}
		BTreeAccess::Equality(_)
		| BTreeAccess::Range {
			..
		}
		| BTreeAccess::Compound {
			..
		}
		| BTreeAccess::FullText {
			..
		}
		| BTreeAccess::Knn {
			..
		} => {}
	}
	true
}

/// Whether `access` bounds a column of `index` that orders only by the whole
/// second with a datetime, so that it lists records its predicate rejects and
/// answers none of the conjuncts it was chosen from.
///
/// Every consumer that treats a b-tree scan as answering those conjuncts asks
/// this: an exact scan, a key-only count, a bitmap that stands for its
/// predicate, and the rendering of a plan whose filter the scan would replace.
pub(crate) fn range_is_widened(index: &IndexRef, access: &BTreeAccess) -> bool {
	// A bound that reads the statement clock is a datetime once resolved.
	let is_datetime = |operand: &BoundOperand| matches!(&*operand.witness(), Value::Datetime(_));
	match access {
		BTreeAccess::Range {
			range,
		} => index.orders_coarsely(0) && range.operands().any(is_datetime),
		BTreeAccess::Compound {
			prefix,
			range,
			upper,
		} => {
			index.orders_coarsely(prefix.len())
				&& range.iter().chain(upper).any(|(_, operand)| is_datetime(operand))
		}
		BTreeAccess::Equality(_)
		| BTreeAccess::FullText {
			..
		}
		| BTreeAccess::Knn {
			..
		} => false,
	}
}

/// `col`, spelled as `id[i]` when it addresses a component of `key`, which is
/// how an index evaluates it.
pub(crate) fn canonical_column(key: Option<&KeyStructure>, col: &Idiom) -> Idiom {
	key.filter(|key| key.origin == KeyOrigin::IdField)
		.and_then(|key| key.canonical_column(col))
		.unwrap_or_else(|| col.clone())
}

/// `cols` with each column spelled as an index evaluates it
/// ([`canonical_column`]), as `cols` itself when none addresses a component.
pub(crate) fn canonical_columns<'a>(
	key: Option<&KeyStructure>,
	cols: &'a [Idiom],
) -> Cow<'a, [Idiom]> {
	let Some(key) = key.filter(|key| key.origin == KeyOrigin::IdField) else {
		return Cow::Borrowed(cols);
	};
	if cols.iter().all(|col| key.canonical_column(col).is_none_or(|canonical| canonical == *col)) {
		return Cow::Borrowed(cols);
	}
	Cow::Owned(cols.iter().map(|col| canonical_column(Some(key), col)).collect())
}

/// The named components of `key` as columns: each name with the read of the
/// element it stands for.
fn key_columns(key: &KeyStructure) -> Vec<KeyColumn> {
	key.components
		.iter()
		.filter_map(|component| {
			let name = component.name.as_ref()?;
			let element = Expr::Idiom(KeyStructure::component_idiom(component.position));
			Some(KeyColumn {
				name: name.as_str().to_owned(),
				body: crate::expr::logical::lower_expr(element).ok()?,
			})
		})
		.collect()
}

/// A `COUNT` index's `guard` with each read of a component of `key` at the
/// record, bare or through the record (`$this.city`, `@.city`, `$this['city']`),
/// spelled as the read of the element of the id, which is how an index
/// evaluates it. `None` when the guard reads no component. The image a guard is
/// evaluated over holds no component fields.
pub(crate) fn canonical_guard(
	key: Option<&KeyStructure>,
	guard: &LogicalExpr,
) -> Option<LogicalExpr> {
	let key = key.filter(|key| key.origin == KeyOrigin::IdField)?;
	crate::exec::logical::reads::substitute_record_key_parts(guard, &key_columns(key))
}

/// Whether `guard`, the stored `WHERE` of a `COUNT` index, is the condition
/// `cond` a count is asked for: both render as the same SurrealQL, as written
/// or, where `key` names the components of the table's record ids, once each
/// read of a component is spelled as the element of the id. A condition that an
/// earlier lowering pass has rewritten to read a component by its id element
/// still matches a guard written with the component's name.
pub(crate) fn guard_counts(key: Option<&KeyStructure>, guard: &Expr, cond: &LogicalExpr) -> bool {
	use surrealdb_types::ToSql;

	if guard.to_sql() == cond.to_sql() {
		return true;
	}
	let Some(key) = key else {
		return false;
	};
	let Ok(guard) = crate::expr::logical::lower_expr(guard.clone()) else {
		return false;
	};
	let canonical = |expr: &LogicalExpr| {
		canonical_guard(Some(key), expr).unwrap_or_else(|| expr.clone()).to_sql()
	};
	canonical(&guard) == canonical(cond)
}

#[cfg(test)]
mod tests {
	use std::str::FromStr;

	use chrono::{TimeZone, Utc};
	use surrealdb_strand::Strand;

	use super::super::access_path::OperandRange;
	use super::*;
	use crate::catalog::{Index, IndexDefinition, IndexId};
	use crate::expr::{KeyNames, Kind, KindLiteral};
	use crate::val::Range;

	/// `[city: string, time: datetime, seq: int]`.
	fn key() -> KeyStructure {
		let names = ["city", "time", "seq"].iter().map(|name| Strand::new(*name)).collect();
		KeyStructure::from_id_field(
			&Kind::Literal(KindLiteral::Array(vec![Kind::String, Kind::Datetime, Kind::Int])),
			&KeyNames::new(names, 3).expect("valid names"),
		)
		.expect("a keyed type")
	}

	fn idiom(text: &str) -> Idiom {
		Idiom::from_str(text).expect("a valid idiom")
	}

	/// An index over `cols`, as the analyzer references it under the key.
	fn index(cols: &[&str]) -> IndexRef {
		let cols: Vec<Idiom> = cols.iter().map(|col| idiom(col)).collect();
		let flags = coarse_columns(&key(), &cols, 1);
		let defs = vec![IndexDefinition {
			index_id: IndexId(1),
			name: Strand::from("i"),
			table_name: "t".into(),
			cols,
			index: Index::Idx,
			count_cond: None,
			comment: None,
			prepare_remove: false,
			format_version: 1,
			nulls_not_distinct: false,
		}];
		let index = IndexRef::new(&defs, 0);
		match flags {
			Some(flags) => index.with_coarse_columns(flags),
			None => index,
		}
	}

	/// 2025-01-01 at `h:m:s` and `nanos` nanoseconds past the second.
	fn at(h: u32, m: u32, s: u32, nanos: u32) -> Datetime {
		let whole = Utc.with_ymd_and_hms(2025, 1, 1, h, m, s).unwrap();
		Datetime(whole + TimeDelta::nanoseconds(i64::from(nanos)))
	}

	fn value(datetime: Datetime) -> BoundOperand {
		BoundOperand::Literal(Value::Datetime(datetime))
	}

	fn range(start: Bound<Datetime>, end: Bound<Datetime>) -> BTreeAccess {
		let bound = |bound: Bound<Datetime>| match bound {
			Bound::Included(d) => Bound::Included(value(d)),
			Bound::Excluded(d) => Bound::Excluded(value(d)),
			Bound::Unbounded => Bound::Unbounded,
		};
		BTreeAccess::Range {
			range: OperandRange {
				start: bound(start),
				end: bound(end),
				point_filter: None,
			},
		}
	}

	#[test]
	fn the_switch_is_on_while_datetime_entries_order_by_their_text() {
		assert!(derived_datetime_index_needs_widening(DATETIME_ORDER_FORMAT_VERSION - 1));
		assert!(!derived_datetime_index_needs_widening(DATETIME_ORDER_FORMAT_VERSION));
	}

	#[test]
	fn only_a_datetime_component_orders_by_the_second() {
		let key = key();
		let old = DATETIME_ORDER_FORMAT_VERSION - 1;
		assert!(coarse_column(&key, &idiom("time"), old));
		assert!(coarse_column(&key, &idiom("id[1]"), old));
		assert!(!coarse_column(&key, &idiom("city"), old));
		assert!(!coarse_column(&key, &idiom("seq"), old));
		assert!(!coarse_column(&key, &idiom("other"), old));
		// A path beneath a component is no component.
		assert!(!coarse_column(&key, &idiom("time.x"), old));
		assert_eq!(
			coarse_columns(&key, &[idiom("city"), idiom("time"), idiom("seq")], old).as_deref(),
			Some(&[false, true, false][..])
		);
		assert!(coarse_columns(&key, &[idiom("city"), idiom("seq")], old).is_none());
		// An index stamped at the datetime order version orders exactly.
		let current = DATETIME_ORDER_FORMAT_VERSION;
		assert!(!coarse_column(&key, &idiom("time"), current));
		assert!(coarse_columns(&key, &[idiom("city"), idiom("time")], current).is_none());
	}

	#[test]
	fn the_record_ids_order_coarsely_where_the_key_holds_a_datetime() {
		assert!(id_orders_coarsely(&key()));
		let names = ["city", "seq"].iter().map(|name| Strand::new(*name)).collect();
		let exact = KeyStructure::from_id_field(
			&Kind::Literal(KindLiteral::Array(vec![Kind::String, Kind::Int])),
			&KeyNames::new(names, 2).expect("valid names"),
		)
		.expect("a keyed type");
		assert!(!id_orders_coarsely(&exact));
	}

	#[test]
	fn a_lower_bound_reaches_the_instant_before_its_second() {
		let ix = index(&["time"]);
		for bound in [Bound::Included, Bound::Excluded] {
			let mut access = range(bound(at(10, 0, 5, 500_000_000)), Bound::Unbounded);
			assert!(widen_access(&ix, &mut access));
			assert_eq!(access, range(Bound::Included(at(10, 0, 4, 999_999_999)), Bound::Unbounded));
		}
	}

	#[test]
	fn an_upper_bound_reaches_the_whole_second() {
		let ix = index(&["time"]);
		for bound in [Bound::Included, Bound::Excluded] {
			let mut access = range(Bound::Unbounded, bound(at(10, 0, 5, 500_000_000)));
			assert!(widen_access(&ix, &mut access));
			assert_eq!(access, range(Bound::Unbounded, Bound::Included(at(10, 0, 5, 0))));
		}
	}

	#[test]
	fn a_bound_on_a_whole_second_widens_as_one_inside_it_does() {
		let ix = index(&["time"]);
		let mut access = range(Bound::Included(at(10, 0, 5, 0)), Bound::Included(at(10, 0, 9, 0)));
		assert!(widen_access(&ix, &mut access));
		assert_eq!(
			access,
			range(Bound::Included(at(10, 0, 4, 999_999_999)), Bound::Included(at(10, 0, 9, 0)))
		);
	}

	#[test]
	fn a_widened_range_reaches_the_key_text_of_every_datetime_between_its_bounds() {
		let ix = index(&["time"]);
		let (low, high) = (at(10, 0, 5, 700_000_000), at(10, 0, 8, 300_000_000));
		let mut access = range(Bound::Included(low), Bound::Included(high));
		assert!(widen_access(&ix, &mut access));
		let BTreeAccess::Range {
			range:
				OperandRange {
					start: Bound::Included(BoundOperand::Literal(Value::Datetime(start))),
					end: Bound::Included(BoundOperand::Literal(Value::Datetime(end))),
					..
				},
		} = access
		else {
			panic!("expected an inclusive range");
		};
		let (start, end) = (start.to_string(), end.to_string());
		let mut reached = 0;
		for second in 4..=9 {
			for nanos in [0, 1, 300_000_000, 700_000_000, 999_999_999] {
				let entry = at(10, 0, second, nanos);
				if entry >= low && entry <= high {
					let text = entry.to_string();
					assert!(start <= text && text <= end, "{text} lies outside {start}..{end}");
					reached += 1;
				}
			}
		}
		assert!(reached > 0);
	}

	#[test]
	fn a_compound_range_widens_the_column_after_its_prefix() {
		let ix = index(&["city", "time"]);
		let compound = |op, datetime| BTreeAccess::Compound {
			prefix: vec![Value::from("london").into()],
			range: Some((op, value(datetime))),
			upper: None,
		};
		let mut lower = compound(BinaryOperator::MoreThan, at(10, 0, 5, 500_000_000));
		assert!(widen_access(&ix, &mut lower));
		assert_eq!(lower, compound(BinaryOperator::MoreThanEqual, at(10, 0, 4, 999_999_999)));
		let mut upper = compound(BinaryOperator::LessThan, at(10, 0, 5, 500_000_000));
		assert!(widen_access(&ix, &mut upper));
		assert_eq!(upper, compound(BinaryOperator::LessThanEqual, at(10, 0, 5, 0)));
	}

	#[test]
	fn a_datetime_bound_with_no_widening_refuses_the_access() {
		let ix = index(&["time"]);
		let far = Datetime(Utc.with_ymd_and_hms(10_000, 1, 1, 0, 0, 0).unwrap());
		let before = range(Bound::Included(far), Bound::Excluded(far));
		let mut access = before.clone();
		assert!(!widen_access(&ix, &mut access));
		assert_eq!(access, before);

		// The first second of year 0000 has no instant before it that a key
		// holds; dropping the bound would leave an access that reads as exact.
		let first = Datetime(Utc.with_ymd_and_hms(0, 1, 1, 0, 0, 0).unwrap());
		let before = range(Bound::Excluded(first), Bound::Unbounded);
		let mut access = before.clone();
		assert!(!widen_access(&ix, &mut access));
		assert_eq!(access, before);

		let compound = index(&["city", "time"]);
		let before = BTreeAccess::Compound {
			prefix: vec![Value::from("london").into()],
			range: Some((BinaryOperator::MoreThan, value(far))),
			upper: None,
		};
		let mut access = before.clone();
		assert!(!widen_access(&compound, &mut access));
		assert_eq!(access, before);
	}

	#[test]
	fn an_equality_a_prefix_and_other_columns_are_not_widened() {
		let ix = index(&["time"]);
		let mut equality = BTreeAccess::Equality(value(at(10, 0, 5, 500_000_000)));
		assert!(widen_access(&ix, &mut equality));
		assert_eq!(equality, BTreeAccess::Equality(value(at(10, 0, 5, 500_000_000))));

		let by_city = index(&["city", "time"]);
		let both = BTreeAccess::Compound {
			prefix: vec![Value::from("london").into(), value(at(10, 0, 5, 500_000_000))],
			range: None,
			upper: None,
		};
		let mut prefix = both.clone();
		assert!(widen_access(&by_city, &mut prefix));
		assert_eq!(prefix, both);

		// A range over a column that is no datetime component is as written.
		let strings = BTreeAccess::literal_range(Range {
			start: Bound::Included(Value::from("a")),
			end: Bound::Unbounded,
		});
		let mut range = strings.clone();
		assert!(widen_access(&index(&["city"]), &mut range));
		assert_eq!(range, strings);

		// A bound that is no datetime is as written on a datetime column.
		let none = BTreeAccess::literal_range(Range {
			start: Bound::Excluded(Value::None),
			end: Bound::Unbounded,
		});
		let mut range = none.clone();
		assert!(widen_access(&ix, &mut range));
		assert_eq!(range, none);
	}

	#[test]
	fn a_scan_is_widened_when_a_datetime_bounds_a_coarse_column() {
		let ix = index(&["time"]);
		assert!(range_is_widened(&ix, &range(Bound::Included(at(10, 0, 5, 0)), Bound::Unbounded)));
		assert!(range_is_widened(&ix, &range(Bound::Unbounded, Bound::Excluded(at(10, 0, 5, 0)))));
		assert!(!range_is_widened(&ix, &range(Bound::Unbounded, Bound::Unbounded)));
		assert!(!range_is_widened(&ix, &BTreeAccess::Equality(value(at(10, 0, 5, 0)))));

		let by_city = index(&["city", "time"]);
		assert!(range_is_widened(
			&by_city,
			&BTreeAccess::Compound {
				prefix: vec![Value::from("london").into()],
				range: Some((BinaryOperator::LessThanEqual, value(at(10, 0, 5, 0)))),
				upper: None,
			}
		));
		assert!(!range_is_widened(
			&by_city,
			&BTreeAccess::Compound {
				prefix: vec![Value::from("london").into()],
				range: None,
				upper: None,
			}
		));
		// The range is on `city`, which orders exactly.
		assert!(!range_is_widened(
			&index(&["city"]),
			&BTreeAccess::literal_range(Range {
				start: Bound::Included(Value::from("a")),
				end: Bound::Unbounded,
			})
		));
	}

	#[test]
	fn a_column_is_canonical_when_it_addresses_a_component() {
		let key = key();
		assert_eq!(canonical_column(Some(&key), &idiom("time")), idiom("id[1]"));
		assert_eq!(canonical_column(Some(&key), &idiom("id[1]")), idiom("id[1]"));
		assert_eq!(canonical_column(Some(&key), &idiom("other")), idiom("other"));
		assert_eq!(canonical_column(Some(&key), &idiom("time.x")), idiom("time.x"));
		assert_eq!(canonical_column(None, &idiom("time")), idiom("time"));
	}

	#[test]
	fn canonical_columns_borrow_what_they_do_not_change() {
		let key = key();
		let plain = [idiom("a"), idiom("b.c")];
		assert!(matches!(canonical_columns(Some(&key), &plain), Cow::Borrowed(_)));
		assert!(matches!(canonical_columns(None, &[idiom("time")]), Cow::Borrowed(_)));
		let keyed = [idiom("city"), idiom("a"), idiom("time")];
		assert_eq!(
			canonical_columns(Some(&key), &keyed).as_ref(),
			&[canonical_column(Some(&key), &idiom("city")), idiom("a"), idiom("id[1]")]
		);
	}

	#[test]
	fn a_guard_reads_a_component_as_the_element_of_the_id() {
		let key = key();
		let guard = |text: &str| {
			let expr = crate::syn::expr(text).expect("an expression");
			crate::expr::logical::lower_expr(expr).expect("a logical form")
		};
		let reads = |expr: &LogicalExpr, field: &str| {
			expr.any(|e| matches!(e, LogicalExpr::Field(name) if name.as_str() == field))
		};
		let before = guard("city = 'london' AND time > d'2025-01-01T00:00:00Z' AND other = 1");
		assert!(reads(&before, "city") && reads(&before, "time") && !reads(&before, "id"));
		let after = canonical_guard(Some(&key), &before).expect("a guard that reads components");
		assert!(!reads(&after, "city") && !reads(&after, "time"));
		assert!(reads(&after, "id") && reads(&after, "other"));
		assert!(canonical_guard(Some(&key), &guard("other = 1")).is_none());
		assert!(canonical_guard(None, &before).is_none());

		// A component read through the record is the same read: the image a
		// guard is evaluated over holds no component fields.
		use surrealdb_types::ToSql;
		let bare = canonical_guard(Some(&key), &guard("city = 'london'")).expect("reads city");
		for text in [
			"$this.city = 'london'",
			"$self.city = 'london'",
			"@.city = 'london'",
			"$this['city'] = 'london'",
			"$self['city'] = 'london'",
		] {
			let through = canonical_guard(Some(&key), &guard(text)).expect(text);
			assert_eq!(through.to_sql(), bare.to_sql(), "{text}");
		}
		// `@['city']` indexes the record id, which holds no field of that name.
		assert!(canonical_guard(Some(&key), &guard("@['city'] = 'london'")).is_none());
		// A field of another record is not a component of this one.
		assert!(canonical_guard(Some(&key), &guard("$other.city = 'london'")).is_none());
	}

	#[test]
	fn a_guard_counts_the_condition_it_names_by_any_spelling_of_a_component() {
		let key = key();
		let expr = |text: &str| crate::syn::expr(text).expect("an expression");
		let logical = |text: &str| crate::expr::logical::lower_expr(expr(text)).expect("lowers");
		let named = expr("city = 'london' AND time > d'2025-01-01T00:00:00Z'");
		let element = logical("id[0] = 'london' AND id[1] > d'2025-01-01T00:00:00Z'");
		assert!(guard_counts(Some(&key), &named, &element));
		assert!(guard_counts(
			Some(&key),
			&named,
			&logical("city = 'london' AND time > d'2025-01-01T00:00:00Z'")
		));
		// Another condition is another count.
		assert!(!guard_counts(
			Some(&key),
			&named,
			&logical("id[0] = 'paris' AND id[1] > d'2025-01-01T00:00:00Z'")
		));
		// With no names the two spellings are different conditions.
		assert!(!guard_counts(None, &named, &element));
	}
}
