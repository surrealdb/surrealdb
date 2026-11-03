//! The range family: PostgreSQL range and multirange types (`int4range`, `int8range`, `numrange`,
//! `tsrange`, `tstzrange`, `daterange`, and their `multirange` forms).
//!
//! A range is a [`Value::Range`] whose bounds are values of the element kind named by the declared
//! base: `int4` bounds are 32-bit integers, `int8` bounds are integers, `num` bounds are decimals,
//! `ts` bounds are timestamps, `tstz` bounds are datetimes, and `date` bounds are dates. A bound of
//! any other kind is refused. The discrete element kinds (`int4`, `int8`, `date`) store a range as
//! `[lower, upper)`, as PostgreSQL does. An empty range is stored with equal bounds and an
//! exclusive end at the element's zero, `Included(zero)` to `Excluded(zero)`, so that every empty
//! range of an element kind is one value. A multirange is a [`Value::Array`] of ranges,
//! kept sorted, with overlapping and adjacent ranges merged and empty ranges dropped.

use std::cmp::Ordering;
use std::ops::Bound;

use super::super::declared_type::{DeclaredSqlType, TypeViolation};
use super::super::field::SqlTypeDialect;
use super::Validated;
use crate::val::{Array, Date, Datetime, Number, Range, Timestamp, Value};

/// The element kind of a range, read from the declared base.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Element {
	Int4,
	Int8,
	Numeric,
	Timestamp,
	Timestamptz,
	Date,
}

impl Element {
	/// The name of the element kind as PostgreSQL spells it in a message.
	pub fn name(self) -> &'static str {
		match self {
			Element::Int4 => "integer",
			Element::Int8 => "bigint",
			Element::Numeric => "numeric",
			Element::Timestamp => "timestamp without time zone",
			Element::Timestamptz => "timestamp with time zone",
			Element::Date => "date",
		}
	}

	/// The zero of the element kind: the number zero, or the Unix epoch. An empty range is stored
	/// at it.
	pub fn zero(self) -> Value {
		match self {
			Element::Int4 => Value::Number(Number::I32(0)),
			Element::Int8 => Value::Number(Number::Int(0)),
			Element::Numeric => Value::Number(Number::Decimal(rust_decimal::Decimal::ZERO)),
			Element::Timestamp => Value::Timestamp(Timestamp::from(
				chrono::DateTime::<chrono::Utc>::UNIX_EPOCH.naive_utc(),
			)),
			Element::Timestamptz => {
				Value::Datetime(Datetime(chrono::DateTime::<chrono::Utc>::UNIX_EPOCH))
			}
			Element::Date => {
				Value::Date(Date::from(chrono::DateTime::<chrono::Utc>::UNIX_EPOCH.date_naive()))
			}
		}
	}

	/// Whether the element kind has a successor, so that a range is stored as `[lower, upper)`.
	pub fn discrete(self) -> bool {
		match self {
			Element::Int4 | Element::Int8 | Element::Date => true,
			Element::Numeric | Element::Timestamp | Element::Timestamptz => false,
		}
	}
}

/// Why a bound or a range is refused, before it is named in a violation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RangeError {
	/// A bound is not a value of the element kind.
	Mismatch,
	/// A bound lies outside the element kind, such as the successor of the greatest integer.
	Overflow,
	/// The lower bound is greater than the upper bound, as written.
	Order,
}

/// The family's entry point. It owns the PostgreSQL range and multirange base names, and declines
/// every other declared type, and every array of them, unchanged.
pub fn validate(declared: &DeclaredSqlType, value: Value, column: &str) -> Validated {
	if declared.dialect() != SqlTypeDialect::PostgreSql || !declared.array_dimensions().is_empty() {
		return Validated::Declined(value);
	}
	let Some((element, multi)) = shape(declared.base()) else {
		return Validated::Declined(value);
	};
	Validated::Decided(
		store(element, multi, value)
			.map_err(|error| violation(error, element, declared.base(), column)),
	)
}

/// The element kind and whether the declared type is a multirange, from a base name.
pub fn shape(base: &str) -> Option<(Element, bool)> {
	let (name, multi) = match base.strip_suffix("multirange") {
		Some(name) => (name, true),
		None => (base.strip_suffix("range")?, false),
	};
	let element = match name {
		"int4" => Element::Int4,
		"int8" => Element::Int8,
		"num" => Element::Numeric,
		"ts" => Element::Timestamp,
		"tstz" => Element::Timestamptz,
		"date" => Element::Date,
		_ => return None,
	};
	Some((element, multi))
}

/// The value to store for a range or multirange. A single range is accepted for a multirange and
/// stored as a one-range multirange, as PostgreSQL does. `Null` and `None` pass through.
fn store(element: Element, multi: bool, value: Value) -> Result<Value, RangeError> {
	if let Value::Range(range) = value {
		return if multi {
			canonical_multirange(element, vec![*range]).map(multirange_value)
		} else {
			canonical_range(element, *range).map(|range| Value::Range(Box::new(range)))
		};
	}
	if value.is_nullish() {
		return Ok(value);
	}
	let Value::Array(items) = value else {
		return Err(RangeError::Mismatch);
	};
	if !multi {
		return Err(RangeError::Mismatch);
	}
	let mut ranges = Vec::with_capacity(items.0.len());
	for item in items.0 {
		let Value::Range(range) = item else {
			return Err(RangeError::Mismatch);
		};
		ranges.push(*range);
	}
	canonical_multirange(element, ranges).map(multirange_value)
}

fn multirange_value(ranges: Vec<Range>) -> Value {
	Value::Array(Array(ranges.into_iter().map(|range| Value::Range(Box::new(range))).collect()))
}

/// The violation a refused bound or range reports, in the SQLSTATE PostgreSQL uses for it.
fn violation(error: RangeError, element: Element, base: &str, column: &str) -> TypeViolation {
	match error {
		RangeError::Mismatch => TypeViolation {
			code: "42804",
			message: format!(
				"column \"{column}\" of type {base} takes bounds of type {}",
				element.name()
			),
		},
		RangeError::Overflow => TypeViolation {
			code: "22003",
			message: format!("{} out of range", element.name()),
		},
		RangeError::Order => TypeViolation {
			code: "22000",
			message: "range lower bound must be less than or equal to range upper bound"
				.to_string(),
		},
	}
}

/// A range in canonical form: the discrete form for a discrete element, and an empty range as
/// equal bounds with an exclusive end at the element's zero.
fn canonical_range(element: Element, range: Range) -> Result<Range, RangeError> {
	let lower = convert_bound(element, range.start)?;
	let upper = convert_bound(element, range.end)?;
	// The order is judged on the bounds as written: `[5,4]` is refused even though its discrete
	// form is empty.
	if let (Some(lo), Some(hi)) = (bound_value(&lower), bound_value(&upper))
		&& lo > hi
	{
		return Err(RangeError::Order);
	}
	// A bound at an infinite date has no successor and keeps its form, as PostgreSQL's
	// `daterange_canonical` leaves it.
	let (lower, upper) = if element.discrete() {
		let lower = match lower {
			Bound::Excluded(value) if !is_infinite_date(&value) => {
				Bound::Included(successor(element, &value)?)
			}
			other @ (Bound::Included(_) | Bound::Excluded(_) | Bound::Unbounded) => other,
		};
		let upper = match upper {
			Bound::Included(value) if !is_infinite_date(&value) => {
				Bound::Excluded(successor(element, &value)?)
			}
			other @ (Bound::Included(_) | Bound::Excluded(_) | Bound::Unbounded) => other,
		};
		(lower, upper)
	} else {
		(lower, upper)
	};
	let is_empty = match (bound_value(&lower), bound_value(&upper)) {
		(Some(lo), Some(hi)) => match lo.cmp(hi) {
			Ordering::Greater => true,
			Ordering::Equal => {
				!(matches!(lower, Bound::Included(_)) && matches!(upper, Bound::Included(_)))
			}
			Ordering::Less => false,
		},
		_ => false,
	};
	Ok(if is_empty {
		let zero = element.zero();
		Range {
			start: Bound::Included(zero.clone()),
			end: Bound::Excluded(zero),
		}
	} else {
		Range {
			start: lower,
			end: upper,
		}
	})
}

/// The sorted, merged form of a multirange: empty ranges dropped, and overlapping or adjacent
/// ranges joined into one.
fn canonical_multirange(element: Element, ranges: Vec<Range>) -> Result<Vec<Range>, RangeError> {
	let mut canonical = Vec::with_capacity(ranges.len());
	for range in ranges {
		let range = canonical_range(element, range)?;
		if !range.is_empty() {
			canonical.push(range);
		}
	}
	canonical.sort_by(|a, b| lower_order(&a.start, &b.start));
	let mut merged: Vec<Range> = Vec::with_capacity(canonical.len());
	for next in canonical {
		if let Some(current) = merged.last_mut()
			&& touches(&current.end, &next.start)
		{
			current.end =
				later_end(std::mem::replace(&mut current.end, Bound::Unbounded), next.end);
		} else {
			merged.push(next);
		}
	}
	Ok(merged)
}

/// PostgreSQL's order of lower bounds: an unbounded one first, then by value, and at one value an
/// inclusive bound before an exclusive one, which starts just after it.
fn lower_order(a: &Bound<Value>, b: &Bound<Value>) -> Ordering {
	match (a, b) {
		(Bound::Unbounded, Bound::Unbounded) => Ordering::Equal,
		(Bound::Unbounded, _) => Ordering::Less,
		(_, Bound::Unbounded) => Ordering::Greater,
		(Bound::Included(x) | Bound::Excluded(x), Bound::Included(y) | Bound::Excluded(y)) => x
			.cmp(y)
			.then_with(|| matches!(b, Bound::Included(_)).cmp(&matches!(a, Bound::Included(_)))),
	}
}

/// Whether a range ending at `end` and a range starting at `start` cover one run of values, so
/// that they merge. Two exclusive bounds at one value leave that value uncovered.
fn touches(end: &Bound<Value>, start: &Bound<Value>) -> bool {
	match (end, start) {
		(Bound::Unbounded, _) | (_, Bound::Unbounded) => true,
		(Bound::Included(e), Bound::Included(s))
		| (Bound::Included(e), Bound::Excluded(s))
		| (Bound::Excluded(e), Bound::Included(s)) => s <= e,
		(Bound::Excluded(e), Bound::Excluded(s)) => s < e,
	}
}

/// The later of two upper bounds. At one value an inclusive bound wins over an exclusive one.
fn later_end(a: Bound<Value>, b: Bound<Value>) -> Bound<Value> {
	let (Some((x, x_inclusive)), Some((y, y_inclusive))) = (finite(a), finite(b)) else {
		return Bound::Unbounded;
	};
	match x.cmp(&y) {
		Ordering::Greater => bound(x, x_inclusive),
		Ordering::Less => bound(y, y_inclusive),
		Ordering::Equal => bound(x, x_inclusive || y_inclusive),
	}
}

fn finite(bound: Bound<Value>) -> Option<(Value, bool)> {
	match bound {
		Bound::Included(value) => Some((value, true)),
		Bound::Excluded(value) => Some((value, false)),
		Bound::Unbounded => None,
	}
}

fn bound(value: Value, inclusive: bool) -> Bound<Value> {
	if inclusive {
		Bound::Included(value)
	} else {
		Bound::Excluded(value)
	}
}

fn bound_value(bound: &Bound<Value>) -> Option<&Value> {
	match bound {
		Bound::Included(value) | Bound::Excluded(value) => Some(value),
		Bound::Unbounded => None,
	}
}

fn convert_bound(element: Element, bound: Bound<Value>) -> Result<Bound<Value>, RangeError> {
	Ok(match bound {
		Bound::Unbounded => Bound::Unbounded,
		Bound::Included(value) => Bound::Included(convert_value(element, value)?),
		Bound::Excluded(value) => Bound::Excluded(convert_value(element, value)?),
	})
}

/// The bound as a value of the element kind. A number is converted to the element's own number
/// kind when it is exact, and a time value must already be of the element's time kind.
fn convert_value(element: Element, value: Value) -> Result<Value, RangeError> {
	match (element, value) {
		(Element::Int4, Value::Number(number)) => {
			let int = exact_integer(&number)?;
			i32::try_from(int)
				.map(|int| Value::Number(Number::I32(int)))
				.map_err(|_| RangeError::Overflow)
		}
		(Element::Int8, Value::Number(number)) => {
			exact_integer(&number).map(|int| Value::Number(Number::Int(int)))
		}
		// A bound beyond the range of `decimal` keeps its bigdecimal value, as a `numeric`
		// column holds it.
		(Element::Numeric, Value::Number(number)) => match number.as_decimal_lossless() {
			Some(decimal) => Ok(Value::Number(Number::Decimal(decimal))),
			None if matches!(number, Number::BigDecimal(_)) => Ok(Value::Number(number)),
			None => Err(RangeError::Mismatch),
		},
		(Element::Timestamp, value @ Value::Timestamp(_))
		| (Element::Timestamptz, value @ Value::Datetime(_))
		| (Element::Date, value @ Value::Date(_)) => Ok(value),
		_ => Err(RangeError::Mismatch),
	}
}

fn exact_integer(number: &Number) -> Result<i64, RangeError> {
	number.as_int_lossless().ok_or(RangeError::Mismatch)
}

fn is_infinite_date(value: &Value) -> bool {
	matches!(value, Value::Date(date) if date.is_infinite())
}

/// The next value of a discrete element kind.
fn successor(element: Element, value: &Value) -> Result<Value, RangeError> {
	match (element, value) {
		(Element::Int4, Value::Number(Number::I32(int))) => int
			.checked_add(1)
			.map(|int| Value::Number(Number::I32(int)))
			.ok_or(RangeError::Overflow),
		(Element::Int8, Value::Number(Number::Int(int))) => int
			.checked_add(1)
			.map(|int| Value::Number(Number::Int(int)))
			.ok_or(RangeError::Overflow),
		(Element::Date, Value::Date(date)) => date
			.days()
			.checked_add(1)
			.and_then(Date::from_days)
			.map(Value::Date)
			.ok_or(RangeError::Overflow),
		_ => Err(RangeError::Mismatch),
	}
}

/// The lower bound of a range, as PostgreSQL's `lower()` gives it: `NULL` for an empty range and
/// for a range with no lower bound.
pub fn lower(range: &Range) -> Value {
	match &range.start {
		Bound::Included(value) | Bound::Excluded(value) if !range.is_empty() => value.clone(),
		Bound::Included(_) | Bound::Excluded(_) | Bound::Unbounded => Value::Null,
	}
}

/// The upper bound of a range, as PostgreSQL's `upper()` gives it: `NULL` for an empty range and
/// for a range with no upper bound.
pub fn upper(range: &Range) -> Value {
	match &range.end {
		Bound::Included(value) | Bound::Excluded(value) if !range.is_empty() => value.clone(),
		Bound::Included(_) | Bound::Excluded(_) | Bound::Unbounded => Value::Null,
	}
}

/// Whether two ranges have a value in common, PostgreSQL's `&&`. An empty range overlaps nothing.
pub fn overlaps(a: &Range, b: &Range) -> bool {
	!a.is_empty() && !b.is_empty() && starts_by(&a.start, &b.end) && starts_by(&b.start, &a.end)
}

/// Whether every value of `inner` lies in `outer`, PostgreSQL's range `@>` range. Every range
/// contains the empty range, and the empty range contains no other.
pub fn contains(outer: &Range, inner: &Range) -> bool {
	if inner.is_empty() {
		return true;
	}
	!outer.is_empty()
		&& compare_starts(&outer.start, &inner.start) != Ordering::Greater
		&& compare_ends(&outer.end, &inner.end) != Ordering::Less
}

/// Whether a value lies at or after the lower bound `start` and at or before the upper bound
/// `end`, so that a range starting at `start` and one ending at `end` can share it.
fn starts_by(start: &Bound<Value>, end: &Bound<Value>) -> bool {
	match (start, end) {
		(Bound::Unbounded, _) | (_, Bound::Unbounded) => true,
		(Bound::Included(s), Bound::Included(e)) => s <= e,
		(Bound::Included(s) | Bound::Excluded(s), Bound::Included(e) | Bound::Excluded(e)) => s < e,
	}
}

/// The order of two lower bounds: no bound first, then by value, and at one value an inclusive
/// bound before an exclusive one.
fn compare_starts(a: &Bound<Value>, b: &Bound<Value>) -> Ordering {
	match (a, b) {
		(Bound::Unbounded, Bound::Unbounded) => Ordering::Equal,
		(Bound::Unbounded, _) => Ordering::Less,
		(_, Bound::Unbounded) => Ordering::Greater,
		(Bound::Included(x) | Bound::Excluded(x), Bound::Included(y) | Bound::Excluded(y)) => x
			.cmp(y)
			.then_with(|| matches!(a, Bound::Excluded(_)).cmp(&matches!(b, Bound::Excluded(_)))),
	}
}

/// The order of two upper bounds: by value, at one value an exclusive bound before an inclusive
/// one, and no bound last.
fn compare_ends(a: &Bound<Value>, b: &Bound<Value>) -> Ordering {
	match (a, b) {
		(Bound::Unbounded, Bound::Unbounded) => Ordering::Equal,
		(Bound::Unbounded, _) => Ordering::Greater,
		(_, Bound::Unbounded) => Ordering::Less,
		(Bound::Included(x) | Bound::Excluded(x), Bound::Included(y) | Bound::Excluded(y)) => x
			.cmp(y)
			.then_with(|| matches!(a, Bound::Included(_)).cmp(&matches!(b, Bound::Included(_)))),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn declared(text: &str) -> DeclaredSqlType {
		DeclaredSqlType::parse(SqlTypeDialect::PostgreSql, text).expect("a PostgreSQL type")
	}

	fn store_as(text: &str, value: Value) -> Result<Value, TypeViolation> {
		match validate(&declared(text), value, "r") {
			Validated::Decided(result) => result,
			Validated::Declined(_) => panic!("{text} is declined"),
		}
	}

	fn int(n: i64) -> Value {
		Value::Number(Number::Int(n))
	}

	fn i32_value(n: i32) -> Value {
		Value::Number(Number::I32(n))
	}

	fn span(start: Bound<Value>, end: Bound<Value>) -> Value {
		Value::Range(Box::new(Range {
			start,
			end,
		}))
	}

	fn included(n: i64) -> Bound<Value> {
		Bound::Included(int(n))
	}

	fn excluded(n: i64) -> Bound<Value> {
		Bound::Excluded(int(n))
	}

	fn discrete_form(lower: i32, upper: i32) -> Value {
		Value::Range(Box::new(Range {
			start: Bound::Included(i32_value(lower)),
			end: Bound::Excluded(i32_value(upper)),
		}))
	}

	fn range(start: Bound<Value>, end: Bound<Value>) -> Range {
		Range {
			start,
			end,
		}
	}

	#[test]
	fn lower_and_upper_are_null_for_an_empty_or_unbounded_side() {
		assert_eq!(lower(&range(included(1), excluded(5))), int(1));
		assert_eq!(upper(&range(included(1), excluded(5))), int(5));
		assert_eq!(lower(&range(Bound::Unbounded, excluded(5))), Value::Null);
		assert_eq!(upper(&range(included(1), Bound::Unbounded)), Value::Null);
		assert_eq!(lower(&range(included(3), excluded(3))), Value::Null);
		assert_eq!(upper(&range(included(3), excluded(3))), Value::Null);
	}

	#[test]
	fn ranges_overlap_when_they_share_a_value() {
		assert!(overlaps(&range(included(1), excluded(5)), &range(included(4), excluded(8))));
		assert!(!overlaps(&range(included(1), excluded(5)), &range(included(5), excluded(8))));
		assert!(overlaps(&range(included(1), included(5)), &range(included(5), excluded(8))));
		assert!(!overlaps(&range(included(1), included(5)), &range(excluded(5), excluded(8))));
		assert!(overlaps(
			&range(Bound::Unbounded, excluded(2)),
			&range(included(1), Bound::Unbounded)
		));
		assert!(!overlaps(
			&range(included(3), excluded(3)),
			&range(Bound::Unbounded, Bound::Unbounded)
		));
	}

	#[test]
	fn a_range_contains_a_range_within_its_bounds() {
		let outer = range(included(1), excluded(10));
		assert!(contains(&outer, &range(included(2), excluded(5))));
		assert!(contains(&outer, &range(included(1), excluded(10))));
		assert!(!contains(&outer, &range(included(0), excluded(5))));
		assert!(!contains(&outer, &range(included(2), included(10))));
		assert!(contains(&outer, &range(excluded(1), included(9))));
		assert!(!contains(&outer, &range(included(2), Bound::Unbounded)));
		assert!(contains(&range(Bound::Unbounded, Bound::Unbounded), &outer));
		assert!(contains(&outer, &range(included(20), excluded(20))));
		assert!(!contains(&range(included(20), excluded(20)), &outer));
	}

	#[test]
	fn a_discrete_range_is_stored_as_lower_inclusive_and_upper_exclusive() {
		let stored = store_as("int4range", span(excluded(1), included(5))).unwrap();
		assert_eq!(stored, discrete_form(2, 6));
		let stored = store_as("int4range", span(included(1), excluded(5))).unwrap();
		assert_eq!(stored, discrete_form(1, 5));
		let stored = store_as("int4range", span(Bound::Unbounded, included(5))).unwrap();
		assert_eq!(
			stored,
			Value::Range(Box::new(Range {
				start: Bound::Unbounded,
				end: Bound::Excluded(i32_value(6)),
			}))
		);
	}

	#[test]
	fn a_single_point_discrete_range_keeps_one_value() {
		assert_eq!(
			store_as("int4range", span(included(3), included(3))).unwrap(),
			discrete_form(3, 4)
		);
	}

	#[test]
	fn an_empty_range_is_equal_bounds_with_an_exclusive_end() {
		let empty = discrete_form(0, 0);
		assert_eq!(store_as("int4range", span(included(3), excluded(3))).unwrap(), empty);
		assert_eq!(store_as("int4range", span(excluded(3), included(3))).unwrap(), empty);
		assert_eq!(store_as("int4range", span(excluded(3), excluded(4))).unwrap(), empty);
		assert_eq!(store_as("int4range", span(included(7), excluded(7))).unwrap(), empty);
	}

	#[test]
	fn a_lower_bound_above_the_upper_bound_is_refused_with_its_sqlstate() {
		let err = store_as("int4range", span(included(5), excluded(4))).unwrap_err();
		assert_eq!(err.code, "22000");
		assert_eq!(
			err.message,
			"range lower bound must be less than or equal to range upper bound"
		);
		assert!(store_as("int4range", span(included(5), included(4))).is_err());
	}

	#[test]
	fn a_continuous_range_keeps_its_bounds_and_inclusivity() {
		let decimal = |n: i64| Value::Number(Number::Decimal(rust_decimal::Decimal::from(n)));
		let stored =
			store_as("numrange", span(Bound::Excluded(int(1)), Bound::Included(int(5)))).unwrap();
		assert_eq!(stored, span(Bound::Excluded(decimal(1)), Bound::Included(decimal(5))));
		let empty =
			store_as("numrange", span(Bound::Excluded(int(3)), Bound::Included(int(3)))).unwrap();
		assert_eq!(empty, span(Bound::Included(decimal(0)), Bound::Excluded(decimal(0))));
		let point =
			store_as("numrange", span(Bound::Included(int(3)), Bound::Included(int(3)))).unwrap();
		assert_eq!(point, span(Bound::Included(decimal(3)), Bound::Included(decimal(3))));
	}

	#[test]
	fn a_numrange_keeps_a_bound_beyond_the_range_of_decimal() {
		let wide = Value::Number(Number::BigDecimal(Box::new(
			"1234567890123456789012345678901234567890".parse().expect("a bigdecimal"),
		)));
		let stored =
			store_as("numrange", span(Bound::Included(int(1)), Bound::Excluded(wide.clone())))
				.unwrap();
		let decimal = Value::Number(Number::Decimal(rust_decimal::Decimal::from(1)));
		assert_eq!(stored, span(Bound::Included(decimal), Bound::Excluded(wide)));
	}

	#[test]
	fn int4_bounds_outside_the_element_range_overflow() {
		let big = Value::Number(Number::Int(i64::from(i32::MAX) + 1));
		let err = store_as("int4range", span(Bound::Included(big), Bound::Unbounded)).unwrap_err();
		assert_eq!(err.code, "22003");
		let max = Value::Number(Number::Int(i64::from(i32::MAX)));
		let err = store_as("int4range", span(Bound::Included(max.clone()), Bound::Included(max)))
			.unwrap_err();
		assert_eq!(err.code, "22003", "the successor of the greatest integer is out of range");
	}

	#[test]
	fn a_bound_of_another_kind_is_refused() {
		let text = Value::String("a".into());
		let err = store_as("int4range", span(Bound::Included(text), Bound::Unbounded)).unwrap_err();
		assert_eq!(err.code, "42804");
		let fraction = Value::Number(Number::Float(1.5));
		assert!(store_as("int8range", span(Bound::Included(fraction), Bound::Unbounded)).is_err());
		let number = Value::Number(Number::Int(1));
		assert!(store_as("tstzrange", span(Bound::Included(number), Bound::Unbounded)).is_err());
	}

	#[test]
	fn a_multirange_is_sorted_and_merged_when_ranges_touch_or_overlap() {
		let ranges = Value::Array(Array(vec![
			span(included(7), excluded(9)),
			span(included(3), excluded(5)),
			span(included(1), excluded(3)),
			span(included(8), excluded(8)),
		]));
		let stored = store_as("int8multirange", ranges).unwrap();
		let expected = Value::Array(Array(vec![
			span(included(1), excluded(5)),
			span(included(7), excluded(9)),
		]));
		assert_eq!(stored, expected);
	}

	#[test]
	fn a_numeric_multirange_keeps_a_gap_at_a_value_both_ends_exclude() {
		let ranges = Value::Array(Array(vec![
			span(included(1), excluded(3)),
			span(excluded(3), excluded(5)),
		]));
		let stored = store_as("nummultirange", ranges).unwrap();
		let decimal = |n: i64| Value::Number(Number::Decimal(rust_decimal::Decimal::from(n)));
		assert_eq!(
			stored,
			Value::Array(Array(vec![
				span(Bound::Included(decimal(1)), Bound::Excluded(decimal(3))),
				span(Bound::Excluded(decimal(3)), Bound::Excluded(decimal(5))),
			]))
		);
		let touching = Value::Array(Array(vec![
			span(included(1), included(3)),
			span(excluded(3), excluded(5)),
		]));
		let stored = store_as("nummultirange", touching).unwrap();
		let Value::Array(Array(ranges)) = stored else {
			panic!("a multirange is an array");
		};
		assert_eq!(ranges.len(), 1, "[1,3] and (3,5) cover one run");
	}

	#[test]
	fn a_multirange_is_sorted_by_bound_value_before_inclusivity() {
		let ranges = Value::Array(Array(vec![
			span(Bound::Excluded(int(1)), excluded(2)),
			span(included(3), excluded(4)),
		]));
		let stored = store_as("nummultirange", ranges).unwrap();
		let decimal = |n: i64| Value::Number(Number::Decimal(rust_decimal::Decimal::from(n)));
		assert_eq!(
			stored,
			Value::Array(Array(vec![
				span(Bound::Excluded(decimal(1)), Bound::Excluded(decimal(2))),
				span(Bound::Included(decimal(3)), Bound::Excluded(decimal(4))),
			]))
		);
	}

	#[test]
	fn empty_ranges_are_dropped_from_a_multirange() {
		let ranges = Value::Array(Array(vec![
			span(included(3), excluded(3)),
			span(included(1), excluded(2)),
		]));
		let stored = store_as("int4multirange", ranges).unwrap();
		assert_eq!(stored, Value::Array(Array(vec![discrete_form(1, 2)])));
		let only_empty = Value::Array(Array(vec![span(included(3), excluded(3))]));
		assert_eq!(store_as("int4multirange", only_empty).unwrap(), Value::Array(Array(vec![])));
	}

	#[test]
	fn a_single_range_is_accepted_as_a_one_range_multirange() {
		let stored = store_as("int4multirange", span(included(1), excluded(2))).unwrap();
		assert_eq!(stored, Value::Array(Array(vec![discrete_form(1, 2)])));
	}

	#[test]
	fn an_array_is_refused_for_a_range_and_a_range_for_an_array_of_non_ranges() {
		let array = Value::Array(Array(vec![int(1)]));
		assert!(store_as("int4range", array).is_err());
		let not_a_range = Value::Array(Array(vec![int(1)]));
		assert!(store_as("int4multirange", not_a_range).is_err());
	}

	#[test]
	fn null_and_none_pass_through_and_other_types_are_declined() {
		assert_eq!(store_as("daterange", Value::Null).unwrap(), Value::Null);
		assert_eq!(store_as("daterange", Value::None).unwrap(), Value::None);
		let text = declared("text");
		assert!(matches!(validate(&text, Value::Null, "c"), Validated::Declined(Value::Null)));
		let array_of_ranges = DeclaredSqlType::parse(SqlTypeDialect::PostgreSql, "int4range[]")
			.expect("a PostgreSQL type");
		assert!(matches!(
			validate(&array_of_ranges, Value::Null, "c"),
			Validated::Declined(Value::None | Value::Null)
		));
		let mysql = DeclaredSqlType::parse(SqlTypeDialect::MySql, "int4range").expect("a type");
		assert!(matches!(validate(&mysql, Value::Null, "c"), Validated::Declined(_)));
	}

	#[test]
	fn dates_are_discrete_and_step_by_a_day() {
		let start = Value::Date(Date::from_days(10).expect("a date"));
		let end = Value::Date(Date::from_days(12).expect("a date"));
		let stored =
			store_as("daterange", span(Bound::Included(start), Bound::Included(end))).unwrap();
		let Value::Range(range) = stored else {
			panic!("a range");
		};
		assert_eq!(range.start, Bound::Included(Value::Date(Date::from_days(10).expect("a date"))));
		assert_eq!(range.end, Bound::Excluded(Value::Date(Date::from_days(13).expect("a date"))));
	}

	#[test]
	fn an_infinite_date_bound_keeps_its_form() {
		let day = Value::Date(Date::from_days(10).expect("a date"));
		let inf = Value::Date(Date::infinity());
		let neg = Value::Date(Date::neg_infinity());
		let stored =
			store_as("daterange", span(Bound::Included(day.clone()), Bound::Included(inf.clone())))
				.unwrap();
		let Value::Range(range) = stored else {
			panic!("a range");
		};
		assert_eq!((range.start, range.end), (Bound::Included(day.clone()), Bound::Included(inf)));
		let stored =
			store_as("daterange", span(Bound::Excluded(neg.clone()), Bound::Excluded(day.clone())))
				.unwrap();
		let Value::Range(range) = stored else {
			panic!("a range");
		};
		assert_eq!((range.start, range.end), (Bound::Excluded(neg), Bound::Excluded(day)));
	}

	#[test]
	fn a_timestamp_range_refuses_a_datetime_bound() {
		let datetime = Value::Datetime(crate::val::Datetime::now());
		let err =
			store_as("tsrange", span(Bound::Included(datetime), Bound::Unbounded)).unwrap_err();
		assert_eq!(err.code, "42804");
	}
}
