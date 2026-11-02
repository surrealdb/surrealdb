//! The record-key byte ranges a key-range scan reads.
//!
//! A table whose record ids name their components stores every record under
//! the encoding of its array key, so the records whose leading components hold
//! given values, and whose next component lies between two bounds, sit in one
//! stretch of the table's keys (one per combination of the values an `IN`
//! lists). [`record_key_ranges`] turns the shape the planner chose
//! ([`ScanKeyRange`]) and the values of its bounds, evaluated when the scan
//! opens, into those stretches.
//!
//! # Contract
//!
//! - **A superset.** The ranges hold every record whose key the bounds admit. They may hold more: a
//!   datetime bound widens to its whole second, and a bound the scan cannot use is dropped. The
//!   predicate the ranges were planned from is judged again over every row the scan yields.
//! - **Never an error.** A bound whose value is not one its component can hold (the wrong kind,
//!   `NONE`, a datetime outside the years 0000 to 9999, a fractional value against an int as an
//!   equality) is not an error: an equality ends the run of pinned components at its position, and
//!   a range bound leaves its side open. An int bound that is fractional rounds inwards, so a range
//!   over ints keeps exactly the ints the comparison admits.
//! - **Within the table, disjoint, in scan order.** Every range is clamped to the table's record
//!   range, a range that is empty or inverted is dropped, overlapping ranges are merged, and the
//!   ranges are listed in the order a scan in the given direction reads them.

use std::cmp::Ordering;

use crate::catalog::ComponentKind;
use crate::key::RawRange;
use crate::kvs::Direction;
use crate::val::{
	Number, RecordIdKey, Value, encode_datetime_second_prefix, try_array_key_prefix,
	try_encode_component,
};

/// The shape of a key-range scan: which bound value constrains which component,
/// and how. Bound values are named by their index into the values the scan
/// evaluates when it opens.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ScanKeyRange {
	/// The kind of every component of the key, in position order.
	pub(crate) kinds: Box<[ComponentKind]>,
	/// The values positions `0..eq.len()` are pinned to.
	pub(crate) eq: Box<[ScanEq]>,
	/// The bounds on position `eq.len()`, if any.
	pub(crate) range: Option<ScanRangeOn>,
}

/// The values one pinned position takes.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ScanEq {
	/// One value.
	One(u16),
	/// Each of several values, one range each.
	In(Box<[u16]>),
}

/// One side of the range on the position after the pinned run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ScanSide {
	/// The index of the bound's value.
	pub(crate) bound: u16,
	/// Whether a key equal to the bound is admitted.
	pub(crate) inclusive: bool,
}

/// The bounds on the position after the pinned run: either side may be open.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ScanRangeOn {
	pub(crate) low: Option<ScanSide>,
	pub(crate) high: Option<ScanSide>,
}

/// Which side of a range a bound limits.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Side {
	Low,
	High,
}

/// `value` as the element an equality on a component of kind `kind` pins, or
/// `None` when no key the component holds can equal it through its encoding.
///
/// An int component holds only a [`Number::Int`]: a float or a decimal equal to
/// an integer pins that integer, and any other number pins nothing. A record
/// component is pinned only by a record id whose key is a string, a number or
/// a uuid, whose encoding is that record id's alone.
pub(crate) fn equality_element(kind: &ComponentKind, value: &Value) -> Option<Value> {
	match kind {
		ComponentKind::Int => {
			if let Value::Number(n) = value {
				n.as_int_lossless().map(|i| Value::Number(Number::Int(i)))
			} else {
				None
			}
		}
		ComponentKind::Record(_) => {
			if let Value::RecordId(rid) = value {
				if matches!(
					rid.key,
					RecordIdKey::String(_) | RecordIdKey::Number(_) | RecordIdKey::Uuid(_)
				) && kind.refusal(value).is_none()
				{
					Some(value.clone())
				} else {
					None
				}
			} else {
				None
			}
		}
		ComponentKind::String
		| ComponentKind::Datetime
		| ComponentKind::Uuid
		| ComponentKind::Duration
		| ComponentKind::Bool => kind.refusal(value).is_none().then(|| value.clone()),
		// A component of an aggregated view's key holds whatever its group
		// expression yielded, and is pinned by a value of a kind whose
		// encoding is that value's alone.
		ComponentKind::Unknown => match value {
			Value::String(_)
			| Value::Uuid(_)
			| Value::Duration(_)
			| Value::Bool(_)
			| Value::Datetime(_) => Some(value.clone()),
			Value::RecordId(rid)
				if matches!(
					rid.key,
					RecordIdKey::String(_) | RecordIdKey::Number(_) | RecordIdKey::Uuid(_)
				) =>
			{
				Some(value.clone())
			}
			Value::None
			| Value::Null
			| Value::Number(_)
			| Value::Date(_)
			| Value::Time(_)
			| Value::Timestamp(_)
			| Value::Interval(_)
			| Value::Array(_)
			| Value::Set(_)
			| Value::Object(_)
			| Value::Geometry(_)
			| Value::Bytes(_)
			| Value::Table(_)
			| Value::RecordId(_)
			| Value::File(_)
			| Value::Regex(_)
			| Value::Range(_)
			| Value::Closure(_) => None,
		},
		// A kind added to the catalog is not one a key range reasons about.
		_ => None,
	}
}

/// `value` as a bound on side `side` of a component of kind `kind`, with
/// whether it is inclusive, or `None` when the side stays open.
///
/// An int bound that is a float or a decimal reads as the integer it equals,
/// or, when fractional, rounds inwards and becomes inclusive (`> 2.5` and
/// `>= 2.5` are `>= 3`); a bound that is not finite or rounds outside the ints
/// is not used. A record component takes no range.
fn side_element(
	kind: &ComponentKind,
	value: &Value,
	inclusive: bool,
	side: Side,
) -> Option<(Value, bool)> {
	match kind {
		ComponentKind::Int => {
			let Value::Number(n) = value else {
				return None;
			};
			if let Some(int) = n.as_int_lossless() {
				return Some((Value::Number(Number::Int(int)), inclusive));
			}
			let rounded = match (n.clone(), side) {
				(Number::Float(f), Side::Low) => Number::Float(f.ceil()),
				(Number::Float(f), Side::High) => Number::Float(f.floor()),
				(Number::F32(f), Side::Low) => Number::F32(f.ceil()),
				(Number::F32(f), Side::High) => Number::F32(f.floor()),
				(Number::F16(f), Side::Low) => {
					Number::F16(surrealdb_types::f16::from_f64(f.to_f64().ceil()))
				}
				(Number::F16(f), Side::High) => {
					Number::F16(surrealdb_types::f16::from_f64(f.to_f64().floor()))
				}
				(Number::Decimal(d), Side::Low) => Number::Decimal(d.ceil()),
				(Number::Decimal(d), Side::High) => Number::Decimal(d.floor()),
				(Number::BigDecimal(d), Side::Low) => Number::BigDecimal(Box::new(d.ceil().ok()?)),
				(Number::BigDecimal(d), Side::High) => {
					Number::BigDecimal(Box::new(d.floor().ok()?))
				}
				(
					Number::Int(_)
					| Number::I8(_)
					| Number::U8(_)
					| Number::I16(_)
					| Number::U16(_)
					| Number::I32(_)
					| Number::U32(_)
					| Number::U64(_),
					_,
				) => return None,
			};
			rounded.as_int_lossless().map(|int| (Value::Number(Number::Int(int)), true))
		}
		ComponentKind::String
		| ComponentKind::Datetime
		| ComponentKind::Uuid
		| ComponentKind::Duration
		| ComponentKind::Bool => kind.refusal(value).is_none().then(|| (value.clone(), inclusive)),
		// A component of an aggregated view's key is bounded by a value of a
		// kind the key encoding orders as it compares; the encoding opens
		// with the kind, in the order the kinds compare.
		ComponentKind::Unknown => matches!(
			value,
			Value::String(_)
				| Value::Uuid(_)
				| Value::Duration(_)
				| Value::Bool(_)
				| Value::Datetime(_)
		)
		.then(|| (value.clone(), inclusive)),
		ComponentKind::Record(_) | _ => None,
	}
}

/// The first byte string after every extension of `bytes`: its last byte
/// below `0xFF` incremented, and what follows it dropped. `None` when every
/// byte is `0xFF`, which no record key is.
fn past_extensions(mut bytes: Vec<u8>) -> Option<Vec<u8>> {
	while let Some(last) = bytes.pop() {
		if last < 0xFF {
			bytes.push(last + 1);
			return Some(bytes);
		}
	}
	None
}

/// The bytes a bound on a component of kind `kind` puts after the prefix `p`
/// of the pinned run, on side `side`: the start of the keys it admits for a
/// lower bound, the end (exclusive) for an upper one. `None` when the bound
/// has no encoding, which leaves the side open.
fn bound_bytes(
	p: &[u8],
	kind: &ComponentKind,
	value: &Value,
	inclusive: bool,
	side: Side,
) -> Option<Vec<u8>> {
	match (kind, value) {
		// A datetime orders only by its whole second: a lower bound starts at
		// the first key of its second, an upper bound ends past the last.
		(ComponentKind::Datetime | ComponentKind::Unknown, Value::Datetime(d)) => {
			let mut bytes = p.to_vec();
			bytes.extend(encode_datetime_second_prefix(d)?);
			match side {
				Side::Low => Some(bytes),
				Side::High => past_extensions(bytes),
			}
		}
		(ComponentKind::Datetime, _) => None,
		_ => {
			let mut bytes = p.to_vec();
			bytes.extend(try_encode_component(value)?);
			// Every key holding the bound's element at this position extends
			// these bytes, so a bound that excludes the element starts (or a
			// bound that admits it ends) past all of them.
			match (side, inclusive) {
				(Side::Low, true) | (Side::High, false) => Some(bytes),
				(Side::Low, false) | (Side::High, true) => past_extensions(bytes),
			}
		}
	}
}

/// The record-key ranges of the table whose record range is `table` that a
/// scan of shape `spec` reads, given `bounds`, the values of its bounds
/// evaluated when the scan opens (`NONE` for a bound whose evaluation
/// failed), listed in the order a scan in `direction` reads them.
///
/// See the module docs for the contract. When the shape pins nothing usable
/// and bounds nothing, the one range is the table's whole record range.
pub(crate) fn record_key_ranges(
	table: &RawRange,
	spec: &ScanKeyRange,
	bounds: &[Value],
	direction: Direction,
) -> Vec<RawRange> {
	let (table_start, table_end) = table.bounds();
	// Every record key is the table's prefix followed by the encoding of the
	// record's key; the table's record range starts one byte past the prefix.
	let Some(head) = table_start.strip_suffix(&[0]) else {
		return vec![table.clone()];
	};
	let value = |slot: u16| bounds.get(slot as usize).unwrap_or(&Value::None);

	// The pinned run, as far as its values are ones the components can hold.
	let mut pinned: Vec<Vec<Value>> = Vec::with_capacity(spec.eq.len());
	for (position, eq) in spec.eq.iter().enumerate() {
		let Some(kind) = spec.kinds.get(position) else {
			break;
		};
		let elements: Option<Vec<Value>> = match eq {
			ScanEq::One(slot) => equality_element(kind, value(*slot)).map(|e| vec![e]),
			ScanEq::In(slots) => {
				let mut elements = Vec::with_capacity(slots.len());
				let mut usable = true;
				for slot in slots.iter() {
					match equality_element(kind, value(*slot)) {
						Some(element) if !elements.contains(&element) => elements.push(element),
						Some(_) => {}
						None => {
							usable = false;
							break;
						}
					}
				}
				usable.then_some(elements)
			}
		};
		match elements {
			Some(elements) if !elements.is_empty() => pinned.push(elements),
			_ => break,
		}
	}
	// The range bounds the position right after the whole run, so a run cut
	// short leaves it unused.
	let range = if pinned.len() == spec.eq.len() {
		spec.range.and_then(|range| {
			let kind = spec.kinds.get(pinned.len())?;
			let side = |held: Option<ScanSide>, side: Side| {
				held.and_then(|held| side_element(kind, value(held.bound), held.inclusive, side))
			};
			let low = side(range.low, Side::Low);
			let high = side(range.high, Side::High);
			(low.is_some() || high.is_some()).then_some((kind, low, high))
		})
	} else {
		None
	};
	if pinned.is_empty() && range.is_none() {
		return vec![table.clone()];
	}

	// One prefix per combination of the pinned values.
	let mut combinations: Vec<Vec<Value>> = vec![Vec::new()];
	for elements in &pinned {
		combinations = combinations
			.into_iter()
			.flat_map(|combination| {
				elements.iter().map(move |element| {
					let mut next = combination.clone();
					next.push(element.clone());
					next
				})
			})
			.collect();
	}

	let mut spans: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(combinations.len());
	for combination in &combinations {
		let Some(tail) = try_array_key_prefix(combination) else {
			return vec![table.clone()];
		};
		let mut p = head.to_vec();
		p.extend(tail);
		let open_end = || past_extensions(p.clone()).unwrap_or_else(|| table_end.to_vec());
		let (start, end) = match &range {
			None => (p.clone(), open_end()),
			Some((kind, low, high)) => {
				let start = low
					.as_ref()
					.and_then(|(v, inclusive)| bound_bytes(&p, kind, v, *inclusive, Side::Low))
					.unwrap_or_else(|| p.clone());
				let end = high
					.as_ref()
					.and_then(|(v, inclusive)| bound_bytes(&p, kind, v, *inclusive, Side::High))
					.unwrap_or_else(open_end);
				(start, end)
			}
		};
		// Clamped to the table, and dropped when empty or inverted.
		let start = start.max(table_start.to_vec());
		let end = end.min(table_end.to_vec());
		if start < end {
			spans.push((start, end));
		}
	}

	// Disjoint, in key order, then in the order the scan reads them.
	spans.sort_by(|a, b| a.0.cmp(&b.0));
	let mut merged: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(spans.len());
	for (start, end) in spans {
		match merged.last_mut() {
			Some(last) if start <= last.1 => {
				if end > last.1 {
					last.1 = end;
				}
			}
			_ => merged.push((start, end)),
		}
	}
	if direction == Direction::Backward {
		merged.reverse();
	}
	merged.into_iter().map(|(start, end)| table.part(&start, &end)).collect()
}

/// Whether a scan in `direction` that reads a record whose key holds `element`
/// at a datetime component has passed the whole second of `floor`: the
/// element's second is after the floor's reading forwards, before it reading
/// backwards. A key-range scan delivers its rows in the order of that
/// component's second, so once it has passed the second of the last row a
/// bounded sort keeps, no row it reads later can take a kept place. `false`
/// when either is not a datetime a key can order.
pub(crate) fn past_second(element: &Value, floor: &Value, direction: Direction) -> bool {
	let (Value::Datetime(element), Value::Datetime(floor)) = (element, floor) else {
		return false;
	};
	let (Some(element), Some(floor)) = (element.key_second_prefix(), floor.key_second_prefix())
	else {
		return false;
	};
	matches!(
		(direction, element.cmp(&floor)),
		(Direction::Forward, Ordering::Greater) | (Direction::Backward, Ordering::Less)
	)
}

/// The element at `position` of the array key of the record id `key`, if it
/// has one.
pub(crate) fn key_element(key: &RecordIdKey, position: u32) -> Option<&Value> {
	if let RecordIdKey::Array(elements) = key {
		elements.get(position as usize)
	} else {
		None
	}
}

#[cfg(test)]
mod tests {
	use std::borrow::Cow;
	use std::str::FromStr;

	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::key::KVKey;
	use crate::key::schema::{RecordKey, RecordPrefix};
	use crate::val::{Array, Datetime, Duration, RecordId, Strand, TableName};

	fn table_range() -> RawRange {
		RecordPrefix {
			ns: NamespaceId(1),
			db: DatabaseId(2),
			tb: Cow::Owned(TableName::from("reading")),
		}
		.range()
		.expect("the table's record range")
	}

	/// The stored key of the record `reading:[elements..]`.
	fn record_key(elements: Vec<Value>) -> Vec<u8> {
		let table = TableName::from("reading");
		let id = RecordIdKey::Array(Array::from(elements));
		RecordKey::new(NamespaceId(1), DatabaseId(2), Cow::Borrowed(&table), Cow::Borrowed(&id))
			.encode_key()
			.expect("a record key")
			.to_vec()
	}

	/// Whether some range of `ranges` holds the stored key of `elements`.
	fn reads(ranges: &[RawRange], elements: Vec<Value>) -> bool {
		let key = record_key(elements);
		ranges.iter().any(|range| {
			let (start, end) = range.bounds();
			start <= key.as_slice() && key.as_slice() < end
		})
	}

	fn s(text: &str) -> Value {
		Value::String(Strand::new(text))
	}

	fn int(i: i64) -> Value {
		Value::Number(Number::Int(i))
	}

	fn dt(text: &str) -> Value {
		Value::Datetime(Datetime::from_str(text).expect("a datetime"))
	}

	fn city_time() -> Box<[ComponentKind]> {
		Box::new([ComponentKind::String, ComponentKind::Datetime])
	}

	fn city_n() -> Box<[ComponentKind]> {
		Box::new([ComponentKind::String, ComponentKind::Int])
	}

	fn shape(
		kinds: Box<[ComponentKind]>,
		eq: Vec<ScanEq>,
		range: Option<ScanRangeOn>,
	) -> ScanKeyRange {
		ScanKeyRange {
			kinds,
			eq: eq.into_boxed_slice(),
			range,
		}
	}

	fn low(bound: u16, inclusive: bool) -> Option<ScanSide> {
		Some(ScanSide {
			bound,
			inclusive,
		})
	}

	fn high(bound: u16, inclusive: bool) -> Option<ScanSide> {
		low(bound, inclusive)
	}

	#[test]
	fn the_stored_key_of_an_array_id_extends_the_prefix_of_its_leading_elements() {
		let table = table_range();
		let (start, _) = table.bounds();
		let head = start.strip_suffix(&[0]).expect("the record range starts past the prefix");
		let key = record_key(vec![s("London"), dt("2025-01-01T10:00:00Z")]);
		let mut prefix = head.to_vec();
		prefix.extend(try_array_key_prefix(&[s("London")]).expect("a prefix"));
		assert!(key.starts_with(&prefix), "{key:?} does not extend {prefix:?}");
	}

	#[test]
	fn an_equality_reads_the_keys_that_begin_with_its_value() {
		let spec = shape(city_time(), vec![ScanEq::One(0)], None);
		let ranges = record_key_ranges(&table_range(), &spec, &[s("London")], Direction::Forward);
		assert_eq!(ranges.len(), 1);
		assert!(reads(&ranges, vec![s("London"), dt("2025-01-01T10:00:00Z")]));
		assert!(reads(&ranges, vec![s("London"), dt("1999-12-31T23:59:59.999Z")]));
		assert!(!reads(&ranges, vec![s("Londonderry"), dt("2025-01-01T10:00:00Z")]));
		assert!(!reads(&ranges, vec![s("Paris"), dt("2025-01-01T10:00:00Z")]));
		assert!(!reads(&ranges, vec![s("Lond"), dt("2025-01-01T10:00:00Z")]));
	}

	#[test]
	fn exact_bounds_admit_what_their_comparison_admits() {
		let ranges = |range: ScanRangeOn| {
			let spec = shape(city_n(), vec![ScanEq::One(0)], Some(range));
			record_key_ranges(&table_range(), &spec, &[s("L"), int(5), int(8)], Direction::Forward)
		};
		let n = |n: i64| vec![s("L"), int(n)];
		// `n >= 5`
		let ge = ranges(ScanRangeOn {
			low: low(1, true),
			high: None,
		});
		assert!(!reads(&ge, n(4)) && reads(&ge, n(5)) && reads(&ge, n(i64::MAX)));
		// `n > 5`
		let gt = ranges(ScanRangeOn {
			low: low(1, false),
			high: None,
		});
		assert!(!reads(&gt, n(5)) && reads(&gt, n(6)));
		// `n < 8`
		let lt = ranges(ScanRangeOn {
			low: None,
			high: high(2, false),
		});
		assert!(reads(&lt, n(i64::MIN)) && reads(&lt, n(7)) && !reads(&lt, n(8)));
		// `n <= 8`
		let le = ranges(ScanRangeOn {
			low: None,
			high: high(2, true),
		});
		assert!(reads(&le, n(8)) && !reads(&le, n(9)));
		// `n > 5 AND n <= 8`
		let both = ranges(ScanRangeOn {
			low: low(1, false),
			high: high(2, true),
		});
		assert!(!reads(&both, n(5)) && reads(&both, n(6)) && reads(&both, n(8)));
		assert!(!reads(&both, n(9)));
		assert!(!reads(&both, vec![s("M"), int(6)]));
	}

	#[test]
	fn a_datetime_bound_widens_to_its_whole_second() {
		let spec = shape(
			city_time(),
			vec![ScanEq::One(0)],
			Some(ScanRangeOn {
				low: low(1, false),
				high: high(2, false),
			}),
		);
		let ranges = record_key_ranges(
			&table_range(),
			&spec,
			&[s("L"), dt("2025-01-01T10:00:00.500Z"), dt("2025-01-01T11:00:00.250Z")],
			Direction::Forward,
		);
		let at = |time: &str| vec![s("L"), dt(time)];
		// The whole second of each bound is read, whatever the comparison.
		assert!(reads(&ranges, at("2025-01-01T10:00:00Z")));
		assert!(reads(&ranges, at("2025-01-01T10:00:00.100Z")));
		assert!(reads(&ranges, at("2025-01-01T10:00:00.999999999Z")));
		assert!(reads(&ranges, at("2025-01-01T10:30:00Z")));
		assert!(reads(&ranges, at("2025-01-01T11:00:00Z")));
		assert!(reads(&ranges, at("2025-01-01T11:00:00.900Z")));
		assert!(!reads(&ranges, at("2025-01-01T09:59:59.999Z")));
		assert!(!reads(&ranges, at("2025-01-01T11:00:01Z")));
	}

	#[test]
	fn an_int_bound_reads_the_integer_a_float_or_decimal_equals() {
		let spec = shape(city_n(), vec![ScanEq::One(0), ScanEq::One(1)], None);
		let five = Value::Number(Number::Float(5.0));
		let ranges = record_key_ranges(&table_range(), &spec, &[s("L"), five], Direction::Forward);
		assert!(reads(&ranges, vec![s("L"), int(5)]));
		assert!(!reads(&ranges, vec![s("L"), int(6)]));
		let decimal = Value::Number(Number::Decimal(rust_decimal::Decimal::new(500, 2)));
		let ranges =
			record_key_ranges(&table_range(), &spec, &[s("L"), decimal], Direction::Forward);
		assert!(reads(&ranges, vec![s("L"), int(5)]));
	}

	#[test]
	fn a_fractional_int_bound_rounds_inwards_and_a_fractional_equality_pins_nothing() {
		let spec = shape(
			city_n(),
			vec![ScanEq::One(0)],
			Some(ScanRangeOn {
				low: low(1, false),
				high: high(2, false),
			}),
		);
		let bounds = [s("L"), Value::from(2.5), Value::from(7.5)];
		let ranges = record_key_ranges(&table_range(), &spec, &bounds, Direction::Forward);
		let n = |n: i64| vec![s("L"), int(n)];
		assert!(!reads(&ranges, n(2)) && reads(&ranges, n(3)));
		assert!(reads(&ranges, n(7)) && !reads(&ranges, n(8)));
		// A fractional equality ends the run before its position: the scan
		// reads every key of the city.
		let spec = shape(city_n(), vec![ScanEq::One(0), ScanEq::One(1)], None);
		let ranges = record_key_ranges(
			&table_range(),
			&spec,
			&[s("L"), Value::from(2.5)],
			Direction::Forward,
		);
		assert!(reads(&ranges, n(2)) && reads(&ranges, n(3)));
		assert!(!reads(&ranges, vec![s("M"), int(2)]));
		// Not finite, or past the ints: the side stays open.
		let spec = shape(
			city_n(),
			vec![ScanEq::One(0)],
			Some(ScanRangeOn {
				low: low(1, true),
				high: None,
			}),
		);
		for bound in [Value::from(f64::NAN), Value::from(f64::INFINITY), Value::from(1e30)] {
			let ranges =
				record_key_ranges(&table_range(), &spec, &[s("L"), bound], Direction::Forward);
			assert!(reads(&ranges, n(i64::MIN)), "the lower side stays open");
		}
	}

	#[test]
	fn an_in_reads_one_range_per_value_sorted_and_deduplicated() {
		let spec = shape(city_time(), vec![ScanEq::In(vec![0, 1, 2].into_boxed_slice())], None);
		let bounds = [s("Paris"), s("London"), s("Paris")];
		let forward = record_key_ranges(&table_range(), &spec, &bounds, Direction::Forward);
		assert_eq!(forward.len(), 2);
		assert!(forward[0].bounds().0 < forward[1].bounds().0, "forward reads in key order");
		assert!(reads(&forward[..1], vec![s("London"), dt("2025-01-01T10:00:00Z")]));
		assert!(reads(&forward[1..], vec![s("Paris"), dt("2025-01-01T10:00:00Z")]));
		let backward = record_key_ranges(&table_range(), &spec, &bounds, Direction::Backward);
		assert_eq!(backward.len(), 2);
		assert!(backward[0].bounds().0 > backward[1].bounds().0, "backward reads in reverse");
		// Two values that pin the same integer are one range.
		let spec =
			shape(city_n(), vec![ScanEq::One(0), ScanEq::In(vec![1, 2].into_boxed_slice())], None);
		let ranges = record_key_ranges(
			&table_range(),
			&spec,
			&[s("L"), int(5), Value::from(5.0)],
			Direction::Forward,
		);
		assert_eq!(ranges.len(), 1);
	}

	#[test]
	fn an_in_holding_a_value_its_component_cannot_hold_ends_the_run() {
		let spec =
			shape(city_n(), vec![ScanEq::One(0), ScanEq::In(vec![1, 2].into_boxed_slice())], None);
		let ranges = record_key_ranges(
			&table_range(),
			&spec,
			&[s("L"), int(5), Value::None],
			Direction::Forward,
		);
		assert_eq!(ranges.len(), 1);
		assert!(reads(&ranges, vec![s("L"), int(9)]));
		assert!(!reads(&ranges, vec![s("M"), int(5)]));
	}

	#[test]
	fn inverted_or_empty_bounds_read_nothing() {
		let spec = shape(
			city_n(),
			vec![ScanEq::One(0)],
			Some(ScanRangeOn {
				low: low(1, false),
				high: high(2, false),
			}),
		);
		// `n > 5 AND n < 3`
		let ranges =
			record_key_ranges(&table_range(), &spec, &[s("L"), int(5), int(3)], Direction::Forward);
		assert!(ranges.is_empty());
		// `n > 5 AND n < 6` admits no int: whatever bytes lie between the two
		// encodings, no int's key is among them.
		let ranges =
			record_key_ranges(&table_range(), &spec, &[s("L"), int(5), int(6)], Direction::Forward);
		assert!(!reads(&ranges, vec![s("L"), int(5)]));
		assert!(!reads(&ranges, vec![s("L"), int(6)]));
		// Inverted datetimes, evaluated when the scan opens.
		let spec = shape(
			city_time(),
			vec![ScanEq::One(0)],
			Some(ScanRangeOn {
				low: low(1, true),
				high: high(2, true),
			}),
		);
		let ranges = record_key_ranges(
			&table_range(),
			&spec,
			&[s("L"), dt("2025-01-02T00:00:00Z"), dt("2025-01-01T00:00:00Z")],
			Direction::Forward,
		);
		assert!(ranges.is_empty());
	}

	#[test]
	fn a_bound_its_component_cannot_hold_widens_rather_than_raising() {
		let spec = shape(
			city_time(),
			vec![ScanEq::One(0)],
			Some(ScanRangeOn {
				low: low(1, true),
				high: high(2, true),
			}),
		);
		// A datetime past the year 9999 and a string where a datetime goes:
		// both sides open, the city still pinned.
		let year_10000 = Value::Datetime(Datetime(
			chrono::DateTime::from_timestamp(253_402_300_800, 0).expect("a datetime"),
		));
		let ranges = record_key_ranges(
			&table_range(),
			&spec,
			&[s("L"), year_10000, s("tomorrow")],
			Direction::Forward,
		);
		assert_eq!(ranges.len(), 1);
		assert!(reads(&ranges, vec![s("L"), dt("0001-01-01T00:00:00Z")]));
		assert!(reads(&ranges, vec![s("L"), dt("9999-12-31T23:59:59Z")]));
		assert!(!reads(&ranges, vec![s("M"), dt("2025-01-01T00:00:00Z")]));
		// A failed or mistyped equality on the first position: nothing pinned,
		// and the range after it unused, so the table is read whole.
		let ranges = record_key_ranges(
			&table_range(),
			&spec,
			&[Value::None, dt("2025-01-01T00:00:00Z"), dt("2025-01-02T00:00:00Z")],
			Direction::Forward,
		);
		assert_eq!(ranges.len(), 1);
		assert_eq!(ranges[0].bounds(), table_range().bounds());
	}

	#[test]
	fn every_range_lies_within_the_table() {
		let table = table_range();
		let (table_start, table_end) = table.bounds();
		let spec = shape(
			Box::new([ComponentKind::Duration]),
			Vec::new(),
			Some(ScanRangeOn {
				low: None,
				high: high(0, true),
			}),
		);
		let bound = Value::Duration(Duration::from_secs(60));
		let ranges = record_key_ranges(&table, &spec, &[bound], Direction::Forward);
		assert_eq!(ranges.len(), 1);
		let (start, end) = ranges[0].bounds();
		assert!(table_start <= start && end <= table_end);
		let spec = shape(city_time(), vec![ScanEq::One(0)], None);
		for bound in [s(""), s("\u{10FFFF}\u{10FFFF}")] {
			for range in record_key_ranges(&table, &spec, &[bound], Direction::Backward) {
				let (start, end) = range.bounds();
				assert!(table_start <= start && end <= table_end && start < end);
			}
		}
	}

	#[test]
	fn a_record_component_is_pinned_only_by_a_record_with_a_scalar_key() {
		let kinds: Box<[ComponentKind]> =
			Box::new([ComponentKind::Record(Box::new([])), ComponentKind::Int]);
		let spec = shape(kinds, vec![ScanEq::One(0)], None);
		let person = Value::RecordId(RecordId::new(TableName::from("person"), "tobie".to_string()));
		let ranges = record_key_ranges(
			&table_range(),
			&spec,
			std::slice::from_ref(&person),
			Direction::Forward,
		);
		assert!(reads(&ranges, vec![person, int(1)]));
		let compound = Value::RecordId(RecordId {
			table: TableName::from("person"),
			key: RecordIdKey::Array(Array::from(vec![int(1)])),
		});
		let ranges = record_key_ranges(&table_range(), &spec, &[compound], Direction::Forward);
		assert_eq!(ranges[0].bounds(), table_range().bounds());
	}

	#[test]
	fn a_view_component_is_bounded_by_the_kind_of_its_value() {
		// An aggregated view's key: every group value of unknown kind.
		let kinds: Box<[ComponentKind]> =
			Box::new([ComponentKind::Unknown, ComponentKind::Unknown]);
		let spec = shape(
			kinds.clone(),
			vec![ScanEq::One(0)],
			Some(ScanRangeOn {
				low: low(1, true),
				high: high(2, true),
			}),
		);
		let ranges = record_key_ranges(
			&table_range(),
			&spec,
			&[s("L"), dt("2025-01-01T10:00:00Z"), dt("2025-01-01T10:00:05.500Z")],
			Direction::Forward,
		);
		let at = |time: &str| vec![s("L"), dt(time)];
		assert!(reads(&ranges, at("2025-01-01T10:00:00Z")));
		assert!(reads(&ranges, at("2025-01-01T10:00:05.900Z")));
		assert!(!reads(&ranges, at("2025-01-01T10:00:06Z")));
		assert!(!reads(&ranges, vec![s("M"), dt("2025-01-01T10:00:01Z")]));
		// A number is never a view key's bound: numbers of different kinds
		// that are equal do not share a key.
		let pinned = shape(kinds, vec![ScanEq::One(0)], None);
		let ranges = record_key_ranges(&table_range(), &pinned, &[int(3)], Direction::Forward);
		assert_eq!(ranges.len(), 1);
		assert_eq!(ranges[0].bounds(), table_range().bounds());
	}

	#[test]
	fn a_scan_passes_a_second_only_once_it_reads_a_later_one() {
		let floor = dt("2025-01-01T10:00:00.500Z");
		let forward = |at: &str| past_second(&dt(at), &floor, Direction::Forward);
		let backward = |at: &str| past_second(&dt(at), &floor, Direction::Backward);
		assert!(!forward("2025-01-01T10:00:00Z"));
		assert!(!forward("2025-01-01T10:00:00.999Z"));
		assert!(forward("2025-01-01T10:00:01Z"));
		assert!(!forward("2025-01-01T09:59:59Z"));
		assert!(!backward("2025-01-01T10:00:00.001Z"));
		assert!(backward("2025-01-01T09:59:59.999Z"));
		assert!(!past_second(&s("x"), &floor, Direction::Forward));
		assert!(!past_second(&dt("2025-01-01T10:00:01Z"), &int(1), Direction::Forward));
	}
}
