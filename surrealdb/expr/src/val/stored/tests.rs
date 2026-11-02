use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use geo_types::{Coord, LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon};
use rust_decimal::Decimal;
use surrealdb_strand::Strand;

use super::tag::*;
use super::*;
use crate::val::{
	Array, Bytes, Date, Datetime, Duration, File, Geometry, Interval, MAX_VALUE_DEPTH, Number,
	Object, Range, RecordId, RecordIdKey, RecordIdKeyRange, Regex, Set, TableName, Time, Timestamp,
	Uuid, Value, check_value_depth,
};

// ---------------------------------------------------------------------------
// Helpers

fn int(i: i64) -> Value {
	Value::Number(Number::Int(i))
}

fn float(f: f64) -> Value {
	Value::Number(Number::Float(f))
}

fn string(s: &str) -> Value {
	Value::String(Strand::new(s))
}

fn array(v: Vec<Value>) -> Value {
	Value::Array(Array(v))
}

fn object(entries: Vec<(&str, Value)>) -> Value {
	Value::Object(Object::from(entries.into_iter().collect::<BTreeMap<&str, Value>>()))
}

fn rid(table: &str, key: RecordIdKey) -> Value {
	Value::RecordId(RecordId {
		table: TableName::new(table),
		key,
	})
}

fn datetime(secs: i64, nanos: u32) -> Value {
	Value::Datetime(Datetime(DateTime::from_timestamp(secs, nanos).expect("in range")))
}

/// Encodes `v`, decodes it, and checks the result is `v` exactly — the same
/// variants, float bits and decimal scales, not merely an equal value — that
/// it encodes to the same bytes, and that the bytes read in place as `v`.
fn round_trip(v: &Value) -> Vec<u8> {
	let bytes = encode(v).expect("encodes");
	let back = decode(&bytes).unwrap_or_else(|e| panic!("{v:?} → {bytes:02x?} fails: {e}"));
	assert!(identical(&back, v), "{v:?} reads back as {back:?}\nbytes {bytes:02x?}");
	assert_eq!(encode(&back).expect("re-encodes"), bytes, "re-encoding {v:?}");
	read_in_place(ValueRef::new(&bytes).unwrap(), v, true);
	bytes
}

/// How many generated cases a test runs: `ci`, unless `STORED_VALUE_CASES`
/// names another count, for a longer run than CI's.
fn cases(ci: usize) -> usize {
	std::env::var("STORED_VALUE_CASES").ok().and_then(|n| n.parse().ok()).unwrap_or(ci)
}

/// The seed a generated test starts from: `ci`, unless `STORED_VALUE_SEED`
/// names another, so that a longer run need not repeat CI's cases.
fn seed(ci: u64) -> u64 {
	std::env::var("STORED_VALUE_SEED").ok().and_then(|s| s.parse().ok()).unwrap_or(ci).max(1)
}

// ---------------------------------------------------------------------------
// Strict comparison

/// Whether `a` and `b` are the same value: the same variant at every level,
/// floats with the same bits, decimals with the same scale and sign, and
/// entries and elements in the same order. `Value`'s `==` is numeric — `1 ==
/// 1.0`, `0.0 == -0.0` and `1.5 == 1.50` — and its `Debug` prints every NaN
/// alike, so neither tells a round trip that loses something from one that
/// does not.
fn identical(a: &Value, b: &Value) -> bool {
	match (a, b) {
		(Value::Number(a), Value::Number(b)) => same_number(a, b),
		(Value::Array(a), Value::Array(b)) => all_identical(&a.0, &b.0),
		(Value::Set(a), Value::Set(b)) => {
			a.0.len() == b.0.len() && a.0.iter().zip(b.0.iter()).all(|(a, b)| identical(a, b))
		}
		(Value::Object(a), Value::Object(b)) => same_object(a, b),
		(Value::Geometry(a), Value::Geometry(b)) => same_geometry(a, b),
		(Value::Interval(a), Value::Interval(b)) => a.identical(b),
		(Value::RecordId(a), Value::RecordId(b)) => same_record_id(a, b),
		(Value::Range(a), Value::Range(b)) => {
			same_bound(&a.start, &b.start, identical) && same_bound(&a.end, &b.end, identical)
		}
		// Every other variant's `==` is exact.
		(a, b) => a == b,
	}
}

fn all_identical(a: &[Value], b: &[Value]) -> bool {
	a.len() == b.len() && a.iter().zip(b).all(|(a, b)| identical(a, b))
}

fn same_number(a: &Number, b: &Number) -> bool {
	match a {
		Number::Int(a) => match b {
			Number::Int(b) => a == b,
			Number::Float(_)
			| Number::Decimal(_)
			| Number::BigDecimal(_)
			| Number::I8(_)
			| Number::U8(_)
			| Number::I16(_)
			| Number::U16(_)
			| Number::I32(_)
			| Number::U32(_)
			| Number::U64(_)
			| Number::F16(_)
			| Number::F32(_) => false,
		},
		Number::Float(a) => match b {
			Number::Float(b) => a.to_bits() == b.to_bits(),
			Number::Int(_)
			| Number::Decimal(_)
			| Number::BigDecimal(_)
			| Number::I8(_)
			| Number::U8(_)
			| Number::I16(_)
			| Number::U16(_)
			| Number::I32(_)
			| Number::U32(_)
			| Number::U64(_)
			| Number::F16(_)
			| Number::F32(_) => false,
		},
		Number::BigDecimal(a) => matches!(b, Number::BigDecimal(b) if a == b),
		Number::Decimal(a) => match b {
			// The serialized form holds the sign and scale as well as the mantissa.
			Number::Decimal(b) => a.serialize() == b.serialize(),
			Number::Int(_)
			| Number::Float(_)
			| Number::BigDecimal(_)
			| Number::I8(_)
			| Number::U8(_)
			| Number::I16(_)
			| Number::U16(_)
			| Number::I32(_)
			| Number::U32(_)
			| Number::U64(_)
			| Number::F16(_)
			| Number::F32(_) => false,
		},
		Number::I8(a) => match b {
			Number::I8(b) => a == b,
			Number::Int(_)
			| Number::Float(_)
			| Number::Decimal(_)
			| Number::BigDecimal(_)
			| Number::U8(_)
			| Number::I16(_)
			| Number::U16(_)
			| Number::I32(_)
			| Number::U32(_)
			| Number::U64(_)
			| Number::F16(_)
			| Number::F32(_) => false,
		},
		Number::U8(a) => match b {
			Number::U8(b) => a == b,
			Number::Int(_)
			| Number::Float(_)
			| Number::Decimal(_)
			| Number::BigDecimal(_)
			| Number::I8(_)
			| Number::I16(_)
			| Number::U16(_)
			| Number::I32(_)
			| Number::U32(_)
			| Number::U64(_)
			| Number::F16(_)
			| Number::F32(_) => false,
		},
		Number::I16(a) => match b {
			Number::I16(b) => a == b,
			Number::Int(_)
			| Number::Float(_)
			| Number::Decimal(_)
			| Number::BigDecimal(_)
			| Number::I8(_)
			| Number::U8(_)
			| Number::U16(_)
			| Number::I32(_)
			| Number::U32(_)
			| Number::U64(_)
			| Number::F16(_)
			| Number::F32(_) => false,
		},
		Number::U16(a) => match b {
			Number::U16(b) => a == b,
			Number::Int(_)
			| Number::Float(_)
			| Number::Decimal(_)
			| Number::BigDecimal(_)
			| Number::I8(_)
			| Number::U8(_)
			| Number::I16(_)
			| Number::I32(_)
			| Number::U32(_)
			| Number::U64(_)
			| Number::F16(_)
			| Number::F32(_) => false,
		},
		Number::I32(a) => match b {
			Number::I32(b) => a == b,
			Number::Int(_)
			| Number::Float(_)
			| Number::Decimal(_)
			| Number::BigDecimal(_)
			| Number::I8(_)
			| Number::U8(_)
			| Number::I16(_)
			| Number::U16(_)
			| Number::U32(_)
			| Number::U64(_)
			| Number::F16(_)
			| Number::F32(_) => false,
		},
		Number::U32(a) => match b {
			Number::U32(b) => a == b,
			Number::Int(_)
			| Number::Float(_)
			| Number::Decimal(_)
			| Number::BigDecimal(_)
			| Number::I8(_)
			| Number::U8(_)
			| Number::I16(_)
			| Number::U16(_)
			| Number::I32(_)
			| Number::U64(_)
			| Number::F16(_)
			| Number::F32(_) => false,
		},
		Number::U64(a) => match b {
			Number::U64(b) => a == b,
			Number::Int(_)
			| Number::Float(_)
			| Number::Decimal(_)
			| Number::BigDecimal(_)
			| Number::I8(_)
			| Number::U8(_)
			| Number::I16(_)
			| Number::U16(_)
			| Number::I32(_)
			| Number::U32(_)
			| Number::F16(_)
			| Number::F32(_) => false,
		},
		Number::F16(a) => match b {
			Number::F16(b) => a.to_bits() == b.to_bits(),
			Number::Int(_)
			| Number::Float(_)
			| Number::Decimal(_)
			| Number::BigDecimal(_)
			| Number::I8(_)
			| Number::U8(_)
			| Number::I16(_)
			| Number::U16(_)
			| Number::I32(_)
			| Number::U32(_)
			| Number::U64(_)
			| Number::F32(_) => false,
		},
		Number::F32(a) => match b {
			Number::F32(b) => a.to_bits() == b.to_bits(),
			Number::Int(_)
			| Number::Float(_)
			| Number::Decimal(_)
			| Number::BigDecimal(_)
			| Number::I8(_)
			| Number::U8(_)
			| Number::I16(_)
			| Number::U16(_)
			| Number::I32(_)
			| Number::U32(_)
			| Number::U64(_)
			| Number::F16(_) => false,
		},
	}
}

fn same_object(a: &Object, b: &Object) -> bool {
	a.0.len() == b.0.len()
		&& a.0.iter().zip(b.0.iter()).all(|((ka, va), (kb, vb))| ka == kb && identical(va, vb))
}

fn same_record_id(a: &RecordId, b: &RecordId) -> bool {
	a.table == b.table && same_key(&a.key, &b.key)
}

fn same_key(a: &RecordIdKey, b: &RecordIdKey) -> bool {
	match (a, b) {
		(RecordIdKey::Array(a), RecordIdKey::Array(b)) => all_identical(&a.0, &b.0),
		(RecordIdKey::Object(a), RecordIdKey::Object(b)) => same_object(a, b),
		(RecordIdKey::Range(a), RecordIdKey::Range(b)) => {
			same_bound(&a.start, &b.start, same_key) && same_bound(&a.end, &b.end, same_key)
		}
		(a, b) => a == b,
	}
}

fn same_bound<T>(a: &Bound<T>, b: &Bound<T>, same: fn(&T, &T) -> bool) -> bool {
	match (a, b) {
		(Bound::Unbounded, Bound::Unbounded) => true,
		(Bound::Included(a), Bound::Included(b)) | (Bound::Excluded(a), Bound::Excluded(b)) => {
			same(a, b)
		}
		_ => false,
	}
}

/// Geometries compare coordinate by coordinate, bit for bit, and a collection
/// member by member, since its coordinates drop its members' kinds.
fn same_geometry(a: &Geometry, b: &Geometry) -> bool {
	match (a, b) {
		(Geometry::Collection(a), Geometry::Collection(b)) => {
			a.len() == b.len() && a.iter().zip(b).all(|(a, b)| same_geometry(a, b))
		}
		(a, b) => a.as_type() == b.as_type() && identical(&a.as_coordinates(), &b.as_coordinates()),
	}
}

/// Whether `v`, or any value inside it, satisfies `f`.
fn holds(v: &Value, f: &impl Fn(&Value) -> bool) -> bool {
	f(v) || match v {
		Value::Array(a) => a.0.iter().any(|v| holds(v, f)),
		Value::Set(s) => s.0.iter().any(|v| holds(v, f)),
		Value::Object(o) => o.0.values().any(|v| holds(v, f)),
		Value::Range(r) => [&r.start, &r.end].into_iter().any(|b| match b {
			Bound::Included(v) | Bound::Excluded(v) => holds(v, f),
			Bound::Unbounded => false,
		}),
		Value::RecordId(rid) => holds(&rid.key.clone().into_value(), f),
		Value::None
		| Value::Null
		| Value::Bool(_)
		| Value::Number(_)
		| Value::String(_)
		| Value::Duration(_)
		| Value::Datetime(_)
		| Value::Date(_)
		| Value::Time(_)
		| Value::Timestamp(_)
		| Value::Interval(_)
		| Value::Uuid(_)
		| Value::Geometry(_)
		| Value::Bytes(_)
		| Value::Table(_)
		| Value::File(_)
		| Value::Regex(_)
		| Value::Closure(_) => false,
	}
}

// ---------------------------------------------------------------------------
// The in-place reader against the decoder

/// Checks the in-place reading of `r` against `v`, the decoder's reading of the
/// same bytes, at every level: each accessor answers for exactly the variant
/// `v` has; each field and element found in place is the decoder's, at the
/// same offset, whatever hint a lookup starts from; and fields and elements
/// that are not there are not found.
///
/// `canonical` bytes are the encoder's, in which a set's elements are in value
/// order and every non-empty array of floats takes a typed form. Other bytes
/// the decoder accepts may differ in both.
fn read_in_place(r: ValueRef<'_>, v: &Value, canonical: bool) {
	assert_eq!(r.kind(), kind(v), "the kind of {v:?}");
	// A container or a record id is checked part by part below; anything else
	// is decoded from where the reader found it, so each byte is decoded once.
	if !matches!(v, Value::Array(_) | Value::Set(_) | Value::Object(_) | Value::RecordId(_)) {
		let back = r.decode().unwrap_or_else(|e| panic!("in place, {v:?} does not decode: {e}"));
		assert!(identical(&back, v), "in place {back:?}\ndecoded  {v:?}");
	}
	assert_eq!(
		r.as_bool(),
		if let Value::Bool(b) = v {
			Some(*b)
		} else {
			None
		}
	);
	let expected_int = match v {
		Value::Number(n) => match n {
			Number::Int(i) => Some(*i),
			Number::I8(i) => Some(*i as i64),
			Number::U8(u) => Some(*u as i64),
			Number::I16(i) => Some(*i as i64),
			Number::U16(u) => Some(*u as i64),
			Number::I32(i) => Some(*i as i64),
			Number::U32(u) => Some(*u as i64),
			Number::U64(u) if *u <= i64::MAX as u64 => Some(*u as i64),
			Number::U64(_)
			| Number::Float(_)
			| Number::Decimal(_)
			| Number::BigDecimal(_)
			| Number::F16(_)
			| Number::F32(_) => None,
		},
		Value::None
		| Value::Null
		| Value::Bool(_)
		| Value::String(_)
		| Value::Duration(_)
		| Value::Datetime(_)
		| Value::Date(_)
		| Value::Time(_)
		| Value::Timestamp(_)
		| Value::Interval(_)
		| Value::Uuid(_)
		| Value::Array(_)
		| Value::Set(_)
		| Value::Object(_)
		| Value::Geometry(_)
		| Value::Bytes(_)
		| Value::Table(_)
		| Value::RecordId(_)
		| Value::Range(_)
		| Value::File(_)
		| Value::Regex(_)
		| Value::Closure(_) => None,
	};
	assert_eq!(r.as_int().unwrap(), expected_int);
	let expected_float = match v {
		Value::Number(n) => match n {
			Number::Float(f) => Some(f.to_bits()),
			Number::F16(f) => Some(f.to_f64().to_bits()),
			Number::F32(f) => Some((*f as f64).to_bits()),
			Number::Int(_)
			| Number::Decimal(_)
			| Number::BigDecimal(_)
			| Number::I8(_)
			| Number::U8(_)
			| Number::I16(_)
			| Number::U16(_)
			| Number::I32(_)
			| Number::U32(_)
			| Number::U64(_) => None,
		},
		Value::None
		| Value::Null
		| Value::Bool(_)
		| Value::String(_)
		| Value::Duration(_)
		| Value::Datetime(_)
		| Value::Date(_)
		| Value::Time(_)
		| Value::Timestamp(_)
		| Value::Interval(_)
		| Value::Uuid(_)
		| Value::Array(_)
		| Value::Set(_)
		| Value::Object(_)
		| Value::Geometry(_)
		| Value::Bytes(_)
		| Value::Table(_)
		| Value::RecordId(_)
		| Value::Range(_)
		| Value::File(_)
		| Value::Regex(_)
		| Value::Closure(_) => None,
	};
	assert_eq!(r.as_float().unwrap().map(f64::to_bits), expected_float);
	assert_eq!(
		r.as_decimal().unwrap().map(|d| d.serialize()),
		if let Value::Number(Number::Decimal(d)) = v {
			Some(d.serialize())
		} else {
			None
		}
	);
	assert_eq!(
		r.as_str().unwrap(),
		if let Value::String(s) = v {
			Some(s.as_str())
		} else {
			None
		}
	);
	assert_eq!(
		r.as_bytes(),
		if let Value::Bytes(b) = v {
			Some(&b.0[..])
		} else {
			None
		}
	);
	assert_eq!(
		r.as_duration().unwrap(),
		if let Value::Duration(d) = v {
			Some(d.0)
		} else {
			None
		}
	);
	assert_eq!(
		r.as_datetime().unwrap(),
		if let Value::Datetime(d) = v {
			Some(d.0)
		} else {
			None
		}
	);
	assert_eq!(
		r.as_uuid().unwrap(),
		if let Value::Uuid(u) = v {
			Some(u.0)
		} else {
			None
		}
	);
	assert_eq!(
		r.as_table().unwrap(),
		if let Value::Table(t) = v {
			Some(t.as_str())
		} else {
			None
		}
	);
	match (r.as_record_id().unwrap(), v) {
		(Some((table, key)), Value::RecordId(rid)) => {
			assert_eq!(table, rid.table.as_str());
			key_in_place(key, &rid.key, canonical);
		}
		(None, Value::RecordId(_)) | (Some(_), _) => panic!("as_record_id disagrees with {v:?}"),
		(None, _) => {}
	}
	match (r.as_object().unwrap(), v) {
		(Some(o), Value::Object(obj)) => object_in_place(o, obj, canonical),
		(None, Value::Object(_)) | (Some(_), _) => panic!("as_object disagrees with {v:?}"),
		(None, _) => {}
	}
	match (r.as_array().unwrap(), v) {
		(Some(a), Value::Array(arr)) => array_in_place(a, &arr.0, canonical),
		(None, Value::Array(_)) | (Some(_), _) => panic!("as_array disagrees with {v:?}"),
		(None, _) => {}
	}
	match (r.as_set().unwrap(), v) {
		(Some(a), Value::Set(set)) => set_in_place(a, set, canonical),
		(None, Value::Set(_)) | (Some(_), _) => panic!("as_set disagrees with {v:?}"),
		(None, _) => {}
	}
}

/// [`read_in_place`] for a record id's key, read in place as a value.
fn key_in_place(r: ValueRef<'_>, key: &RecordIdKey, canonical: bool) {
	match key {
		RecordIdKey::Number(n) => {
			assert_eq!((r.kind(), r.as_int().unwrap()), (Kind::Int, Some(*n)))
		}
		RecordIdKey::String(s) => {
			assert_eq!((r.kind(), r.as_str().unwrap()), (Kind::String, Some(s.as_str())))
		}
		RecordIdKey::Uuid(u) => {
			assert_eq!((r.kind(), r.as_uuid().unwrap()), (Kind::Uuid, Some(u.0)))
		}
		RecordIdKey::Array(a) => {
			array_in_place(r.as_array().unwrap().expect("an array key"), &a.0, canonical)
		}
		RecordIdKey::Object(o) => {
			object_in_place(r.as_object().unwrap().expect("an object key"), o, canonical)
		}
		// A range has no reader in place: its bytes decode, as a key and as a
		// value, to the key the decoder read.
		RecordIdKey::Range(_) => {
			let mut bytes = Vec::new();
			r.write_to(&mut bytes);
			let k = decode_key(&bytes).unwrap();
			assert!(same_key(&k, key), "key in place {k:?}\ndecoded      {key:?}");
			let back = r.decode().unwrap();
			assert!(identical(&back, &key.clone().into_value()), "in place {back:?}\nkey {key:?}");
		}
	}
}

fn object_in_place(o: ObjectRef<'_>, obj: &Object, canonical: bool) {
	let o = o.ordered().expect("a decoded object's keys ascend");
	let n = obj.0.len();
	assert_eq!((o.len(), o.is_empty()), (n, n == 0));
	// Every field of a small object is looked up from every hint; a sample of
	// a large one's.
	let stride = (n / 64).max(1);
	let mut entries = o.iter();
	for (i, (key, value)) in obj.0.iter().enumerate() {
		let (k, at) = entries.next().expect("an entry for every field").unwrap();
		assert_eq!(k, key.as_str());
		let (k2, at2) = o.entry(i).unwrap();
		assert_eq!(k2, k);
		same_place(at2, at);
		same_place(o.value_at(i).unwrap(), at);
		same_place(o.get(k).unwrap().unwrap_or_else(|| panic!("{k:?} is not found")), at);
		for start in hints(i, n).into_iter().filter(|_| i % stride == 0) {
			let mut hint = start;
			let found = o.get_hinted(k, &mut hint).unwrap();
			same_place(found.unwrap_or_else(|| panic!("{k:?} is not found from {start}")), at);
			assert_eq!(hint as usize, i, "the hint after finding {k:?} from {start}");
		}
		read_in_place(at, value, canonical);
	}
	assert!(entries.next().is_none(), "more entries than fields");
	for key in absent(obj) {
		assert_eq!(o.get(&key).unwrap(), None, "{key:?} is found but absent");
		for start in hints(n / 2, n) {
			let mut hint = start;
			assert_eq!(o.get_hinted(&key, &mut hint).unwrap(), None, "{key:?} from {start}");
			assert_eq!(hint, start, "a miss for {key:?} moves the hint");
		}
	}
}

/// Hints to start a lookup of entry `i` of `n` from: none, the entry, its
/// neighbours, both ends, and past the end.
fn hints(i: usize, n: usize) -> [u32; 9] {
	let (i, n) = (i as u32, n as u32);
	[u32::MAX, 0, i.saturating_sub(1), i, i + 1, i + 2, n.saturating_sub(1), n, n + 1]
}

/// Keys next to the object's own, in byte order, that it does not hold.
fn absent(obj: &Object) -> Vec<String> {
	let mut keys = vec![String::new(), "\u{10FFFF}".repeat(2)];
	for k in obj.0.keys().step_by((obj.0.len() / 8).max(1)) {
		let k = k.as_str();
		let mut shorter = k.to_owned();
		shorter.pop();
		keys.extend([format!("{k}\0"), format!("{k}~"), shorter]);
	}
	keys.retain(|k| !obj.0.contains_key(k.as_str()));
	keys
}

/// Two in-place readings of the same bytes, not merely of equal ones.
fn same_place(a: ValueRef<'_>, b: ValueRef<'_>) {
	assert_eq!(a.tag(), b.tag());
	assert!(
		std::ptr::eq(a.payload(), b.payload()),
		"{a:?} and {b:?} are read at different offsets"
	);
}

fn array_in_place(a: ArrayRef<'_>, items: &[Value], canonical: bool) {
	let n = items.len();
	assert_eq!((a.len(), a.is_empty()), (n, n == 0));
	let mut elements = a.iter();
	for (i, item) in items.iter().enumerate() {
		let at = a.get(i).unwrap().unwrap_or_else(|| panic!("no element {i} of {n}"));
		same_place(elements.next().expect("an element for every item").unwrap(), at);
		read_in_place(at, item, canonical);
	}
	assert!(elements.next().is_none(), "more elements than items");
	for i in [n, n + 1, usize::MAX] {
		assert_eq!(a.get(i).unwrap(), None, "element {i} of {n}");
	}
	let floats: Option<Vec<u64>> = items
		.iter()
		.map(|v| {
			if let Value::Number(Number::Float(f)) = v {
				Some(f.to_bits())
			} else {
				None
			}
		})
		.collect();
	match a.floats() {
		Some(read) => assert_eq!(Some(read.map(f64::to_bits).collect()), floats),
		None => assert!(
			!canonical || n == 0 || floats.is_none(),
			"the encoder wrote an array of floats untyped"
		),
	}
}

fn set_in_place(a: ArrayRef<'_>, set: &Set, canonical: bool) {
	assert!(a.floats().is_none(), "a set read as floats");
	let at: Vec<ValueRef<'_>> = a.iter().map(|e| e.unwrap()).collect();
	assert_eq!(a.get(at.len()).unwrap(), None);
	// The encoder writes a set's elements in value order, so in its bytes the
	// elements in place are the set's, one for one.
	if canonical {
		assert_eq!(at.len(), set.0.len(), "a set's elements in place");
		for (e, item) in at.iter().zip(set.0.iter()) {
			read_in_place(*e, item, canonical);
		}
		return;
	}
	// Elsewhere the decoder orders and deduplicates elements out of order.
	let mut elements: Vec<Value> = at.iter().map(|e| e.decode().unwrap()).collect();
	for (e, v) in at.iter().zip(&elements) {
		read_in_place(*e, v, canonical);
	}
	if !elements.windows(2).all(|w| w[0] < w[1]) {
		elements.sort_unstable();
		elements.dedup();
	}
	let set: Vec<Value> = set.0.iter().cloned().collect();
	assert!(all_identical(&elements, &set), "in place {elements:?}\ndecoded  {set:?}");
}

fn kind(v: &Value) -> Kind {
	match v {
		Value::None => Kind::None,
		Value::Null => Kind::Null,
		Value::Bool(_) => Kind::Bool,
		Value::Number(n) => match n {
			Number::Int(_) => Kind::Int,
			Number::Float(_) => Kind::Float,
			Number::Decimal(_) => Kind::Decimal,
			Number::BigDecimal(_) => Kind::BigDecimal,
			Number::I8(_) => Kind::I8,
			Number::U8(_) => Kind::U8,
			Number::I16(_) => Kind::I16,
			Number::U16(_) => Kind::U16,
			Number::I32(_) => Kind::I32,
			Number::U32(_) => Kind::U32,
			Number::U64(_) => Kind::U64,
			Number::F16(_) => Kind::F16,
			Number::F32(_) => Kind::F32,
		},
		Value::String(_) => Kind::String,
		Value::Duration(_) => Kind::Duration,
		Value::Datetime(_) => Kind::Datetime,
		Value::Date(_) => Kind::Date,
		Value::Time(_) => Kind::Time,
		Value::Timestamp(_) => Kind::Timestamp,
		Value::Interval(_) => Kind::Interval,
		Value::Uuid(_) => Kind::Uuid,
		Value::Bytes(_) => Kind::Bytes,
		Value::Table(_) => Kind::Table,
		Value::RecordId(_) => Kind::RecordId,
		Value::File(_) => Kind::File,
		Value::Regex(_) => Kind::Regex,
		Value::Range(_) => Kind::Range,
		Value::Geometry(_) => Kind::Geometry,
		Value::Array(_) => Kind::Array,
		Value::Set(_) => Kind::Set,
		Value::Object(_) => Kind::Object,
		Value::Closure(_) => Kind::Unknown,
	}
}

// ---------------------------------------------------------------------------
// Generation

/// A small deterministic generator, so failures reproduce.
///
/// It reaches the edges of every stored form: NaNs of either sign and any
/// payload, decimals of every scale and sign, both widths of a time, strings
/// and containers either side of a one-byte length or offset, and every kind
/// of record id key and geometry. It leaves out three kinds of value the
/// stored form does not read back as written:
///
/// * a NaN coordinate. `Polygon::new` closes a ring unless its last coordinate is `==` its first,
///   which a ring starting at NaN never is, so each read of one adds a point.
/// * a file key without a leading `/`. The decoder makes every file through `File::new`, which adds
///   one.
/// * a set holding two record ids keyed by ranges. `RecordIdKeyRange`'s ordering compares one
///   range's start with the other's end, so no order of the two is ascending both ways.
struct Rng(u64);

impl Rng {
	fn next(&mut self) -> u64 {
		self.0 ^= self.0 << 13;
		self.0 ^= self.0 >> 7;
		self.0 ^= self.0 << 17;
		self.0
	}

	fn below(&mut self, n: u64) -> u64 {
		self.next() % n
	}

	fn pick<T: Copy>(&mut self, from: &[T]) -> T {
		from[self.below(from.len() as u64) as usize]
	}

	fn int(&mut self) -> i64 {
		// Spread over every width, with the boundaries over-represented.
		let bits = self.below(65) as u32;
		let v = (self.next() >> (64 - bits.max(1))) as i64;
		match self.below(6) {
			0 => v.wrapping_neg(),
			1 => self.pick(&[0, 126, 127, 128, -1, -128, -129, i64::MIN, i64::MAX]),
			_ => v,
		}
	}

	/// A float of any bits: NaNs of either sign and any payload, both zeros,
	/// both infinities, subnormals, and values an `f32` holds exactly.
	fn float(&mut self) -> f64 {
		match self.below(8) {
			0 => self.pick(&[
				0.0,
				-0.0,
				f64::INFINITY,
				f64::NEG_INFINITY,
				f64::MIN_POSITIVE,
				f64::MAX,
				1e-310,
				f64::NAN,
			]),
			1 => {
				let b = self.next();
				let payload = (b & 0x000F_FFFF_FFFF_FFFF).max(1);
				f64::from_bits(0x7FF0_0000_0000_0000 | (b & 0x8000_0000_0000_0000) | payload)
			}
			2 => f32::from_bits(self.next() as u32) as f64,
			3 | 4 => (self.int() as f32) as f64 / 4.0,
			_ => f64::from_bits(self.next()),
		}
	}

	/// Any float but NaN.
	fn coordinate(&mut self) -> f64 {
		match self.float() {
			f if f.is_nan() => 0.5,
			f => f,
		}
	}

	/// A decimal of any scale and sign, negative zero included, its mantissa
	/// sometimes at the edge of a byte width.
	fn decimal(&mut self) -> Decimal {
		let m = match self.below(4) {
			0 => 0,
			1 => ((1i128 << 96) - 1) >> (8 * self.below(12)),
			_ => (self.next() as i128) >> self.below(64),
		};
		let mut d = Decimal::try_from_i128_with_scale(m, self.below(29) as u32).expect("in range");
		d.set_sign_negative(self.below(2) == 0);
		d
	}

	/// A duration, sometimes either side of the most nanoseconds a u64 counts.
	fn duration(&mut self) -> std::time::Duration {
		use std::time::Duration as D;
		match self.below(4) {
			0 => self.pick(&[
				D::ZERO,
				D::from_nanos(u64::MAX),
				D::new(18_446_744_073, 709_551_616),
				D::new(u64::MAX, 999_999_999),
			]),
			1 => D::from_nanos(self.next()),
			_ => D::new(self.next() >> self.below(64), self.below(1_000_000_000) as u32),
		}
	}

	/// A datetime, sometimes either side of an end of the nanoseconds an i64
	/// counts, at the first instant of 1500 or the last of 9999, at either end
	/// of what a datetime holds, or in a leap second.
	fn datetime(&mut self) -> Value {
		const NS: i64 = 1_000_000_000;
		let (secs, nanos) = match self.below(6) {
			0 => {
				let (s, n) = (i64::MIN.div_euclid(NS), i64::MIN.rem_euclid(NS) as u32);
				self.pick(&[(s, n), (s, n - 1)])
			}
			1 => {
				let (s, n) = (i64::MAX.div_euclid(NS), i64::MAX.rem_euclid(NS) as u32);
				self.pick(&[(s, n), (s, n + 1)])
			}
			2 => self.pick(&[(-14_831_769_600, 0), (253_402_300_799, 999_999_999)]),
			// A leap second's sub-second part is a second or more.
			3 => ((self.int() >> 30) * 60 + 59, 1_000_000_000 + self.below(1_000_000_000) as u32),
			4 => {
				return Value::Datetime(Datetime(
					self.pick(&[DateTime::<Utc>::MIN_UTC, DateTime::<Utc>::MAX_UTC]),
				));
			}
			_ => (
				(self.int() >> 20).clamp(-8_000_000_000_000, 8_000_000_000_000),
				self.below(1_000_000_000) as u32,
			),
		};
		datetime(secs, nanos)
	}

	fn string(&mut self) -> String {
		const PARTS: [&str; 8] = ["a", "id", "é", "名前", "🦀", "\0", " ", "field_"];
		let n = match self.below(10) {
			0 => 0,
			1 => 300,
			// Either side of the longest string a one-byte length holds.
			2 if self.below(2) == 0 => return "x".repeat(self.pick(&[254, 255, 256])),
			_ => self.below(12),
		};
		(0..n).map(|_| PARTS[self.below(PARTS.len() as u64) as usize]).collect()
	}

	/// An element count: a few, or near a top-level container, either side of
	/// the most entries a one-byte offset counts.
	fn count(&mut self, depth: usize, most: u64) -> usize {
		match self.below(64) {
			0 if depth == 0 => self.pick(&[255, 256]),
			_ => self.below(most) as usize,
		}
	}

	fn key(&mut self, depth: usize) -> RecordIdKey {
		match self.below(if depth > 2 {
			3
		} else {
			6
		}) {
			0 => RecordIdKey::Number(self.int()),
			1 => RecordIdKey::String(Strand::new(self.string())),
			2 => RecordIdKey::Uuid(Uuid(uuid::Uuid::from_u128(self.next() as u128))),
			3 => RecordIdKey::Array(Array(
				(0..self.below(4)).map(|_| self.value(depth + 1)).collect(),
			)),
			4 => RecordIdKey::Object(
				(0..self.below(4))
					.map(|_| (Strand::new(self.string()), self.value(depth + 1)))
					.collect(),
			),
			_ => RecordIdKey::Range(Box::new(RecordIdKeyRange {
				start: self.bound(|r| r.key(depth + 1)),
				end: self.bound(|r| r.key(depth + 1)),
			})),
		}
	}

	fn bound<T>(&mut self, mut f: impl FnMut(&mut Self) -> T) -> Bound<T> {
		match self.below(3) {
			0 => Bound::Unbounded,
			1 => Bound::Included(f(self)),
			_ => Bound::Excluded(f(self)),
		}
	}

	fn coord(&mut self) -> Coord<f64> {
		Coord {
			x: self.coordinate(),
			y: self.coordinate(),
		}
	}

	fn line(&mut self) -> LineString<f64> {
		LineString((0..self.below(5)).map(|_| self.coord()).collect())
	}

	fn polygon(&mut self) -> Polygon<f64> {
		let exterior = self.line();
		let interiors = (0..self.below(3)).map(|_| self.line()).collect();
		Polygon::new(exterior, interiors)
	}

	fn geometry(&mut self, depth: usize) -> Geometry {
		match self.below(if depth > 1 {
			6
		} else {
			7
		}) {
			0 => Geometry::Point(Point(self.coord())),
			1 => Geometry::Line(self.line()),
			2 => Geometry::Polygon(self.polygon()),
			3 => Geometry::MultiPoint(MultiPoint(
				(0..self.below(4)).map(|_| Point(self.coord())).collect(),
			)),
			4 => Geometry::MultiLine(MultiLineString(
				(0..self.below(4)).map(|_| self.line()).collect(),
			)),
			5 => Geometry::MultiPolygon(MultiPolygon(
				(0..self.below(3)).map(|_| self.polygon()).collect(),
			)),
			_ => {
				Geometry::Collection((0..self.below(4)).map(|_| self.geometry(depth + 1)).collect())
			}
		}
	}

	/// A set built element by element, as an insertion builds one.
	fn set(&mut self, depth: usize) -> Set {
		#[expect(clippy::mutable_key_type)]
		let mut set = BTreeSet::new();
		for _ in 0..self.count(depth, 6) {
			let v = self.value(depth + 1);
			let keyed_by_range = |v: &Value| {
				matches!(
					v,
					Value::RecordId(RecordId {
						key: RecordIdKey::Range(_),
						..
					})
				)
			};
			if !holds(&v, &keyed_by_range) {
				set.insert(v);
			}
		}
		Set::from(set)
	}

	fn value(&mut self, depth: usize) -> Value {
		let kinds = if depth >= 4 {
			16
		} else {
			21
		};
		match self.below(kinds) {
			0 => Value::None,
			1 => Value::Null,
			2 => Value::Bool(self.below(2) == 0),
			3 => int(self.below(127) as i64),
			4 => int(self.int()),
			5 => float(self.float()),
			6 => Value::Number(Number::Decimal(self.decimal())),
			7 => string(&self.string()),
			8 => Value::Duration(Duration(self.duration())),
			9 => self.datetime(),
			10 => Value::Uuid(Uuid(uuid::Uuid::from_u128(
				((self.next() as u128) << 64) | self.next() as u128,
			))),
			11 => Value::Bytes(Bytes(::bytes::Bytes::from(
				(0..self.below(40)).map(|_| self.next() as u8).collect::<Vec<u8>>(),
			))),
			12 => Value::Table(TableName::new(self.string())),
			13 => rid(&self.string(), self.key(depth + 1)),
			14 => Value::File(File::new(self.string(), self.string())),
			15 => Value::Regex(
				Regex::from_str(["a+", "^[a-z]+$", "(?i)x.y", ""][self.below(4) as usize]).unwrap(),
			),
			16 => match self.below(3) {
				// Arrays of floats take the typed form.
				0 => array((0..self.count(depth, 6).max(1)).map(|_| float(self.float())).collect()),
				_ => array((0..self.count(depth, 6)).map(|_| self.value(depth + 1)).collect()),
			},
			17 => Value::Set(self.set(depth)),
			18 => Value::Object(
				(0..self.count(depth, 8))
					.map(|_| (Strand::new(self.string()), self.value(depth + 1)))
					.collect(),
			),
			19 => Value::Range(Box::new(Range {
				start: self.bound(|r| r.value(depth + 1)),
				end: self.bound(|r| r.value(depth + 1)),
			})),
			_ => Value::Geometry(self.geometry(0)),
		}
	}

	/// A value nested `levels` deep, each level whatever the generator
	/// chooses: an array, set or object holding the level below, a range
	/// bounded by it, a record id whose key holds it, or, at the bottom, a
	/// geometry collection.
	fn deep(&mut self, levels: usize) -> Value {
		// A collection nests only geometries, so collections take the bottom.
		let collections = match self.below(4) {
			0 => self.below(levels as u64 + 1) as usize,
			_ => 0,
		};
		let mut v = if collections == 0 {
			self.value(4)
		} else {
			let mut g = Geometry::Point(Point::new(0.5, -0.5));
			for _ in 0..collections {
				g = Geometry::Collection(vec![g]);
			}
			Value::Geometry(g)
		};
		let mut depth = collections;
		while depth < levels {
			let left = levels - depth;
			let (next, added) = match self.below(8) {
				1 => (Value::Set(Set::from(BTreeSet::from([v]))), 1),
				2 => (object(vec![("k", v)]), 1),
				3 => (
					Value::Range(Box::new(Range {
						start: Bound::Included(v),
						end: Bound::Unbounded,
					})),
					1,
				),
				4 => (
					Value::Range(Box::new(Range {
						start: Bound::Unbounded,
						end: Bound::Excluded(v),
					})),
					1,
				),
				5 if left >= 2 => (rid("t", RecordIdKey::Array(Array(vec![v]))), 2),
				6 if left >= 2 => {
					(rid("t", RecordIdKey::Object(Object::from(BTreeMap::from([("k", v)])))), 2)
				}
				7 if left >= 3 => (
					rid(
						"t",
						RecordIdKey::Range(Box::new(RecordIdKeyRange {
							start: Bound::Included(RecordIdKey::Array(Array(vec![v]))),
							end: Bound::Unbounded,
						})),
					),
					3,
				),
				_ => (array(vec![v]), 1),
			};
			v = next;
			depth += added;
		}
		v
	}

	/// `bytes` with one byte changed, inserted or removed, or cut short.
	fn mutate(&mut self, bytes: &[u8]) -> Vec<u8> {
		let mut b = bytes.to_vec();
		match self.below(4) {
			0 if !b.is_empty() => {
				let i = self.below(b.len() as u64) as usize;
				b[i] = self.next() as u8;
			}
			1 => b.truncate(self.below(b.len() as u64 + 1) as usize),
			2 => {
				let i = self.below(b.len() as u64 + 1) as usize;
				b.insert(i, self.next() as u8);
			}
			_ if !b.is_empty() => {
				let i = self.below(b.len() as u64) as usize;
				b.remove(i);
			}
			_ => {}
		}
		b
	}
}

// ---------------------------------------------------------------------------
// The encoding, byte for byte. These pin the grammar: a change here changes
// what every stored record means.

#[test]
fn scalars_encode_to_their_documented_bytes() {
	let cases: Vec<(Value, Vec<u8>)> = vec![
		(Value::None, vec![0x00]),
		(Value::Null, vec![0x01]),
		(Value::Bool(false), vec![0x02]),
		(Value::Bool(true), vec![0x03]),
		(int(0), vec![0x80]),
		(int(5), vec![0x85]),
		(int(126), vec![0xFE]),
		(int(127), vec![0x04, 0x7F]),
		(int(128), vec![0x04, 0x80, 0x00]),
		(int(-1), vec![0x04, 0xFF]),
		(int(-128), vec![0x04, 0x80]),
		(int(-129), vec![0x04, 0x7F, 0xFF]),
		(int(i64::MAX), vec![0x04, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F]),
		(int(i64::MIN), vec![0x04, 0, 0, 0, 0, 0, 0, 0, 0x80]),
		(float(1.5), vec![0x05, 0x00, 0x00, 0xC0, 0x3F]),
		(float(0.1), [&[0x05][..], &0.1f64.to_le_bytes()].concat()),
		(
			Value::Number(Number::Decimal(Decimal::from_str("1.50").unwrap())),
			vec![0x06, 0x02, 0x96],
		),
		(
			Value::Number(Number::Decimal(Decimal::from_str("-300").unwrap())),
			vec![0x06, 0x80, 0x2C, 0x01],
		),
		(Value::Number(Number::Decimal(Decimal::ZERO)), vec![0x06, 0x00]),
		(Value::Number(Number::I8(0)), vec![I8, 0x00]),
		(Value::Number(Number::I8(-1)), vec![I8, 0xFF]),
		(Value::Number(Number::I8(127)), vec![I8, 0x7F]),
		(Value::Number(Number::I8(-128)), vec![I8, 0x80]),
		(Value::Number(Number::U8(0)), vec![U8, 0x00]),
		(Value::Number(Number::U8(255)), vec![U8, 0xFF]),
		(Value::Number(Number::I16(0)), vec![I16, 0x00]),
		(Value::Number(Number::I16(-1)), vec![I16, 0xFF]),
		(Value::Number(Number::I16(128)), vec![I16, 0x80, 0x00]),
		(Value::Number(Number::I16(-129)), vec![I16, 0x7F, 0xFF]),
		(Value::Number(Number::U16(0)), vec![U16, 0x00]),
		(Value::Number(Number::U16(256)), vec![U16, 0x00, 0x01]),
		(Value::Number(Number::I32(0)), vec![I32, 0x00]),
		(Value::Number(Number::I32(i32::MAX)), vec![I32, 0xFF, 0xFF, 0xFF, 0x7F]),
		(Value::Number(Number::U32(0)), vec![U32, 0x00]),
		(Value::Number(Number::U32(u32::MAX)), vec![U32, 0xFF, 0xFF, 0xFF, 0xFF]),
		(Value::Number(Number::U64(0)), vec![U64, 0x00]),
		(
			Value::Number(Number::U64(u64::MAX)),
			vec![U64, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
		),
		(
			Value::Number(Number::F16(half::f16::from_f32(1.5))),
			[&[F16][..], &half::f16::from_f32(1.5).to_le_bytes()].concat(),
		),
		(Value::Number(Number::F32(1.5)), [&[F32][..], &1.5f32.to_le_bytes()].concat()),
		(string(""), vec![0x07]),
		(string("ab"), vec![0x07, b'a', b'b']),
		(
			Value::Duration(Duration(std::time::Duration::from_nanos(1))),
			vec![0x08, 1, 0, 0, 0, 0, 0, 0, 0],
		),
		(datetime(0, 0), vec![0x09, 0, 0, 0, 0, 0, 0, 0, 0]),
		(Value::Uuid(Uuid(uuid::Uuid::from_u128(1))), [&[0x0A][..], &[0; 15], &[1]].concat()),
		(Value::Bytes(Bytes(::bytes::Bytes::from_static(&[9, 8]))), vec![0x0B, 9, 8]),
		(Value::Table(TableName::new("t")), vec![0x0C, b't']),
		(rid("t", RecordIdKey::Number(1)), vec![0x0D, 0x01, b't', 0x81]),
		(rid("t", RecordIdKey::String(Strand::new("k"))), vec![0x0D, 0x01, b't', 0x07, b'k']),
		(Value::File(File::new("b".into(), "/k".into())), vec![0x0E, 0x01, b'b', b'/', b'k']),
		(Value::Regex(Regex::from_str("a+").unwrap()), vec![0x0F, b'a', b'+']),
		(
			Value::Range(Box::new(Range {
				start: Bound::Included(int(1)),
				end: Bound::Excluded(int(2)),
			})),
			vec![0x10, 0x01, 0x01, 0x81, 0x02, 0x82],
		),
		(
			Value::Range(Box::new(Range {
				start: Bound::Unbounded,
				end: Bound::Unbounded,
			})),
			vec![0x10, 0x00, 0x00],
		),
		(
			Value::Geometry(Geometry::Point(Point::new(1.0, 2.0))),
			[&[0x11, 0x00][..], &1.0f64.to_le_bytes(), &2.0f64.to_le_bytes()].concat(),
		),
	];
	for (v, bytes) in cases {
		assert_eq!(round_trip(&v), bytes, "{v:?}");
	}
}

#[test]
fn containers_encode_to_their_documented_bytes() {
	let cases: Vec<(Value, Vec<u8>)> = vec![
		(array(vec![]), vec![0x14, 0x00]),
		(array(vec![int(1), string("a")]), vec![0x14, 0x02, 0x00, 0x01, 0x81, 0x07, b'a']),
		(
			array(vec![float(1.5), float(2.0)]),
			vec![0x13, 0x00, 0x00, 0xC0, 0x3F, 0x00, 0x00, 0x00, 0x40],
		),
		(
			array(vec![float(1.5), float(0.1)]),
			[&[0x12][..], &1.5f64.to_le_bytes(), &0.1f64.to_le_bytes()].concat(),
		),
		// An array of floats and an int is not an array of floats.
		(
			array(vec![float(1.5), int(1)]),
			vec![0x14, 0x02, 0x00, 0x05, 0x05, 0x00, 0x00, 0xC0, 0x3F, 0x81],
		),
		(
			Value::Set(Set::from(std::collections::BTreeSet::from([int(2), int(1)]))),
			vec![0x17, 0x02, 0x00, 0x01, 0x81, 0x82],
		),
		(object(vec![]), vec![0x1A, 0x00]),
		(
			object(vec![("b", Value::Bool(true)), ("a", int(1))]),
			vec![0x1A, 0x02, 0x00, 0x03, 0x01, b'a', 0x81, 0x01, b'b', 0x03],
		),
		// Byte order, not length-first order.
		(
			object(vec![("bb", int(1)), ("c", int(2))]),
			vec![0x1A, 0x02, 0x00, 0x04, 0x02, b'b', b'b', 0x81, 0x01, b'c', 0x82],
		),
	];
	for (v, bytes) in cases {
		assert_eq!(round_trip(&v), bytes, "{v:?}");
	}
}

#[test]
fn containers_take_the_narrowest_offset_width() {
	// 255 entries of one byte: width 1. The entries are not integers, which
	// would be written as a typed integer array.
	let b = round_trip(&array((0..255).map(|_| Value::Bool(true)).collect()));
	assert_eq!(b[0], ARRAY);
	// 256 entries: the count needs width 2.
	let b = round_trip(&array((0..256).map(|_| Value::Bool(true)).collect()));
	assert_eq!(b[0], ARRAY + 1);
	// Few entries, but more than 255 bytes of them: width 2.
	let b = round_trip(&array(vec![string(&"x".repeat(300))]));
	assert_eq!(&b[..3], &[ARRAY + 1, 0x01, 0x00]);
	// More than 65535 bytes: width 4.
	let b = round_trip(&object(vec![("k", string(&"x".repeat(70_000)))]));
	assert_eq!(b[0], OBJECT + 2);
	// Exactly 255 bytes of entries still fits width 1.
	let b = round_trip(&array(vec![string(&"x".repeat(254))]));
	assert_eq!(&b[..3], &[ARRAY, 0x01, 0x00]);
	let b = round_trip(&array(vec![string(&"x".repeat(255))]));
	assert_eq!(b[0], ARRAY + 1);
}

#[test]
fn long_keys_and_names_take_a_wide_length_prefix() {
	let long = "k".repeat(300);
	let b = round_trip(&object(vec![(long.as_str(), int(1))]));
	// tag, n, offset, then 0xFF and the u32 length.
	assert_eq!(&b[..8], &[OBJECT + 1, 0x01, 0x00, 0x00, 0x00, 0xFF, 0x2C, 0x01]);
	round_trip(&rid(&long, RecordIdKey::Number(1)));
	round_trip(&Value::File(File::new(long.clone(), long.clone())));
	round_trip(&Value::Range(Box::new(Range {
		start: Bound::Included(string(&long)),
		end: Bound::Included(string(&long)),
	})));
}

#[test]
fn floats_keep_their_exact_bits() {
	for f in [
		0.0,
		-0.0,
		1.0 / 3.0,
		f64::INFINITY,
		f64::NEG_INFINITY,
		f64::MIN_POSITIVE,
		1e-310,
		f64::MAX,
	] {
		let b = round_trip(&float(f));
		let v = decode(&b).unwrap();
		if let Value::Number(Number::Float(g)) = v {
			assert_eq!(g.to_bits(), f.to_bits());
		} else {
			panic!("{v:?}");
		}
	}
	// NaN takes the f64 form, so its payload survives.
	let nan = f64::from_bits(0x7FF8_0000_0000_0001);
	let b = encode(&float(nan)).unwrap();
	assert_eq!(b.len(), 9);
	let v = decode(&b).unwrap();
	if let Value::Number(Number::Float(g)) = v {
		assert_eq!(g.to_bits(), nan.to_bits());
	} else {
		panic!("{v:?}");
	}
	let b = encode(&array(vec![float(1.0), float(nan)])).unwrap();
	assert_eq!(b[0], F64_ARRAY);
}

#[test]
fn decimals_keep_their_scale_and_sign() {
	for s in [
		"0",
		"0.000",
		"-0.5",
		"1.50",
		"79228162514264337593543950335",
		"-7.9228162514264337593543950335",
		"0.0000000000000000000000000001",
	] {
		round_trip(&Value::Number(Number::Decimal(Decimal::from_str(s).unwrap())));
	}
	let mut neg_zero = Decimal::ZERO;
	neg_zero.set_sign_negative(true);
	round_trip(&Value::Number(Number::Decimal(neg_zero)));
}

#[test]
fn times_take_the_wide_form_only_when_nanoseconds_cannot_hold_them() {
	let b = round_trip(&Value::Duration(Duration(std::time::Duration::from_nanos(u64::MAX))));
	assert_eq!(b.len(), 9);
	let b = round_trip(&Value::Duration(Duration(std::time::Duration::new(u64::MAX, 999_999_999))));
	assert_eq!(b.len(), 13);
	let b = round_trip(&datetime(-1, 5));
	assert_eq!(b.len(), 9);
	let b = round_trip(&datetime(8_000_000_000_000, 7));
	assert_eq!(b.len(), 13);
	let b = round_trip(&Value::Datetime(Datetime(DateTime::<Utc>::MIN_UTC)));
	assert_eq!(b.len(), 13);
	round_trip(&Value::Datetime(Datetime(DateTime::<Utc>::MAX_UTC)));
	// A leap second, whose sub-second part is a second or more.
	let leap = chrono::NaiveDate::from_ymd_opt(2016, 12, 31)
		.unwrap()
		.and_hms_nano_opt(23, 59, 59, 1_500_000_000)
		.unwrap()
		.and_utc();
	assert!(leap.timestamp_subsec_nanos() >= 1_000_000_000);
	let b = round_trip(&Value::Datetime(Datetime(leap)));
	assert_eq!(b.len(), 13);
}

#[test]
fn record_id_keys_round_trip() {
	let keys = vec![
		RecordIdKey::Number(-5),
		RecordIdKey::Number(1),
		RecordIdKey::String(Strand::new("名前")),
		RecordIdKey::Uuid(Uuid(uuid::Uuid::from_u128(7))),
		RecordIdKey::Array(Array(vec![int(1), string("x"), float(2.5)])),
		RecordIdKey::Array(Array(vec![float(1.5), float(2.5)])),
		RecordIdKey::Array(Array(vec![])),
		RecordIdKey::Object(Object::from(BTreeMap::from([("a", int(1))]))),
		RecordIdKey::Range(Box::new(RecordIdKeyRange {
			start: Bound::Included(RecordIdKey::Number(1)),
			end: Bound::Excluded(RecordIdKey::Range(Box::new(RecordIdKeyRange {
				start: Bound::Unbounded,
				end: Bound::Included(RecordIdKey::String(Strand::new("z"))),
			}))),
		})),
	];
	for k in keys {
		round_trip(&rid("t", k.clone()));
		let mut enc = Encoder::default();
		enc.value(&rid("t", k.clone())).unwrap();
		let key_bytes = &enc.as_bytes()[3..];
		assert_eq!(decode_key(key_bytes).unwrap(), k);
	}
}

#[test]
fn geometries_round_trip() {
	let square = LineString(vec![
		Coord {
			x: 0.0,
			y: 0.0,
		},
		Coord {
			x: 1.0,
			y: 0.0,
		},
		Coord {
			x: 1.0,
			y: 1.0,
		},
		Coord {
			x: 0.0,
			y: 0.0,
		},
	]);
	let polygon = Polygon::new(square.clone(), vec![square.clone()]);
	let all = vec![
		Geometry::Point(Point::new(-1.5, 2.25)),
		Geometry::Line(square.clone()),
		Geometry::Line(LineString(vec![])),
		Geometry::Polygon(polygon.clone()),
		Geometry::Polygon(Polygon::new(LineString(vec![]), vec![])),
		Geometry::MultiPoint(MultiPoint(vec![Point::new(1.0, 2.0), Point::new(3.0, 4.0)])),
		Geometry::MultiPoint(MultiPoint(vec![])),
		Geometry::MultiLine(MultiLineString(vec![square, LineString(vec![])])),
		Geometry::MultiPolygon(MultiPolygon(vec![polygon.clone(), polygon])),
		Geometry::Collection(vec![]),
	];
	let nested = Geometry::Collection(vec![
		Geometry::Collection(all.clone()),
		Geometry::Point(Point::new(0.0, 0.0)),
	]);
	for g in all.into_iter().chain([nested]) {
		round_trip(&Value::Geometry(g));
	}
}

#[test]
fn generated_values_round_trip() {
	let mut r = Rng(seed(0x9E37_79B9_7F4A_7C15));
	for _ in 0..cases(20_000) {
		round_trip(&r.value(0));
	}
}

/// [`generated_values_round_trip`] and [`mutated_bytes_never_panic`] at
/// length, for a run on demand: `cargo test -p surrealdb-expr stored --
/// --ignored`, with `STORED_VALUE_CASES` and `STORED_VALUE_SEED` to scale and
/// vary it.
#[test]
#[ignore = "runs for minutes; run on demand"]
fn generated_values_round_trip_at_length() {
	let mut r = Rng(seed(0x2545_F491_4F6C_DD1D));
	for _ in 0..cases(5_000_000) {
		let v = r.value(0);
		let bytes = round_trip(&v);
		for _ in 0..4 {
			read_mutated(&r.mutate(&bytes));
		}
	}
}

/// Whatever nesting the encoder writes, the decoder reads, through every kind
/// of level and either side of both depth limits; and the encoder refuses only
/// values the record depth guard refuses too.
#[test]
fn deep_values_the_encoder_writes_are_read() {
	let mut r = Rng(seed(0x51DE_C0DE_D00D_FEED));
	for _ in 0..cases(300) {
		let levels = r.pick(&[
			MAX_VALUE_DEPTH - 2,
			MAX_VALUE_DEPTH - 1,
			MAX_VALUE_DEPTH,
			MAX_VALUE_DEPTH + 1,
			DEPTH_LIMIT - 1,
			DEPTH_LIMIT,
			DEPTH_LIMIT + 1,
			DEPTH_LIMIT + 2,
		]);
		let v = r.deep(levels);
		match encode(&v) {
			Ok(_) => {
				round_trip(&v);
			}
			Err(Error::TooDeep(_)) => {
				assert!(
					check_value_depth(&v).is_err(),
					"{levels} levels are admitted but not written"
				)
			}
			Err(e) => panic!("{levels} levels are not written: {e}"),
		}
	}
}

#[test]
fn identical_values_encode_identically() {
	let mut r = Rng(42);
	for _ in 0..2_000 {
		let v = r.value(0);
		assert_eq!(encode(&v).unwrap(), encode(&v.clone()).unwrap());
	}
}

#[test]
fn the_record_id_field_is_left_out_on_request() {
	let with = object(vec![("id", rid("t", RecordIdKey::Number(1))), ("a", int(1)), ("z", int(2))]);
	let without = object(vec![("a", int(1)), ("z", int(2))]);
	let Value::Object(obj) = &with else {
		unreachable!()
	};
	let mut enc = Encoder::default();
	enc.object_without(obj, "id").unwrap();
	assert_eq!(enc.as_bytes(), encode(&without).unwrap());
	// Absent, nothing is left out.
	let mut enc = Encoder::default();
	let Value::Object(obj) = &without else {
		unreachable!()
	};
	enc.object_without(obj, "id").unwrap();
	assert_eq!(enc.as_bytes(), encode(&without).unwrap());
}

#[test]
fn an_encoder_in_use_is_not_shared() {
	let inner = with_encoder(|outer| {
		outer.value(&int(1)).unwrap();
		let inner = encode(&string("nested")).unwrap();
		assert_eq!(outer.as_bytes(), &[0x81]);
		inner
	});
	assert_eq!(inner, encode(&string("nested")).unwrap());
}

#[test]
fn nesting_is_bounded_both_ways() {
	let mut v = int(1);
	for _ in 0..MAX_VALUE_DEPTH {
		v = array(vec![v]);
	}
	round_trip(&v);
	for _ in 0..DEPTH_LIMIT {
		v = array(vec![v]);
	}
	assert_eq!(encode(&v), Err(Error::TooDeep(DEPTH_LIMIT)));

	let mut b = vec![ARRAY, 0x00];
	for _ in 0..DEPTH_LIMIT + 2 {
		b = [&[ARRAY, 0x01, 0x00][..], &b].concat();
	}
	assert_eq!(decode(&b), Err(Error::TooDeep(DEPTH_LIMIT)));
}

// ---------------------------------------------------------------------------
// In-place reads

#[test]
fn fields_are_found_by_name_with_or_without_a_hint() {
	let names: Vec<String> = (0..40).map(|i| format!("f{i:02}")).collect();
	let v = Value::Object(
		names.iter().enumerate().map(|(i, n)| (Strand::new(n), int(i as i64 * 1000))).collect(),
	);
	let bytes = encode(&v).unwrap();
	let obj = ValueRef::new(&bytes).unwrap().as_ordered_object().unwrap().unwrap();
	assert_eq!(obj.len(), 40);
	for (i, n) in names.iter().enumerate() {
		assert_eq!(obj.get(n).unwrap().unwrap().as_int().unwrap(), Some(i as i64 * 1000));
		for start in [0, i.saturating_sub(1), i, i + 1, 39, 40, u32::MAX as usize] {
			let mut hint = start as u32;
			let found = obj.get_hinted(n, &mut hint).unwrap().unwrap();
			assert_eq!(found.as_int().unwrap(), Some(i as i64 * 1000));
			assert_eq!(hint as usize, i);
		}
	}
	for absent in ["", "f", "f005", "f40", "g", "f00\0"] {
		let mut hint = 7;
		assert_eq!(obj.get_hinted(absent, &mut hint).unwrap(), None);
		assert_eq!(hint, 7, "{absent}");
	}
}

#[test]
fn typed_float_arrays_read_as_floats() {
	for v in [vec![1.5, -2.0, 0.25], vec![0.1, 1.5]] {
		let bytes = encode(&array(v.iter().copied().map(float).collect())).unwrap();
		let arr = ValueRef::new(&bytes).unwrap().as_array().unwrap().unwrap();
		assert_eq!(arr.len(), v.len());
		assert_eq!(arr.floats().unwrap().collect::<Vec<_>>(), v);
		for (i, f) in v.iter().enumerate() {
			let e = arr.get(i).unwrap().unwrap();
			assert_eq!(e.kind(), Kind::Float);
			assert_eq!(e.as_float().unwrap(), Some(*f));
			assert_eq!(e.decode().unwrap(), float(*f));
		}
		assert_eq!(arr.get(v.len()).unwrap(), None);
	}
}

#[test]
fn values_are_read_in_place() {
	let v = object(vec![
		("id", rid("person", RecordIdKey::String(Strand::new("tobie")))),
		("name", string("Tobie")),
		("tags", Value::Set(Set::from(std::collections::BTreeSet::from([string("a")])))),
		("score", float(9.5)),
	]);
	let bytes = encode(&v).unwrap();
	let obj = ValueRef::new(&bytes).unwrap().as_ordered_object().unwrap().unwrap();
	let (table, key) = obj.get("id").unwrap().unwrap().as_record_id().unwrap().unwrap();
	assert_eq!(table, "person");
	assert_eq!(key.as_str().unwrap(), Some("tobie"));
	assert_eq!(obj.get("name").unwrap().unwrap().as_str().unwrap(), Some("Tobie"));
	let tags = obj.get("tags").unwrap().unwrap();
	assert_eq!(tags.kind(), Kind::Set);
	assert_eq!(tags.as_array().unwrap().map(|a| a.len()), None);
	assert_eq!(tags.as_set().unwrap().unwrap().len(), 1);
	assert_eq!(obj.get("score").unwrap().unwrap().as_float().unwrap(), Some(9.5));
	assert_eq!(obj.get("name").unwrap().unwrap().as_int().unwrap(), None);
	let keys: Vec<&str> = obj.iter().map(|e| e.unwrap().0).collect();
	assert_eq!(keys, ["id", "name", "score", "tags"]);
	let mut out = Vec::new();
	obj.get("name").unwrap().unwrap().write_to(&mut out);
	assert_eq!(decode(&out).unwrap(), string("Tobie"));
}

#[test]
fn only_an_object_whose_keys_ascend_is_looked_into() {
	for bytes in [
		vec![OBJECT, 0x02, 0x00, 0x03, 0x01, b'b', 0x81, 0x01, b'a', 0x81],
		vec![OBJECT, 0x02, 0x00, 0x03, 0x01, b'a', 0x81, 0x01, b'a', 0x81],
	] {
		let v = ValueRef::new(&bytes).unwrap();
		let o = v.as_object().unwrap().expect("an object");
		assert_eq!(o.len(), 2, "its entries are still read in order");
		assert!(matches!(o.ordered(), Err(Error::Malformed("object keys out of order"))));
		assert!(v.as_ordered_object().is_err());
		assert!(decode(&bytes).is_err(), "a whole decode refuses it too");
	}
	let bytes = vec![OBJECT, 0x02, 0x00, 0x03, 0x01, b'a', 0x81, 0x01, b'b', 0x82];
	let o = ValueRef::new(&bytes).unwrap().as_ordered_object().unwrap().unwrap();
	assert_eq!(o.get("b").unwrap().unwrap().as_int().unwrap(), Some(2));
	// Keys that ascend byte-wise but are not UTF-8 are refused too, as the whole decode refuses
	// them, though no lookup names them.
	let bytes = vec![OBJECT, 0x02, 0x00, 0x03, 0x01, b'a', 0x81, 0x01, 0xFF, 0x82];
	let o = ValueRef::new(&bytes).unwrap().as_object().unwrap().expect("an object");
	assert!(o.ordered().is_err());
	assert!(decode(&bytes).is_err(), "a whole decode refuses it too");
}

#[test]
fn a_value_whose_tag_is_the_whole_value_has_no_payload() {
	for tag in [NONE, NULL, FALSE, TRUE, SMALL_INT, SMALL_INT + 5, 0xFE] {
		let bare = [tag];
		let v = ValueRef::new(&bare).unwrap_or_else(|e| panic!("{tag:#x}: {e}"));
		assert!(v.payload().is_empty());
		assert!(
			matches!(ValueRef::new(&[tag, 0x01]), Err(Error::Malformed(_))),
			"{tag:#x} with a payload"
		);
		assert!(decode(&[tag, 0x01]).is_err(), "a whole decode refuses {tag:#x} with a payload");
	}
	// The payload is refused wherever the value is reached: as an object's field.
	let bytes = vec![OBJECT, 0x01, 0x00, 0x01, b'a', NULL, 0x00];
	let o = ValueRef::new(&bytes).unwrap().as_ordered_object().unwrap().unwrap();
	assert!(o.get("a").is_err());
	assert!(o.entry(0).is_err());
}

#[test]
fn every_tag_has_a_kind() {
	for t in 0..=0xFFu8 {
		let k = Kind::of(t);
		let known = t <= I64_ARRAY || (I8..=BIGDECIMAL).contains(&t) || (0x80..=0xFE).contains(&t);
		assert_eq!(k != Kind::Unknown, known, "{t:#04x}");
	}
}

#[test]
fn sized_numbers_decode_wider_and_refuse_malformed() {
	assert_eq!(decode(&[I16, 0x00, 0x00]).unwrap(), Value::Number(Number::I16(0)));
	assert_eq!(decode(&[U16, 0x00, 0x00]).unwrap(), Value::Number(Number::U16(0)));
	assert_eq!(decode(&[I32, 0x00, 0x00, 0x00, 0x00]).unwrap(), Value::Number(Number::I32(0)));
	assert_eq!(decode(&[U32, 0x00, 0x00, 0x00, 0x00]).unwrap(), Value::Number(Number::U32(0)));
	assert_eq!(
		decode(&[U64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]).unwrap(),
		Value::Number(Number::U64(0))
	);

	assert!(decode(&[I8, 0x00, 0x00]).is_err());
	assert!(decode(&[U8, 0x00, 0x00]).is_err());
	assert!(decode(&[I16, 0x00, 0x00, 0x00]).is_err());
	assert!(decode(&[U16, 0x00, 0x00, 0x00]).is_err());
	assert!(decode(&[I32, 0x00, 0x00, 0x00, 0x00, 0x00]).is_err());
	assert!(decode(&[U32, 0x00, 0x00, 0x00, 0x00, 0x00]).is_err());
	assert!(decode(&[U64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]).is_err());
	assert!(decode(&[F16, 0x00]).is_err());
	assert!(decode(&[F16, 0x00, 0x00, 0x00]).is_err());
	assert!(decode(&[F32, 0x00, 0x00, 0x00]).is_err());
	assert!(decode(&[F32, 0x00, 0x00, 0x00, 0x00, 0x00]).is_err());
}

fn bigdecimal(s: &str) -> Value {
	Value::Number(Number::BigDecimal(Box::new(surrealdb_types::BigDecimal::from_str(s).unwrap())))
}

#[test]
fn bigdecimals_round_trip_with_their_canonical_bytes() {
	assert_eq!(round_trip(&bigdecimal("0")), [BIGDECIMAL, 0x00, 0x00]);
	assert_eq!(round_trip(&bigdecimal("0.000")), [BIGDECIMAL, 0x00, 0x03]);
	assert_eq!(round_trip(&bigdecimal("1.50")), [BIGDECIMAL, 0x00, 0x02, 0x96]);
	assert_eq!(round_trip(&bigdecimal("-0.5")), [BIGDECIMAL, 0x01, 0x01, 0x05]);
	// A scale past one varint byte: 200 is 0xC8 0x01.
	assert_eq!(round_trip(&bigdecimal("1e-200")), [BIGDECIMAL, 0x00, 0xC8, 0x01, 0x01]);
	for s in [
		"79228162514264337593543950335",
		"-7.9228162514264337593543950335",
		"123456789012345678901234567890123456789012345678901234567890123456789",
		"-99999999999999999999999999999999999999999999999999999999999999999999.5",
		"1.5e-20000",
		"-2e300",
	] {
		round_trip(&bigdecimal(s));
	}
}

#[test]
fn bigdecimals_refuse_malformed_bytes() {
	let refused = |b: &[u8]| decode(b).is_err();
	assert!(matches!(decode(&[BIGDECIMAL]), Err(Error::Truncated)));
	assert!(matches!(decode(&[BIGDECIMAL, 0x00]), Err(Error::Truncated)));
	assert!(matches!(decode(&[BIGDECIMAL, 0x00, 0x80]), Err(Error::Truncated)));
	// A sign other than 0 or 1.
	assert!(refused(&[BIGDECIMAL, 0x02, 0x00, 0x05]));
	// A negative zero.
	assert!(refused(&[BIGDECIMAL, 0x01, 0x00]));
	// A scale written with an overlong varint.
	assert!(refused(&[BIGDECIMAL, 0x00, 0x80, 0x00]));
	// A magnitude that ends in a zero byte.
	assert!(refused(&[BIGDECIMAL, 0x00, 0x00, 0x05, 0x00]));
	// A scale past the largest scale.
	let mut buf = [0u8; varint::MAX_LEN];
	let too_large = varint::write_u64(1 << 40, &mut buf).to_vec();
	let mut b = vec![BIGDECIMAL, 0x00];
	b.extend_from_slice(&too_large);
	b.push(0x05);
	assert!(refused(&b));
}

#[test]
fn bigdecimal_payloads_never_panic() {
	let mut state = seed(0xB16_DEC1_0000_0001);
	let mut next = move || {
		state ^= state << 13;
		state ^= state >> 7;
		state ^= state << 17;
		state
	};
	for _ in 0..cases(20_000) {
		let len = (next() % 48) as usize;
		let mut bytes = vec![BIGDECIMAL];
		bytes.extend((0..len).map(|_| next() as u8));
		if let Ok(v) = decode(&bytes) {
			let back = encode(&v).expect("a decoded bigdecimal encodes");
			assert_eq!(back, bytes, "{bytes:02x?} decodes to {v:?} but re-encodes differently");
		}
	}
}

#[test]
fn temporal_values_round_trip_with_their_canonical_bytes() {
	let cases: Vec<(Value, Vec<u8>)> = vec![
		(Value::Date(Date(0)), vec![DATE, 0x00]),
		(Value::Date(Date(-1)), vec![DATE, 0xFF]),
		(Value::Date(Date(1_000)), vec![DATE, 0xE8, 0x03]),
		(Value::Date(Date::from(chrono::NaiveDate::MIN)), {
			let d = Date::from(chrono::NaiveDate::MIN);
			[&[DATE][..], &d.0.to_le_bytes()].concat()
		}),
		(Value::Time(Time(0)), vec![TIME, 0x00]),
		(Value::Time(Time(255)), vec![TIME, 0xFF]),
		(Value::Time(Time(Time::NANOS_PER_DAY)), {
			let n = Time::NANOS_PER_DAY.to_le_bytes();
			[&[TIME][..], &n[..6]].concat()
		}),
		(
			Value::Timestamp(Timestamp {
				secs: 1,
				nanos: 5,
			}),
			[&[TIMESTAMP][..], &1_000_000_005i64.to_le_bytes()].concat(),
		),
		(Value::Timestamp(Timestamp::from(chrono::NaiveDateTime::MIN)), {
			let t = Timestamp::from(chrono::NaiveDateTime::MIN);
			[&[TIMESTAMP][..], &t.secs.to_le_bytes(), &t.nanos.to_le_bytes()].concat()
		}),
		(
			Value::Interval(Interval::new(1, -2, 3)),
			[&[INTERVAL][..], &1i32.to_le_bytes(), &(-2i32).to_le_bytes(), &3i64.to_le_bytes()]
				.concat(),
		),
	];
	for (v, bytes) in cases {
		assert_eq!(round_trip(&v), bytes, "{v:?}");
	}
	let thirty = Value::Interval(Interval::new(0, 30, 0));
	let month = Value::Interval(Interval::new(1, 0, 0));
	assert_eq!(thirty, month);
	assert_ne!(encode(&thirty).unwrap(), encode(&month).unwrap());
	for t in [DATE, TIME, TIMESTAMP, INTERVAL] {
		assert_ne!(Kind::of(t), Kind::Unknown, "{t:#04x}");
	}
	assert_eq!(Kind::of(DATE), Kind::Date);
	assert_eq!(Kind::of(INTERVAL), Kind::Interval);
	assert_eq!(Kind::of(BIGDECIMAL), Kind::BigDecimal);
}

#[test]
fn temporal_values_refuse_malformed_and_out_of_range_bytes() {
	assert_eq!(decode(&[DATE, 0x00, 0x00, 0x00, 0x00]).unwrap(), Value::Date(Date(0)));
	assert_eq!(decode(&[TIME, 0, 0, 0, 0, 0, 0, 0, 0]).unwrap(), Value::Time(Time(0)));
	let cases: Vec<(&str, Vec<u8>)> = vec![
		("a date of no bytes", vec![DATE]),
		("a date of five bytes", vec![DATE, 0, 0, 0, 0, 0]),
		("a date out of range", [&[DATE][..], &i32::MAX.to_le_bytes()].concat()),
		("a time of no bytes", vec![TIME]),
		("a time of nine bytes", vec![TIME, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
		("a time past 24 hours", [&[TIME][..], &(Time::NANOS_PER_DAY + 1).to_le_bytes()].concat()),
		("a timestamp of seven bytes", vec![TIMESTAMP, 0, 0, 0, 0, 0, 0, 0]),
		("a timestamp out of range", [&[TIMESTAMP][..], &i64::MAX.to_le_bytes(), &[0; 4]].concat()),
		("an interval of fifteen bytes", [&[INTERVAL][..], &[0; 15]].concat()),
		("an interval of seventeen bytes", [&[INTERVAL][..], &[0; 17]].concat()),
	];
	for (what, bytes) in cases {
		assert!(decode(&bytes).is_err(), "{what} decodes");
		assert!(validate(&bytes).is_err(), "{what} validates");
	}
}

// ---------------------------------------------------------------------------
// Malformed input

#[test]
fn malformed_bytes_are_refused() {
	let cases: Vec<(&str, Vec<u8>)> = vec![
		("empty", vec![]),
		("a payload on NONE", vec![NONE, 0]),
		("a payload on a small int", vec![0x81, 0]),
		("an int of no bytes", vec![INT]),
		("an int of nine bytes", vec![INT, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
		("a float of three bytes", vec![FLOAT, 1, 2, 3]),
		("an i8 of no bytes", vec![I8]),
		("an i8 of two bytes", vec![I8, 1, 2]),
		("a u8 of no bytes", vec![U8]),
		("a u8 of two bytes", vec![U8, 1, 2]),
		("an i16 of three bytes", vec![I16, 1, 2, 3]),
		("a u16 of three bytes", vec![U16, 1, 2, 3]),
		("an i32 of five bytes", vec![I32, 1, 2, 3, 4, 5]),
		("a u32 of five bytes", vec![U32, 1, 2, 3, 4, 5]),
		("a u64 of nine bytes", vec![U64, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
		("an f16 of one byte", vec![F16, 1]),
		("an f16 of three bytes", vec![F16, 1, 2, 3]),
		("an f32 of three bytes", vec![F32, 1, 2, 3]),
		("an f32 of five bytes", vec![F32, 1, 2, 3, 4, 5]),
		("a decimal of thirteen bytes", [&[DECIMAL, 0][..], &[1; 13]].concat()),
		("a decimal with no head", vec![DECIMAL]),
		("a decimal scale past 28", vec![DECIMAL, 29, 1]),
		("invalid UTF-8", vec![STRING, 0xFF]),
		("a duration of seven bytes", vec![DURATION, 0, 0, 0, 0, 0, 0, 0]),
		(
			"a duration with a nanosecond part past a second",
			[&[DURATION][..], &[0; 8], &1_000_000_000u32.to_le_bytes()].concat(),
		),
		("a datetime out of range", [&[DATETIME][..], &i64::MAX.to_le_bytes(), &[0; 4]].concat()),
		("a uuid of fifteen bytes", [&[UUID][..], &[0; 15]].concat()),
		("a record id with a NULL key", vec![RECORD_ID, 0x01, b't', NULL]),
		("a record id with no key", vec![RECORD_ID, 0x01, b't']),
		("a record id whose name runs past it", vec![RECORD_ID, 0x05, b't']),
		("a regex that does not compile", vec![REGEX, b'(']),
		("an unknown bound", vec![RANGE, 0x03, 0x00]),
		("bytes after an unbounded end", vec![RANGE, 0x00, 0x00, 0x81]),
		("a range with no end", vec![RANGE, 0x00]),
		("an empty f64 array", vec![F64_ARRAY]),
		("an f32 array of five bytes", vec![F32_ARRAY, 0, 0, 0, 0, 0]),
		("bytes after an empty container", vec![ARRAY, 0x00, 0x81]),
		("a container with no count", vec![ARRAY]),
		("an offset table past the end", vec![ARRAY, 0x03, 0x00]),
		("bytes before the first entry", vec![ARRAY, 0x01, 0x01, 0x81, 0x81]),
		("offsets that go backwards", vec![ARRAY, 0x02, 0x00, 0x00, 0x81]),
		("an offset past the end", vec![ARRAY, 0x02, 0x00, 0x09, 0x81]),
		(
			"object keys out of order",
			vec![OBJECT, 0x02, 0x00, 0x03, 0x01, b'b', 0x81, 0x01, b'a', 0x81],
		),
		(
			"a repeated object key",
			vec![OBJECT, 0x02, 0x00, 0x03, 0x01, b'a', 0x81, 0x01, b'a', 0x81],
		),
		("an object key past its entry", vec![OBJECT, 0x01, 0x00, 0x05, b'a', 0x81]),
		("an object entry with no value", vec![OBJECT, 0x01, 0x00, 0x01, b'a']),
		("a geometry of an unknown kind", vec![GEOMETRY, 0x07]),
		("a point of fifteen bytes", [&[GEOMETRY, GEO_POINT][..], &[0; 15]].concat()),
		("a point with trailing bytes", [&[GEOMETRY, GEO_POINT][..], &[0; 17]].concat()),
		("a line of seventeen bytes", [&[GEOMETRY, GEO_LINE][..], &[0; 17]].concat()),
		("a polygon with no rings", [&[GEOMETRY, GEO_POLYGON][..], &0u32.to_le_bytes()].concat()),
		(
			"a ring count past the end",
			[&[GEOMETRY, GEO_POLYGON][..], &u32::MAX.to_le_bytes()].concat(),
		),
		(
			"a ring longer than the geometry",
			[&[GEOMETRY, GEO_MULTILINE][..], &1u32.to_le_bytes(), &u32::MAX.to_le_bytes()].concat(),
		),
		(
			"a collection count past the end",
			[&[GEOMETRY, GEO_COLLECTION][..], &u32::MAX.to_le_bytes()].concat(),
		),
		("an unknown tag", vec![0x40]),
		("an unknown tag with a payload", vec![0x40, 0, 0, 0, 0, 0, 0, 0, 0]),
		("the reserved tag of a u8 array", vec![reserved::U8_ARRAY, 1, 0, 0, 0, 7]),
		("the reserved tag 0x7F", vec![0x7F]),
		("the reserved tag 0xFF", vec![0xFF]),
	];
	for (what, bytes) in cases {
		assert!(decode(&bytes).is_err(), "{what}: {bytes:02x?} decoded to {:?}", decode(&bytes));
	}
}

#[test]
fn set_elements_out_of_order_are_sorted() {
	let out_of_order = vec![SET, 0x03, 0x00, 0x01, 0x02, 0x82, 0x81, 0x82];
	assert_eq!(
		decode(&out_of_order).unwrap(),
		Value::Set(Set::from(std::collections::BTreeSet::from([int(1), int(2)])))
	);
}

/// Mutated encodings never panic, in either reader. What the decoder accepts
/// reads the same in place, and is written back to canonical bytes that read
/// back as exactly what it decoded.
#[test]
fn mutated_bytes_never_panic() {
	let mut r = Rng(seed(0xDEAD_BEEF_CAFE_F00D));
	for _ in 0..cases(3_000) {
		let bytes = encode(&r.value(0)).unwrap();
		for _ in 0..20 {
			read_mutated(&r.mutate(&bytes));
		}
	}
}

/// Reads bytes that need not be well formed. Bytes the decoder refuses are read
/// in place as far as they go; bytes it accepts read the same in place, and a
/// key it accepts decodes as a value too.
///
/// What the decoder accepts is written back and read again, unless it holds a
/// geometry: a mutated coordinate may be NaN, which the generator leaves out
/// (see [`Rng`]).
fn read_mutated(b: &[u8]) {
	// Geometries, and record ids keyed by ranges, need not order against one
	// another (see [`Rng`]), so the decoder's sort of a set of them can leave
	// it out of the strict order `VecSet` asserts. Bytes that may hold such a
	// set, by the tags in them, are left out.
	let set = b.iter().any(|t| (SET..SET + 3).contains(t));
	if set && (b.contains(&GEOMETRY) || (b.contains(&RECORD_ID) && b.contains(&RANGE))) {
		return;
	}
	let decoded = decode(b);
	if let Ok(key) = decode_key(b) {
		let v = decoded
			.as_ref()
			.unwrap_or_else(|e| panic!("{key:?} decodes as a key but not as a value: {e}"));
		assert!(identical(v, &key.clone().into_value()), "as a key {key:?}\nas a value {v:?}");
	}
	let Ok(at) = ValueRef::new(b) else {
		return;
	};
	match decoded {
		Err(_) => {
			let _ = walk_loosely(at, 0);
		}
		Ok(v) => {
			read_in_place(at, &v, false);
			if !holds(&v, &|v| matches!(v, Value::Geometry(_))) {
				round_trip(&v);
			}
		}
	}
}

/// Reads `v` through every accessor, descending as far as the bytes allow, for
/// bytes the decoder may refuse: any answer will do, short of a panic.
fn walk_loosely(v: ValueRef<'_>, depth: usize) -> Result<(), Error> {
	if depth > DEPTH_LIMIT {
		return Err(Error::TooDeep(DEPTH_LIMIT));
	}
	let _ = (v.kind(), v.as_bool(), v.as_bytes(), v.as_table());
	v.as_int()?;
	v.as_float()?;
	v.as_decimal()?;
	v.as_str()?;
	v.as_duration()?;
	v.as_datetime()?;
	v.as_uuid()?;
	if let Some((_, key)) = v.as_record_id()? {
		walk_loosely(key, depth + 1)?;
	}
	if let Some(o) = v.as_object()? {
		let n = o.len();
		let ordered = o.ordered();
		for (i, e) in o.iter().enumerate() {
			let (k, ev) = e?;
			if let Ok(ordered) = &ordered {
				for start in hints(i, n) {
					let mut hint = start;
					ordered.get_hinted(k, &mut hint)?;
				}
			}
			walk_loosely(ev, depth + 1)?;
		}
		if let Ok(ordered) = &ordered {
			let _ = ordered.get("");
		}
		let _ = (o.entry(n), o.value_at(n));
	}
	for a in [v.as_array()?, v.as_set()?].into_iter().flatten() {
		for e in a.iter() {
			walk_loosely(e?, depth + 1)?;
		}
		if let Some(floats) = a.floats() {
			let _ = floats.sum::<f64>();
		}
		let _ = a.get(a.len());
	}
	Ok(())
}

/// `validate_key_nested` and `decode_key_nested` accept the same bytes and refuse them with the
/// same error, at the top level and at the depth limit, over the encodings of keys of every kind,
/// every single-byte flip and truncation of them, and bytes that are no key.
#[test]
fn validate_key_agrees_with_decode_key() {
	fn assert_agrees(b: &[u8]) {
		for depth in [0, 1, DEPTH_LIMIT, DEPTH_LIMIT + 1] {
			assert_eq!(
				validate_key_nested(b, depth),
				decode_key_nested(b, depth).map(|_| ()),
				"at depth {depth}, validate_key_nested and decode_key_nested disagree on {b:02x?}"
			);
		}
	}
	let keys = vec![
		RecordIdKey::Number(-5),
		RecordIdKey::Number(1),
		RecordIdKey::Number(i64::MAX),
		RecordIdKey::String(Strand::new("名前")),
		RecordIdKey::Uuid(Uuid(uuid::Uuid::from_u128(7))),
		RecordIdKey::Array(Array(vec![int(1), string("x"), float(2.5)])),
		RecordIdKey::Array(Array(vec![float(1.5), float(2.5)])),
		RecordIdKey::Array(Array(vec![])),
		RecordIdKey::Object(Object::from(BTreeMap::from([("a", int(1)), ("b", string("y"))]))),
		RecordIdKey::Range(Box::new(RecordIdKeyRange {
			start: Bound::Included(RecordIdKey::Number(1)),
			end: Bound::Excluded(RecordIdKey::Range(Box::new(RecordIdKeyRange {
				start: Bound::Unbounded,
				end: Bound::Included(RecordIdKey::String(Strand::new("z"))),
			}))),
		})),
	];
	for k in keys {
		let mut enc = Encoder::default();
		enc.value(&rid("t", k)).unwrap();
		let bytes = enc.as_bytes()[3..].to_vec();
		assert_agrees(&bytes);
		let mut mutant = bytes.clone();
		for at in 0..bytes.len() {
			for bit in 0..8 {
				mutant[at] ^= 1 << bit;
				assert_agrees(&mutant);
				mutant[at] ^= 1 << bit;
			}
		}
		for length in 0..bytes.len() {
			assert_agrees(&bytes[..length]);
		}
	}
	for malformed in [
		vec![],
		vec![NONE],
		vec![NULL],
		vec![TRUE],
		vec![INT, 1, 2],
		vec![STRING, 0xFF],
		vec![UUID, 0, 0],
		vec![0x7F],
		vec![0x80, 0x00],
		vec![RANGE, 0x03, 0x00],
		vec![ARRAY, 0x03, 0x00],
		vec![OBJECT, 0x01, 0x00, 0x05, b'a', 0x81],
		vec![FLOAT, 0, 0, 0, 0, 0, 0, 0, 0],
		vec![RECORD_ID, 0x01, b't', 0x81],
	] {
		assert_agrees(&malformed);
	}
}

/// `validate` and `decode` accept the same bytes, and where both refuse they refuse with the same
/// error, at the top level and at the depth limit. Checked on malformed byte strings, on valid
/// encodings, and on every single-byte flip and truncation of those encodings.
#[test]
fn validate_agrees_with_decode() {
	fn assert_agrees(b: &[u8]) {
		assert_eq!(validate(b), decode(b).map(|_| ()), "validate and decode disagree on {b:02x?}");
		for depth in [0, DEPTH_LIMIT] {
			assert_eq!(
				validate_nested(b, depth),
				decode_nested(b, depth).map(|_| ()),
				"at depth {depth}, validate and decode disagree on {b:02x?}"
			);
		}
	}
	// A set's elements are checked by building them, and a set holding geometries, or record ids
	// keyed by ranges, need not order against one another. Bytes that may hold such a set are left
	// out, as in `read_mutated`.
	let may_break_set_order = |b: &[u8]| {
		b.iter().any(|t| (SET..SET + 3).contains(t))
			&& (b.contains(&GEOMETRY) || (b.contains(&RECORD_ID) && b.contains(&RANGE)))
	};

	let malformed: Vec<Vec<u8>> = vec![
		vec![],
		vec![NONE, 0],
		vec![INT, 1, 2, 3, 4, 5, 6, 7, 8, 9],
		vec![FLOAT, 1, 2, 3],
		[&[DECIMAL, 0][..], &[1; 13]].concat(),
		vec![STRING, 0xFF],
		[&[DURATION][..], &[0; 8], &1_000_000_000u32.to_le_bytes()].concat(),
		[&[DATETIME][..], &i64::MAX.to_le_bytes(), &[0; 4]].concat(),
		[&[UUID][..], &[0; 15]].concat(),
		vec![RECORD_ID, 0x01, b't', NULL],
		vec![REGEX, b'('],
		vec![RANGE, 0x03, 0x00],
		vec![RANGE, 0x00, 0x00, 0x81],
		vec![F64_ARRAY],
		vec![ARRAY, 0x03, 0x00],
		vec![OBJECT, 0x02, 0x00, 0x03, 0x01, b'b', 0x81, 0x01, b'a', 0x81],
		vec![OBJECT, 0x01, 0x00, 0x05, b'a', 0x81],
		[&[GEOMETRY, GEO_POINT][..], &[0; 17]].concat(),
		[&[GEOMETRY, GEO_POLYGON][..], &u32::MAX.to_le_bytes()].concat(),
		vec![0x40],
		vec![0x7F],
	];
	for b in &malformed {
		assert!(decode(b).is_err(), "{b:02x?} should be refused");
		assert_agrees(b);
	}

	let mut values = vec![
		Value::None,
		Value::Null,
		Value::Bool(true),
		int(-1),
		int(126),
		int(i64::MIN),
		float(1.5),
		float(0.1),
		Value::Number(Number::Decimal(Decimal::new(125, 2))),
		string("héllo"),
		Value::Duration(Duration(std::time::Duration::new(u64::MAX, 999_999_999))),
		datetime(1_700_000_000, 5),
		Value::Uuid(Uuid(uuid::Uuid::from_u128(7))),
		Value::Bytes(Bytes(::bytes::Bytes::from_static(&[9, 8]))),
		Value::Table(TableName::new("t")),
		Value::File(File {
			bucket: "b".to_owned(),
			key: "/k".to_owned(),
		}),
		Value::Regex(Regex::from_str("^[a-z]+$").unwrap()),
		Value::Geometry(Geometry::Point(Point::new(1.0, 2.0))),
		Value::Range(Box::new(Range {
			start: Bound::Included(int(1)),
			end: Bound::Excluded(string("z")),
		})),
		rid("t", RecordIdKey::Number(-5)),
		rid("t", RecordIdKey::String(Strand::new("名前"))),
		rid("t", RecordIdKey::Uuid(Uuid(uuid::Uuid::from_u128(7)))),
		array(vec![]),
		array(vec![int(1), int(2)]),
		array(vec![float(1.5), float(2.5)]),
		array(vec![int(1), string("x")]),
		Value::Set(Set::from(BTreeSet::from([int(1), string("a")]))),
		object(vec![("a", int(1)), ("b", array(vec![float(0.1)]))]),
	];
	let mut r = Rng(seed(0x5EED_CAFE));
	for _ in 0..cases(120) {
		values.push(r.value(0));
	}
	for v in &values {
		let bytes = encode(v).expect("encodes");
		if may_break_set_order(&bytes[..]) {
			continue;
		}
		assert!(validate(&bytes).is_ok(), "{bytes:02x?} encodes but does not validate");
		assert_agrees(&bytes);
		for i in 0..bytes.len() {
			let mut flipped = bytes.clone();
			flipped[i] ^= 0xFF;
			if !may_break_set_order(&flipped[..]) {
				assert_agrees(&flipped);
			}
		}
		for n in 0..bytes.len() {
			let cut = &bytes[..n];
			if !may_break_set_order(cut) {
				assert_agrees(cut);
			}
		}
	}
}
