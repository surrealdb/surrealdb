//! What the stored-value fuzz targets share: a strict comparison of values, a
//! walk that checks the in-place reader against the full decoder, a generator
//! of values driven by the fuzzer's bytes, and an allocator that measures what
//! a decode allocates.

#![allow(dead_code, reason = "each target uses a different part of this module")]

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::ops::Bound;
use std::sync::atomic::{AtomicUsize, Ordering};

use libfuzzer_sys::arbitrary::{Arbitrary, Unstructured};
use surrealdb_expr::val::stored::{self, ArrayRef, DEPTH_LIMIT, Kind, ObjectRef, ValueRef};
use surrealdb_expr::val::{
	Array, Bytes, Datetime, Duration, File, Geometry, MAX_VALUE_DEPTH, Number, Object, Range,
	RecordId, RecordIdKey, RecordIdKeyRange, Regex, Set, Strand, TableName, Uuid, Value,
};
use surrealdb_types::{Decimal, f16};

// ---------------------------------------------------------------------------
// Strict comparison

/// Whether `a` and `b` are the same value: the same variant at every level,
/// floats with the same bits, decimals with the same scale and sign, and
/// object entries and set elements in the same order.
///
/// `Value`'s `==` is numeric, so `1 == 1.0`, `0.0 == -0.0` and `1.5 == 1.50`,
/// and its `Debug` prints every NaN alike. Neither tells a round trip that
/// loses something from one that does not.
pub fn identical(a: &Value, b: &Value) -> bool {
	match (a, b) {
		(Value::None, Value::None) | (Value::Null, Value::Null) => true,
		(Value::Bool(a), Value::Bool(b)) => a == b,
		(Value::Number(a), Value::Number(b)) => same_number(a, b),
		(Value::String(a), Value::String(b)) => a.as_str() == b.as_str(),
		(Value::Duration(a), Value::Duration(b)) => a.0 == b.0,
		// `DateTime` equality covers a leap second's sub-second part.
		(Value::Datetime(a), Value::Datetime(b)) => a.0 == b.0,
		(Value::Uuid(a), Value::Uuid(b)) => a.0 == b.0,
		(Value::Date(a), Value::Date(b)) => a == b,
		(Value::Time(a), Value::Time(b)) => a == b,
		(Value::Timestamp(a), Value::Timestamp(b)) => a == b,
		(Value::Interval(a), Value::Interval(b)) => a.identical(b),
		(Value::Array(a), Value::Array(b)) => all_identical(&a.0, &b.0),
		(Value::Set(a), Value::Set(b)) => {
			a.0.len() == b.0.len() && a.0.iter().zip(b.0.iter()).all(|(a, b)| identical(a, b))
		}
		(Value::Object(a), Value::Object(b)) => same_object(a, b),
		(Value::Geometry(a), Value::Geometry(b)) => same_geometry(a, b),
		(Value::Bytes(a), Value::Bytes(b)) => a.0 == b.0,
		(Value::Table(a), Value::Table(b)) => a.as_str() == b.as_str(),
		(Value::RecordId(a), Value::RecordId(b)) => same_record_id(a, b),
		(Value::File(a), Value::File(b)) => a.bucket == b.bucket && a.key == b.key,
		// A regex compares by its pattern.
		(Value::Regex(a), Value::Regex(b)) => a == b,
		(Value::Range(a), Value::Range(b)) => {
			same_bound(&a.start, &b.start, identical) && same_bound(&a.end, &b.end, identical)
		}
		_ => false,
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
			| Number::I8(_)
			| Number::U8(_)
			| Number::I16(_)
			| Number::U16(_)
			| Number::I32(_)
			| Number::U32(_)
			| Number::U64(_)
			| Number::F16(_)
			| Number::F32(_)
			| Number::BigDecimal(_) => false,
		},
		Number::Float(a) => match b {
			Number::Float(b) => a.to_bits() == b.to_bits(),
			Number::Int(_)
			| Number::Decimal(_)
			| Number::I8(_)
			| Number::U8(_)
			| Number::I16(_)
			| Number::U16(_)
			| Number::I32(_)
			| Number::U32(_)
			| Number::U64(_)
			| Number::F16(_)
			| Number::F32(_)
			| Number::BigDecimal(_) => false,
		},
		Number::Decimal(a) => match b {
			// The serialized form holds the sign and scale as well as the mantissa.
			Number::Decimal(b) => a.serialize() == b.serialize(),
			Number::Int(_)
			| Number::Float(_)
			| Number::I8(_)
			| Number::U8(_)
			| Number::I16(_)
			| Number::U16(_)
			| Number::I32(_)
			| Number::U32(_)
			| Number::U64(_)
			| Number::F16(_)
			| Number::F32(_)
			| Number::BigDecimal(_) => false,
		},
		Number::I8(a) => match b {
			Number::I8(b) => a == b,
			Number::Int(_)
			| Number::Float(_)
			| Number::Decimal(_)
			| Number::U8(_)
			| Number::I16(_)
			| Number::U16(_)
			| Number::I32(_)
			| Number::U32(_)
			| Number::U64(_)
			| Number::F16(_)
			| Number::F32(_)
			| Number::BigDecimal(_) => false,
		},
		Number::U8(a) => match b {
			Number::U8(b) => a == b,
			Number::Int(_)
			| Number::Float(_)
			| Number::Decimal(_)
			| Number::I8(_)
			| Number::I16(_)
			| Number::U16(_)
			| Number::I32(_)
			| Number::U32(_)
			| Number::U64(_)
			| Number::F16(_)
			| Number::F32(_)
			| Number::BigDecimal(_) => false,
		},
		Number::I16(a) => match b {
			Number::I16(b) => a == b,
			Number::Int(_)
			| Number::Float(_)
			| Number::Decimal(_)
			| Number::I8(_)
			| Number::U8(_)
			| Number::U16(_)
			| Number::I32(_)
			| Number::U32(_)
			| Number::U64(_)
			| Number::F16(_)
			| Number::F32(_)
			| Number::BigDecimal(_) => false,
		},
		Number::U16(a) => match b {
			Number::U16(b) => a == b,
			Number::Int(_)
			| Number::Float(_)
			| Number::Decimal(_)
			| Number::I8(_)
			| Number::U8(_)
			| Number::I16(_)
			| Number::I32(_)
			| Number::U32(_)
			| Number::U64(_)
			| Number::F16(_)
			| Number::F32(_)
			| Number::BigDecimal(_) => false,
		},
		Number::I32(a) => match b {
			Number::I32(b) => a == b,
			Number::Int(_)
			| Number::Float(_)
			| Number::Decimal(_)
			| Number::I8(_)
			| Number::U8(_)
			| Number::I16(_)
			| Number::U16(_)
			| Number::U32(_)
			| Number::U64(_)
			| Number::F16(_)
			| Number::F32(_)
			| Number::BigDecimal(_) => false,
		},
		Number::U32(a) => match b {
			Number::U32(b) => a == b,
			Number::Int(_)
			| Number::Float(_)
			| Number::Decimal(_)
			| Number::I8(_)
			| Number::U8(_)
			| Number::I16(_)
			| Number::U16(_)
			| Number::I32(_)
			| Number::U64(_)
			| Number::F16(_)
			| Number::F32(_)
			| Number::BigDecimal(_) => false,
		},
		Number::U64(a) => match b {
			Number::U64(b) => a == b,
			Number::Int(_)
			| Number::Float(_)
			| Number::Decimal(_)
			| Number::I8(_)
			| Number::U8(_)
			| Number::I16(_)
			| Number::U16(_)
			| Number::I32(_)
			| Number::U32(_)
			| Number::F16(_)
			| Number::F32(_)
			| Number::BigDecimal(_) => false,
		},
		Number::F16(a) => match b {
			Number::F16(b) => a.to_bits() == b.to_bits(),
			Number::Int(_)
			| Number::Float(_)
			| Number::Decimal(_)
			| Number::I8(_)
			| Number::U8(_)
			| Number::I16(_)
			| Number::U16(_)
			| Number::I32(_)
			| Number::U32(_)
			| Number::U64(_)
			| Number::F32(_)
			| Number::BigDecimal(_) => false,
		},
		Number::F32(a) => match b {
			Number::F32(b) => a.to_bits() == b.to_bits(),
			Number::Int(_)
			| Number::Float(_)
			| Number::Decimal(_)
			| Number::I8(_)
			| Number::U8(_)
			| Number::I16(_)
			| Number::U16(_)
			| Number::I32(_)
			| Number::U32(_)
			| Number::U64(_)
			| Number::F16(_)
			| Number::BigDecimal(_) => false,
		},
		// A bigdecimal keeps its scale, so `1.50` and `1.5` are two values here.
		Number::BigDecimal(a) => match b {
			Number::BigDecimal(b) => a == b && a.scale() == b.scale(),
			Number::Int(_)
			| Number::Float(_)
			| Number::Decimal(_)
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
	}
}

fn same_object(a: &Object, b: &Object) -> bool {
	a.0.len() == b.0.len()
		&& a.0
			.iter()
			.zip(b.0.iter())
			.all(|((ka, va), (kb, vb))| ka.as_str() == kb.as_str() && identical(va, vb))
}

pub fn same_record_id(a: &RecordId, b: &RecordId) -> bool {
	a.table.as_str() == b.table.as_str() && same_key(&a.key, &b.key)
}

pub fn same_key(a: &RecordIdKey, b: &RecordIdKey) -> bool {
	match (a, b) {
		(RecordIdKey::Number(a), RecordIdKey::Number(b)) => a == b,
		(RecordIdKey::String(a), RecordIdKey::String(b)) => a.as_str() == b.as_str(),
		(RecordIdKey::Uuid(a), RecordIdKey::Uuid(b)) => a.0 == b.0,
		(RecordIdKey::Array(a), RecordIdKey::Array(b)) => all_identical(&a.0, &b.0),
		(RecordIdKey::Object(a), RecordIdKey::Object(b)) => same_object(a, b),
		(RecordIdKey::Range(a), RecordIdKey::Range(b)) => {
			same_bound(&a.start, &b.start, same_key) && same_bound(&a.end, &b.end, same_key)
		}
		_ => false,
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

/// Coordinates compare as arrays of floats, bit for bit; a collection compares
/// member by member, since its coordinate array drops the members' kinds.
fn same_geometry(a: &Geometry, b: &Geometry) -> bool {
	match (a, b) {
		(Geometry::Collection(a), Geometry::Collection(b)) => {
			a.len() == b.len() && a.iter().zip(b).all(|(a, b)| same_geometry(a, b))
		}
		(a, b) => a.as_type() == b.as_type() && identical(&a.as_coordinates(), &b.as_coordinates()),
	}
}

/// The leading bytes of `b` in hex, for a failure message.
pub fn hex(b: &[u8]) -> String {
	let mut out = String::new();
	for byte in b.iter().take(1024) {
		let _ = write!(out, "{byte:02x}");
	}
	if b.len() > 1024 {
		let _ = write!(out, "... ({} bytes)", b.len());
	}
	out
}

// ---------------------------------------------------------------------------
// The in-place reader against the decoder

/// Checks the in-place reading of `r` against `v`, the decoder's reading of the
/// same bytes, at every level: each accessor answers for exactly the variant
/// `v` has; each object field and array element found in place is the entry
/// the decoder read, at the same offset, whatever hint the lookup starts from;
/// and fields and elements that are not there are not found.
///
/// `canonical` bytes are the encoder's own, in which a set's elements are in
/// value order and every non-empty array of floats takes a typed form. Bytes
/// the decoder accepts may differ in both.
pub fn walk(r: ValueRef<'_>, v: &Value, canonical: bool) {
	assert_eq!(r.kind(), kind(v), "the kind of {v:?}");
	// A container and a record id are checked part by part below. Anything
	// else is decoded from where the reader found it, so the walk decodes each
	// byte once and stays linear in the size of the value.
	if !matches!(v, Value::Array(_) | Value::Set(_) | Value::Object(_) | Value::RecordId(_)) {
		let back = r.decode().unwrap_or_else(|e| panic!("in place, {v:?} does not decode: {e}"));
		assert!(identical(&back, v), "in place {back:?}\ndecoded  {v:?}");
	}
	assert_eq!(
		r.as_bool(),
		match v {
			Value::Bool(b) => Some(*b),
			_ => None,
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
	assert_eq!(read(r.as_int()), expected_int);
	let expected_float = match v {
		Value::Number(n) => match n {
			Number::Float(f) => Some(f.to_bits()),
			Number::F16(f) => Some(f.to_f64().to_bits()),
			Number::F32(f) => Some((*f as f64).to_bits()),
			Number::Int(_)
			| Number::Decimal(_)
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
	assert_eq!(read(r.as_float()).map(f64::to_bits), expected_float);
	assert_eq!(
		read(r.as_decimal()).map(|d| d.serialize()),
		match v {
			Value::Number(Number::Decimal(d)) => Some(d.serialize()),
			_ => None,
		}
	);
	assert_eq!(
		read(r.as_str()),
		match v {
			Value::String(s) => Some(s.as_str()),
			_ => None,
		}
	);
	assert_eq!(
		r.as_bytes(),
		match v {
			Value::Bytes(b) => Some(&b.0[..]),
			_ => None,
		}
	);
	assert_eq!(
		read(r.as_duration()),
		match v {
			Value::Duration(d) => Some(d.0),
			_ => None,
		}
	);
	assert_eq!(
		read(r.as_datetime()),
		match v {
			Value::Datetime(d) => Some(d.0),
			_ => None,
		}
	);
	assert_eq!(
		read(r.as_uuid()),
		match v {
			Value::Uuid(u) => Some(u.0),
			_ => None,
		}
	);
	assert_eq!(
		read(r.as_table()),
		match v {
			Value::Table(t) => Some(t.as_str()),
			_ => None,
		}
	);
	match (read(r.as_record_id()), v) {
		(Some((table, key)), Value::RecordId(rid)) => {
			assert_eq!(table, rid.table.as_str());
			walk_key(key, &rid.key, canonical);
		}
		(None, Value::RecordId(_)) | (Some(_), _) => panic!("as_record_id disagrees with {v:?}"),
		(None, _) => {}
	}
	match (read(r.as_object()), v) {
		(Some(o), Value::Object(obj)) => walk_object(o, obj, canonical),
		(None, Value::Object(_)) | (Some(_), _) => panic!("as_object disagrees with {v:?}"),
		(None, _) => {}
	}
	match (read(r.as_array()), v) {
		(Some(a), Value::Array(arr)) => walk_array(a, &arr.0, canonical),
		(None, Value::Array(_)) | (Some(_), _) => panic!("as_array disagrees with {v:?}"),
		(None, _) => {}
	}
	match (read(r.as_set()), v) {
		(Some(a), Value::Set(set)) => walk_set(a, set, canonical),
		(None, Value::Set(_)) | (Some(_), _) => panic!("as_set disagrees with {v:?}"),
		(None, _) => {}
	}
}

/// An accessor's answer, which on bytes the decoder accepts is never an error.
fn read<T>(r: Result<T, stored::Error>) -> T {
	r.unwrap_or_else(|e| panic!("an accessor fails on bytes the decoder accepts: {e}"))
}

/// [`walk`] for a record id's key, read in place as a value.
fn walk_key(r: ValueRef<'_>, key: &RecordIdKey, canonical: bool) {
	match key {
		RecordIdKey::Number(n) => assert_eq!((r.kind(), read(r.as_int())), (Kind::Int, Some(*n))),
		RecordIdKey::String(s) => {
			assert_eq!((r.kind(), read(r.as_str())), (Kind::String, Some(s.as_str())))
		}
		RecordIdKey::Uuid(u) => assert_eq!((r.kind(), read(r.as_uuid())), (Kind::Uuid, Some(u.0))),
		RecordIdKey::Array(a) => match read(r.as_array()) {
			Some(at) => walk_array(at, &a.0, canonical),
			None => panic!("the key {key:?} is not read as an array"),
		},
		RecordIdKey::Object(o) => match read(r.as_object()) {
			Some(at) => walk_object(at, o, canonical),
			None => panic!("the key {key:?} is not read as an object"),
		},
		// A range has no reader in place: its bytes decode, as a key and as a
		// value, to the key the decoder read.
		RecordIdKey::Range(_) => {
			let mut bytes = Vec::new();
			r.write_to(&mut bytes);
			let k = stored::decode_key(&bytes)
				.unwrap_or_else(|e| panic!("the key {key:?} does not decode in place: {e}"));
			assert!(same_key(&k, key), "key in place {k:?}\ndecoded      {key:?}");
			let back = read(r.decode());
			assert!(
				identical(&back, &key.clone().into_value()),
				"in place {back:?}\nkey      {key:?}"
			);
		}
	}
}

fn walk_object(o: ObjectRef<'_>, obj: &Object, canonical: bool) {
	let o = read(o.ordered());
	let n = obj.0.len();
	assert_eq!((o.len(), o.is_empty()), (n, n == 0));
	// Every field of a small object is looked up from every hint; a sample of
	// a large one's, so the walk stays linear in its size.
	let stride = (n / 64).max(1);
	let mut entries = o.iter();
	for (i, (key, value)) in obj.0.iter().enumerate() {
		let (k, at) = read(entries.next().expect("an entry for every field"));
		assert_eq!(k, key.as_str());
		let (k2, at2) = read(o.entry(i));
		assert_eq!(k2, k);
		same_place(at2, at);
		same_place(read(o.value_at(i)), at);
		same_place(read(o.get(k)).unwrap_or_else(|| panic!("{k:?} is not found")), at);
		for start in hints(i, n).into_iter().filter(|_| i % stride == 0) {
			let mut hint = start;
			let found = read(o.get_hinted(k, &mut hint))
				.unwrap_or_else(|| panic!("{k:?} is not found from hint {start}"));
			same_place(found, at);
			assert_eq!(hint as usize, i, "the hint after finding {k:?} from {start}");
		}
		walk(at, value, canonical);
	}
	assert!(entries.next().is_none(), "more entries than fields");
	for key in absent(obj) {
		assert_eq!(read(o.get(&key)), None, "{key:?} is found but absent");
		for start in hints(n / 2, n) {
			let mut hint = start;
			assert_eq!(read(o.get_hinted(&key, &mut hint)), None, "{key:?} from hint {start}");
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
		keys.push(format!("{k}\0"));
		keys.push(format!("{k}~"));
		let mut shorter = k.to_owned();
		shorter.pop();
		keys.push(shorter);
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

fn walk_array(a: ArrayRef<'_>, items: &[Value], canonical: bool) {
	let n = items.len();
	assert_eq!((a.len(), a.is_empty()), (n, n == 0));
	let mut elements = a.iter();
	for (i, item) in items.iter().enumerate() {
		let at = read(a.get(i)).unwrap_or_else(|| panic!("no element {i} of {n}"));
		same_place(read(elements.next().expect("an element for every item")), at);
		walk(at, item, canonical);
	}
	assert!(elements.next().is_none(), "more elements than items");
	for i in [n, n + 1, usize::MAX] {
		assert_eq!(read(a.get(i)), None, "element {i} of {n}");
	}
	match a.floats() {
		Some(floats) => {
			assert_eq!(floats.len(), n);
			for (f, item) in floats.zip(items) {
				match item {
					Value::Number(Number::Float(g)) => assert_eq!(f.to_bits(), g.to_bits()),
					_ => panic!("floats() reads {f} where the decoder reads {item:?}"),
				}
			}
		}
		None => assert!(
			!canonical
				|| n == 0 || !items.iter().all(|v| matches!(v, Value::Number(Number::Float(_)))),
			"the encoder wrote an array of floats untyped"
		),
	}
}

fn walk_set(a: ArrayRef<'_>, set: &Set, canonical: bool) {
	let at: Vec<ValueRef<'_>> =
		(0..a.len()).map(|i| read(a.get(i)).unwrap_or_else(|| panic!("no element {i}"))).collect();
	assert!(a.floats().is_none(), "a set read as floats");
	assert_eq!(read(a.get(at.len())), None);
	// The encoder writes a set's elements in value order, so in its bytes the
	// elements in place are the set's, one for one.
	if canonical {
		assert_eq!(at.len(), set.0.len(), "a set's elements in place");
		for (e, item) in at.iter().zip(set.0.iter()) {
			walk(*e, item, canonical);
		}
		return;
	}
	// Elsewhere the decoder orders and deduplicates elements out of order.
	let mut elements: Vec<Value> = at.iter().map(|e| read(e.decode())).collect();
	if elements.windows(2).all(|w| w[0] < w[1]) {
		assert_eq!(elements.len(), set.0.len(), "a set's elements in place");
		for (e, item) in at.iter().zip(set.0.iter()) {
			walk(*e, item, canonical);
		}
	} else {
		elements.sort_unstable();
		elements.dedup();
		assert_eq!(elements.len(), set.0.len(), "a set's elements in place, reordered");
		for (e, item) in elements.iter().zip(set.0.iter()) {
			assert!(identical(e, item), "in place {e:?}\ndecoded  {item:?}");
		}
		for e in &at {
			let v = read(e.decode());
			walk(*e, &v, canonical);
		}
	}
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

/// Reads `r` through every accessor, descending as far as the bytes allow, for
/// bytes the decoder refuses. Any answer will do, short of a panic or of work
/// out of proportion to the bytes.
pub fn read_loosely(r: ValueRef<'_>, depth: usize) {
	if depth > DEPTH_LIMIT {
		return;
	}
	let _ = (r.kind(), r.as_bool(), r.as_bytes(), r.as_int(), r.as_float(), r.as_decimal());
	let _ = (r.as_str(), r.as_duration(), r.as_datetime(), r.as_uuid(), r.as_table());
	if let Ok(Some((_, key))) = r.as_record_id() {
		let mut bytes = Vec::new();
		key.write_to(&mut bytes);
		let _ = stored::decode_key(&bytes);
		read_loosely(key, depth + 1);
	}
	if let Ok(Some(o)) = r.as_object() {
		let n = o.len();
		let ordered = o.ordered();
		for (i, e) in o.iter().enumerate() {
			let Ok((k, v)) = e else {
				continue;
			};
			if let Ok(ordered) = &ordered {
				for start in hints(i, n) {
					let mut hint = start;
					let _ = ordered.get_hinted(k, &mut hint);
				}
			}
			read_loosely(v, depth + 1);
		}
		if let Ok(ordered) = &ordered {
			let _ = (ordered.get(""), ordered.get("id"));
		}
		let _ = (o.entry(n), o.value_at(n));
	}
	for a in [r.as_array(), r.as_set()].into_iter().flatten().flatten() {
		for e in a.iter().flatten() {
			read_loosely(e, depth + 1);
		}
		if let Some(floats) = a.floats() {
			let _ = floats.fold(0.0, |sum, f| sum + f);
		}
		let _ = a.get(a.len());
	}
}

// ---------------------------------------------------------------------------
// Generation

/// How many values one input may generate.
const BUDGET: usize = 200_000;

/// Builds values from the fuzzer's bytes, after the seeded generator in the
/// stored-value unit tests, reaching what that one leaves out: NaNs with
/// payloads, every decimal scale and sign, both edges of the nanosecond time
/// forms, sets built the ways set functions build them, containers either side
/// of an offset-width boundary, and nesting either side of the depth limits.
///
/// Every choice draws on the bytes. Once they run out every choice is the
/// first, which is always the smallest, and once the budget is spent every
/// value is `NONE`, so a value is always finite and bounded in size.
pub struct Gen<'a, 'b> {
	u: &'b mut Unstructured<'a>,
	budget: usize,
}

impl<'a, 'b> Gen<'a, 'b> {
	pub fn new(u: &'b mut Unstructured<'a>) -> Self {
		Self {
			u,
			budget: BUDGET,
		}
	}

	/// A choice of `0..n`.
	pub fn below(&mut self, n: u64) -> u64 {
		match n {
			0 | 1 => 0,
			n => self.u.int_in_range(0..=n - 1).unwrap_or(0),
		}
	}

	pub fn bits(&mut self) -> u64 {
		u64::arbitrary(self.u).unwrap_or(0)
	}

	fn pick<T: Copy>(&mut self, from: &[T]) -> T {
		from[self.below(from.len() as u64) as usize]
	}

	/// A value of any shape: usually an ordinary one, sometimes one nested to
	/// either side of a depth limit.
	pub fn any(&mut self) -> Value {
		match self.below(8) {
			7 => self.deep(),
			_ => self.value(0),
		}
	}

	/// A value whose containers nest a few levels at most.
	pub fn value(&mut self, depth: usize) -> Value {
		if self.budget == 0 {
			return Value::None;
		}
		self.budget -= 1;
		// Past a few levels only leaves, so that a value stays small; `deep`
		// builds the deep ones.
		let kinds = if depth >= 4 {
			17
		} else {
			25
		};
		match self.below(kinds) {
			0 => Value::None,
			1 => Value::Null,
			2 => Value::Bool(self.below(2) == 1),
			3 => Value::Number(Number::Int(self.below(127) as i64)),
			4 => Value::Number(Number::Int(self.int())),
			5 => Value::Number(Number::Float(self.float())),
			6 => Value::Number(Number::Decimal(self.decimal())),
			7 => Value::String(Strand::new(self.string())),
			8 => Value::Duration(Duration(self.duration())),
			9 => Value::Datetime(self.datetime()),
			10 => Value::Uuid(self.uuid()),
			11 => Value::Bytes(self.bytes()),
			12 => Value::Table(TableName::new(self.string())),
			13 => Value::File(self.file()),
			14 => Value::Regex(self.regex()),
			15 => Value::Geometry(self.geometry(0)),
			16 => self.number(),
			17 => Value::RecordId(RecordId {
				table: TableName::new(self.string()),
				key: self.key(depth + 1),
			}),
			18 | 19 => Value::Array(self.array(depth)),
			20 => Value::Set(self.set(depth)),
			21 | 22 => Value::Object(self.object(depth)),
			23 => Value::Range(Box::new(Range {
				start: self.bound(|g| g.value(depth + 1)),
				end: self.bound(|g| g.value(depth + 1)),
			})),
			_ => self.sized(),
		}
	}

	/// An integer of any width, the edges of each width more likely.
	pub fn int(&mut self) -> i64 {
		match self.below(4) {
			0 => self.pick(&[
				0,
				1,
				126,
				127,
				128,
				255,
				256,
				-1,
				-2,
				-127,
				-128,
				-129,
				-256,
				-257,
				i64::MIN,
				i64::MIN + 1,
				i64::MAX,
				i64::MAX - 1,
			]),
			// Either side of each boundary between widths.
			1 => {
				let v = 1i64 << (7 + 8 * self.below(7));
				self.pick(&[v - 1, v, -v, -v - 1])
			}
			_ => (self.bits() as i64) >> self.below(64),
		}
	}

	/// A float of any bits: NaNs of every sign and payload, infinities, both
	/// zeros, subnormals, values an `f32` holds exactly and values one step off.
	pub fn float(&mut self) -> f64 {
		match self.below(8) {
			0 => self.pick(&[
				0.0,
				-0.0,
				1.0,
				-1.5,
				0.1,
				f64::INFINITY,
				f64::NEG_INFINITY,
				f64::MIN_POSITIVE,
				f64::MAX,
				f64::MIN,
				f64::EPSILON,
				5e-324,
				-5e-324,
				f32::MAX as f64,
				f32::MIN_POSITIVE as f64,
				f32::from_bits(1) as f64,
				16_777_217.0,
				f64::NAN,
			]),
			1 => {
				let b = self.bits();
				f64::from_bits(
					0x7FF0_0000_0000_0000
						| (b & 0x8000_0000_0000_0000)
						| (b & 0x000F_FFFF_FFFF_FFFF).max(1),
				)
			}
			2 | 3 => f32::from_bits(self.bits() as u32) as f64,
			4 => f64::from_bits((f32::from_bits(self.bits() as u32) as f64).to_bits() ^ 1),
			5 => f64::from_bits(self.bits() & 0x800F_FFFF_FFFF_FFFF),
			_ => f64::from_bits(self.bits()),
		}
	}

	/// A decimal of any scale and sign, negative zero included, its mantissa
	/// often at the edge of a byte width.
	pub fn decimal(&mut self) -> Decimal {
		const MAX: i128 = (1 << 96) - 1;
		let mantissa = match self.below(5) {
			0 => 0,
			1 => MAX >> (8 * self.below(12)),
			2 => 1 << (8 * self.below(12)),
			3 => self.below(1000) as i128,
			_ => ((((self.bits() as i128) << 64) | self.bits() as i128) & MAX) >> self.below(96),
		};
		let mut d = Decimal::try_from_i128_with_scale(mantissa, self.below(29) as u32)
			.unwrap_or(Decimal::ZERO);
		if self.below(2) == 1 {
			d.set_sign_negative(true);
		}
		d
	}

	/// A bigdecimal of 1 to 80 digits, a scale of 0 to 70 and either sign: past what a decimal
	/// holds as often as within it.
	pub fn bigdecimal(&mut self) -> surrealdb_types::BigDecimal {
		let len = 1 + self.below(80) as usize;
		let mut text: String = (0..len).map(|_| char::from(b'0' + self.below(10) as u8)).collect();
		let scale = self.below(71) as usize;
		if scale > 0 {
			if text.len() <= scale {
				text = format!("{}{text}", "0".repeat(scale + 1 - text.len()));
			}
			text.insert(text.len() - scale, '.');
		}
		if self.below(2) == 1 {
			text.insert(0, '-');
		}
		text.parse().unwrap_or_default()
	}

	/// A number of any variant within a few units in the last place of one of
	/// a few values, so that the variants interleave when ordered.
	pub fn number(&mut self) -> Value {
		let base: f64 =
			self.pick(&[0.0, 0.1, 1.0, -2.5, 123.456, 1e-10, 1e16, 9_007_199_254_740_993.0]);
		Value::Number(match self.below(12) {
			0 => Number::Int((base as i64).wrapping_add(self.below(3) as i64 - 1)),
			1 => Number::Float(f64::from_bits(
				base.to_bits().wrapping_add(self.below(5)).wrapping_sub(2),
			)),
			2 => {
				let d = Decimal::try_from(base).unwrap_or(Decimal::ZERO);
				let m = d.mantissa() + self.below(5) as i128 - 2;
				Number::Decimal(Decimal::try_from_i128_with_scale(m, d.scale()).unwrap_or(d))
			}
			3 => Number::I8((base as i8).wrapping_add(self.below(3) as i8 - 1)),
			4 => Number::U8((base as u8).wrapping_add(self.below(3) as u8)),
			5 => Number::I16((base as i16).wrapping_add(self.below(3) as i16 - 1)),
			6 => Number::U16((base as u16).wrapping_add(self.below(3) as u16)),
			7 => Number::I32((base as i32).wrapping_add(self.below(3) as i32 - 1)),
			8 => Number::U32((base as u32).wrapping_add(self.below(3) as u32)),
			9 => Number::U64((base as u64).wrapping_add(self.below(3))),
			10 => Number::F16(f16::from_bits(
				f16::from_f64(base)
					.to_bits()
					.wrapping_add(self.below(5) as u16)
					.wrapping_sub(2),
			)),
			_ => Number::F32(f32::from_bits(
				(base as f32)
					.to_bits()
					.wrapping_add(self.below(5) as u32)
					.wrapping_sub(2),
			)),
		})
	}

	/// A string: often short and made of awkward parts, sometimes either side
	/// of the length a one-byte length prefix holds.
	pub fn string(&mut self) -> String {
		const PARTS: [&str; 12] = [
			"a",
			"b",
			"id",
			"é",
			"名前",
			"🦀",
			"\0",
			" ",
			"field_",
			"\u{7F}",
			"\u{FFFF}",
			"\u{10FFFF}",
		];
		match self.below(16) {
			0 => String::new(),
			1 => "x".repeat(self.pick(&[254, 255, 256])),
			2 => "y".repeat(self.below(600) as usize),
			_ => (0..self.below(10)).map(|_| self.pick(&PARTS)).collect(),
		}
	}

	/// An object key: often `id` or a near neighbour of another key.
	pub fn field(&mut self) -> String {
		match self.below(8) {
			0 => self.string(),
			1 => "id".to_owned(),
			_ => (0..self.below(4))
				.map(|_| self.pick(&["a", "b", "id", "\0", "é", "🦀", "z"]))
				.collect(),
		}
	}

	/// A duration, often either side of the most nanoseconds a u64 counts.
	pub fn duration(&mut self) -> std::time::Duration {
		use std::time::Duration as D;
		match self.below(4) {
			0 => D::new(self.bits() >> self.below(64), self.below(1_000_000_000) as u32),
			1 => D::from_nanos(self.bits()),
			2 => self.pick(&[
				D::ZERO,
				D::from_nanos(1),
				D::from_nanos(u64::MAX),
				D::new(18_446_744_073, 709_551_616),
				D::new(u64::MAX, 0),
				D::MAX,
			]),
			_ => D::new(self.bits(), self.below(1_000_000_000) as u32),
		}
	}

	/// A datetime, often either side of an end of the nanosecond range of an
	/// i64, at the years 1500 and 9999, at either end of what a datetime holds,
	/// or in a leap second.
	pub fn datetime(&mut self) -> Datetime {
		const NS: i64 = 1_000_000_000;
		let (secs, nanos) = match self.below(6) {
			0 => ((self.bits() as i64) >> self.below(64).max(22), self.below(1_000_000_000) as u32),
			1 => {
				let (s, n) = (i64::MIN.div_euclid(NS), i64::MIN.rem_euclid(NS) as u32);
				self.pick(&[(s, n), (s, n - 1)])
			}
			2 => {
				let (s, n) = (i64::MAX.div_euclid(NS), i64::MAX.rem_euclid(NS) as u32);
				self.pick(&[(s, n), (s, n + 1)])
			}
			3 => self.pick(&[
				(-14_831_769_600, 0),
				(253_402_300_799, 999_999_999),
				(-1, 999_999_999),
			]),
			// A leap second's sub-second part is a second or more.
			4 => (
				(self.bits() as i64 >> 30) * 60 + 59,
				1_000_000_000 + self.below(1_000_000_000) as u32,
			),
			_ => return self.pick(&[Datetime::MIN_UTC, Datetime::MAX_UTC]),
		};
		Datetime::try_from((secs, nanos)).unwrap_or(Datetime::MIN_UTC)
	}

	fn uuid(&mut self) -> Uuid {
		match self.below(4) {
			0 => Uuid::nil(),
			1 => Uuid::max(),
			_ => {
				let mut b = [0u8; 16];
				b[..8].copy_from_slice(&self.bits().to_le_bytes());
				b[8..].copy_from_slice(&self.bits().to_le_bytes());
				Uuid::from_slice(&b).expect("16 bytes")
			}
		}
	}

	fn bytes(&mut self) -> Bytes {
		let n = match self.below(16) {
			0 => self.pick(&[254, 255, 256]),
			_ => self.below(40) as usize,
		};
		Bytes::from((0..n).map(|_| self.below(256) as u8).collect::<Vec<u8>>())
	}

	/// A file, its key sometimes without the leading `/` that `File::new`
	/// adds, as a file arriving from a client may be.
	fn file(&mut self) -> File {
		let (bucket, key) = (self.string(), self.string());
		match self.below(4) {
			3 => File {
				bucket,
				key,
			},
			_ => File::new(bucket, key),
		}
	}

	fn regex(&mut self) -> Regex {
		const PATTERNS: [&str; 7] =
			["a+", "^[a-z]+$", "(?i)x.y", "", "a\\/b", "名前|🦀", "\\d{2,4}"];
		let pattern = match self.below(4) {
			3 => self.string(),
			_ => self.pick(&PATTERNS).to_owned(),
		};
		pattern.parse::<Regex>().unwrap_or_else(|_| "a+".parse().expect("compiles"))
	}

	pub fn bound<T>(&mut self, mut f: impl FnMut(&mut Self) -> T) -> Bound<T> {
		match self.below(3) {
			0 => Bound::Unbounded,
			1 => Bound::Included(f(self)),
			_ => Bound::Excluded(f(self)),
		}
	}

	/// A record id key of every kind, ranges nesting a few levels.
	pub fn key(&mut self, depth: usize) -> RecordIdKey {
		let kinds = if depth > 3 {
			3
		} else {
			6
		};
		match self.below(kinds) {
			0 => RecordIdKey::Number(self.int()),
			1 => RecordIdKey::String(Strand::new(self.string())),
			2 => RecordIdKey::Uuid(self.uuid()),
			3 => RecordIdKey::Array(self.array(depth)),
			4 => RecordIdKey::Object(self.object(depth)),
			_ => RecordIdKey::Range(Box::new(RecordIdKeyRange {
				start: self.bound(|g| g.key(depth + 1)),
				end: self.bound(|g| g.key(depth + 1)),
			})),
		}
	}

	/// An element count: mostly a few, sometimes either side of the most a
	/// one-byte offset counts, rarely more than a two-byte offset counts.
	fn count(&mut self) -> usize {
		match self.below(64) {
			62 => self.pick(&[255, 256]),
			63 => 65_536,
			_ => self.below(6) as usize,
		}
	}

	pub fn array(&mut self, depth: usize) -> Array {
		match self.below(4) {
			// Every element a float: a typed float array.
			3 => {
				let n = self.count().max(1);
				Array((0..n).map(|_| Value::Number(Number::Float(self.float()))).collect())
			}
			_ => Array((0..self.count()).map(|_| self.value(depth + 1)).collect()),
		}
	}

	/// A set built as sets are built: from distinct elements, one element at a
	/// time, with some elements taken out again, or collected, as a set
	/// arriving from a client is. Its elements are sometimes numbers of every
	/// variant close to one another.
	pub fn set(&mut self, depth: usize) -> Set {
		let n = self.count();
		let numbers = self.below(3) == 2;
		let items: Vec<Value> = (0..n)
			.map(|_| {
				if numbers {
					self.number()
				} else {
					self.value(depth + 1)
				}
			})
			.collect();
		// Collecting sorts, and a sort of more elements than an insertion sort
		// takes may panic on the ordering of values, which is not total. A
		// larger set, and a `BTreeSet`, is built by insertion instead.
		if n <= 20 && self.below(4) == 3 {
			return items.into_iter().collect();
		}
		#[expect(clippy::mutable_key_type)]
		let mut distinct = BTreeSet::new();
		match self.below(3) {
			0 => {
				for v in items {
					distinct.insert(v);
				}
				from_btree(distinct)
			}
			1 => inserted(items),
			_ => {
				for v in items.iter().cloned() {
					distinct.insert(v);
				}
				let mut set = from_btree(distinct);
				for v in items.iter().step_by(2) {
					set.0.remove(v);
				}
				set
			}
		}
	}

	pub fn object(&mut self, depth: usize) -> Object {
		(0..self.count()).map(|_| (Strand::new(self.field()), self.value(depth + 1))).collect()
	}

	/// A geometry of every kind, collections nesting a few levels.
	pub fn geometry(&mut self, depth: usize) -> Geometry {
		let kinds = if depth > 2 {
			6
		} else {
			7
		};
		match self.below(kinds) {
			0 => Geometry::Point(self.coord().into()),
			1 => Geometry::Line(self.coords(6).into()),
			2 => self.polygon(),
			3 => Geometry::MultiPoint(self.coords(5).into()),
			4 => Geometry::MultiLine((0..self.below(4)).map(|_| self.coords(5)).collect()),
			5 => Geometry::MultiPolygon(
				(0..self.below(3))
					.map(|_| match self.polygon() {
						Geometry::Polygon(p) => p,
						_ => unreachable!("a polygon"),
					})
					.collect(),
			),
			_ => {
				Geometry::Collection((0..self.below(4)).map(|_| self.geometry(depth + 1)).collect())
			}
		}
	}

	fn coord(&mut self) -> (f64, f64) {
		(self.float(), self.float())
	}

	fn coords(&mut self, most: u64) -> Vec<(f64, f64)> {
		(0..self.below(most)).map(|_| self.coord()).collect()
	}

	/// A polygon of up to three rings, built from a GeoJSON coordinate array:
	/// the exterior ring first.
	fn polygon(&mut self) -> Geometry {
		let rings: Vec<Value> = (0..1 + self.below(3))
			.map(|_| {
				Value::Array(Array(
					self.coords(6)
						.into_iter()
						.map(|(x, y)| {
							Value::Array(Array(vec![
								Value::Number(Number::Float(x)),
								Value::Number(Number::Float(y)),
							]))
						})
						.collect(),
				))
			})
			.collect();
		Geometry::Polygon(
			Geometry::array_to_polygon(&Value::Array(Array(rings)))
				.expect("an array of rings is a polygon"),
		)
	}

	/// A container whose entries take a length either side of the most a one-
	/// or two-byte offset holds, one whose count is either side of the most a
	/// one-byte offset counts, or a range whose start bound's length takes a
	/// one- or a five-byte prefix.
	fn sized(&mut self) -> Value {
		let len = self.pick(&[254usize, 255, 256, 65_535, 65_536]);
		match self.below(4) {
			// One string element: its tag, then its bytes.
			0 => Value::Array(Array(vec![Value::String(Strand::new("x".repeat(len - 1)))])),
			// One field `k`: a one-byte length, the key, then a string value.
			1 => Value::Object(Object::from_iter([(
				Strand::new("k"),
				Value::String(Strand::new("x".repeat(len - 3))),
			)])),
			// 255 one-byte elements, or 254 and one of two bytes.
			2 => {
				let mut items = vec![Value::Number(Number::Int(1)); 254];
				items.push(Value::Number(Number::Int(self.pick(&[1, 127]))));
				Value::Array(Array(items))
			}
			_ => Value::Range(Box::new(Range {
				start: Bound::Included(Value::String(Strand::new("x".repeat(len - 1)))),
				end: Bound::Unbounded,
			})),
		}
	}

	/// A value nested to around one of the depth limits, each level whatever
	/// the bytes choose: an array, set or object holding the level below, a
	/// range bounded by it, a record id whose key holds it, or, at the bottom,
	/// a geometry collection.
	pub fn deep(&mut self) -> Value {
		let levels = match self.below(3) {
			0 => self.below(DEPTH_LIMIT as u64 + 4) as usize,
			1 => self.pick(&[
				MAX_VALUE_DEPTH - 2,
				MAX_VALUE_DEPTH - 1,
				MAX_VALUE_DEPTH,
				MAX_VALUE_DEPTH + 1,
			]),
			_ => self.pick(&[DEPTH_LIMIT - 1, DEPTH_LIMIT, DEPTH_LIMIT + 1, DEPTH_LIMIT + 2]),
		};
		// A collection nests only geometries, so collections take the bottom.
		let collections = match self.below(4) {
			3 => self.below(levels as u64 + 1) as usize,
			_ => 0,
		};
		let mut v = if collections == 0 {
			self.value(4)
		} else {
			let mut g = Geometry::Point((0.5, -0.5).into());
			for _ in 0..collections {
				g = Geometry::Collection(vec![g]);
			}
			Value::Geometry(g)
		};
		let table = || TableName::new("t");
		let mut depth = collections;
		while depth < levels {
			let left = levels - depth;
			let (v2, added) = match self.below(8) {
				1 => (Value::Set(Set::from(BTreeSet::from([v]))), 1),
				2 => (Value::Object(Object::from_iter([(Strand::new("k"), v)])), 1),
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
				5 if left >= 2 => (
					Value::RecordId(RecordId {
						table: table(),
						key: RecordIdKey::Array(Array(vec![v])),
					}),
					2,
				),
				6 if left >= 2 => (
					Value::RecordId(RecordId {
						table: table(),
						key: RecordIdKey::Object(Object::from_iter([(Strand::new("k"), v)])),
					}),
					2,
				),
				7 if left >= 3 => (
					Value::RecordId(RecordId {
						table: table(),
						key: RecordIdKey::Range(Box::new(RecordIdKeyRange {
							start: Bound::Included(RecordIdKey::Array(Array(vec![v]))),
							end: Bound::Unbounded,
						})),
					}),
					3,
				),
				_ => (Value::Array(Array(vec![v])), 1),
			};
			v = v2;
			depth += added;
		}
		v
	}
}

/// `distinct` as a set. `Set::from` takes a `BTreeSet`'s order as it is, which
/// is strictly ascending only where `Value`'s ordering is total over the
/// elements; elsewhere the set is built one element at a time.
#[expect(clippy::mutable_key_type)]
fn from_btree(distinct: BTreeSet<Value>) -> Set {
	if distinct.iter().zip(distinct.iter().skip(1)).all(|(a, b)| a < b) {
		Set::from(distinct)
	} else {
		inserted(distinct)
	}
}

/// A set built by inserting each of `items` in turn.
fn inserted(items: impl IntoIterator<Item = Value>) -> Set {
	let mut set = Set::default();
	for v in items {
		set.0.insert(v);
	}
	set
}

/// Saves `bytes` to the directory `FUZZ_STORED_SEEDS` names, when it names
/// one, so the encodings a target writes can seed the decoding target.
pub fn save_seed(bytes: &[u8]) {
	if let Some(dir) = std::env::var_os("FUZZ_STORED_SEEDS") {
		let mut h = DefaultHasher::new();
		bytes.hash(&mut h);
		let _ =
			std::fs::write(std::path::Path::new(&dir).join(format!("{:016x}", h.finish())), bytes);
	}
}

// ---------------------------------------------------------------------------
// Allocation

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

/// The system allocator, counting the bytes it has handed out and not taken
/// back, and the most of them held at once.
pub struct Counting;

impl Counting {
	fn grew(by: usize) {
		let live = LIVE.fetch_add(by, Ordering::Relaxed) + by;
		PEAK.fetch_max(live, Ordering::Relaxed);
	}
}

// SAFETY: every call is forwarded to `System` unchanged; the counters only
// observe the sizes.
unsafe impl GlobalAlloc for Counting {
	unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
		// SAFETY: the caller upholds `alloc`'s contract, which `System` shares.
		let p = unsafe { System.alloc(layout) };
		if !p.is_null() {
			Self::grew(layout.size());
		}
		p
	}

	unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
		// SAFETY: as for `alloc`.
		let p = unsafe { System.alloc_zeroed(layout) };
		if !p.is_null() {
			Self::grew(layout.size());
		}
		p
	}

	unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
		// SAFETY: `p` came from this allocator, so from `System`, with `layout`.
		unsafe { System.dealloc(p, layout) };
		LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
	}

	unsafe fn realloc(&self, p: *mut u8, layout: Layout, size: usize) -> *mut u8 {
		// SAFETY: `p` came from this allocator, so from `System`, with `layout`.
		let q = unsafe { System.realloc(p, layout, size) };
		if !q.is_null() {
			if size >= layout.size() {
				Self::grew(size - layout.size());
			} else {
				LIVE.fetch_sub(layout.size() - size, Ordering::Relaxed);
			}
		}
		q
	}
}

/// Runs `f`, answering its result and the most bytes allocated at once while
/// it ran, beyond what was allocated before it, what it returns included.
/// Meaningful only in a binary whose global allocator is [`Counting`].
pub fn peak_allocation<T>(f: impl FnOnce() -> T) -> (T, usize) {
	let base = LIVE.load(Ordering::Relaxed);
	PEAK.store(base, Ordering::Relaxed);
	let r = f();
	(r, PEAK.load(Ordering::Relaxed).saturating_sub(base))
}

/// Whether `v` holds a regex anywhere. Compiling a pattern allocates out of
/// proportion to its length, within the regex engine's own size limit.
pub fn holds_regex(v: &Value) -> bool {
	match v {
		Value::Regex(_) => true,
		Value::Array(a) => a.0.iter().any(holds_regex),
		Value::Set(s) => s.0.iter().any(holds_regex),
		Value::Object(o) => o.0.values().any(holds_regex),
		Value::Range(r) => [&r.start, &r.end].into_iter().any(|b| match b {
			Bound::Included(v) | Bound::Excluded(v) => holds_regex(v),
			Bound::Unbounded => false,
		}),
		Value::RecordId(rid) => holds_regex(&rid.key.clone().into_value()),
		_ => false,
	}
}
