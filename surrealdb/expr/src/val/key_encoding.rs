//! Byte-level building blocks for key ranges over array record ids.
//!
//! A record is stored under the storekey encoding of its [`RecordIdKey`], in the
//! default format, which keeps a number's kind. The key of an array id is the
//! array discriminant, the encoding of each element in turn, and a closing
//! terminator byte. Every element is led by the discriminant of its [`Value`]
//! variant, and every kind a record id component can hold is self-delimiting,
//! so the encoding of a run of leading elements is a byte prefix of exactly the
//! keys whose leading elements encode the same.
//!
//! The helpers here produce those prefixes. Every byte they emit comes from the
//! same `Encode` implementations that write a stored key; no discriminant or
//! terminator is written by hand.
//!
//! # Order
//!
//! Byte order of an encoded element equals [`Value`] order for strings, uuids,
//! durations, booleans and integers ([`Number::Int`]), so a bound on such a
//! component is exact. An integer is an order-preserving decimal buffer followed
//! by the tag of its number kind; every integer carries the same tag, so the
//! buffer alone orders them. Two kinds need care:
//!
//! - A datetime is encoded as its RFC 3339 text, whose sub-second digits and zone letter do not
//!   sort chronologically (`.500Z` sorts before `Z`). Only the fixed-width `YYYY-MM-DDTHH:MM:SS`
//!   prefix orders chronologically, so a bound on a datetime component is widened to the whole
//!   second ([`encode_datetime_second_prefix`]).
//! - A float or a decimal does not share the encoding of the integer it equals: `5`, `5.0` and
//!   `5dec` have the same decimal buffer and different kind tags, and `5.00dec` has a longer
//!   buffer. A key orders by (value, kind tag, next element), not by value alone, so an integer
//!   component holds only [`Number::Int`]. A bound built from an `Int` reaches no stored float or
//!   decimal, and one stored there would sort out of order against the elements that follow it.

use std::sync::LazyLock;

use storekey::{Encode, Writer};

use crate::val::datetime::KEY_SECOND_PREFIX_LEN;
use crate::val::{Array, Datetime, RecordIdKey, Value};

/// The number of bytes the derive for [`Value`] writes ahead of a variant's
/// payload.
const VALUE_DISCRIMINANT_LEN: usize = 1;

/// The bytes that open the key of every array record id: the discriminant of
/// the array variant of [`RecordIdKey`].
static ARRAY_KEY_HEAD: LazyLock<Vec<u8>> = LazyLock::new(|| {
	let mut head = storekey::encode_vec(&RecordIdKey::Array(Array::default()))
		.expect("the empty array record id key encodes");
	// An array key closes with a terminator byte, which a prefix leaves off.
	head.pop();
	head
});

/// Appends the encoding of `v` as an element of an array key.
///
/// Returns `false`, leaving `out` as it was, when `v` has no key encoding.
fn write_element(out: &mut Vec<u8>, v: &Value) -> bool {
	let start = out.len();
	let mut w = Writer::new(&mut *out);
	// An array writes this marker before every element, so an element is
	// escaped the same way here as in a full key.
	w.mark_terminator();
	if Encode::<()>::encode(v, &mut w).is_ok() {
		return true;
	}
	out.truncate(start);
	false
}

/// The encoding an array key holds for the element `v`, or `None` when `v` is a
/// value no key can hold (a closure or a regex).
///
/// There is no infallible form: an empty vector is not a usable bound, so a
/// caller has to decide what a value with no encoding means to it.
pub fn try_encode_component(v: &Value) -> Option<Vec<u8>> {
	let mut out = Vec::new();
	write_element(&mut out, v).then_some(out)
}

/// The encoding of the array key `[elems...]` without its closing terminator,
/// or `None` when an element is a value no key can hold.
///
/// The result is a byte prefix of the key of every array id that begins with
/// elements encoding the same as `elems`, and of no other key. For an integer
/// element it is the prefix of the keys that hold that [`Number::Int`] there, and
/// of none that hold the same value as a float or a decimal.
pub fn try_array_key_prefix(elems: &[Value]) -> Option<Vec<u8>> {
	let mut out = ARRAY_KEY_HEAD.clone();
	for elem in elems {
		if !write_element(&mut out, elem) {
			return None;
		}
	}
	Some(out)
}

/// The leading bytes of the encoding of every datetime that falls within the
/// same whole second as `d`: the discriminant of [`Value::Datetime`] followed by
/// the `YYYY-MM-DDTHH:MM:SS` text of [`Datetime::key_second_prefix`].
///
/// Returns `None` unless [`Datetime::in_key_range`].
pub fn encode_datetime_second_prefix(d: &Datetime) -> Option<Vec<u8>> {
	if !d.in_key_range() {
		return None;
	}
	let mut bytes = try_encode_component(&Value::Datetime(*d))?;
	bytes.truncate(VALUE_DISCRIMINANT_LEN + KEY_SECOND_PREFIX_LEN);
	Some(bytes)
}

#[cfg(test)]
mod key_order {
	//! Property tests for the key encodings the record id key ranges rely on.
	//!
	//! Every test draws from a fixed-seed generator, so a failure reproduces.

	use std::cmp::Ordering;

	use chrono::{TimeZone, Timelike, Utc};
	use rust_decimal::Decimal;
	use surrealdb_kvs::Key;

	use super::*;
	use crate::val::number::NumberKind;
	use crate::val::{
		Bytes, Duration, File, Geometry, Number, Object, Range, RecordId, Regex, Set, Strand,
		TableName, Uuid,
	};

	/// Random cases per kind.
	const CASES: usize = 20_000;

	// ------------------------------
	// Generator
	// ------------------------------

	/// A xorshift64* generator.
	struct Rng(u64);

	impl Rng {
		fn new(seed: u64) -> Self {
			Self(seed | 1)
		}

		fn next(&mut self) -> u64 {
			let mut x = self.0;
			x ^= x >> 12;
			x ^= x << 25;
			x ^= x >> 27;
			self.0 = x;
			x.wrapping_mul(0x2545_F491_4F6C_DD1D)
		}

		fn below(&mut self, n: u64) -> u64 {
			self.next() % n
		}

		fn index(&mut self, len: usize) -> usize {
			(self.next() % len as u64) as usize
		}

		fn one_in(&mut self, n: u64) -> bool {
			self.below(n) == 0
		}

		fn between(&mut self, lo: i64, hi: i64) -> i64 {
			lo + (self.next() % ((hi - lo) as u64 + 1)) as i64
		}

		fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
			&items[self.index(items.len())]
		}
	}

	// ------------------------------
	// Encoding helpers
	// ------------------------------

	/// The element encoding of a value that has one.
	fn encode_component(v: &Value) -> Vec<u8> {
		try_encode_component(v).unwrap_or_else(|| panic!("{v:?} has no key encoding"))
	}

	/// The array key prefix of elements that all have an encoding.
	fn array_key_prefix(elems: &[Value]) -> Vec<u8> {
		try_array_key_prefix(elems).unwrap_or_else(|| panic!("{elems:?} have no key encoding"))
	}

	// ------------------------------
	// Value constructors
	// ------------------------------

	fn string(s: &str) -> Value {
		Value::String(Strand::new(s))
	}

	fn int(v: i64) -> Value {
		Value::Number(Number::Int(v))
	}

	fn uuid(bytes: [u8; 16]) -> Value {
		Value::Uuid(Uuid(uuid::Uuid::from_bytes(bytes)))
	}

	fn duration(secs: u64, nanos: u32) -> Value {
		Value::Duration(Duration(std::time::Duration::new(secs, nanos)))
	}

	fn datetime(secs: i64, nanos: u32) -> Datetime {
		Datetime(Utc.timestamp_opt(secs, nanos).single().expect("a valid timestamp"))
	}

	fn decimal(mantissa: i128, scale: u32) -> Number {
		Number::Decimal(
			Decimal::try_from_i128_with_scale(mantissa, scale).expect("a representable decimal"),
		)
	}

	fn record(table: &str, key: RecordIdKey) -> Value {
		Value::RecordId(RecordId {
			table: TableName::from(table),
			key,
		})
	}

	/// The key a record with this array id is stored under, after the table
	/// prefix.
	fn key_of(elems: &[Value]) -> Vec<u8> {
		storekey::encode_vec(&RecordIdKey::Array(Array(elems.to_vec()))).unwrap()
	}

	/// The smallest key above every key that begins with `bytes`.
	fn successor(bytes: &[u8]) -> Option<Vec<u8>> {
		Key::from(bytes.to_vec()).next_neighbour().map(Key::into_vec)
	}

	fn concat(a: &[u8], b: &[u8]) -> Vec<u8> {
		[a, b].concat()
	}

	// ------------------------------
	// Generators for the exactly ordered kinds
	// ------------------------------

	const LONG_PREFIX: &str = "a-prefix-longer-than-the-inline-capacity-of-a-strand/";

	const TEXT_PIECES: [&str; 12] =
		["\0", "\u{1}", "\u{2}", "a", "b", "z", "~", "\u{7f}", "é", "€", "𝄞", "\u{10ffff}"];

	fn gen_text(rng: &mut Rng) -> String {
		let mut s = String::new();
		if rng.one_in(16) {
			s.push_str(LONG_PREFIX);
		}
		for _ in 0..rng.below(5) {
			let piece = *rng.pick(&TEXT_PIECES);
			s.push_str(piece);
		}
		s
	}

	fn gen_string(rng: &mut Rng) -> Value {
		string(&gen_text(rng))
	}

	fn edge_strings() -> Vec<Value> {
		let mut values: Vec<Value> = [
			"",
			"\0",
			"\u{1}",
			"\u{2}",
			"\0\0",
			"\0a",
			"\u{1}\u{1}",
			"a",
			"a\0",
			"a\u{1}",
			"ab",
			"b",
			"e",
			"é",
			"f",
			"€",
			"𝄞",
			"\u{10ffff}",
		]
		.iter()
		.map(|s| string(s))
		.collect();
		for suffix in ["", "a", "\0", "\u{1}", "b"] {
			values.push(string(&format!("{LONG_PREFIX}{suffix}")));
		}
		values
	}

	fn gen_uuid(rng: &mut Rng) -> Value {
		let mut bytes = [0u8; 16];
		for byte in &mut bytes {
			*byte = match rng.below(4) {
				0 => 0,
				1 => 1,
				2 => 0xff,
				_ => rng.next() as u8,
			};
		}
		uuid(bytes)
	}

	fn edge_uuids() -> Vec<Value> {
		let mut one_last = [0u8; 16];
		one_last[15] = 1;
		let mut one_first = [0u8; 16];
		one_first[0] = 1;
		let mut max_but_last = [0xffu8; 16];
		max_but_last[15] = 0xfe;
		vec![
			Value::Uuid(Uuid::nil()),
			Value::Uuid(Uuid::max()),
			uuid(one_last),
			uuid(one_first),
			uuid(max_but_last),
		]
	}

	fn gen_duration(rng: &mut Rng) -> Value {
		let secs = match rng.below(6) {
			0 => 0,
			1 => 1,
			2 => u64::MAX,
			3 => rng.below(300),
			4 => rng.below(1 << 40),
			_ => rng.next(),
		};
		let nanos = match rng.below(5) {
			0 => 0,
			1 => 1,
			2 => 999_999_999,
			3 => 500_000_000,
			_ => rng.below(1_000_000_000) as u32,
		};
		duration(secs, nanos)
	}

	fn edge_durations() -> Vec<Value> {
		vec![
			duration(0, 0),
			duration(0, 1),
			duration(0, 999_999_999),
			duration(1, 0),
			duration(255, 0),
			duration(256, 0),
			duration(65_535, 0),
			duration(65_536, 0),
			duration(u64::MAX, 0),
			Value::Duration(Duration::MAX),
		]
	}

	/// The integers an int component can hold at its extremes, and around the
	/// points where the width of a decimal buffer changes.
	fn edge_ints() -> Vec<Value> {
		[
			i64::MIN,
			i64::MIN + 1,
			-(1 << 53) - 1,
			-(1 << 53),
			-1000,
			-10,
			-9,
			-1,
			0,
			1,
			5,
			9,
			10,
			11,
			99,
			100,
			255,
			256,
			1000,
			1 << 53,
			(1 << 53) + 1,
			i64::MAX - 1,
			i64::MAX,
		]
		.into_iter()
		.map(int)
		.collect()
	}

	fn gen_bool(rng: &mut Rng) -> Value {
		Value::Bool(rng.one_in(2))
	}

	fn gen_record(rng: &mut Rng) -> Value {
		let table = *rng.pick(&["a", "b", "ab", "t"]);
		let key = match rng.below(3) {
			0 => RecordIdKey::String(Strand::new(gen_text(rng))),
			1 => RecordIdKey::Number(gen_i64(rng)),
			_ => {
				let other = gen_uuid(rng);
				let Value::Uuid(u) = other else {
					unreachable!("gen_uuid produced {other:?}")
				};
				RecordIdKey::Uuid(u)
			}
		};
		record(table, key)
	}

	/// The generators of the exactly ordered kinds, indexed as `EXACT_KINDS`.
	fn gen_exact(rng: &mut Rng, kind: usize) -> Value {
		match kind {
			0 => gen_string(rng),
			1 => gen_uuid(rng),
			2 => gen_duration(rng),
			3 => gen_bool(rng),
			_ => int(gen_i64(rng)),
		}
	}

	const EXACT_KINDS: [&str; 5] = ["string", "uuid", "duration", "bool", "int"];

	/// An element for the equality prefix of a range, of any kind a component
	/// can hold. A number is an `Int`: a component holds no other kind.
	fn gen_eq_element(rng: &mut Rng) -> Value {
		match rng.below(7) {
			0 => gen_string(rng),
			1 => gen_uuid(rng),
			2 => gen_duration(rng),
			3 => gen_bool(rng),
			4 => Value::Datetime(gen_datetime(rng)),
			5 => int(gen_i64(rng)),
			_ => gen_record(rng),
		}
	}

	/// An element of any kind a component can hold.
	fn gen_element(rng: &mut Rng) -> Value {
		if rng.one_in(8) {
			Value::Number(gen_number(rng))
		} else {
			gen_eq_element(rng)
		}
	}

	// ------------------------------
	// Datetimes
	// ------------------------------

	/// 0000-01-01T00:00:00Z and 9999-12-31T23:59:59Z, as seconds since the epoch.
	const MIN_SECS: i64 = -62_167_219_200;
	const MAX_SECS: i64 = 253_402_300_799;
	const BASE_SECS: i64 = 1_700_000_000;

	const NANOS: [u32; 9] = [
		0,
		1,
		500_000_000,
		500_100_000,
		100_000_000,
		123_456_000,
		999_000_000,
		999_999_999,
		10_000_000,
	];

	fn gen_nanos(rng: &mut Rng) -> u32 {
		if rng.one_in(3) {
			rng.below(1_000_000_000) as u32
		} else {
			*rng.pick(&NANOS)
		}
	}

	/// A datetime within the key range, often within a few seconds of the others.
	fn gen_datetime(rng: &mut Rng) -> Datetime {
		let secs = match rng.below(5) {
			0 => rng.between(MIN_SECS, MAX_SECS),
			1 | 2 => BASE_SECS + rng.between(-2, 2),
			3 => BASE_SECS + rng.between(-100_000, 100_000),
			_ => *rng.pick(&[MIN_SECS, MAX_SECS, 0, 59, 60, 86_399, 86_400]),
		};
		datetime(secs, gen_nanos(rng))
	}

	// ------------------------------
	// Numbers
	// ------------------------------

	fn gen_i64(rng: &mut Rng) -> i64 {
		const EDGES: [i64; 16] = [
			i64::MIN,
			i64::MIN + 1,
			-(1 << 53) - 1,
			-(1 << 53),
			-1000,
			-10,
			-1,
			0,
			1,
			10,
			1000,
			1 << 53,
			(1 << 53) + 1,
			1_000_000_000_000_000_000,
			i64::MAX - 1,
			i64::MAX,
		];
		match rng.below(4) {
			0 => *rng.pick(&EDGES),
			1 => rng.between(-20, 20),
			2 => rng.between(-1_000_000, 1_000_000),
			_ => rng.next() as i64,
		}
	}

	/// A whole number as an int, a float, a decimal, or a decimal written with
	/// trailing zeros.
	fn gen_integral(rng: &mut Rng) -> Number {
		let v = gen_i64(rng);
		match rng.below(5) {
			0 | 1 => Number::Int(v),
			2 => Number::Float(v as f64),
			3 => decimal(v as i128, 0),
			_ => {
				let scale = 1 + rng.below(9) as u32;
				decimal(v as i128 * 10i128.pow(scale), scale)
			}
		}
	}

	/// A dyadic rational, which a float and a decimal both hold exactly.
	fn gen_dyadic(rng: &mut Rng) -> Number {
		let k = rng.between(-(1 << 40), 1 << 40);
		let n = rng.below(21) as u32;
		let exact_decimal = decimal(k as i128 * 5i128.pow(n), n);
		match rng.below(3) {
			0 => Number::Float(k as f64 / (1u64 << n) as f64),
			1 => {
				if let Number::Decimal(d) = exact_decimal {
					Number::Decimal(d.normalize())
				} else {
					exact_decimal
				}
			}
			_ => exact_decimal,
		}
	}

	fn gen_wild_float(rng: &mut Rng) -> Number {
		let f = match rng.below(5) {
			0 => f64::from_bits(rng.next()),
			1 => f64::from_bits(rng.next() & ((1u64 << 52) - 1)),
			2 => -f64::from_bits(rng.next() & ((1u64 << 52) - 1)),
			3 => rng.between(-1_000_000_000, 1_000_000_000) as f64 / 1000.0,
			_ => rng.between(-1_000_000_000, 1_000_000_000) as f64 * 1e-7,
		};
		Number::Float(if f.is_finite() {
			f
		} else {
			1.5
		})
	}

	/// A number within a hair of a whole number.
	fn gen_near_integral(rng: &mut Rng) -> Number {
		let v = rng.between(-50, 50);
		let delta = rng.between(-1, 1) as i128;
		match rng.below(3) {
			0 => decimal(v as i128 * 10i128.pow(18) + delta, 18),
			1 => decimal(v as i128 * 10i128.pow(27) + delta, 27),
			_ => {
				let f = v as f64;
				let near = match delta {
					-1 => f64::from_bits(f.to_bits().wrapping_sub(1)),
					1 => f64::from_bits(f.to_bits().wrapping_add(1)),
					_ => f,
				};
				Number::Float(if near.is_finite() {
					near
				} else {
					f
				})
			}
		}
	}

	fn gen_decimal(rng: &mut Rng) -> Number {
		let mantissa = rng.next() as i128 * (rng.below(1 << 20) as i128 + 1);
		let mantissa = if rng.one_in(2) {
			-mantissa
		} else {
			mantissa
		};
		decimal(mantissa, rng.below(29) as u32)
	}

	/// Any finite number.
	fn gen_number(rng: &mut Rng) -> Number {
		match rng.below(10) {
			0..=2 => gen_integral(rng),
			3 | 4 => gen_dyadic(rng),
			5 => gen_wild_float(rng),
			6 => gen_near_integral(rng),
			7 => Number::Int(gen_i64(rng)),
			8 => Number::Float(rng.between(-1_000_000_000, 1_000_000_000) as f64 / 1000.0),
			_ => gen_decimal(rng),
		}
	}

	fn edge_numbers() -> Vec<Number> {
		vec![
			Number::Int(i64::MIN),
			Number::Int(-1),
			Number::Int(0),
			Number::Int(1),
			Number::Int(5),
			Number::Int(1000),
			Number::Int(i64::MAX),
			Number::Float(5.0),
			Number::Float(0.0),
			Number::Float(-0.0),
			Number::Float(-1.0),
			Number::Float(1000.0),
			Number::Float(1e15),
			Number::Float(1e20),
			Number::Float(i64::MIN as f64),
			Number::Float(i64::MAX as f64),
			Number::Float(0.1),
			Number::Float(-0.1),
			Number::Float(2.5),
			Number::Float(4.999_999_999_999_999),
			Number::Float(5.000_000_000_000_001),
			Number::Float(1e300),
			Number::Float(-1e300),
			Number::Float(f64::MAX),
			Number::Float(f64::MIN),
			Number::Float(f64::MIN_POSITIVE),
			Number::Float(5e-324),
			Number::Float(-5e-324),
			decimal(5, 0),
			decimal(500, 2),
			decimal(5000, 3),
			decimal(-5, 0),
			decimal(-500, 2),
			decimal(0, 0),
			decimal(0, 5),
			decimal(1, 1),
			decimal(10, 2),
			decimal(25, 1),
			decimal(250, 2),
			decimal(100, 0),
			decimal(1000, 0),
			decimal(10_000_000_000_000_000_000, 0),
			decimal(5_000_000_000_000_000_001, 18),
			decimal(4_999_999_999_999_999_999, 18),
			Number::Decimal(Decimal::MAX),
			Number::Decimal(Decimal::MIN),
		]
	}

	// ------------------------------
	// Exact reference for numbers
	// ------------------------------

	/// A little-endian natural number in base 10^9.
	struct Big(Vec<u32>);

	const LIMB: u64 = 1_000_000_000;

	impl Big {
		fn from_u64(mut v: u64) -> Self {
			let mut limbs = Vec::new();
			while v > 0 {
				limbs.push((v % LIMB) as u32);
				v /= LIMB;
			}
			Self(limbs)
		}

		fn mul_small(&mut self, factor: u32) {
			let mut carry = 0u64;
			for limb in &mut self.0 {
				let cur = u64::from(*limb) * u64::from(factor) + carry;
				*limb = (cur % LIMB) as u32;
				carry = cur / LIMB;
			}
			while carry > 0 {
				self.0.push((carry % LIMB) as u32);
				carry /= LIMB;
			}
		}

		/// Multiplies by `base^exp`, in steps of `base^chunk`, which must fit a `u32`.
		fn mul_pow(&mut self, base: u32, mut exp: u32, chunk: u32) {
			let step = u64::from(base).pow(chunk) as u32;
			while exp >= chunk {
				self.mul_small(step);
				exp -= chunk;
			}
			self.mul_small(u64::from(base).pow(exp) as u32);
		}

		fn digits(&self) -> String {
			let Some(top) = self.0.last() else {
				return "0".to_owned();
			};
			let mut s = top.to_string();
			for limb in self.0.iter().rev().skip(1) {
				s.push_str(&format!("{limb:09}"));
			}
			s
		}
	}

	/// A finite number written out in full: `±0.<digits> × 10^point`, with no
	/// leading or trailing zero digits.
	#[derive(Clone, Debug, PartialEq, Eq)]
	struct Exact {
		negative: bool,
		digits: String,
		point: i64,
	}

	impl Exact {
		fn new(negative: bool, digits: &str, point: i64) -> Self {
			let trimmed = digits.trim_start_matches('0');
			let point = point - (digits.len() - trimmed.len()) as i64;
			let digits = trimmed.trim_end_matches('0').to_owned();
			if digits.is_empty() {
				return Self {
					negative: false,
					digits,
					point: 0,
				};
			}
			Self {
				negative,
				digits,
				point,
			}
		}

		fn magnitude_cmp(&self, other: &Self) -> Ordering {
			match (self.digits.is_empty(), other.digits.is_empty()) {
				(true, true) => Ordering::Equal,
				(true, false) => Ordering::Less,
				(false, true) => Ordering::Greater,
				(false, false) => {
					self.point.cmp(&other.point).then_with(|| self.digits.cmp(&other.digits))
				}
			}
		}
	}

	impl Ord for Exact {
		fn cmp(&self, other: &Self) -> Ordering {
			match (self.negative, other.negative) {
				(false, true) => Ordering::Greater,
				(true, false) => Ordering::Less,
				(false, false) => self.magnitude_cmp(other),
				(true, true) => other.magnitude_cmp(self),
			}
		}
	}

	impl PartialOrd for Exact {
		fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
			Some(self.cmp(other))
		}
	}

	/// The exact value of a finite number, or `None` for NaN and the infinities.
	fn exact_of(n: &Number) -> Option<Exact> {
		match *n {
			Number::Int(v) => {
				let digits = v.unsigned_abs().to_string();
				let point = digits.len() as i64;
				Some(Exact::new(v < 0, &digits, point))
			}
			Number::I8(v) => exact_of(&Number::Int(v as i64)),
			Number::U8(v) => exact_of(&Number::Int(v as i64)),
			Number::I16(v) => exact_of(&Number::Int(v as i64)),
			Number::U16(v) => exact_of(&Number::Int(v as i64)),
			Number::I32(v) => exact_of(&Number::Int(v as i64)),
			Number::U32(v) => exact_of(&Number::Int(v as i64)),
			Number::U64(v) => {
				let digits = v.to_string();
				let point = digits.len() as i64;
				Some(Exact::new(false, &digits, point))
			}
			Number::BigDecimal(_) => None,
			Number::Decimal(d) => {
				let mantissa = d.mantissa();
				let digits = mantissa.unsigned_abs().to_string();
				let point = digits.len() as i64 - i64::from(d.scale());
				Some(Exact::new(mantissa < 0, &digits, point))
			}
			Number::F16(v) => exact_of(&Number::Float(v.to_f64())),
			Number::F32(v) => exact_of(&Number::Float(v as f64)),
			Number::Float(f) => {
				if !f.is_finite() {
					return None;
				}
				let bits = f.to_bits();
				let negative = bits >> 63 == 1;
				let exponent = ((bits >> 52) & 0x7ff) as i32;
				let fraction = bits & ((1u64 << 52) - 1);
				let (mantissa, exponent) = if exponent == 0 {
					(fraction, -1074)
				} else {
					(fraction | (1u64 << 52), exponent - 1075)
				};
				let mut big = Big::from_u64(mantissa);
				if exponent >= 0 {
					big.mul_pow(2, exponent as u32, 31);
					let digits = big.digits();
					let point = digits.len() as i64;
					Some(Exact::new(negative, &digits, point))
				} else {
					// m / 2^k = m * 5^k / 10^k
					let k = (-exponent) as u32;
					big.mul_pow(5, k, 13);
					let digits = big.digits();
					let point = digits.len() as i64 - i64::from(k);
					Some(Exact::new(negative, &digits, point))
				}
			}
		}
	}

	#[test]
	fn exact_reference_agrees_with_known_values() {
		let ord = |a: Number, b: Number| exact_of(&a).unwrap().cmp(&exact_of(&b).unwrap());
		assert_eq!(ord(Number::Int(5), Number::Float(5.0)), Ordering::Equal);
		assert_eq!(ord(Number::Int(5), decimal(500, 2)), Ordering::Equal);
		assert_eq!(ord(Number::Float(0.0), Number::Float(-0.0)), Ordering::Equal);
		assert_eq!(ord(Number::Int(0), decimal(0, 5)), Ordering::Equal);
		assert_eq!(ord(Number::Float(0.5), decimal(5, 1)), Ordering::Equal);
		assert_eq!(ord(Number::Float(0.1), decimal(1, 1)), Ordering::Greater);
		assert_eq!(ord(Number::Float(-0.1), decimal(-1, 1)), Ordering::Less);
		assert_eq!(ord(Number::Int(9), Number::Int(10)), Ordering::Less);
		assert_eq!(ord(Number::Int(-9), Number::Int(-10)), Ordering::Greater);
		assert_eq!(ord(Number::Int(i64::MAX), Number::Float(i64::MAX as f64)), Ordering::Less);
		assert_eq!(ord(Number::Float(5e-324), Number::Float(0.0)), Ordering::Greater);
		assert_eq!(ord(decimal(5_000_000_000_000_000_001, 18), Number::Int(5)), Ordering::Greater);
		assert!(exact_of(&Number::Float(f64::NAN)).is_none());
	}

	// ------------------------------
	// (a) exactly ordered kinds
	// ------------------------------

	fn assert_bytes_follow_value(name: &str, a: &Value, b: &Value) {
		let (ea, eb) = (encode_component(a), encode_component(b));
		assert_eq!(
			ea.cmp(&eb),
			a.cmp(b),
			"{name}: the byte order of {a:?} and {b:?} differs from their order as values"
		);
		assert_eq!(
			ea == eb,
			a == b,
			"{name}: {a:?} and {b:?} are equal as bytes exactly when equal as values"
		);
		assert!(
			ea == eb || (!ea.starts_with(&eb) && !eb.starts_with(&ea)),
			"{name}: the encoding of {a:?} or {b:?} is a proper prefix of the other"
		);
	}

	#[test]
	fn exact_kinds_order_matches_value_order() {
		let mut rng = Rng::new(0x5EED_0001);
		let edges = [
			edge_strings(),
			edge_uuids(),
			edge_durations(),
			vec![Value::Bool(false), Value::Bool(true)],
			edge_ints(),
		];
		for (kind, name) in EXACT_KINDS.iter().enumerate() {
			for a in &edges[kind] {
				for b in &edges[kind] {
					assert_bytes_follow_value(name, a, b);
				}
			}
			for _ in 0..CASES {
				let a = gen_exact(&mut rng, kind);
				let b = if rng.one_in(8) {
					a.clone()
				} else {
					gen_exact(&mut rng, kind)
				};
				assert_bytes_follow_value(name, &a, &b);
				// An edge value against a random one, for the extremes.
				let edge = rng.pick(&edges[kind]);
				assert_bytes_follow_value(name, edge, &a);
			}
		}
	}

	/// Lexicographic order over elements, as the order of two array ids.
	fn tuple_cmp(a: &[Value], b: &[Value]) -> Ordering {
		a.iter().zip(b).map(|(x, y)| x.cmp(y)).find(|o| o.is_ne()).unwrap_or(Ordering::Equal)
	}

	#[test]
	fn array_keys_of_exact_kinds_order_lexicographically() {
		let mut rng = Rng::new(0x5EED_0002);
		for _ in 0..CASES {
			let arity = 1 + rng.index(3);
			// Per-position pools, so that leading elements often tie.
			let kinds: Vec<usize> = (0..arity).map(|_| rng.index(EXACT_KINDS.len())).collect();
			let pools: Vec<Vec<Value>> = kinds
				.iter()
				.map(|kind| (0..3).map(|_| gen_exact(&mut rng, *kind)).collect())
				.collect();
			let draw = |rng: &mut Rng| -> Vec<Value> {
				pools.iter().map(|pool| rng.pick(pool).clone()).collect()
			};
			let (a, b) = (draw(&mut rng), draw(&mut rng));
			assert_eq!(
				key_of(&a).cmp(&key_of(&b)),
				tuple_cmp(&a, &b),
				"the keys of {a:?} and {b:?} do not order as the arrays do"
			);
		}
	}

	#[test]
	fn record_components_encode_injectively() {
		let mut rng = Rng::new(0x5EED_0003);
		let mut values = vec![
			record("a", RecordIdKey::Number(1)),
			record("a", RecordIdKey::Number(-1)),
			record("a", RecordIdKey::String(Strand::new("1"))),
			record("ab", RecordIdKey::Number(1)),
			record("a", RecordIdKey::Uuid(Uuid::nil())),
		];
		for _ in 0..2000 {
			values.push(gen_record(&mut rng));
		}
		let encoded: Vec<Vec<u8>> = values.iter().map(encode_component).collect();
		for (a, ea) in values.iter().zip(&encoded) {
			for (b, eb) in values.iter().zip(&encoded) {
				assert_eq!(ea == eb, a == b, "{a:?} and {b:?}");
				assert!(
					ea == eb || (!ea.starts_with(eb) && !eb.starts_with(ea)),
					"the encoding of {a:?} or {b:?} is a proper prefix of the other"
				);
			}
		}
	}

	// ------------------------------
	// (b) numbers of different kinds
	// ------------------------------
	//
	// An int component holds only `Number::Int`. The facts below, about floats and
	// decimals that equal an integer, are why. Each is pinned, so that a change to
	// the number encoding that made the rule unnecessary or insufficient fails here.

	/// The number of bytes a number's kind adds after its decimal buffer.
	const NUMBER_KIND_TAG_LEN: usize = 1;

	/// The element encoding of a number without its kind tag, as it is stored.
	fn stored_prefix(n: &Number) -> Vec<u8> {
		let mut bytes = encode_component(&Value::Number(n.clone()));
		bytes.truncate(bytes.len() - NUMBER_KIND_TAG_LEN);
		bytes
	}

	/// The stored prefix of the number in its normalised form, which every number
	/// equal to it shares.
	fn value_prefix(n: &Number) -> Vec<u8> {
		if let Number::Decimal(d) = n {
			stored_prefix(&Number::Decimal(d.normalize()))
		} else {
			stored_prefix(n)
		}
	}

	/// The encoded number kind tags, in the order int, float, decimal.
	fn kind_tags() -> [Vec<u8>; 3] {
		[NumberKind::Int, NumberKind::Float, NumberKind::Decimal]
			.map(|kind| storekey::encode_vec(&kind).expect("a number kind encodes"))
	}

	struct NumberCase {
		n: Number,
		exact: Exact,
		/// `value_prefix`.
		prefix: Vec<u8>,
		/// `encode_component`.
		element: Vec<u8>,
		/// `stored_prefix`: the prefix of the number as stored, without normalising it.
		stored_prefix: Vec<u8>,
	}

	fn number_corpus(rng: &mut Rng, random: usize) -> Vec<NumberCase> {
		let mut numbers = edge_numbers();
		for _ in 0..random {
			numbers.push(gen_number(rng));
		}
		numbers
			.into_iter()
			.map(|n| NumberCase {
				exact: exact_of(&n).expect("a finite number"),
				prefix: value_prefix(&n),
				element: encode_component(&Value::Number(n.clone())),
				stored_prefix: stored_prefix(&n),
				n,
			})
			.collect()
	}

	#[test]
	fn numbers_order_by_value_prefix_across_kinds() {
		let mut rng = Rng::new(0x5EED_0004);
		let corpus = number_corpus(&mut rng, 1400);
		let tags = kind_tags();

		// The value prefix orders as the exact value does, and equal values share it.
		let mut misses = Vec::new();
		for a in &corpus {
			for b in &corpus {
				if a.prefix.cmp(&b.prefix) != a.exact.cmp(&b.exact) {
					misses.push((a.n.clone(), b.n.clone()));
				}
			}
		}
		assert!(
			misses.is_empty(),
			"the value prefix disagrees with the numeric order for {} pairs, such as {:?}",
			misses.len(),
			&misses[..misses.len().min(5)]
		);

		// Fresh random pairs, beyond those of the corpus.
		for _ in 0..CASES {
			let (a, b) = (gen_number(&mut rng), gen_number(&mut rng));
			let (ea, eb) = (exact_of(&a).unwrap(), exact_of(&b).unwrap());
			assert_eq!(value_prefix(&a).cmp(&value_prefix(&b)), ea.cmp(&eb), "{a:?} against {b:?}");
		}

		for a in &corpus {
			// An element is its prefix, as normalised, followed by a kind tag, when
			// the number is stored in normalised form.
			let normalised = a.stored_prefix == a.prefix;
			if normalised {
				assert!(a.element.starts_with(&a.prefix), "{:?}", a.n);
				let tag = &a.element[a.prefix.len()..];
				assert!(tags.iter().any(|t| t == tag), "{:?} carries the tag {tag:?}", a.n);
			}
			// The prefix of a number is the prefix of exactly the elements that hold an
			// equal number in normalised form.
			for b in &corpus {
				let has_prefix = b.element.starts_with(&a.prefix);
				let equal = a.exact == b.exact;
				let b_normalised = b.stored_prefix == b.prefix;
				assert_eq!(
					has_prefix,
					equal && b_normalised,
					"the element of {:?} {} the value prefix of {:?}",
					b.n,
					if has_prefix {
						"has"
					} else {
						"lacks"
					},
					a.n
				);
			}
		}
	}

	/// A float or a decimal that equals an integer is not reached by a bound built
	/// from that integer. Even the value prefix, which ignores the kind tag, misses
	/// a decimal written with trailing zeros.
	#[test]
	fn an_int_bound_reaches_only_ints() {
		let five = Number::Int(5);
		let bound = array_key_prefix(&[int(5)]);
		for key in [key_of(&[int(5)]), key_of(&[int(5), string("x")])] {
			assert!(contains_prefix_range(&bound, &key));
		}
		for other in [Number::Float(5.0), decimal(5, 0), decimal(500, 2)] {
			assert_eq!(five, other, "equal as numbers");
			for key in [
				key_of(&[Value::Number(other.clone())]),
				key_of(&[Value::Number(other.clone()), string("x")]),
			] {
				assert!(!contains_prefix_range(&bound, &key), "{other:?}");
			}
		}

		// `5.0` and `5dec` share the decimal buffer of `5` and differ in the kind tag.
		for other in [Number::Float(5.0), decimal(5, 0)] {
			assert_eq!(stored_prefix(&other), stored_prefix(&five));
			assert_ne!(encode_component(&Value::Number(other)), encode_component(&int(5)));
		}
		// `5.00dec` has a longer buffer, so it shares only the normalised prefix.
		let padded = decimal(500, 2);
		assert_eq!(value_prefix(&padded), value_prefix(&five));
		assert_ne!(stored_prefix(&padded), stored_prefix(&five));
		assert!(!encode_component(&Value::Number(padded)).starts_with(&value_prefix(&five)));
	}

	/// Where numbers of different kinds share a position, a key orders by value,
	/// then kind tag, then the next element, so two keys with equal numbers do not
	/// order by the element that follows.
	#[test]
	fn numbers_of_different_kinds_order_by_kind_before_the_next_element() {
		let as_int = [int(5), string("b")];
		let as_float = [Value::Number(Number::Float(5.0)), string("a")];
		// As values the numbers are equal, so `"a"` decides.
		assert_eq!(tuple_cmp(&as_float, &as_int), Ordering::Less);
		// In bytes the int's kind tag sorts before the float's and decides first.
		assert!(kind_tags()[0] < kind_tags()[1]);
		assert!(key_of(&as_int) < key_of(&as_float));
	}

	// ------------------------------
	// (c) array key prefixes
	// ------------------------------

	fn contains_prefix_range(prefix: &[u8], key: &[u8]) -> bool {
		let end = successor(prefix).expect("a prefix with a successor");
		prefix <= key && key < end.as_slice()
	}

	#[test]
	fn array_prefix_ranges_contain_exactly_their_extensions() {
		let mut rng = Rng::new(0x5EED_0006);
		let head = array_key_prefix(&[]);
		for other in [
			RecordIdKey::String(Strand::new("x")),
			RecordIdKey::Number(5),
			RecordIdKey::Uuid(Uuid::nil()),
			RecordIdKey::Object(Object::default()),
		] {
			let key = storekey::encode_vec(&other).unwrap();
			assert!(!key.starts_with(&head), "{other:?} is not an array id");
			assert!(!contains_prefix_range(&head, &key), "{other:?} is not an array id");
		}
		for _ in 0..CASES {
			let pool: Vec<Value> = (0..8).map(|_| gen_element(&mut rng)).collect();
			let draw = |rng: &mut Rng| -> Vec<Value> {
				(0..1 + rng.index(4)).map(|_| rng.pick(&pool).clone()).collect()
			};
			let (a, b) = (draw(&mut rng), draw(&mut rng));
			let (key_a, key_b) = (key_of(&a), key_of(&b));
			assert_eq!(key_a.last(), Some(&0), "an array key closes with a terminator");
			for k in 0..=a.len() {
				let prefix = array_key_prefix(&a[..k]);
				assert!(prefix.starts_with(&head));
				assert!(key_a.starts_with(&prefix), "{a:?} lacks its own prefix of {k}");
				if k == a.len() {
					assert_eq!(concat(&prefix, &[0]), key_a);
				}
				let shares = b.len() >= k
					&& (0..k).all(|i| encode_component(&a[i]) == encode_component(&b[i]));
				assert_eq!(
					key_b.starts_with(&prefix),
					shares,
					"the first {k} elements of {a:?} and {b:?} are shared exactly when the prefix is"
				);
				assert_eq!(
					contains_prefix_range(&prefix, &key_b),
					shares,
					"the range of the first {k} elements of {a:?} holds {b:?} exactly when they are shared"
				);
			}
		}
	}

	// ------------------------------
	// (d) widened ranges
	// ------------------------------

	#[derive(Clone, Copy, Debug, PartialEq)]
	enum Op {
		Eq,
		Lt,
		Le,
		Gt,
		Ge,
	}

	impl Op {
		fn holds(self, stored_against_bound: Ordering) -> bool {
			match self {
				Op::Eq => stored_against_bound == Ordering::Equal,
				Op::Lt => stored_against_bound == Ordering::Less,
				Op::Le => stored_against_bound != Ordering::Greater,
				Op::Gt => stored_against_bound == Ordering::Greater,
				Op::Ge => stored_against_bound != Ordering::Less,
			}
		}
	}

	#[derive(Clone, Copy, Debug, PartialEq)]
	enum Granularity {
		Exact,
		CoarseSecond,
	}

	/// A half-open byte range; `end: None` is unbounded.
	struct Span {
		start: Vec<u8>,
		end: Option<Vec<u8>>,
	}

	impl Span {
		fn contains(&self, key: &[u8]) -> bool {
			key >= self.start.as_slice() && self.end.as_ref().is_none_or(|end| key < end.as_slice())
		}
	}

	fn bound_bytes(g: Granularity, bound: &Value) -> Vec<u8> {
		match (g, bound) {
			(Granularity::Exact, v) => encode_component(v),
			(Granularity::CoarseSecond, Value::Datetime(d)) => {
				encode_datetime_second_prefix(d).expect("a datetime within the key range")
			}
			(g, v) => panic!("a {g:?} bound cannot be {v:?}"),
		}
	}

	/// The byte range for conjuncts on the component that follows `prefix`.
	fn span_for(prefix: &[u8], g: Granularity, bounds: &[(Op, Value)]) -> Span {
		let mut span = Span {
			start: prefix.to_vec(),
			end: successor(prefix),
		};
		for (op, bound) in bounds {
			let at = concat(prefix, &bound_bytes(g, bound));
			let after = successor(&at);
			let (lo, hi) = match (g, op) {
				(_, Op::Eq) => (Some(at), after),
				(Granularity::Exact, Op::Ge) => (Some(at), None),
				(Granularity::Exact, Op::Gt) => (after, None),
				(Granularity::Exact, Op::Lt) => (None, Some(at)),
				(Granularity::Exact, Op::Le) => (None, after),
				(_, Op::Ge | Op::Gt) => (Some(at), None),
				(_, Op::Lt | Op::Le) => (None, after),
			};
			if let Some(lo) = lo
				&& lo > span.start
			{
				span.start = lo;
			}
			if let Some(hi) = hi
				&& span.end.as_ref().is_none_or(|end| hi < *end)
			{
				span.end = Some(hi);
			}
		}
		span
	}

	/// Whether the stored tuple satisfies the equality prefix and every bound,
	/// judged by `Value` comparison.
	fn satisfies(tuple: &[Value], eq: &[Value], bounds: &[(Op, Value)]) -> bool {
		let k = eq.len();
		tuple.len() > k
			&& eq.iter().zip(tuple).all(|(e, t)| e == t)
			&& bounds.iter().all(|(op, bound)| op.holds(tuple[k].cmp(bound)))
	}

	/// Builds the range, asserts that every satisfying tuple is inside it, and
	/// returns, per tuple, whether it was inside and whether it satisfied.
	fn check_case(
		name: &str,
		g: Granularity,
		eq: &[Value],
		bounds: &[(Op, Value)],
		tuples: &[Vec<Value>],
	) -> Vec<(bool, bool)> {
		let span = span_for(&array_key_prefix(eq), g, bounds);
		tuples
			.iter()
			.map(|tuple| {
				let inside = span.contains(&key_of(tuple));
				let wanted = satisfies(tuple, eq, bounds);
				assert!(
					!wanted || inside,
					"{name}: {tuple:?} satisfies {bounds:?} after {eq:?} but is outside the range"
				);
				(inside, wanted)
			})
			.collect()
	}

	fn gen_bounds(rng: &mut Rng, pool: &[Value]) -> Vec<(Op, Value)> {
		let any = [Op::Eq, Op::Lt, Op::Le, Op::Gt, Op::Ge];
		match rng.below(4) {
			0 => vec![(Op::Eq, rng.pick(pool).clone())],
			1 => vec![(*rng.pick(&any), rng.pick(pool).clone())],
			2 => vec![
				(*rng.pick(&[Op::Gt, Op::Ge]), rng.pick(pool).clone()),
				(*rng.pick(&[Op::Lt, Op::Le]), rng.pick(pool).clone()),
			],
			_ => vec![
				(*rng.pick(&any), rng.pick(pool).clone()),
				(*rng.pick(&any), rng.pick(pool).clone()),
			],
		}
	}

	/// The stored tuples for a case: the equality prefix (occasionally with one
	/// element changed), a value from the pool, and a short tail.
	fn stored_tuples(rng: &mut Rng, eq: &[Value], pool: &[Value]) -> Vec<Vec<Value>> {
		(0..24)
			.map(|_| {
				let mut tuple = eq.to_vec();
				if !eq.is_empty() && rng.one_in(4) {
					let i = rng.index(eq.len());
					tuple[i] = gen_eq_element(rng);
				}
				tuple.push(rng.pick(pool).clone());
				for _ in 0..rng.below(3) {
					tuple.push(gen_eq_element(rng));
				}
				tuple
			})
			.collect()
	}

	fn gen_eq_prefix(rng: &mut Rng) -> Vec<Value> {
		(0..rng.below(3)).map(|_| gen_eq_element(rng)).collect()
	}

	/// Exactly ordered kinds: the range holds the satisfying tuples and no others.
	fn exact_ranges_are_exact() {
		let mut rng = Rng::new(0x5EED_0007);
		for case in 0..CASES {
			let kind = case % EXACT_KINDS.len();
			let eq = gen_eq_prefix(&mut rng);
			let mut pool: Vec<Value> = (0..6).map(|_| gen_exact(&mut rng, kind)).collect();
			pool.push(match kind {
				0 => string(""),
				1 => Value::Uuid(Uuid::max()),
				2 => Value::Duration(Duration::MAX),
				3 => Value::Bool(true),
				_ => int(i64::MAX),
			});
			let bounds = gen_bounds(&mut rng, &pool);
			let tuples = stored_tuples(&mut rng, &eq, &pool);
			let outcome = check_case(EXACT_KINDS[kind], Granularity::Exact, &eq, &bounds, &tuples);
			for ((inside, wanted), tuple) in outcome.iter().zip(&tuples) {
				if tuple.len() > eq.len() {
					assert_eq!(
						inside, wanted,
						"{}: the range for {bounds:?} after {eq:?} is not exact for {tuple:?}",
						EXACT_KINDS[kind]
					);
				}
			}
		}
	}

	/// Datetime components: every satisfying tuple is inside, and a tuple outside
	/// the condition is inside only when it shares a whole second with the bound.
	fn datetime_ranges_are_supersets() {
		let mut rng = Rng::new(0x5EED_0009);
		let mut false_positives = 0usize;
		for _ in 0..CASES {
			let eq = gen_eq_prefix(&mut rng);
			let pool: Vec<Value> =
				(0..10).map(|_| Value::Datetime(gen_datetime(&mut rng))).collect();
			let bounds = gen_bounds(&mut rng, &pool);
			let tuples = stored_tuples(&mut rng, &eq, &pool);
			let outcome = check_case("datetime", Granularity::CoarseSecond, &eq, &bounds, &tuples);
			if let [(_, Value::Datetime(bound))] = bounds.as_slice() {
				for ((inside, wanted), tuple) in outcome.iter().zip(&tuples) {
					if *inside && !*wanted && tuple.len() > eq.len() && eq == tuple[..eq.len()] {
						false_positives += 1;
						let Value::Datetime(stored) = &tuple[eq.len()] else {
							panic!("{tuple:?} holds no datetime after {eq:?}");
						};
						assert_eq!(
							stored.floor_second(),
							bound.floor_second(),
							"{tuple:?} is inside the range for {bounds:?} from another second"
						);
					}
				}
			}
		}
		assert!(false_positives > 0);
	}

	#[test]
	fn widened_ranges_are_supersets() {
		exact_ranges_are_exact();
		datetime_ranges_are_supersets();
	}

	// ------------------------------
	// (e) datetime second prefix
	// ------------------------------

	#[test]
	fn datetime_second_prefix_orders_and_contains_its_second() {
		let mut rng = Rng::new(0x5EED_000A);
		let mut same_second = 0usize;
		for _ in 0..CASES {
			let a = gen_datetime(&mut rng);
			let b = if rng.one_in(2) {
				// The same second, another fraction.
				datetime(a.timestamp(), gen_nanos(&mut rng))
			} else {
				gen_datetime(&mut rng)
			};
			assert!(a.in_key_range() && b.in_key_range());
			let (pa, pb) = (a.key_second_prefix().unwrap(), b.key_second_prefix().unwrap());

			// The prefix is the first 19 bytes of the text the key holds.
			assert_eq!(&pa[..], &a.to_string().as_bytes()[..KEY_SECOND_PREFIX_LEN]);

			// Prefix order is the order of the floors.
			assert_eq!(pa.cmp(&pb), a.floor_second().cmp(&b.floor_second()), "{a} {b}");
			let same = a.floor_second() == b.floor_second();
			assert_eq!(pa == pb, same, "{a} {b}");
			same_second += usize::from(same);

			// The floor drops the fraction and nothing else.
			assert_eq!(a.floor_second().nanosecond(), 0);
			assert_eq!(a.floor_second().timestamp(), a.timestamp());
			assert!(a.floor_second() <= a);

			// The element prefix opens the element of every datetime in the second, and
			// of no other.
			let prefix_a = encode_datetime_second_prefix(&a).unwrap();
			assert_eq!(&prefix_a[VALUE_DISCRIMINANT_LEN..], &pa[..]);
			let element_a = encode_component(&Value::Datetime(a));
			let element_b = encode_component(&Value::Datetime(b));
			assert!(element_a.starts_with(&prefix_a));
			assert_eq!(element_b.starts_with(&prefix_a), same, "{a} {b}");

			// Across seconds the whole element orders chronologically, which is what lets
			// a second-granular bound separate them.
			if !same {
				assert_eq!(element_a.cmp(&element_b), a.cmp(&b), "{a} {b}");
			}

			// Equal instants have one encoding, so an equality needs no widening.
			assert_eq!(element_a == element_b, a == b, "{a} {b}");
		}
		assert!(same_second > CASES / 4);
	}

	#[test]
	fn datetime_fraction_inversion_is_pinned() {
		let element = |nanos: u32| encode_component(&Value::Datetime(datetime(BASE_SECS, nanos)));
		let whole = element(0);
		let half = element(500_000_000);
		// 500ms is later than the whole second, and its encoding is smaller.
		assert!(datetime(BASE_SECS, 500_000_000) > datetime(BASE_SECS, 0));
		assert!(half < whole);
		assert_eq!(datetime(BASE_SECS, 500_000_000).to_string(), "2023-11-14T22:13:20.500Z");
		assert_eq!(datetime(BASE_SECS, 0).to_string(), "2023-11-14T22:13:20Z");

		// In chronological order the fractions encode in this order of byte rank.
		let chronological = [0, 1, 500_000_000, 500_100_000, 999_999_999];
		let mut by_bytes = chronological;
		by_bytes.sort_by_key(|nanos| element(*nanos));
		assert_eq!(by_bytes, [1, 500_100_000, 500_000_000, 999_999_999, 0]);

		// A bound at the second delimits all of them, and the next second excludes them.
		let prefix = encode_datetime_second_prefix(&datetime(BASE_SECS, 0)).unwrap();
		for nanos in chronological {
			assert!(element(nanos).starts_with(&prefix));
		}
		let next = encode_component(&Value::Datetime(datetime(BASE_SECS + 1, 0)));
		assert!(!next.starts_with(&prefix));
		assert!(next > whole && next > half);
	}

	#[test]
	fn datetime_outside_the_key_range_has_no_prefix() {
		let year = |y: i32| Utc.with_ymd_and_hms(y, 1, 1, 0, 0, 0).single().map(Datetime).unwrap();
		assert_eq!(year(0).timestamp(), MIN_SECS);
		assert_eq!(year(10_000).timestamp(), MAX_SECS + 1);
		assert_eq!(
			Utc.with_ymd_and_hms(9999, 12, 31, 23, 59, 59).single().unwrap().timestamp(),
			MAX_SECS
		);
		assert_eq!(
			encode_datetime_second_prefix(&year(0)).map(|p| p[1..].to_vec()),
			Some(b"0000-01-01T00:00:00".to_vec())
		);
		let last = datetime(MAX_SECS, 999_999_999);
		assert_eq!(last.key_second_prefix(), Some(*b"9999-12-31T23:59:59"));
		assert_eq!(last.floor_second().timestamp(), MAX_SECS);
		for outside in [year(-1), year(10_000), year(-262_143), datetime(MAX_SECS + 1, 0)] {
			assert!(!outside.in_key_range(), "{outside}");
			assert_eq!(outside.key_second_prefix(), None, "{outside}");
			assert_eq!(encode_datetime_second_prefix(&outside), None, "{outside}");
		}
		// Outside the key range the text neither has the fixed width nor sorts with the rest.
		let early = encode_component(&Value::Datetime(year(-1)));
		let late = encode_component(&Value::Datetime(year(10_000)));
		let inside = encode_component(&Value::Datetime(year(0)));
		assert!(early < inside && late < inside, "signed years sort before four-digit years");

		// A leap second renders as second 60.
		let leap = datetime(1_483_228_799, 1_500_000_000);
		assert_eq!(leap.to_string(), "2016-12-31T23:59:60.500Z");
		assert!(!leap.in_key_range());
		assert_eq!(leap.key_second_prefix(), None);
	}

	// ------------------------------
	// (g) discriminants
	// ------------------------------

	#[test]
	fn variant_discriminant_order_matches_value_cmp() {
		// One value of every variant that has a key encoding, in declaration order.
		let samples = vec![
			Value::None,
			Value::Null,
			Value::Bool(false),
			int(0),
			string("a"),
			duration(1, 0),
			Value::Datetime(datetime(0, 0)),
			Value::Uuid(Uuid::nil()),
			Value::Array(Array::default()),
			Value::Set(Set::new()),
			Value::Object(Object::default()),
			Value::Geometry(Geometry::Point(geo_types::Point::new(1.0, 2.0))),
			Value::Bytes(Bytes::from(vec![1u8])),
			Value::Table(TableName::from("t")),
			record("t", RecordIdKey::Number(1)),
			Value::File(File::new("bucket".to_owned(), "key".to_owned())),
			Value::Range(Box::new(Range::unbounded())),
			Value::Date(crate::val::Date(0)),
			Value::Time(crate::val::Time(0)),
			Value::Timestamp(crate::val::Timestamp {
				secs: 0,
				nanos: 0,
			}),
			Value::Interval(crate::val::Interval::new(0, 0, 0)),
		];
		let encoded: Vec<Vec<u8>> = samples.iter().map(encode_component).collect();
		for (i, (a, ea)) in samples.iter().zip(&encoded).enumerate() {
			for (j, (b, eb)) in samples.iter().zip(&encoded).enumerate() {
				assert_eq!(a.cmp(b), i.cmp(&j), "{a:?} against {b:?}");
				assert_eq!(
					ea[..VALUE_DISCRIMINANT_LEN].cmp(&eb[..VALUE_DISCRIMINANT_LEN]),
					i.cmp(&j)
				);
				assert_eq!(ea.cmp(eb), i.cmp(&j), "{a:?} against {b:?}");
			}
		}
		// Every element of an array key is led by its discriminant, so the discriminants
		// are all distinct and the first byte of a key's element names its variant.
		let mut heads: Vec<u8> = encoded.iter().map(|e| e[0]).collect();
		heads.dedup();
		assert_eq!(heads.len(), samples.len());
	}

	#[test]
	fn values_without_a_key_encoding_are_refused() {
		let regex = Value::Regex("a+".parse::<Regex>().unwrap());
		assert_eq!(try_encode_component(&regex), None);
		assert_eq!(try_array_key_prefix(&[string("a"), regex.clone(), string("b")]), None);
		assert_eq!(
			try_array_key_prefix(&[string("a"), string("b")]),
			Some(array_key_prefix(&[string("a"), string("b")]))
		);
		// A failed element leaves nothing behind.
		let mut out = vec![1, 2, 3];
		assert!(!write_element(&mut out, &regex));
		assert_eq!(out, [1, 2, 3]);
	}

	#[test]
	fn encoding_widths_the_helpers_rely_on_are_pinned() {
		assert_eq!(encode_component(&Value::None).len(), VALUE_DISCRIMINANT_LEN);
		let tags = kind_tags();
		for tag in &tags {
			assert_eq!(tag.len(), NUMBER_KIND_TAG_LEN);
		}
		assert!(tags[0] < tags[1] && tags[1] < tags[2], "{tags:?}");
		for (n, tag) in
			[(Number::Int(7), &tags[0]), (Number::Float(7.5), &tags[1]), (decimal(75, 1), &tags[2])]
		{
			let element = encode_component(&Value::Number(n.clone()));
			let prefix = value_prefix(&n);
			assert_eq!(element, concat(&prefix, tag), "{n:?}");
		}
		// An empty array key is its head and a terminator.
		assert_eq!(key_of(&[]), concat(&array_key_prefix(&[]), &[0]));
		// An element's encoding is what an array key holds for it.
		let elems = [string("a"), int(1), Value::Bool(true)];
		let expected: Vec<u8> = elems.iter().flat_map(encode_component).collect();
		assert_eq!(key_of(&elems), [array_key_prefix(&[]), expected, vec![0]].concat());
	}
}
