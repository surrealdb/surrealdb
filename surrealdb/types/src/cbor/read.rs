//! Reading a CBOR item straight into a [`Value`], without building a `ciborium::Value` first.
//!
//! [`read`] answers exactly what decoding the item with `ciborium` and converting it with
//! [`super::convert`] answers, including the error, for every input. It reads the forms a
//! result row is made of where they lie: integers, floats, `true`, `false`, `null` and
//! `undefined`, definite-length text, bytes, arrays and maps, and the tags of `NONE`, a table,
//! a record id with an integer or text key, a datetime, a duration, a uuid, a decimal and, in
//! `cbor.2`, the sized numbers. Any other item, and any item one of those forms does not read
//! as written (an indefinite length, a malformed tag, a value out of range, a truncated input,
//! a nesting past the limit), is read again from its first byte through `ciborium` and the
//! conversion, so what it answers, value or error, is theirs.

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;

use anyhow::{Result, anyhow};

use super::RecordIdParser;
use super::convert::{
	TAG_BIGDECIMAL, TAG_F32, TAG_I8, TAG_I16, TAG_I32, TAG_U8, TAG_U16, TAG_U32, TAG_U64, convert,
};
use crate::{
	Array, BigDecimal, Datetime, Duration, Number, Object, RecordId, RecordIdKey, Table, Uuid,
	Value,
};

const TAG_NONE: u64 = 6;
const TAG_TABLE: u64 = 7;
const TAG_RECORDID: u64 = 8;
const TAG_STRING_DECIMAL: u64 = 10;
const TAG_CUSTOM_DATETIME: u64 = 12;
const TAG_CUSTOM_DURATION: u64 = 14;
const TAG_SPEC_UUID: u64 = 37;

/// The longest collection a header is trusted to pre-size: a longer one grows as it is read, so
/// a header cannot make the reader reserve more than the input could hold.
const PRESIZE_LIMIT: u64 = 4096;

/// The item at the start of `bytes`, and how many bytes it took.
///
/// `recursion_limit` bounds how many arrays, maps and tags may enclose an item, as it bounds
/// `ciborium::de::from_reader_with_recursion_limit`. `v2` reads the `cbor.2` sized number tags.
pub(super) fn read(
	bytes: &[u8],
	recursion_limit: usize,
	v2: bool,
	record_id: Option<RecordIdParser>,
) -> Result<(Value, usize)> {
	let mut reader = Reader {
		bytes,
		pos: 0,
		v2,
		record_id,
	};
	let value = reader.item(recursion_limit)?;
	Ok((value, reader.pos))
}

/// The entries of a CBOR map: each key with the bytes of its value, in the order written.
pub type MapEntries<'a> = Vec<(&'a str, &'a [u8])>;

/// The entries of the CBOR map at the start of `bytes`, each key with the bytes of its value,
/// and how many bytes the map took; nothing is decoded. `None` where the item is not a map of
/// text keys in definite-length forms nesting within `recursion_limit`, or is malformed.
pub fn map_entries(bytes: &[u8], recursion_limit: usize) -> Option<(MapEntries<'_>, usize)> {
	let mut reader = Reader {
		bytes,
		pos: 0,
		v2: true,
		record_id: None,
	};
	let budget = recursion_limit.checked_sub(1)?;
	let head = reader.head().filter(|h| h.major == 5)?;
	let mut entries = Vec::with_capacity(head.arg.min(PRESIZE_LIMIT) as usize);
	for _ in 0..head.arg {
		let key = reader.head().filter(|h| h.major == 3)?;
		let key = std::str::from_utf8(reader.take(key.arg)?).ok()?;
		let start = reader.pos;
		reader.skip(budget)?;
		entries.push((key, &bytes[start..reader.pos]));
	}
	Some((entries, reader.pos))
}

/// A CBOR head: its major type, its additional information and the argument that follows.
#[derive(Clone, Copy)]
struct Head {
	major: u8,
	info: u8,
	arg: u64,
}

struct Reader<'a> {
	bytes: &'a [u8],
	pos: usize,
	v2: bool,
	record_id: Option<RecordIdParser>,
}

impl<'a> Reader<'a> {
	/// The head at the read position, advancing past it; `None` where the input ends inside it
	/// or it uses a reserved argument form, which the fallback reads (and refuses).
	#[inline]
	fn head(&mut self) -> Option<Head> {
		let first = *self.bytes.get(self.pos)?;
		let major = first >> 5;
		let info = first & 0x1f;
		let (arg, len) = match info {
			0..=23 => (u64::from(info), 0),
			24 => (u64::from(*self.bytes.get(self.pos + 1)?), 1),
			25 => (u64::from(u16::from_be_bytes(self.array_at(self.pos + 1)?)), 2),
			26 => (u64::from(u32::from_be_bytes(self.array_at(self.pos + 1)?)), 4),
			27 => (u64::from_be_bytes(self.array_at(self.pos + 1)?), 8),
			// Reserved forms and indefinite lengths.
			_ => return None,
		};
		self.pos += 1 + len;
		Some(Head {
			major,
			info,
			arg,
		})
	}

	#[inline]
	fn array_at<const N: usize>(&self, at: usize) -> Option<[u8; N]> {
		self.bytes.get(at..at + N)?.try_into().ok()
	}

	/// The next `len` bytes, advancing past them.
	#[inline]
	fn take(&mut self, len: u64) -> Option<&'a [u8]> {
		let len = usize::try_from(len).ok()?;
		let end = self.pos.checked_add(len)?;
		let slice = self.bytes.get(self.pos..end)?;
		self.pos = end;
		Some(slice)
	}

	/// Move past the item at the read position, enclosed by containers that leave `budget`
	/// levels, without reading it; `None` where it is not in a definite-length form, nests past
	/// `budget` or is cut short.
	fn skip(&mut self, budget: usize) -> Option<()> {
		let mut pending: u64 = 1;
		// The levels left for the items still to skip at each depth, innermost last.
		let mut stack: Vec<(u64, usize)> = Vec::new();
		let mut budget = budget;
		loop {
			while pending == 0 {
				match stack.pop() {
					Some(outer) => (pending, budget) = outer,
					None => return Some(()),
				}
			}
			pending -= 1;
			let head = self.head()?;
			let (nested, count) = match head.major {
				0 | 1 | 7 => (false, 0),
				2 | 3 => {
					self.take(head.arg)?;
					(false, 0)
				}
				4 => (true, head.arg),
				5 => (true, head.arg.checked_mul(2)?),
				6 => (true, 1),
				_ => return None,
			};
			if nested {
				stack.push((pending, budget));
				budget = budget.checked_sub(1)?;
				pending = count;
			}
		}
	}

	/// The item at the read position, enclosed by containers that leave `budget` levels.
	fn item(&mut self, budget: usize) -> Result<Value> {
		let start = self.pos;
		match self.fast(budget) {
			Some(value) => Ok(value),
			None => self.fallback(start, budget),
		}
	}

	/// The item at `start`, read through `ciborium` and the conversion.
	#[cold]
	fn fallback(&mut self, start: usize, budget: usize) -> Result<Value> {
		let mut rest = &self.bytes[start..];
		let item: ciborium::Value =
			ciborium::de::from_reader_with_recursion_limit(&mut rest, budget)
				.map_err(|e| anyhow!(e.to_string()))?;
		self.pos = self.bytes.len() - rest.len();
		convert(item, self.v2, self.record_id)
	}

	/// The item at the read position, where every part of it is a form this reader reads;
	/// `None` otherwise, with the position anywhere.
	fn fast(&mut self, budget: usize) -> Option<Value> {
		let head = self.head()?;
		match head.major {
			0 => Some(Value::Number(Number::Int(i64::try_from(head.arg).ok()?))),
			1 => Some(Value::Number(Number::Int(negative(head.arg)?))),
			2 => Some(Value::Bytes(self.take(head.arg)?.to_vec().into())),
			3 => Some(Value::String(self.text(head.arg)?)),
			4 => {
				let budget = budget.checked_sub(1)?;
				let mut values = Vec::with_capacity(head.arg.min(PRESIZE_LIMIT) as usize);
				for _ in 0..head.arg {
					values.push(self.nested(budget)?);
				}
				Some(Value::Array(Array(values)))
			}
			5 => {
				let budget = budget.checked_sub(1)?;
				let mut entries = Vec::with_capacity(head.arg.min(PRESIZE_LIMIT) as usize);
				// Whether every key so far follows the one before it, as the keys of an object
				// written from a map do.
				let mut ascending = true;
				for _ in 0..head.arg {
					let key = self.head().filter(|h| h.major == 3)?;
					let key = self.text(key.arg)?;
					let value = self.nested(budget)?;
					if let Some((last, _)) = entries.last() {
						ascending &= key.as_str() > String::as_str(last);
					}
					entries.push((key, value));
				}
				// Keys in strictly ascending order are distinct and are the object's entries as
				// they are.
				if ascending {
					return Some(Value::Object(Object::from_sorted(entries)));
				}
				let mut map = BTreeMap::new();
				for (key, value) in entries {
					match map.entry(key) {
						Entry::Vacant(slot) => {
							slot.insert(value);
						}
						// The conversion refuses a duplicate key, with a message it writes.
						Entry::Occupied(_) => return None,
					}
				}
				Some(Value::Object(Object(map.into_iter().collect())))
			}
			6 => self.tag(head.arg, budget),
			7 => match head.info {
				20 => Some(Value::Bool(false)),
				21 => Some(Value::Bool(true)),
				// `null` and `undefined` both read as `null`.
				22 | 23 => Some(Value::Null),
				25 => Some(float(f64::from(half::f16::from_bits(head.arg as u16)))),
				26 => Some(float(f64::from(f32::from_bits(head.arg as u32)))),
				27 => Some(float(f64::from_bits(head.arg))),
				_ => None,
			},
			_ => None,
		}
	}

	/// An item inside a container, read in place or, failing that, through the fallback, so
	/// that one unusual item does not send its whole container to the fallback. An item the
	/// fallback refuses refuses the container too.
	#[inline]
	fn nested(&mut self, budget: usize) -> Option<Value> {
		let start = self.pos;
		match self.fast(budget) {
			Some(value) => Some(value),
			None => {
				self.pos = start;
				None
			}
		}
		.or_else(|| self.fallback(start, budget).ok())
	}

	fn text(&mut self, len: u64) -> Option<String> {
		let bytes = self.take(len)?;
		std::str::from_utf8(bytes).ok().map(str::to_owned)
	}

	/// The tagged item whose tag `tag` was just read.
	fn tag(&mut self, tag: u64, budget: usize) -> Option<Value> {
		// A tag is a level of nesting for the recursion limit.
		let budget = budget.checked_sub(1)?;
		match tag {
			TAG_NONE => (*self.bytes.get(self.pos)? == 0xf6).then(|| {
				self.pos += 1;
				Value::None
			}),
			TAG_TABLE => {
				let head = self.head().filter(|h| h.major == 3)?;
				Some(Value::Table(Table::new(self.text(head.arg)?)))
			}
			TAG_RECORDID => {
				budget.checked_sub(1)?;
				self.head().filter(|h| h.major == 4 && h.arg == 2)?;
				let table = self.head().filter(|h| h.major == 3)?;
				let table = self.text(table.arg)?;
				let key = self.head()?;
				let key = match key.major {
					0 => RecordIdKey::Number(i64::try_from(key.arg).ok()?),
					1 => RecordIdKey::Number(negative(key.arg)?),
					3 => RecordIdKey::String(self.text(key.arg)?),
					_ => return None,
				};
				Some(Value::RecordId(RecordId {
					table: table.into(),
					key,
				}))
			}
			TAG_CUSTOM_DATETIME => {
				budget.checked_sub(1)?;
				self.head().filter(|h| h.major == 4 && h.arg == 2)?;
				let seconds = self.head()?;
				let seconds = match seconds.major {
					0 => i64::try_from(seconds.arg).ok()?,
					1 => negative(seconds.arg)?,
					_ => return None,
				};
				let nanos = self.head().filter(|h| h.major == 0)?;
				let nanos = u32::try_from(nanos.arg).ok()?;
				Datetime::try_from((seconds, nanos)).ok().map(Value::Datetime)
			}
			TAG_CUSTOM_DURATION => {
				budget.checked_sub(1)?;
				let head = self.head().filter(|h| h.major == 4 && h.arg <= 2)?;
				let seconds = match head.arg {
					0 => 0,
					_ => self.head().filter(|h| h.major == 0)?.arg,
				};
				let nanos = match head.arg {
					2 => u32::try_from(self.head().filter(|h| h.major == 0)?.arg).ok()?,
					_ => 0,
				};
				let seconds = seconds.checked_add(u64::from(nanos / 1_000_000_000))?;
				Some(Value::Duration(Duration::new(seconds, nanos % 1_000_000_000)))
			}
			TAG_SPEC_UUID => {
				let head = self.head().filter(|h| h.major == 2 && h.arg == 16)?;
				let bytes: [u8; 16] = self.take(head.arg)?.try_into().ok()?;
				Some(Value::Uuid(Uuid::from(uuid::Uuid::from_bytes(bytes))))
			}
			TAG_STRING_DECIMAL => {
				let head = self.head().filter(|h| h.major == 3)?;
				let text = self.text(head.arg)?;
				let decimal = super::convert::decimal_normalized(&text).ok()?;
				Some(Value::Number(Number::Decimal(decimal)))
			}
			TAG_BIGDECIMAL if self.v2 => {
				let head = self.head().filter(|h| h.major == 3)?;
				let text = self.text(head.arg)?;
				let decimal = text.parse::<BigDecimal>().ok()?;
				Some(Value::Number(Number::BigDecimal(Box::new(decimal))))
			}
			TAG_I8 | TAG_U8 | TAG_I16 | TAG_U16 | TAG_I32 | TAG_U32 | TAG_U64 if self.v2 => {
				let head = self.head()?;
				let int = match head.major {
					0 => i128::from(head.arg),
					1 => -1 - i128::from(head.arg),
					_ => return None,
				};
				let number = match tag {
					TAG_I8 => Number::I8(i8::try_from(int).ok()?),
					TAG_U8 => Number::U8(u8::try_from(int).ok()?),
					TAG_I16 => Number::I16(i16::try_from(int).ok()?),
					TAG_U16 => Number::U16(u16::try_from(int).ok()?),
					TAG_I32 => Number::I32(i32::try_from(int).ok()?),
					TAG_U32 => Number::U32(u32::try_from(int).ok()?),
					_ => Number::U64(u64::try_from(int).ok()?),
				};
				Some(Value::Number(number))
			}
			TAG_F32 if self.v2 => {
				let head = self.head().filter(|h| h.major == 7)?;
				let f = match head.info {
					25 => f64::from(half::f16::from_bits(head.arg as u16)),
					26 => f64::from(f32::from_bits(head.arg as u32)),
					27 => f64::from_bits(head.arg),
					_ => return None,
				};
				let val = f as f32;
				(f.is_nan() || f64::from(val).to_bits() == f.to_bits())
					.then_some(Value::Number(Number::F32(val)))
			}
			// `f16` (rare enough not to matter) and every other tag are read by the fallback.
			_ => None,
		}
	}
}

/// The integer a CBOR negative argument `arg` stands for (`-1 - arg`), where an `i64` holds it.
#[inline]
fn negative(arg: u64) -> Option<i64> {
	i64::try_from(arg).ok().map(|n| -1 - n)
}

#[inline]
fn float(f: f64) -> Value {
	Value::Number(Number::Float(f))
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;
	use std::str::FromStr;

	use geo_types::Point;
	use rust_decimal::Decimal;

	use super::super::{
		decode, decode_exact, decode_exact_v2, decode_tree, decode_v2, encode, encode_v2,
	};
	use crate::{
		Array, Datetime, Duration, File, Geometry, Number, Object, Range, RecordId, RecordIdKey,
		Set, Table, Uuid, Value,
	};

	/// What a decode answers, comparable: the value's debug form (so a NaN equals itself), or
	/// the error's message.
	fn outcome(result: anyhow::Result<Value>) -> Result<String, String> {
		result.map(|v| format!("{v:?}")).map_err(|e| e.to_string())
	}

	/// Every decoder answers for `bytes` what decoding the `ciborium` tree answers.
	fn agree(bytes: &[u8], limit: usize) {
		let parse: Option<super::RecordIdParser> = None;
		for v2 in [false, true] {
			for exact in [false, true] {
				let fast = match (v2, exact) {
					(false, false) => decode(bytes, limit, parse),
					(true, false) => decode_v2(bytes, limit, parse),
					(false, true) => decode_exact(bytes, limit, parse),
					(true, true) => decode_exact_v2(bytes, limit, parse),
				};
				let tree = decode_tree(bytes, limit, v2, parse, exact);
				assert_eq!(
					outcome(fast),
					outcome(tree),
					"v2 {v2}, exact {exact}, limit {limit}, bytes {bytes:02x?}"
				);
			}
		}
	}

	fn record(key: RecordIdKey) -> Value {
		Value::RecordId(RecordId {
			table: Table::new("person"),
			key,
		})
	}

	fn corpus() -> Vec<Value> {
		let mut object = BTreeMap::new();
		object.insert("name".to_owned(), Value::String("Tobie".to_owned()));
		object.insert("age".to_owned(), Value::Number(Number::Int(42)));
		object.insert(
			"tags".to_owned(),
			Value::Array(Array::from(vec![
				Value::String("a".to_owned()),
				Value::Bool(true),
				Value::None,
			])),
		);
		let object = Object::from(object);
		let mut values = vec![
			Value::None,
			Value::Null,
			Value::Bool(false),
			Value::Bool(true),
			Value::String(String::new()),
			Value::String("ü".repeat(40)),
			Value::Bytes(vec![1, 2, 3].into()),
			Value::Bytes(Vec::new().into()),
			Value::Table(Table::new("person")),
			Value::Uuid(Uuid::from(uuid::Uuid::from_u128(
				0x1234_5678_9abc_def0_1234_5678_9abc_def0,
			))),
			Value::Duration(Duration::new(0, 0)),
			Value::Duration(Duration::new(5, 0)),
			Value::Duration(Duration::new(5, 7)),
			Value::Duration(Duration::new(u64::MAX, 999_999_999)),
			Value::Datetime(Datetime::try_from((1_700_000_000, 123)).unwrap()),
			Value::Datetime(Datetime::try_from((-62_135_596_800, 0)).unwrap()),
			Value::Number(Number::Decimal(Decimal::from_str("12.3400").unwrap())),
			Value::Geometry(Geometry::Point(Point::new(1.5, -2.5))),
			Value::File(File::new("bucket", "key")),
			Value::Set(Set::from(vec![
				Value::Number(Number::Int(1)),
				Value::Number(Number::Int(2)),
			])),
			Value::Range(Box::new(Range::new(
				std::ops::Bound::Included(Value::Number(Number::Int(1))),
				std::ops::Bound::Excluded(Value::Number(Number::Int(9))),
			))),
			record(RecordIdKey::Number(-7)),
			record(RecordIdKey::Number(i64::MAX)),
			record(RecordIdKey::String("tobie".to_owned())),
			record(RecordIdKey::Uuid(Uuid::from(uuid::Uuid::from_u128(7)))),
			record(RecordIdKey::Array(Array::from(vec![Value::Number(Number::Int(1))]))),
			record(RecordIdKey::Object(object.clone())),
			Value::Object(object.clone()),
			Value::Array(Array::from(vec![Value::Object(object); 3])),
		];
		for n in [
			0,
			1,
			23,
			24,
			255,
			256,
			65_535,
			65_536,
			u32::MAX as i64,
			i64::MAX,
			-1,
			-24,
			-25,
			-256,
			-257,
			i64::MIN,
		] {
			values.push(Value::Number(Number::Int(n)));
		}
		for f in
			[0.0, -0.0, 1.5, f64::MAX, f64::MIN_POSITIVE, f64::INFINITY, f64::NAN, 65504.0, 0.1]
		{
			values.push(Value::Number(Number::Float(f)));
		}
		values.extend([
			Value::Number(Number::I8(-128)),
			Value::Number(Number::U8(255)),
			Value::Number(Number::I16(-300)),
			Value::Number(Number::U16(65_535)),
			Value::Number(Number::I32(i32::MIN)),
			Value::Number(Number::U32(u32::MAX)),
			Value::Number(Number::U64(u64::MAX)),
			Value::Number(Number::F16(half::f16::from_f32(1.5))),
			Value::Number(Number::F32(1.25)),
			Value::Number(Number::F32(0.1)),
		]);
		let nested = values.clone();
		values.push(Value::Array(Array::from(nested.clone())));
		values.push(Value::Object(Object::from(
			nested
				.into_iter()
				.enumerate()
				.map(|(i, v)| (format!("k{i}"), v))
				.collect::<BTreeMap<_, _>>(),
		)));
		// Outside the nested containers, which `cbor` would then refuse whole.
		values.push(Value::Regex("^a/b[0-9]+$".parse().unwrap()));
		values
	}

	/// A small deterministic generator, so a failure names its seed.
	struct XorShift(u64);

	impl XorShift {
		fn next(&mut self) -> u64 {
			self.0 ^= self.0 << 13;
			self.0 ^= self.0 >> 7;
			self.0 ^= self.0 << 17;
			self.0
		}
	}

	#[test]
	fn encoded_values_decode_as_the_tree_decodes_them() {
		for value in corpus() {
			for bytes in [encode(value.clone()), encode_v2(value.clone())].into_iter().flatten() {
				for limit in [0, 1, 2, 3, 4, 512] {
					agree(&bytes, limit);
				}
				let mut trailing = bytes.clone();
				trailing.push(0);
				agree(&trailing, 512);
			}
		}
	}

	#[test]
	fn map_entries_split_a_map_at_its_values() {
		for value in corpus() {
			let mut map = BTreeMap::new();
			map.insert("a".to_owned(), value);
			map.insert("b".to_owned(), Value::Number(Number::Int(1)));
			let bytes = encode_v2(Value::Object(Object::from(map))).unwrap();
			let (entries, len) = super::super::map_entries(&bytes, 512).expect("a map");
			assert_eq!(len, bytes.len());
			assert_eq!(entries.iter().map(|(k, _)| *k).collect::<Vec<_>>(), ["a", "b"]);
			let Value::Object(whole) = decode_exact_v2(&bytes, 512, None).unwrap() else {
				panic!("a map decodes as an object");
			};
			let a = decode_exact_v2(entries[0].1, 512, None).unwrap();
			assert_eq!(format!("{a:?}"), format!("{:?}", whole["a"]));
			assert_eq!(
				decode_exact_v2(entries[1].1, 512, None).unwrap(),
				Value::Number(Number::Int(1))
			);
			// Every prefix is refused rather than read past.
			for cut in 0..bytes.len() {
				assert!(
					super::super::map_entries(&bytes[..cut], 512).is_none_or(|(_, l)| l <= cut)
				);
			}
		}
	}

	#[test]
	fn mutated_bytes_decode_as_the_tree_decodes_them() {
		let mut rng = XorShift(0x9e37_79b9_7f4a_7c15);
		for value in corpus() {
			for bytes in [encode(value.clone()), encode_v2(value)].into_iter().flatten() {
				for _ in 0..300 {
					let mut mutated = bytes.clone();
					match rng.next() % 4 {
						0 if !mutated.is_empty() => {
							let at = rng.next() as usize % mutated.len();
							mutated[at] = rng.next() as u8;
						}
						1 if !mutated.is_empty() => {
							let at = rng.next() as usize % mutated.len();
							mutated.truncate(at);
						}
						2 => {
							let at = rng.next() as usize % (mutated.len() + 1);
							mutated.insert(at, rng.next() as u8);
						}
						_ if !mutated.is_empty() => {
							let at = rng.next() as usize % mutated.len();
							mutated[at] ^= 1 << (rng.next() % 8);
						}
						_ => {}
					}
					agree(&mutated, 512);
					agree(&mutated, 3);
					if let Some((_, len)) = super::super::map_entries(&mutated, 512) {
						assert!(len <= mutated.len());
					}
				}
			}
		}
	}

	#[test]
	fn unusual_forms_decode_as_the_tree_decodes_them() {
		let cases: &[&[u8]] = &[
			// Indefinite-length array, map, text and bytes.
			&[0x9f, 0x01, 0x02, 0xff],
			&[0xbf, 0x61, b'a', 0x01, 0xff],
			&[0x7f, 0x61, b'a', 0x61, b'b', 0xff],
			&[0x5f, 0x41, 0x01, 0xff],
			// `undefined`, alone and under the NONE tag.
			&[0xf7],
			&[0xc6, 0xf7],
			&[0xc6, 0x01],
			// Bignums.
			&[0xc2, 0x41, 0x01],
			&[0xc3, 0x49, 0x01, 0, 0, 0, 0, 0, 0, 0, 0],
			// Integers past `i64`.
			&[0x1b, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
			&[0x3b, 0x80, 0, 0, 0, 0, 0, 0, 0],
			&[0x3b, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
			// Keys out of order, a duplicate key, and a key that is not text.
			&[0xa2, 0x61, b'b', 0x01, 0x61, b'a', 0x02],
			&[0xa3, 0x61, b'a', 0x01, 0x61, b'b', 0x02, 0x61, b'a', 0x03],
			&[0xa2, 0x61, b'a', 0x01, 0x61, b'a', 0x02],
			&[0xa1, 0x01, 0x02],
			// A record id written as text, and one with three parts.
			&[0xc8, 0x63, b'a', b':', b'b'],
			&[0xc8, 0x83, 0x61, b't', 0x01, 0x02],
			// A datetime with negative nanoseconds, and a duration with a text part.
			&[0xcc, 0x82, 0x01, 0x20],
			&[0xce, 0x82, 0x61, b'x', 0x01],
			&[0xce, 0x81, 0x05],
			&[0xce, 0x80],
			&[0xce, 0x83, 0x01, 0x02, 0x03],
			// Sized numbers out of range, and under the wrong item.
			&[0xd8, 0xb4, 0x18, 0xff],
			&[0xd8, 0xbc, 0xfb, 0x3f, 0xb9, 0x99, 0x99, 0x99, 0x99, 0x99, 0x9a],
			&[0xd8, 0xbc, 0x01],
			// A uuid of the wrong length, invalid UTF-8, an unknown tag, a simple value, a break.
			&[0xd8, 0x25, 0x41, 0x01],
			&[0x62, 0xc3, 0x28],
			&[0xd8, 0x63, 0x01],
			&[0xf0],
			&[0xff],
			&[0x1c],
			&[],
			// A head that claims more than the input holds.
			&[0x9b, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
			&[0x5b, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
		];
		for bytes in cases {
			agree(bytes, 512);
			agree(bytes, 1);
		}
		// Nesting at and past the limit.
		for depth in 0..6 {
			let mut bytes = vec![0x81; depth];
			bytes.push(0x01);
			for limit in 0..6 {
				agree(&bytes, limit);
			}
			let mut tagged = vec![0xc8, 0x82, 0x61, b't'];
			tagged.extend(vec![0x81; depth]);
			tagged.push(0x01);
			for limit in 0..6 {
				agree(&tagged, limit);
			}
		}
	}
}
