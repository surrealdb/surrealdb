use std::ops::Bound;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use surrealdb_collections::{VecMap, VecSet};
use surrealdb_strand::Strand;

use super::read::{Container, Kind};
use super::tag::*;
use super::{DEPTH_LIMIT, Error, geometry, read_int, split_clen, utf8};
use crate::val::{
	Array, Bytes, Datetime, Duration, File, Number, Object, Range, RecordId, RecordIdKey,
	RecordIdKeyRange, Regex, Set, TableName, Uuid, Value,
};

/// What a walk of the stored grammar produces. [`Build`] constructs each value it reads; [`Check`]
/// reads the same bytes through the same code and produces nothing. Every byte-level check runs in
/// both modes, so the two accept and refuse the same bytes with the same errors.
///
/// Walked values are `Self::Out<T>` and sequences of them are `Self::Seq<T>`. Generic code never
/// looks inside an `Out`: it combines walked values only through these methods, and the closures
/// passed to them run only in [`Build`].
pub(super) trait Mode {
	/// A walked `T`: the value itself in [`Build`], nothing in [`Check`].
	type Out<T>;
	/// A sequence of walked `T`s: a `Vec<T>` in [`Build`], nothing in [`Check`].
	type Seq<T>;
	/// Wraps the value `f` constructs. `f` is not called in [`Check`].
	fn make<T>(f: impl FnOnce() -> T) -> Self::Out<T>;
	/// Applies `f` to a walked value. `f` is not called in [`Check`].
	fn map<T, U>(v: Self::Out<T>, f: impl FnOnce(T) -> U) -> Self::Out<U>;
	/// Pairs two walked values.
	fn zip<A, B>(a: Self::Out<A>, b: Self::Out<B>) -> Self::Out<(A, B)>;
	/// An empty sequence with room for `capacity` items.
	fn seq<T>(capacity: usize) -> Self::Seq<T>;
	/// Appends a walked item to a sequence.
	fn push<T>(seq: &mut Self::Seq<T>, item: Self::Out<T>);
	/// Collects a sequence of walked items.
	fn finish<T>(seq: Self::Seq<T>) -> Self::Out<Vec<T>>;
}

/// The walk that constructs the values it reads.
pub(super) enum Build {}

/// The walk that reads and checks the bytes and constructs nothing, so it allocates nothing for
/// the kinds whose check does not need their value.
pub(super) enum Check {}

impl Mode for Build {
	type Out<T> = T;
	type Seq<T> = Vec<T>;

	#[inline(always)]
	fn make<T>(f: impl FnOnce() -> T) -> T {
		f()
	}

	#[inline(always)]
	fn map<T, U>(v: T, f: impl FnOnce(T) -> U) -> U {
		f(v)
	}

	#[inline(always)]
	fn zip<A, B>(a: A, b: B) -> (A, B) {
		(a, b)
	}

	#[inline(always)]
	fn seq<T>(capacity: usize) -> Vec<T> {
		Vec::with_capacity(capacity)
	}

	#[inline(always)]
	fn push<T>(seq: &mut Vec<T>, item: T) {
		seq.push(item);
	}

	#[inline(always)]
	fn finish<T>(seq: Vec<T>) -> Vec<T> {
		seq
	}
}

impl Mode for Check {
	type Out<T> = ();
	type Seq<T> = ();

	#[inline(always)]
	fn make<T>(_: impl FnOnce() -> T) {}

	#[inline(always)]
	fn map<T, U>(_: (), _: impl FnOnce(T) -> U) {}

	#[inline(always)]
	fn zip<A, B>(_: (), _: ()) {}

	#[inline(always)]
	fn seq<T>(_: usize) {}

	#[inline(always)]
	fn push<T>(_: &mut (), _: ()) {}

	#[inline(always)]
	fn finish<T>(_: ()) {}
}

/// Decodes the value whose stored encoding is exactly `bytes`.
pub fn decode(bytes: &[u8]) -> Result<Value, Error> {
	decode_nested(bytes, 0)
}

/// Decodes the value whose stored encoding is exactly `bytes`, as a value nested `depth`
/// containers deep, so the grammar's depth limit counts the levels above it. An encode and a
/// decode at the same depth accept the same values.
pub fn decode_nested(bytes: &[u8], depth: usize) -> Result<Value, Error> {
	value::<Build>(bytes, depth)
}

/// Whether `bytes` are exactly one value of the stored grammar. Refuses what [`decode`] refuses,
/// with the same error. It builds no value except where a check needs one: a set, a regex or a
/// geometry is constructed to be checked.
pub fn validate(bytes: &[u8]) -> Result<(), Error> {
	validate_nested(bytes, 0)
}

/// [`validate`] for a value nested `depth` containers deep, as [`decode_nested`] counts depth.
pub fn validate_nested(bytes: &[u8], depth: usize) -> Result<(), Error> {
	value::<Check>(bytes, depth)
}

/// Decodes the record id key whose stored encoding is exactly `bytes`.
pub fn decode_key(bytes: &[u8]) -> Result<RecordIdKey, Error> {
	decode_key_nested(bytes, 0)
}

/// Decodes the record id key whose stored encoding is exactly `bytes`, nested `depth` containers
/// deep.
pub fn decode_key_nested(bytes: &[u8], depth: usize) -> Result<RecordIdKey, Error> {
	key::<Build>(bytes, depth)
}

/// Reads a DECIMAL payload (the head byte and the magnitude, without the tag) that is exactly
/// `p`.
///
/// Only the payload [`put_decimal_payload`](super::put_decimal_payload) writes is accepted: a
/// magnitude whose last, most significant byte is zero is refused, so every decimal has one
/// payload.
pub fn decimal_payload(p: &[u8]) -> Result<Decimal, Error> {
	let (_, magnitude) = p.split_first().ok_or(Error::Truncated)?;
	if magnitude.last() == Some(&0) {
		return Err(Error::Malformed("a decimal magnitude with a zero high byte"));
	}
	decimal(p)
}

/// Reads the DATETIME payload `p` that is exactly 12 bytes: `i64` little-endian seconds, then
/// `u32` little-endian nanoseconds. A value outside the representable range is refused.
pub fn decode_datetime_payload(p: &[u8]) -> Result<DateTime<Utc>, Error> {
	if p.len() != 12 {
		return Err(Error::Malformed("a datetime payload that is not 12 bytes"));
	}
	datetime(p)
}

/// Reads the DURATION payload `p` that is exactly 12 bytes: `u64` little-endian seconds, then
/// `u32` little-endian nanoseconds below a second.
pub fn decode_duration_payload(p: &[u8]) -> Result<std::time::Duration, Error> {
	if p.len() != 12 {
		return Err(Error::Malformed("a duration payload that is not 12 bytes"));
	}
	duration(p)
}

#[inline(always)]
pub(super) fn fixed<const N: usize>(p: &[u8]) -> Result<[u8; N], Error> {
	p.try_into().map_err(|_| Error::Malformed("a fixed-width value of the wrong width"))
}

#[inline(always)]
fn empty(p: &[u8]) -> Result<(), Error> {
	match p.is_empty() {
		true => Ok(()),
		false => Err(Error::Malformed("a payload on a value that has none")),
	}
}

#[inline]
pub(super) fn float(p: &[u8]) -> Result<f64, Error> {
	match p.len() {
		4 => Ok(f32::from_le_bytes(fixed(p)?) as f64),
		8 => Ok(f64::from_le_bytes(fixed(p)?)),
		_ => Err(Error::Malformed("a float of the wrong width")),
	}
}

#[inline]
pub(super) fn decimal(p: &[u8]) -> Result<Decimal, Error> {
	let (&head, magnitude) = p.split_first().ok_or(Error::Truncated)?;
	if magnitude.len() > 12 {
		return Err(Error::Malformed("a decimal mantissa wider than 96 bits"));
	}
	let mut m = [0u8; 16];
	m.get_mut(..magnitude.len())
		.ok_or(Error::Malformed("a decimal mantissa wider than 96 bits"))?
		.copy_from_slice(magnitude);
	let mut d =
		Decimal::try_from_i128_with_scale(u128::from_le_bytes(m) as i128, (head & 0x7F) as u32)
			.map_err(|_| Error::Malformed("a decimal out of range"))?;
	d.set_sign_negative(head & 0x80 != 0);
	Ok(d)
}

#[inline]
pub(super) fn duration(p: &[u8]) -> Result<std::time::Duration, Error> {
	match p.len() {
		8 => Ok(std::time::Duration::from_nanos(u64::from_le_bytes(fixed(p)?))),
		12 => {
			let (secs, nanos) = p.split_at_checked(8).ok_or(Error::Truncated)?;
			let secs = u64::from_le_bytes(fixed(secs)?);
			let nanos = u32::from_le_bytes(fixed(nanos)?);
			if nanos >= 1_000_000_000 {
				return Err(Error::Malformed("a duration with a nanosecond part past a second"));
			}
			Ok(std::time::Duration::new(secs, nanos))
		}
		_ => Err(Error::Malformed("a duration of the wrong width")),
	}
}

#[inline]
pub(super) fn datetime(p: &[u8]) -> Result<DateTime<Utc>, Error> {
	match p.len() {
		8 => Ok(DateTime::from_timestamp_nanos(i64::from_le_bytes(fixed(p)?))),
		12 => {
			let (secs, nanos) = p.split_at_checked(8).ok_or(Error::Truncated)?;
			let secs = i64::from_le_bytes(fixed(secs)?);
			let nanos = u32::from_le_bytes(fixed(nanos)?);
			DateTime::from_timestamp(secs, nanos).ok_or(Error::Malformed("a datetime out of range"))
		}
		_ => Err(Error::Malformed("a datetime of the wrong width")),
	}
}

fn value<M: Mode>(v: &[u8], depth: usize) -> Result<M::Out<Value>, Error> {
	let (&t, p) = v.split_first().ok_or(Error::Truncated)?;
	parts::<M>(t, p, depth)
}

/// Walks the value with tag `t` and payload `p`.
pub(super) fn parts<M: Mode>(t: u8, p: &[u8], depth: usize) -> Result<M::Out<Value>, Error> {
	if depth > DEPTH_LIMIT {
		return Err(Error::TooDeep(DEPTH_LIMIT));
	}
	Ok(match t {
		NONE => {
			empty(p)?;
			M::make(|| Value::None)
		}
		NULL => {
			empty(p)?;
			M::make(|| Value::Null)
		}
		FALSE => {
			empty(p)?;
			M::make(|| Value::Bool(false))
		}
		TRUE => {
			empty(p)?;
			M::make(|| Value::Bool(true))
		}
		INT => {
			let i = read_int(p)?;
			M::make(|| Value::Number(Number::Int(i)))
		}
		FLOAT => {
			let f = float(p)?;
			M::make(|| Value::Number(Number::Float(f)))
		}
		DECIMAL => {
			let d = decimal(p)?;
			M::make(|| Value::Number(Number::Decimal(d)))
		}
		STRING => {
			let s = utf8(p)?;
			M::make(|| Value::String(Strand::new(s)))
		}
		DURATION => {
			let d = duration(p)?;
			M::make(|| Value::Duration(Duration(d)))
		}
		DATETIME => {
			let d = datetime(p)?;
			M::make(|| Value::Datetime(Datetime(d)))
		}
		UUID => {
			let u: [u8; 16] = fixed(p)?;
			M::make(|| Value::Uuid(Uuid(uuid::Uuid::from_bytes(u))))
		}
		BYTES => M::make(|| Value::Bytes(Bytes(::bytes::Bytes::copy_from_slice(p)))),
		TABLE => {
			let s = utf8(p)?;
			M::make(|| Value::Table(TableName::new(s)))
		}
		RECORD_ID => {
			let (table, k) = split_clen(p)?;
			let table = utf8(table)?;
			let key = key::<M>(k, depth + 1)?;
			M::map(key, |key: RecordIdKey| {
				Value::RecordId(RecordId {
					table: TableName::new(table),
					key,
				})
			})
		}
		FILE => {
			let (bucket, k) = split_clen(p)?;
			let bucket = utf8(bucket)?;
			let key = utf8(k)?;
			// Built field by field: `File::new` prepends a `/` to a key without one.
			M::make(|| {
				Value::File(File {
					bucket: bucket.to_owned(),
					key: key.to_owned(),
				})
			})
		}
		REGEX => {
			let r = regex(utf8(p)?)?;
			M::make(|| Value::Regex(r))
		}
		RANGE => {
			let b = bounds::<M, Value, _>(p, |b| value::<M>(b, depth + 1))?;
			M::map(b, |(start, end): (Bound<Value>, Bound<Value>)| {
				Value::Range(Box::new(Range {
					start,
					end,
				}))
			})
		}
		GEOMETRY => {
			let g = geometry::decode(p, depth)?;
			M::make(|| Value::Geometry(g))
		}
		F64_ARRAY | F32_ARRAY | I8_ARRAY..=I64_ARRAY | 0x14..=0x16 => {
			M::map(array::<M>(t, p, depth)?, Value::Array)
		}
		0x17..=0x19 => M::map(set::<M>(p, width(t, SET), depth)?, Value::Set),
		0x1A..=0x1C => M::map(object::<M>(p, width(t, OBJECT), depth)?, Value::Object),
		0x80..=0xFE => {
			empty(p)?;
			let i = (t - SMALL_INT) as i64;
			M::make(|| Value::Number(Number::Int(i)))
		}
		other => return Err(Error::UnknownTag(other)),
	})
}

/// Scalar accessors for readers that treat a value's tag as part of a declared schema.
///
/// Each takes a value's tag and payload and applies exactly the payload checks of [`parts`]: no
/// payload at all on NONE, FALSE, TRUE and the small ints, the widths of the fixed-width kinds, a
/// whole UTF-8 string. `Ok(None)` means the tag is another kind, so the caller can report the wrong
/// kind; an `Err` means the tag is this kind and its payload is malformed. They read a tag and
/// payload split by the caller, so they check for themselves what
/// [`ValueRef::new`](super::read::ValueRef::new) checks of a value it is handed.
pub(crate) mod strict {
	use chrono::{DateTime, Utc};
	use rust_decimal::Decimal;

	use super::{empty, fixed};
	use crate::val::stored::tag::*;
	use crate::val::stored::{Error, read_int, utf8};

	/// NONE, whose payload is empty.
	#[inline(always)]
	pub(crate) fn none(t: u8, p: &[u8]) -> Result<Option<()>, Error> {
		match t {
			NONE => empty(p).map(Some),
			_ => Ok(None),
		}
	}

	/// FALSE or TRUE, whose payload is empty.
	#[inline(always)]
	pub(crate) fn bool(t: u8, p: &[u8]) -> Result<Option<bool>, Error> {
		match t {
			FALSE => empty(p).map(|()| Some(false)),
			TRUE => empty(p).map(|()| Some(true)),
			_ => Ok(None),
		}
	}

	/// An INT of 1 to 8 bytes, or a small int, whose payload is empty.
	#[inline(always)]
	pub(crate) fn int(t: u8, p: &[u8]) -> Result<Option<i64>, Error> {
		match t {
			INT => read_int(p).map(Some),
			0x80..=0xFE => empty(p).map(|()| Some(i64::from(t - SMALL_INT))),
			_ => Ok(None),
		}
	}

	/// A FLOAT of 4 or 8 bytes.
	#[inline(always)]
	pub(crate) fn float(t: u8, p: &[u8]) -> Result<Option<f64>, Error> {
		match t {
			FLOAT => super::float(p).map(Some),
			_ => Ok(None),
		}
	}

	/// A DECIMAL.
	#[inline]
	pub(crate) fn decimal(t: u8, p: &[u8]) -> Result<Option<Decimal>, Error> {
		match t {
			DECIMAL => super::decimal(p).map(Some),
			_ => Ok(None),
		}
	}

	/// A STRING of valid UTF-8.
	#[inline(always)]
	pub(crate) fn string(t: u8, p: &[u8]) -> Result<Option<&str>, Error> {
		match t {
			STRING => utf8(p).map(Some),
			_ => Ok(None),
		}
	}

	/// A UUID of 16 bytes.
	#[inline(always)]
	pub(crate) fn uuid(t: u8, p: &[u8]) -> Result<Option<uuid::Uuid>, Error> {
		match t {
			UUID => Ok(Some(uuid::Uuid::from_bytes(fixed(p)?))),
			_ => Ok(None),
		}
	}

	/// A DURATION in either of its two widths.
	#[inline]
	pub(crate) fn duration(t: u8, p: &[u8]) -> Result<Option<std::time::Duration>, Error> {
		match t {
			DURATION => super::duration(p).map(Some),
			_ => Ok(None),
		}
	}

	/// A DATETIME in either of its two widths.
	#[inline]
	pub(crate) fn datetime(t: u8, p: &[u8]) -> Result<Option<DateTime<Utc>>, Error> {
		match t {
			DATETIME => super::datetime(p).map(Some),
			_ => Ok(None),
		}
	}
}

/// Compiles a stored pattern exactly as written. A pattern that compiled when it
/// was stored stays readable when the configured size limit has since been
/// lowered: past the limit it compiles with the regex engine's default.
fn regex(s: &str) -> Result<Regex, Error> {
	crate::val::regex::regex_new(s)
		.or_else(|_| surrealdb_types::regex_engine::compile(s, None))
		.map(Regex)
		.map_err(|_| Error::Malformed("a regex that does not compile"))
}

/// The element width in bytes of the integer array with tag `t` (one of
/// `I8_ARRAY..=I64_ARRAY`) and payload `p`.
///
/// An integer array is never empty and holds a whole number of elements; the
/// element count is `p.len() / width`.
#[inline(always)]
pub(super) fn int_array_element_width(t: u8, p: &[u8]) -> Result<usize, Error> {
	let w = width(t, I8_ARRAY);
	match p.is_empty() || !p.len().is_multiple_of(w) {
		true => Err(Error::Malformed("an integer array of the wrong width")),
		false => Ok(w),
	}
}

/// The elements of an integer array whose payload is a whole number of `W`-byte elements.
#[inline]
fn ints<const W: usize>(p: &[u8], from: fn([u8; W]) -> i64) -> Vec<Value> {
	p.as_chunks::<W>().0.iter().map(|c| Value::Number(Number::Int(from(*c)))).collect()
}

/// Walks the array with tag `t` (a typed array or a container) and payload `p`.
fn array<M: Mode>(t: u8, p: &[u8], depth: usize) -> Result<M::Out<Array>, Error> {
	match t {
		I8_ARRAY..=I64_ARRAY => {
			let w = int_array_element_width(t, p)?;
			Ok(match w {
				1 => M::make(|| Array(ints::<1>(p, |c| i8::from_le_bytes(c) as i64))),
				2 => M::make(|| Array(ints::<2>(p, |c| i16::from_le_bytes(c) as i64))),
				4 => M::make(|| Array(ints::<4>(p, |c| i32::from_le_bytes(c) as i64))),
				_ => M::make(|| Array(ints::<8>(p, i64::from_le_bytes))),
			})
		}
		F64_ARRAY => {
			if p.is_empty() || !p.len().is_multiple_of(8) {
				return Err(Error::Malformed("a float array of the wrong width"));
			}
			Ok(M::make(|| {
				Array(
					p.as_chunks::<8>()
						.0
						.iter()
						.map(|c| Value::Number(Number::Float(f64::from_le_bytes(*c))))
						.collect(),
				)
			}))
		}
		F32_ARRAY => {
			if p.is_empty() || !p.len().is_multiple_of(4) {
				return Err(Error::Malformed("a float array of the wrong width"));
			}
			Ok(M::make(|| {
				Array(
					p.as_chunks::<4>()
						.0
						.iter()
						.map(|c| Value::Number(Number::Float(f32::from_le_bytes(*c) as f64)))
						.collect(),
				)
			}))
		}
		0x14..=0x16 => Ok(M::map(items::<M>(p, width(t, ARRAY), depth)?, Array)),
		other => Err(Error::UnknownTag(other)),
	}
}

/// Walks the entries of a container of values, in order.
fn items<M: Mode>(p: &[u8], w: usize, depth: usize) -> Result<M::Out<Vec<Value>>, Error> {
	let c = Container::split(p, w)?;
	let mut out = M::seq::<Value>(c.n);
	for i in 0..c.n {
		M::push::<Value>(&mut out, value::<M>(c.entry(i)?, depth + 1)?);
	}
	Ok(M::finish::<Value>(out))
}

/// Walks the set with tag `t`'s container payload `p`.
fn set<M: Mode>(p: &[u8], w: usize, depth: usize) -> Result<M::Out<Set>, Error> {
	let s = set_value(p, w, depth)?;
	Ok(M::make(|| s))
}

/// Builds the set in `p`. Set elements are checked by building them, so both walks use this.
fn set_value(p: &[u8], w: usize, depth: usize) -> Result<Set, Error> {
	let v = items::<Build>(p, w, depth)?;
	// A set is ordered by `Value`'s ordering, which, unlike byte order, belongs
	// to the code rather than to the bytes. Elements are taken in that order:
	// out-of-order elements are sorted and elements that compare equal are
	// merged, so a set written under another ordering still reads. A set that
	// no order can sort into a strictly ascending run is refused. The sort
	// cannot panic whatever the comparison does.
	let set = VecSet::try_from_vec(v)
		.ok_or(Error::Malformed("a set whose elements have no consistent order"))?;
	Ok(Set(set))
}

/// Walks the object whose container payload is `p`.
pub(super) fn object<M: Mode>(p: &[u8], w: usize, depth: usize) -> Result<M::Out<Object>, Error> {
	let c = Container::split(p, w)?;
	let mut entries = M::seq::<(Strand, Value)>(c.n);
	let mut prev: Option<&[u8]> = None;
	for i in 0..c.n {
		let (k, v) = split_clen(c.entry(i)?)?;
		if prev.is_some_and(|prev| prev >= k) {
			return Err(Error::Malformed("object keys out of order"));
		}
		prev = Some(k);
		let ks = utf8(k)?;
		let name = M::make(|| Strand::new(ks));
		let val = value::<M>(v, depth + 1)?;
		M::push::<(Strand, Value)>(&mut entries, M::zip::<Strand, Value>(name, val));
	}
	Ok(M::map(M::finish::<(Strand, Value)>(entries), |entries: Vec<(Strand, Value)>| {
		Object(VecMap::from_sorted_vec_unchecked(entries))
	}))
}

/// Walks the record id key `b`, nested `depth` containers deep.
fn key<M: Mode>(b: &[u8], depth: usize) -> Result<M::Out<RecordIdKey>, Error> {
	if depth > DEPTH_LIMIT {
		return Err(Error::TooDeep(DEPTH_LIMIT));
	}
	let (&t, p) = b.split_first().ok_or(Error::Truncated)?;
	Ok(match t {
		INT => {
			let i = read_int(p)?;
			M::make(|| RecordIdKey::Number(i))
		}
		0x80..=0xFE => {
			empty(p)?;
			let i = (t - SMALL_INT) as i64;
			M::make(|| RecordIdKey::Number(i))
		}
		STRING => {
			let s = utf8(p)?;
			M::make(|| RecordIdKey::String(Strand::new(s)))
		}
		UUID => {
			let u: [u8; 16] = fixed(p)?;
			M::make(|| RecordIdKey::Uuid(Uuid(uuid::Uuid::from_bytes(u))))
		}
		F64_ARRAY | F32_ARRAY | I8_ARRAY..=I64_ARRAY | 0x14..=0x16 => {
			M::map(array::<M>(t, p, depth)?, RecordIdKey::Array)
		}
		0x1A..=0x1C => M::map(object::<M>(p, width(t, OBJECT), depth)?, RecordIdKey::Object),
		RANGE => {
			let b = bounds::<M, RecordIdKey, _>(p, |b| key::<M>(b, depth + 1))?;
			M::map(b, |(start, end): (Bound<RecordIdKey>, Bound<RecordIdKey>)| {
				RecordIdKey::Range(Box::new(RecordIdKeyRange {
					start,
					end,
				}))
			})
		}
		t if Kind::of(t) == Kind::Unknown => return Err(Error::UnknownTag(t)),
		_ => return Err(Error::Malformed("a record id key of a kind keys cannot have")),
	})
}

/// A range's start and end bounds.
type Bounds<T> = (Bound<T>, Bound<T>);

/// Walks a range's two bounds. The start bound's value is `clen`-prefixed; the end bound's runs to
/// the end of the range.
fn bounds<M: Mode, T, F>(p: &[u8], f: F) -> Result<M::Out<Bounds<T>>, Error>
where
	F: Fn(&[u8]) -> Result<M::Out<T>, Error>,
{
	let (&kind, rest) = p.split_first().ok_or(Error::Truncated)?;
	let (start, rest): (M::Out<Bound<T>>, &[u8]) = match kind {
		UNBOUNDED => (M::make(|| Bound::<T>::Unbounded), rest),
		INCLUDED | EXCLUDED => {
			let (v, rest) = split_clen(rest)?;
			let v = f(v)?;
			(
				M::map(v, |v: T| match kind {
					INCLUDED => Bound::Included(v),
					_ => Bound::Excluded(v),
				}),
				rest,
			)
		}
		_ => return Err(Error::Malformed("a range bound of an unknown kind")),
	};
	let (&kind, v) = rest.split_first().ok_or(Error::Truncated)?;
	let end: M::Out<Bound<T>> = match kind {
		UNBOUNDED => {
			empty(v)?;
			M::make(|| Bound::<T>::Unbounded)
		}
		INCLUDED => M::map(f(v)?, Bound::Included),
		EXCLUDED => M::map(f(v)?, Bound::Excluded),
		_ => return Err(Error::Malformed("a range bound of an unknown kind")),
	};
	Ok(M::zip::<Bound<T>, Bound<T>>(start, end))
}
