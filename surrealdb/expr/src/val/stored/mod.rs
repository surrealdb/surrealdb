//! The persisted encoding of [`Value`].
//!
//! Record data in the v1 record envelope (`surrealdb_catalog::StoredRecord`) is stored in this
//! grammar; a record whose metadata the envelope cannot hold keeps its data in `revision`. A
//! stored value is read two ways: decoded into a [`Value`] ([`decode`]), or read in place without
//! allocating ([`ValueRef`]) — a field looked up by name, a scalar compared, a row transcoded.
//!
//! # Grammar
//!
//! A value is a tag byte followed by its payload. No value states its own
//! length: its extent is always known from where it sits, so a reader can step
//! over a value whose tag it does not recognise, and the grammar can gain value
//! kinds without changing the encoding of the kinds it has. A value's extent is
//!
//! * for an entry of a container, the span from its offset to the next entry's offset, or to the
//!   end of the container for the last entry;
//! * for a record's data, a record id's key and a range's end bound, the rest of the enclosing
//!   extent;
//! * for a range's start bound, a `clen` length prefix.
//!
//! ```text
//! tag    kind       payload, running to the end of the value's extent
//! 00     NONE
//! 01     NULL
//! 02     FALSE
//! 03     TRUE
//! 04     INT        1..=8 bytes, little-endian two's complement, the fewest that hold it
//! 05     FLOAT      4 bytes when an f32 holds the value exactly, else 8
//! 06     DECIMAL    sign << 7 | scale, then the magnitude of the mantissa,
//!                   little-endian, the fewest bytes that hold it (none for zero)
//! 07     STRING     UTF-8
//! 08     DURATION   u64 nanoseconds when they fit, else u64 seconds, u32 nanoseconds
//! 09     DATETIME   i64 nanoseconds from the Unix epoch when they fit,
//!                   else i64 seconds, u32 nanoseconds
//! 0A     UUID       16 bytes
//! 0B     BYTES
//! 0C     TABLE      UTF-8 table name
//! 0D     RECORD ID  clen table name, then the key as a value: an INT, STRING,
//!                   UUID, array, OBJECT, or a RANGE whose bounds are keys
//! 0E     FILE       clen bucket, then the UTF-8 key
//! 0F     REGEX      UTF-8 pattern
//! 10     RANGE      start bound, end bound
//! 11     GEOMETRY   see below
//! 12     F64 ARRAY  n × f64: a non-empty array of floats
//! 13     F32 ARRAY  n × f32: a non-empty array of floats each held exactly by an f32
//! 14-16  ARRAY      container of values, offset width 1, 2, 4
//! 17-19  SET        container of values, unique, ascending in value order
//! 1A-1C  OBJECT     container of entries `clen key, value`, keys unique and
//!                   ascending in byte order
//! 1D-20  INT ARRAY  n × i8, i16, i32 or i64, little-endian: a non-empty array of
//!                   integers, in the narrowest of 1, 2, 4 and 8 bytes that
//!                   holds every element
//! 21-25  reserved   arrays of future u8, u16, u32, f16 and bf16 value kinds
//! 26-28  MAP        schema-only container of entries `number, item`, offset width 1, 2, 4
//! 80-FE  INT        the integer tag − 0x80, 0 to 126
//! others reserved
//!
//! container = n, n offsets, entries; n and each offset are w bytes,
//!             little-endian; offsets count from the first entry
//! bound     = 00 unbounded | 01 included, value | 02 excluded, value
//!             (a start bound's value is clen-prefixed; an end bound's runs to
//!             the end of the range)
//! clen      = one byte below 255, else 0xFF then a u32
//!
//! map       = a container whose entries are `number, item`
//! number    = a canonical unsigned LEB128, 1 to u32::MAX, strictly ascending within a map
//! item      = a value, a map, or a list: an ARRAY container whose elements are items
//!
//! geometry  = 00 point x, y
//!           | 01 line points
//!           | 02 polygon u32 n, n × line          (the exterior ring first)
//!           | 03 multipoint points
//!           | 04 multiline u32 n, n × line
//!           | 05 multipolygon u32 n, n × (u32 m, m × line)
//!           | 06 collection u32 n, n × (clen, geometry)
//!   points  = n × (x f64, y f64), n from the extent
//!   line    = u32 n, n × (x f64, y f64)
//! ```
//!
//! A MAP is a schema's container: a schema declares what each number means, so the grammar
//! carries no names. MAP is schema-only. The generic decoder refuses it as an unknown tag, a
//! [`Value`] never contains one, and an ARRAY that holds MAPs is read only by a schema. A schema's
//! items may hold generic values. Every item is at least one byte, its tag, so an entry with
//! nothing after its number is malformed. [`MapRef`] and [`ListRef`] read the schema containers.
//!
//! # Canonical form
//!
//! [`Encoder`] writes exactly one byte string for a value: the narrowest int, an
//! f32 whenever it is exact, nanosecond times whenever they fit, the narrowest
//! offset width (the smallest of 1, 2 and 4 bytes that holds both the entry
//! count and the entries' total length), a typed float array for every
//! non-empty array of floats, and a typed integer array for every non-empty
//! array of integers. An array that mixes integers with anything else, floats
//! included, is a generic ARRAY; sets are always containers. Two identical
//! values therefore encode to identical bytes. Values that are equal without
//! being identical — `1` and `1.0` — do not.
//!
//! These forms are permanent. An all-integer array is an INT ARRAY in every
//! release, and a reserved tag is never another encoding of an integer or float
//! array: a future value kind takes a tag of its own. FLOAT's 4-byte form and
//! F32 ARRAY keep meaning an `f64` that an `f32` holds exactly.
//!
//! The decoder accepts any width that holds its value, and so reads a typed
//! array wider than its elements need and an untyped array of integers. It
//! rejects what would break a reader: object keys out of order or repeated, an offset outside its
//! container, invalid UTF-8. Set elements out of order or repeated are sorted
//! and deduplicated rather than refused, because value order belongs to the
//! code, not to the bytes.

mod decode;
mod encode;
mod geometry;
mod map;
mod read;
pub mod schema;
#[cfg(test)]
mod tests;
pub mod varint;

pub use decode::{
	decimal_payload, decode, decode_datetime_payload, decode_duration_payload, decode_key,
	decode_key_nested, decode_nested, validate, validate_entries_nested, validate_key_nested,
	validate_nested,
};
pub use encode::{Encoder, Open, encode, put_decimal_payload, with_encoder};
pub use map::{Item, ListRef, MapRef};
pub use read::{ArrayIter, ArrayRef, Kind, ObjectIter, ObjectRef, OrderedObjectRef, ValueRef};

use crate::val::MAX_VALUE_DEPTH;

/// Value tags. See the module documentation for the grammar.
pub mod tag {
	pub const NONE: u8 = 0x00;
	pub const NULL: u8 = 0x01;
	pub const FALSE: u8 = 0x02;
	pub const TRUE: u8 = 0x03;
	pub const INT: u8 = 0x04;
	pub const FLOAT: u8 = 0x05;
	pub const DECIMAL: u8 = 0x06;
	pub const STRING: u8 = 0x07;
	pub const DURATION: u8 = 0x08;
	pub const DATETIME: u8 = 0x09;
	pub const UUID: u8 = 0x0A;
	pub const BYTES: u8 = 0x0B;
	pub const TABLE: u8 = 0x0C;
	pub const RECORD_ID: u8 = 0x0D;
	pub const FILE: u8 = 0x0E;
	pub const REGEX: u8 = 0x0F;
	pub const RANGE: u8 = 0x10;
	pub const GEOMETRY: u8 = 0x11;
	pub const F64_ARRAY: u8 = 0x12;
	pub const F32_ARRAY: u8 = 0x13;
	/// The array tag for offset width 1; +1 for width 2, +2 for width 4.
	pub const ARRAY: u8 = 0x14;
	/// The set tag for offset width 1; +1 for width 2, +2 for width 4.
	pub const SET: u8 = 0x17;
	/// The object tag for offset width 1; +1 for width 2, +2 for width 4.
	pub const OBJECT: u8 = 0x1A;
	/// A non-empty array of integers held as `i8`s. The wider forms follow: the element width in
	/// bytes of tag `t` is `1 << (t - I8_ARRAY)`.
	pub const I8_ARRAY: u8 = 0x1D;
	/// A non-empty array of integers held as little-endian `i16`s.
	pub const I16_ARRAY: u8 = 0x1E;
	/// A non-empty array of integers held as little-endian `i32`s.
	pub const I32_ARRAY: u8 = 0x1F;
	/// A non-empty array of integers held as little-endian `i64`s.
	pub const I64_ARRAY: u8 = 0x20;
	/// The schema-only map tag for offset width 1; +1 for width 2, +2 for width 4. Tags `0x29`
	/// and up are unassigned.
	pub const MAP: u8 = 0x26;
	/// The tag of the integer 0; the integer `n` in `0..=SMALL_INT_MAX` is
	/// `SMALL_INT + n`.
	pub const SMALL_INT: u8 = 0x80;
	/// The largest integer with a tag of its own.
	pub const SMALL_INT_MAX: i64 = 126;

	/// A range bound's kind.
	pub const UNBOUNDED: u8 = 0;
	pub const INCLUDED: u8 = 1;
	pub const EXCLUDED: u8 = 2;

	/// A geometry's kind, the first byte of its payload.
	pub const GEO_POINT: u8 = 0;
	pub const GEO_LINE: u8 = 1;
	pub const GEO_POLYGON: u8 = 2;
	pub const GEO_MULTIPOINT: u8 = 3;
	pub const GEO_MULTILINE: u8 = 4;
	pub const GEO_MULTIPOLYGON: u8 = 5;
	pub const GEO_COLLECTION: u8 = 6;

	/// Tags reserved for typed arrays of value kinds that do not exist yet: each names an array
	/// of that element kind and nothing else. A reserved tag is never another encoding of an
	/// integer or float array and is never reused for anything else; a reader refuses it as an
	/// unknown tag until a release defines it.
	pub mod reserved {
		/// An array of unsigned 8-bit integers.
		pub const U8_ARRAY: u8 = 0x21;
		/// An array of unsigned 16-bit integers.
		pub const U16_ARRAY: u8 = 0x22;
		/// An array of unsigned 32-bit integers.
		pub const U32_ARRAY: u8 = 0x23;
		/// An array of IEEE 754 half-precision floats.
		pub const F16_ARRAY: u8 = 0x24;
		/// An array of bfloat16 floats.
		pub const BF16_ARRAY: u8 = 0x25;
	}

	/// The offset width a container tag declares, given the tag of its
	/// width-1 form.
	#[inline(always)]
	pub const fn width(tag: u8, base: u8) -> usize {
		1 << (tag - base)
	}
}

/// How deeply decoding descends before it refuses. Values are written only
/// after [`crate::val::check_value_depth`] admits them; the margin covers the
/// levels a record id key and a geometry collection add on top.
pub const DEPTH_LIMIT: usize = MAX_VALUE_DEPTH + 16;

/// Why stored bytes could not be read, or a value could not be stored.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Error {
	#[error("a stored value ends before its encoding does")]
	Truncated,
	#[error("a stored value has the unknown tag {0:#04x}")]
	UnknownTag(u8),
	#[error("a stored value is malformed: {0}")]
	Malformed(&'static str),
	#[error("a stored value nests deeper than {0} levels")]
	TooDeep(usize),
	#[error("the value cannot be stored: {0}")]
	Unstorable(&'static str),
	/// A closure is a value only while a query runs, and has no stored form.
	#[error("a closure cannot be stored")]
	Closure,
}

/// Reads a `clen` length prefix: the length, and the bytes the prefix took.
#[inline(always)]
pub fn read_clen(b: &[u8]) -> Result<(usize, usize), Error> {
	match b.first() {
		Some(&n) if n < 0xFF => Ok((n as usize, 1)),
		Some(_) => match b.get(1..).and_then(<[u8]>::first_chunk::<4>) {
			Some(n) => Ok((u32::from_le_bytes(*n) as usize, 5)),
			None => Err(Error::Truncated),
		},
		None => Err(Error::Truncated),
	}
}

/// Splits a `clen`-prefixed byte string off the front of `b`: the string, and
/// everything after it.
#[inline(always)]
pub fn split_clen(b: &[u8]) -> Result<(&[u8], &[u8]), Error> {
	let (n, used) = read_clen(b)?;
	// `used + n` can wrap where `usize` is 32 bits.
	let end = used.checked_add(n).ok_or(Error::Truncated)?;
	let s = b.get(used..end).ok_or(Error::Truncated)?;
	let rest = b.get(end..).ok_or(Error::Truncated)?;
	Ok((s, rest))
}

#[inline(always)]
pub(crate) fn utf8(b: &[u8]) -> Result<&str, Error> {
	std::str::from_utf8(b).map_err(|_| Error::Malformed("a string is not valid UTF-8"))
}

/// A little-endian unsigned integer of width `w` (1, 2 or 4) at `at`.
#[inline(always)]
pub(crate) fn read_width(b: &[u8], at: usize, w: usize) -> Option<usize> {
	Some(match w {
		1 => *b.get(at)? as usize,
		2 => u16::from_le_bytes(b.get(at..at.checked_add(2)?)?.try_into().ok()?) as usize,
		_ => u32::from_le_bytes(b.get(at..at.checked_add(4)?)?.try_into().ok()?) as usize,
	})
}

/// A little-endian two's complement integer of 1 to 8 bytes.
///
/// Accepts any width from 1 to 8 bytes, not only the fewest that hold the
/// value: an element of a typed integer array is read through it at its
/// array's width, so it must never insist on the minimal width.
#[inline(always)]
pub(crate) fn read_int(p: &[u8]) -> Result<i64, Error> {
	let wrong = Error::Malformed("an integer of the wrong width");
	let last = p.last().ok_or(wrong)?;
	let fill = if last & 0x80 != 0 {
		0xFF
	} else {
		0
	};
	let mut b = [fill; 8];
	b.get_mut(..p.len()).ok_or(wrong)?.copy_from_slice(p);
	Ok(i64::from_le_bytes(b))
}

/// The number of bytes [`read_int`] needs for `v`.
#[inline(always)]
pub(crate) const fn int_width(v: i64) -> usize {
	((65 - (v ^ (v >> 63)).leading_zeros()) as usize).div_ceil(8)
}
