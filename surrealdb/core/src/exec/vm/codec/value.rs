//! `Value` and the types it is made of, read and written natively.
//!
//! A `Value` decodes to itself rather than to an object describing it, so
//! its family is transcribed here instead of as descriptors: `Value`
//! revisions 1 to 3, and each type a variant holds at every revision it
//! was written in.
//!
//! | type | revisions | framing |
//! |---|---|---|
//! | `Value` | 1, 2, 3 | 1: discriminant varint; 2 and 3: `optimised` tag, inline `None`/`Null`, fixed(1) `Bool`, every other variant `varlen`; `Date`, `Time`, `Timestamp` and `Interval` start at 3 |
//! | `Array`, `Set`, `Object` | 1, 2 | 1: length and items; 2: `optimised` struct around an indexed sequence, set or map |
//! | `Number` | 1, 2, 3 | 1: `Int`, `Float`, `Decimal`; 2 adds sized number variants; 3 adds `BigDecimal`, as its text |
//! | `RecordIdKey`, `Geometry` | 1 | plain enums |
//! | `Date`, `Time`, `Timestamp`, `Interval`, `Duration`, `Datetime`, `Uuid`, `Bytes`, `Regex`, `File`, `Range`, `RecordId`, `RecordIdKeyRange` | 1 | plain structs |
//!
//! An indexed map sorts its entries by their encoded key bytes, an indexed
//! set its elements by their encoded bytes, and both — like an indexed
//! sequence — skip the offset table below eight entries.

use std::ops::Bound;

use anyhow::{Context, Result, bail};
use rust_decimal::Decimal;
use surrealdb_strand::Strand;

use super::wire::{Reader, Writer};
use crate::val::geometry::ring_before_closing;
use crate::val::{
	Array, Bytes, Date, Datetime, Duration, File, Geometry, Interval, Number, Object, Range,
	RecordId, RecordIdKey, RecordIdKeyRange, Set, TableName, Time, Timestamp, Uuid, Value,
};

/// Below this many entries an indexed compound has no offset table.
const OFFSET_TABLE_MIN_LEN: usize = 8;
const FLAG_INDEXED: u8 = 1;
const NANOS_PER_SEC: u32 = 1_000_000_000;

fn revision(r: &mut Reader<'_>, name: &str, max: u16) -> Result<u16> {
	let rev = r.varint()?;
	match u16::try_from(rev) {
		Ok(rev) if (1..=max).contains(&rev) => Ok(rev),
		_ => bail!("{name} has no revision {rev}"),
	}
}

// ---------------------------------------------------------------------------
// Decoding
// ---------------------------------------------------------------------------

pub(super) fn decode(r: &mut Reader<'_>) -> Result<Value> {
	let rev = revision(r, "Value", 3)?;
	match rev {
		1 => {
			let variant = u8::try_from(r.varint()?).context("a Value variant past 255")?;
			if variant > LAST_VARIANT_BEFORE_REVISION_3 {
				bail!("no Value variant {variant} at revision 1");
			}
			decode_variant(variant, r)
		}
		_ => {
			let tag = r.u8()?;
			let variant = tag & 0b1_1111;
			if rev == 2 && variant > LAST_VARIANT_BEFORE_REVISION_3 {
				bail!("no Value variant {variant} at revision 2");
			}
			match (tag >> 5) & 0b11 {
				0b00 => decode_variant(variant, &mut Reader::new(&[])),
				0b01 => {
					if variant != 2 {
						bail!("Value variant {variant} is not fixed-size");
					}
					decode_variant(variant, &mut Reader::new(r.take(1)?))
				}
				0b10 => {
					let mut body = r.varlen()?;
					let value = decode_variant(variant, &mut body)?;
					if !body.is_empty() {
						bail!("{} trailing bytes in a Value variant {variant}", body.remaining());
					}
					Ok(value)
				}
				_ => bail!("an invalid Value tag {tag:#x}"),
			}
		}
	}
}

/// The last variant a `Value` of revision 1 or 2 holds (`Closure`).
const LAST_VARIANT_BEFORE_REVISION_3: u8 = 18;

fn decode_variant(variant: u8, r: &mut Reader<'_>) -> Result<Value> {
	Ok(match variant {
		0 => Value::None,
		1 => Value::Null,
		2 => Value::Bool(match r.u8()? {
			0 => false,
			1 => true,
			b => bail!("an invalid bool {b}"),
		}),
		3 => Value::Number(decode_number(r)?),
		4 => Value::String(decode_strand(r)?),
		5 => {
			revision(r, "Duration", 1)?;
			Value::Duration(Duration(decode_std_duration(r)?))
		}
		6 => {
			revision(r, "Datetime", 1)?;
			Value::Datetime(decode_datetime(r)?)
		}
		7 => Value::Uuid(decode_uuid(r)?),
		8 => Value::Array(decode_array(r)?),
		9 => Value::Set(decode_set(r)?),
		10 => Value::Object(decode_object(r)?),
		11 => Value::Geometry(decode_geometry(r)?),
		12 => {
			revision(r, "Bytes", 1)?;
			Value::Bytes(Bytes(r.bytes()?.to_vec().into()))
		}
		13 => Value::Table(TableName::new(decode_strand(r)?)),
		14 => Value::RecordId(decode_record_id(r)?),
		15 => {
			revision(r, "File", 1)?;
			let bucket = decode_string(r)?;
			let key = decode_string(r)?;
			Value::File(File {
				bucket,
				key,
			})
		}
		16 => {
			revision(r, "Regex", 1)?;
			let source = decode_string(r)?;
			Value::Regex(source.parse().map_err(|e| anyhow::anyhow!("an invalid regex: {e}"))?)
		}
		17 => {
			revision(r, "Range", 1)?;
			let start = decode_bound(r, decode)?;
			let end = decode_bound(r, decode)?;
			Value::Range(Box::new(Range {
				start,
				end,
			}))
		}
		18 => bail!("a closure is not stored"),
		19 => {
			revision(r, "Date", 1)?;
			let days = i32::try_from(r.zigzag()?)?;
			Value::Date(Date::from_days(days).context("a date out of range")?)
		}
		20 => {
			revision(r, "Time", 1)?;
			let nanos = u64::try_from(r.varint()?)?;
			Value::Time(Time::from_nanos(nanos).context("a time past 24:00:00")?)
		}
		21 => {
			revision(r, "Timestamp", 1)?;
			let secs = i64::try_from(r.zigzag()?)?;
			let nanos = u32::try_from(r.varint()?)?;
			Value::Timestamp(
				Timestamp::from_parts(secs, nanos).context("a timestamp out of range")?,
			)
		}
		22 => {
			revision(r, "Interval", 1)?;
			let months = i32::try_from(r.zigzag()?)?;
			let days = i32::try_from(r.zigzag()?)?;
			let micros = i64::try_from(r.zigzag()?)?;
			Value::Interval(Interval::new(months, days, micros))
		}
		v => bail!("no Value variant {v}"),
	})
}

fn decode_string(r: &mut Reader<'_>) -> Result<String> {
	Ok(std::str::from_utf8(r.bytes()?).context("a string that is not UTF-8")?.to_owned())
}

fn decode_strand(r: &mut Reader<'_>) -> Result<Strand> {
	Ok(Strand::new(std::str::from_utf8(r.bytes()?).context("a string that is not UTF-8")?))
}

fn decode_std_duration(r: &mut Reader<'_>) -> Result<std::time::Duration> {
	let secs = u64::try_from(r.varint()?)?;
	let nanos = u32::try_from(r.varint()?)?;
	// `Duration::new` carries whole seconds out of `nanos` and panics when that overflows `secs`.
	let secs = secs
		.checked_add(u64::from(nanos / NANOS_PER_SEC))
		.context("a duration past the representable range")?;
	Ok(std::time::Duration::new(secs, nanos % NANOS_PER_SEC))
}

fn decode_datetime(r: &mut Reader<'_>) -> Result<Datetime> {
	let secs = i64::try_from(r.zigzag()?)?;
	let nanos = u32::try_from(r.varint()?)?;
	Ok(Datetime(chrono::DateTime::from_timestamp(secs, nanos).context("a datetime out of range")?))
}

fn decode_uuid(r: &mut Reader<'_>) -> Result<Uuid> {
	revision(r, "Uuid", 1)?;
	Ok(Uuid(uuid::Uuid::from_bytes(r.array()?)))
}

pub(super) fn decode_number(r: &mut Reader<'_>) -> Result<Number> {
	let rev = revision(r, "Number", 3)?;
	let variant = r.varint()?;
	match (rev, variant) {
		(1, 0) | (2, 0) => Ok(Number::Int(i64::try_from(r.zigzag()?)?)),
		(1, 1) | (2, 1) => Ok(Number::Float(f64::from_le_bytes(r.array()?))),
		(1, 2) | (2, 2) => Ok(Number::Decimal(Decimal::deserialize(r.array()?))),
		(2, 3) => Ok(Number::I8(r.u8()? as i8)),
		(2, 4) => Ok(Number::U8(r.u8()?)),
		(2, 5) => Ok(Number::I16(i16::try_from(r.zigzag()?)?)),
		(2, 6) => Ok(Number::U16(u16::try_from(r.varint()?)?)),
		(2, 7) => Ok(Number::I32(i32::try_from(r.zigzag()?)?)),
		(2, 8) => Ok(Number::U32(u32::try_from(r.varint()?)?)),
		(2, 9) => Ok(Number::U64(u64::try_from(r.varint()?)?)),
		(2, 10) => Ok(Number::F16(surrealdb_types::f16::from_bits(u16::try_from(r.varint()?)?))),
		(2, 11) => Ok(Number::F32(f32::from_le_bytes(r.array()?))),
		(3, 12) => {
			let text = std::str::from_utf8(r.bytes()?).context("a bigdecimal's text")?;
			let decimal = text.parse::<surrealdb_types::BigDecimal>()?;
			Ok(Number::BigDecimal(Box::new(decimal)))
		}
		_ => bail!("no Number variant {variant} at revision {rev}"),
	}
}

/// The items of a revision-1 sequence, or of the body an indexed sequence
/// or set holds.
fn decode_items<T>(
	r: &mut Reader<'_>,
	rev: u16,
	mut item: impl FnMut(&mut Reader<'_>) -> Result<T>,
) -> Result<Vec<T>> {
	if rev == 1 {
		let len = r.len()?;
		let mut out = Vec::with_capacity(len.min(4096));
		for _ in 0..len {
			out.push(item(r)?);
		}
		return Ok(out);
	}
	let mut body = r.varlen()?;
	let flags = body.u8()?;
	let len = body.len()?;
	if flags & FLAG_INDEXED != 0 {
		body.take(len.checked_mul(4).context("an offset table past usize")?)?;
	}
	let mut out = Vec::with_capacity(len.min(4096));
	for _ in 0..len {
		out.push(item(&mut body)?);
	}
	if !body.is_empty() {
		bail!("{} trailing bytes in an indexed sequence", body.remaining());
	}
	Ok(out)
}

fn decode_array(r: &mut Reader<'_>) -> Result<Array> {
	let rev = revision(r, "Array", 2)?;
	Ok(Array(decode_items(r, rev, decode)?))
}

fn decode_set(r: &mut Reader<'_>) -> Result<Set> {
	let rev = revision(r, "Set", 2)?;
	Ok(Set(decode_items(r, rev, decode)?.into_iter().collect()))
}

fn decode_object(r: &mut Reader<'_>) -> Result<Object> {
	let rev = revision(r, "Object", 2)?;
	let mut object = Object::default();
	if rev == 1 {
		let len = r.len()?;
		for _ in 0..len {
			let key = decode_strand(r)?;
			let value = decode(r)?;
			object.0.insert(key, value);
		}
		return Ok(object);
	}
	let mut body = r.varlen()?;
	let flags = body.u8()?;
	let len = body.len()?;
	if flags & FLAG_INDEXED == 0 {
		for _ in 0..len {
			let key = decode_strand(&mut body)?;
			let value = decode(&mut body)?;
			object.0.insert(key, value);
		}
	} else {
		// `(key_off, val_off)` per entry and the two region lengths.
		let table = len
			.checked_mul(8)
			.and_then(|n| n.checked_add(8))
			.context("an offset table past usize")?;
		body.take(table)?;
		let mut keys = Vec::with_capacity(len.min(4096));
		for _ in 0..len {
			keys.push(decode_strand(&mut body)?);
		}
		for key in keys {
			let value = decode(&mut body)?;
			object.0.insert(key, value);
		}
	}
	if !body.is_empty() {
		bail!("{} trailing bytes in an indexed map", body.remaining());
	}
	Ok(object)
}

fn decode_bound<T>(
	r: &mut Reader<'_>,
	mut value: impl FnMut(&mut Reader<'_>) -> Result<T>,
) -> Result<Bound<T>> {
	Ok(match r.varint()? {
		0 => Bound::Unbounded,
		1 => Bound::Included(value(r)?),
		2 => Bound::Excluded(value(r)?),
		v => bail!("no Bound variant {v}"),
	})
}

pub(super) fn decode_record_id(r: &mut Reader<'_>) -> Result<RecordId> {
	revision(r, "RecordId", 1)?;
	let table = TableName::new(decode_strand(r)?);
	let key = decode_record_id_key(r)?;
	Ok(RecordId {
		table,
		key,
	})
}

pub(super) fn decode_record_id_key(r: &mut Reader<'_>) -> Result<RecordIdKey> {
	revision(r, "RecordIdKey", 1)?;
	Ok(match r.varint()? {
		0 => RecordIdKey::Number(i64::try_from(r.zigzag()?)?),
		1 => RecordIdKey::String(decode_strand(r)?),
		2 => RecordIdKey::Uuid(decode_uuid(r)?),
		3 => RecordIdKey::Array(decode_array(r)?),
		4 => RecordIdKey::Object(decode_object(r)?),
		5 => {
			revision(r, "RecordIdKeyRange", 1)?;
			let start = decode_bound(r, decode_record_id_key)?;
			let end = decode_bound(r, decode_record_id_key)?;
			RecordIdKey::Range(Box::new(RecordIdKeyRange {
				start,
				end,
			}))
		}
		v => bail!("no RecordIdKey variant {v}"),
	})
}

fn decode_coord(r: &mut Reader<'_>) -> Result<geo_types::Coord<f64>> {
	let x = f64::from_le_bytes(r.array()?);
	let y = f64::from_le_bytes(r.array()?);
	Ok(geo_types::Coord {
		x,
		y,
	})
}

fn decode_line(r: &mut Reader<'_>) -> Result<geo_types::LineString<f64>> {
	let len = r.len()?;
	let mut coords = Vec::with_capacity(len.min(4096));
	for _ in 0..len {
		coords.push(decode_coord(r)?);
	}
	Ok(geo_types::LineString(coords))
}

fn decode_polygon(r: &mut Reader<'_>) -> Result<geo_types::Polygon<f64>> {
	let exterior = ring_before_closing(decode_line(r)?);
	let len = r.len()?;
	let mut interiors = Vec::with_capacity(len.min(4096));
	for _ in 0..len {
		interiors.push(ring_before_closing(decode_line(r)?));
	}
	Ok(geo_types::Polygon::new(exterior, interiors))
}

fn decode_vec<T>(
	r: &mut Reader<'_>,
	mut item: impl FnMut(&mut Reader<'_>) -> Result<T>,
) -> Result<Vec<T>> {
	let len = r.len()?;
	let mut out = Vec::with_capacity(len.min(4096));
	for _ in 0..len {
		out.push(item(r)?);
	}
	Ok(out)
}

fn decode_geometry(r: &mut Reader<'_>) -> Result<Geometry> {
	revision(r, "Geometry", 1)?;
	Ok(match r.varint()? {
		0 => Geometry::Point(geo_types::Point(decode_coord(r)?)),
		1 => Geometry::Line(decode_line(r)?),
		2 => Geometry::Polygon(decode_polygon(r)?),
		3 => Geometry::MultiPoint(geo_types::MultiPoint(decode_vec(r, |r| {
			Ok(geo_types::Point(decode_coord(r)?))
		})?)),
		4 => Geometry::MultiLine(geo_types::MultiLineString(decode_vec(r, decode_line)?)),
		5 => Geometry::MultiPolygon(geo_types::MultiPolygon(decode_vec(r, decode_polygon)?)),
		6 => Geometry::Collection(decode_vec(r, decode_geometry)?),
		v => bail!("no Geometry variant {v}"),
	})
}

// ---------------------------------------------------------------------------
// Encoding, at each type's current revision
// ---------------------------------------------------------------------------

pub(super) fn encode(value: &Value, w: &mut Writer) -> Result<()> {
	let is_new =
		matches!(value, Value::Date(_) | Value::Time(_) | Value::Timestamp(_) | Value::Interval(_));
	w.varint(if is_new {
		3
	} else {
		2
	});
	let (variant, class): (u8, u8) = match value {
		Value::None => (0, 0b00),
		Value::Null => (1, 0b00),
		Value::Bool(_) => (2, 0b01),
		Value::Number(_) => (3, 0b10),
		Value::String(_) => (4, 0b10),
		Value::Duration(_) => (5, 0b10),
		Value::Datetime(_) => (6, 0b10),
		Value::Uuid(_) => (7, 0b10),
		Value::Array(_) => (8, 0b10),
		Value::Set(_) => (9, 0b10),
		Value::Object(_) => (10, 0b10),
		Value::Geometry(_) => (11, 0b10),
		Value::Bytes(_) => (12, 0b10),
		Value::Table(_) => (13, 0b10),
		Value::RecordId(_) => (14, 0b10),
		Value::File(_) => (15, 0b10),
		Value::Regex(_) => (16, 0b10),
		Value::Range(_) => (17, 0b10),
		Value::Date(_) => (19, 0b10),
		Value::Time(_) => (20, 0b10),
		Value::Timestamp(_) => (21, 0b10),
		Value::Interval(_) => (22, 0b10),
		Value::Closure(_) => return Err(closure_not_stored()),
	};
	w.u8(variant | class << 5);
	match value {
		Value::None | Value::Null => Ok(()),
		Value::Bool(b) => {
			w.u8(*b as u8);
			Ok(())
		}
		other @ (Value::Number(_)
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
		| Value::File(_)
		| Value::Regex(_)
		| Value::Range(_)
		| Value::Closure(_)) => w.varlen(|w| encode_body(other, w)),
	}
}

fn encode_body(value: &Value, w: &mut Writer) -> Result<()> {
	match value {
		Value::Number(n) => encode_number(n, w)?,
		Value::String(s) => w.bytes(s.as_bytes()),
		Value::Duration(d) => {
			w.varint(1);
			encode_std_duration(&d.0, w);
		}
		Value::Datetime(d) => {
			w.varint(1);
			encode_datetime(d, w);
		}
		Value::Date(d) => {
			w.varint(1);
			w.zigzag(i128::from(d.0));
		}
		Value::Time(t) => {
			w.varint(1);
			w.varint(u128::from(t.0));
		}
		Value::Timestamp(t) => {
			w.varint(1);
			w.zigzag(i128::from(t.secs));
			w.varint(u128::from(t.nanos));
		}
		Value::Interval(i) => {
			w.varint(1);
			w.zigzag(i128::from(i.months));
			w.zigzag(i128::from(i.days));
			w.zigzag(i128::from(i.micros));
		}
		Value::Uuid(u) => encode_uuid(u, w),
		Value::Array(a) => encode_array(a, w)?,
		Value::Set(s) => {
			w.varint(2);
			let mut items = Vec::with_capacity(s.0.len());
			for item in s.0.iter() {
				let mut b = Writer::default();
				encode(item, &mut b)?;
				items.push(b.into_bytes());
			}
			items.sort();
			w.varlen(|w| {
				encode_indexed_seq(&items, w);
				Ok(())
			})?;
		}
		Value::Object(o) => encode_object(o, w)?,
		Value::Geometry(g) => encode_geometry(g, w),
		Value::Bytes(b) => {
			w.varint(1);
			w.bytes(&b.0);
		}
		Value::Table(t) => w.bytes(t.as_str().as_bytes()),
		Value::RecordId(rid) => encode_record_id(rid, w)?,
		Value::File(f) => {
			w.varint(1);
			w.bytes(f.bucket.as_bytes());
			w.bytes(f.key.as_bytes());
		}
		Value::Regex(re) => {
			w.varint(1);
			w.bytes(re.0.as_str().as_bytes());
		}
		Value::Range(range) => {
			w.varint(1);
			encode_bound(&range.start, w, encode)?;
			encode_bound(&range.end, w, encode)?;
		}
		Value::None | Value::Null | Value::Bool(_) | Value::Closure(_) => {
			unreachable!("written by the tag")
		}
	}
	Ok(())
}

fn encode_std_duration(d: &std::time::Duration, w: &mut Writer) {
	w.varint(d.as_secs() as u128);
	w.varint(d.subsec_nanos() as u128);
}

fn encode_datetime(d: &Datetime, w: &mut Writer) {
	w.zigzag(d.0.timestamp() as i128);
	w.varint(d.0.timestamp_subsec_nanos() as u128);
}

fn encode_uuid(u: &Uuid, w: &mut Writer) {
	w.varint(1);
	w.raw(u.0.as_bytes());
}

pub(super) fn encode_number(n: &Number, w: &mut Writer) -> Result<()> {
	match n {
		// Revision 3 adds the bigdecimal, held as its text.
		Number::BigDecimal(d) => {
			w.varint(3);
			w.varint(12);
			w.bytes(d.to_string().as_bytes());
		}
		Number::Int(v) => {
			w.varint(1);
			w.varint(0);
			w.zigzag(*v as i128);
		}
		Number::Float(v) => {
			w.varint(1);
			w.varint(1);
			w.raw(&v.to_le_bytes());
		}
		Number::Decimal(d) => {
			w.varint(1);
			w.varint(2);
			w.raw(&d.serialize());
		}
		Number::I8(v) => {
			w.varint(2);
			w.varint(3);
			w.u8(*v as u8);
		}
		Number::U8(v) => {
			w.varint(2);
			w.varint(4);
			w.u8(*v);
		}
		Number::I16(v) => {
			w.varint(2);
			w.varint(5);
			w.zigzag(*v as i128);
		}
		Number::U16(v) => {
			w.varint(2);
			w.varint(6);
			w.varint(*v as u128);
		}
		Number::I32(v) => {
			w.varint(2);
			w.varint(7);
			w.zigzag(*v as i128);
		}
		Number::U32(v) => {
			w.varint(2);
			w.varint(8);
			w.varint(*v as u128);
		}
		Number::U64(v) => {
			w.varint(2);
			w.varint(9);
			w.varint(*v as u128);
		}
		Number::F16(v) => {
			w.varint(2);
			w.varint(10);
			w.varint(v.to_bits() as u128);
		}
		Number::F32(v) => {
			w.varint(2);
			w.varint(11);
			w.raw(&v.to_le_bytes());
		}
	}
	Ok(())
}

/// An indexed sequence or set body of already-encoded items.
fn encode_indexed_seq(items: &[Vec<u8>], w: &mut Writer) {
	if items.len() < OFFSET_TABLE_MIN_LEN {
		w.u8(0);
		w.varint(items.len() as u128);
	} else {
		w.u8(FLAG_INDEXED);
		w.varint(items.len() as u128);
		let mut off = 0u32;
		for item in items {
			w.raw(&off.to_le_bytes());
			off += item.len() as u32;
		}
	}
	for item in items {
		w.raw(item);
	}
}

/// An indexed sequence body of `items`, each encoded in place: behind an
/// offset table of where each starts, relative to the first, from
/// [`OFFSET_TABLE_MIN_LEN`] items.
fn encode_seq_in_place<'v>(
	items: impl ExactSizeIterator<Item = &'v Value>,
	w: &mut Writer,
) -> Result<()> {
	let len = items.len();
	if len < OFFSET_TABLE_MIN_LEN {
		w.u8(0);
		w.varint(len as u128);
		for item in items {
			encode(item, w)?;
		}
		return Ok(());
	}
	w.u8(FLAG_INDEXED);
	w.varint(len as u128);
	let table = w.reserve(len * 4);
	let base = w.len();
	for (at, item) in items.enumerate() {
		let off = w.since(base)?;
		w.patch_u32(table + at * 4, off);
		encode(item, w)?;
	}
	Ok(())
}

fn encode_array(a: &Array, w: &mut Writer) -> Result<()> {
	w.varint(2);
	w.varlen(|w| encode_seq_in_place(a.0.iter(), w))
}

/// An object key's length prefix as its encoding writes it, which is what its
/// entries sort by first: a varint's encodings of different lengths differ in
/// their first byte, so ordering by the prefix and then the key's bytes is
/// ordering by the key's encoded bytes.
fn key_prefix(key: &str) -> ([u8; 9], usize) {
	let len = key.len() as u64;
	let mut prefix = [0u8; 9];
	let used = if len < 251 {
		prefix[0] = len as u8;
		1
	} else if len < 1 << 16 {
		prefix[0] = 251;
		prefix[1..3].copy_from_slice(&(len as u16).to_le_bytes());
		3
	} else if len < 1 << 32 {
		prefix[0] = 252;
		prefix[1..5].copy_from_slice(&(len as u32).to_le_bytes());
		5
	} else {
		prefix[0] = 253;
		prefix[1..9].copy_from_slice(&len.to_le_bytes());
		9
	};
	(prefix, used)
}

/// An object entry as [`encode_object`] orders it: its key's length prefix
/// and the number of bytes that prefix takes, its key, and its value.
type Entry<'o> = (([u8; 9], usize), &'o str, &'o Value);

fn encode_object(o: &Object, w: &mut Writer) -> Result<()> {
	w.varint(2);
	let mut pairs: Vec<Entry<'_>> =
		o.0.iter().map(|(k, v)| (key_prefix(k), k.as_str(), v)).collect();
	pairs.sort_unstable_by(|a, b| {
		(&a.0.0[..a.0.1], a.1.as_bytes()).cmp(&(&b.0.0[..b.0.1], b.1.as_bytes()))
	});
	w.varlen(|w| {
		if pairs.len() < OFFSET_TABLE_MIN_LEN {
			w.u8(0);
			w.varint(pairs.len() as u128);
			for ((prefix, len), k, v) in &pairs {
				w.raw(&prefix[..*len]);
				w.raw(k.as_bytes());
				encode(v, w)?;
			}
			return Ok(());
		}
		w.u8(FLAG_INDEXED);
		w.varint(pairs.len() as u128);
		// Each entry's key and value offsets, relative to the first key and
		// the first value, then the keys' and the values' total lengths.
		let table = w.reserve((pairs.len() + 1) * 8);
		let keys = w.len();
		for (at, ((prefix, len), k, _)) in pairs.iter().enumerate() {
			let off = w.since(keys)?;
			w.patch_u32(table + at * 8, off);
			w.raw(&prefix[..*len]);
			w.raw(k.as_bytes());
		}
		let keys_len = w.since(keys)?;
		w.patch_u32(table + pairs.len() * 8, keys_len);
		let values = w.len();
		for (at, (_, _, v)) in pairs.iter().enumerate() {
			let off = w.since(values)?;
			w.patch_u32(table + at * 8 + 4, off);
			encode(v, w)?;
		}
		let values_len = w.since(values)?;
		w.patch_u32(table + pairs.len() * 8 + 4, values_len);
		Ok(())
	})
}

fn encode_bound<T>(
	bound: &Bound<T>,
	w: &mut Writer,
	mut value: impl FnMut(&T, &mut Writer) -> Result<()>,
) -> Result<()> {
	match bound {
		Bound::Unbounded => w.varint(0),
		Bound::Included(v) => {
			w.varint(1);
			value(v, w)?;
		}
		Bound::Excluded(v) => {
			w.varint(2);
			value(v, w)?;
		}
	}
	Ok(())
}

pub(super) fn encode_record_id(rid: &RecordId, w: &mut Writer) -> Result<()> {
	w.varint(1);
	w.bytes(rid.table.as_str().as_bytes());
	encode_record_id_key(&rid.key, w)
}

pub(super) fn encode_record_id_key(key: &RecordIdKey, w: &mut Writer) -> Result<()> {
	w.varint(1);
	match key {
		RecordIdKey::Number(n) => {
			w.varint(0);
			w.zigzag(*n as i128);
		}
		RecordIdKey::String(s) => {
			w.varint(1);
			w.bytes(s.as_bytes());
		}
		RecordIdKey::Uuid(u) => {
			w.varint(2);
			encode_uuid(u, w);
		}
		RecordIdKey::Array(a) => {
			w.varint(3);
			encode_array(a, w)?;
		}
		RecordIdKey::Object(o) => {
			w.varint(4);
			encode_object(o, w)?;
		}
		RecordIdKey::Range(range) => {
			w.varint(5);
			w.varint(1);
			encode_bound(&range.start, w, encode_record_id_key)?;
			encode_bound(&range.end, w, encode_record_id_key)?;
		}
	}
	Ok(())
}

fn encode_coord(c: &geo_types::Coord<f64>, w: &mut Writer) {
	w.raw(&c.x.to_le_bytes());
	w.raw(&c.y.to_le_bytes());
}

fn encode_line(l: &geo_types::LineString<f64>, w: &mut Writer) {
	w.varint(l.0.len() as u128);
	for c in &l.0 {
		encode_coord(c, w);
	}
}

fn encode_polygon(p: &geo_types::Polygon<f64>, w: &mut Writer) {
	encode_line(p.exterior(), w);
	w.varint(p.interiors().len() as u128);
	for interior in p.interiors() {
		encode_line(interior, w);
	}
}

fn encode_geometry(g: &Geometry, w: &mut Writer) {
	w.varint(1);
	match g {
		Geometry::Point(p) => {
			w.varint(0);
			encode_coord(&p.0, w);
		}
		Geometry::Line(l) => {
			w.varint(1);
			encode_line(l, w);
		}
		Geometry::Polygon(p) => {
			w.varint(2);
			encode_polygon(p, w);
		}
		Geometry::MultiPoint(m) => {
			w.varint(3);
			w.varint(m.0.len() as u128);
			for p in &m.0 {
				encode_coord(&p.0, w);
			}
		}
		Geometry::MultiLine(m) => {
			w.varint(4);
			w.varint(m.0.len() as u128);
			for l in &m.0 {
				encode_line(l, w);
			}
		}
		Geometry::MultiPolygon(m) => {
			w.varint(5);
			w.varint(m.0.len() as u128);
			for p in &m.0 {
				encode_polygon(p, w);
			}
		}
		Geometry::Collection(c) => {
			w.varint(6);
			w.varint(c.len() as u128);
			for g in c {
				encode_geometry(g, w);
			}
		}
	}
}

pub(super) fn decode_val_datetime(r: &mut Reader<'_>) -> Result<Datetime> {
	revision(r, "Datetime", 1)?;
	decode_datetime(r)
}

pub(super) fn encode_val_datetime(d: &Datetime, w: &mut Writer) {
	w.varint(1);
	encode_datetime(d, w);
}

/// The error encoding a closure fails with: a closure value is never stored,
/// and a write that would store one fails with the error the value's own
/// encoding raises.
pub(super) fn closure_not_stored() -> anyhow::Error {
	anyhow::Error::new(revision::Error::Conversion("Closures cannot be stored on disk".to_owned()))
}
