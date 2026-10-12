//! Output frames: result rows encoded straight into a host-supplied buffer.
//!
//! A [`RowFrame`] holds rows as concatenated CBOR items with an offsets table,
//! so a host can route, split or concatenate a frame without decoding it.
//! Each row's bytes are exactly what the RPC layer's CBOR encoder
//! (`rpc::format::cbor::encode`) writes for the public value the row converts
//! to (`convert_value_to_public_value`): the same tags, the same shortest
//! integer and float forms, object keys in the same order.
//!
//! Rows reach a frame two ways, neither of which builds the public value:
//!
//! - [`RowFrame::push_value`] encodes an in-memory [`Value`];
//! - [`RowFrame::push_stored`] transcodes a stored record — its key and its stored value, in the
//!   stored-value encoding or in `revision` — reading the stored bytes in place. Objects, arrays,
//!   strings, integers and the other leaves are copied from where they lie; a value of a kind the
//!   transcoder does not read in place is decoded alone and encoded as an in-memory value.
//!
//! A frame's buffers are the host's: [`RowFrame::clear`] keeps their capacity,
//! so a frame reused across batches encodes in steady state without asking
//! the global allocator for anything. The transcoder's scratch (the order in
//! which an object's stored entries are written) lives in the frame too, for
//! the same reason.

use std::fmt::Write as _;

use revision::optimised::IndexedSeqWalker;
use revision::optimised::tag::{SizeClass, Tag};
use surrealdb_rpc::{EncodedRows, RowFormat};

use super::scalar::Scalar;
use crate::catalog::{StoredRecord, TableLayouts};
use crate::exec::object_extract::indexed_map_walker_from_object_bytes;
use crate::exec::object_extract::wire_skip::{rev2_optimised_payload_unchecked, skip_value_wire};
use crate::exec::pre_decode_filter::strand_payload_str;
use crate::key::KVKeyDecode as _;
use crate::val::stored::{Kind, OrderedObjectRef, ValueRef};
use crate::val::{Number, RecordIdKey, Value};

// CBOR tags the RPC layer's encoder writes (`rpc::format::cbor::convert`).
const TAG_SPEC_UUID: u64 = 37;
const TAG_NONE: u64 = 6;
const TAG_TABLE: u64 = 7;
const TAG_RECORDID: u64 = 8;
const TAG_STRING_DECIMAL: u64 = 10;
const TAG_CUSTOM_DATETIME: u64 = 12;
const TAG_CUSTOM_DURATION: u64 = 14;

// CBOR major types.
const MAJOR_POSITIVE: u8 = 0;
const MAJOR_NEGATIVE: u8 = 1;
const MAJOR_BYTES: u8 = 2;
const MAJOR_TEXT: u8 = 3;
const MAJOR_ARRAY: u8 = 4;
const MAJOR_MAP: u8 = 5;
const MAJOR_TAG: u8 = 6;

/// rev-2 [`Value`] variant ids, in the declaration order of `Value`.
mod variant {
	pub(super) const STRING: u8 = 4;
	pub(super) const ARRAY: u8 = 8;
	pub(super) const OBJECT: u8 = 10;
	pub(super) const RECORD_ID: u8 = 14;
}

#[cfg(test)]
thread_local! {
	/// How many stored records [`RowFrame::push_stored`] decoded whole rather
	/// than transcoding in place.
	static WHOLE_DECODES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
	/// How many stored values the transcoder decoded whole into a [`Value`].
	static VALUE_DECODES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// The record field a stored record's key supplies.
const ID_KEY: &str = "id";

/// Rows encoded as CBOR items back to back, or as JSON texts joined by `,`,
/// with where each ends.
#[derive(Debug)]
pub(crate) struct RowFrame {
	format: RowFormat,
	data: Vec<u8>,
	ends: Vec<u32>,
	/// A reused buffer for the text renderings JSON rows hold.
	text: String,
	/// An object's stored entries, as offsets into the stored value being
	/// transcoded, in the order they are written. Each nested object uses
	/// the tail past its parent's entries.
	scratch: Vec<Entry>,
}

/// One stored object entry: where its key's UTF-8 and its value wire lie,
/// relative to the start of the stored value being transcoded.
#[derive(Debug, Clone, Copy)]
struct Entry {
	key: (u32, u32),
	value: (u32, u32),
}

/// The byte range of a `(start, len)` span, summed in `usize` so it cannot wrap.
fn span_range((start, len): (u32, u32)) -> std::ops::Range<usize> {
	let start = start as usize;
	start..start.saturating_add(len as usize)
}

impl Default for RowFrame {
	fn default() -> Self {
		Self::with_format(RowFormat::Cbor)
	}
}

impl RowFrame {
	pub(crate) fn new() -> Self {
		Self::default()
	}

	/// A frame whose rows are encoded in `format`.
	pub(crate) fn with_format(format: RowFormat) -> Self {
		Self {
			format,
			data: Vec::new(),
			ends: Vec::new(),
			text: String::new(),
			scratch: Vec::new(),
		}
	}

	/// The frame's rows, handed on as [`EncodedRows`]; the frame is left
	/// empty, with buffers of the same capacity for the rows that follow.
	pub(crate) fn take_encoded(&mut self) -> EncodedRows {
		let (data_capacity, ends_capacity) = (self.data.capacity(), self.ends.capacity());
		let bytes = std::mem::replace(&mut self.data, Vec::with_capacity(data_capacity));
		let ends = std::mem::replace(&mut self.ends, Vec::with_capacity(ends_capacity));
		EncodedRows::new(self.format, bytes, ends, crate::rpc::format::decode_rows)
	}

	/// How many rows the frame holds.
	pub(crate) fn len(&self) -> usize {
		self.ends.len()
	}

	/// Row `at`'s CBOR bytes.
	#[cfg(test)]
	pub(crate) fn row(&self, at: usize) -> &[u8] {
		let start = match at {
			0 => 0,
			_ if self.format == RowFormat::Json => self.ends[at - 1] as usize + 1,
			_ => self.ends[at - 1] as usize,
		};
		&self.data[start..self.ends[at] as usize]
	}

	/// Every row's bytes, back to back.
	pub(crate) fn data(&self) -> &[u8] {
		&self.data
	}

	/// Drop every row, keeping the buffers' capacity.
	pub(crate) fn clear(&mut self) {
		self.data.clear();
		self.ends.clear();
	}

	/// Run `write` as the next row, which it leaves whole or, on an error,
	/// not at all.
	fn row_with(
		&mut self,
		write: impl FnOnce(&mut Self) -> anyhow::Result<()>,
	) -> anyhow::Result<()> {
		let start = self.data.len();
		if self.format == RowFormat::Json && !self.ends.is_empty() {
			self.data.push(b',');
		}
		match write(self) {
			Ok(()) => {
				let end = u32::try_from(self.data.len())
					.map_err(|_| anyhow::anyhow!("an output frame larger than 4 GiB"))?;
				self.ends.push(end);
				Ok(())
			}
			Err(e) => {
				self.data.truncate(start);
				Err(e)
			}
		}
	}

	/// Append `value` as a row.
	pub(crate) fn push_value(&mut self, value: &Value) -> anyhow::Result<()> {
		self.row_with(|frame| match frame.format {
			RowFormat::Cbor => encode_value(&mut frame.data, value, 1),
			RowFormat::Json => {
				super::row_json::encode_json(&mut frame.data, value, 1, &mut frame.text)
			}
		})
	}

	/// Append the object of `fields` as a row: what [`Self::push_value`]
	/// appends for the object holding each value under its key. `fields`
	/// holds each key once, in ascending order of its text.
	pub(crate) fn push_fields(&mut self, fields: &[(&str, Scalar<'_>)]) -> anyhow::Result<()> {
		debug_assert!(fields.windows(2).all(|w| w[0].0 < w[1].0), "fields in key order");
		self.row_with(|frame| {
			let RowFrame {
				format,
				data,
				text: text_buf,
				..
			} = frame;
			match format {
				RowFormat::Cbor => {
					head(data, MAJOR_MAP, fields.len() as u64);
					for (key, value) in fields {
						text(data, key);
						encode_scalar(data, None, value, 2)?;
					}
				}
				RowFormat::Json => {
					data.push(b'{');
					for (at, (key, value)) in fields.iter().enumerate() {
						if at > 0 {
							data.push(b',');
						}
						super::row_json::json_string(data, key)?;
						data.push(b':');
						encode_scalar(data, Some(text_buf), value, 2)?;
					}
					data.push(b'}');
				}
			}
			Ok(())
		})
	}

	/// Append the stored record `key` names, whose stored value is `value`, as
	/// a row: what [`Self::push_value`] appends for the record decoded, with
	/// its `id` taken from the key.
	///
	/// The caller has admitted `value`'s generation against its table's layouts
	/// ([`crate::kvs::RowGate`]) and passes the layouts that admitted it. A
	/// document row is transcoded in place; a packed row is decoded through its
	/// generation's descriptor in `layouts` and transcoded as a value, and is
	/// refused without them. An envelope this build does not read fails with its
	/// typed error.
	pub(crate) fn push_stored(
		&mut self,
		key: &[u8],
		value: &[u8],
		layouts: Option<&TableLayouts>,
	) -> anyhow::Result<()> {
		self.row_with(|frame| {
			let RowFrame {
				format,
				data,
				scratch,
				text,
				..
			} = frame;
			let mut stored = Stored {
				base: value,
				out: data,
				scratch,
				json: (*format == RowFormat::Json).then_some(text),
			};
			// A document the whole decode refuses falls through to that decode, which reports it.
			if let Some(record) = StoredRecord::parse(value)?
				&& let Ok(Some(document)) = record.checked_document()
				&& let Some(object) = document.value()?.as_ordered_object()?
			{
				return stored.stored_object(object, Some(key), 1);
			}
			if let Some(data) = crate::exec::object_extract::rev2_record_data_bytes(value)
				&& let Some(object) = varlen_body(data, variant::OBJECT)
				&& stored.object(object, Some(key), 1)?
			{
				return Ok(());
			}
			#[cfg(test)]
			WHOLE_DECODES.with(|n| n.set(n.get() + 1));
			let decoded =
				crate::exec::operators::scan::pipeline::decode_record_value(key, value, layouts)?;
			stored.encoded(&decoded, 1)
		})
	}
}

// --------------------------------------------------
// CBOR primitives
// --------------------------------------------------

/// A CBOR head: major type `major` with argument `n`, in its shortest form.
#[inline]
fn head(out: &mut Vec<u8>, major: u8, n: u64) {
	let m = major << 5;
	if n <= 23 {
		out.push(m | n as u8);
	} else if n <= u8::MAX as u64 {
		out.extend_from_slice(&[m | 24, n as u8]);
	} else if n <= u16::MAX as u64 {
		out.push(m | 25);
		out.extend_from_slice(&(n as u16).to_be_bytes());
	} else if n <= u32::MAX as u64 {
		out.push(m | 26);
		out.extend_from_slice(&(n as u32).to_be_bytes());
	} else {
		out.push(m | 27);
		out.extend_from_slice(&n.to_be_bytes());
	}
}

#[inline]
fn int(out: &mut Vec<u8>, v: i64) {
	if v < 0 {
		head(out, MAJOR_NEGATIVE, v as u64 ^ !0);
	} else {
		head(out, MAJOR_POSITIVE, v as u64);
	}
}

#[inline]
fn text(out: &mut Vec<u8>, s: &str) {
	head(out, MAJOR_TEXT, s.len() as u64);
	out.extend_from_slice(s.as_bytes());
}

#[inline]
fn bytes(out: &mut Vec<u8>, b: &[u8]) {
	head(out, MAJOR_BYTES, b.len() as u64);
	out.extend_from_slice(b);
}

#[inline]
fn tag(out: &mut Vec<u8>, t: u64) {
	head(out, MAJOR_TAG, t);
}

/// A float in the shortest of half, single and double precision that holds
/// it bit for bit, as the RPC layer's encoder writes it.
fn float(out: &mut Vec<u8>, v: f64) {
	if let Some(half) = f16_bits(v) {
		out.push(0xf9);
		out.extend_from_slice(&half.to_be_bytes());
		return;
	}
	let single = v as f32;
	if f64::from(single).to_bits() == v.to_bits() {
		out.push(0xfa);
		out.extend_from_slice(&single.to_be_bytes());
		return;
	}
	out.push(0xfb);
	out.extend_from_slice(&v.to_be_bytes());
}

/// The half-precision bits of `v` when converting it to half precision and
/// back gives the same bits.
fn f16_bits(v: f64) -> Option<u16> {
	let bits = v.to_bits();
	let sign = ((bits >> 48) & 0x8000) as u16;
	let exp = ((bits >> 52) & 0x7ff) as i32;
	let man = bits & 0x000f_ffff_ffff_ffff;
	if exp == 0x7ff {
		if man == 0 {
			return Some(sign | 0x7c00);
		}
		// A NaN round-trips only when it is quiet and its payload lies in the
		// mantissa bits a half keeps.
		if man & (1 << 51) == 0 || man & ((1 << 42) - 1) != 0 {
			return None;
		}
		return Some(sign | 0x7c00 | ((man >> 42) as u16 & 0x3ff));
	}
	if exp == 0 && man == 0 {
		return Some(sign);
	}
	if exp == 0 {
		// A double subnormal is far below a half's smallest subnormal.
		return None;
	}
	let unbiased = exp - 1023;
	if unbiased > 15 {
		return None;
	}
	if unbiased >= -14 {
		// A normal half keeps 10 of the 52 mantissa bits.
		if man & ((1 << 42) - 1) != 0 {
			return None;
		}
		return Some(sign | (((unbiased + 15) as u16) << 10) | (man >> 42) as u16);
	}
	// A subnormal half: `value = m * 2^-24` for an integer `m` in `1..1024`.
	let shift = -14 - unbiased; // 1 ..
	if shift > 10 {
		return None;
	}
	let full = (1u64 << 52) | man; // the significand with its implicit bit
	let drop = 42 + shift as u32;
	if full & ((1u64 << drop) - 1) != 0 {
		return None;
	}
	Some(sign | (full >> drop) as u16)
}

/// A `fmt::Write` into a fixed buffer on the stack.
struct StackText {
	buf: [u8; 64],
	len: usize,
}

impl std::fmt::Write for StackText {
	fn write_str(&mut self, s: &str) -> std::fmt::Result {
		let end = self.len + s.len();
		let dst = self.buf.get_mut(self.len..end).ok_or(std::fmt::Error)?;
		dst.copy_from_slice(s.as_bytes());
		self.len = end;
		Ok(())
	}
}

/// Write a decimal as CBOR text, formatted on the stack.
fn decimal_text(out: &mut Vec<u8>, d: rust_decimal::Decimal) {
	let mut stack = StackText {
		buf: [0; 64],
		len: 0,
	};
	if write!(stack, "{d}").is_ok() {
		head(out, MAJOR_TEXT, stack.len as u64);
		out.extend_from_slice(&stack.buf[..stack.len]);
	} else {
		text(out, &d.to_string());
	}
}

fn too_deep(depth: usize) -> anyhow::Result<()> {
	if depth > crate::val::MAX_VALUE_DEPTH {
		anyhow::bail!(crate::expr::Error::ValueTooDeep {
			max: crate::val::MAX_VALUE_DEPTH,
		});
	}
	Ok(())
}

// --------------------------------------------------
// In-memory values
// --------------------------------------------------

/// Encode `value`, at nesting depth `depth`, as the RPC layer encodes the
/// public value it converts to.
fn encode_value(out: &mut Vec<u8>, value: &Value, depth: usize) -> anyhow::Result<()> {
	too_deep(depth)?;
	match value {
		Value::None => {
			tag(out, TAG_NONE);
			out.push(0xf6);
		}
		Value::Null => out.push(0xf6),
		Value::Bool(v) => out.push(if *v {
			0xf5
		} else {
			0xf4
		}),
		Value::Number(n) => number(out, *n),
		Value::String(s) => text(out, s.as_str()),
		Value::Duration(d) => duration(out, d.0.as_secs(), d.0.subsec_nanos()),
		Value::Datetime(d) => datetime(out, d),
		Value::Uuid(u) => uuid(out, u.0.as_bytes()),
		Value::Array(a) => {
			head(out, MAJOR_ARRAY, a.len() as u64);
			for v in a.iter() {
				encode_value(out, v, depth + 1)?;
			}
		}
		Value::Object(o) => {
			head(out, MAJOR_MAP, o.len() as u64);
			for (k, v) in o.iter() {
				text(out, k.as_str());
				encode_value(out, v, depth + 1)?;
			}
		}
		Value::Bytes(b) => bytes(out, &b.0),
		Value::Table(t) => {
			tag(out, TAG_TABLE);
			text(out, t.as_str());
		}
		Value::RecordId(rid) => {
			encode_record_id(out, rid.table.as_str(), &rid.key, depth, || value.clone())?
		}
		// Kinds rare in result rows go through the public conversion.
		Value::Set(_)
		| Value::Geometry(_)
		| Value::File(_)
		| Value::Regex(_)
		| Value::Range(_)
		| Value::Closure(_) => public(out, value, depth)?,
	}
	Ok(())
}

/// Encode `value` through the public conversion and the RPC layer's encoder:
/// the definition the direct encoding answers to, for the kinds it does not
/// write itself.
#[cold]
fn public(out: &mut Vec<u8>, value: &Value, depth: usize) -> anyhow::Result<()> {
	too_deep(depth)?;
	let converted = crate::val::convert_value_to_public_value(value.clone())?;
	out.extend_from_slice(&crate::rpc::format::cbor::encode(converted)?);
	Ok(())
}

fn number(out: &mut Vec<u8>, n: Number) {
	match n {
		Number::Int(v) => int(out, v),
		Number::Float(v) => float(out, v),
		Number::Decimal(v) => {
			tag(out, TAG_STRING_DECIMAL);
			decimal_text(out, v);
		}
	}
}

fn duration(out: &mut Vec<u8>, secs: u64, nanos: u32) {
	tag(out, TAG_CUSTOM_DURATION);
	match (secs, nanos) {
		(0, 0) => head(out, MAJOR_ARRAY, 0),
		(_, 0) => {
			head(out, MAJOR_ARRAY, 1);
			head(out, MAJOR_POSITIVE, secs);
		}
		_ => {
			head(out, MAJOR_ARRAY, 2);
			head(out, MAJOR_POSITIVE, secs);
			head(out, MAJOR_POSITIVE, nanos as u64);
		}
	}
}

fn datetime(out: &mut Vec<u8>, d: &crate::val::Datetime) {
	tag(out, TAG_CUSTOM_DATETIME);
	head(out, MAJOR_ARRAY, 2);
	int(out, d.0.timestamp());
	int(out, d.0.timestamp_subsec_nanos() as i64);
}

fn uuid(out: &mut Vec<u8>, b: &[u8; 16]) {
	tag(out, TAG_SPEC_UUID);
	bytes(out, b);
}

/// Encode the record id of table `table` and key `key` at depth `depth`;
/// `whole` makes it as a value for the public conversion of a range key.
fn encode_record_id(
	out: &mut Vec<u8>,
	table: &str,
	key: &RecordIdKey,
	depth: usize,
	whole: impl FnOnce() -> Value,
) -> anyhow::Result<()> {
	let key_depth = depth + 1;
	too_deep(key_depth)?;
	match key {
		RecordIdKey::Number(n) => record_id(out, table, |out| {
			int(out, *n);
			Ok(())
		}),
		RecordIdKey::String(s) => record_id(out, table, |out| {
			text(out, s.as_str());
			Ok(())
		}),
		RecordIdKey::Uuid(u) => record_id(out, table, |out| {
			uuid(out, u.0.as_bytes());
			Ok(())
		}),
		RecordIdKey::Array(a) => record_id(out, table, |out| {
			head(out, MAJOR_ARRAY, a.len() as u64);
			for v in a.iter() {
				encode_value(out, v, key_depth + 1)?;
			}
			Ok(())
		}),
		RecordIdKey::Object(o) => record_id(out, table, |out| {
			head(out, MAJOR_MAP, o.len() as u64);
			for (k, v) in o.iter() {
				text(out, k.as_str());
				encode_value(out, v, key_depth + 1)?;
			}
			Ok(())
		}),
		RecordIdKey::Range(_) => public(out, &whole(), depth),
	}
}

fn record_id(
	out: &mut Vec<u8>,
	table: &str,
	key: impl FnOnce(&mut Vec<u8>) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
	tag(out, TAG_RECORDID);
	head(out, MAJOR_ARRAY, 2);
	text(out, table);
	key(out)
}

/// Encode `scalar` at depth `depth`: as JSON when given the scratch text
/// buffer JSON renderings use, as CBOR otherwise.
fn encode_scalar(
	out: &mut Vec<u8>,
	json: Option<&mut String>,
	scalar: &Scalar<'_>,
	depth: usize,
) -> anyhow::Result<()> {
	if let Some(text_buf) = json {
		return super::row_json::encode_json_scalar(out, scalar, depth, text_buf);
	}
	match scalar {
		Scalar::Value(value) => return encode_value(out, value, depth),
		Scalar::None => {
			tag(out, TAG_NONE);
			out.push(0xf6);
		}
		Scalar::Null => out.push(0xf6),
		Scalar::Bool(v) => out.push(if *v {
			0xf5
		} else {
			0xf4
		}),
		Scalar::Number(n) => number(out, *n),
		Scalar::Str(s) => text(out, s),
		Scalar::Bytes(b) => bytes(out, b),
		Scalar::Duration(d) => duration(out, d.0.as_secs(), d.0.subsec_nanos()),
		Scalar::Datetime(d) => datetime(out, d),
		Scalar::Uuid(u) => uuid(out, u.0.as_bytes()),
	}
	Ok(())
}

// --------------------------------------------------
// Stored values
// --------------------------------------------------

/// The transcoder over one stored value.
struct Stored<'b, 'o> {
	/// The stored value every [`Entry`] offset is relative to.
	base: &'b [u8],
	out: &'o mut Vec<u8>,
	scratch: &'o mut Vec<Entry>,
	/// For JSON rows, the scratch text renderings are written into; `None`
	/// for CBOR.
	json: Option<&'o mut String>,
}

/// The payload of a rev-2 `Value` wire of variant `want`, past its rev, tag
/// and length: the inner type's own wire.
fn varlen_body(wire: &[u8], want: u8) -> Option<&[u8]> {
	let [rev, t, rest @ ..] = wire else {
		return None;
	};
	let t = Tag(*t);
	if *rev != 2 || t.variant_id() != want || !matches!(t.size_class().ok()?, SizeClass::Varlen) {
		return None;
	}
	let len = u32::from_le_bytes(rest.get(..4)?.try_into().ok()?) as usize;
	rest.get(4..4usize.checked_add(len)?)
}

impl<'b> Stored<'b, '_> {
	/// Where `part` lies within the stored value, or `None` when it does not lie within it or
	/// the value is too large for the offsets to hold.
	fn offset(&self, part: &[u8]) -> Option<(u32, u32)> {
		let start = (part.as_ptr() as usize).checked_sub(self.base.as_ptr() as usize)?;
		let end = start.checked_add(part.len())?;
		if end > self.base.len() {
			return None;
		}
		Some((u32::try_from(start).ok()?, u32::try_from(part.len()).ok()?))
	}

	/// The entry whose key UTF-8 is `key` and whose value wire is `value`, or `None` when either
	/// has no [`Self::offset`].
	fn entry(&self, key: &[u8], value: &[u8]) -> Option<Entry> {
		Some(Entry {
			key: self.offset(key)?,
			value: self.offset(value)?,
		})
	}

	/// The bytes an [`Self::offset`] names. An offset is only ever made from a part of `base`, so
	/// its range is within it and its end cannot overflow.
	fn at(&self, span: (u32, u32)) -> &'b [u8] {
		&self.base[span_range(span)]
	}

	/// Write `value`, decoded, at depth `depth`.
	fn encoded(&mut self, value: &Value, depth: usize) -> anyhow::Result<()> {
		match &mut self.json {
			Some(text) => super::row_json::encode_json(self.out, value, depth, text),
			None => encode_value(self.out, value, depth),
		}
	}

	/// Write `scalar` at depth `depth`.
	fn scalar(&mut self, scalar: &Scalar<'_>, depth: usize) -> anyhow::Result<()> {
		encode_scalar(self.out, self.json.as_deref_mut(), scalar, depth)
	}

	/// Open a map of `count` entries.
	fn open_map(&mut self, count: usize) {
		match self.json {
			Some(_) => self.out.push(b'{'),
			None => head(self.out, MAJOR_MAP, count as u64),
		}
	}

	/// Write the key of the map's `at`th entry.
	fn map_key(&mut self, at: usize, key: &str) -> anyhow::Result<()> {
		match self.json {
			Some(_) => {
				if at > 0 {
					self.out.push(b',');
				}
				super::row_json::json_string(self.out, key)?;
				self.out.push(b':');
			}
			None => text(self.out, key),
		}
		Ok(())
	}

	fn close_map(&mut self) {
		if self.json.is_some() {
			self.out.push(b'}');
		}
	}

	/// Open a list of `len` elements.
	fn open_array(&mut self, len: usize) {
		match self.json {
			Some(_) => self.out.push(b'['),
			None => head(self.out, MAJOR_ARRAY, len as u64),
		}
	}

	/// Separate the list's `at`th element from the one before it.
	fn element(&mut self, at: usize) {
		if self.json.is_some() && at > 0 {
			self.out.push(b',');
		}
	}

	fn close_array(&mut self) {
		if self.json.is_some() {
			self.out.push(b']');
		}
	}

	/// Transcode the value wire `wire` at depth `depth`.
	fn value(&mut self, wire: &'b [u8], depth: usize) -> anyhow::Result<()> {
		too_deep(depth)?;
		if let [2, t, ..] = wire {
			let t = Tag(*t);
			match t.variant_id() {
				variant::STRING => {
					if let Some(s) = strand_payload_str(wire) {
						return self.scalar(&Scalar::Str(s), depth);
					}
				}
				variant::OBJECT => {
					if let Some(object) = varlen_body(wire, variant::OBJECT)
						&& self.object(object, None, depth)?
					{
						return Ok(());
					}
				}
				variant::ARRAY => {
					if let Some(array) = varlen_body(wire, variant::ARRAY)
						&& self.array(array, depth)?
					{
						return Ok(());
					}
				}
				variant::RECORD_ID => {
					if let Some(body) = varlen_body(wire, variant::RECORD_ID)
						&& let Some((table, key)) = stored_record_id(body)
					{
						too_deep(depth + 1)?;
						self.record_id_in_place(table, key)?;
						return Ok(());
					}
				}
				_ => {}
			}
		}
		// A scalar kind is read from its payload in place.
		let scalar = super::row::scalar_from_wire(wire)?;
		#[cfg(test)]
		if scalar.as_whole().is_some() {
			VALUE_DECODES.with(|n| n.set(n.get() + 1));
		}
		self.scalar(&scalar, depth)
	}

	/// Transcode the stored object `object` (an `Object` wire), with `id`
	/// taken from the record key `key` when given. `false`, having written
	/// nothing, for an object the walker cannot read.
	fn object(
		&mut self,
		object: &'b [u8],
		key: Option<&[u8]>,
		depth: usize,
	) -> anyhow::Result<bool> {
		too_deep(depth)?;
		let Ok(walker) = indexed_map_walker_from_object_bytes(object) else {
			return Ok(false);
		};
		let mark = self.scratch.len();
		if let Some(entries) = walker.entries() {
			for (k, v) in entries {
				let Some(k) = strand_utf8(k) else {
					self.scratch.truncate(mark);
					return Ok(false);
				};
				let Some(entry) = self.entry(k, v) else {
					self.scratch.truncate(mark);
					return Ok(false);
				};
				self.scratch.push(entry);
			}
		} else {
			let Some(body) = walker.legacy_body() else {
				return Ok(false);
			};
			let mut reader = body;
			for _ in 0..walker.len() {
				let Ok(len) =
					<usize as revision::DeserializeRevisioned>::deserialize_revisioned(&mut reader)
				else {
					self.scratch.truncate(mark);
					return Ok(false);
				};
				let (Some(k), Some(rest)) = (reader.get(..len), reader.get(len..)) else {
					self.scratch.truncate(mark);
					return Ok(false);
				};
				let mut probe = rest;
				if skip_value_wire(&mut probe).is_err() {
					self.scratch.truncate(mark);
					return Ok(false);
				}
				let v = &rest[..rest.len() - probe.len()];
				reader = probe;
				let Some(entry) = self.entry(k, v) else {
					self.scratch.truncate(mark);
					return Ok(false);
				};
				self.scratch.push(entry);
			}
		}
		// A record's `id` is its key, whatever the stored value holds.
		if key.is_some() {
			let base = self.base;
			let mut kept = mark;
			for at in mark..self.scratch.len() {
				let e = self.scratch[at];
				if &base[span_range(e.key)] != ID_KEY.as_bytes() {
					self.scratch[kept] = e;
					kept += 1;
				}
			}
			self.scratch.truncate(kept);
		}
		// Stored keys are ordered by their encoding, length first; the public
		// object orders them by their text.
		let base = self.base;
		let utf8 = |e: &Entry| &base[span_range(e.key)];
		self.scratch[mark..].sort_unstable_by(|a, b| utf8(a).cmp(utf8(b)));
		let count = self.scratch.len() - mark + usize::from(key.is_some());
		self.open_map(count);
		let mut id_written = key.is_none();
		let mut written = 0;
		let mut at = mark;
		while at < self.scratch.len() {
			let entry = self.scratch[at];
			let k = self.at(entry.key);
			if !id_written && ID_KEY.as_bytes() < k {
				self.record_key(written, key.unwrap_or_default(), depth + 1)?;
				written += 1;
				id_written = true;
			}
			let k = std::str::from_utf8(k)
				.map_err(|_| anyhow::anyhow!("an object key that is not UTF-8"))?;
			self.map_key(written, k)?;
			written += 1;
			self.value(self.at(entry.value), depth + 1)?;
			at += 1;
		}
		if !id_written {
			self.record_key(written, key.unwrap_or_default(), depth + 1)?;
		}
		self.close_map();
		self.scratch.truncate(mark);
		Ok(true)
	}

	/// Write the `id` entry, the map's `at`th, for the record key `key`.
	fn record_key(&mut self, at: usize, key: &[u8], depth: usize) -> anyhow::Result<()> {
		self.map_key(at, ID_KEY)?;
		let decoded = crate::key::schema::RecordKey::decode_key(key)
			.map_err(|e| anyhow::anyhow!("Failed to decode record key: {e}"))?;
		let whole = || {
			Value::RecordId(crate::val::RecordId {
				table: decoded.tb.as_ref().clone(),
				key: decoded.id.as_ref().clone(),
			})
		};
		match &mut self.json {
			Some(text_buf) => {
				too_deep(depth + 1)?;
				match decoded.id.as_ref() {
					RecordIdKey::Number(_) | RecordIdKey::String(_) => {
						super::row_json::record_id_text(
							self.out,
							decoded.tb.as_str(),
							decoded.id.as_ref(),
							text_buf,
						)
					}
					_ => super::row_json::encode_json(self.out, &whole(), depth, text_buf),
				}
			}
			None => encode_record_id(self.out, decoded.tb.as_str(), &decoded.id, depth, whole),
		}
	}

	/// Transcode the stored-value object `object`, with `id` taken from the
	/// record key `key` when given. Its keys ascend in byte order, which is the
	/// order the public object holds them in.
	fn stored_object(
		&mut self,
		object: OrderedObjectRef<'b>,
		key: Option<&[u8]>,
		depth: usize,
	) -> anyhow::Result<()> {
		too_deep(depth)?;
		// The entry count is declared before the entries, from a lookup of `id`.
		// A record's `id` is its key, whatever the stored value holds.
		let replaced = key.is_some() && object.get(ID_KEY)?.is_some();
		self.open_map(object.len() - usize::from(replaced) + usize::from(key.is_some()));
		let mut pending = key;
		let mut written = 0;
		for entry in object.iter() {
			let (k, v) = entry?;
			if key.is_some() && k == ID_KEY {
				continue;
			}
			if let Some(record) = pending
				&& ID_KEY < k
			{
				self.record_key(written, record, depth + 1)?;
				written += 1;
				pending = None;
			}
			self.map_key(written, k)?;
			written += 1;
			self.stored_value(v, depth + 1)?;
		}
		if let Some(record) = pending {
			self.record_key(written, record, depth + 1)?;
		}
		self.close_map();
		Ok(())
	}

	/// Transcode the stored-value `value` at depth `depth`.
	fn stored_value(&mut self, value: ValueRef<'b>, depth: usize) -> anyhow::Result<()> {
		too_deep(depth)?;
		match value.kind() {
			Kind::Object => {
				if let Some(object) = value.as_ordered_object()? {
					return self.stored_object(object, None, depth);
				}
			}
			Kind::Array => {
				if let Some(array) = value.as_array()? {
					self.open_array(array.len());
					for (at, element) in array.iter().enumerate() {
						self.element(at);
						self.stored_value(element?, depth + 1)?;
					}
					self.close_array();
					return Ok(());
				}
			}
			Kind::RecordId => {
				if let Some((table, key)) = value.as_record_id()? {
					let key = match key.kind() {
						Kind::Int => key.as_int()?.map(StoredKey::Number),
						Kind::String => key.as_str()?.map(StoredKey::String),
						_ => None,
					};
					if let Some(key) = key {
						too_deep(depth + 1)?;
						self.record_id_in_place(table, key)?;
						return Ok(());
					}
				}
			}
			_ => {}
		}
		let scalar = super::row::scalar_from_stored(value)?;
		#[cfg(test)]
		if scalar.as_whole().is_some() {
			VALUE_DECODES.with(|n| n.set(n.get() + 1));
		}
		self.scalar(&scalar, depth)
	}

	/// Write a record id whose key is a number or a string, read in place.
	fn record_id_in_place(&mut self, table: &str, key: StoredKey<'_>) -> anyhow::Result<()> {
		match &mut self.json {
			Some(text_buf) => match key {
				StoredKey::Number(n) => {
					super::row_json::record_id_text_int(self.out, table, n, text_buf)
				}
				StoredKey::String(s) => {
					super::row_json::record_id_text_str(self.out, table, s, text_buf)
				}
			},
			None => {
				tag(self.out, TAG_RECORDID);
				head(self.out, MAJOR_ARRAY, 2);
				text(self.out, table);
				match key {
					StoredKey::Number(n) => int(self.out, n),
					StoredKey::String(s) => text(self.out, s),
				}
				Ok(())
			}
		}
	}

	/// Transcode the stored array `array` (an `Array` wire). `false`, having
	/// written nothing, for an array the walker cannot read.
	fn array(&mut self, array: &'b [u8], depth: usize) -> anyhow::Result<bool> {
		let Ok(payload) = rev2_optimised_payload_unchecked(array) else {
			return Ok(false);
		};
		let Ok(walker) = IndexedSeqWalker::<'_, Value>::from_payload_unvalidated(payload) else {
			return Ok(false);
		};
		// Find every element before writing any, so a wire that does not read
		// leaves nothing written.
		let len = walker.len();
		if walker.is_indexed() {
			for i in 0..len {
				if walker.element_bytes(i).is_err() {
					return Ok(false);
				}
			}
			self.open_array(len);
			for i in 0..len {
				let element = walker.element_bytes(i)?;
				self.element(i);
				self.value(element, depth + 1)?;
			}
			self.close_array();
		} else {
			let body = walker.body();
			let mut probe = body;
			for _ in 0..len {
				if skip_value_wire(&mut probe).is_err() {
					return Ok(false);
				}
			}
			self.open_array(len);
			let mut reader = body;
			for i in 0..len {
				let mut next = reader;
				skip_value_wire(&mut next)?;
				let element = &reader[..reader.len() - next.len()];
				reader = next;
				self.element(i);
				self.value(element, depth + 1)?;
			}
			self.close_array();
		}
		Ok(true)
	}
}

/// A record id key read in place.
#[derive(Clone, Copy)]
enum StoredKey<'b> {
	Number(i64),
	String(&'b str),
}

/// The table and key of a stored `RecordId` wire (revision 1: `rev, table
/// strand, key`), for a key that is a number or a string.
fn stored_record_id(wire: &[u8]) -> Option<(&str, StoredKey<'_>)> {
	use revision::DeserializeRevisioned as _;
	let mut r = wire;
	if u16::deserialize_revisioned(&mut r).ok()? != 1 {
		return None;
	}
	let table = strand_str(&mut r)?;
	if u16::deserialize_revisioned(&mut r).ok()? != 1 {
		return None;
	}
	let key = match u32::deserialize_revisioned(&mut r).ok()? {
		0 => StoredKey::Number(i64::deserialize_revisioned(&mut r).ok()?),
		1 => StoredKey::String(strand_str(&mut r)?),
		_ => return None,
	};
	r.is_empty().then_some((table, key))
}

/// Read a strand (`varint len || utf8`) from the front of `r`.
fn strand_str<'b>(r: &mut &'b [u8]) -> Option<&'b str> {
	let len = <usize as revision::DeserializeRevisioned>::deserialize_revisioned(r).ok()?;
	let (utf8, rest) = (r.get(..len)?, r.get(len..)?);
	*r = rest;
	std::str::from_utf8(utf8).ok()
}

/// The UTF-8 of a stored strand wire (`varint len || utf8`).
fn strand_utf8(wire: &[u8]) -> Option<&[u8]> {
	let mut r = wire;
	let len = <usize as revision::DeserializeRevisioned>::deserialize_revisioned(&mut r).ok()?;
	(r.len() == len).then_some(r)
}

#[cfg(test)]
mod tests {
	use super::*;

	/// The format the generation-0 samples are written in.
	const DOCUMENT: crate::catalog::RecordFormat = crate::catalog::RecordFormat::Document {
		generation: None,
	};

	/// What the RPC layer writes for `value`.
	fn expected(value: &Value) -> Vec<u8> {
		expected_in(RowFormat::Cbor, value)
	}

	/// What the RPC layer writes for `value` in `format`.
	fn expected_in(format: RowFormat, value: &Value) -> Vec<u8> {
		let public = crate::val::convert_value_to_public_value(value.clone()).expect("converts");
		match format {
			RowFormat::Cbor => crate::rpc::format::cbor::encode(public).expect("encodes"),
			RowFormat::Json => {
				crate::rpc::format::json::encode_str(public).expect("encodes").into_bytes()
			}
		}
	}

	#[test]
	fn a_float_takes_the_shortest_exact_form() {
		for bits in (0u64..=u16::MAX as u64).step_by(7) {
			// Every half widened, and its neighbours either side.
			let half_bits = bits as u16;
			let exp = (half_bits >> 10) & 0x1f;
			let wide = if exp == 0x1f {
				continue;
			} else {
				let sign = if half_bits & 0x8000 != 0 {
					-1.0
				} else {
					1.0
				};
				let man = (half_bits & 0x3ff) as f64;
				sign * if exp == 0 {
					man * 2f64.powi(-24)
				} else {
					(1.0 + man / 1024.0) * 2f64.powi(exp as i32 - 15)
				}
			};
			for v in [wide, f64::from_bits(wide.to_bits() + 1)] {
				let value = Value::Number(Number::Float(v));
				let mut frame = RowFrame::new();
				frame.push_value(&value).expect("encodes");
				assert_eq!(frame.row(0), expected(&value).as_slice(), "{v:e}");
			}
		}
	}

	/// `src`, a SurrealQL value literal, as a value.
	fn val(src: &str) -> Value {
		Value::from(crate::syn::value(src).expect("parses"))
	}

	/// A stored record, in either encoding, transcodes to the bytes its decoded
	/// form encodes to.
	#[test]
	fn a_stored_record_transcodes_as_its_decoded_form_encodes() {
		use crate::key::KVKey as _;

		let mut datas = Vec::new();
		for src in [
			"{}",
			"{ a: 1 }",
			"{ id: 'ignored', b: 'x' }",
			"{ f: t:1, g: t:abc, h: other:-5, i: [t:2] }",
			"{ a: t:⟨with space⟩, b: t:⟨123⟩, c: `select`:x, d: t:⟨a\"b⟩, e: 'q\"\\\\x\\té', \
			   f: 1h30m, g: d'2024-01-02T03:04:05.006Z', h: u'018f9b8e-0000-7000-8000-000000000000', \
			   i: 1.5f, j: 2.25dec, l: NONE, m: NULL, n: false }",
			"{ zz: 1, a: [1, 2, { q: 'r', p: NONE }], m: { k: true, kk: 1.5f, kkk: 2dec } }",
		] {
			datas.push(val(src));
		}
		// Wide enough for the indexed map and seq forms.
		let mut wide = crate::val::Object::default();
		for i in 0..40 {
			wide.insert(format!("f{}", 40 - i), Value::Array((0..i).map(Value::from).collect()));
		}
		datas.push(Value::Object(wide));
		for (i, data) in datas.into_iter().enumerate() {
			let rid = crate::val::RecordId {
				table: "t".into(),
				key: RecordIdKey::Number(i as i64),
			};
			let key = crate::key::schema::RecordKey::new(
				crate::catalog::NamespaceId(1),
				crate::catalog::DatabaseId(2),
				std::borrow::Cow::Borrowed(&rid.table),
				std::borrow::Cow::Borrowed(&rid.key),
			)
			.encode_key()
			.expect("encodes");
			// The record as a writer holds it: its `id` the key's, unless the
			// data names another.
			let mut held = data.clone();
			if let Value::Object(object) = &mut held
				&& !object.contains_key("id")
			{
				object.insert("id".to_owned(), Value::RecordId(rid.clone()));
			}
			let legacy = revision::to_vec(&crate::catalog::Record::new(data)).expect("encodes");
			let decoded =
				crate::exec::operators::scan::pipeline::decode_record(&key, &legacy, None)
					.expect("decodes");
			let keyed =
				crate::catalog::encode_record(DOCUMENT, None, &held, Some(&rid)).expect("encodes");
			let unkeyed =
				crate::catalog::encode_record(DOCUMENT, None, &held, None).expect("encodes");
			for value in [&legacy, &keyed, &unkeyed] {
				assert_eq!(
					crate::exec::operators::scan::pipeline::decode_record(&key, value, None)
						.expect("decodes"),
					decoded,
					"{i}: every encoding decodes alike"
				);
				for format in [RowFormat::Cbor, RowFormat::Json] {
					let mut frame = RowFrame::with_format(format);
					let before = WHOLE_DECODES.with(|n| n.get());
					let values_before = VALUE_DECODES.with(|n| n.get());
					frame.push_stored(&key, value, None).expect("transcodes");
					assert_eq!(WHOLE_DECODES.with(|n| n.get()), before, "transcoded in place");
					assert_eq!(
						VALUE_DECODES.with(|n| n.get()),
						values_before,
						"{i}: leaves in place"
					);
					assert_eq!(
						String::from_utf8_lossy(frame.row(0)),
						String::from_utf8_lossy(&expected_in(format, &decoded)),
						"{format:?} {decoded:?}"
					);
				}
			}
		}
	}

	/// A stored-value record whose object keys are out of order is refused, as
	/// a whole decode refuses it, rather than transcoded to a map whose
	/// declared length its entries do not fill.
	#[test]
	fn a_stored_object_with_keys_out_of_order_is_refused() {
		use crate::key::KVKey as _;

		let rid = crate::val::RecordId {
			table: "t".into(),
			key: RecordIdKey::Number(1),
		};
		let key = crate::key::schema::RecordKey::new(
			crate::catalog::NamespaceId(1),
			crate::catalog::DatabaseId(2),
			std::borrow::Cow::Borrowed(&rid.table),
			std::borrow::Cow::Borrowed(&rid.key),
		)
		.encode_key()
		.expect("encodes");
		let mut value =
			crate::catalog::encode_record(DOCUMENT, None, &val("{ aa: 1, bb: 2 }"), Some(&rid))
				.expect("encodes");
		let at = |v: &[u8], needle: &[u8]| {
			v.windows(needle.len()).position(|w| w == needle).expect("the key is stored")
		};
		let (a, b) = (at(&value, b"aa"), at(&value, b"bb"));
		value[a..a + 2].copy_from_slice(b"bb");
		value[b..b + 2].copy_from_slice(b"aa");
		assert!(
			crate::exec::operators::scan::pipeline::decode_record(&key, &value, None).is_err(),
			"a whole decode refuses it"
		);
		for format in [RowFormat::Cbor, RowFormat::Json] {
			let mut frame = RowFrame::with_format(format);
			assert!(frame.push_stored(&key, &value, None).is_err(), "{format:?}");
		}
	}

	/// A stored-value record whose data is no object is decoded whole, and
	/// one that does not parse fails the row and writes nothing.
	#[test]
	fn a_stored_value_record_not_transcoded_in_place() {
		use crate::key::KVKey as _;

		let rid = crate::val::RecordId {
			table: "t".into(),
			key: RecordIdKey::Number(1),
		};
		let key = crate::key::schema::RecordKey::new(
			crate::catalog::NamespaceId(1),
			crate::catalog::DatabaseId(2),
			std::borrow::Cow::Borrowed(&rid.table),
			std::borrow::Cow::Borrowed(&rid.key),
		)
		.encode_key()
		.expect("encodes");
		let value =
			crate::catalog::encode_record(DOCUMENT, None, &Value::from(5), None).expect("encodes");
		let mut frame = RowFrame::new();
		let before = WHOLE_DECODES.with(|n| n.get());
		frame.push_stored(&key, &value, None).expect("transcodes");
		assert_eq!(WHOLE_DECODES.with(|n| n.get()), before + 1, "decoded whole");
		assert_eq!(frame.row(0), expected(&Value::from(5)).as_slice());
		let broken = [crate::catalog::StoredRecord::MARKER, 0x80, 0x00];
		assert!(frame.push_stored(&key, &broken, None).is_err());
		assert_eq!(frame.len(), 1, "the failed row is not kept");
	}
}
