//! The reference decoder: blocks back into public values.

use std::collections::BTreeMap;
use std::sync::Arc;

use rust_decimal::Decimal;

use super::shape::{Kind, Reader, Root, Shape};
use super::{
	CBOR_TAG, MAX_EMPTY_ROWS, MAX_SHAPES, MAY_BE_ABSENT, MAY_BE_NULL, MESSAGE_BATCH, MESSAGE_SHAPE,
	NO_SLOT, STATE_ABSENT, STATE_NONE, STATE_NULL, STATE_VALUE, ShapedError, VERSION,
};
use crate::{Bytes, Datetime, Duration, Number, Object, RecordId, RecordIdKey, Table, Uuid, Value};

/// The contents of a block from its CBOR item: tag [`CBOR_TAG`] around a definite-length byte
/// string, which must end the item.
pub fn block_contents(item: &[u8]) -> Result<&[u8], ShapedError> {
	let bad = || ShapedError::new("not a shaped block");
	let mut r = item;
	let (major, tag) = cbor_head(&mut r).ok_or_else(bad)?;
	if major != 6 || tag != CBOR_TAG {
		return Err(bad());
	}
	let (major, len) = cbor_head(&mut r).ok_or_else(bad)?;
	if major != 2 || !usize::try_from(len).is_ok_and(|len| len == r.len()) {
		return Err(bad());
	}
	Ok(r)
}

/// A CBOR head's major type and argument, for a definite argument.
fn cbor_head(r: &mut &[u8]) -> Option<(u8, u64)> {
	let (&first, rest) = r.split_first()?;
	let major = first >> 5;
	let (n, rest) = match first & 0x1f {
		n @ 0..=23 => (u64::from(n), rest),
		24 => (u64::from(*rest.first()?), rest.get(1..)?),
		25 => (u64::from(u16::from_be_bytes(rest.get(..2)?.try_into().ok()?)), rest.get(2..)?),
		26 => (u64::from(u32::from_be_bytes(rest.get(..4)?.try_into().ok()?)), rest.get(4..)?),
		27 => (u64::from_be_bytes(rest.get(..8)?.try_into().ok()?), rest.get(8..)?),
		_ => return None,
	};
	*r = rest;
	Some((major, n))
}

/// A decoder of one dynamic slice, given the slice and the depth ([`Shape::depth`]) of the shape
/// it belongs to.
pub type Dynamic<'a> = dyn FnMut(&[u8], usize) -> Result<Value, String> + 'a;

/// What reading blocks may materialise, in units of about a byte of memory: [`VALUE_UNITS`] per
/// row and per array element, [`VALUE_UNITS`] + [`ENTRY_UNITS`] + the name's length per present
/// column, a column-level record table's length per record value, the bytes of every variable
/// array element, and [`VALUE_UNITS`] per byte of every dynamic slice (CBOR can decode a value
/// from each byte). One budget is shared by every block of a request, so what a request
/// materialises is bounded by a multiple of its bytes.
#[derive(Debug)]
pub struct Budget {
	units: u64,
	/// The deepest [`Shape::depth`] a shape announced in a block may have.
	pub max_shape_depth: usize,
}

impl Budget {
	/// A budget of `units`, for shapes of any depth the format allows.
	pub fn new(units: u64) -> Self {
		Self {
			units,
			max_shape_depth: usize::MAX,
		}
	}

	/// A budget that no reading exhausts.
	pub fn unbounded() -> Self {
		Self::new(u64::MAX)
	}

	/// Spends `units`, or refuses when fewer remain.
	pub fn charge(&mut self, units: u64) -> Result<(), ShapedError> {
		self.units = self.units.checked_sub(units).ok_or_else(|| {
			ShapedError::new("a shaped block decodes to more than its request's budget")
		})?;
		Ok(())
	}
}

/// The units a materialised value costs.
pub const VALUE_UNITS: u64 = 64;
/// The units an object entry costs beyond its value and its name's bytes.
pub const ENTRY_UNITS: u64 = 32;

/// The shapes one scope has announced, reading its blocks in order.
#[derive(Debug, Default)]
pub struct BlockReader {
	shapes: BTreeMap<u32, Arc<Shape>>,
}

impl BlockReader {
	/// A reader of a scope that has announced no shape.
	pub fn new() -> Self {
		Self::default()
	}

	/// The rows of the block whose contents are `contents` ([`block_contents`]), with every
	/// dynamic slice decoded by `dynamic` (one CBOR item, which the decoder must bound in depth).
	/// The shapes it announces hold for the blocks this reader reads after it.
	pub fn read_block(
		&mut self,
		contents: &[u8],
		dynamic: &mut dyn FnMut(&[u8]) -> Result<Value, String>,
	) -> Result<Vec<Value>, ShapedError> {
		self.read_block_within(contents, &mut |slice, _| dynamic(slice), &mut Budget::unbounded())
	}

	/// [`read_block`](Self::read_block) charging what it materialises to `budget`, and refusing a
	/// shape nested deeper than the budget's [`max_shape_depth`](Budget::max_shape_depth).
	/// `dynamic` is given each slice and the depth of the shape it belongs to.
	pub fn read_block_within(
		&mut self,
		contents: &[u8],
		dynamic: &mut Dynamic,
		budget: &mut Budget,
	) -> Result<Vec<Value>, ShapedError> {
		let mut r = Reader(contents);
		if r.u8()? != VERSION {
			return Err(ShapedError::new("an unknown block version"));
		}
		let mut rows = Vec::new();
		while !r.0.is_empty() {
			let kind = r.u8()?;
			let len = r.u32()? as usize;
			let payload = r.take(len)?;
			match kind {
				MESSAGE_SHAPE => {
					let mut p = Reader(payload);
					let id = p.u32()?;
					let shape = Shape::decode(p.0)?;
					if shape.depth() > budget.max_shape_depth {
						return Err(ShapedError::new("a shape nested too deep"));
					}
					if !self.shapes.contains_key(&id) && self.shapes.len() >= MAX_SHAPES {
						return Err(ShapedError::new("too many shapes in one scope"));
					}
					self.shapes.insert(id, Arc::new(shape));
				}
				MESSAGE_BATCH => {
					let mut p = Reader(payload);
					let id = p.u32()?;
					let count = p.u32()? as usize;
					let shape = self
						.shapes
						.get(&id)
						.cloned()
						.ok_or_else(|| ShapedError::new("a batch of an unannounced shape"))?;
					// Each row takes at least its header, or one byte; the count cannot reserve
					// more than the batch could hold.
					let least = shape.header_len().max(1);
					rows.reserve(count.min(p.0.len() / least));
					// A row of a zero-width shape takes no bytes, so the payload cannot bound
					// the count; a batch of them is bounded by MAX_EMPTY_ROWS instead.
					if shape.header_len() == 0 && count > MAX_EMPTY_ROWS as usize {
						return Err(ShapedError::new("a batch of too many empty rows"));
					}
					let mut bytes = p.0;
					for _ in 0..count {
						budget.charge(VALUE_UNITS)?;
						let (value, used) = decode_row_within(&shape, bytes, dynamic, budget)?;
						rows.push(value);
						bytes = &bytes[used..];
					}
					if !bytes.is_empty() {
						return Err(ShapedError::new("bytes past a batch's rows"));
					}
				}
				// A message this version does not know is skipped.
				_ => {}
			}
		}
		Ok(rows)
	}
}

/// One row of `shape` at the start of `bytes`: its value and its length.
pub fn decode_row(
	shape: &Shape,
	bytes: &[u8],
	dynamic: &mut dyn FnMut(&[u8]) -> Result<Value, String>,
) -> Result<(Value, usize), ShapedError> {
	decode_row_within(shape, bytes, &mut |slice, _| dynamic(slice), &mut Budget::unbounded())
}

/// [`decode_row`] charging what it materialises to `budget`.
pub fn decode_row_within(
	shape: &Shape,
	bytes: &[u8],
	dynamic: &mut Dynamic,
	budget: &mut Budget,
) -> Result<(Value, usize), ShapedError> {
	let def = shape.def();
	let header = shape.header_len();
	let head = bytes.get(..header).ok_or_else(|| ShapedError::new("a truncated row"))?;
	let presence = &head[..shape.presence_len()];
	// Every presence bit past the last state is zero.
	let used_bits = def.presence_bits as usize;
	for (at, byte) in presence.iter().enumerate() {
		let low = 8 * at;
		if low + 8 > used_bits {
			let keep = used_bits.saturating_sub(low);
			if byte >> keep != 0 {
				return Err(ShapedError::new("presence padding bits set"));
			}
		}
	}
	let fixed = &head[shape.presence_len()..shape.ends_at()];
	let ends_bytes = &head[shape.ends_at()..];
	let mut ends = Vec::with_capacity(def.var_count as usize);
	let mut last = 0u32;
	for chunk in ends_bytes.chunks_exact(4) {
		let end = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
		if end < last {
			return Err(ShapedError::new("var ends out of order"));
		}
		last = end;
		ends.push(end);
	}
	let total =
		header.checked_add(last as usize).ok_or_else(|| ShapedError::new("a row too long"))?;
	let var = bytes.get(header..total).ok_or_else(|| ShapedError::new("a truncated row"))?;
	let row = Row {
		shape,
		presence,
		fixed,
		ends: &ends,
		var,
	};
	let value = match def.root {
		Root::Value => decode_dynamic(row.slice(def.root_rest)?, shape.depth(), dynamic, budget)?,
		Root::Object => Value::Object(row.node(None, dynamic, budget)?),
	};
	Ok((value, total))
}

struct Row<'r> {
	shape: &'r Shape,
	presence: &'r [u8],
	fixed: &'r [u8],
	ends: &'r [u32],
	var: &'r [u8],
}

impl Row<'_> {
	fn bit(&self, at: u32) -> Result<bool, ShapedError> {
		let byte = self
			.presence
			.get((at / 8) as usize)
			.ok_or_else(|| ShapedError::new("a presence bit out of range"))?;
		Ok(byte & (1 << (at % 8)) != 0)
	}

	/// `column`'s presence state, read from the bits its flags give it.
	fn state(&self, column: &super::Column) -> Result<u8, ShapedError> {
		let p = column.presence;
		Ok(match column.presence_width() {
			0 => STATE_VALUE,
			1 => match (self.bit(p)?, column.flags & MAY_BE_ABSENT != 0) {
				(true, _) => STATE_VALUE,
				(false, true) => STATE_ABSENT,
				(false, false) => STATE_NULL,
			},
			_ => u8::from(self.bit(p)?) | (u8::from(self.bit(p + 1)?) << 1),
		})
	}

	fn fixed_bit(&self, at: u32) -> Result<bool, ShapedError> {
		let byte = self
			.fixed
			.get((at / 8) as usize)
			.ok_or_else(|| ShapedError::new("a fixed bit out of range"))?;
		Ok(byte & (1 << (at % 8)) != 0)
	}

	fn slice(&self, slot: u32) -> Result<&[u8], ShapedError> {
		let slot = slot as usize;
		let end =
			*self.ends.get(slot).ok_or_else(|| ShapedError::new("a var slot out of range"))?;
		let start = match slot {
			0 => 0,
			_ => self.ends[slot - 1],
		};
		self.var
			.get(start as usize..end as usize)
			.ok_or_else(|| ShapedError::new("a var slice out of range"))
	}

	fn fixed(&self, offset: u32, width: usize) -> Result<&[u8], ShapedError> {
		let start = offset as usize;
		self.fixed
			.get(start..start + width)
			.ok_or_else(|| ShapedError::new("a fixed slot out of range"))
	}

	fn node(
		&self,
		node: Option<usize>,
		dynamic: &mut Dynamic,
		budget: &mut Budget,
	) -> Result<Object, ShapedError> {
		let mut object = BTreeMap::new();
		let rest = self.shape.rest(node);
		if rest != NO_SLOT {
			let slice = self.slice(rest)?;
			if !slice.is_empty() {
				if let Value::Object(entries) =
					decode_dynamic(slice, self.shape.depth(), dynamic, budget)?
				{
					object = entries.into_inner();
				} else {
					return Err(ShapedError::new("a remainder that is not an object"));
				}
			}
		}
		for at in self.shape.children(node) {
			let column = &self.shape.columns()[at];
			let state = self.state(column)?;
			if state != STATE_ABSENT {
				budget.charge(VALUE_UNITS + ENTRY_UNITS + column.name.len() as u64)?;
			}
			let value = match state {
				STATE_ABSENT if column.flags & MAY_BE_ABSENT != 0 => continue,
				STATE_NULL if column.flags & MAY_BE_NULL != 0 => Value::Null,
				STATE_NONE if column.flags & MAY_BE_ABSENT != 0 => Value::None,
				STATE_VALUE => self.leaf(at, dynamic, budget)?,
				_ => return Err(ShapedError::new("a presence state the column does not allow")),
			};
			if object.insert(column.name.clone(), value).is_some() {
				return Err(ShapedError::new("a remainder key that has a column"));
			}
		}
		Ok(Object::from(object))
	}

	fn leaf(
		&self,
		at: usize,
		dynamic: &mut Dynamic,
		budget: &mut Budget,
	) -> Result<Value, ShapedError> {
		let column = &self.shape.columns()[at];
		let bad = ShapedError::new;
		let slot = column.slot;
		Ok(match column.kind {
			Kind::Bool => match self.fixed(slot, 1)?[0] {
				0 => Value::Bool(false),
				1 => Value::Bool(true),
				_ => return Err(bad("a bool that is neither 0 nor 1")),
			},
			Kind::Int => {
				Value::Number(Number::Int(i64::from_le_bytes(array(self.fixed(slot, 8)?))))
			}
			Kind::Float => Value::Number(Number::Float(f64::from_bits(u64::from_le_bytes(array(
				self.fixed(slot, 8)?,
			))))),
			Kind::Datetime => {
				let b = self.fixed(slot, 12)?;
				let secs = i64::from_le_bytes(array(&b[..8]));
				let nanos = u32::from_le_bytes(array(&b[8..]));
				Value::Datetime(
					Datetime::try_from((secs, nanos))
						.map_err(|_| bad("a datetime out of range"))?,
				)
			}
			Kind::Duration => {
				let b = self.fixed(slot, 12)?;
				let secs = u64::from_le_bytes(array(&b[..8]));
				let nanos = u32::from_le_bytes(array(&b[8..]));
				if nanos >= 1_000_000_000 {
					return Err(bad("a duration's nanoseconds out of range"));
				}
				Value::Duration(Duration(std::time::Duration::new(secs, nanos)))
			}
			Kind::Uuid => Value::Uuid(Uuid(::uuid::Uuid::from_bytes(array(self.fixed(slot, 16)?)))),
			Kind::String => Value::String(
				std::str::from_utf8(self.slice(slot)?)
					.map_err(|_| bad("a string that is not UTF-8"))?
					.to_owned(),
			),
			Kind::Bytes => Value::Bytes(Bytes(::bytes::Bytes::copy_from_slice(self.slice(slot)?))),
			Kind::Decimal => {
				let text = std::str::from_utf8(self.slice(slot)?)
					.map_err(|_| bad("a decimal that is not UTF-8"))?;
				// Read as the CBOR format reads tag 10: parsed, then normalized.
				#[allow(clippy::disallowed_methods)]
				let decimal = <Decimal as std::str::FromStr>::from_str(text)
					.map_err(|_| bad("an invalid decimal"))?
					.normalize();
				Value::Number(Number::Decimal(decimal))
			}
			Kind::Record => {
				if !column.table.is_empty() {
					budget.charge(column.table.len() as u64)?;
				}
				record(self.slice(slot)?, &column.table, self.shape.depth(), dynamic, budget)?
			}
			Kind::Dynamic => {
				decode_dynamic(self.slice(slot)?, self.shape.depth(), dynamic, budget)?
			}
			Kind::Object => Value::Object(self.node(Some(at), dynamic, budget)?),
			Kind::Bit => Value::Bool(self.fixed_bit(slot)?),
			Kind::Array => array_of(
				self.slice(slot)?,
				column.element.ok_or_else(|| bad("an array of no element kind"))?,
				budget,
			)?,
			Kind::I8 => Value::Number(Number::I8(i8::from_le_bytes(array(self.fixed(slot, 1)?)))),
			Kind::I16 => {
				Value::Number(Number::I16(i16::from_le_bytes(array(self.fixed(slot, 2)?))))
			}
			Kind::I32 => {
				Value::Number(Number::I32(i32::from_le_bytes(array(self.fixed(slot, 4)?))))
			}
			Kind::U8 => Value::Number(Number::U8(u8::from_le_bytes(array(self.fixed(slot, 1)?)))),
			Kind::U16 => {
				Value::Number(Number::U16(u16::from_le_bytes(array(self.fixed(slot, 2)?))))
			}
			Kind::U32 => {
				Value::Number(Number::U32(u32::from_le_bytes(array(self.fixed(slot, 4)?))))
			}
			Kind::U64 => {
				Value::Number(Number::U64(u64::from_le_bytes(array(self.fixed(slot, 8)?))))
			}
			Kind::F16 => Value::Number(Number::F16(half::f16::from_bits(u16::from_le_bytes(
				array(self.fixed(slot, 2)?),
			)))),
			Kind::F32 => Value::Number(Number::F32(f32::from_bits(u32::from_le_bytes(array(
				self.fixed(slot, 4)?,
			))))),
			Kind::DatetimeNs => {
				let ns = i64::from_le_bytes(array(self.fixed(slot, 8)?));
				let (secs, nanos) = if ns == i64::MIN || ns == i64::MAX {
					let b = self.slice(column.rest)?;
					if b.len() != 12 {
						return Err(bad("a datetime overflow that is not 12 bytes"));
					}
					(i64::from_le_bytes(array(&b[..8])), u32::from_le_bytes(array(&b[8..])))
				} else {
					(ns.div_euclid(1_000_000_000), ns.rem_euclid(1_000_000_000) as u32)
				};
				Value::Datetime(
					Datetime::try_from((secs, nanos))
						.map_err(|_| bad("a datetime out of range"))?,
				)
			}
			Kind::DurationNs => {
				let ns = u64::from_le_bytes(array(self.fixed(slot, 8)?));
				if ns == u64::MAX {
					let b = self.slice(column.rest)?;
					if b.len() != 12 {
						return Err(bad("a duration overflow that is not 12 bytes"));
					}
					let secs = u64::from_le_bytes(array(&b[..8]));
					let nanos = u32::from_le_bytes(array(&b[8..]));
					if nanos >= 1_000_000_000 {
						return Err(bad("a duration's nanoseconds out of range"));
					}
					Value::Duration(Duration(std::time::Duration::new(secs, nanos)))
				} else {
					Value::Duration(Duration(std::time::Duration::from_nanos(ns)))
				}
			}
		})
	}
}

/// An `array` slice of elements of `kind`.
fn array_of(slice: &[u8], kind: Kind, budget: &mut Budget) -> Result<Value, ShapedError> {
	let bad = ShapedError::new;
	let mut r = Reader(slice);
	let count = r.u32()? as usize;
	if kind.is_var() {
		let ends = r.take(count.checked_mul(4).ok_or_else(|| bad("an array too long"))?)?;
		let bytes = r.0;
		budget.charge(
			(count as u64).saturating_mul(VALUE_UNITS).saturating_add(bytes.len() as u64),
		)?;
		let mut out = Vec::with_capacity(count.min(slice.len()));
		let mut start = 0usize;
		for at in 0..count {
			let end = u32::from_le_bytes(array(&ends[4 * at..4 * at + 4])) as usize;
			let element =
				bytes.get(start..end).ok_or_else(|| bad("an array element out of range"))?;
			out.push(if kind == Kind::String {
				Value::String(
					std::str::from_utf8(element)
						.map_err(|_| bad("a string that is not UTF-8"))?
						.to_owned(),
				)
			} else if kind == Kind::Bytes {
				Value::Bytes(Bytes(::bytes::Bytes::copy_from_slice(element)))
			} else {
				let text =
					std::str::from_utf8(element).map_err(|_| bad("a decimal that is not UTF-8"))?;
				#[allow(clippy::disallowed_methods)]
				let decimal = <Decimal as std::str::FromStr>::from_str(text)
					.map_err(|_| bad("an invalid decimal"))?
					.normalize();
				Value::Number(Number::Decimal(decimal))
			});
			start = end;
		}
		if start != bytes.len() {
			return Err(bad("bytes past an array's elements"));
		}
		return Ok(Value::Array(out.into()));
	}
	let width = kind.fixed_width().ok_or_else(|| bad("an invalid array element kind"))? as usize;
	if r.0.len() != count.checked_mul(width).ok_or_else(|| bad("an array too long"))? {
		return Err(bad("an array of the wrong length"));
	}
	budget.charge((count as u64).saturating_mul(VALUE_UNITS))?;
	let mut out = Vec::with_capacity(count.min(slice.len()));
	for element in r.0.chunks_exact(width) {
		out.push(match kind {
			Kind::Bool => match element[0] {
				0 => Value::Bool(false),
				1 => Value::Bool(true),
				_ => return Err(bad("a bool that is neither 0 nor 1")),
			},
			Kind::I8 => Value::Number(Number::I8(i8::from_le_bytes(array(element)))),
			Kind::U8 => Value::Number(Number::U8(u8::from_le_bytes(array(element)))),
			Kind::I16 => Value::Number(Number::I16(i16::from_le_bytes(array(element)))),
			Kind::U16 => Value::Number(Number::U16(u16::from_le_bytes(array(element)))),
			Kind::I32 => Value::Number(Number::I32(i32::from_le_bytes(array(element)))),
			Kind::U32 => Value::Number(Number::U32(u32::from_le_bytes(array(element)))),
			Kind::Int => int(i64::from_le_bytes(array(element))),
			Kind::U64 => Value::Number(Number::U64(u64::from_le_bytes(array(element)))),
			Kind::F16 => {
				Value::Number(Number::F16(half::f16::from_bits(u16::from_le_bytes(array(element)))))
			}
			Kind::F32 => {
				Value::Number(Number::F32(f32::from_bits(u32::from_le_bytes(array(element)))))
			}
			Kind::Float => {
				Value::Number(Number::Float(f64::from_bits(u64::from_le_bytes(array(element)))))
			}
			Kind::Uuid => Value::Uuid(Uuid(::uuid::Uuid::from_bytes(array(element)))),
			Kind::Datetime => {
				let secs = i64::from_le_bytes(array(&element[..8]));
				let nanos = u32::from_le_bytes(array(&element[8..]));
				Value::Datetime(
					Datetime::try_from((secs, nanos))
						.map_err(|_| bad("a datetime out of range"))?,
				)
			}
			Kind::Duration => {
				let secs = u64::from_le_bytes(array(&element[..8]));
				let nanos = u32::from_le_bytes(array(&element[8..]));
				if nanos >= 1_000_000_000 {
					return Err(bad("a duration's nanoseconds out of range"));
				}
				Value::Duration(Duration(std::time::Duration::new(secs, nanos)))
			}
			Kind::String
			| Kind::Bytes
			| Kind::Decimal
			| Kind::Record
			| Kind::Dynamic
			| Kind::Object
			| Kind::Bit
			| Kind::Array
			| Kind::DatetimeNs
			| Kind::DurationNs => return Err(bad("an invalid array element kind")),
		});
	}
	Ok(Value::Array(out.into()))
}

fn int(v: i64) -> Value {
	Value::Number(Number::Int(v))
}

fn array<const N: usize>(bytes: &[u8]) -> [u8; N] {
	let mut out = [0u8; N];
	out.copy_from_slice(&bytes[..N]);
	out
}

fn decode_dynamic(
	slice: &[u8],
	depth: usize,
	dynamic: &mut Dynamic,
	budget: &mut Budget,
) -> Result<Value, ShapedError> {
	if slice.is_empty() {
		return Err(ShapedError::new("an empty dynamic slice"));
	}
	budget.charge((slice.len() as u64).saturating_mul(VALUE_UNITS))?;
	dynamic(slice, depth).map_err(|e| ShapedError::new(format!("a dynamic value: {e}")))
}

fn record(
	slice: &[u8],
	column_table: &str,
	depth: usize,
	dynamic: &mut Dynamic,
	budget: &mut Budget,
) -> Result<Value, ShapedError> {
	let bad = ShapedError::new;
	let mut r = Reader(slice);
	let code = r.u8()?;
	if code == 3 {
		let value = decode_dynamic(r.0, depth, dynamic, budget)?;
		let Value::RecordId(id) = &value else {
			return Err(bad("a record held as CBOR that is not a record id"));
		};
		if !column_table.is_empty() && *id.table != *column_table {
			return Err(bad("a record held as CBOR that names another table than its column"));
		}
		return Ok(value);
	}
	let table = if column_table.is_empty() {
		let len = r.u16()? as usize;
		std::str::from_utf8(r.take(len)?).map_err(|_| bad("a table that is not UTF-8"))?
	} else {
		column_table
	};
	let key = match code {
		0 => {
			let id = r.take(8)?;
			if !r.0.is_empty() {
				return Err(bad("bytes past a record's number id"));
			}
			RecordIdKey::Number(i64::from_le_bytes(array(id)))
		}
		1 => RecordIdKey::String(
			std::str::from_utf8(r.0).map_err(|_| bad("a record id that is not UTF-8"))?.to_owned(),
		),
		2 => {
			let id = r.take(16)?;
			if !r.0.is_empty() {
				return Err(bad("bytes past a record's uuid id"));
			}
			RecordIdKey::Uuid(Uuid(::uuid::Uuid::from_bytes(array(id))))
		}
		_ => return Err(bad("an unknown record key kind")),
	};
	Ok(Value::RecordId(RecordId {
		table: Table::new(table),
		key,
	}))
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::shaped::shape::{ColumnDef, Shape, ShapeDef};
	use crate::shaped::write::RowWriter;

	/// An `F32` slot decodes to `Number::F32` preserving signaling NaN bits verbatim.
	#[test]
	fn f32_leaf_preserves_signaling_nan_bits() {
		let shape = Shape::new(ShapeDef::object(
			false,
			&[ColumnDef {
				name: "f".into(),
				kind: Kind::F32,
				element: None,
				flags: 0,
				table: String::new(),
				open: false,
				children: vec![],
			}],
		))
		.expect("shape");

		for f32_bits in [0x7f80_0001u32, 0xff80_0001u32, 0x7fc0_0001u32] {
			let mut row = Vec::new();
			let mut writer = RowWriter::begin(&mut row, &shape);
			writer.fixed(0, &f32_bits.to_le_bytes());
			writer.finish().expect("finishes");

			let (decoded, len) =
				decode_row(&shape, &row, &mut |_| unreachable!()).expect("decodes");
			assert_eq!(len, row.len());
			let Value::Object(obj) = decoded else {
				panic!("expected object, got {decoded:?}");
			};
			let Some(Value::Number(Number::F32(v))) = obj.get("f") else {
				panic!("expected f32 field f, got {obj:?}");
			};
			assert_eq!(v.to_bits(), f32_bits, "f32 bits {f32_bits:#010x}");
		}
	}
}
