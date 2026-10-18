//! The kind table: what the codec does with each [`PhysicalKind`].
//!
//! [`ops`] holds the codec's one `match` on a physical kind, exhaustive and with no wildcard, so a
//! kind added to the enum fails to compile until it has a row here. A row is plain data: the kind's
//! escape, how a value is written into its slot, how a present slot decodes whole, how it is
//! checked, and how a checked slot is read in place. The check of a kind is its decode instantiated
//! to build nothing, so the two refuse the same slots with the same error. Kinds that differ only
//! in width share their functions and read the width from the slot they are given. The row
//! grammar itself, in `row.rs` and `encode.rs`, matches only on a column's location, presence and
//! role.

use chrono::{DateTime, Utc};
use surrealdb_types::Decimal;

use super::mode::{Build, Check, Mode};
use super::row::FieldRef;
use super::{DATETIME_ABOVE, DATETIME_BELOW, DURATION_ESCAPE, PackedError, is_datetime_escape};
use crate::layout::{Column, PhysicalKind, Presence};
use crate::val::stored::{self, Encoder, ValueRef};
use crate::val::{self, Number, RecordIdKey, TableName, Value};

/// The width of the trailer payload of every kind that escapes: the 12-byte DATETIME or DURATION
/// payload of the stored grammar.
pub(super) const ESCAPE_WIDTH: usize = 12;

/// A present column's slot as a row holds it.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Slot<'a> {
	/// A bool column's bit.
	Bit(bool),
	/// A fixed slot of exactly the kind's width, not at an escape value.
	Fixed(&'a [u8]),
	/// A fixed slot at an escape value, with the payload of its trailer entry.
	Escaped {
		slot: &'a [u8],
		payload: &'a [u8],
	},
	/// A variable slice whose end offsets have been checked.
	Var(&'a [u8]),
}

/// Where a written value goes.
pub(crate) enum Dest<'o> {
	/// The bool column's bit. The caller sets the bit in the row when the writer sets this to
	/// `true`.
	Bit(&'o mut bool),
	/// The fixed slot, zeroed, exactly the kind's width.
	Fixed(&'o mut [u8]),
	/// The variable region, appended to. `enc` is the row's scratch encoder, which the writer
	/// clears before it uses it, and `depth` is the depth a grammar value of the column is encoded
	/// at: the length of the column's path.
	Var {
		out: &'o mut Vec<u8>,
		enc: &'o mut Encoder,
		depth: usize,
	},
}

/// What writing a value into a column did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Written {
	/// The value is in the destination.
	Fits,
	/// The slot holds an escape value, and this is the payload of the trailer entry that holds the
	/// value.
	Escaped([u8; ESCAPE_WIDTH]),
	/// The value is not one the column can hold. The destination is unspecified.
	Misfit,
}

/// The trailer behaviour of a kind whose fixed slot can hold an escape value.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Escape {
	/// The width of the payload of a trailer entry.
	pub(crate) width: usize,
	/// Whether a slot of exactly the kind's width holds one of the kind's escape values.
	pub(crate) at_escape: fn(&[u8]) -> bool,
}

/// Writes a present value into `dest`, or reports that the column cannot hold it. Only a value no
/// layout can store (a closure, or one nested past the grammar's depth limit) and a destination of
/// the wrong shape for the kind are errors.
pub(crate) type WriteFn = fn(&Column, &Value, Dest<'_>) -> Result<Written, stored::Error>;

/// Reads a present slot in place. `index` is the column's index in its descriptor, for errors. A
/// `NULL` value slice answers [`FieldRef::Null`].
pub(crate) type ReadFn = for<'a> fn(&'a Column, u32, Slot<'a>) -> Result<FieldRef<'a>, PackedError>;

/// Decodes a present slot whole. The last argument is the depth a grammar value of the column is
/// decoded at: the length of the column's path.
pub(crate) type DecodeFn = fn(&Column, u32, Slot<'_>, usize) -> Result<Value, PackedError>;

/// Checks a present slot: [`DecodeFn`] run in [`Check`](super::mode::Check) mode, which refuses
/// what the decode refuses with the same error and builds nothing.
pub(crate) type CheckFn = fn(&Column, u32, Slot<'_>, usize) -> Result<(), PackedError>;

/// What the codec does with the values of one physical kind: one row of the kind table.
#[derive(Clone, Copy, Debug)]
pub(crate) struct KindOps {
	/// The kind's escape, when its fixed slot can hold a value that lives in the trailer.
	pub(crate) escape: Option<Escape>,
	/// Writes a present value. A `NONE` is never written, and an absent or `NULL` column is
	/// written empty by the row grammar without calling this.
	pub(crate) write: WriteFn,
	/// Reads a present slot in place, for a row that has been checked.
	pub(crate) read: ReadFn,
	/// Decodes a present slot whole.
	pub(crate) decode: DecodeFn,
	/// Checks a present slot: the same function as `decode`, instantiated to build nothing.
	pub(crate) check: CheckFn,
}

/// The kind table.
pub(crate) fn ops(kind: PhysicalKind) -> &'static KindOps {
	match kind {
		PhysicalKind::Bool => &KindOps {
			escape: None,
			write: write_bool,
			read: read_bool,
			decode: decode_bool::<Build>,
			check: decode_bool::<Check>,
		},
		PhysicalKind::I8 | PhysicalKind::I16 | PhysicalKind::I32 | PhysicalKind::I64 => &KindOps {
			escape: None,
			write: write_signed,
			read: read_signed,
			decode: decode_signed::<Build>,
			check: decode_signed::<Check>,
		},
		PhysicalKind::U8 | PhysicalKind::U16 | PhysicalKind::U32 | PhysicalKind::U64 => &KindOps {
			escape: None,
			write: write_unsigned,
			read: read_unsigned,
			decode: decode_unsigned::<Build>,
			check: decode_unsigned::<Check>,
		},
		PhysicalKind::F32 => &KindOps {
			escape: None,
			write: write_f32,
			read: read_f32,
			decode: decode_f32::<Build>,
			check: decode_f32::<Check>,
		},
		PhysicalKind::F64 => &KindOps {
			escape: None,
			write: write_f64,
			read: read_f64,
			decode: decode_f64::<Build>,
			check: decode_f64::<Check>,
		},
		PhysicalKind::Decimal16 => &KindOps {
			escape: None,
			write: write_decimal16,
			read: read_decimal16,
			decode: decode_decimal16::<Build>,
			check: decode_decimal16::<Check>,
		},
		PhysicalKind::Fixed(_) => &KindOps {
			escape: None,
			write: write_fixed_bytes,
			read: read_fixed_bytes,
			decode: decode_fixed_bytes::<Build>,
			check: decode_fixed_bytes::<Check>,
		},
		PhysicalKind::Uuid => &KindOps {
			escape: None,
			write: write_uuid,
			read: read_uuid,
			decode: decode_uuid::<Build>,
			check: decode_uuid::<Check>,
		},
		PhysicalKind::Datetime => &KindOps {
			escape: Some(Escape {
				width: ESCAPE_WIDTH,
				at_escape: datetime_at_escape,
			}),
			write: write_datetime,
			read: read_datetime,
			decode: decode_datetime::<Build>,
			check: decode_datetime::<Check>,
		},
		PhysicalKind::DatetimeWide => &KindOps {
			escape: None,
			write: write_datetime_wide,
			read: read_datetime_wide,
			decode: decode_datetime_wide::<Build>,
			check: decode_datetime_wide::<Check>,
		},
		PhysicalKind::Duration => &KindOps {
			escape: Some(Escape {
				width: ESCAPE_WIDTH,
				at_escape: duration_at_escape,
			}),
			write: write_duration,
			read: read_duration,
			decode: decode_duration::<Build>,
			check: decode_duration::<Check>,
		},
		PhysicalKind::Point => &KindOps {
			escape: None,
			write: write_point,
			read: read_point,
			decode: decode_point::<Build>,
			check: decode_point::<Check>,
		},
		PhysicalKind::LiteralCode(_) => &KindOps {
			escape: None,
			write: write_literal,
			read: read_literal,
			decode: decode_literal::<Build>,
			check: decode_literal::<Check>,
		},
		PhysicalKind::String => &KindOps {
			escape: None,
			write: write_string,
			read: read_string,
			decode: decode_string::<Build>,
			check: decode_string::<Check>,
		},
		PhysicalKind::Bytes => &KindOps {
			escape: None,
			write: write_var_bytes,
			read: read_var_bytes,
			decode: decode_var_bytes::<Build>,
			check: decode_var_bytes::<Check>,
		},
		PhysicalKind::RecordKey => &KindOps {
			escape: None,
			write: write_record_key,
			read: read_record_key,
			decode: decode_record_key::<Build>,
			check: decode_record_key::<Check>,
		},
		PhysicalKind::DecimalVar => &KindOps {
			escape: None,
			write: write_decimal_var,
			read: read_decimal_var,
			decode: decode_decimal_var::<Build>,
			check: decode_decimal_var::<Check>,
		},
		PhysicalKind::Value => &KindOps {
			escape: None,
			write: write_value,
			read: read_value,
			decode: decode_value::<Build>,
			check: decode_value::<Check>,
		},
		PhysicalKind::Object => &KindOps {
			escape: None,
			write: write_no_slot,
			read: read_no_slot,
			decode: decode_no_slot::<Build>,
			check: decode_no_slot::<Check>,
		},
	}
}

/// Reads a present slot of `column` in place: the kind table's [`KindOps::read`], dispatched on the
/// kind directly so that a reader walking many columns inlines it.
#[inline]
pub(crate) fn read<'a>(
	column: &'a Column,
	index: u32,
	slot: Slot<'a>,
) -> Result<FieldRef<'a>, PackedError> {
	match column.kind {
		PhysicalKind::Bool => read_bool(column, index, slot),
		PhysicalKind::I8 | PhysicalKind::I16 | PhysicalKind::I32 | PhysicalKind::I64 => {
			read_signed(column, index, slot)
		}
		PhysicalKind::U8 | PhysicalKind::U16 | PhysicalKind::U32 | PhysicalKind::U64 => {
			read_unsigned(column, index, slot)
		}
		PhysicalKind::F32 => read_f32(column, index, slot),
		PhysicalKind::F64 => read_f64(column, index, slot),
		PhysicalKind::Decimal16 => read_decimal16(column, index, slot),
		PhysicalKind::Fixed(_) => read_fixed_bytes(column, index, slot),
		PhysicalKind::Uuid => read_uuid(column, index, slot),
		PhysicalKind::Datetime => read_datetime(column, index, slot),
		PhysicalKind::DatetimeWide => read_datetime_wide(column, index, slot),
		PhysicalKind::Duration => read_duration(column, index, slot),
		PhysicalKind::Point => read_point(column, index, slot),
		PhysicalKind::LiteralCode(_) => read_literal(column, index, slot),
		PhysicalKind::String => read_string(column, index, slot),
		PhysicalKind::Bytes => read_var_bytes(column, index, slot),
		PhysicalKind::RecordKey => read_record_key(column, index, slot),
		PhysicalKind::DecimalVar => read_decimal_var(column, index, slot),
		PhysicalKind::Value => read_value(column, index, slot),
		PhysicalKind::Object => read_no_slot(column, index, slot),
	}
}

/// Where a temporal value goes in its 8-byte slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SlotFit<T> {
	/// The value is the slot's.
	Fits(T),
	/// The slot holds this escape value and the value is in the trailer.
	Escape(T),
	/// The value is not held by the slot or the trailer.
	Misfit,
}

/// Where `d` goes in a `Datetime` slot, which holds `i64` nanoseconds since the epoch.
///
/// A datetime outside the nanosecond range, or exactly at one of its ends, holds the escape value
/// of its side. A leap second shares its nanosecond count with the next second, so it fits nowhere.
pub(crate) fn datetime_slot(d: &DateTime<Utc>) -> SlotFit<i64> {
	if d.timestamp_subsec_nanos() >= 1_000_000_000 {
		return SlotFit::Misfit;
	}
	match d.timestamp_nanos_opt() {
		Some(ns) if is_datetime_escape(ns) => SlotFit::Escape(ns),
		Some(ns) => SlotFit::Fits(ns),
		// More than 292 years from the epoch, so the sign of the whole seconds names the side.
		None if d.timestamp() < 0 => SlotFit::Escape(DATETIME_BELOW),
		None => SlotFit::Escape(DATETIME_ABOVE),
	}
}

/// Where `d` goes in a `Duration` slot, which holds `u64` nanoseconds. A duration always fits the
/// slot or the trailer.
pub(crate) fn duration_slot(d: &std::time::Duration) -> SlotFit<u64> {
	match u64::try_from(d.as_nanos()) {
		Ok(ns) if ns != DURATION_ESCAPE => SlotFit::Fits(ns),
		_ => SlotFit::Escape(DURATION_ESCAPE),
	}
}

/// The `f64` an `F32` slot's bits stand for.
///
/// A NaN keeps its sign and mantissa bits, as the top of the `f64`'s, instead of going through a
/// float conversion, whose treatment of a NaN's payload is unspecified.
pub(crate) fn widen_f32(bits: u32) -> f64 {
	let mantissa = bits & 0x007F_FFFF;
	if (bits & 0x7F80_0000) == 0x7F80_0000 && mantissa != 0 {
		f64::from_bits(
			(u64::from(bits >> 31) << 63) | 0x7FF0_0000_0000_0000 | (u64::from(mantissa) << 29),
		)
	} else {
		f64::from(f32::from_bits(bits))
	}
}

/// The `F32` slot bits that stand for exactly `f`, when there are any: those whose widening has
/// `f`'s own bits. A NaN keeps its sign and the top 23 bits of its mantissa, and fits only when the
/// rest of its mantissa is zero; any other value fits when an `f32` holds it exactly.
pub(crate) fn narrow_f32(f: f64) -> Option<u32> {
	let bits = f.to_bits();
	let candidate = if f.is_nan() {
		// The mask keeps 23 bits, so the cast loses nothing.
		(u32::from((bits >> 63) != 0) << 31) | 0x7F80_0000 | (((bits >> 29) & 0x007F_FFFF) as u32)
	} else {
		(f as f32).to_bits()
	};
	(widen_f32(candidate).to_bits() == bits).then_some(candidate)
}

/// The narrowest end-offset width in bytes, of 1, 2 and 4, whose maximum is at least `len`; `None`
/// when `len` is above `u32::MAX`.
pub(crate) fn narrowest_width(len: usize) -> Option<usize> {
	if len <= usize::from(u8::MAX) {
		Some(1)
	} else if len <= usize::from(u16::MAX) {
		Some(2)
	} else if u32::try_from(len).is_ok() {
		Some(4)
	} else {
		None
	}
}

/// The refusal of a slot that is not the shape its kind has: a descriptor and a row that agree
/// never produce it.
#[cold]
fn wrong_shape(column: u32) -> PackedError {
	PackedError::Slot {
		column,
		reason: "a slot of the wrong shape for its kind",
	}
}

/// The refusal of stored-grammar bytes that do not read.
#[inline]
fn stored_error(column: u32) -> impl Fn(stored::Error) -> PackedError {
	move |error| PackedError::Stored {
		column,
		error,
	}
}

/// The first `N` bytes of `bytes` when it is exactly `N` bytes.
#[inline]
fn array<const N: usize>(bytes: &[u8]) -> Option<[u8; N]> {
	bytes.try_into().ok()
}

/// The bytes of a fixed slot.
#[inline]
fn fixed(index: u32, slot: Slot<'_>) -> Result<&[u8], PackedError> {
	match slot {
		Slot::Fixed(bytes) => Ok(bytes),
		_ => Err(wrong_shape(index)),
	}
}

/// The bytes of a variable slice.
#[inline]
fn var(index: u32, slot: Slot<'_>) -> Result<&[u8], PackedError> {
	match slot {
		Slot::Var(bytes) => Ok(bytes),
		_ => Err(wrong_shape(index)),
	}
}

// Bool

fn read_bool<'a>(
	_column: &'a Column,
	index: u32,
	slot: Slot<'a>,
) -> Result<FieldRef<'a>, PackedError> {
	match slot {
		Slot::Bit(bit) => Ok(FieldRef::Bool(bit)),
		_ => Err(wrong_shape(index)),
	}
}

fn decode_bool<M: Mode>(
	_column: &Column,
	index: u32,
	slot: Slot<'_>,
	_depth: usize,
) -> Result<M::Out<Value>, PackedError> {
	match slot {
		Slot::Bit(bit) => Ok(M::make(|| Value::Bool(bit))),
		_ => Err(wrong_shape(index)),
	}
}

// Integers

/// A little-endian two's complement integer of 1, 2, 4 or 8 bytes.
fn le_signed(bytes: &[u8]) -> Option<i64> {
	Some(match bytes.len() {
		1 => i64::from(i8::from_le_bytes(array(bytes)?)),
		2 => i64::from(i16::from_le_bytes(array(bytes)?)),
		4 => i64::from(i32::from_le_bytes(array(bytes)?)),
		8 => i64::from_le_bytes(array(bytes)?),
		_ => return None,
	})
}

/// A little-endian unsigned integer of 1, 2, 4 or 8 bytes.
fn le_unsigned(bytes: &[u8]) -> Option<u64> {
	Some(match bytes.len() {
		1 => u64::from(u8::from_le_bytes(array(bytes)?)),
		2 => u64::from(u16::from_le_bytes(array(bytes)?)),
		4 => u64::from(u32::from_le_bytes(array(bytes)?)),
		8 => u64::from_le_bytes(array(bytes)?),
		_ => return None,
	})
}

fn read_signed<'a>(
	_column: &'a Column,
	index: u32,
	slot: Slot<'a>,
) -> Result<FieldRef<'a>, PackedError> {
	le_signed(fixed(index, slot)?).map(FieldRef::Int).ok_or_else(|| wrong_shape(index))
}

fn decode_signed<M: Mode>(
	_column: &Column,
	index: u32,
	slot: Slot<'_>,
	_depth: usize,
) -> Result<M::Out<Value>, PackedError> {
	let v = le_signed(fixed(index, slot)?).ok_or_else(|| wrong_shape(index))?;
	Ok(M::make(|| Value::Number(Number::Int(v))))
}

/// Reads `U8` to `U64`. A value above `i64::MAX`, which only a `U64` slot can hold, is the exactly
/// equal decimal.
fn read_unsigned<'a>(
	_column: &'a Column,
	index: u32,
	slot: Slot<'a>,
) -> Result<FieldRef<'a>, PackedError> {
	let v = le_unsigned(fixed(index, slot)?).ok_or_else(|| wrong_shape(index))?;
	Ok(match i64::try_from(v) {
		Ok(int) => FieldRef::Int(int),
		Err(_) => FieldRef::Decimal(Decimal::from(v)),
	})
}

fn decode_unsigned<M: Mode>(
	_column: &Column,
	index: u32,
	slot: Slot<'_>,
	_depth: usize,
) -> Result<M::Out<Value>, PackedError> {
	let v = le_unsigned(fixed(index, slot)?).ok_or_else(|| wrong_shape(index))?;
	Ok(M::make(|| {
		Value::Number(match i64::try_from(v) {
			Ok(int) => Number::Int(int),
			Err(_) => Number::Decimal(Decimal::from(v)),
		})
	}))
}

// Floats

fn f32_of(index: u32, slot: Slot<'_>) -> Result<f64, PackedError> {
	let bits = u32::from_le_bytes(array(fixed(index, slot)?).ok_or_else(|| wrong_shape(index))?);
	Ok(widen_f32(bits))
}

fn f64_of(index: u32, slot: Slot<'_>) -> Result<f64, PackedError> {
	let bits = u64::from_le_bytes(array(fixed(index, slot)?).ok_or_else(|| wrong_shape(index))?);
	Ok(f64::from_bits(bits))
}

fn read_f32<'a>(
	_column: &'a Column,
	index: u32,
	slot: Slot<'a>,
) -> Result<FieldRef<'a>, PackedError> {
	f32_of(index, slot).map(FieldRef::Float)
}

fn decode_f32<M: Mode>(
	_column: &Column,
	index: u32,
	slot: Slot<'_>,
	_depth: usize,
) -> Result<M::Out<Value>, PackedError> {
	let v = f32_of(index, slot)?;
	Ok(M::make(|| Value::Number(Number::Float(v))))
}

fn read_f64<'a>(
	_column: &'a Column,
	index: u32,
	slot: Slot<'a>,
) -> Result<FieldRef<'a>, PackedError> {
	f64_of(index, slot).map(FieldRef::Float)
}

fn decode_f64<M: Mode>(
	_column: &Column,
	index: u32,
	slot: Slot<'_>,
	_depth: usize,
) -> Result<M::Out<Value>, PackedError> {
	let v = f64_of(index, slot)?;
	Ok(M::make(|| Value::Number(Number::Float(v))))
}

// Decimals

/// The decimal of a 16-byte `Decimal::serialize` slot. The flag word may set only the scale bits
/// and the sign, and the scale is at most 28, so one decimal has one slot.
fn decimal16_of(index: u32, slot: Slot<'_>) -> Result<Decimal, PackedError> {
	let raw: [u8; 16] = array(fixed(index, slot)?).ok_or_else(|| wrong_shape(index))?;
	let flags = u32::from_le_bytes(*raw.first_chunk::<4>().ok_or_else(|| wrong_shape(index))?);
	if (flags & !0x801F_0000) != 0 {
		return Err(PackedError::Slot {
			column: index,
			reason: "a decimal with flag bits no decimal sets",
		});
	}
	if ((flags >> 16) & 0xFF) > 28 {
		return Err(PackedError::Slot {
			column: index,
			reason: "a decimal with a scale above 28",
		});
	}
	Ok(Decimal::deserialize(raw))
}

fn read_decimal16<'a>(
	_column: &'a Column,
	index: u32,
	slot: Slot<'a>,
) -> Result<FieldRef<'a>, PackedError> {
	decimal16_of(index, slot).map(FieldRef::Decimal)
}

fn decode_decimal16<M: Mode>(
	_column: &Column,
	index: u32,
	slot: Slot<'_>,
	_depth: usize,
) -> Result<M::Out<Value>, PackedError> {
	let v = decimal16_of(index, slot)?;
	Ok(M::make(|| Value::Number(Number::Decimal(v))))
}

fn read_decimal_var<'a>(
	_column: &'a Column,
	index: u32,
	slot: Slot<'a>,
) -> Result<FieldRef<'a>, PackedError> {
	stored::decimal_payload(var(index, slot)?).map(FieldRef::Decimal).map_err(stored_error(index))
}

fn decode_decimal_var<M: Mode>(
	_column: &Column,
	index: u32,
	slot: Slot<'_>,
	_depth: usize,
) -> Result<M::Out<Value>, PackedError> {
	let v = stored::decimal_payload(var(index, slot)?).map_err(stored_error(index))?;
	Ok(M::make(|| Value::Number(Number::Decimal(v))))
}

// Bytes, uuids and points

fn read_fixed_bytes<'a>(
	_column: &'a Column,
	index: u32,
	slot: Slot<'a>,
) -> Result<FieldRef<'a>, PackedError> {
	fixed(index, slot).map(FieldRef::Bytes)
}

fn decode_fixed_bytes<M: Mode>(
	_column: &Column,
	index: u32,
	slot: Slot<'_>,
	_depth: usize,
) -> Result<M::Out<Value>, PackedError> {
	let bytes = fixed(index, slot)?;
	Ok(M::make(|| Value::Bytes(val::Bytes::from(bytes.to_vec()))))
}

fn read_var_bytes<'a>(
	_column: &'a Column,
	index: u32,
	slot: Slot<'a>,
) -> Result<FieldRef<'a>, PackedError> {
	var(index, slot).map(FieldRef::Bytes)
}

fn decode_var_bytes<M: Mode>(
	_column: &Column,
	index: u32,
	slot: Slot<'_>,
	_depth: usize,
) -> Result<M::Out<Value>, PackedError> {
	let bytes = var(index, slot)?;
	Ok(M::make(|| Value::Bytes(val::Bytes::from(bytes.to_vec()))))
}

fn uuid_of(index: u32, slot: Slot<'_>) -> Result<uuid::Uuid, PackedError> {
	array(fixed(index, slot)?).map(uuid::Uuid::from_bytes).ok_or_else(|| wrong_shape(index))
}

fn read_uuid<'a>(
	_column: &'a Column,
	index: u32,
	slot: Slot<'a>,
) -> Result<FieldRef<'a>, PackedError> {
	uuid_of(index, slot).map(FieldRef::Uuid)
}

fn decode_uuid<M: Mode>(
	_column: &Column,
	index: u32,
	slot: Slot<'_>,
	_depth: usize,
) -> Result<M::Out<Value>, PackedError> {
	let v = uuid_of(index, slot)?;
	Ok(M::make(|| Value::Uuid(val::Uuid(v))))
}

/// The coordinates of a 16-byte point slot: `x`, then `y`, as `f64` bits.
fn point_of(index: u32, slot: Slot<'_>) -> Result<(f64, f64), PackedError> {
	let raw: [u8; 16] = array(fixed(index, slot)?).ok_or_else(|| wrong_shape(index))?;
	let (x, y) = raw.split_at(8);
	let x = array(x).map(u64::from_le_bytes).ok_or_else(|| wrong_shape(index))?;
	let y = array(y).map(u64::from_le_bytes).ok_or_else(|| wrong_shape(index))?;
	Ok((f64::from_bits(x), f64::from_bits(y)))
}

fn read_point<'a>(
	_column: &'a Column,
	index: u32,
	slot: Slot<'a>,
) -> Result<FieldRef<'a>, PackedError> {
	point_of(index, slot).map(|(x, y)| FieldRef::Point(x, y))
}

fn decode_point<M: Mode>(
	_column: &Column,
	index: u32,
	slot: Slot<'_>,
	_depth: usize,
) -> Result<M::Out<Value>, PackedError> {
	let (x, y) = point_of(index, slot)?;
	Ok(M::make(|| Value::Geometry(val::Geometry::Point((x, y).into()))))
}

// Datetimes and durations

fn datetime_at_escape(slot: &[u8]) -> bool {
	array(slot).map(i64::from_le_bytes).is_some_and(is_datetime_escape)
}

fn duration_at_escape(slot: &[u8]) -> bool {
	array(slot).map(u64::from_le_bytes).is_some_and(|ns| ns == DURATION_ESCAPE)
}

/// The datetime of a `Datetime` slot, or of its slot and trailer payload when the slot holds an
/// escape value. The payload must be exactly what the writer escapes to that slot value.
fn datetime_of(index: u32, slot: Slot<'_>) -> Result<DateTime<Utc>, PackedError> {
	match slot {
		Slot::Fixed(bytes) => {
			let ns = i64::from_le_bytes(array(bytes).ok_or_else(|| wrong_shape(index))?);
			if is_datetime_escape(ns) {
				return Err(PackedError::Escape {
					column: index,
				});
			}
			Ok(DateTime::from_timestamp_nanos(ns))
		}
		Slot::Escaped {
			slot,
			payload,
		} => {
			let ns = i64::from_le_bytes(array(slot).ok_or_else(|| wrong_shape(index))?);
			let value = stored::decode_datetime_payload(payload).map_err(stored_error(index))?;
			if datetime_slot(&value) == SlotFit::Escape(ns) {
				Ok(value)
			} else {
				Err(PackedError::Escape {
					column: index,
				})
			}
		}
		_ => Err(wrong_shape(index)),
	}
}

fn read_datetime<'a>(
	_column: &'a Column,
	index: u32,
	slot: Slot<'a>,
) -> Result<FieldRef<'a>, PackedError> {
	datetime_of(index, slot).map(FieldRef::Datetime)
}

fn decode_datetime<M: Mode>(
	_column: &Column,
	index: u32,
	slot: Slot<'_>,
	_depth: usize,
) -> Result<M::Out<Value>, PackedError> {
	let v = datetime_of(index, slot)?;
	Ok(M::make(|| Value::Datetime(val::Datetime(v))))
}

/// The datetime of a 12-byte `DatetimeWide` slot: `i64` seconds, then `u32` nanoseconds.
fn datetime_wide_of(index: u32, slot: Slot<'_>) -> Result<DateTime<Utc>, PackedError> {
	stored::decode_datetime_payload(fixed(index, slot)?).map_err(stored_error(index))
}

fn read_datetime_wide<'a>(
	_column: &'a Column,
	index: u32,
	slot: Slot<'a>,
) -> Result<FieldRef<'a>, PackedError> {
	datetime_wide_of(index, slot).map(FieldRef::Datetime)
}

fn decode_datetime_wide<M: Mode>(
	_column: &Column,
	index: u32,
	slot: Slot<'_>,
	_depth: usize,
) -> Result<M::Out<Value>, PackedError> {
	let v = datetime_wide_of(index, slot)?;
	Ok(M::make(|| Value::Datetime(val::Datetime(v))))
}

/// The duration of a `Duration` slot, or of its slot and trailer payload when the slot holds the
/// escape value. The payload must be a duration the writer escapes.
fn duration_of(index: u32, slot: Slot<'_>) -> Result<std::time::Duration, PackedError> {
	match slot {
		Slot::Fixed(bytes) => {
			let ns = u64::from_le_bytes(array(bytes).ok_or_else(|| wrong_shape(index))?);
			if ns == DURATION_ESCAPE {
				return Err(PackedError::Escape {
					column: index,
				});
			}
			Ok(std::time::Duration::from_nanos(ns))
		}
		Slot::Escaped {
			slot,
			payload,
		} => {
			let ns = u64::from_le_bytes(array(slot).ok_or_else(|| wrong_shape(index))?);
			let value = stored::decode_duration_payload(payload).map_err(stored_error(index))?;
			if ns == DURATION_ESCAPE && duration_slot(&value) == SlotFit::Escape(DURATION_ESCAPE) {
				Ok(value)
			} else {
				Err(PackedError::Escape {
					column: index,
				})
			}
		}
		_ => Err(wrong_shape(index)),
	}
}

fn read_duration<'a>(
	_column: &'a Column,
	index: u32,
	slot: Slot<'a>,
) -> Result<FieldRef<'a>, PackedError> {
	duration_of(index, slot).map(FieldRef::Duration)
}

fn decode_duration<M: Mode>(
	_column: &Column,
	index: u32,
	slot: Slot<'_>,
	_depth: usize,
) -> Result<M::Out<Value>, PackedError> {
	let v = duration_of(index, slot)?;
	Ok(M::make(|| Value::Duration(val::Duration(v))))
}

// Literal unions

/// The literal a 1- or 2-byte code slot names.
fn literal_of<'a>(
	column: &'a Column,
	index: u32,
	slot: Slot<'a>,
) -> Result<&'a Value, PackedError> {
	let bytes = fixed(index, slot)?;
	let code = match bytes.len() {
		1 => usize::from(u8::from_le_bytes(array(bytes).ok_or_else(|| wrong_shape(index))?)),
		2 => usize::from(u16::from_le_bytes(array(bytes).ok_or_else(|| wrong_shape(index))?)),
		_ => return Err(wrong_shape(index)),
	};
	column.literals.get(code).ok_or(PackedError::Code {
		column: index,
	})
}

fn read_literal<'a>(
	column: &'a Column,
	index: u32,
	slot: Slot<'a>,
) -> Result<FieldRef<'a>, PackedError> {
	literal_of(column, index, slot).map(FieldRef::Literal)
}

fn decode_literal<M: Mode>(
	column: &Column,
	index: u32,
	slot: Slot<'_>,
	_depth: usize,
) -> Result<M::Out<Value>, PackedError> {
	let literal = literal_of(column, index, slot)?;
	Ok(M::make(|| literal.clone()))
}

// Strings

fn str_of(index: u32, slot: Slot<'_>) -> Result<&str, PackedError> {
	std::str::from_utf8(var(index, slot)?).map_err(|_| PackedError::Stored {
		column: index,
		error: stored::Error::Malformed("a string is not valid UTF-8"),
	})
}

fn read_string<'a>(
	_column: &'a Column,
	index: u32,
	slot: Slot<'a>,
) -> Result<FieldRef<'a>, PackedError> {
	str_of(index, slot).map(FieldRef::String)
}

fn decode_string<M: Mode>(
	_column: &Column,
	index: u32,
	slot: Slot<'_>,
	_depth: usize,
) -> Result<M::Out<Value>, PackedError> {
	let s = str_of(index, slot)?;
	Ok(M::make(|| Value::String(val::Strand::new(s))))
}

// Record keys

/// The table and the key bytes of a record key slice: a one-byte table code when the column names
/// several tables, then the key in the stored key grammar.
fn record_key_of<'a>(
	column: &'a Column,
	index: u32,
	slot: Slot<'a>,
) -> Result<(&'a TableName, &'a [u8]), PackedError> {
	let bytes = var(index, slot)?;
	let (code, key) = if column.tables.len() > 1 {
		let (&code, key) = bytes.split_first().ok_or(PackedError::Stored {
			column: index,
			error: stored::Error::Truncated,
		})?;
		(usize::from(code), key)
	} else {
		(0, bytes)
	};
	let table = column.tables.get(code).ok_or(PackedError::Code {
		column: index,
	})?;
	if key.is_empty() {
		return Err(PackedError::Stored {
			column: index,
			error: stored::Error::Truncated,
		});
	}
	Ok((table, key))
}

fn read_record_key<'a>(
	column: &'a Column,
	index: u32,
	slot: Slot<'a>,
) -> Result<FieldRef<'a>, PackedError> {
	record_key_of(column, index, slot).map(|(table, key)| FieldRef::RecordId(table, key))
}

/// Decodes the key at one level below the record id, which sits at `depth`.
fn decode_record_key<M: Mode>(
	column: &Column,
	index: u32,
	slot: Slot<'_>,
	depth: usize,
) -> Result<M::Out<Value>, PackedError> {
	let (table, key) = record_key_of(column, index, slot)?;
	let key = M::key(key, depth.saturating_add(1)).map_err(stored_error(index))?;
	Ok(M::map::<RecordIdKey, Value>(key, |key| {
		Value::RecordId(val::RecordId {
			table: table.clone(),
			key,
		})
	}))
}

// Grammar values

/// Whether a column's presence bits can say NULL.
const fn holds_null(presence: Presence) -> bool {
	matches!(presence, Presence::Nullable { .. } | Presence::OptionalNullable { .. })
}

/// Reads a grammar value of the column in place, refusing what [`decode_value`] refuses of its
/// tag: a `NONE`, a `NULL` in a column whose presence bits hold it, and (through
/// [`ValueRef::new`]) a `NULL` with a payload.
fn read_value<'a>(
	column: &'a Column,
	index: u32,
	slot: Slot<'a>,
) -> Result<FieldRef<'a>, PackedError> {
	let value = ValueRef::new(var(index, slot)?).map_err(stored_error(index))?;
	match value.tag() {
		stored::tag::NULL if holds_null(column.presence) => Err(PackedError::Slot {
			column: index,
			reason: "NULL in a value column whose presence bits hold it",
		}),
		stored::tag::NULL => Ok(FieldRef::Null),
		stored::tag::NONE => Err(PackedError::Slot {
			column: index,
			reason: "NONE in a value column",
		}),
		_ => Ok(FieldRef::Stored(value)),
	}
}

/// Decodes a grammar value of the column, at `depth`. A `NONE` is refused, and so is a `NULL` in a
/// column whose presence bits hold it.
fn decode_value<M: Mode>(
	column: &Column,
	index: u32,
	slot: Slot<'_>,
	depth: usize,
) -> Result<M::Out<Value>, PackedError> {
	let bytes = var(index, slot)?;
	let value = M::value(bytes, depth).map_err(stored_error(index))?;
	match ValueRef::new(bytes).map_err(stored_error(index))?.tag() {
		stored::tag::NONE => Err(PackedError::Slot {
			column: index,
			reason: "NONE in a value column",
		}),
		stored::tag::NULL if holds_null(column.presence) => Err(PackedError::Slot {
			column: index,
			reason: "NULL in a value column whose presence bits hold it",
		}),
		_ => Ok(value),
	}
}

// Object nodes

/// An object node has no slot: a row reads it from its presence alone, so these are never called
/// for a descriptor that agrees with its rows.
fn read_no_slot<'a>(
	_column: &'a Column,
	index: u32,
	_slot: Slot<'a>,
) -> Result<FieldRef<'a>, PackedError> {
	Err(PackedError::Slot {
		column: index,
		reason: "an object node has no slot",
	})
}

fn decode_no_slot<M: Mode>(
	_column: &Column,
	index: u32,
	_slot: Slot<'_>,
	_depth: usize,
) -> Result<M::Out<Value>, PackedError> {
	Err(PackedError::Slot {
		column: index,
		reason: "an object node has no slot",
	})
}

// Writing values

/// The refusal of a destination that is not the shape its kind has: a descriptor and a writer that
/// agree never produce it.
#[cold]
fn wrong_dest() -> stored::Error {
	stored::Error::Unstorable("a destination of the wrong shape for its kind")
}

/// The fixed slot of a destination.
fn slot_of(dest: Dest<'_>) -> Result<&mut [u8], stored::Error> {
	match dest {
		Dest::Fixed(slot) => Ok(slot),
		_ => Err(wrong_dest()),
	}
}

/// The variable region and scratch encoder of a destination, and the depth grammar values are
/// encoded at.
fn var_of(dest: Dest<'_>) -> Result<(&mut Vec<u8>, &mut Encoder, usize), stored::Error> {
	match dest {
		Dest::Var {
			out,
			enc,
			depth,
		} => Ok((out, enc, depth)),
		_ => Err(wrong_dest()),
	}
}

/// Copies `bytes` into `slot`, which must be exactly as wide.
fn put(slot: &mut [u8], bytes: &[u8]) -> Result<Written, stored::Error> {
	if slot.len() != bytes.len() {
		return Err(wrong_dest());
	}
	slot.copy_from_slice(bytes);
	Ok(Written::Fits)
}

/// The payload of a trailer entry: whole seconds, then the nanoseconds of the second.
fn escape_payload(seconds: [u8; 8], nanoseconds: [u8; 4]) -> [u8; ESCAPE_WIDTH] {
	let mut payload = [0u8; ESCAPE_WIDTH];
	let (head, tail) = payload.split_at_mut(8);
	head.copy_from_slice(&seconds);
	tail.copy_from_slice(&nanoseconds);
	payload
}

fn write_bool(_column: &Column, value: &Value, dest: Dest<'_>) -> Result<Written, stored::Error> {
	let Dest::Bit(bit) = dest else {
		return Err(wrong_dest());
	};
	match value {
		Value::Bool(set) => {
			*bit = *set;
			Ok(Written::Fits)
		}
		_ => Ok(Written::Misfit),
	}
}

/// Writes an `Int` into a slot of 1, 2, 4 or 8 bytes when it is in the slot's range.
fn write_signed(_column: &Column, value: &Value, dest: Dest<'_>) -> Result<Written, stored::Error> {
	let slot = slot_of(dest)?;
	let width = slot.len();
	let Value::Number(Number::Int(v)) = value else {
		return Ok(Written::Misfit);
	};
	let in_range = match width {
		1 => i8::try_from(*v).is_ok(),
		2 => i16::try_from(*v).is_ok(),
		4 => i32::try_from(*v).is_ok(),
		8 => true,
		_ => return Err(wrong_dest()),
	};
	if !in_range {
		return Ok(Written::Misfit);
	}
	// In range, the low bytes of the little-endian two's complement are the narrow integer's.
	let bytes = v.to_le_bytes();
	put(slot, bytes.get(..width).ok_or_else(wrong_dest)?)
}

/// Writes a non-negative `Int` into a slot of 1, 2, 4 or 8 bytes when it is in the slot's range. A
/// decimal is never written, even an integral one: it would read back as an `Int`.
fn write_unsigned(
	_column: &Column,
	value: &Value,
	dest: Dest<'_>,
) -> Result<Written, stored::Error> {
	let slot = slot_of(dest)?;
	let width = slot.len();
	let Value::Number(Number::Int(v)) = value else {
		return Ok(Written::Misfit);
	};
	let Ok(v) = u64::try_from(*v) else {
		return Ok(Written::Misfit);
	};
	let in_range = match width {
		1 => u8::try_from(v).is_ok(),
		2 => u16::try_from(v).is_ok(),
		4 => u32::try_from(v).is_ok(),
		8 => true,
		_ => return Err(wrong_dest()),
	};
	if !in_range {
		return Ok(Written::Misfit);
	}
	let bytes = v.to_le_bytes();
	put(slot, bytes.get(..width).ok_or_else(wrong_dest)?)
}

fn write_f32(_column: &Column, value: &Value, dest: Dest<'_>) -> Result<Written, stored::Error> {
	let slot = slot_of(dest)?;
	let Value::Number(Number::Float(f)) = value else {
		return Ok(Written::Misfit);
	};
	match narrow_f32(*f) {
		Some(bits) => put(slot, &bits.to_le_bytes()),
		None => Ok(Written::Misfit),
	}
}

fn write_f64(_column: &Column, value: &Value, dest: Dest<'_>) -> Result<Written, stored::Error> {
	let slot = slot_of(dest)?;
	let Value::Number(Number::Float(f)) = value else {
		return Ok(Written::Misfit);
	};
	put(slot, &f.to_bits().to_le_bytes())
}

fn write_decimal16(
	_column: &Column,
	value: &Value,
	dest: Dest<'_>,
) -> Result<Written, stored::Error> {
	let slot = slot_of(dest)?;
	let Value::Number(Number::Decimal(d)) = value else {
		return Ok(Written::Misfit);
	};
	put(slot, &d.serialize())
}

fn write_decimal_var(
	_column: &Column,
	value: &Value,
	dest: Dest<'_>,
) -> Result<Written, stored::Error> {
	let (out, _, _) = var_of(dest)?;
	let Value::Number(Number::Decimal(d)) = value else {
		return Ok(Written::Misfit);
	};
	stored::put_decimal_payload(d, out);
	Ok(Written::Fits)
}

/// Writes `Bytes` of exactly the slot's width.
fn write_fixed_bytes(
	_column: &Column,
	value: &Value,
	dest: Dest<'_>,
) -> Result<Written, stored::Error> {
	let slot = slot_of(dest)?;
	let Value::Bytes(bytes) = value else {
		return Ok(Written::Misfit);
	};
	let bytes: &[u8] = &bytes.0;
	if bytes.len() != slot.len() {
		return Ok(Written::Misfit);
	}
	put(slot, bytes)
}

fn write_var_bytes(
	_column: &Column,
	value: &Value,
	dest: Dest<'_>,
) -> Result<Written, stored::Error> {
	let (out, _, _) = var_of(dest)?;
	let Value::Bytes(bytes) = value else {
		return Ok(Written::Misfit);
	};
	let bytes: &[u8] = &bytes.0;
	out.extend_from_slice(bytes);
	Ok(Written::Fits)
}

fn write_uuid(_column: &Column, value: &Value, dest: Dest<'_>) -> Result<Written, stored::Error> {
	let slot = slot_of(dest)?;
	let Value::Uuid(uuid) = value else {
		return Ok(Written::Misfit);
	};
	put(slot, uuid.0.as_bytes())
}

fn write_point(_column: &Column, value: &Value, dest: Dest<'_>) -> Result<Written, stored::Error> {
	let slot = slot_of(dest)?;
	let Value::Geometry(val::Geometry::Point(point)) = value else {
		return Ok(Written::Misfit);
	};
	let mut raw = [0u8; 16];
	let (x, y) = raw.split_at_mut(8);
	x.copy_from_slice(&point.x().to_le_bytes());
	y.copy_from_slice(&point.y().to_le_bytes());
	put(slot, &raw)
}

/// Writes a datetime into its 8-byte slot, or its escape value and the trailer payload.
fn write_datetime(
	_column: &Column,
	value: &Value,
	dest: Dest<'_>,
) -> Result<Written, stored::Error> {
	let slot = slot_of(dest)?;
	let Value::Datetime(d) = value else {
		return Ok(Written::Misfit);
	};
	match datetime_slot(&d.0) {
		SlotFit::Fits(ns) => put(slot, &ns.to_le_bytes()),
		SlotFit::Escape(ns) => {
			put(slot, &ns.to_le_bytes())?;
			Ok(Written::Escaped(escape_payload(
				d.0.timestamp().to_le_bytes(),
				d.0.timestamp_subsec_nanos().to_le_bytes(),
			)))
		}
		SlotFit::Misfit => Ok(Written::Misfit),
	}
}

/// Writes a datetime into its 12-byte slot: whole seconds, then the nanoseconds of the second.
fn write_datetime_wide(
	_column: &Column,
	value: &Value,
	dest: Dest<'_>,
) -> Result<Written, stored::Error> {
	let slot = slot_of(dest)?;
	let Value::Datetime(d) = value else {
		return Ok(Written::Misfit);
	};
	let payload =
		escape_payload(d.0.timestamp().to_le_bytes(), d.0.timestamp_subsec_nanos().to_le_bytes());
	put(slot, &payload)
}

/// Writes a duration into its 8-byte slot, or its escape value and the trailer payload.
fn write_duration(
	_column: &Column,
	value: &Value,
	dest: Dest<'_>,
) -> Result<Written, stored::Error> {
	let slot = slot_of(dest)?;
	let Value::Duration(d) = value else {
		return Ok(Written::Misfit);
	};
	match duration_slot(&d.0) {
		SlotFit::Fits(ns) => put(slot, &ns.to_le_bytes()),
		SlotFit::Escape(ns) => {
			put(slot, &ns.to_le_bytes())?;
			Ok(Written::Escaped(escape_payload(
				d.0.as_secs().to_le_bytes(),
				d.0.subsec_nanos().to_le_bytes(),
			)))
		}
		SlotFit::Misfit => Ok(Written::Misfit),
	}
}

/// Writes the code of the literal that is the same value as `value`. A value that is none of the
/// literals, or only equal to one of another kind, is a misfit.
fn write_literal(column: &Column, value: &Value, dest: Dest<'_>) -> Result<Written, stored::Error> {
	let slot = slot_of(dest)?;
	let Some(code) = column.literals.iter().position(|literal| val::same_value(literal, value))
	else {
		return Ok(Written::Misfit);
	};
	match slot.len() {
		1 => {
			let code = u8::try_from(code).map_err(|_| wrong_dest())?;
			put(slot, &[code])
		}
		2 => {
			let code = u16::try_from(code).map_err(|_| wrong_dest())?;
			put(slot, &code.to_le_bytes())
		}
		_ => Err(wrong_dest()),
	}
}

fn write_string(_column: &Column, value: &Value, dest: Dest<'_>) -> Result<Written, stored::Error> {
	let (out, _, _) = var_of(dest)?;
	let Value::String(s) = value else {
		return Ok(Written::Misfit);
	};
	out.extend_from_slice(s.as_str().as_bytes());
	Ok(Written::Fits)
}

/// Writes a record id of one of the column's tables: a one-byte table code when the column names
/// several, then the key in the stored key grammar, one level below the record id.
fn write_record_key(
	column: &Column,
	value: &Value,
	dest: Dest<'_>,
) -> Result<Written, stored::Error> {
	let (out, enc, depth) = var_of(dest)?;
	let Value::RecordId(id) = value else {
		return Ok(Written::Misfit);
	};
	let Some(code) = column.tables.iter().position(|table| *table == id.table) else {
		return Ok(Written::Misfit);
	};
	if column.tables.len() > 1 {
		out.push(u8::try_from(code).map_err(|_| wrong_dest())?);
	}
	enc.clear();
	enc.record_key_nested(&id.key, depth.saturating_add(1))?;
	out.extend_from_slice(enc.as_bytes());
	Ok(Written::Fits)
}

/// Writes any value but `NONE` in the stored grammar. The row grammar holds a `NULL` in the
/// presence bits when they can say it, and passes it here only when they cannot.
fn write_value(_column: &Column, value: &Value, dest: Dest<'_>) -> Result<Written, stored::Error> {
	let (out, enc, depth) = var_of(dest)?;
	if matches!(value, Value::None) {
		return Ok(Written::Misfit);
	}
	enc.clear();
	enc.value_nested(value, depth)?;
	out.extend_from_slice(enc.as_bytes());
	Ok(Written::Fits)
}

/// An object node has no slot: the row grammar writes it from its presence alone, so this is never
/// called for a descriptor that agrees with the writer.
fn write_no_slot(
	_column: &Column,
	_value: &Value,
	_dest: Dest<'_>,
) -> Result<Written, stored::Error> {
	Err(stored::Error::Unstorable("an object node has no slot"))
}
