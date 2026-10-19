//! Shaped frames: query results that announce each row shape once and then carry rows as values
//! only, with no field names and no per-value type tags.
//!
//! The byte layout is specified in `doc/design/shaped-frames.md` ("Wire format v1"); this module
//! is its reference implementation:
//!
//! - [`Shape`] is a validated shape: its columns, the row layout they imply, and its SHAPE message
//!   payload.
//! - [`RowWriter`] writes one row of a shape; [`ValueShaper`] encodes values through it, choosing
//!   the shape each row is written in ([`ShapedValue`] is what it reads a value through).
//! - [`ShapeScope`] and [`write_block`] frame rows into a block, announcing each shape once per
//!   scope.
//! - [`BlockReader`] turns blocks back into public [`Value`](crate::Value)s.
//!
//! Dynamic slices hold one CBOR item each. This crate has no CBOR codec: a writer supplies one
//! through [`ShapedValue::write_dynamic`], and a reader through the decoder it passes to
//! [`BlockReader::read_block`].

mod read;
mod shape;
mod value;
mod write;

#[cfg(test)]
mod tests;

pub use read::{BlockReader, block_contents, decode_row};
pub use shape::{Column, ColumnDef, Kind, Root, Shape, ShapeDef, presence_width};
pub use value::{
	KeyLeaf, Leaf, MAX_DERIVED_COLUMNS, MAX_DERIVED_DEPTH, MAX_JOINS, PublicShaped, ShapedValue,
	ValueShaper, cbor_head, decimal_text, fits, write_record,
};
pub use write::{RowWriter, ShapeScope, block_head, write_block};

/// The CBOR tag that wraps a block's byte string.
pub const CBOR_TAG: u64 = 57;
/// The block format version this build writes and reads.
pub const VERSION: u8 = 1;
/// A SHAPE message.
pub const MESSAGE_SHAPE: u8 = 0x01;
/// A BATCH message.
pub const MESSAGE_BATCH: u8 = 0x02;
/// A presence index, slot or remainder a column does not have.
pub const NO_SLOT: u32 = u32::MAX;
/// A column flag: the column may be absent (a missing key, or a field the caller may not see) or
/// hold `NONE`.
pub const MAY_BE_ABSENT: u8 = 0b01;
/// A column flag: the column may be `NULL`.
pub const MAY_BE_NULL: u8 = 0b10;
/// Presence state: absent.
pub const STATE_ABSENT: u8 = 0b00;
/// Presence state: a value.
pub const STATE_VALUE: u8 = 0b01;
/// Presence state: `NULL`.
pub const STATE_NULL: u8 = 0b10;
/// Presence state: the key is present and its value is `NONE`.
pub const STATE_NONE: u8 = 0b11;
/// The most columns a shape holds, at every depth.
pub const MAX_COLUMNS: usize = 4096;
/// The deepest object nesting a shape holds.
pub const MAX_DEPTH: usize = 32;
/// The most rows one BATCH holds of a shape whose rows take no bytes; a writer splits a longer
/// run, and a reader refuses a larger count.
pub const MAX_EMPTY_ROWS: u32 = 65_536;
/// The most shapes one scope announces.
pub const MAX_SHAPES: usize = 65_536;
/// The largest fixed region of a row.
pub const MAX_FIXED: u32 = 1 << 20;

/// Why shaped bytes were refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShapedError(pub String);

impl ShapedError {
	pub(crate) fn new(msg: impl Into<String>) -> Self {
		Self(msg.into())
	}
}

impl std::fmt::Display for ShapedError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "Invalid shaped frame: {}", self.0)
	}
}

impl std::error::Error for ShapedError {}

/// The bytes of a row's presence region of `bits` bits.
#[inline]
pub const fn presence_bytes(bits: u32) -> usize {
	(bits as usize).div_ceil(8)
}
