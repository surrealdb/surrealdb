//! Packed rows: the rows of a table generation whose descriptor
//! ([`PackedDescriptor`](crate::layout::PackedDescriptor)) lists columns, written by [`pack`], and
//! decoded whole by [`PackedRow::decode`]. A reader that takes a packed row in place takes it only
//! as a [`CheckedPackedRow`], which
//! [`StoredRecord::checked_packed`](crate::record::StoredRecord::checked_packed) makes by walking
//! the row through the same code as the whole decode, instantiated to build nothing. So an
//! in-place read refuses every row the whole decode refuses, with the same error, whichever
//! fields it asks for.
//!
//! # The row
//!
//! A packed row is a v1 record ([`StoredRecord`](crate::record::StoredRecord)) whose header names
//! one of the packed layouts and whose generation names the descriptor the body is read against.
//! The body, with `P` presence bits, `B` bool bits, `F` bytes of fixed region (presence and bool
//! bytes included) and `N` variable fields:
//!
//! ```text
//! body  = presence[ceil(P/8)] bools[ceil(B/8)] fixed-slots | ends[N*w] var[L] trailer?
//!         |<------------------------ F bytes ---------------->|
//! ```
//!
//! * Bit `i` of a bit region is bit `i % 8`, least significant first, of byte `i / 8` of that
//!   region. Bits past `P` and past `B` are zero.
//! * A fixed slot sits at the offset its column names, from the start of the body, with no padding.
//! * `ends[i]` is a little-endian unsigned integer of width `w`, counted from the start of `var`.
//!   Variable slice `i` is `var[ends[i-1]..ends[i]]`, with `0` before the first. `L` is the last
//!   end, or `0` when `N` is `0`. The layout value names `w`: 1, 2 or 4 for the layouts with 1-, 2-
//!   and 4-byte ends.
//! * The width is the narrowest of 1, 2 and 4 whose maximum is at least `L`, and a row with no
//!   variable field is in the layout with 1-byte ends.
//! * With ESCAPES clear the body ends exactly at the end of `var`. With ESCAPES set, the bytes
//!   after it are the trailer, which holds at least one entry.
//!
//! The row's own `"id"` is never in it: a reader restores the id from the record's key.
//!
//! The row-level refusals (the narrowest end width, no padding, ESCAPES agreeing with the
//! trailer, the remainder rules) are permanent reader rules, like the descriptor rules of
//! [`PackedDescriptor::validate`](crate::layout::PackedDescriptor::validate). A writer that
//! relaxes one must add a descriptor field that raises the descriptor's read floor.
//!
//! # Presence
//!
//! A column's presence is meaningful only while every object node that encloses it is present.
//!
//! | Presence | Bits | Key absent | `NULL` | Present |
//! |---|---|---|---|---|
//! | `Required` | none | not packable | not packable, except in a `Value` column | slot |
//! | `Optional` | `b` | `b` clear | not packable, except in a `Value` column | `b` set |
//! | `Nullable` | `b` | not packable | `b` clear | `b` set |
//! | `OptionalNullable` | `b`, `b + 1` | both clear | only `b + 1` set | only `b` set |
//!
//! Both bits of an `OptionalNullable` set is malformed. When an enclosing object node is absent or
//! `NULL`, every column under it has its presence bits clear, its bool bit clear, its fixed slot
//! all zero, its variable slice empty and no trailer entry. A column that is absent or `NULL` in a
//! present node is held the same way.
//!
//! A `NONE` is never in a column. A `NULL` is held by the presence bits, except in a `Value` column
//! whose presence has no `NULL` state, where the slice holds it. Either way the decode yields
//! `NULL`.
//!
//! # The trailer
//!
//! A fixed slot of a kind that escapes (a datetime or a duration beyond what 8 bytes hold) holds a
//! reserved extreme, and the value is in the trailer:
//!
//! ```text
//! trailer = entry+
//! entry   = column payload[12]
//! column  = canonical LEB128 of the column's index in the descriptor
//! ```
//!
//! Entries ascend strictly by column index, and each names a present column whose slot holds an
//! escape value, once. The payload is the 12-byte DATETIME or DURATION payload of the stored
//! grammar, and it is the value the writer escapes to exactly that slot value: a datetime slot at
//! [`DATETIME_BELOW`] holds a datetime before the 8-byte range, a slot at [`DATETIME_ABOVE`] one
//! after it, and a duration slot at [`DURATION_ESCAPE`] a duration of `u64::MAX` nanoseconds or
//! more. A payload that fits its slot, lies on the other side, or is a leap second is refused. So a
//! slot's raw value always orders correctly against its own trailer value.
//!
//! # Open nodes
//!
//! A `Remainder` column holds, as one stored OBJECT, the keys of an open object node that have no
//! column of their own. A present remainder holds at least one key and no `NONE`, and the root's
//! holds no `"id"`. A key that has a column of its own may also be in the remainder when that
//! column is absent. The whole-row decode merges the remainder's entries into their node in key
//! order, so it places an absent column's key in the node through the remainder, and it refuses a
//! key that is in both a column holding a value or `NULL` and the remainder.
//!
//! # What the writer does
//!
//! [`pack`] writes a record's data, the object whose `"id"` is the record's key, as one row of a
//! descriptor, in one pass over the record's sorted keys and the descriptor's columns, which ascend
//! in the same order. It visits each column once and writes into one buffer: the fixed region is
//! zeroed and filled in place, variable values are appended in variable-index order, and the end
//! offsets, reserved at 4 bytes, are narrowed in place once the variable region's length is known.
//! Every grammar value is encoded at the depth its reader decodes it at.
//!
//! * A column whose value is absent or `NULL`, and every column under an absent or `NULL` object
//!   node, is written empty: its presence bits clear (or set to say `NULL`), its fixed slot zero,
//!   its variable slice empty.
//! * A value that does not fit its column, a key that has no column in a closed node, and a
//!   variable region longer than `u32::MAX` bytes make [`pack`] answer [`Packing::Misfit`], and the
//!   caller writes the record in the document layout. A key that has a column is never written into
//!   a remainder, so a record has one packed form.
//! * Every row [`pack`] returns has passed [`check_packed`], whatever the build.
//!
//! # What the decode checks
//!
//! The descriptor is validated when it is decoded, so the offsets it names lie inside the sizes it
//! declares. Beyond that:
//!
//! * [`PackedRow::new`] checks, once per row and in constant time, the envelope, that the body is
//!   at least as long as the fixed region and end-offset table, where the row ends, the canonical
//!   end-offset width, the trailer's presence against ESCAPES, and the padding bits.
//! * [`PackedRow::decode`] checks every column and what spans columns: the presence state of each
//!   column and of every object node that encloses it, the slot or variable slice and the value
//!   bytes of each present column, every remainder (an object with at least one key, with no `NONE`
//!   among its values and, at the root, no `"id"`), that a column the row leaves empty holds
//!   nothing, that no key is both in a column holding a value or `NULL` and in a remainder, and
//!   that every trailer entry is consumed in column order.
//!
//! [`PackedRow::visit`] reads a row for a writer that copies the record out without building it:
//! it walks the columns in key order and reads only what it hands on, so it does not prove the
//! rules that span what it does not read. Those are the writer's to keep ([`check_packed`]).
//!
//! The check of a [`CheckedPackedRow`] is [`PackedRow::decode`] run in a mode that constructs no
//! value: one walk, one body for every rule, so the two accept the same rows and refuse them in
//! the same order. A refusal is the decode's own error.
//!
//! Every failure is a [`PackedError`]; none panics, and none reads outside the row's bytes.
//!
//! # Totality
//!
//! A record a descriptor cannot hold (a value that does not fit its column, a key with no column
//! in a closed node) is not a packed row: it is written in the document layout stamped with the
//! same generation, which every reader of the generation accepts.
//!
//! # Physical kinds
//!
//! One table gives every [`PhysicalKind`](crate::layout::PhysicalKind) its width, how a value is
//! written, how a present slot decodes whole, how it is checked and read in place, and whether it
//! escapes. The row grammar above is the
//! same for every kind.

#[cfg(test)]
mod check_tests;
mod encode;
mod kind;
mod mode;
mod row;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod visit_tests;

pub use encode::{Packing, check_packed, pack};
pub use row::{CheckedPackedRow, FieldRef, PackedRow, PackedVisitor, PathRead};

/// The 8-byte datetime slot value of a datetime before the slot's nanosecond range, or at exactly
/// `i64::MIN` nanoseconds: its value is in the trailer, and it orders below every datetime the slot
/// holds.
pub const DATETIME_BELOW: i64 = i64::MIN;

/// The 8-byte datetime slot value of a datetime after the slot's nanosecond range, or at exactly
/// `i64::MAX` nanoseconds: its value is in the trailer, and it orders above every datetime the slot
/// holds.
pub const DATETIME_ABOVE: i64 = i64::MAX;

/// The 8-byte duration slot value of a duration of `u64::MAX` nanoseconds or more: its value is in
/// the trailer, and it orders above every duration the slot holds.
pub const DURATION_ESCAPE: u64 = u64::MAX;

/// Whether an 8-byte datetime slot holds one of its two escape values.
pub const fn is_datetime_escape(ns: i64) -> bool {
	ns == DATETIME_BELOW || ns == DATETIME_ABOVE
}

/// Why a packed row, or one of its fields, was refused.
///
/// Messages name column indexes, never record data.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum PackedError {
	#[error("a record not in a packed layout was read as a packed row")]
	NotPacked,
	#[error("a packed row has no column {column}")]
	NoColumn {
		column: u32,
	},
	#[error("a packed row is shorter than its fixed region and end-offset table")]
	Short,
	#[error("a packed row does not end where its variable region or trailer does")]
	Length,
	#[error("a packed row's end offsets are not the narrowest that hold its variable region")]
	Width,
	#[error("a packed row sets a bit past its presence or bool count")]
	Padding,
	#[error(
		"a packed row's end offset {var} is below the one before it or past its variable region"
	)]
	EndOffset {
		var: u32,
	},
	#[error("a packed row's presence state for column {column} is not defined")]
	Presence {
		column: u32,
	},
	#[error("a packed row holds column {column} under an absent object")]
	UnderAbsent {
		column: u32,
	},
	#[error("a packed row's slot for absent column {column} is not empty")]
	Unused {
		column: u32,
	},
	#[error("a packed row's code for column {column} is past its list")]
	Code {
		column: u32,
	},
	#[error("a packed row's value for column {column} is malformed: {reason}")]
	Slot {
		column: u32,
		reason: &'static str,
	},
	#[error("a packed row's value for column {column} does not read: {error}")]
	Stored {
		column: u32,
		error: crate::val::stored::Error,
	},
	#[error("a packed row holds the key of column {column} in its remainder too")]
	KeyInColumnAndRemainder {
		column: u32,
	},
	#[error("a packed row's root remainder holds an id")]
	RemainderId,
	#[error("a packed row's trailer is malformed")]
	Trailer,
	#[error(
		"a packed row's escaped value for column {column} is missing, repeated, out of order, or not one its slot's escape value stands for"
	)]
	Escape {
		column: u32,
	},
	#[cfg(test)]
	#[error("a field plan was asked of a descriptor that lays rows out as documents")]
	NotPackedDescriptor,
	#[error("a packed row's envelope is not the one it was written with")]
	Envelope,
}
