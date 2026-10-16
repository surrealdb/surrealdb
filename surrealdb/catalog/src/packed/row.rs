//! Reading packed rows.
//!
//! [`PackedRow::new`] proves a row's shape against its descriptor once. [`PackedRow::decode`]
//! checks every column and what spans columns, then builds the record's data. The same walk,
//! instantiated to build nothing ([`Check`]), is the check a row passes to become a
//! [`CheckedPackedRow`], the only form of a packed row a reader takes in place: it refuses every
//! row the whole decode refuses, with the same error, whichever fields the reader asks of it. The
//! byte-level layout and what the decode checks are in the [module documentation](super).
//!
//! A descriptor given to a row has passed
//! [`PackedDescriptor::validate`](crate::layout::PackedDescriptor::validate), which every
//! descriptor a [`TableLayouts`](crate::layout::TableLayouts) lists has. Every offset a read
//! follows is nevertheless taken with a checked slice, so a descriptor that did not pass it can
//! make a read refuse or answer wrongly, never read outside the row.

use std::cmp::Ordering;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::Relaxed;

use chrono::{DateTime, Utc};
use surrealdb_types::Decimal;

use super::PackedError;
use super::kind::{self, Slot};
use super::mode::{Build, Check, Collisions, Mode, RemainderView, refuse_remainder};
use crate::layout::{Column, ColumnRole, Location, PackedDescriptor, PhysicalKind, Presence};
use crate::record::StoredRecord;
use crate::val::stored::{self, ValueRef, varint};
use crate::val::{Object, Strand, TableName, Value};

/// A trailer entry: its column, its payload, and where the next entry starts.
type TrailerEntry<'a> = (u64, &'a [u8], usize);

/// The key the record's own id is restored under.
const ID_KEY: Strand = Strand::new_static("id");

const _: () = assert!(usize::BITS >= 32, "offsets are widened from u32 to usize");

/// A `u32` offset or index as a `usize`, which holds it on every supported target.
#[inline]
pub(super) const fn widen(v: u32) -> usize {
	v as usize
}

/// A column or var index as the `u32` errors carry. An index no descriptor holds is `u32::MAX`.
#[inline]
fn id_of(index: usize) -> u32 {
	u32::try_from(index).unwrap_or(u32::MAX)
}

/// A packed row whose shape has been proved against its descriptor: the envelope, the length,
/// where the row ends, the end-offset width and the padding bits. [`decode`](Self::decode) reads
/// every column of it.
#[derive(Clone, Copy, Debug)]
pub struct PackedRow<'a> {
	descriptor: &'a PackedDescriptor,
	#[cfg(test)]
	edge: bool,
	#[cfg(test)]
	generation: u64,
	body: &'a [u8],
	/// The width of an end offset: 1, 2 or 4.
	width: usize,
	/// Where the bool region starts: the length of the presence region.
	bools: usize,
	/// Where the end-offset table starts: the descriptor's fixed size.
	ends: usize,
	/// Where the variable region starts: after the end-offset table.
	var: usize,
	/// The length of the variable region: the last end offset.
	var_len: usize,
	/// The bytes after the variable region.
	trailer: &'a [u8],
}

impl<'a> PackedRow<'a> {
	/// Proves `record`'s shape against `descriptor`.
	///
	/// Refuses, in constant time: a record that is not packed (`NotPacked`); a body shorter than
	/// the descriptor's fixed region and end-offset table (`Short`); a variable region that runs
	/// past the body (`Length`); an end-offset width that is not the narrowest the variable region
	/// needs, or that is not 1 when there are no variable fields (`Width`); a trailer without
	/// ESCAPES, or ESCAPES without a trailer (`Length`, `Trailer`); and a set bit past the presence
	/// or bool count (`Padding`).
	pub fn new(
		record: &StoredRecord<'a>,
		descriptor: &'a PackedDescriptor,
	) -> Result<Self, PackedError> {
		if !record.is_packed() {
			return Err(PackedError::NotPacked);
		}
		let width = match record.layout() {
			layout if layout == StoredRecord::PACKED_U8 => 1,
			layout if layout == StoredRecord::PACKED_U16 => 2,
			layout if layout == StoredRecord::PACKED_U32 => 4,
			_ => return Err(PackedError::NotPacked),
		};
		let body = record.body();
		let ends = widen(descriptor.fixed_size);
		let count = widen(descriptor.var_count);
		let table = count.checked_mul(width).ok_or(PackedError::Short)?;
		let var = ends.checked_add(table).ok_or(PackedError::Short)?;
		if body.len() < var {
			return Err(PackedError::Short);
		}
		let var_len = match table.checked_sub(width) {
			// At least one variable field: the last end offset is the region's length.
			Some(last) => {
				let at = ends.checked_add(last).ok_or(PackedError::Short)?;
				read_end(body, at, width).ok_or(PackedError::Short)?
			}
			None => 0,
		};
		let end = var.checked_add(var_len).ok_or(PackedError::Length)?;
		if end > body.len() {
			return Err(PackedError::Length);
		}
		let canonical = if count == 0 {
			width == 1
		} else {
			kind::narrowest_width(var_len) == Some(width)
		};
		if !canonical {
			return Err(PackedError::Width);
		}
		let trailer = body.get(end..).ok_or(PackedError::Length)?;
		match (record.has_escapes(), trailer.is_empty()) {
			(true, true) => return Err(PackedError::Trailer),
			(false, false) => return Err(PackedError::Length),
			_ => {}
		}
		let presence_bits = widen(descriptor.presence_bits);
		let bools = presence_bits.div_ceil(8);
		check_padding(body, 0, presence_bits)?;
		check_padding(body, bools, widen(descriptor.bool_bits))?;
		Ok(Self {
			descriptor,
			#[cfg(test)]
			edge: record.is_edge(),
			#[cfg(test)]
			generation: record.stamp(),
			body,
			width,
			bools,
			ends,
			var,
			var_len,
			trailer,
		})
	}

	/// The generation the row is stamped with.
	#[cfg(test)]
	pub fn generation(&self) -> u64 {
		self.generation
	}

	/// Whether the row's header says the record is an edge.
	#[cfg(test)]
	pub fn is_edge(&self) -> bool {
		self.edge
	}

	/// The descriptor the row is read against.
	#[cfg(test)]
	pub fn descriptor(&self) -> &'a PackedDescriptor {
		self.descriptor
	}

	/// This row, checked: the whole decode of it run to build nothing ([`Check`]), which refuses
	/// exactly the rows [`decode`](Self::decode) refuses, with the same error. Only
	/// [`StoredRecord::checked_packed`] calls it.
	pub(crate) fn checked(self) -> Result<CheckedPackedRow<'a>, PackedError> {
		// The record's own `id` sits at its place in key order and is never inspected.
		self.walk::<Check>(Some(()))?;
		Ok(CheckedPackedRow {
			row: self,
		})
	}

	/// Checks that every end offset is at least the one before it and at most the variable
	/// region's length.
	pub(crate) fn check_ends(&self) -> Result<(), PackedError> {
		let count = self.var.saturating_sub(self.ends) / self.width;
		let mut previous = 0usize;
		for index in 0..count {
			let end = self.end(index)?;
			if end < previous || end > self.var_len {
				return Err(PackedError::EndOffset {
					var: id_of(index),
				});
			}
			previous = end;
		}
		Ok(())
	}

	/// Checks that the trailer is framed by the escape widths of the columns its entries name, that
	/// its entries ascend strictly by column, and that every entry names a column whose slot holds
	/// an escape value.
	pub(crate) fn check_trailer(&self) -> Result<(), PackedError> {
		let mut at = 0usize;
		let mut previous: Option<u64> = None;
		while let Some((entry, _, next)) = self.entry_at(at)? {
			if previous.is_some_and(|before| entry <= before) {
				return Err(PackedError::Trailer);
			}
			previous = Some(entry);
			let index = usize::try_from(entry).map_err(|_| PackedError::Trailer)?;
			let column = self.descriptor.columns.get(index).ok_or(PackedError::Trailer)?;
			let (Location::Fixed(offset), Some(escape)) =
				(column.location, kind::ops(column.kind).escape)
			else {
				return Err(PackedError::Trailer);
			};
			let slot = self.fixed(widen(offset), fixed_width(column, id_of(index))?)?;
			if !(escape.at_escape)(slot) {
				return Err(PackedError::Escape {
					column: id_of(index),
				});
			}
			at = next;
		}
		Ok(())
	}

	/// The record's data: every column and every open node's remainder, with `id` merged into the
	/// root at its place in key order when given. No column or remainder holds a root `id`.
	///
	/// Checks every column, that a column the row leaves empty holds nothing, that no key is both
	/// in a column that holds a value or `NULL` and in a remainder, and that every trailer entry is
	/// consumed in column order.
	pub fn decode(&self, id: Option<Value>) -> Result<Object, PackedError> {
		self.walk::<Build>(id)
	}

	/// The walk of the whole row in mode `M`: [`decode`](Self::decode) in [`Build`], the check of
	/// [`checked`](Self::checked) in [`Check`]. `id` is the root's `id` entry, when it has one.
	fn walk<M: Mode>(&self, id: Option<M::Out<Value>>) -> Result<M::Out<Object>, PackedError> {
		let mut cursor = Cursor {
			column: 0,
			trailer: 0,
		};
		let root = self.node::<M>(&mut cursor, &[], true, id)?;
		if cursor.column != self.descriptor.columns.len() {
			return Err(PackedError::NoColumn {
				column: id_of(cursor.column),
			});
		}
		if cursor.trailer != self.trailer.len() {
			return Err(PackedError::Trailer);
		}
		Ok(root)
	}

	// Reading a field in place, of a row that has been checked

	/// Reads column `index` in place: the presence state of every enclosing node and of the
	/// column, then its slot (one load at a constant offset for a fixed slot), or the open-node
	/// lookup when the column is empty.
	///
	/// Each enclosing node is found by a search of the columns between the node above it and the
	/// column, and an open node's remainder is the column right after the node's, so a column at
	/// the root is read with no search at all.
	///
	/// The row has been checked whole, which proves that a column the row leaves empty holds
	/// nothing, the trailer an escape is read from and the remainder a lookup reads, so the read
	/// follows the offsets the check proved and re-checks none of them.
	fn read_column(&self, index: usize) -> Result<FieldRef<'a>, PackedError> {
		let id = id_of(index);
		let descriptor: &'a PackedDescriptor = self.descriptor;
		let column: &'a Column = descriptor.columns.get(index).ok_or(PackedError::NoColumn {
			column: id,
		})?;
		let missing = PackedError::NoColumn {
			column: id,
		};
		// The nodes that enclose a field are the proper prefixes of its path; those that enclose a
		// remainder are the prefixes up to and including its own node.
		let enclosing = match column.role {
			ColumnRole::Field => column.path.len().saturating_sub(1),
			ColumnRole::Remainder => column.path.len(),
		};
		// The innermost present node above the column read so far (`None` for the root), and the
		// first column that may lie below it.
		let mut parent: Option<usize> = None;
		let mut from = 0usize;
		for depth in 0..enclosing {
			let node_path = column.path.get(..=depth).ok_or(missing)?;
			let node_index =
				column_between(descriptor, from, index, &node_path.iter().map(String::as_str))
					.ok_or(missing)?;
			let node = descriptor.columns.get(node_index).ok_or(missing)?;
			let state = self.state(node.presence, id_of(node_index))?;
			if state == State::Present {
				parent = Some(node_index);
				from = node_index.saturating_add(1);
				continue;
			}
			// The column lies under an absent or NULL node, so every node between that one and
			// the column has its bits clear, the column's own bits are clear and its slot is
			// unused, as the whole-row decode requires.
			return if state == State::Null || column.role == ColumnRole::Remainder {
				Ok(FieldRef::Absent)
			} else {
				let keys = column.path.get(depth..).ok_or(missing)?;
				self.lookup_remainder(self.remainder_of(parent)?, keys.iter().map(String::as_str))
			};
		}
		match self.state(column.presence, id)? {
			State::Present => self.read_slot(column, index),
			State::Null => Ok(FieldRef::Null),
			State::Absent => {
				if column.role == ColumnRole::Remainder {
					Ok(FieldRef::Absent)
				} else {
					let last = column.path.len().saturating_sub(1);
					let keys = column.path.get(last..).ok_or(missing)?;
					self.lookup_remainder(
						self.remainder_of(parent)?,
						keys.iter().map(String::as_str),
					)
				}
			}
		}
	}

	/// The value of column `column`, at `index`, which the row holds present: its slot, or the
	/// object node itself.
	fn read_slot(&self, column: &'a Column, index: usize) -> Result<FieldRef<'a>, PackedError> {
		let id = id_of(index);
		let ops = kind::ops(column.kind);
		match column.location {
			Location::None => Ok(FieldRef::Object),
			Location::Bit(bit) => {
				let set = self.bool_bit(bit)?;
				(ops.read)(column, id, Slot::Bit(set))
			}
			Location::Fixed(offset) => {
				let slot = self.fixed(widen(offset), fixed_width(column, id)?)?;
				match ops.escape {
					Some(escape) if (escape.at_escape)(slot) => {
						let payload = self.escape_payload(id)?;
						(ops.read)(
							column,
							id,
							Slot::Escaped {
								slot,
								payload,
							},
						)
					}
					_ => (ops.read)(column, id, Slot::Fixed(slot)),
				}
			}
			Location::Var(var) => {
				let slice = self.var_slice(widen(var))?;
				if column.role == ColumnRole::Remainder {
					ValueRef::new_nested(slice, column.path.len()).map(FieldRef::Stored).map_err(
						|error| PackedError::Stored {
							column: id,
							error,
						},
					)
				} else {
					(ops.read)(column, id, Slot::Var(slice))
				}
			}
		}
	}

	/// The remainder of the open node at column `node`, or of the record root for `None`, when it
	/// has one. A node's remainder sorts right after the node and the root's is the first column,
	/// as the whole decode finds them, so this is one comparison and no search.
	fn remainder_of(&self, node: Option<usize>) -> Result<Option<RemainderSlot>, PackedError> {
		let columns = &self.descriptor.columns;
		let (at, path): (usize, &[String]) = match node {
			Some(node) => {
				let path = columns.get(node).map(|column| column.path.as_slice()).ok_or(
					PackedError::NoColumn {
						column: id_of(node),
					},
				)?;
				(node.saturating_add(1), path)
			}
			None => (0, &[]),
		};
		match columns.get(at) {
			Some(column) if column.role == ColumnRole::Remainder && column.path == path => {
				remainder_slot(column, at).map(Some)
			}
			_ => Ok(None),
		}
	}

	/// The value at `keys` in the remainder `remainder` holds, when the node it completes has one.
	/// Absent for a closed node, an empty remainder, a miss, and a step that is not an object.
	fn lookup_remainder<'k>(
		&self,
		remainder: Option<RemainderSlot>,
		mut keys: impl Iterator<Item = &'k str>,
	) -> Result<FieldRef<'a>, PackedError> {
		let Some(remainder) = remainder else {
			return Ok(FieldRef::Absent);
		};
		if !self.presence_bit(remainder.bit)? {
			return Ok(FieldRef::Absent);
		}
		let bad = |error: stored::Error| PackedError::Stored {
			column: remainder.column,
			error,
		};
		// The check validated the remainder whole, which confirms the key order of every object in
		// it, so a lookup searches its keys by halves without reading them all first.
		let slice = self.var_slice(remainder.var)?;
		let root = ValueRef::new_nested(slice, remainder.depth).map_err(bad)?;
		let Some(object) = root.as_object().map_err(bad)? else {
			return Err(PackedError::Slot {
				column: remainder.column,
				reason: "a remainder is not an object",
			});
		};
		let Some(first) = keys.next() else {
			return Ok(FieldRef::Absent);
		};
		let Some(found) = object.validated().get(first).map_err(bad)? else {
			return Ok(FieldRef::Absent);
		};
		descend(remainder.column, found, keys)
	}

	// The regions of the body

	/// Bit `mask` of byte `byte` of the body, which lies in the fixed region.
	#[inline]
	fn bit(&self, byte: usize, mask: u8) -> Result<bool, PackedError> {
		if byte >= self.ends {
			return Err(PackedError::Short);
		}
		let byte = self.body.get(byte).ok_or(PackedError::Short)?;
		Ok((byte & mask) != 0)
	}

	/// Presence bit `bit`.
	#[inline]
	fn presence_bit(&self, bit: u32) -> Result<bool, PackedError> {
		self.bit(widen(bit / 8), 1u8 << (bit % 8))
	}

	/// Bool bit `bit`.
	#[inline]
	fn bool_bit(&self, bit: u32) -> Result<bool, PackedError> {
		let byte = self.bools.checked_add(widen(bit / 8)).ok_or(PackedError::Short)?;
		self.bit(byte, 1u8 << (bit % 8))
	}

	/// The fixed slot of `width` bytes at `offset`, which lies in the fixed region.
	#[inline]
	fn fixed(&self, offset: usize, width: usize) -> Result<&'a [u8], PackedError> {
		let end = offset.checked_add(width).ok_or(PackedError::Short)?;
		if end > self.ends {
			return Err(PackedError::Short);
		}
		let body: &'a [u8] = self.body;
		body.get(offset..end).ok_or(PackedError::Short)
	}

	/// End offset `index`, which lies in the end-offset table.
	#[inline]
	fn end(&self, index: usize) -> Result<usize, PackedError> {
		let bad = PackedError::EndOffset {
			var: id_of(index),
		};
		let from = index.checked_mul(self.width).ok_or(bad)?;
		let to = from.checked_add(self.width).ok_or(bad)?;
		if to > self.var.saturating_sub(self.ends) {
			return Err(bad);
		}
		let at = self.ends.checked_add(from).ok_or(bad)?;
		read_end(self.body, at, self.width).ok_or(bad)
	}

	/// Variable slice `var`: between end offset `var - 1` (0 for the first) and end offset `var`,
	/// which must be ordered and inside the variable region.
	#[inline]
	fn var_slice(&self, var: usize) -> Result<&'a [u8], PackedError> {
		let bad = PackedError::EndOffset {
			var: id_of(var),
		};
		let start = match var.checked_sub(1) {
			Some(before) => self.end(before)?,
			None => 0,
		};
		let end = self.end(var)?;
		if start > end || end > self.var_len {
			return Err(bad);
		}
		let from = self.var.checked_add(start).ok_or(bad)?;
		let to = self.var.checked_add(end).ok_or(bad)?;
		let body: &'a [u8] = self.body;
		body.get(from..to).ok_or(bad)
	}

	// Presence

	/// The state of a column or node from its presence bits.
	#[inline]
	fn state(&self, presence: Presence, column: u32) -> Result<State, PackedError> {
		Ok(match presence {
			Presence::Required => State::Present,
			Presence::Optional {
				bit,
			} => {
				if self.presence_bit(bit)? {
					State::Present
				} else {
					State::Absent
				}
			}
			Presence::Nullable {
				bit,
			} => {
				if self.presence_bit(bit)? {
					State::Present
				} else {
					State::Null
				}
			}
			Presence::OptionalNullable {
				bit,
			} => {
				let next = bit.checked_add(1).ok_or(PackedError::Presence {
					column,
				})?;
				match (self.presence_bit(bit)?, self.presence_bit(next)?) {
					(true, false) => State::Present,
					(false, false) => State::Absent,
					(false, true) => State::Null,
					(true, true) => {
						return Err(PackedError::Presence {
							column,
						});
					}
				}
			}
		})
	}

	/// Checks that `presence`'s bits are all clear, as a column under an absent or NULL node, or an
	/// absent one, holds them.
	fn require_clear(&self, presence: Presence, column: u32) -> Result<(), PackedError> {
		let under = PackedError::UnderAbsent {
			column,
		};
		match presence {
			Presence::Required => {}
			Presence::Optional {
				bit,
			}
			| Presence::Nullable {
				bit,
			} => {
				if self.presence_bit(bit)? {
					return Err(under);
				}
			}
			Presence::OptionalNullable {
				bit,
			} => {
				let next = bit.checked_add(1).ok_or(PackedError::Presence {
					column,
				})?;
				if self.presence_bit(bit)? || self.presence_bit(next)? {
					return Err(under);
				}
			}
		}
		Ok(())
	}

	// The trailer

	/// The trailer entry at `at`: its column, its payload, and where the next entry starts. `None`
	/// at the end of the trailer. The payload's width is the escape width of the kind of the
	/// column the entry names.
	fn entry_at(&self, at: usize) -> Result<Option<TrailerEntry<'a>>, PackedError> {
		let trailer: &'a [u8] = self.trailer;
		let rest = trailer.get(at..).ok_or(PackedError::Trailer)?;
		if rest.is_empty() {
			return Ok(None);
		}
		let (column, used) = varint::read_u64(rest).map_err(|_| PackedError::Trailer)?;
		let named = usize::try_from(column)
			.ok()
			.and_then(|named| self.descriptor.columns.get(named))
			.ok_or(PackedError::Trailer)?;
		let escape = kind::ops(named.kind).escape.ok_or(PackedError::Trailer)?;
		let start = at.checked_add(used).ok_or(PackedError::Trailer)?;
		let end = start.checked_add(escape.width).ok_or(PackedError::Trailer)?;
		let payload = trailer.get(start..end).ok_or(PackedError::Trailer)?;
		Ok(Some((column, payload, end)))
	}

	/// The payload of the trailer entry of `column`, in a row that has been checked: the check
	/// proved the trailer's framing, its ascending entries and that each names a slot that holds an
	/// escape value.
	fn escape_payload(&self, column: u32) -> Result<&'a [u8], PackedError> {
		let mut at = 0usize;
		while let Some((entry, payload, next)) = self.entry_at(at)? {
			match entry.cmp(&u64::from(column)) {
				Ordering::Equal => return Ok(payload),
				Ordering::Greater => break,
				Ordering::Less => at = next,
			}
		}
		Err(PackedError::Escape {
			column,
		})
	}

	/// The offset in the trailer of the first entry that names column `first` or a later one: where
	/// a decode that starts at column `first` takes its escapes from. The row has been checked, so
	/// the entries ascend.
	fn trailer_from(&self, first: usize) -> Result<usize, PackedError> {
		let first = u64::try_from(first).unwrap_or(u64::MAX);
		let mut at = 0usize;
		while let Some((entry, _, next)) = self.entry_at(at)? {
			if entry >= first {
				break;
			}
			at = next;
		}
		Ok(at)
	}

	/// Takes the trailer entry that must come next, for `column`, in a full decode.
	fn take_escape(&self, cursor: &mut Cursor, column: u32) -> Result<&'a [u8], PackedError> {
		let Some((entry, payload, next)) = self.entry_at(cursor.trailer)? else {
			return Err(PackedError::Escape {
				column,
			});
		};
		match entry.cmp(&u64::from(column)) {
			Ordering::Equal => {
				cursor.trailer = next;
				Ok(payload)
			}
			// An entry for a column the decode has passed, which holds no escape value.
			Ordering::Less => Err(PackedError::Trailer),
			Ordering::Greater => Err(PackedError::Escape {
				column,
			}),
		}
	}

	// Decoding the whole row

	/// Walks the object node at `path`, whose columns follow the cursor, in mode `M`. `present`
	/// says whether the node is present: under an absent or NULL node every column and remainder
	/// must be empty. `id`, for the root, is merged in at its place in key order.
	fn node<M: Mode>(
		&self,
		cursor: &mut Cursor,
		path: &[String],
		present: bool,
		id: Option<M::Out<Value>>,
	) -> Result<M::Out<Object>, PackedError> {
		let columns = &self.descriptor.columns;
		let mut remainder: Option<Open<M::Remainder<'a>>> = None;
		if let Some(column) = columns.get(cursor.column)
			&& column.role == ColumnRole::Remainder
			&& column.path.as_slice() == path
		{
			let index = id_of(cursor.column);
			remainder = self.decode_remainder::<M>(column, index, path, present)?;
			cursor.column += 1;
		}
		// The key-collision rules run only for a node that has a remainder. The columns of one
		// that has none are walked by a loop that holds no trace of them.
		if remainder.is_some() {
			self.entries::<M, true>(cursor, path, present, id, remainder)
		} else {
			self.entries::<M, false>(cursor, path, present, id, None)
		}
	}

	/// The columns of the object node at `path`, which hold `remainder` where `OPEN` is true and
	/// none where it is false, in mode `M`: [`node`](Self::node) after its remainder.
	#[inline(always)]
	fn entries<M: Mode, const OPEN: bool>(
		&self,
		cursor: &mut Cursor,
		path: &[String],
		present: bool,
		mut id: Option<M::Out<Value>>,
		mut remainder: Option<Open<M::Remainder<'a>>>,
	) -> Result<M::Out<Object>, PackedError> {
		let columns = &self.descriptor.columns;
		let mut entries = M::seq::<(Strand, Value)>(0);
		while let Some(column) = columns.get(cursor.column)
			&& column.path.len() > path.len()
			&& column.path.starts_with(path)
		{
			let index = id_of(cursor.column);
			let child_len = path.len().saturating_add(1);
			if column.role != ColumnRole::Field || column.path.len() != child_len {
				return Err(PackedError::NoColumn {
					column: index,
				});
			}
			let name = column.path.get(path.len()).ok_or(PackedError::NoColumn {
				column: index,
			})?;
			if name.as_str() > "id"
				&& let Some(id) = id.take()
			{
				if OPEN {
					Open::visit(&mut remainder, "id")?;
				}
				M::push::<(Strand, Value)>(&mut entries, id_entry::<M>(id));
			}
			let state = if present {
				self.state(column.presence, index)?
			} else {
				self.require_clear(column.presence, index)?;
				State::Absent
			};
			if column.kind == PhysicalKind::Object {
				cursor.column += 1;
				let child = self.node::<M>(cursor, &column.path, state == State::Present, None)?;
				match state {
					State::Present => {
						if OPEN {
							Open::visit(&mut remainder, name)?;
						}
						let value = M::map::<Object, Value>(child, Value::Object);
						M::push::<(Strand, Value)>(&mut entries, entry::<M>(name, value));
					}
					State::Null => {
						if OPEN {
							Open::visit(&mut remainder, name)?;
						}
						let value = M::make(|| Value::Null);
						M::push::<(Strand, Value)>(&mut entries, entry::<M>(name, value));
					}
					State::Absent => {}
				}
			} else {
				match state {
					State::Present => {
						let value = self.decode_slot::<M>(column, index, cursor)?;
						if OPEN {
							Open::visit(&mut remainder, name)?;
						}
						M::push::<(Strand, Value)>(&mut entries, entry::<M>(name, value));
					}
					State::Null | State::Absent => {
						self.require_unused(column, index)?;
						if state == State::Null {
							if OPEN {
								Open::visit(&mut remainder, name)?;
							}
							let value = M::make(|| Value::Null);
							M::push::<(Strand, Value)>(&mut entries, entry::<M>(name, value));
						}
					}
				}
				cursor.column += 1;
			}
		}
		if let Some(id) = id.take() {
			if OPEN {
				Open::visit(&mut remainder, "id")?;
			}
			M::push::<(Strand, Value)>(&mut entries, id_entry::<M>(id));
		}
		// A key in both a column and the remainder is refused where the node ends, after every
		// column and nested node of it.
		let remainder = match remainder {
			Some(open) => {
				open.collisions.finish()?;
				Some(open.view)
			}
			None => None,
		};
		Ok(M::assemble(entries, remainder))
	}

	/// The remainder column `column`, at `index`, of the node at `path`: its walked value when
	/// present. The slice must be empty when the remainder is, and the remainder must be clear
	/// under an absent node.
	fn decode_remainder<M: Mode>(
		&self,
		column: &Column,
		index: u32,
		path: &[String],
		present: bool,
	) -> Result<Option<Open<M::Remainder<'a>>>, PackedError> {
		let (
			Presence::Optional {
				bit,
			},
			Location::Var(var),
		) = (column.presence, column.location)
		else {
			return Err(PackedError::Slot {
				column: index,
				reason: "a remainder of the wrong shape",
			});
		};
		let set = self.presence_bit(bit)?;
		let slice = self.var_slice(widen(var))?;
		if !set {
			return if slice.is_empty() {
				Ok(None)
			} else {
				Err(PackedError::Unused {
					column: index,
				})
			};
		}
		if !present {
			return Err(PackedError::UnderAbsent {
				column: index,
			});
		}
		let bad = |error| PackedError::Stored {
			column: index,
			error,
		};
		let view = M::remainder(slice, path.len()).map_err(bad)?;
		refuse_remainder(view.facts().map_err(bad)?, index, path.is_empty())?;
		Ok(Some(Open {
			view,
			collisions: Collisions::new(index),
		}))
	}

	/// Walks the present slot of leaf column `column`, at `index`, taking its trailer entry when
	/// the slot holds an escape value.
	fn decode_slot<M: Mode>(
		&self,
		column: &Column,
		index: u32,
		cursor: &mut Cursor,
	) -> Result<M::Out<Value>, PackedError> {
		let ops = kind::ops(column.kind);
		let depth = column.path.len();
		match column.location {
			Location::Bit(bit) => {
				let set = self.bool_bit(bit)?;
				M::slot(ops, column, index, Slot::Bit(set), depth)
			}
			Location::Fixed(offset) => {
				let width = fixed_width(column, index)?;
				let slot = self.fixed(widen(offset), width)?;
				match ops.escape {
					Some(escape) if (escape.at_escape)(slot) => {
						let payload = self.take_escape(cursor, index)?;
						M::slot(
							ops,
							column,
							index,
							Slot::Escaped {
								slot,
								payload,
							},
							depth,
						)
					}
					_ => M::slot(ops, column, index, Slot::Fixed(slot), depth),
				}
			}
			Location::Var(var) => {
				let slice = self.var_slice(widen(var))?;
				M::slot(ops, column, index, Slot::Var(slice), depth)
			}
			Location::None => Err(PackedError::Slot {
				column: index,
				reason: "an object node has no slot",
			}),
		}
	}

	/// Checks that the slot of `column`, at `index`, which the row leaves empty, holds nothing: a
	/// clear bool bit, a zeroed fixed slot, an empty variable slice.
	fn require_unused(&self, column: &Column, index: u32) -> Result<(), PackedError> {
		let unused = PackedError::Unused {
			column: index,
		};
		match column.location {
			Location::Bit(bit) => {
				if self.bool_bit(bit)? {
					return Err(unused);
				}
			}
			Location::Fixed(offset) => {
				let width = fixed_width(column, index)?;
				if self.fixed(widen(offset), width)?.iter().any(|&byte| byte != 0) {
					return Err(unused);
				}
			}
			Location::Var(var) => {
				if !self.var_slice(widen(var))?.is_empty() {
					return Err(unused);
				}
			}
			Location::None => {}
		}
		Ok(())
	}
}

/// The state of a column or object node as its presence bits say.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
	Absent,
	Null,
	Present,
}

/// Where a full decode is in the descriptor's columns and the trailer.
#[derive(Clone, Copy, Debug)]
struct Cursor {
	/// The index of the next column.
	column: usize,
	/// The offset in the trailer of the next entry.
	trailer: usize,
}

/// The entry `name`: `value` of an object node.
fn entry<M: Mode>(name: &str, value: M::Out<Value>) -> M::Out<(Strand, Value)> {
	M::zip::<Strand, Value>(M::make(|| Strand::new(name)), value)
}

/// The entry of the record's own `id`.
fn id_entry<M: Mode>(id: M::Out<Value>) -> M::Out<(Strand, Value)> {
	M::zip::<Strand, Value>(M::make(|| ID_KEY), id)
}

/// An open node's remainder as the walk holds it: its value, and the collisions between its keys
/// and the keys the node's columns hold.
struct Open<R> {
	view: R,
	collisions: Collisions,
}

impl<R: RemainderView> Open<R> {
	/// Visits `name`, a key the node holds, when the node has a remainder.
	fn visit(open: &mut Option<Self>, name: &str) -> Result<(), PackedError> {
		if let Some(open) = open {
			open.collisions.visit(&open.view, name)?;
		}
		Ok(())
	}
}

/// The width of `column`'s fixed slot.
fn fixed_width(column: &Column, index: u32) -> Result<usize, PackedError> {
	column.kind.fixed_width().map(widen).ok_or(PackedError::Slot {
		column: index,
		reason: "a fixed slot of a kind with no width",
	})
}

/// The value at `keys` below `found`, a value of the remainder at column `column`: absent where a
/// step is not an object or holds no such key, `NULL` as such, and otherwise the value in place.
/// The row's check validated the remainder, so every object in it holds its keys in order.
fn descend<'a, 'k>(
	column: u32,
	mut found: ValueRef<'a>,
	keys: impl Iterator<Item = &'k str>,
) -> Result<FieldRef<'a>, PackedError> {
	let bad = |error: stored::Error| PackedError::Stored {
		column,
		error,
	};
	for key in keys {
		let Some(inner) = found.as_object().map_err(bad)? else {
			return Ok(FieldRef::Absent);
		};
		let Some(next) = inner.validated().get(key).map_err(bad)? else {
			return Ok(FieldRef::Absent);
		};
		found = next;
	}
	match found.tag() {
		stored::tag::NULL => Ok(FieldRef::Null),
		stored::tag::NONE => Err(PackedError::Slot {
			column,
			reason: "NONE in a remainder",
		}),
		_ => Ok(FieldRef::Stored(found)),
	}
}

/// The end offset of `width` bytes at `at` in `bytes`.
#[inline]
fn read_end(bytes: &[u8], at: usize, width: usize) -> Option<usize> {
	let raw = bytes.get(at..at.checked_add(width)?)?;
	match width {
		1 => raw.first().map(|&byte| usize::from(byte)),
		2 => <[u8; 2]>::try_from(raw).ok().map(|raw| usize::from(u16::from_le_bytes(raw))),
		4 => <[u8; 4]>::try_from(raw)
			.ok()
			.and_then(|raw| usize::try_from(u32::from_le_bytes(raw)).ok()),
		_ => None,
	}
}

/// Checks that no bit at or past `bits` is set in the bit region of `bits` bits that starts at
/// `region`.
fn check_padding(body: &[u8], region: usize, bits: usize) -> Result<(), PackedError> {
	let used = bits % 8;
	if used == 0 {
		return Ok(());
	}
	let at = region.checked_add(bits / 8).ok_or(PackedError::Short)?;
	let last = body.get(at).ok_or(PackedError::Short)?;
	if (last >> used) != 0 {
		return Err(PackedError::Padding);
	}
	Ok(())
}

/// The remainder column `column`, at `at`, as a lookup reads it.
fn remainder_slot(column: &Column, at: usize) -> Result<RemainderSlot, PackedError> {
	let id = id_of(at);
	match (column.presence, column.location) {
		(
			Presence::Optional {
				bit,
			},
			Location::Var(var),
		) => Ok(RemainderSlot {
			column: id,
			bit,
			var: widen(var),
			depth: column.path.len(),
		}),
		_ => Err(PackedError::Slot {
			column: id,
			reason: "a remainder of the wrong shape",
		}),
	}
}

/// An open node's remainder: its column, its presence bit (a remainder's presence is `Optional`),
/// its variable index, and the depth of its node, which is 0 for the record root, whose remainder
/// holds no `"id"`.
#[derive(Clone, Copy, Debug)]
struct RemainderSlot {
	column: u32,
	bit: u32,
	var: usize,
	depth: usize,
}

/// The `Field` column at the path `path` among the columns `from..to` of `packed`, by binary search
/// over the column order.
fn column_between<'p, P>(
	packed: &PackedDescriptor,
	from: usize,
	to: usize,
	path: &P,
) -> Option<usize>
where
	P: IntoIterator<Item = &'p str> + Clone,
{
	let columns = packed.columns.get(from..to)?;
	columns
		.binary_search_by(|column| {
			cmp_path(&column.path, path.clone().into_iter())
				.then_with(|| column.role.cmp(&ColumnRole::Field))
		})
		.ok()
		.map(|at| at.saturating_add(from))
}

/// The order of the path `path` against the segments `segments`: by segment, then the shorter
/// first, as [`PackedDescriptor::column_at`] orders them.
fn cmp_path<'p>(path: &[String], mut segments: impl Iterator<Item = &'p str>) -> Ordering {
	let mut path = path.iter();
	loop {
		match (path.next(), segments.next()) {
			(None, None) => return Ordering::Equal,
			(None, Some(_)) => return Ordering::Less,
			(Some(_), None) => return Ordering::Greater,
			(Some(a), Some(b)) => match a.as_str().cmp(b) {
				Ordering::Equal => {}
				other => return other,
			},
		}
	}
}

/// A packed row that has passed the check: the whole decode of it run to build nothing, which
/// refuses every row [`PackedRow::decode`] refuses, whichever fields a reader asks of it. A reader
/// takes a packed row in place only as this.
///
/// Only [`StoredRecord::checked_packed`] makes one, so the reads below follow offsets the check
/// proved and re-check nothing.
#[derive(Clone, Copy, Debug)]
pub struct CheckedPackedRow<'a> {
	row: PackedRow<'a>,
}

impl<'a> CheckedPackedRow<'a> {
	/// The descriptor the row is read against.
	pub fn descriptor(&self) -> &'a PackedDescriptor {
		self.row.descriptor
	}

	/// The root field `name`, as the whole decode holds it: [`FieldRef::Absent`] when the record
	/// holds no such key. A name with no column, or whose column the row leaves absent, is read
	/// from the root's remainder.
	pub fn read_root(&self, name: &str) -> Result<FieldRef<'a>, PackedError> {
		match self.row.descriptor.column_at(&[name], ColumnRole::Field) {
			Some(index) => self.row.read_column(index),
			None => self.read_root_remainder(name),
		}
	}

	/// The key `name` in the root's remainder: what a root name the descriptor has no column for
	/// reads, as [`read_root`](Self::read_root) reads it once it has found no column for the name.
	pub fn read_root_remainder(&self, name: &str) -> Result<FieldRef<'a>, PackedError> {
		self.row.lookup_remainder(self.row.remainder_of(None)?, std::iter::once(name))
	}

	/// The column of the root field `name`, resolved through `hint`: the index the name's column
	/// held when it was last found. The hint is verified against the column on every call, so one
	/// that names another column, or another descriptor's, costs a search and is replaced. `None`
	/// for a name with no column, which leaves the hint as it is;
	/// [`read_root_remainder`](Self::read_root_remainder) reads such a name.
	pub fn root_column(&self, name: &str, hint: &AtomicU32) -> Option<usize> {
		let columns = &self.row.descriptor.columns;
		let hinted = usize::try_from(hint.load(Relaxed)).ok().filter(|&at| {
			columns.get(at).is_some_and(|column| {
				column.role == ColumnRole::Field
					&& matches!(column.path.as_slice(), [only] if only == name)
			})
		});
		if hinted.is_some() {
			return hinted;
		}
		let found = self.row.descriptor.column_at(&[name], ColumnRole::Field)?;
		hint.store(id_of(found), Relaxed);
		Some(found)
	}

	/// Column `index` of the descriptor, as the whole decode holds it at the column's path:
	/// [`FieldRef::Absent`] when the column or an object that encloses it is absent, or a node
	/// encloses it that is `NULL`, and no open ancestor holds the key. An object node that is
	/// present reads as [`FieldRef::Object`], and [`decode_node`](Self::decode_node) builds it.
	pub fn read_column(&self, index: usize) -> Result<FieldRef<'a>, PackedError> {
		self.row.read_column(index)
	}

	/// The path `segments` walked from the root as the record's document is walked: each step reads
	/// the key from the object the step before reached, which is an object node's column, or an
	/// open node's remainder when the node has no column for the key or leaves it absent.
	///
	/// The walk reads each node on the path once, searching for its column among the columns below
	/// the node before it. It stops at the first step that does not reach an object node:
	/// [`PathRead::Missing`] where the key is absent, [`PathRead::Held`] where a stored value holds
	/// it and the path goes on into that value, and [`PathRead::NotAnObject`] where the path goes
	/// on past any other value, `NULL` among them. The whole path answers [`PathRead::Field`].
	pub fn read_path<'p>(
		&self,
		segments: impl Iterator<Item = &'p str> + Clone,
	) -> Result<PathRead<'a>, PackedError> {
		let packed = self.row.descriptor;
		let total = segments.clone().count();
		if total == 0 {
			return Err(PackedError::NoColumn {
				column: u32::MAX,
			});
		}
		// The present object node the walk stands in (`None` for the root), and the first column
		// that may lie below it.
		let mut node: Option<usize> = None;
		let mut from = 0usize;
		let head = segments.clone();
		for (at, name) in segments.enumerate() {
			let depth = at.saturating_add(1);
			let last = depth == total;
			let to = packed.columns.len();
			let (field, column) = match column_between(packed, from, to, &head.clone().take(depth))
			{
				Some(index) => {
					let column = packed.columns.get(index).ok_or(PackedError::NoColumn {
						column: id_of(index),
					})?;
					match self.row.state(column.presence, id_of(index))? {
						State::Present if column.kind == PhysicalKind::Object => {
							if last {
								return Ok(PathRead::Field {
									field: FieldRef::Object,
									column: Some(index),
								});
							}
							node = Some(index);
							from = index.saturating_add(1);
							continue;
						}
						State::Present => (self.row.read_slot(column, index)?, Some(index)),
						State::Null => (FieldRef::Null, Some(index)),
						State::Absent => (self.in_remainder(node, name)?, None),
					}
				}
				None => (self.in_remainder(node, name)?, None),
			};
			return Ok(match field {
				FieldRef::Absent => PathRead::Missing,
				field if last => PathRead::Field {
					field,
					column,
				},
				FieldRef::Stored(value) => PathRead::Held {
					value,
					depth,
				},
				_ => PathRead::NotAnObject,
			});
		}
		Err(PackedError::NoColumn {
			column: u32::MAX,
		})
	}

	/// The key `name` in the remainder of the present node `node` (`None` for the root).
	fn in_remainder(&self, node: Option<usize>, name: &str) -> Result<FieldRef<'a>, PackedError> {
		self.row.lookup_remainder(self.row.remainder_of(node)?, std::iter::once(name))
	}

	/// The object node at column `column`, which the row holds present, decoded alone: the node's
	/// columns, its nested nodes and its remainder, and nothing outside it.
	pub fn decode_node(&self, column: usize) -> Result<Object, PackedError> {
		let packed = self.row.descriptor;
		let index = id_of(column);
		let node = packed.columns.get(column).ok_or(PackedError::NoColumn {
			column: index,
		})?;
		if node.role != ColumnRole::Field || node.kind != PhysicalKind::Object {
			return Err(PackedError::Slot {
				column: index,
				reason: "a column that is not an object node read as one",
			});
		}
		let first = column.saturating_add(1);
		let mut cursor = Cursor {
			column: first,
			trailer: self.row.trailer_from(first)?,
		};
		self.row.node::<Build>(&mut cursor, &node.path, true, None)
	}

	/// The record's data, whole, with `id` merged into the root at its place in key order when
	/// given: [`PackedRow::decode`].
	pub fn decode(&self, id: Option<Value>) -> Result<Object, PackedError> {
		self.row.decode(id)
	}
}

/// What a walk of a path down a checked row reaches ([`CheckedPackedRow::read_path`]).
#[derive(Clone, Copy, Debug)]
pub enum PathRead<'a> {
	/// The value at the whole path, which is never [`FieldRef::Absent`], and its column when the
	/// row holds it in one. An object node always has its column.
	Field {
		field: FieldRef<'a>,
		column: Option<usize>,
	},
	/// A key on the path is absent from the object that should hold it.
	Missing,
	/// The first `depth` segments of the path reach `value`, a stored value of a `Value` column or
	/// a remainder, and the rest of the path steps into it unread.
	Held {
		value: ValueRef<'a>,
		depth: usize,
	},
	/// A proper prefix of the path reaches a value that is no object: a scalar, a literal or
	/// `NULL`.
	NotAnObject,
}

/// One field of a packed row, read in place.
#[derive(Clone, Copy, Debug)]
pub enum FieldRef<'a> {
	/// NONE: the key is absent, or an object that encloses it is absent or `NULL`, and no open
	/// ancestor holds it.
	Absent,
	Null,
	/// An object node that is present; its fields are columns of their own.
	Object,
	Bool(bool),
	Int(i64),
	Float(f64),
	/// A `Decimal16` or `DecimalVar` value, or a `U64` value above `i64::MAX`.
	Decimal(Decimal),
	Uuid(uuid::Uuid),
	Datetime(DateTime<Utc>),
	Duration(std::time::Duration),
	Point(f64, f64),
	String(&'a str),
	/// A `Bytes` or `Fixed(n)` value.
	Bytes(&'a [u8]),
	/// The literal of a literal-union column that the slot's code names.
	Literal(&'a Value),
	/// A record id: its table and its key in the stored key grammar, which the reader that walks
	/// the key validates.
	RecordId(&'a TableName, &'a [u8]),
	/// A `Value` column's value, a remainder, or a value read from an open ancestor's remainder.
	/// Never NULL and never NONE: a NULL from the column's bits, a `Value` slice or a remainder
	/// entry is [`Null`](Self::Null), and an absent one is [`Absent`](Self::Absent). The bytes are
	/// validated by the reader that walks them.
	Stored(ValueRef<'a>),
}
