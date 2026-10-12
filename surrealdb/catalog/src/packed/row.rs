//! Reading packed rows: a row is decoded whole.
//!
//! [`PackedRow::new`] proves a row's shape against its descriptor once, and [`PackedRow::decode`]
//! checks every column and what spans columns, then builds the record's data. Production reads a
//! packed row only this way. A test build also reads one field at a time in place, through
//! `PackedRow::field` or a `FieldPlan`, and the tests hold that reader to the whole decode. The
//! byte-level layout and what the decode checks are in the [module documentation](super).
//!
//! A descriptor given to a row has passed
//! [`PackedDescriptor::validate`](crate::layout::PackedDescriptor::validate), which every
//! descriptor a [`TableLayouts`](crate::layout::TableLayouts) lists has. Every offset a read
//! follows is nevertheless taken with a checked slice, so a descriptor that did not pass it can
//! make a read refuse or answer wrongly, never read outside the row.

use std::cmp::Ordering;
#[cfg(test)]
use std::sync::Arc;

#[cfg(test)]
use chrono::{DateTime, Utc};
use surrealdb_collections::VecMap;
#[cfg(test)]
use surrealdb_types::Decimal;

use super::PackedError;
#[cfg(test)]
use super::kind::KindOps;
use super::kind::{self, Slot};
use crate::layout::{Column, ColumnRole, Location, PackedDescriptor, PhysicalKind, Presence};
#[cfg(test)]
use crate::layout::{LayoutDescriptor, RowLayout};
use crate::record::StoredRecord;
#[cfg(test)]
use crate::val::TableName;
#[cfg(test)]
use crate::val::stored::ValueRef;
use crate::val::stored::{self, varint};
use crate::val::{Object, Strand, Value};

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

	/// Reads column `column` in place.
	///
	/// The column is resolved on each call, which costs a binary search per enclosing object node
	/// and an allocation for a column that has any: a reader of many rows builds a [`FieldPlan`]
	/// once and calls [`field_with`](Self::field_with).
	#[cfg(test)]
	pub fn field(&self, column: usize) -> Result<FieldRef<'a>, PackedError> {
		self.read_planned(&ColumnPlan::new(self.descriptor, column)?)
	}

	/// Reads the column of `plan` in place: the presence state of every enclosing node and of the
	/// column, then its slot (one load at a constant offset for a fixed slot), or the open-node
	/// lookup when the column is empty.
	///
	/// Refuses a plan built for any descriptor allocation but this row's (`ForeignPlan`).
	#[cfg(test)]
	pub fn field_with(&self, plan: &FieldPlan) -> Result<FieldRef<'a>, PackedError> {
		if plan.address != address_of(self.descriptor) {
			return Err(PackedError::ForeignPlan);
		}
		self.read_planned(&plan.plan)
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
		let mut cursor = Cursor {
			column: 0,
			trailer: 0,
		};
		let root = self.node(&mut cursor, &[], true, id)?;
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

	// Reading a field in place

	#[cfg(test)]
	fn read_planned(&self, plan: &ColumnPlan) -> Result<FieldRef<'a>, PackedError> {
		let index = plan.column;
		let descriptor: &'a PackedDescriptor = self.descriptor;
		let column: &'a Column =
			descriptor.columns.get(widen(index)).ok_or(PackedError::NoColumn {
				column: index,
			})?;
		let target = &plan.target;
		for (depth, node) in plan.enclosing.iter().enumerate() {
			let state = self.state(node.presence, node.column)?;
			if state == State::Present {
				continue;
			}
			// The column lies under an absent or NULL node, so every node between that one and
			// the column has its bits clear, the column's own bits are clear and its slot is
			// unused, as the whole-row decode requires.
			for inner in plan.enclosing.get(depth + 1..).unwrap_or(&[]) {
				self.require_clear(inner.presence, inner.column)?;
			}
			self.require_clear(target.presence, index)?;
			self.require_empty(column, index, target.ops.escape.is_some())?;
			return if state == State::Null || target.role == ColumnRole::Remainder {
				Ok(FieldRef::Absent)
			} else {
				self.lookup(node.parent_remainder, column.path.get(depth..).unwrap_or(&[]))
			};
		}
		let state = self.state(target.presence, index)?;
		if state != State::Present {
			// An absent or NULL column's slot is unused, as the whole-row decode requires.
			self.require_empty(column, index, target.ops.escape.is_some())?;
		}
		match state {
			State::Present => {}
			State::Null => return Ok(FieldRef::Null),
			State::Absent => {
				return if target.role == ColumnRole::Remainder {
					Ok(FieldRef::Absent)
				} else {
					let last = column.path.len().saturating_sub(1);
					self.lookup(plan.parent_remainder, column.path.get(last..).unwrap_or(&[]))
				};
			}
		}
		match target.at {
			At::Node => Ok(FieldRef::Object),
			At::Bit {
				byte,
				mask,
			} => {
				let set = self.bit(byte, mask)?;
				(target.ops.read)(column, index, Slot::Bit(set))
			}
			At::Fixed {
				offset,
				width,
			} => {
				let slot = self.fixed(offset, width)?;
				match target.ops.escape {
					Some(escape) if (escape.at_escape)(slot) => {
						let payload = self.escaped(index)?;
						(target.ops.read)(
							column,
							index,
							Slot::Escaped {
								slot,
								payload,
							},
						)
					}
					_ => (target.ops.read)(column, index, Slot::Fixed(slot)),
				}
			}
			At::Var {
				index: var,
			} => {
				let slice = self.var_slice(var)?;
				if target.role == ColumnRole::Remainder {
					checked_remainder(slice, index, column.path.is_empty()).map(FieldRef::Stored)
				} else {
					(target.ops.read)(column, index, Slot::Var(slice))
				}
			}
		}
	}

	/// The value at `keys` in the remainder `remainder` holds, when the node it completes has one.
	/// Absent for a closed node, an empty remainder, a miss, and a step that is not an object.
	#[cfg(test)]
	fn lookup(
		&self,
		remainder: Option<RemainderAt>,
		keys: &[String],
	) -> Result<FieldRef<'a>, PackedError> {
		let Some(remainder) = remainder else {
			return Ok(FieldRef::Absent);
		};
		if !self.presence_bit(remainder.bit)? {
			// A clear bit leaves the remainder's slot empty, as the whole-row decode requires.
			if !self.var_slice(remainder.var)?.is_empty() {
				return Err(PackedError::Unused {
					column: remainder.column,
				});
			}
			return Ok(FieldRef::Absent);
		}
		let bad = |error: stored::Error| PackedError::Stored {
			column: remainder.column,
			error,
		};
		// A lookup searches the keys by halves, so it answers only over a remainder whose keys
		// ascend, which the check proves along with the rest of what the whole-row decode refuses
		// of a remainder.
		let root =
			checked_remainder(self.var_slice(remainder.var)?, remainder.column, remainder.root)?;
		let Some(object) = root.as_object().map_err(bad)? else {
			return Err(PackedError::Slot {
				column: remainder.column,
				reason: "a remainder is not an object",
			});
		};
		let mut keys = keys.iter();
		let Some(first) = keys.next() else {
			return Ok(FieldRef::Absent);
		};
		let Some(mut found) = object.ordered().map_err(bad)?.get(first).map_err(bad)? else {
			return Ok(FieldRef::Absent);
		};
		for key in keys {
			let Some(inner) = found.as_ordered_object().map_err(bad)? else {
				return Ok(FieldRef::Absent);
			};
			let Some(next) = inner.get(key).map_err(bad)? else {
				return Ok(FieldRef::Absent);
			};
			found = next;
		}
		match found.tag() {
			stored::tag::NULL => Ok(FieldRef::Null),
			stored::tag::NONE => Err(PackedError::Slot {
				column: remainder.column,
				reason: "NONE in a remainder",
			}),
			_ => Ok(FieldRef::Stored(found)),
		}
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

	/// The payload of the trailer entry of `column`.
	///
	/// The whole-row decode consumes the whole trailer, so the read of any escaped slot first
	/// checks all of it ([`check_trailer`](Self::check_trailer)): its framing, its strictly
	/// ascending entries, and that each names a slot that holds an escape value. A trailer is a
	/// few entries, and a row without ESCAPES has none to check.
	#[cfg(test)]
	fn escaped(&self, column: u32) -> Result<&'a [u8], PackedError> {
		self.check_trailer()?;
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

	/// Decodes the object node at `path`, whose columns follow the cursor. `present` says whether
	/// the node is present: under an absent or NULL node every column and remainder must be empty.
	/// `id`, for the root, is merged in at its place in key order.
	fn node(
		&self,
		cursor: &mut Cursor,
		path: &[String],
		present: bool,
		mut id: Option<Value>,
	) -> Result<Object, PackedError> {
		let columns = &self.descriptor.columns;
		let mut remainder: Option<(u32, Object)> = None;
		if let Some(column) = columns.get(cursor.column)
			&& column.role == ColumnRole::Remainder
			&& column.path.as_slice() == path
		{
			let index = id_of(cursor.column);
			remainder = self.decode_remainder(column, index, path, present)?;
			cursor.column += 1;
		}
		let mut entries: Vec<(Strand, Value)> = Vec::new();
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
				entries.push((ID_KEY, id));
			}
			let state = if present {
				self.state(column.presence, index)?
			} else {
				self.require_clear(column.presence, index)?;
				State::Absent
			};
			if column.kind == PhysicalKind::Object {
				cursor.column += 1;
				let child = self.node(cursor, &column.path, state == State::Present, None)?;
				match state {
					State::Present => entries.push((Strand::new(name), Value::Object(child))),
					State::Null => entries.push((Strand::new(name), Value::Null)),
					State::Absent => {}
				}
			} else {
				match state {
					State::Present => {
						let value = self.decode_slot(column, index, cursor)?;
						entries.push((Strand::new(name), value));
					}
					State::Null | State::Absent => {
						self.require_unused(column, index)?;
						if state == State::Null {
							entries.push((Strand::new(name), Value::Null));
						}
					}
				}
				cursor.column += 1;
			}
		}
		if let Some(id) = id.take() {
			entries.push((ID_KEY, id));
		}
		if let Some((column, remainder)) = remainder {
			entries = merge(entries, remainder, column)?;
		}
		Ok(Object(VecMap::from_sorted_vec_unchecked(entries)))
	}

	/// The remainder column `column`, at `index`, of the node at `path`: its object when present.
	/// The slice must be empty when the remainder is, and the remainder must be clear under an
	/// absent node.
	fn decode_remainder(
		&self,
		column: &Column,
		index: u32,
		path: &[String],
		present: bool,
	) -> Result<Option<(u32, Object)>, PackedError> {
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
		let value =
			stored::decode_nested(slice, path.len()).map_err(|error| PackedError::Stored {
				column: index,
				error,
			})?;
		let Value::Object(object) = value else {
			return Err(PackedError::Slot {
				column: index,
				reason: "a remainder that is not an object",
			});
		};
		if object.is_empty() {
			return Err(PackedError::Slot {
				column: index,
				reason: "a remainder with no keys",
			});
		}
		if path.is_empty() && object.contains_key("id") {
			return Err(PackedError::RemainderId);
		}
		if object.values().any(|value| matches!(value, Value::None)) {
			return Err(PackedError::Slot {
				column: index,
				reason: "NONE in a remainder",
			});
		}
		Ok(Some((index, object)))
	}

	/// Decodes the present slot of leaf column `column`, at `index`, taking its trailer entry when
	/// the slot holds an escape value.
	fn decode_slot(
		&self,
		column: &Column,
		index: u32,
		cursor: &mut Cursor,
	) -> Result<Value, PackedError> {
		let ops = kind::ops(column.kind);
		let depth = column.path.len();
		match column.location {
			Location::Bit(bit) => {
				let set = self.bool_bit(bit)?;
				(ops.decode)(column, index, Slot::Bit(set), depth)
			}
			Location::Fixed(offset) => {
				let width = fixed_width(column, index)?;
				let slot = self.fixed(widen(offset), width)?;
				match ops.escape {
					Some(escape) if (escape.at_escape)(slot) => {
						let payload = self.take_escape(cursor, index)?;
						(ops.decode)(
							column,
							index,
							Slot::Escaped {
								slot,
								payload,
							},
							depth,
						)
					}
					_ => (ops.decode)(column, index, Slot::Fixed(slot), depth),
				}
			}
			Location::Var(var) => {
				let slice = self.var_slice(widen(var))?;
				(ops.decode)(column, index, Slot::Var(slice), depth)
			}
			Location::None => Err(PackedError::Slot {
				column: index,
				reason: "an object node has no slot",
			}),
		}
	}

	/// [`require_unused`](Self::require_unused), and, for a column whose kind escapes (`escapes`),
	/// that no trailer entry names it: with its slot zero, [`check_trailer`](Self::check_trailer)
	/// refuses one, as the whole-row decode, which consumes the whole trailer, does.
	#[cfg(test)]
	fn require_empty(&self, column: &Column, index: u32, escapes: bool) -> Result<(), PackedError> {
		self.require_unused(column, index)?;
		if escapes && !self.trailer.is_empty() {
			self.check_trailer()?;
		}
		Ok(())
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

/// The entries of a node's columns and its remainder in key order. A key in both is refused.
fn merge(
	entries: Vec<(Strand, Value)>,
	remainder: Object,
	column: u32,
) -> Result<Vec<(Strand, Value)>, PackedError> {
	let mut merged = Vec::with_capacity(entries.len().saturating_add(remainder.len()));
	let mut left = entries.into_iter().peekable();
	let mut right = remainder.into_iter().peekable();
	loop {
		let order = match (left.peek(), right.peek()) {
			(Some((a, _)), Some((b, _))) => Some(a.cmp(b)),
			(Some(_), None) => Some(Ordering::Less),
			(None, Some(_)) => Some(Ordering::Greater),
			(None, None) => None,
		};
		let next = match order {
			Some(Ordering::Less) => left.next(),
			Some(Ordering::Greater) => right.next(),
			Some(Ordering::Equal) => {
				return Err(PackedError::KeyInColumnAndRemainder {
					column,
				});
			}
			None => break,
		};
		merged.extend(next);
	}
	Ok(merged)
}

/// The width of `column`'s fixed slot.
fn fixed_width(column: &Column, index: u32) -> Result<usize, PackedError> {
	column.kind.fixed_width().map(widen).ok_or(PackedError::Slot {
		column: index,
		reason: "a fixed slot of a kind with no width",
	})
}

/// A present remainder's slice as the OBJECT a whole decode accepts: an object with at least one
/// key, whose keys strictly ascend, that holds no `NONE`, and that holds no `"id"` when it is the
/// root's (`root`). Reads every key and tag once; the values are validated by the reader that walks
/// them.
#[cfg(test)]
fn checked_remainder(slice: &[u8], column: u32, root: bool) -> Result<ValueRef<'_>, PackedError> {
	let bad = |error: stored::Error| PackedError::Stored {
		column,
		error,
	};
	let value = ValueRef::new(slice).map_err(bad)?;
	let Some(object) = value.as_object().map_err(bad)? else {
		return Err(PackedError::Slot {
			column,
			reason: "a remainder is not an object",
		});
	};
	if object.is_empty() {
		return Err(PackedError::Slot {
			column,
			reason: "a remainder with no keys",
		});
	}
	object.ordered().map_err(bad)?;
	for at in 0..object.len() {
		let (key, entry) = object.entry(at).map_err(bad)?;
		if entry.tag() == stored::tag::NONE {
			return Err(PackedError::Slot {
				column,
				reason: "NONE in a remainder",
			});
		}
		if root && key == "id" {
			return Err(PackedError::RemainderId);
		}
	}
	Ok(value)
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

/// The address of a descriptor, which identifies its allocation while something holds it.
#[cfg(test)]
fn address_of(descriptor: &PackedDescriptor) -> usize {
	std::ptr::from_ref(descriptor).addr()
}

/// How one column of a descriptor's rows is read, resolved once per descriptor and column: the
/// column's kind table row, its place in the row and its presence bits, its parent's remainder,
/// and every object node that encloses it, outermost first. It serves every row of that
/// descriptor and no other.
///
/// The plan holds the descriptor it was built for, which keeps that allocation, and so its
/// address, from being reused by another descriptor while the plan exists.
/// [`PackedRow::field_with`] compares that address with its own descriptor's, so the identity
/// cannot be forged by address reuse. An equal descriptor decoded separately is another
/// allocation and is refused: a reader rebuilds its plans when its layouts change.
#[derive(Clone, Debug)]
#[cfg(test)]
pub struct FieldPlan {
	/// The descriptor the plan was built for.
	descriptor: Arc<LayoutDescriptor>,
	/// The address of `descriptor`'s packed layout: compared with a row's descriptor, never
	/// dereferenced.
	address: usize,
	plan: ColumnPlan,
}

#[cfg(test)]
impl FieldPlan {
	/// The plan of column `column` of `descriptor`. Refuses a descriptor that lays rows out as
	/// documents (`NotPackedDescriptor`) and a column past the count (`NoColumn`).
	pub fn new(descriptor: &Arc<LayoutDescriptor>, column: usize) -> Result<Self, PackedError> {
		let RowLayout::Packed(packed) = &descriptor.layout else {
			return Err(PackedError::NotPackedDescriptor);
		};
		let plan = ColumnPlan::new(packed, column)?;
		Ok(Self {
			address: address_of(packed),
			descriptor: Arc::clone(descriptor),
			plan,
		})
	}

	/// The index of the column the plan reads.
	pub fn column(&self) -> usize {
		widen(self.plan.column)
	}

	/// The descriptor whose rows the plan reads.
	pub fn descriptor(&self) -> &Arc<LayoutDescriptor> {
		&self.descriptor
	}
}

/// A column's read, resolved against one descriptor, without that descriptor's identity: what a
/// [`FieldPlan`] holds, and what [`PackedRow::field`] builds from the row's own descriptor.
#[derive(Clone, Debug)]
#[cfg(test)]
pub(crate) struct ColumnPlan {
	column: u32,
	target: Target,
	/// The remainder of the column's parent node, when that node is open. `None` for a remainder.
	parent_remainder: Option<RemainderAt>,
	/// The enclosing object nodes, outermost first: empty, and unallocated, for a column at the
	/// root.
	enclosing: Box<[Enclosing]>,
}

#[cfg(test)]
impl ColumnPlan {
	/// Resolves column `column` of `packed`.
	pub(crate) fn new(packed: &PackedDescriptor, column: usize) -> Result<Self, PackedError> {
		let index = id_of(column);
		let c = packed.columns.get(column).ok_or(PackedError::NoColumn {
			column: index,
		})?;
		let at = match c.location {
			Location::Bit(bit) => At::Bit {
				byte: widen(packed.presence_bits)
					.div_ceil(8)
					.checked_add(widen(bit / 8))
					.ok_or(PackedError::Short)?,
				mask: 1u8 << (bit % 8),
			},
			Location::Fixed(offset) => At::Fixed {
				offset: widen(offset),
				width: fixed_width(c, index)?,
			},
			Location::Var(var) => At::Var {
				index: widen(var),
			},
			Location::None => At::Node,
		};
		// The nodes that enclose a field are the proper prefixes of its path; those that enclose a
		// remainder are the prefixes up to and including its own node.
		let (depth, parent_remainder) = match c.role {
			ColumnRole::Field => {
				let depth = c.path.len().saturating_sub(1);
				let parent = c.path.get(..depth).unwrap_or(&[]);
				(depth, remainder_at(packed, parent)?)
			}
			ColumnRole::Remainder => (c.path.len(), None),
		};
		let mut enclosing = Vec::with_capacity(depth);
		for length in 1..=depth {
			let missing = PackedError::NoColumn {
				column: index,
			};
			let node_path = c.path.get(..length).ok_or(missing)?;
			let node_index = packed.column_at(node_path, ColumnRole::Field).ok_or(missing)?;
			let node = packed.columns.get(node_index).ok_or(missing)?;
			let above = c.path.get(..length - 1).ok_or(missing)?;
			enclosing.push(Enclosing {
				column: id_of(node_index),
				presence: node.presence,
				parent_remainder: remainder_at(packed, above)?,
			});
		}
		Ok(Self {
			column: index,
			target: Target {
				ops: kind::ops(c.kind),
				at,
				presence: c.presence,
				role: c.role,
			},
			parent_remainder,
			enclosing: enclosing.into_boxed_slice(),
		})
	}
}

/// The remainder of the open node at `path`, when the descriptor has one.
#[cfg(test)]
fn remainder_at(
	packed: &PackedDescriptor,
	path: &[String],
) -> Result<Option<RemainderAt>, PackedError> {
	let Some(at) = packed.column_at(path, ColumnRole::Remainder) else {
		return Ok(None);
	};
	let column = id_of(at);
	match packed.columns.get(at) {
		Some(Column {
			presence: Presence::Optional {
				bit,
			},
			location: Location::Var(var),
			..
		}) => Ok(Some(RemainderAt {
			column,
			bit: *bit,
			var: widen(*var),
			root: path.is_empty(),
		})),
		_ => Err(PackedError::Slot {
			column,
			reason: "a remainder of the wrong shape",
		}),
	}
}

/// The column's own read: its kind table row, where its value sits, and its presence.
#[derive(Clone, Copy, Debug)]
#[cfg(test)]
struct Target {
	ops: KindOps,
	at: At,
	presence: Presence,
	role: ColumnRole,
}

/// Where a column's value sits in the body.
#[derive(Clone, Copy, Debug)]
#[cfg(test)]
enum At {
	/// The bool bit: bit `mask` of byte `byte` of the body.
	Bit {
		byte: usize,
		mask: u8,
	},
	/// The fixed slot `body[offset..offset + width]`.
	Fixed {
		offset: usize,
		width: usize,
	},
	/// Variable slice `index`.
	Var {
		index: usize,
	},
	/// An object node: no slot.
	Node,
}

/// An open node's remainder: its column, its presence bit (a remainder's presence is `Optional`),
/// its variable index, and whether it is the root's, which holds no `"id"`.
#[derive(Clone, Copy, Debug)]
#[cfg(test)]
struct RemainderAt {
	column: u32,
	bit: u32,
	var: usize,
	root: bool,
}

/// An object node that encloses a column: the node's column, its presence, and the remainder of
/// its parent when that parent is open.
#[derive(Clone, Copy, Debug)]
#[cfg(test)]
struct Enclosing {
	column: u32,
	presence: Presence,
	parent_remainder: Option<RemainderAt>,
}

/// One field of a packed row, read in place.
#[derive(Clone, Copy, Debug)]
#[cfg(test)]
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
