//! Visiting a packed row's record in key order, for a reader that writes it out without building
//! it.
//!
//! [`PackedRow::visit`] hands a [`PackedVisitor`] the record [`PackedRow::decode`] builds, as a
//! stream: each object opens with its entry count, then each entry's key and value in key order,
//! then closes. Leaves are [`FieldRef`]s read in place; the record's own `id` is announced where it
//! sorts in the root, for the visitor to write from the record's key.
//!
//! The walk does not prove the row whole. It reads only what it writes, through the checked slices
//! and per-slot reads every in-place read uses, so malformed bytes are refused or read as some
//! other value, never read outside the row. The rules that span what the walk does not read (a
//! column the row leaves empty holds nothing, the bits under an absent node are clear, every end
//! offset is ordered, the trailer holds no entry the walk does not take, a remainder holds no
//! `NONE` and at least one key) are the writer's to keep: a row that breaks one of them is visited
//! as the bytes the walk reads say. Where the walk sees a violation in passing (a remainder key out
//! of order, in a column that holds a value, or `id` in the root's remainder) it refuses the row.

use std::cmp::Ordering;

use super::{PackedRow, State, fixed_width, id_of, widen};
use crate::layout::{Column, ColumnRole, Location, PhysicalKind, Presence};
use crate::packed::kind::{self, Slot};
use crate::packed::{FieldRef, PackedError};
use crate::val::stored::{self, ObjectRef, ValueRef};

/// The receiver of a packed row's record, in the order a whole decode of it holds its entries.
///
/// Every object is announced by [`open`](Self::open), then for each entry [`key`](Self::key)
/// followed by exactly one of [`field`](Self::field) or a nested object, then
/// [`close`](Self::close) with the number of entries it held. The record's own `id` entry is
/// [`id`](Self::id) alone, which writes both its key and its value.
pub trait PackedVisitor<'a> {
	type Error: From<PackedError>;

	/// An object opens. The answer is handed back to the [`close`](Self::close) of this object.
	fn open(&mut self) -> Result<usize, Self::Error>;

	/// The key of the open object's `at`th entry.
	fn key(&mut self, at: usize, key: &str) -> Result<(), Self::Error>;

	/// The root's `id` entry, its open object's `at`th, whose value is the record's key.
	fn id(&mut self, at: usize) -> Result<(), Self::Error>;

	/// The value of the entry whose key came last: never [`FieldRef::Absent`] or
	/// [`FieldRef::Object`].
	fn field(&mut self, field: FieldRef<'a>) -> Result<(), Self::Error>;

	/// Whether the visitor takes a string column's value as its stored bytes, handed to
	/// [`text`](Self::text) unchecked, instead of as a [`FieldRef::String`] whose UTF-8 the read
	/// checked. A string is checked to be UTF-8 when it is written; a visitor that answers `true`
	/// copies the bytes out as they are and never forms a `str` from them.
	fn raw_text(&self) -> bool {
		false
	}

	/// The value of the entry whose key came last, a string column's stored bytes, for a visitor
	/// whose [`raw_text`](Self::raw_text) is `true`: UTF-8 as written, not checked on this read.
	fn text(&mut self, bytes: &'a [u8]) -> Result<(), Self::Error> {
		let _ = bytes;
		Err(PackedError::Slot {
			column: u32::MAX,
			reason: "a string handed unchecked to a visitor that checks its strings",
		}
		.into())
	}

	/// The object [`open`](Self::open) answered `mark` for closes, having held `len` entries.
	fn close(&mut self, mark: usize, len: usize) -> Result<(), Self::Error>;
}

/// The next entry a node's merge writes.
enum Next<'a> {
	Id,
	Column(usize, State),
	Remainder(&'a str, ValueRef<'a>),
}

impl<'a> PackedRow<'a> {
	/// Hands `visitor` the record [`decode`](Self::decode) builds with an `id`, as the module
	/// documentation describes, without proving the row whole.
	pub fn visit<V: PackedVisitor<'a>>(&self, visitor: &mut V) -> Result<(), V::Error> {
		// Where the next escape is sought in the trailer: columns are visited in descriptor order
		// and trailer entries ascend by column, so the search only moves forward.
		let mut trailer = 0usize;
		let end = self.visit_node(visitor, 0, 0, true, &mut trailer)?;
		if end != self.descriptor.columns.len() {
			return Err(PackedError::NoColumn {
				column: id_of(end),
			}
			.into());
		}
		Ok(())
	}

	/// Visits the present object node at depth `depth` (the length of its path), whose remainder,
	/// if it has one, and columns start at column `start`, seeking escapes in the trailer from
	/// `trailer` on. Answers the first column past the node.
	fn visit_node<V: PackedVisitor<'a>>(
		&self,
		visitor: &mut V,
		start: usize,
		depth: usize,
		root: bool,
		trailer: &mut usize,
	) -> Result<usize, V::Error> {
		let descriptor: &'a crate::layout::PackedDescriptor = self.descriptor;
		let columns: &'a [Column] = &descriptor.columns;
		let mut first = start;
		// The remainder's column, for its errors.
		let held = id_of(start);
		let mut remainder: Option<ObjectRef<'a>> = None;
		if let Some(column) = columns.get(start)
			&& column.role == ColumnRole::Remainder
			&& column.path.len() == depth
		{
			remainder = self.visit_remainder(column, start, depth)?;
			first = start.saturating_add(1);
		}
		let mark = visitor.open()?;
		let mut written = 0usize;
		let mut id = root;
		let mut pending_column = self.next_column(first, depth)?;
		let mut taken = 0usize;
		let mut pending_entry = next_entry(remainder, taken, held)?;
		let mut previous: Option<&'a str> = None;
		loop {
			let column_key = match pending_column {
				Ok((index, _)) => Some(segment(columns, index, depth)?),
				Err(_) => None,
			};
			let mut next = None;
			let mut least: Option<&str> = None;
			if id {
				next = Some(Next::Id);
				least = Some("id");
			}
			if let (Some(key), Ok((index, state))) = (column_key, pending_column)
				&& least.is_none_or(|least| key < least)
			{
				next = Some(Next::Column(index, state));
				least = Some(key);
			}
			if let Some((key, value)) = pending_entry {
				match least.map(|least| key.cmp(least)) {
					None | Some(Ordering::Less) => next = Some(Next::Remainder(key, value)),
					Some(Ordering::Equal) => {
						let column = match pending_column {
							Ok((index, _)) if column_key == Some(key) => id_of(index),
							_ => held,
						};
						return Err(if root && key == "id" {
							PackedError::RemainderId
						} else {
							PackedError::KeyInColumnAndRemainder {
								column,
							}
						}
						.into());
					}
					Some(Ordering::Greater) => {}
				}
			}
			match next {
				None => break,
				Some(Next::Id) => {
					visitor.id(written)?;
					id = false;
				}
				Some(Next::Column(index, state)) => {
					let name = segment(columns, index, depth)?;
					visitor.key(written, name)?;
					let after = self.visit_column(visitor, index, depth, state, trailer)?;
					pending_column = self.next_column(after, depth)?;
				}
				Some(Next::Remainder(key, value)) => {
					if previous.is_some_and(|previous| previous >= key) {
						return Err(PackedError::Stored {
							column: held,
							error: stored::Error::Malformed("object keys out of order"),
						}
						.into());
					}
					previous = Some(key);
					visitor.key(written, key)?;
					let field = match value.tag() {
						stored::tag::NULL => FieldRef::Null,
						stored::tag::NONE => {
							return Err(PackedError::Slot {
								column: held,
								reason: "NONE in a remainder",
							}
							.into());
						}
						_ => FieldRef::Stored(value),
					};
					visitor.field(field)?;
					taken = taken.saturating_add(1);
					pending_entry = next_entry(remainder, taken, held)?;
				}
			}
			written = written.saturating_add(1);
		}
		visitor.close(mark, written)?;
		// Every column of the node has been passed: `next_column` stopped at the first past it.
		match pending_column {
			Err(end) => Ok(end),
			Ok((index, _)) => Err(PackedError::NoColumn {
				column: id_of(index),
			}
			.into()),
		}
	}

	/// The first column at or after `from` that is a child of the node at `depth` and holds a value
	/// or NULL, with its state; or, when there is none, the first column past the node: the first
	/// at or after `from` that does not lie below it. A child that is absent is passed over with
	/// the columns under it.
	fn next_column(
		&self,
		mut from: usize,
		depth: usize,
	) -> Result<Result<(usize, State), usize>, PackedError> {
		let columns = &self.descriptor.columns;
		while let Some(column) = columns.get(from)
			&& column.path.len() > depth
		{
			if column.role != ColumnRole::Field || column.path.len() != depth.saturating_add(1) {
				return Err(PackedError::NoColumn {
					column: id_of(from),
				});
			}
			match self.state(column.presence, id_of(from))? {
				State::Absent => from = self.past(from),
				state => return Ok(Ok((from, state))),
			}
		}
		Ok(Err(from))
	}

	/// The first column past column `index` and the columns under it: an object node's remainder,
	/// which sorts right after the node at the node's depth, and its descendants.
	fn past(&self, index: usize) -> usize {
		let columns = &self.descriptor.columns;
		let depth = columns.get(index).map_or(0, |column| column.path.len());
		let mut at = index.saturating_add(1);
		if columns.get(at).is_some_and(|column| {
			column.role == ColumnRole::Remainder && column.path.len() == depth
		}) {
			at = at.saturating_add(1);
		}
		while columns.get(at).is_some_and(|column| column.path.len() > depth) {
			at = at.saturating_add(1);
		}
		at
	}

	/// Visits the value of column `index`, a child of the node at `depth` in state `state`, which
	/// holds a value or NULL. Answers the first column past it.
	fn visit_column<V: PackedVisitor<'a>>(
		&self,
		visitor: &mut V,
		index: usize,
		depth: usize,
		state: State,
		trailer: &mut usize,
	) -> Result<usize, V::Error> {
		let column: &'a Column =
			self.descriptor.columns.get(index).ok_or(PackedError::NoColumn {
				column: id_of(index),
			})?;
		if state == State::Null {
			visitor.field(FieldRef::Null)?;
			return Ok(self.past(index));
		}
		if column.kind == PhysicalKind::Object {
			return self.visit_node(
				visitor,
				index.saturating_add(1),
				depth.saturating_add(1),
				false,
				trailer,
			);
		}
		if column.kind == PhysicalKind::String
			&& let Location::Var(var) = column.location
			&& visitor.raw_text()
		{
			visitor.text(self.var_slice(widen(var))?)?;
			return Ok(index.saturating_add(1));
		}
		visitor.field(self.visit_slot(column, index, trailer)?)?;
		Ok(index.saturating_add(1))
	}

	/// The present slot of leaf column `column`, at `index`, read in place, an escape's payload
	/// sought in the trailer from `trailer` on.
	#[inline]
	fn visit_slot(
		&self,
		column: &'a Column,
		index: usize,
		trailer: &mut usize,
	) -> Result<FieldRef<'a>, PackedError> {
		let id = id_of(index);
		match column.location {
			Location::None => Err(PackedError::Slot {
				column: id,
				reason: "an object node has no slot",
			}),
			Location::Bit(bit) => kind::read(column, id, Slot::Bit(self.bool_bit(bit)?)),
			Location::Fixed(offset) => {
				let slot = self.fixed(widen(offset), fixed_width(column, id)?)?;
				match kind::ops(column.kind).escape {
					Some(escape) if (escape.at_escape)(slot) => {
						let payload = self.escape_from(trailer, id)?;
						kind::read(
							column,
							id,
							Slot::Escaped {
								slot,
								payload,
							},
						)
					}
					_ => kind::read(column, id, Slot::Fixed(slot)),
				}
			}
			Location::Var(var) => kind::read(column, id, Slot::Var(self.var_slice(widen(var))?)),
		}
	}

	/// The remainder column `column`, at `index`, of the node at `depth`: its object when its bit
	/// is set, `None` when it is clear.
	fn visit_remainder(
		&self,
		column: &Column,
		index: usize,
		depth: usize,
	) -> Result<Option<ObjectRef<'a>>, PackedError> {
		let id = id_of(index);
		let (
			Presence::Optional {
				bit,
			},
			Location::Var(var),
		) = (column.presence, column.location)
		else {
			return Err(PackedError::Slot {
				column: id,
				reason: "a remainder of the wrong shape",
			});
		};
		if !self.presence_bit(bit)? {
			return Ok(None);
		}
		let bad = |error| PackedError::Stored {
			column: id,
			error,
		};
		let value = ValueRef::new_nested(self.var_slice(widen(var))?, depth).map_err(bad)?;
		match value.as_object().map_err(bad)? {
			Some(object) => Ok(Some(object)),
			None => Err(PackedError::Slot {
				column: id,
				reason: "a remainder is not an object",
			}),
		}
	}
}

/// Entry `at` of `remainder`, when it has one.
fn next_entry<'a>(
	remainder: Option<ObjectRef<'a>>,
	at: usize,
	column: u32,
) -> Result<Option<(&'a str, ValueRef<'a>)>, PackedError> {
	match remainder {
		Some(object) if at < object.len() => {
			object.entry(at).map(Some).map_err(|error| PackedError::Stored {
				column,
				error,
			})
		}
		_ => Ok(None),
	}
}

/// The key column `index`, a child of the node at `depth`, holds in that node.
fn segment(columns: &[Column], index: usize, depth: usize) -> Result<&str, PackedError> {
	columns.get(index).and_then(|column| column.path.get(depth)).map(String::as_str).ok_or(
		PackedError::NoColumn {
			column: id_of(index),
		},
	)
}
