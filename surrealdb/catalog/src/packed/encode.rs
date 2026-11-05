//! Writing a record's data as a packed row.
//!
//! [`pack`] visits the columns of a descriptor once, front to back, merging each object node's
//! sorted keys with the node's direct child columns (which ascend in the same order), and writes
//! straight into one buffer. The byte-level layout, the presence and trailer rules, and what
//! happens to a value that does not fit are in the [module documentation](super).

use super::PackedError;
use super::kind::{self, Dest, ESCAPE_WIDTH, Written};
use super::row::{PackedRow, widen};
use crate::Generation;
use crate::layout::{Column, ColumnRole, Location, PackedDescriptor, PhysicalKind, Presence};
use crate::record::{RowFlags, StoredRecord};
use crate::val::stored::{self, Encoder, varint};
use crate::val::{Object, Strand, Value};

/// The width the end offsets are written at before they are narrowed to the narrowest that holds
/// the variable region's length.
const RESERVED_END: usize = 4;

/// The bytes of capacity a row's buffer has beyond its fixed region and end-offset table.
const SLACK: usize = 64;

/// What [`pack`] made of a record.
#[derive(Debug, PartialEq, Eq)]
pub enum Packing {
	/// The packed row, which has passed [`check_packed`].
	Packed(Vec<u8>),
	/// The descriptor cannot hold the record: a value does not fit its column, a key has no column
	/// in a closed node, or the variable region is longer than `u32::MAX` bytes. The caller
	/// writes the record in the document layout.
	Misfit,
}

/// Packs `data`, the object of a record whose `"id"` is its key, as a row of `descriptor` stamped
/// with `generation` and the header flags `flags`.
///
/// The row leaves out `"id"`: a reader restores it from the record's key, so the key `"id"` of the
/// root object is never judged. The descriptor must have passed [`PackedDescriptor::validate`];
/// one that did not makes `pack` fail or misfit, and never write outside the row's regions.
///
/// Every grammar value is encoded at the depth its reader decodes it at, so `pack` never writes a
/// row that a reader refuses as too deep. A caller that stores the row also owes the depth check
/// of the record as a whole.
///
/// Only a value that cannot be stored at all (a closure, or one nested past the grammar's depth
/// limit) and a row that fails its own [`check_packed`] are errors. Every other record the
/// descriptor cannot hold is [`Packing::Misfit`].
///
/// The thread's reusable encoder is the scratch space, or a fresh one when it is already in use.
pub fn pack(
	descriptor: &PackedDescriptor,
	generation: Generation,
	flags: RowFlags,
	data: &Object,
) -> Result<Packing, stored::Error> {
	stored::with_encoder(|enc| match run(descriptor, generation, flags, data, enc) {
		Ok(row) => Ok(Packing::Packed(row)),
		Err(Stop::Misfit) => Ok(Packing::Misfit),
		Err(Stop::Fault(error)) => Err(error),
	})
}

/// Checks that `bytes` read back as a packed row of `descriptor` with exactly `header` and
/// `generation`.
///
/// Checks the envelope, the row's shape against the descriptor (the checks of
/// [`PackedRow::new`]), that its end offsets ascend to the end of its variable region, and that
/// its trailer is framed by the escape widths of the columns its entries name, in ascending column
/// order, each entry naming a column whose slot holds an escape value. It reads no value. The cost
/// is one pass over the end offsets and the trailer.
pub fn check_packed(
	bytes: &[u8],
	header: u8,
	generation: Generation,
	descriptor: &PackedDescriptor,
) -> Result<(), PackedError> {
	let [marker, found, rest @ ..] = bytes else {
		return Err(PackedError::Envelope);
	};
	if *marker != StoredRecord::MARKER || *found != header || (header & StoredRecord::RESERVED) != 0
	{
		return Err(PackedError::Envelope);
	}
	let (named, used) = varint::read_u64(rest).map_err(|_| PackedError::Envelope)?;
	if named != generation.stamp() {
		return Err(PackedError::Envelope);
	}
	let body = rest.get(used..).ok_or(PackedError::Envelope)?;
	let record = StoredRecord::from_parts(header, generation.stamp(), body);
	let row = PackedRow::new(&record, descriptor)?;
	row.check_ends()?;
	row.check_trailer()
}

/// Why a walk over the columns stopped.
enum Stop {
	/// The descriptor cannot hold the record.
	Misfit,
	/// The record cannot be stored at all, or the writer broke its own rules.
	Fault(stored::Error),
}

impl From<stored::Error> for Stop {
	fn from(error: stored::Error) -> Self {
		Self::Fault(error)
	}
}

type Walk<T> = Result<T, Stop>;

/// A fault of the writer's own: a descriptor or a buffer that does not agree with what is being
/// written.
#[cold]
fn fault(what: &'static str) -> Stop {
	Stop::Fault(stored::Error::Unstorable(what))
}

/// What a row holds of an object node.
#[derive(Clone, Copy)]
enum Node<'v> {
	/// The node is present, and this is its object.
	Present(&'v Object),
	/// The node is absent or `NULL`, or lies under one that is: everything under it is written
	/// empty.
	Empty,
}

/// What the value at a key is to the presence bits of its column.
#[derive(Clone, Copy)]
enum Held<'v> {
	/// The key is not in the object.
	Absent,
	/// The key holds `NULL`.
	Null,
	/// The key holds this value, which is neither `NONE` nor `NULL`.
	Present(&'v Value),
}

/// A state a column's presence bits can be in.
#[derive(Clone, Copy)]
enum State {
	Absent,
	Null,
	Present,
}

/// Classifies the value at a key. A key that holds `NONE` is a misfit for every column.
fn classify(value: Option<&Value>) -> Walk<Held<'_>> {
	match value {
		None => Ok(Held::Absent),
		Some(Value::None) => Err(Stop::Misfit),
		Some(Value::Null) => Ok(Held::Null),
		Some(value) => Ok(Held::Present(value)),
	}
}

fn run(
	descriptor: &PackedDescriptor,
	generation: Generation,
	flags: RowFlags,
	data: &Object,
	enc: &mut Encoder,
) -> Walk<Vec<u8>> {
	let fixed = widen(descriptor.fixed_size);
	let count = widen(descriptor.var_count);
	let table = count
		.checked_mul(RESERVED_END)
		.ok_or_else(|| fault("an end-offset table larger than the address space"))?;
	let presence_bytes = widen(descriptor.presence_bits).div_ceil(8);
	let bool_bytes = widen(descriptor.bool_bits).div_ceil(8);
	if presence_bytes.checked_add(bool_bytes).is_none_or(|bytes| bytes > fixed) {
		return Err(fault("a fixed region smaller than its bit regions"));
	}
	let stamp = generation.stamp();
	// The marker, the header and the generation.
	let body = varint::len_u64(stamp)
		.checked_add(2)
		.ok_or_else(|| fault("an envelope larger than the address space"))?;
	let ends = body
		.checked_add(fixed)
		.ok_or_else(|| fault("a fixed region larger than the address space"))?;
	let var = ends
		.checked_add(table)
		.ok_or_else(|| fault("an end-offset table larger than the address space"))?;
	let mut out = Vec::with_capacity(var.saturating_add(SLACK));
	out.push(StoredRecord::MARKER);
	out.push(0);
	let mut buf = [0u8; varint::MAX_LEN];
	out.extend_from_slice(varint::write_u64(stamp, &mut buf));
	if out.len() != body {
		return Err(fault("an envelope of an unexpected length"));
	}
	out.resize(var, 0);
	let mut packer = Packer {
		descriptor,
		enc,
		out,
		body,
		presence_bytes,
		bool_bytes,
		ends,
		count,
		var,
		escapes: Vec::new(),
	};
	let visited = packer.walk(0, &[], Node::Present(data))?;
	if visited != descriptor.columns.len() {
		return Err(fault("a column that no object node reaches"));
	}
	packer.finish(generation, flags)
}

/// A row being written.
struct Packer<'d, 'e> {
	descriptor: &'d PackedDescriptor,
	/// The scratch encoder grammar values are encoded in.
	enc: &'e mut Encoder,
	/// The row: the envelope, the fixed region, the end-offset table at its reserved width, and
	/// the variable region so far.
	out: Vec<u8>,
	/// Where the body, and so the fixed region, starts in `out`.
	body: usize,
	/// The length of the presence region, which the bool region follows.
	presence_bytes: usize,
	/// The length of the bool region.
	bool_bytes: usize,
	/// Where the end-offset table starts in `out`: the end of the fixed region.
	ends: usize,
	/// The number of variable fields.
	count: usize,
	/// Where the variable region starts in `out`, while the end offsets are at their reserved
	/// width.
	var: usize,
	/// The trailer entries so far: a column index, ascending, and the entry's payload.
	escapes: Vec<(u32, [u8; ESCAPE_WIDTH])>,
}

impl Packer<'_, '_> {
	/// Writes the columns of the object node at `path` against the node's content, starting at
	/// column `i`. Returns the index of the first column after the node's.
	fn walk<'v>(&mut self, mut i: usize, path: &[String], node: Node<'v>) -> Walk<usize> {
		let descriptor = self.descriptor;
		let columns = descriptor.columns.as_slice();
		// An open node's remainder comes first among its columns.
		let remainder = columns.get(i).filter(|column| {
			column.role == ColumnRole::Remainder && column.path.as_slice() == path
		});
		let open = remainder.is_some();
		if let Some(column) = remainder {
			if let Node::Present(object) = node {
				self.remainder(i, column, path, object)?;
			}
			self.end_of(column)?;
			i += 1;
		}
		let entries: &'v [(Strand, Value)] = match node {
			Node::Present(object) => object.0.as_slice(),
			Node::Empty => &[],
		};
		let mut next = 0usize;
		while let Some(column) = columns.get(i)
			&& column.path.len() > path.len()
			&& column.path.starts_with(path)
		{
			if column.role != ColumnRole::Field || column.path.len() != path.len().saturating_add(1)
			{
				return Err(fault("a column that is not a direct child of its node"));
			}
			let name = column
				.path
				.get(path.len())
				.ok_or_else(|| fault("a column without a name"))?
				.as_str();
			let mut value: Option<&'v Value> = None;
			// A key before the column's name has no column: an open node holds it in its remainder,
			// and the root's own id is left out.
			while let Some((key, _)) = entries.get(next)
				&& key.as_str() < name
			{
				if !(open || (path.is_empty() && key.as_str() == "id")) {
					return Err(Stop::Misfit);
				}
				next += 1;
			}
			if let Some((key, found)) = entries.get(next)
				&& key.as_str() == name
			{
				value = Some(found);
				next += 1;
			}
			if column.kind == PhysicalKind::Object {
				let child = match node {
					Node::Empty => Node::Empty,
					Node::Present(_) => self.object_node(column, value)?,
				};
				i = self.walk(i.saturating_add(1), &column.path, child)?;
			} else {
				self.write_column(i, column, value, matches!(node, Node::Empty))?;
				i += 1;
			}
		}
		// A key after every column has no column either.
		if !open {
			for (key, _) in entries.iter().skip(next) {
				if !(path.is_empty() && key.as_str() == "id") {
					return Err(Stop::Misfit);
				}
			}
		}
		Ok(i)
	}

	/// Writes the remainder, column `at`, of the open node at `path`: the keys of `object` that no
	/// direct child field of the node has, as one OBJECT, with its presence bit set. A node with no
	/// such key writes nothing and leaves the bit clear.
	fn remainder(
		&mut self,
		at: usize,
		column: &Column,
		path: &[String],
		object: &Object,
	) -> Walk<()> {
		let Presence::Optional {
			bit,
		} = column.presence
		else {
			return Err(fault("a remainder whose presence is not optional"));
		};
		let descriptor = self.descriptor;
		let columns = descriptor.columns.as_slice();
		// The names of the node's direct child fields, ascending. A child node's own remainder has
		// the child's path, and would repeat its name.
		let child_len = path.len().saturating_add(1);
		let mut names = columns
			.iter()
			.skip(at.saturating_add(1))
			.take_while(|child| child.path.len() > path.len() && child.path.starts_with(path))
			.filter(|child| child.role == ColumnRole::Field && child.path.len() == child_len)
			.filter_map(|child| child.path.last().map(String::as_str))
			.peekable();
		let mut rest: Vec<(&str, &Value)> = Vec::new();
		for (key, value) in object.0.iter() {
			let key = key.as_str();
			if path.is_empty() && key == "id" {
				continue;
			}
			while names.peek().is_some_and(|name| *name < key) {
				names.next();
			}
			if names.peek().is_some_and(|name| *name == key) {
				continue;
			}
			// A remainder holds no NONE: a read of its key says so.
			if matches!(value, Value::None) {
				return Err(Stop::Misfit);
			}
			rest.push((key, value));
		}
		if rest.is_empty() {
			return Ok(());
		}
		self.set_presence_bit(bit)?;
		self.enc.clear();
		self.enc.object_entries_nested(rest.iter().rev().copied(), rest.len(), path.len())?;
		self.out.extend_from_slice(self.enc.as_bytes());
		Ok(())
	}

	/// Writes the presence bits of the object node `column`, whose key holds `value`, and answers
	/// what the node holds.
	fn object_node<'v>(&mut self, column: &Column, value: Option<&'v Value>) -> Walk<Node<'v>> {
		match classify(value)? {
			Held::Absent => {
				self.require(column.presence, State::Absent)?;
				Ok(Node::Empty)
			}
			Held::Null => {
				self.require(column.presence, State::Null)?;
				Ok(Node::Empty)
			}
			Held::Present(Value::Object(object)) => {
				self.require(column.presence, State::Present)?;
				Ok(Node::Present(object))
			}
			Held::Present(_) => Err(Stop::Misfit),
		}
	}

	/// Writes column `index`, a leaf, whose key holds `value`: its presence bits, then its slot. A
	/// column under an absent or `NULL` object node is written empty.
	fn write_column(
		&mut self,
		index: usize,
		column: &Column,
		value: Option<&Value>,
		under_absent: bool,
	) -> Walk<()> {
		if !under_absent {
			match classify(value)? {
				Held::Absent => self.require(column.presence, State::Absent)?,
				Held::Null => {
					if !self.state_in(column.presence, State::Null)? {
						// A `Value` column whose presence bits cannot say NULL holds the NULL
						// value.
						if column.kind != PhysicalKind::Value {
							return Err(Stop::Misfit);
						}
						self.require(column.presence, State::Present)?;
						self.write_slot(index, column, &Value::Null)?;
					}
				}
				Held::Present(value) => {
					self.require(column.presence, State::Present)?;
					self.write_slot(index, column, value)?;
				}
			}
		}
		self.end_of(column)
	}

	/// Writes the value of leaf column `index` into its slot, through the kind table.
	fn write_slot(&mut self, index: usize, column: &Column, value: &Value) -> Walk<()> {
		let ops = kind::ops(column.kind);
		match column.location {
			Location::Bit(bit) => {
				let mut set = false;
				match (ops.write)(column, value, Dest::Bit(&mut set))? {
					Written::Fits => {}
					Written::Misfit => return Err(Stop::Misfit),
					Written::Escaped(_) => return Err(fault("a bit that escapes")),
				}
				if set {
					self.set_bit(self.presence_bytes, self.bool_bytes, bit)?;
				}
			}
			Location::Fixed(offset) => {
				let width = column
					.kind
					.fixed_width()
					.map(widen)
					.ok_or_else(|| fault("a fixed slot of a kind with no width"))?;
				let start = self
					.body
					.checked_add(widen(offset))
					.ok_or_else(|| fault("a fixed slot past the address space"))?;
				let end = start
					.checked_add(width)
					.ok_or_else(|| fault("a fixed slot past the address space"))?;
				if end > self.ends {
					return Err(fault("a fixed slot past the fixed region"));
				}
				let slot = self
					.out
					.get_mut(start..end)
					.ok_or_else(|| fault("a fixed slot outside the row"))?;
				match (ops.write)(column, value, Dest::Fixed(slot))? {
					Written::Fits => {}
					Written::Misfit => return Err(Stop::Misfit),
					Written::Escaped(payload) => {
						let named = u32::try_from(index)
							.map_err(|_| fault("a column index past u32::MAX"))?;
						self.escapes.push((named, payload));
					}
				}
			}
			Location::Var(_) => {
				let dest = Dest::Var {
					out: &mut self.out,
					enc: &mut *self.enc,
					depth: column.path.len(),
				};
				match (ops.write)(column, value, dest)? {
					Written::Fits => {}
					Written::Misfit => return Err(Stop::Misfit),
					Written::Escaped(_) => return Err(fault("a variable value that escapes")),
				}
			}
			Location::None => return Err(fault("a value written into a column with no slot")),
		}
		Ok(())
	}

	/// Records where a variable column's slice ends: the variable region's length so far. Every
	/// variable column is ended once, in variable-index order, whatever it holds. Any other column
	/// has nothing to end.
	fn end_of(&mut self, column: &Column) -> Walk<()> {
		if let Location::Var(var) = column.location {
			self.set_end(var)
		} else {
			Ok(())
		}
	}

	fn set_end(&mut self, var: u32) -> Walk<()> {
		let index = widen(var);
		if index >= self.count {
			return Err(fault("a variable index past the variable count"));
		}
		let length = self
			.out
			.len()
			.checked_sub(self.var)
			.ok_or_else(|| fault("a variable region that starts past the row's end"))?;
		// A variable region past `u32::MAX` bytes does not pack.
		let end = u32::try_from(length).map_err(|_| Stop::Misfit)?;
		let at = at_entry(self.ends, index, RESERVED_END)?;
		let to = at
			.checked_add(RESERVED_END)
			.ok_or_else(|| fault("an end offset past the address space"))?;
		self.out
			.get_mut(at..to)
			.ok_or_else(|| fault("an end offset outside the row"))?
			.copy_from_slice(&end.to_le_bytes());
		Ok(())
	}

	/// Records `state` in the presence bits of `presence`. Answers `false`, changing nothing, when
	/// the presence has no state for it.
	fn state_in(&mut self, presence: Presence, state: State) -> Walk<bool> {
		match state {
			State::Present => match presence {
				Presence::Required => Ok(true),
				Presence::Optional {
					bit,
				}
				| Presence::Nullable {
					bit,
				}
				| Presence::OptionalNullable {
					bit,
				} => {
					self.set_presence_bit(bit)?;
					Ok(true)
				}
			},
			State::Absent => Ok(matches!(
				presence,
				Presence::Optional { .. } | Presence::OptionalNullable { .. }
			)),
			State::Null => match presence {
				Presence::Required
				| Presence::Optional {
					..
				} => Ok(false),
				Presence::Nullable {
					..
				} => Ok(true),
				Presence::OptionalNullable {
					bit,
				} => {
					let next =
						bit.checked_add(1).ok_or_else(|| fault("a presence bit past u32::MAX"))?;
					self.set_presence_bit(next)?;
					Ok(true)
				}
			},
		}
	}

	/// [`state_in`](Self::state_in), where a presence with no state for it is a misfit.
	fn require(&mut self, presence: Presence, state: State) -> Walk<()> {
		if self.state_in(presence, state)? {
			Ok(())
		} else {
			Err(Stop::Misfit)
		}
	}

	fn set_presence_bit(&mut self, bit: u32) -> Walk<()> {
		self.set_bit(0, self.presence_bytes, bit)
	}

	/// Sets bit `bit` of the bit region that starts `region` bytes into the body and is `bytes`
	/// long.
	fn set_bit(&mut self, region: usize, bytes: usize, bit: u32) -> Walk<()> {
		let byte = widen(bit / 8);
		if byte >= bytes {
			return Err(fault("a bit past its region"));
		}
		let at = self
			.body
			.checked_add(region)
			.and_then(|start| start.checked_add(byte))
			.ok_or_else(|| fault("a bit past the address space"))?;
		let target = self.out.get_mut(at).ok_or_else(|| fault("a bit outside the row"))?;
		*target |= 1u8 << (bit % 8);
		Ok(())
	}

	/// Narrows the end offsets to the narrowest width that holds the variable region, appends the
	/// trailer, patches the header, and checks the row.
	fn finish(self, generation: Generation, flags: RowFlags) -> Walk<Vec<u8>> {
		let Self {
			descriptor,
			mut out,
			ends,
			count,
			var,
			escapes,
			..
		} = self;
		let length = out
			.len()
			.checked_sub(var)
			.ok_or_else(|| fault("a variable region that starts past the row's end"))?;
		let width = kind::narrowest_width(length).ok_or(Stop::Misfit)?;
		if count == 0 && length != 0 {
			return Err(fault("variable bytes with no variable field"));
		}
		let layout = match (count, width) {
			(0, _) | (_, 1) => StoredRecord::PACKED_U8,
			(_, 2) => StoredRecord::PACKED_U16,
			_ => StoredRecord::PACKED_U32,
		};
		if count > 0 && width < RESERVED_END {
			// Each end offset moves down to its narrow place, which is never past its wide one, so
			// no entry that is yet to be read is overwritten.
			for index in 0..count {
				let from = at_entry(ends, index, RESERVED_END)?;
				let wide = out
					.get(from..)
					.and_then(|rest| rest.first_chunk::<RESERVED_END>())
					.copied()
					.ok_or_else(|| fault("an end offset outside the row"))?;
				let to = at_entry(ends, index, width)?;
				out.get_mut(to..)
					.and_then(|rest| rest.get_mut(..width))
					.ok_or_else(|| fault("an end offset outside the row"))?
					.copy_from_slice(wide.get(..width).ok_or_else(|| fault("a width past 4"))?);
			}
			let dest = at_entry(ends, count, width)?;
			if dest > var {
				return Err(fault("a narrowed end-offset table that grew"));
			}
			out.copy_within(var.., dest);
			let end = dest
				.checked_add(length)
				.ok_or_else(|| fault("a variable region past the address space"))?;
			out.truncate(end);
		}
		for (column, payload) in &escapes {
			let mut buf = [0u8; varint::MAX_LEN];
			out.extend_from_slice(varint::write_u64(u64::from(*column), &mut buf));
			let escape = descriptor
				.columns
				.get(widen(*column))
				.and_then(|named| kind::ops(named.kind).escape)
				.ok_or_else(|| fault("a trailer entry for a column that does not escape"))?;
			out.extend_from_slice(
				payload.get(..escape.width).ok_or_else(|| fault("an escape payload too short"))?,
			);
		}
		let mut header = layout | flags.bits();
		if !escapes.is_empty() {
			header |= StoredRecord::ESCAPES;
		}
		*out.get_mut(1).ok_or_else(|| fault("a row with no header"))? = header;
		check_packed(&out, header, generation, descriptor)
			.map_err(|_| fault("an encoded packed row does not read back as written"))?;
		Ok(out)
	}
}

/// Where entry `index` of a table of `width`-byte entries that starts at `base` is.
fn at_entry(base: usize, index: usize, width: usize) -> Walk<usize> {
	index
		.checked_mul(width)
		.and_then(|offset| base.checked_add(offset))
		.ok_or_else(|| fault("an end offset past the address space"))
}
