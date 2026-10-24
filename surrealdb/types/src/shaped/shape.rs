//! Shapes: the SHAPE message, its validation, and the row layout a shape implies.

use super::{
	MAX_COLUMNS, MAX_DEPTH, MAX_FIXED, MAY_BE_ABSENT, MAY_BE_NULL, NO_SLOT, ShapedError,
	presence_bytes,
};

/// What each row of a shape is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Root {
	/// An object whose entries are the shape's columns (and its remainder, when open).
	Object,
	/// One value, held as a dynamic slice.
	Value,
}

impl Root {
	fn code(self) -> u8 {
		match self {
			Root::Object => 0,
			Root::Value => 1,
		}
	}

	fn of(code: u8) -> Result<Self, ShapedError> {
		match code {
			0 => Ok(Root::Object),
			1 => Ok(Root::Value),
			_ => Err(ShapedError::new("an unknown root")),
		}
	}
}

/// A column's kind: how its value is held in a row.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Kind {
	/// `bool`: one byte, 0 or 1.
	Bool,
	/// `int`: an `i64`.
	Int,
	/// `float`: binary64 bits.
	Float,
	/// `datetime`: `i64` seconds and `u32` nanoseconds.
	Datetime,
	/// `duration`: `u64` seconds and `u32` nanoseconds.
	Duration,
	/// `uuid`: 16 bytes.
	Uuid,
	/// `string`: UTF-8 in a var slot.
	String,
	/// `bytes`: a var slot.
	Bytes,
	/// `decimal`: its text in a var slot.
	Decimal,
	/// `record`: a key kind byte, the table when the column has none, the key.
	Record,
	/// `dynamic`: one CBOR item in a var slot.
	Dynamic,
	/// `object`: no slot; its child columns follow it.
	Object,
	/// `bit`: a bool held as bit `slot` of the fixed region.
	Bit,
	/// `array`: an array whose elements are all of the column's element kind, in a var slot.
	Array,
	/// `i8`: a signed integer of 1 byte.
	I8,
	/// `i16`: a signed integer of 2 bytes.
	I16,
	/// `i32`: a signed integer of 4 bytes.
	I32,
	/// `u8`: an unsigned integer of 1 byte.
	U8,
	/// `u16`: an unsigned integer of 2 bytes.
	U16,
	/// `u32`: an unsigned integer of 4 bytes.
	U32,
	/// `u64`: an unsigned integer of 8 bytes.
	U64,
	/// `f16`: binary16 bits.
	F16,
	/// `f32`: binary32 bits.
	F32,
	/// `datetime_ns`: `i64` nanoseconds since the epoch; `i64::MIN` and `i64::MAX` say the value is
	/// in the column's overflow slot.
	DatetimeNs,
	/// `duration_ns`: `u64` nanoseconds; `u64::MAX` says the value is in the column's overflow
	/// slot.
	DurationNs,
}

impl Kind {
	/// The kind's wire code.
	pub const fn code(self) -> u8 {
		match self {
			Kind::Bool => 0x01,
			Kind::Int => 0x02,
			Kind::Float => 0x03,
			Kind::Datetime => 0x04,
			Kind::Duration => 0x05,
			Kind::Uuid => 0x06,
			Kind::String => 0x10,
			Kind::Bytes => 0x11,
			Kind::Decimal => 0x12,
			Kind::Record => 0x13,
			Kind::Dynamic => 0x14,
			Kind::Object => 0x20,
			Kind::Bit => 0x07,
			Kind::Array => 0x16,
			Kind::I8 => 0x08,
			Kind::I16 => 0x09,
			Kind::I32 => 0x0a,
			Kind::U8 => 0x0b,
			Kind::U16 => 0x0c,
			Kind::U32 => 0x0d,
			Kind::U64 => 0x0e,
			Kind::F16 => 0x17,
			Kind::F32 => 0x0f,
			Kind::DatetimeNs => 0x18,
			Kind::DurationNs => 0x19,
		}
	}

	/// The kind a wire code names.
	pub fn of(code: u8) -> Result<Self, ShapedError> {
		Ok(match code {
			0x01 => Kind::Bool,
			0x02 => Kind::Int,
			0x03 => Kind::Float,
			0x04 => Kind::Datetime,
			0x05 => Kind::Duration,
			0x06 => Kind::Uuid,
			0x10 => Kind::String,
			0x11 => Kind::Bytes,
			0x12 => Kind::Decimal,
			0x13 => Kind::Record,
			0x14 => Kind::Dynamic,
			0x20 => Kind::Object,
			0x07 => Kind::Bit,
			0x16 => Kind::Array,
			0x08 => Kind::I8,
			0x09 => Kind::I16,
			0x0a => Kind::I32,
			0x0b => Kind::U8,
			0x0c => Kind::U16,
			0x0d => Kind::U32,
			0x0e => Kind::U64,
			0x17 => Kind::F16,
			0x0f => Kind::F32,
			0x18 => Kind::DatetimeNs,
			0x19 => Kind::DurationNs,
			_ => return Err(ShapedError::new("an unknown column kind")),
		})
	}

	/// The width of a fixed kind's slot in bytes; `None` for a variable kind, for `Object` and for
	/// `Bit`, which takes one bit.
	pub const fn fixed_width(self) -> Option<u32> {
		match self {
			Kind::Bool | Kind::I8 | Kind::U8 => Some(1),
			Kind::I16 | Kind::U16 | Kind::F16 => Some(2),
			Kind::I32 | Kind::U32 | Kind::F32 => Some(4),
			Kind::U64 | Kind::DatetimeNs | Kind::DurationNs => Some(8),
			Kind::Int | Kind::Float => Some(8),
			Kind::Datetime | Kind::Duration => Some(12),
			Kind::Uuid => Some(16),
			Kind::Bit
			| Kind::String
			| Kind::Bytes
			| Kind::Decimal
			| Kind::Record
			| Kind::Dynamic
			| Kind::Array
			| Kind::Object => None,
		}
	}

	/// The kind a value of this kind is written as by a [`ValueShaper`](super::ValueShaper): a
	/// compact timestamp or bit widened to its value kind, and every number kind preserved at its
	/// own width.
	pub const fn value_kind(self) -> Kind {
		match self {
			Kind::Bit => Kind::Bool,
			Kind::DatetimeNs => Kind::Datetime,
			Kind::DurationNs => Kind::Duration,
			other @ (Kind::Bool
			| Kind::Int
			| Kind::Float
			| Kind::I8
			| Kind::I16
			| Kind::I32
			| Kind::U8
			| Kind::U16
			| Kind::U32
			| Kind::U64
			| Kind::F16
			| Kind::F32
			| Kind::Datetime
			| Kind::Duration
			| Kind::Uuid
			| Kind::String
			| Kind::Bytes
			| Kind::Decimal
			| Kind::Record
			| Kind::Dynamic
			| Kind::Object
			| Kind::Array) => other,
		}
	}

	/// Whether the kind may be the element kind of an `array` column.
	pub const fn is_element(self) -> bool {
		matches!(
			self,
			Kind::String
				| Kind::Bytes
				| Kind::Decimal
				| Kind::Int | Kind::Float
				| Kind::I8 | Kind::I16
				| Kind::I32 | Kind::U8
				| Kind::U16 | Kind::U32
				| Kind::U64 | Kind::F16
				| Kind::F32 | Kind::Bool
				| Kind::Uuid | Kind::Datetime
				| Kind::Duration
		)
	}

	/// Whether a value of the kind may be held in the column's overflow var slot.
	pub const fn has_overflow(self) -> bool {
		matches!(self, Kind::DatetimeNs | Kind::DurationNs)
	}

	/// Whether the kind is held in a var slot.
	pub const fn is_var(self) -> bool {
		matches!(
			self,
			Kind::String | Kind::Bytes | Kind::Decimal | Kind::Record | Kind::Dynamic | Kind::Array
		)
	}
}

/// One column of a shape, as the SHAPE message lists it.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Column {
	/// The key, non-empty UTF-8.
	pub name: String,
	/// How the value is held.
	pub kind: Kind,
	/// For `Array`, the kind of every element; `None` for every other kind.
	pub element: Option<Kind>,
	/// [`MAY_BE_ABSENT`] and [`MAY_BE_NULL`].
	pub flags: u8,
	/// The first bit of the column's presence in the presence region, or [`NO_SLOT`] for a
	/// required column ([`Column::presence_width`] bits).
	pub presence: u32,
	/// The fixed offset of a fixed kind, the var slot of a variable kind, [`NO_SLOT`] for
	/// `Object`.
	pub slot: u32,
	/// For `Record`, the table every value names, or empty when each value carries its table.
	pub table: String,
	/// For `Object`, how many child columns follow it.
	pub children: u32,
	/// For `Object`, the var slot of the node's remainder, or [`NO_SLOT`] for a closed node; for a
	/// kind with an overflow slot, that var slot; [`NO_SLOT`] otherwise.
	pub rest: u32,
}

/// The presence bits of a column of `flags`: none for a required column, one for a column that may
/// be absent or `NULL` but not both (set: a value), two for one that may be both (`0b00` absent,
/// `0b01` a value, `0b10` `NULL`, `0b11` `NONE`).
pub const fn presence_width(flags: u8) -> u32 {
	match flags & (MAY_BE_ABSENT | MAY_BE_NULL) {
		0 => 0,
		f if f == MAY_BE_ABSENT | MAY_BE_NULL => 2,
		_ => 1,
	}
}

impl Column {
	/// How many presence bits the column takes.
	pub const fn presence_width(&self) -> u32 {
		presence_width(self.flags)
	}
}

/// A shape as the SHAPE message lists it: its root, its row layout and its columns depth-first.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ShapeDef {
	/// What each row is.
	pub root: Root,
	/// The number of presence bits in a row.
	pub presence_bits: u32,
	/// The bytes of a row's fixed region.
	pub fixed_size: u32,
	/// The number of var slots in a row.
	pub var_count: u32,
	/// The root's remainder slot ([`NO_SLOT`] for a closed root), or a VALUE root's value slot.
	pub root_rest: u32,
	/// The columns, depth-first.
	pub columns: Vec<Column>,
}

/// A column before its layout is assigned, with its children nested.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnDef {
	/// The key.
	pub name: String,
	/// How the value is held.
	pub kind: Kind,
	/// For `Array`, the kind of every element.
	pub element: Option<Kind>,
	/// [`MAY_BE_ABSENT`] and [`MAY_BE_NULL`].
	pub flags: u8,
	/// For `Record`, the table every value names, or empty.
	pub table: String,
	/// For `Object`, whether the node has a remainder.
	pub open: bool,
	/// For `Object`, its child columns, in key order.
	pub children: Vec<ColumnDef>,
}

impl ShapeDef {
	/// The shape of rows that are each one value.
	pub fn value() -> Self {
		Self {
			root: Root::Value,
			presence_bits: 0,
			fixed_size: 0,
			var_count: 1,
			root_rest: 0,
			columns: Vec::new(),
		}
	}

	/// The OBJECT shape of `columns` (each object's children in key order), its root open when
	/// `open`, laid out the way [`RowWriter`](super::RowWriter)s write rows: one walk of the nodes
	/// depth-first assigns a node's remainder slot before its children, and each column's
	/// presence state and var slot as it is reached; fixed slots are assigned in the same order,
	/// every non-`Bool` slot before every `Bool` slot.
	pub fn object(open: bool, columns: &[ColumnDef]) -> Self {
		let mut def = Self {
			root: Root::Object,
			presence_bits: 0,
			fixed_size: 0,
			var_count: 0,
			root_rest: NO_SLOT,
			columns: Vec::new(),
		};
		if open {
			def.root_rest = def.next_var();
		}
		def.push_columns(columns);
		// Non-bool fixed slots first, so runs of them can mirror a stored layout; bools after.
		for bools in [false, true] {
			for column in &mut def.columns {
				if let Some(width) = column.kind.fixed_width()
					&& (column.kind == Kind::Bool) == bools
				{
					column.slot = def.fixed_size;
					def.fixed_size += width;
				}
			}
		}
		def
	}

	fn next_var(&mut self) -> u32 {
		let slot = self.var_count;
		self.var_count += 1;
		slot
	}

	fn push_columns(&mut self, columns: &[ColumnDef]) {
		for def in columns {
			let presence = match presence_width(def.flags) {
				0 => NO_SLOT,
				width => {
					let at = self.presence_bits;
					self.presence_bits += width;
					at
				}
			};
			let at = self.columns.len();
			self.columns.push(Column {
				name: def.name.clone(),
				kind: def.kind,
				element: if def.kind == Kind::Array {
					def.element
				} else {
					None
				},
				flags: def.flags & (MAY_BE_ABSENT | MAY_BE_NULL),
				presence,
				slot: NO_SLOT,
				table: if def.kind == Kind::Record {
					def.table.clone()
				} else {
					String::new()
				},
				children: 0,
				rest: NO_SLOT,
			});
			if def.kind.is_var() {
				self.columns[at].slot = self.next_var();
			}
			if def.kind.has_overflow() {
				self.columns[at].rest = self.next_var();
			}
			if def.kind == Kind::Object {
				if def.open {
					self.columns[at].rest = self.next_var();
				}
				let before = self.columns.len();
				self.push_columns(&def.children);
				self.columns[at].children = (self.columns.len() - before) as u32;
			}
		}
	}

	/// The columns depth-first as nested definitions, with the root's openness, each in its
	/// [`Kind::value_kind`].
	pub fn tree(&self) -> (bool, Vec<ColumnDef>) {
		fn take(columns: &[Column], at: &mut usize, end: usize) -> Vec<ColumnDef> {
			let mut out = Vec::new();
			while *at < end {
				let column = &columns[*at];
				*at += 1;
				let stop = (*at + column.children as usize).min(end);
				let children = if column.kind == Kind::Object {
					take(columns, at, stop)
				} else {
					Vec::new()
				};
				out.push(ColumnDef {
					name: column.name.clone(),
					kind: column.kind.value_kind(),
					element: column.element,
					flags: column.flags,
					table: column.table.clone(),
					open: column.kind == Kind::Object && column.rest != NO_SLOT,
					children,
				});
			}
			out
		}
		let mut at = 0;
		(self.root_rest != NO_SLOT, take(&self.columns, &mut at, self.columns.len()))
	}
}

/// A validated shape, with the row layout it implies and its SHAPE payload.
#[derive(Debug)]
pub struct Shape {
	def: ShapeDef,
	/// For column `i`, the index past its subtree.
	ends: Vec<u32>,
	/// The bytes before a row's var region: presence, fixed region and ends.
	header: usize,
	/// The SHAPE payload past the shape id.
	payload: Vec<u8>,
	/// The deepest nesting of object columns.
	depth: usize,
}

impl PartialEq for Shape {
	fn eq(&self, other: &Self) -> bool {
		self.def == other.def
	}
}

impl Eq for Shape {}

impl Shape {
	/// Validates `def` against the wire rules and the limits.
	pub fn new(def: ShapeDef) -> Result<Self, ShapedError> {
		let bad = ShapedError::new;
		if def.columns.len() > MAX_COLUMNS {
			return Err(bad("too many columns"));
		}
		if def.fixed_size > MAX_FIXED {
			return Err(bad("a fixed region too large"));
		}
		if def.presence_bits as usize > 2 * MAX_COLUMNS {
			return Err(bad("too many presence states"));
		}
		if def.var_count as usize > 2 * MAX_COLUMNS + 1 {
			return Err(bad("too many var slots"));
		}
		match def.root {
			Root::Value => {
				if !def.columns.is_empty() || def.root_rest >= def.var_count {
					return Err(bad("a value root with columns or no value slot"));
				}
			}
			Root::Object => {
				if def.root_rest != NO_SLOT && def.root_rest >= def.var_count {
					return Err(bad("a remainder slot out of range"));
				}
			}
		}
		let mut ends = vec![0u32; def.columns.len()];
		let depth = check_node(&def, &mut ends, 0, def.columns.len(), 0)?;
		let header = presence_bytes(def.presence_bits)
			+ def.fixed_size as usize
			+ 4 * def.var_count as usize;
		let mut payload = Vec::new();
		encode(&def, &mut payload);
		Ok(Self {
			def,
			ends,
			header,
			payload,
			depth,
		})
	}

	/// Reads a shape from a SHAPE payload past its shape id, which it must fill exactly.
	pub fn decode(bytes: &[u8]) -> Result<Self, ShapedError> {
		let mut r = Reader(bytes);
		let root = Root::of(r.u8()?)?;
		let presence_bits = r.u32()?;
		let fixed_size = r.u32()?;
		let var_count = r.u32()?;
		let root_rest = r.u32()?;
		let count = r.u32()? as usize;
		if count > MAX_COLUMNS {
			return Err(ShapedError::new("too many columns"));
		}
		let mut columns = Vec::with_capacity(count);
		for _ in 0..count {
			let name_len = r.u16()? as usize;
			let name = std::str::from_utf8(r.take(name_len)?)
				.map_err(|_| ShapedError::new("a column name that is not UTF-8"))?
				.to_owned();
			let kind = Kind::of(r.u8()?)?;
			let element = match r.u8()? {
				0 => None,
				code => Some(Kind::of(code)?),
			};
			let flags = r.u8()?;
			let presence = r.u32()?;
			let slot = r.u32()?;
			let table_len = r.u16()? as usize;
			let table = std::str::from_utf8(r.take(table_len)?)
				.map_err(|_| ShapedError::new("a table name that is not UTF-8"))?
				.to_owned();
			let children = r.u32()?;
			let rest = r.u32()?;
			columns.push(Column {
				name,
				kind,
				element,
				flags,
				presence,
				slot,
				table,
				children,
				rest,
			});
		}
		if !r.0.is_empty() {
			return Err(ShapedError::new("bytes past a shape's columns"));
		}
		Self::new(ShapeDef {
			root,
			presence_bits,
			fixed_size,
			var_count,
			root_rest,
			columns,
		})
	}

	/// The shape as its message lists it.
	pub fn def(&self) -> &ShapeDef {
		&self.def
	}

	/// What each row is.
	pub fn root(&self) -> Root {
		self.def.root
	}

	/// The columns, depth-first.
	pub fn columns(&self) -> &[Column] {
		&self.def.columns
	}

	/// The deepest nesting of object columns: 0 for a shape with none, 1 for one whose object
	/// columns hold no object column.
	pub fn depth(&self) -> usize {
		self.depth
	}

	/// The SHAPE payload past the shape id.
	pub fn payload(&self) -> &[u8] {
		&self.payload
	}

	/// The bytes of a row before its var region.
	pub fn header_len(&self) -> usize {
		self.header
	}

	/// The bytes of a row's presence region.
	pub fn presence_len(&self) -> usize {
		presence_bytes(self.def.presence_bits)
	}

	/// Where a row's ends table starts.
	pub fn ends_at(&self) -> usize {
		self.presence_len() + self.def.fixed_size as usize
	}

	/// The child columns of `node` (`None` for the root), in order.
	pub fn children(&self, node: Option<usize>) -> Children<'_> {
		let (at, end) = match node {
			None => (0, self.def.columns.len()),
			Some(i) => (i + 1, self.ends[i] as usize),
		};
		Children {
			ends: &self.ends,
			at,
			end,
		}
	}

	/// The child of `node` named `name`.
	pub fn child(&self, node: Option<usize>, name: &str) -> Option<usize> {
		self.children(node).find(|&i| self.def.columns[i].name == name)
	}

	/// The remainder slot of `node`.
	pub fn rest(&self, node: Option<usize>) -> u32 {
		match node {
			None => self.def.root_rest,
			Some(i) => self.def.columns[i].rest,
		}
	}
}

/// The child columns of a node, in order.
pub struct Children<'s> {
	ends: &'s [u32],
	at: usize,
	end: usize,
}

impl Iterator for Children<'_> {
	type Item = usize;

	fn next(&mut self) -> Option<usize> {
		if self.at >= self.end {
			return None;
		}
		let at = self.at;
		self.at = self.ends[at] as usize;
		Some(at)
	}
}

/// Checks the columns `start..end`, the children of one node at depth `depth`, filling each
/// column's subtree end. Returns the deepest nesting of object columns below the node.
fn check_node(
	def: &ShapeDef,
	ends: &mut [u32],
	start: usize,
	end: usize,
	depth: usize,
) -> Result<usize, ShapedError> {
	let bad = ShapedError::new;
	let mut deepest = 0;
	if depth >= MAX_DEPTH {
		return Err(bad("objects nested too deep"));
	}
	let mut at = start;
	let mut previous: Option<&str> = None;
	while at < end {
		let column = &def.columns[at];
		if column.name.is_empty() {
			return Err(bad("an empty column name"));
		}
		// The SHAPE message holds a name's and a table's lengths as `u16`.
		if column.name.len() > usize::from(u16::MAX) || column.table.len() > usize::from(u16::MAX) {
			return Err(bad("a column name or table longer than 65535 bytes"));
		}
		if previous.is_some_and(|p| p >= column.name.as_str()) {
			return Err(bad("column names out of order"));
		}
		previous = Some(&column.name);
		if column.flags & !(MAY_BE_ABSENT | MAY_BE_NULL) != 0 {
			return Err(bad("unknown column flags"));
		}
		match (column.presence_width(), column.presence) {
			(0, NO_SLOT) => {}
			(w, p) if w > 0 && p.checked_add(w).is_some_and(|e| e <= def.presence_bits) => {}
			_ => return Err(bad("a presence index out of range")),
		}
		if column.kind != Kind::Record && !column.table.is_empty() {
			return Err(bad("a table on a column that is not a record"));
		}
		match (column.kind, column.element) {
			(Kind::Array, Some(element)) if element.is_element() => {}
			(Kind::Array, _) => return Err(bad("an array of no element kind")),
			(_, None) => {}
			(_, Some(_)) => return Err(bad("an element kind on a column that is not an array")),
		}
		if column.kind != Kind::Object && column.children != 0 {
			return Err(bad("children on a column that is not an object"));
		}
		if column.kind.has_overflow() {
			if column.rest >= def.var_count {
				return Err(bad("an overflow slot out of range"));
			}
		} else if column.kind != Kind::Object && column.rest != NO_SLOT {
			return Err(bad("a remainder on a column that is not an object"));
		}
		if column.kind == Kind::Bit {
			if u64::from(column.slot) >= u64::from(def.fixed_size) * 8 {
				return Err(bad("a bit slot out of range"));
			}
		} else if let Some(width) = column.kind.fixed_width() {
			if column.slot.checked_add(width).is_none_or(|e| e > def.fixed_size) {
				return Err(bad("a fixed slot out of range"));
			}
		} else if column.kind.is_var() {
			if column.slot >= def.var_count {
				return Err(bad("a var slot out of range"));
			}
		} else if column.slot != NO_SLOT {
			return Err(bad("a slot on an object column"));
		}
		let stop = at
			.checked_add(1)
			.and_then(|a| a.checked_add(column.children as usize))
			.filter(|s| *s <= end)
			.ok_or_else(|| bad("children past their parent"))?;
		if column.kind == Kind::Object {
			if column.rest != NO_SLOT && column.rest >= def.var_count {
				return Err(bad("a remainder slot out of range"));
			}
			deepest = deepest.max(1 + check_node(def, ends, at + 1, stop, depth + 1)?);
		}
		ends[at] = stop as u32;
		at = stop;
	}
	Ok(deepest)
}

fn encode(def: &ShapeDef, out: &mut Vec<u8>) {
	out.push(def.root.code());
	for n in [def.presence_bits, def.fixed_size, def.var_count, def.root_rest] {
		out.extend_from_slice(&n.to_le_bytes());
	}
	out.extend_from_slice(&(def.columns.len() as u32).to_le_bytes());
	for column in &def.columns {
		out.extend_from_slice(&(column.name.len() as u16).to_le_bytes());
		out.extend_from_slice(column.name.as_bytes());
		out.push(column.kind.code());
		out.push(column.element.map_or(0, Kind::code));
		out.push(column.flags);
		out.extend_from_slice(&column.presence.to_le_bytes());
		out.extend_from_slice(&column.slot.to_le_bytes());
		out.extend_from_slice(&(column.table.len() as u16).to_le_bytes());
		out.extend_from_slice(column.table.as_bytes());
		out.extend_from_slice(&column.children.to_le_bytes());
		out.extend_from_slice(&column.rest.to_le_bytes());
	}
}

/// A bounds-checked little-endian reader.
pub(super) struct Reader<'b>(pub(super) &'b [u8]);

impl<'b> Reader<'b> {
	pub(super) fn take(&mut self, n: usize) -> Result<&'b [u8], ShapedError> {
		if n > self.0.len() {
			return Err(ShapedError::new("truncated"));
		}
		let (head, rest) = self.0.split_at(n);
		self.0 = rest;
		Ok(head)
	}

	pub(super) fn u8(&mut self) -> Result<u8, ShapedError> {
		Ok(self.take(1)?[0])
	}

	pub(super) fn u16(&mut self) -> Result<u16, ShapedError> {
		let b = self.take(2)?;
		Ok(u16::from_le_bytes([b[0], b[1]]))
	}

	pub(super) fn u32(&mut self) -> Result<u32, ShapedError> {
		let b = self.take(4)?;
		Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
	}
}
