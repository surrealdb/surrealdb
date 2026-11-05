//! Encoding values as shaped rows, and choosing the shape each row is written in.

use std::fmt::Write as _;
use std::sync::Arc;

use rust_decimal::Decimal;

use super::shape::{ColumnDef, Kind, Root, Shape, ShapeDef};
use super::{
	MAY_BE_ABSENT, MAY_BE_NULL, NO_SLOT, RowWriter, STATE_ABSENT, STATE_NONE, STATE_NULL,
	STATE_VALUE, ShapedError,
};

/// The most shapes a [`ValueShaper`] derives from rows before it settles on the open shape.
pub const MAX_JOINS: u32 = 16;
/// The most columns a shape derived from rows holds before the shaper settles on the open shape.
pub const MAX_DERIVED_COLUMNS: usize = 256;
/// The deepest object a shape derived from rows gives columns of its own; deeper objects are
/// dynamic.
pub const MAX_DERIVED_DEPTH: usize = 4;

/// What a value is, as far as a shape is concerned.
#[derive(Clone, Copy, Debug)]
pub enum Leaf<'v> {
	/// `NONE`: absent.
	None,
	/// `NULL`.
	Null,
	/// A bool.
	Bool(bool),
	/// An `i8`.
	I8(i8),
	/// A `u8`.
	U8(u8),
	/// An `i16`.
	I16(i16),
	/// A `u16`.
	U16(u16),
	/// An `i32`.
	I32(i32),
	/// A `u32`.
	U32(u32),
	/// An `int` (`i64`).
	Int(i64),
	/// A `u64`.
	U64(u64),
	/// An `f16`.
	F16(half::f16),
	/// An `f32`.
	F32(f32),
	/// A `float` (`f64`).
	Float(f64),
	/// A decimal.
	Decimal(Decimal),
	/// A string.
	String(&'v str),
	/// Bytes.
	Bytes(&'v [u8]),
	/// Seconds since the epoch and nanoseconds.
	Datetime(i64, u32),
	/// Seconds and nanoseconds.
	Duration(u64, u32),
	/// A uuid.
	Uuid([u8; 16]),
	/// A record id: its table and its key.
	Record(&'v str, KeyLeaf<'v>),
	/// An object.
	Object,
	/// An array.
	Array,
	/// Any other value: held dynamic.
	Other,
}

/// A record id's key.
#[derive(Clone, Copy, Debug)]
pub enum KeyLeaf<'v> {
	/// A number id.
	Int(i64),
	/// A string id.
	String(&'v str),
	/// A uuid id.
	Uuid([u8; 16]),
	/// An array, object or range key: the record id is held whole as CBOR.
	Other,
}

/// A value a [`ValueShaper`] can encode.
pub trait ShapedValue<'v>: Copy {
	/// What the value is.
	fn leaf(self) -> Leaf<'v>;
	/// For an object, its value for `key`.
	fn get(self, key: &str) -> Option<Self>;
	/// For an object, its entries in ascending key order.
	fn entries(self) -> impl Iterator<Item = (&'v str, Self)>;
	/// For an array, its elements in order.
	fn elements(self) -> impl Iterator<Item = Self>;
	/// Writes the value as one CBOR item, as the CBOR format encodes it.
	fn write_dynamic(self, out: &mut Vec<u8>) -> Result<(), ShapedError>;
}

/// The shape kind a leaf is held in; `None` for `NONE` and `NULL`, which say nothing of it.
fn kind_of(leaf: &Leaf<'_>) -> Option<Kind> {
	Some(match leaf {
		Leaf::None | Leaf::Null => return None,
		Leaf::Bool(_) => Kind::Bool,
		Leaf::I8(_) => Kind::I8,
		Leaf::U8(_) => Kind::U8,
		Leaf::I16(_) => Kind::I16,
		Leaf::U16(_) => Kind::U16,
		Leaf::I32(_) => Kind::I32,
		Leaf::U32(_) => Kind::U32,
		Leaf::Int(_) => Kind::Int,
		Leaf::U64(_) => Kind::U64,
		Leaf::F16(_) => Kind::F16,
		Leaf::F32(_) => Kind::F32,
		Leaf::Float(_) => Kind::Float,
		Leaf::Decimal(_) => Kind::Decimal,
		Leaf::String(_) => Kind::String,
		Leaf::Bytes(_) => Kind::Bytes,
		Leaf::Datetime(..) => Kind::Datetime,
		Leaf::Duration(..) => Kind::Duration,
		Leaf::Uuid(_) => Kind::Uuid,
		Leaf::Record(..) => Kind::Record,
		Leaf::Object => Kind::Object,
		Leaf::Array => Kind::Dynamic,
		Leaf::Other => Kind::Dynamic,
	})
}

/// Chooses the shape each value-path row is written in, and writes it.
///
/// An object row is written in the current shape when it fits; the first row whose `id` names
/// the seed's table is written in the seed when it fits; otherwise a row is written in the join
/// of the current shape (or, for that first row, the seed) and the row. After [`MAX_JOINS`] joins,
/// or once a join passes [`MAX_DERIVED_COLUMNS`], object rows are written in the open shape with no
/// columns. A row that is not an object is written in the VALUE shape.
#[derive(Debug, Default)]
pub struct ValueShaper {
	current: Option<Arc<Shape>>,
	value: Option<Arc<Shape>>,
	seed: Option<(String, Arc<Shape>)>,
	joins: u32,
}

impl ValueShaper {
	/// A shaper with no shape yet.
	pub fn new() -> Self {
		Self::default()
	}

	/// A shaper whose first row whose `id` names `table` starts from `seed`.
	pub fn with_seed(table: String, seed: Arc<Shape>) -> Self {
		Self {
			seed: Some((table, seed)),
			..Self::default()
		}
	}

	/// The shape the last object row was written in.
	pub fn current(&self) -> Option<&Arc<Shape>> {
		self.current.as_ref()
	}

	/// Makes `shape` the current shape, for rows written in it by another writer.
	pub fn set_current(&mut self, shape: &Arc<Shape>) {
		if self.current.as_ref().is_some_and(|current| Arc::ptr_eq(current, shape)) {
			return;
		}
		if shape.root() == Root::Object {
			self.current = Some(Arc::clone(shape));
		}
	}

	/// Writes `value` as a row at the end of `out`, answering the shape it was written in. On an
	/// error, `out` is as it was.
	pub fn write<'v, V: ShapedValue<'v>>(
		&mut self,
		value: V,
		out: &mut Vec<u8>,
	) -> Result<Arc<Shape>, ShapedError> {
		let start = out.len();
		let result = self.write_inner(value, out);
		if result.is_err() {
			out.truncate(start);
		}
		result
	}

	fn write_inner<'v, V: ShapedValue<'v>>(
		&mut self,
		value: V,
		out: &mut Vec<u8>,
	) -> Result<Arc<Shape>, ShapedError> {
		if !matches!(value.leaf(), Leaf::Object) {
			let shape = match &self.value {
				Some(shape) => Arc::clone(shape),
				None => {
					let shape = Arc::new(Shape::new(ShapeDef::value())?);
					self.value = Some(Arc::clone(&shape));
					shape
				}
			};
			let mut row = RowWriter::begin(out, &shape);
			row.open_var(0)?;
			value.write_dynamic(row.out())?;
			row.close_var()?;
			row.finish()?;
			return Ok(shape);
		}
		let shape = match &self.current {
			Some(shape) if fits(shape, None, value) => Arc::clone(shape),
			None if self.seed.as_ref().is_some_and(|(table, seed)| {
				names_table(value, table) && fits(seed, None, value)
			}) =>
			{
				let seed = self.seed.as_ref().map(|(_, seed)| Arc::clone(seed));
				let shape = seed.ok_or_else(|| ShapedError::new("a seed shape vanished"))?;
				self.current = Some(Arc::clone(&shape));
				shape
			}
			current => {
				let base = match (current, &self.seed) {
					(Some(current), _) => Some(Arc::clone(current)),
					(None, Some((table, seed))) if names_table(value, table) => {
						Some(Arc::clone(seed))
					}
					_ => None,
				};
				let shape = if self.joins >= MAX_JOINS {
					open_shape()?
				} else {
					let joined = join(base.as_deref(), value);
					if joined.columns.len() > MAX_DERIVED_COLUMNS {
						open_shape()?
					} else {
						Arc::new(Shape::new(joined)?)
					}
				};
				self.joins = self.joins.saturating_add(1);
				self.current = Some(Arc::clone(&shape));
				shape
			}
		};
		write_row(&shape, value, out)?;
		Ok(shape)
	}
}

/// The open shape with no columns: every entry of an object row is in the root's remainder.
fn open_shape() -> Result<Arc<Shape>, ShapedError> {
	Ok(Arc::new(Shape::new(ShapeDef::object(true, &[]))?))
}

/// Whether the object `value`'s `id` is a record of `table`.
fn names_table<'v, V: ShapedValue<'v>>(value: V, table: &str) -> bool {
	matches!(value.get("id").map(|id| id.leaf()), Some(Leaf::Record(t, _)) if t == table)
}

/// Whether the object `value` can be written as node `node` of `shape`.
pub fn fits<'v, V: ShapedValue<'v>>(shape: &Shape, node: Option<usize>, value: V) -> bool {
	if shape.root() != Root::Object {
		return false;
	}
	let mut matched = 0usize;
	for at in shape.children(node) {
		let column = &shape.columns()[at];
		let leaf = value.get(&column.name).map(|v| (v, v.leaf()));
		match leaf {
			None => {
				if column.flags & MAY_BE_ABSENT == 0 {
					return false;
				}
			}
			Some((_, Leaf::None)) => {
				if column.presence_width() != 2 {
					return false;
				}
				matched += 1;
			}
			Some((_, Leaf::Null)) => {
				if column.flags & MAY_BE_NULL == 0 {
					return false;
				}
				matched += 1;
			}
			Some((child, leaf)) => {
				let ok = match column.kind {
					Kind::Dynamic => true,
					Kind::Object => matches!(leaf, Leaf::Object) && fits(shape, Some(at), child),
					Kind::Record => {
						if let Leaf::Record(table, _) = leaf {
							column.table.is_empty() || column.table == table
						} else {
							false
						}
					}
					Kind::Array => {
						matches!(leaf, Leaf::Array)
							&& column.element.is_some_and(|element| elements_fit(element, child))
					}
					kind @ (Kind::Bool
					| Kind::Int
					| Kind::Float
					| Kind::Datetime
					| Kind::Duration
					| Kind::Uuid
					| Kind::String
					| Kind::Bytes
					| Kind::Decimal
					| Kind::Bit
					| Kind::I8
					| Kind::I16
					| Kind::I32
					| Kind::U8
					| Kind::U16
					| Kind::U32
					| Kind::U64
					| Kind::F16
					| Kind::F32
					| Kind::DatetimeNs
					| Kind::DurationNs) => leaf_fits(kind, &leaf),
				};
				if !ok {
					return false;
				}
				matched += 1;
			}
		}
	}
	// A closed node holds no key without a column.
	shape.rest(node) != NO_SLOT || value.entries().count() == matched
}

/// The shape every row `base` fits, and `value` too.
fn join<'v, V: ShapedValue<'v>>(base: Option<&Shape>, value: V) -> ShapeDef {
	let (mut open, mut columns) = match base {
		Some(base) => base.def().tree(),
		None => (false, Vec::new()),
	};
	join_node(&mut columns, &mut open, value, 0);
	ShapeDef::object(open, &columns)
}

fn join_node<'v, V: ShapedValue<'v>>(
	columns: &mut Vec<ColumnDef>,
	open: &mut bool,
	value: V,
	depth: usize,
) {
	for column in columns.iter_mut() {
		match value.get(&column.name).map(|v| (v, v.leaf())) {
			None => column.flags |= MAY_BE_ABSENT,
			Some((_, Leaf::None)) => column.flags |= MAY_BE_ABSENT | MAY_BE_NULL,
			Some((_, Leaf::Null)) => column.flags |= MAY_BE_NULL,
			Some((child, leaf)) => match (column.kind, leaf) {
				(Kind::Dynamic, _) => {}
				(Kind::Object, Leaf::Object) => {
					join_node(&mut column.children, &mut column.open, child, depth + 1)
				}
				(Kind::Record, Leaf::Record(table, _)) => {
					if column.table != table {
						column.table.clear();
					}
				}
				(Kind::Array, Leaf::Array)
					if column.element.is_some_and(|element| elements_fit(element, child)) => {}
				(kind, leaf) if kind_of(&leaf) == Some(kind) => {}
				_ => {
					column.kind = Kind::Dynamic;
					column.element = None;
					column.table.clear();
					column.open = false;
					column.children.clear();
				}
			},
		}
	}
	if *open {
		return;
	}
	for (key, child) in value.entries() {
		// A key longer than a SHAPE message holds is no column: the node opens, and the key is
		// held in its remainder.
		if key.len() > usize::from(u16::MAX) {
			*open = true;
			continue;
		}
		let leaf = child.leaf();
		if let Err(at) = columns.binary_search_by(|c| c.name.as_str().cmp(key)) {
			columns.insert(at, derive_column(key, child, leaf, depth));
		}
	}
}

/// A column for the entry `key` of a row that has none, which may be absent or `NULL`.
fn derive_column<'v, V: ShapedValue<'v>>(
	key: &str,
	value: V,
	leaf: Leaf<'v>,
	depth: usize,
) -> ColumnDef {
	let mut column = ColumnDef {
		name: key.to_owned(),
		kind: kind_of(&leaf).unwrap_or(Kind::Dynamic),
		element: None,
		flags: MAY_BE_ABSENT | MAY_BE_NULL,
		table: String::new(),
		open: false,
		children: Vec::new(),
	};
	match leaf {
		// A table longer than a SHAPE message holds is carried with each value.
		Leaf::Record(table, _) if table.len() <= usize::from(u16::MAX) => {
			table.clone_into(&mut column.table)
		}
		Leaf::Object if depth + 1 < MAX_DERIVED_DEPTH => {
			join_node(&mut column.children, &mut column.open, value, depth + 1);
		}
		Leaf::Object => column.kind = Kind::Dynamic,
		Leaf::None
		| Leaf::Null
		| Leaf::Bool(_)
		| Leaf::I8(_)
		| Leaf::U8(_)
		| Leaf::I16(_)
		| Leaf::U16(_)
		| Leaf::I32(_)
		| Leaf::U32(_)
		| Leaf::Int(_)
		| Leaf::U64(_)
		| Leaf::F16(_)
		| Leaf::F32(_)
		| Leaf::Float(_)
		| Leaf::Decimal(_)
		| Leaf::String(_)
		| Leaf::Bytes(_)
		| Leaf::Datetime(..)
		| Leaf::Duration(..)
		| Leaf::Uuid(_)
		| Leaf::Record(..)
		| Leaf::Array
		| Leaf::Other => {}
	}
	column
}

/// Writes the object `value`, which fits `shape`, as a row at the end of `out`.
fn write_row<'v, V: ShapedValue<'v>>(
	shape: &Shape,
	value: V,
	out: &mut Vec<u8>,
) -> Result<(), ShapedError> {
	let mut row = RowWriter::begin(out, shape);
	write_node(shape, &mut row, None, value)?;
	row.finish()
}

fn write_node<'v, V: ShapedValue<'v>>(
	shape: &Shape,
	row: &mut RowWriter<'_>,
	node: Option<usize>,
	value: V,
) -> Result<(), ShapedError> {
	let rest = shape.rest(node);
	if rest != NO_SLOT {
		row.open_var(rest)?;
		let extra = |key: &str| shape.child(node, key).is_none();
		let count = value.entries().filter(|(k, _)| extra(k)).count();
		if count > 0 {
			let out = row.out();
			cbor_head(out, 5, count as u64);
			for (key, v) in value.entries().filter(|(k, _)| extra(k)) {
				cbor_head(out, 3, key.len() as u64);
				out.extend_from_slice(key.as_bytes());
				v.write_dynamic(out)?;
			}
		}
		row.close_var()?;
	}
	// A root `id` whose var slot is the shape's last (a copy plan's layout) is written after the
	// other columns, so the row's var slots are opened in order.
	let last_var = shape.def().var_count.checked_sub(1);
	let trailing = |at: usize| {
		let column = &shape.columns()[at];
		node.is_none()
			&& column.kind.is_var()
			&& last_var == Some(column.slot)
			&& column.name == "id"
	};
	let mut deferred = None;
	for at in shape.children(node) {
		if trailing(at) {
			deferred = Some(at);
			continue;
		}
		write_column(shape, row, value, at)?;
	}
	if let Some(at) = deferred {
		write_column(shape, row, value, at)?;
	}
	Ok(())
}

fn write_column<'v, V: ShapedValue<'v>>(
	shape: &Shape,
	row: &mut RowWriter<'_>,
	value: V,
	at: usize,
) -> Result<(), ShapedError> {
	let column = &shape.columns()[at];
	let Some(child) = value.get(&column.name) else {
		row.set_presence(column, STATE_ABSENT);
		return Ok(());
	};
	match child.leaf() {
		Leaf::None => row.set_presence(column, STATE_NONE),
		Leaf::Null => row.set_presence(column, STATE_NULL),
		leaf @ (Leaf::Bool(_)
		| Leaf::I8(_)
		| Leaf::U8(_)
		| Leaf::I16(_)
		| Leaf::U16(_)
		| Leaf::I32(_)
		| Leaf::U32(_)
		| Leaf::Int(_)
		| Leaf::U64(_)
		| Leaf::F16(_)
		| Leaf::F32(_)
		| Leaf::Float(_)
		| Leaf::Decimal(_)
		| Leaf::String(_)
		| Leaf::Bytes(_)
		| Leaf::Datetime(..)
		| Leaf::Duration(..)
		| Leaf::Uuid(_)
		| Leaf::Record(..)
		| Leaf::Object
		| Leaf::Array
		| Leaf::Other) => {
			row.set_presence(column, STATE_VALUE);
			write_leaf(shape, row, at, child, leaf)?;
		}
	}
	Ok(())
}

fn write_leaf<'v, V: ShapedValue<'v>>(
	shape: &Shape,
	row: &mut RowWriter<'_>,
	at: usize,
	value: V,
	leaf: Leaf<'v>,
) -> Result<(), ShapedError> {
	let column = &shape.columns()[at];
	let slot = column.slot;
	let misfit = || ShapedError::new("a value that does not fit its column");
	match (column.kind, leaf) {
		(Kind::Bool, Leaf::Bool(v)) => row.fixed(slot, &[u8::from(v)]),
		(Kind::Bit, Leaf::Bool(v)) => row.fixed_bit(slot, v),
		(Kind::I8, Leaf::I8(v)) => row.fixed(slot, &v.to_le_bytes()),
		(Kind::I16, Leaf::I16(v)) => row.fixed(slot, &v.to_le_bytes()),
		(Kind::I32, Leaf::I32(v)) => row.fixed(slot, &v.to_le_bytes()),
		(Kind::U8, Leaf::U8(v)) => row.fixed(slot, &v.to_le_bytes()),
		(Kind::U16, Leaf::U16(v)) => row.fixed(slot, &v.to_le_bytes()),
		(Kind::U32, Leaf::U32(v)) => row.fixed(slot, &v.to_le_bytes()),
		(Kind::U64, Leaf::U64(v)) => row.fixed(slot, &v.to_le_bytes()),
		(Kind::F16, Leaf::F16(v)) => row.fixed(slot, &v.to_bits().to_le_bytes()),
		(Kind::F32, Leaf::F32(v)) => row.fixed(slot, &v.to_bits().to_le_bytes()),
		(Kind::DatetimeNs, Leaf::Datetime(secs, nanos)) => match datetime_ns(secs, nanos) {
			Some(ns) => row.fixed(slot, &ns.to_le_bytes()),
			None => {
				let escape = if secs < 0 {
					i64::MIN
				} else {
					i64::MAX
				};
				row.fixed(slot, &escape.to_le_bytes());
				var(row, column.rest, |out| {
					out.extend_from_slice(&secs.to_le_bytes());
					out.extend_from_slice(&nanos.to_le_bytes());
					Ok(())
				})?;
			}
		},
		(Kind::DurationNs, Leaf::Duration(secs, nanos)) => match duration_ns(secs, nanos) {
			Some(ns) => row.fixed(slot, &ns.to_le_bytes()),
			None => {
				row.fixed(slot, &u64::MAX.to_le_bytes());
				var(row, column.rest, |out| {
					out.extend_from_slice(&secs.to_le_bytes());
					out.extend_from_slice(&nanos.to_le_bytes());
					Ok(())
				})?;
			}
		},
		(Kind::Int, Leaf::Int(v)) => row.fixed(slot, &v.to_le_bytes()),
		(Kind::Float, Leaf::Float(v)) => row.fixed(slot, &v.to_bits().to_le_bytes()),
		(Kind::Datetime, Leaf::Datetime(secs, nanos)) => {
			row.fixed(slot, &secs.to_le_bytes());
			row.fixed(slot + 8, &nanos.to_le_bytes());
		}
		(Kind::Duration, Leaf::Duration(secs, nanos)) => {
			row.fixed(slot, &secs.to_le_bytes());
			row.fixed(slot + 8, &nanos.to_le_bytes());
		}
		(Kind::Uuid, Leaf::Uuid(v)) => row.fixed(slot, &v),
		(Kind::Object, Leaf::Object) => write_node(shape, row, Some(at), value)?,
		(Kind::String, Leaf::String(v)) => var(row, slot, |out| {
			out.extend_from_slice(v.as_bytes());
			Ok(())
		})?,
		(Kind::Bytes, Leaf::Bytes(v)) => var(row, slot, |out| {
			out.extend_from_slice(v);
			Ok(())
		})?,
		(Kind::Decimal, Leaf::Decimal(v)) => var(row, slot, |out| {
			decimal_text(out, &v);
			Ok(())
		})?,
		(Kind::Record, Leaf::Record(table, key)) => {
			var(row, slot, |out| write_record(out, &column.table, table, key, value))?
		}
		(Kind::Dynamic, _) => var(row, slot, |out| value.write_dynamic(out))?,
		(Kind::Array, Leaf::Array) => {
			let element = column.element.ok_or_else(misfit)?;
			var(row, slot, |out| write_array(out, element, value))?
		}
		_ => return Err(misfit()),
	}
	Ok(())
}

fn var(
	row: &mut RowWriter<'_>,
	slot: u32,
	write: impl FnOnce(&mut Vec<u8>) -> Result<(), ShapedError>,
) -> Result<(), ShapedError> {
	row.open_var(slot)?;
	write(row.out())?;
	row.close_var()
}

/// Writes a `record` slice for the record id of `table` and `key`, in a column whose table is
/// `column_table` (empty when each value carries its table); `whole` is the record id, for a key
/// held as CBOR.
pub fn write_record<'v, V: ShapedValue<'v>>(
	out: &mut Vec<u8>,
	column_table: &str,
	table: &str,
	key: KeyLeaf<'_>,
	whole: V,
) -> Result<(), ShapedError> {
	// A key held as CBOR, or a table too long for the slice's `u16` length where the slice carries
	// it, is the whole record id as CBOR.
	let carried_too_long = column_table.is_empty() && table.len() > usize::from(u16::MAX);
	let code = match key {
		_ if carried_too_long => 3,
		KeyLeaf::Int(_) => 0,
		KeyLeaf::String(_) => 1,
		KeyLeaf::Uuid(_) => 2,
		KeyLeaf::Other => 3,
	};
	if code == 3 {
		out.push(3);
		return whole.write_dynamic(out);
	}
	out.push(code);
	if column_table.is_empty() {
		let len = u16::try_from(table.len())
			.map_err(|_| ShapedError::new("a table name longer than 65535 bytes"))?;
		out.extend_from_slice(&len.to_le_bytes());
		out.extend_from_slice(table.as_bytes());
	} else if column_table != table {
		return Err(ShapedError::new("a record of another table than its column's"));
	}
	match key {
		KeyLeaf::Int(n) => out.extend_from_slice(&n.to_le_bytes()),
		KeyLeaf::String(s) => out.extend_from_slice(s.as_bytes()),
		KeyLeaf::Uuid(u) => out.extend_from_slice(&u),
		KeyLeaf::Other => {}
	}
	Ok(())
}

/// Nanoseconds since the epoch of a datetime, when they fit an `i64` strictly between the two
/// escape values and the nanoseconds are not a leap second's.
fn datetime_ns(secs: i64, nanos: u32) -> Option<i64> {
	if nanos >= 1_000_000_000 {
		return None;
	}
	let ns = i128::from(secs) * 1_000_000_000 + i128::from(nanos);
	i64::try_from(ns).ok().filter(|ns| *ns != i64::MIN && *ns != i64::MAX)
}

/// Nanoseconds of a duration, when they fit a `u64` below the escape value.
fn duration_ns(secs: u64, nanos: u32) -> Option<u64> {
	let ns = u128::from(secs) * 1_000_000_000 + u128::from(nanos);
	u64::try_from(ns).ok().filter(|ns| *ns != u64::MAX)
}

/// Whether `leaf` can be written in a column of `kind`: its own kind, or a compact timestamp or bit
/// representation that holds it.
fn leaf_fits(kind: Kind, leaf: &Leaf<'_>) -> bool {
	match (kind, leaf) {
		(Kind::Bit, Leaf::Bool(_)) => true,
		(Kind::DatetimeNs, Leaf::Datetime(..)) | (Kind::DurationNs, Leaf::Duration(..)) => true,
		(kind, leaf) => kind_of(leaf) == Some(kind),
	}
}

/// Whether every element of the array `value` can be written as an element of `kind`.
fn elements_fit<'v, V: ShapedValue<'v>>(kind: Kind, value: V) -> bool {
	value.elements().all(|e| {
		let leaf = e.leaf();
		!matches!(leaf, Leaf::None | Leaf::Null) && kind_of(&leaf) == Some(kind)
	})
}

/// Writes the array `value`, whose elements are all of `kind`, as an `array` slice: the count, then
/// for a variable element kind each element's end and the elements' bytes, for a fixed one the
/// elements back to back.
fn write_array<'v, V: ShapedValue<'v>>(
	out: &mut Vec<u8>,
	kind: Kind,
	value: V,
) -> Result<(), ShapedError> {
	let count = value.elements().count();
	let count32 = u32::try_from(count).map_err(|_| ShapedError::new("an array too long"))?;
	out.extend_from_slice(&count32.to_le_bytes());
	let misfit = || ShapedError::new("an array element of another kind than its column's");
	if kind.is_var() {
		let ends = out.len();
		out.resize(ends + 4 * count, 0);
		let base = out.len();
		for (at, element) in value.elements().enumerate() {
			match element.leaf() {
				Leaf::String(v) => out.extend_from_slice(v.as_bytes()),
				Leaf::Bytes(v) => out.extend_from_slice(v),
				Leaf::Decimal(v) => decimal_text(out, &v),
				Leaf::None
				| Leaf::Null
				| Leaf::Bool(_)
				| Leaf::I8(_)
				| Leaf::U8(_)
				| Leaf::I16(_)
				| Leaf::U16(_)
				| Leaf::I32(_)
				| Leaf::U32(_)
				| Leaf::Int(_)
				| Leaf::U64(_)
				| Leaf::F16(_)
				| Leaf::F32(_)
				| Leaf::Float(_)
				| Leaf::Datetime(..)
				| Leaf::Duration(..)
				| Leaf::Uuid(_)
				| Leaf::Record(..)
				| Leaf::Object
				| Leaf::Array
				| Leaf::Other => return Err(misfit()),
			}
			let end = u32::try_from(out.len() - base)
				.map_err(|_| ShapedError::new("an array larger than 4 GiB"))?;
			out[ends + 4 * at..ends + 4 * at + 4].copy_from_slice(&end.to_le_bytes());
		}
		return Ok(());
	}
	for element in value.elements() {
		match element.leaf() {
			Leaf::I8(v) => out.extend_from_slice(&v.to_le_bytes()),
			Leaf::U8(v) => out.extend_from_slice(&v.to_le_bytes()),
			Leaf::I16(v) => out.extend_from_slice(&v.to_le_bytes()),
			Leaf::U16(v) => out.extend_from_slice(&v.to_le_bytes()),
			Leaf::I32(v) => out.extend_from_slice(&v.to_le_bytes()),
			Leaf::U32(v) => out.extend_from_slice(&v.to_le_bytes()),
			Leaf::Int(v) => out.extend_from_slice(&v.to_le_bytes()),
			Leaf::U64(v) => out.extend_from_slice(&v.to_le_bytes()),
			Leaf::F16(v) => out.extend_from_slice(&v.to_bits().to_le_bytes()),
			Leaf::F32(v) => out.extend_from_slice(&v.to_bits().to_le_bytes()),
			Leaf::Float(v) => out.extend_from_slice(&v.to_bits().to_le_bytes()),
			Leaf::Bool(v) => out.push(u8::from(v)),
			Leaf::Uuid(v) => out.extend_from_slice(&v),
			Leaf::Datetime(secs, nanos) => {
				out.extend_from_slice(&secs.to_le_bytes());
				out.extend_from_slice(&nanos.to_le_bytes());
			}
			Leaf::Duration(secs, nanos) => {
				out.extend_from_slice(&secs.to_le_bytes());
				out.extend_from_slice(&nanos.to_le_bytes());
			}
			Leaf::None
			| Leaf::Null
			| Leaf::Decimal(_)
			| Leaf::String(_)
			| Leaf::Bytes(_)
			| Leaf::Record(..)
			| Leaf::Object
			| Leaf::Array
			| Leaf::Other => return Err(misfit()),
		}
	}
	Ok(())
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

/// Writes a decimal's text, as CBOR tag 10 holds it.
pub fn decimal_text(out: &mut Vec<u8>, d: &Decimal) {
	let mut stack = StackText {
		buf: [0; 64],
		len: 0,
	};
	if write!(stack, "{d}").is_ok() {
		out.extend_from_slice(&stack.buf[..stack.len]);
	} else {
		out.extend_from_slice(d.to_string().as_bytes());
	}
}

/// A CBOR head: major type `major` with argument `n`, in its shortest form.
pub fn cbor_head(out: &mut Vec<u8>, major: u8, n: u64) {
	let m = major << 5;
	if n <= 23 {
		out.push(m | n as u8);
	} else if n <= u64::from(u8::MAX) {
		out.extend_from_slice(&[m | 24, n as u8]);
	} else if n <= u64::from(u16::MAX) {
		out.push(m | 25);
		out.extend_from_slice(&(n as u16).to_be_bytes());
	} else if n <= u64::from(u32::MAX) {
		out.push(m | 26);
		out.extend_from_slice(&(n as u32).to_be_bytes());
	} else {
		out.push(m | 27);
		out.extend_from_slice(&n.to_be_bytes());
	}
}

/// A public [`Value`](crate::Value) as a [`ShapedValue`], with the encoder its dynamic slices
/// are written by (the CBOR format's, on the wire).
#[derive(Clone, Copy)]
pub struct PublicShaped<'v> {
	/// The value.
	pub value: &'v crate::Value,
	/// Writes a value as one dynamic item.
	pub dynamic: fn(&crate::Value, &mut Vec<u8>) -> Result<(), ShapedError>,
}

impl<'v> PublicShaped<'v> {
	/// `value`, its dynamic slices written by `dynamic`.
	pub fn new(
		value: &'v crate::Value,
		dynamic: fn(&crate::Value, &mut Vec<u8>) -> Result<(), ShapedError>,
	) -> Self {
		Self {
			value,
			dynamic,
		}
	}

	fn with(self, value: &'v crate::Value) -> Self {
		Self {
			value,
			dynamic: self.dynamic,
		}
	}
}

impl<'v> ShapedValue<'v> for PublicShaped<'v> {
	fn leaf(self) -> Leaf<'v> {
		use crate::{Number, RecordIdKey, Value};
		match self.value {
			Value::None => Leaf::None,
			Value::Null => Leaf::Null,
			Value::Bool(v) => Leaf::Bool(*v),
			Value::Number(Number::I8(v)) => Leaf::I8(*v),
			Value::Number(Number::U8(v)) => Leaf::U8(*v),
			Value::Number(Number::I16(v)) => Leaf::I16(*v),
			Value::Number(Number::U16(v)) => Leaf::U16(*v),
			Value::Number(Number::I32(v)) => Leaf::I32(*v),
			Value::Number(Number::U32(v)) => Leaf::U32(*v),
			Value::Number(Number::Int(v)) => Leaf::Int(*v),
			Value::Number(Number::U64(v)) => Leaf::U64(*v),
			Value::Number(Number::F16(v)) => Leaf::F16(*v),
			Value::Number(Number::F32(v)) => Leaf::F32(*v),
			Value::Number(Number::Float(v)) => Leaf::Float(*v),
			Value::Number(Number::Decimal(v)) => Leaf::Decimal(*v),
			// A bigdecimal that fits a decimal is a decimal leaf. Any other value has no
			// fixed-width leaf and is written through the dynamic path.
			Value::Number(Number::BigDecimal(v)) => match Decimal::try_from(v.as_ref()) {
				Ok(d) => Leaf::Decimal(d),
				Err(_) => Leaf::Other,
			},
			Value::String(v) => Leaf::String(v),
			Value::Bytes(v) => Leaf::Bytes(&v.0),
			Value::Datetime(v) => Leaf::Datetime(v.0.timestamp(), v.0.timestamp_subsec_nanos()),
			Value::Duration(v) => Leaf::Duration(v.0.as_secs(), v.0.subsec_nanos()),
			Value::Uuid(v) => Leaf::Uuid(*v.0.as_bytes()),
			Value::RecordId(r) => Leaf::Record(
				r.table.as_str(),
				match &r.key {
					RecordIdKey::Number(n) => KeyLeaf::Int(*n),
					RecordIdKey::String(s) => KeyLeaf::String(s),
					RecordIdKey::Uuid(u) => KeyLeaf::Uuid(*u.0.as_bytes()),
					RecordIdKey::Array(_) | RecordIdKey::Object(_) | RecordIdKey::Range(_) => {
						KeyLeaf::Other
					}
				},
			),
			Value::Object(_) => Leaf::Object,
			Value::Array(_) => Leaf::Array,
			Value::Table(_)
			| Value::Geometry(_)
			| Value::File(_)
			| Value::Range(_)
			| Value::Regex(_)
			| Value::Set(_)
			| Value::Date(_)
			| Value::Time(_)
			| Value::Timestamp(_)
			| Value::Interval(_) => Leaf::Other,
		}
	}

	fn get(self, key: &str) -> Option<Self> {
		self.value.as_object()?.get(key).map(|v| self.with(v))
	}

	fn entries(self) -> impl Iterator<Item = (&'v str, Self)> {
		self.value
			.as_object()
			.map(|o| o.iter())
			.into_iter()
			.flatten()
			.map(move |(k, v)| (k.as_str(), self.with(v)))
	}

	fn elements(self) -> impl Iterator<Item = Self> {
		self.value.as_array().map(|a| a.0.iter()).into_iter().flatten().map(move |v| self.with(v))
	}

	fn write_dynamic(self, out: &mut Vec<u8>) -> Result<(), ShapedError> {
		(self.dynamic)(self.value, out)
	}
}
