//! Shaped frames, physical side: the shape a packed generation's rows are written in, and the
//! copy plan that writes them.
//!
//! Both are pure functions of the generation's descriptor, the catalog fact the scan resolves for
//! the snapshot it reads ([`crate::kvs::RowGate`]). A row reaches the copy plan only when the plan
//! emits it untouched, so every field it holds is one the caller may select: a field whose select
//! permission is not FULL for the caller makes the body read the row, which takes the value path.
//!
//! The shape mirrors the stored row's fixed region: the same presence bits, bool bits, slot offsets
//! and widths. A row's presence and fixed region are therefore one copy of the stored bytes the
//! descriptor names (`fixed_size` bytes), after which the plan zeroes every slot and clears every
//! bit of a column the row does not hold, so no byte of an absent column reaches the wire, and
//! writes the columns the wire holds in another form (variable slices, values held as CBOR, the
//! escaped datetimes and durations). The layout rules are in `doc/design/shaped-frames.md`.

use std::sync::Arc;

use surrealdb_types::shaped::{
	Column as WireColumn, ColumnDef, Kind, MAY_BE_ABSENT, MAY_BE_NULL, NO_SLOT, Root, Shape,
	ShapeDef, presence_width,
};

use crate::catalog::layout::{ColumnRole, Location, PackedDescriptor, PhysicalKind, Presence};

/// The record field a stored record's key supplies.
const ID_KEY: &str = "id";

/// Where a wire column's value comes from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Source {
	/// The record's key.
	Id,
	/// Descriptor column `index`.
	Column(usize),
}

/// How a present wire column is written, beyond the copy of the fixed region.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Fill {
	/// The record id, from the key.
	Id,
	/// Held in the copied fixed region as stored: nothing more to write.
	Copied,
	/// An 8-byte datetime or duration slot: copied, and at an escape value its value written to
	/// the column's overflow slot.
	DatetimeNs,
	DurationNs,
	/// A string's stored UTF-8, spliced as written.
	String,
	/// Bytes from a variable slot, spliced as stored.
	RawBytes,
	/// Bytes converted from a fixed slot.
	Bytes,
	/// A string or bytes column whose slice a [`Run`] writes.
	InRun,
	Decimal,
	Record,
	/// One CBOR item.
	Dynamic,
	/// A stored array written as an `array` of the element kind: a `Value` column the statement's
	/// seed declares an array of one plain kind.
	Array(Kind),
	/// An object node: its remainder (descriptor column, depth), then its children.
	Object {
		rest: Option<usize>,
		depth: usize,
	},
}

/// One wire column a row needs written or checked, in the shape's depth-first order.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Op {
	pub source: Source,
	/// The wire column's index in the shape.
	pub column: u32,
	/// The op of the enclosing object column, or `None` for a root column.
	pub parent: Option<u32>,
	/// The wire column's first presence bit, or `NO_SLOT`.
	pub presence: u32,
	/// How many presence bits it takes: 0, 1 or 2.
	pub width: u32,
	/// The wire column's fixed offset (a bit index for `bit`) or var slot.
	pub slot: u32,
	/// The bytes of its fixed slot to zero when the row does not hold it: 0 for a var column, and
	/// for a bit (which `bit` clears).
	pub zero: u32,
	pub bit: bool,
	/// The wire column's remainder slot (an object) or overflow slot (a datetime or duration).
	pub rest_slot: u32,
	/// For a column held in a stored variable slice, its index.
	pub var: u32,
	pub fill: Fill,
	/// The run of variable slices this op ends, written once its members' presence is known.
	pub run: Option<Run>,
}

/// Consecutive stored variable slices written as consecutive wire var slots with one copy: ops
/// `first_op..first_op + count`, stored slices `first_var..`, wire slots `first_slot..`.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Run {
	pub first_op: u32,
	pub first_var: u32,
	pub first_slot: u32,
	pub count: u32,
}

/// The copy plan of one packed generation: the shape its rows are written in and how each row is
/// written.
#[derive(Debug)]
pub(crate) struct CopyPlan {
	pub shape: Arc<Shape>,
	/// The generation's descriptor, which every row of the generation is read against.
	pub descriptor: PackedDescriptor,
	/// The stored bytes copied as the row's presence and fixed regions: the descriptor's fixed
	/// size.
	pub fixed_len: u32,
	/// Wire fixed-region ranges (offset, length) of stored slots the wire holds in another form,
	/// zeroed in every row.
	pub junk: Vec<(u32, u32)>,
	/// The root's remainder: its descriptor column and its var slot.
	pub root_rest: Option<(usize, u32)>,
	pub ops: Vec<Op>,
}

/// A wire column before its var slots are numbered.
struct Node {
	column: WireColumn,
	source: Source,
	fill: Fill,
	/// A stored fixed slot the wire does not hold there: its wire offset and width.
	junk: Option<(u32, u32)>,
	/// The stored variable slice the column is held in, or `NO_SLOT`.
	var: u32,
	kids: Vec<Node>,
}

/// The copy plan of the generation `descriptor` describes, for rows of table `table`, whose root
/// `Value` columns that `seed` (the statement's seed shape) declares arrays of one plain kind are
/// held natively.
pub(crate) fn copy_plan(
	descriptor: &PackedDescriptor,
	table: &str,
	seed: Option<&Shape>,
) -> anyhow::Result<CopyPlan> {
	let presence_len = descriptor.presence_bits.div_ceil(8);
	anyhow::ensure!(presence_len <= descriptor.fixed_size, "a descriptor's fixed size too small");
	let cx = Cx {
		descriptor,
		presence_len,
		seed,
	};
	let (mut kids, rest, end) = cx.node(0, 0)?;
	anyhow::ensure!(end == descriptor.columns.len(), "a descriptor column outside every node");
	anyhow::ensure!(
		kids.iter().all(|k| k.column.name != ID_KEY),
		"a packed root column named `id`"
	);
	let id = Node {
		column: WireColumn {
			name: ID_KEY.to_owned(),
			kind: Kind::Record,
			element: None,
			flags: 0,
			presence: NO_SLOT,
			slot: NO_SLOT,
			table: table.to_owned(),
			children: 0,
			rest: NO_SLOT,
		},
		source: Source::Id,
		fill: Fill::Id,
		junk: None,
		var: NO_SLOT,
		kids: Vec::new(),
	};
	let at = kids.partition_point(|k| k.column.name.as_str() < ID_KEY);
	kids.insert(at, id);
	// Var slots in the order a row writes them: the root's remainder, then each column depth-first,
	// a node's remainder before its children.
	let mut var_count = 0u32;
	let mut next = || {
		let slot = var_count;
		var_count += 1;
		slot
	};
	let root_rest = rest.map(|_| next());
	let mut columns = Vec::new();
	let mut ops = Vec::new();
	let mut junk = Vec::new();
	flatten(&mut kids, None, &mut columns, &mut ops, &mut junk, &mut next);
	// The id's slot is the last, and its op runs last, so the stored slices the columns before it
	// splice lie in consecutive wire slots.
	let id_slot = next();
	if let Some(at) = ops.iter().position(|op: &Op| op.fill == Fill::Id) {
		let mut op = ops.remove(at);
		op.slot = id_slot;
		columns[op.column as usize].slot = id_slot;
		// No op is a child of the id, and every op after it moves down by one.
		for later in &mut ops[at..] {
			if let Some(parent) = &mut later.parent
				&& *parent as usize > at
			{
				*parent -= 1;
			}
		}
		ops.push(op);
	}
	runs(&mut ops);
	let shape = Arc::new(Shape::new(ShapeDef {
		root: Root::Object,
		presence_bits: descriptor.presence_bits,
		fixed_size: descriptor.fixed_size - presence_len,
		var_count,
		root_rest: root_rest.unwrap_or(NO_SLOT),
		columns,
	})?);
	Ok(CopyPlan {
		shape,
		descriptor: descriptor.clone(),
		fixed_len: descriptor.fixed_size,
		junk,
		root_rest: rest.zip(root_rest),
		ops,
	})
}

/// Numbers the var slots of `nodes` and lists them depth-first as wire columns and ops; a column
/// that needs no work per row (a required column whose value the copy holds) has no op.
fn flatten(
	nodes: &mut [Node],
	parent: Option<u32>,
	columns: &mut Vec<WireColumn>,
	ops: &mut Vec<Op>,
	junk: &mut Vec<(u32, u32)>,
	next: &mut impl FnMut() -> u32,
) {
	for node in nodes {
		let mut column = node.column.clone();
		if column.kind.is_var() && node.fill != Fill::Id {
			column.slot = next();
		}
		if column.kind.has_overflow() {
			column.rest = next();
		}
		if let Fill::Object {
			rest: Some(_),
			..
		} = node.fill
		{
			column.rest = next();
		}
		let at_column = columns.len();
		columns.push(column.clone());
		if let Some(range) = node.junk {
			junk.push(range);
		}
		let needs_op = parent.is_some()
			|| column.presence != NO_SLOT
			|| node.fill != Fill::Copied
			|| column.kind == Kind::Object;
		let op = needs_op.then(|| {
			ops.push(Op {
				source: node.source,
				column: at_column as u32,
				parent,
				presence: column.presence,
				width: presence_width(column.flags),
				slot: column.slot,
				zero: match (column.kind, column.kind.fixed_width()) {
					(Kind::Bit, _) | (_, None) => 0,
					(_, Some(width)) => width,
				},
				bit: column.kind == Kind::Bit,
				rest_slot: column.rest,
				var: node.var,
				fill: node.fill,
				run: None,
			});
			(ops.len() - 1) as u32
		});
		let before = columns.len();
		flatten(&mut node.kids, op, columns, ops, junk, next);
		columns[at_column].children = (columns.len() - before) as u32;
	}
}

/// Groups consecutive root ops that splice stored slices (strings and stored bytes) into runs:
/// their stored slices are consecutive, as are their wire slots. A run is written by its last op.
fn runs(ops: &mut [Op]) {
	let splices = |op: &Op| {
		op.parent.is_none() && matches!(op.fill, Fill::String | Fill::RawBytes) && op.var != NO_SLOT
	};
	let mut at = 0;
	while at < ops.len() {
		let mut end = at;
		while end < ops.len()
			&& end - at < 32
			&& splices(&ops[end])
			&& (end == at
				|| (ops[end].var == ops[end - 1].var + 1 && ops[end].slot == ops[end - 1].slot + 1))
		{
			end += 1;
		}
		if end - at >= 2 {
			let run = Run {
				first_op: at as u32,
				first_var: ops[at].var,
				first_slot: ops[at].slot,
				count: (end - at) as u32,
			};
			for op in &mut ops[at..end] {
				op.fill = Fill::InRun;
			}
			ops[end - 1].run = Some(run);
			at = end;
		} else {
			at += 1;
		}
	}
}

struct Cx<'d> {
	descriptor: &'d PackedDescriptor,
	presence_len: u32,
	seed: Option<&'d Shape>,
}

impl Cx<'_> {
	/// The element kind of the root column `name`, where the seed declares it an array of one.
	fn declared_array(&self, depth: usize, name: &str) -> Option<Kind> {
		if depth != 0 {
			return None;
		}
		let seed = self.seed?;
		let at = seed.child(None, name)?;
		let column = &seed.columns()[at];
		(column.kind == Kind::Array).then_some(column.element).flatten()
	}

	/// The wire columns of the node at depth `depth` whose remainder and columns start at
	/// descriptor column `start`: its children, its remainder column, and the first column past it.
	fn node(
		&self,
		start: usize,
		depth: usize,
	) -> anyhow::Result<(Vec<Node>, Option<usize>, usize)> {
		let columns = &self.descriptor.columns;
		let mut at = start;
		let mut rest = None;
		if let Some(column) = columns.get(at)
			&& column.role == ColumnRole::Remainder
			&& column.path.len() == depth
		{
			rest = Some(at);
			at += 1;
		}
		let mut kids = Vec::new();
		while let Some(column) = columns.get(at)
			&& column.path.len() > depth
		{
			anyhow::ensure!(
				column.role == ColumnRole::Field && column.path.len() == depth + 1,
				"a descriptor column out of its node"
			);
			let (flags, presence) = match column.presence {
				Presence::Required => (0, NO_SLOT),
				Presence::Optional {
					bit,
				} => (MAY_BE_ABSENT, bit),
				Presence::Nullable {
					bit,
				} => (MAY_BE_NULL, bit),
				Presence::OptionalNullable {
					bit,
				} => (MAY_BE_ABSENT | MAY_BE_NULL, bit),
			};
			// The wire offset of a stored fixed slot, and its width.
			let fixed = if let Location::Fixed(offset) = column.location {
				let width = column.kind.fixed_width().unwrap_or(0);
				anyhow::ensure!(offset >= self.presence_len, "a fixed slot in the presence region");
				Some((offset - self.presence_len, width))
			} else {
				None
			};
			let (kind, slot, fill, junk) = match (column.kind, column.location) {
				(PhysicalKind::Bool, Location::Bit(bit)) => (Kind::Bit, bit, Fill::Copied, None),
				(PhysicalKind::Object, _) => (Kind::Object, NO_SLOT, Fill::Copied, None),
				(kind, Location::Fixed(_)) => {
					let (offset, width) = fixed.unwrap_or((0, 0));
					match kind {
						PhysicalKind::I8 => (Kind::I8, offset, Fill::Copied, None),
						PhysicalKind::I16 => (Kind::I16, offset, Fill::Copied, None),
						PhysicalKind::I32 => (Kind::I32, offset, Fill::Copied, None),
						PhysicalKind::I64 => (Kind::Int, offset, Fill::Copied, None),
						PhysicalKind::U8 => (Kind::U8, offset, Fill::Copied, None),
						PhysicalKind::U16 => (Kind::U16, offset, Fill::Copied, None),
						PhysicalKind::U32 => (Kind::U32, offset, Fill::Copied, None),
						PhysicalKind::U64 => (Kind::U64, offset, Fill::Copied, None),
						PhysicalKind::F16 => (Kind::F16, offset, Fill::Copied, None),
						PhysicalKind::F32 => (Kind::F32, offset, Fill::Copied, None),
						PhysicalKind::F64 => (Kind::Float, offset, Fill::Copied, None),
						PhysicalKind::Uuid => (Kind::Uuid, offset, Fill::Copied, None),
						PhysicalKind::DatetimeWide => (Kind::Datetime, offset, Fill::Copied, None),
						PhysicalKind::Datetime => {
							(Kind::DatetimeNs, offset, Fill::DatetimeNs, None)
						}
						PhysicalKind::Duration => {
							(Kind::DurationNs, offset, Fill::DurationNs, None)
						}
						PhysicalKind::Decimal16 => {
							(Kind::Decimal, NO_SLOT, Fill::Decimal, Some((offset, width)))
						}
						PhysicalKind::Fixed(_) => {
							(Kind::Bytes, NO_SLOT, Fill::Bytes, Some((offset, width)))
						}
						// A point and a literal code are held as CBOR.
						PhysicalKind::Bool
						| PhysicalKind::Point
						| PhysicalKind::LiteralCode(_)
						| PhysicalKind::String
						| PhysicalKind::Bytes
						| PhysicalKind::RecordKey
						| PhysicalKind::DecimalVar
						| PhysicalKind::BigDecimal
						| PhysicalKind::Value
						| PhysicalKind::Object => (Kind::Dynamic, NO_SLOT, Fill::Dynamic, Some((offset, width))),
					}
				}
				(PhysicalKind::String, _) => (Kind::String, NO_SLOT, Fill::String, None),
				(PhysicalKind::Bytes, _) => (Kind::Bytes, NO_SLOT, Fill::RawBytes, None),
				(PhysicalKind::DecimalVar, _) => (Kind::Decimal, NO_SLOT, Fill::Decimal, None),
				(PhysicalKind::RecordKey, _) => (Kind::Record, NO_SLOT, Fill::Record, None),
				// A bigdecimal is read as the stored BIGDECIMAL it holds, and written as its CBOR.
				(PhysicalKind::BigDecimal, _) => (Kind::Dynamic, NO_SLOT, Fill::Dynamic, None),
				// A `Value` slice holds `NULL` as a value of its own, written as CBOR `null`.
				_ => match self.declared_array(depth, &column.path[depth]) {
					Some(element) => (Kind::Array, NO_SLOT, Fill::Array(element), None),
					None => (Kind::Dynamic, NO_SLOT, Fill::Dynamic, None),
				},
			};
			let table = match (kind, column.tables.as_slice()) {
				(Kind::Record, [one]) => one.as_str().to_owned(),
				_ => String::new(),
			};
			let (kids_of, fill, next) = if kind == Kind::Object {
				let (kids_of, rest, next) = self.node(at + 1, depth + 1)?;
				(
					kids_of,
					Fill::Object {
						rest,
						depth: depth + 1,
					},
					next,
				)
			} else {
				(Vec::new(), fill, at + 1)
			};
			kids.push(Node {
				column: WireColumn {
					name: column.path[depth].clone(),
					kind,
					element: if let Fill::Array(element) = fill {
						Some(element)
					} else {
						None
					},
					flags,
					presence,
					slot,
					table,
					children: 0,
					rest: NO_SLOT,
				},
				source: Source::Column(at),
				fill,
				junk,
				var: if let Location::Var(var) = column.location {
					var
				} else {
					NO_SLOT
				},
				kids: kids_of,
			});
			at = next;
		}
		Ok((kids, rest, at))
	}
}

/// The seed shape of the value-path rows of a statement over table `table`, from the table's
/// field definitions `fields`, for a caller whose permissions are checked when `checked`: the
/// record `id`, then one column per top-level field the caller may see.
///
/// A field whose select permission never admits the caller (`NONE`, or a predicate that is
/// `false`) has no column, so a stream never names it. A FULL field, or any field for a caller
/// whose permissions are not checked, is a column as declared. A field behind a row-dependent
/// predicate is a column that may be absent: a row it is hidden in holds it absent, as it holds
/// `NONE`. A computed field is a dynamic column that may be absent or `NULL`.
pub(crate) fn seed_shape(
	fields: &[crate::catalog::FieldDefinition],
	table: &str,
	checked: bool,
) -> anyhow::Result<Arc<Shape>> {
	use crate::catalog::Permission;
	use crate::expr::{Expr, Literal};
	let mut columns = vec![ColumnDef {
		name: ID_KEY.to_owned(),
		kind: Kind::Record,
		element: None,
		flags: 0,
		table: table.to_owned(),
		open: false,
		children: Vec::new(),
	}];
	for field in fields {
		let [crate::expr::Part::Field(name)] = field.name.0.as_slice() else {
			continue;
		};
		let name = name.as_str();
		if name == ID_KEY || columns.iter().any(|c| c.name == name) {
			continue;
		}
		let gated = match (&field.select_permission, checked) {
			(_, false) | (Permission::Full, true) => false,
			(Permission::None, true) => continue,
			(Permission::Specific(Expr::Literal(Literal::Bool(false))), true) => continue,
			(Permission::Specific(_), true) => true,
		};
		let (kind, element, mut flags, table) = match (&field.computed, &field.field_kind) {
			(Some(_), _) | (None, None) => {
				(Kind::Dynamic, None, MAY_BE_ABSENT | MAY_BE_NULL, String::new())
			}
			(None, Some(kind)) => declared(kind),
		};
		if gated {
			flags |= MAY_BE_ABSENT;
		}
		columns.push(ColumnDef {
			name: name.to_owned(),
			kind,
			element,
			flags,
			table,
			open: false,
			children: Vec::new(),
		});
	}
	columns.sort_by(|a, b| a.name.cmp(&b.name));
	Ok(Arc::new(Shape::new(ShapeDef::object(false, &columns))?))
}

/// The wire kind, element kind, flags and record table a declared field kind gives its column.
fn declared(kind: &crate::expr::Kind) -> (Kind, Option<Kind>, u8, String) {
	use crate::expr::Kind as K;
	let plain = |kind: Kind| (kind, None, 0, String::new());
	match kind {
		K::Bool => plain(Kind::Bool),
		K::Int => plain(Kind::Int),
		K::I8 => plain(Kind::I8),
		K::I16 => plain(Kind::I16),
		K::I32 => plain(Kind::I32),
		K::U8 => plain(Kind::U8),
		K::U16 => plain(Kind::U16),
		K::U32 => plain(Kind::U32),
		K::U64 => plain(Kind::U64),
		K::Float => plain(Kind::Float),
		K::F16 => plain(Kind::F16),
		K::F32 => plain(Kind::F32),
		K::Decimal => plain(Kind::Decimal),
		K::String => plain(Kind::String),
		K::Datetime => plain(Kind::Datetime),
		K::Duration => plain(Kind::Duration),
		K::Uuid => plain(Kind::Uuid),
		K::Bytes => plain(Kind::Bytes),
		K::Record(tables) => match tables.as_slice() {
			[one] => (Kind::Record, None, 0, one.as_str().to_owned()),
			_ => plain(Kind::Record),
		},
		// An array whose elements are declared of one plain kind is held natively.
		K::Array(inner, _) => match declared(inner) {
			(element, None, 0, _) if element.is_element() => {
				(Kind::Array, Some(element), 0, String::new())
			}
			_ => plain(Kind::Dynamic),
		},
		K::None => (Kind::Dynamic, None, MAY_BE_ABSENT, String::new()),
		K::Null => (Kind::Dynamic, None, MAY_BE_NULL, String::new()),
		K::Either(kinds) => {
			let mut flags = 0;
			let mut rest = Vec::new();
			for kind in kinds {
				if matches!(kind, K::None) {
					flags |= MAY_BE_ABSENT;
				} else if matches!(kind, K::Null) {
					flags |= MAY_BE_NULL;
				} else {
					rest.push(kind);
				}
			}
			match rest.as_slice() {
				[one] => {
					let (kind, element, inner, table) = declared(one);
					(kind, element, flags | inner, table)
				}
				_ => (Kind::Dynamic, None, flags, String::new()),
			}
		}
		K::Any
		| K::Named(_)
		| K::Number
		| K::Object
		| K::Regex
		| K::Table(_)
		| K::Geometry(_)
		| K::Set(..)
		| K::Function(..)
		| K::Range
		| K::Literal(_)
		| K::File(_)
		| K::Enum(_)
		| K::Date
		| K::Time
		| K::Timestamp
		| K::Interval
		| K::BigDecimal => plain(Kind::Dynamic),
	}
}
