//! The layout a table's rows are written in, derived from the table's definition and its field
//! definitions.
//!
//! [`derive_layout`] is a pure function of a [`TableShape`] and the compiled field definitions
//! ([`FieldCandidate`]) of the table, whatever the order they are given in. A store mints a
//! generation when the layout derived at a table's stamp differs from the head's, so the output
//! is a cross-release contract: for a given feature level, every release derives the same bytes
//! from the same catalog. A change to any rule below is gated by the feature level.
//!
//! # Which tables pack
//!
//! Only a `SCHEMAFULL` table that is not a view, not `DROP` and not a lightweight relation packs
//! ([`TableShape::may_pack`]). Every other table derives the document layout. A `SCHEMAFULL` table
//! always derives a packed layout, with no columns when it defines no field.
//!
//! # Columns
//!
//! The columns ascend by path, an object node before every column under it.
//!
//! * A field is a column at its path. A `FLEXIBLE` object, an `object` with no defined child and
//!   every kind with no fixed-width or compact physical form is one `Value` column holding the
//!   whole value.
//! * An `object` that is not `FLEXIBLE` and has at least one defined child is a node with no slot:
//!   its children are columns of their own, so `address.city` is a column and `address` only says
//!   whether the object is present. A path that a definition of an element (`P.*`, `P[0]`) names is
//!   never a node.
//! * A path with no definition whose descendants are defined, and a path with more than one
//!   definition, is a `Value` column that holds any value of the key or its absence. The packed
//!   form never depends on the schema being tidy.
//! * Fields rooted at `id`, `COMPUTED` fields and everything under them have no column: the row's
//!   id is its key and a computed value is not stored.
//! * The `in` and `out` of a relation table are columns even without a definition: a key column
//!   over the tables the relation names, or a `Value` column when it names none.
//!
//! A column's presence follows the field's kind: a bit when it admits `NONE`, a bit when it
//! admits `NULL`, two bits when it admits both, none when it must be present. A record that does
//! not fit the layout (a value of another kind, a key with no column) is not packed: it is
//! written in the document layout at the same generation.
//!
//! A table whose columns would exceed what a descriptor can hold derives the document layout.

use std::collections::HashSet;

use crate::expr::kind::GeometryKind;
use crate::expr::{Kind, KindLiteral, Part};
use crate::layout::{
	Column, ColumnRole, LayoutDescriptor, Location, MAX_COLUMNS, MAX_FIXED_SIZE, MAX_KEY_TABLES,
	MAX_PATH_DEPTH, PackedDescriptor, PhysicalKind, Presence, RowLayout,
};
use crate::val::{Number, TableName, Value};
use crate::{FieldCandidate, StoredTableDefinition, TableType};

/// The most literals one `LiteralCode` column lists: what a 2-byte code names.
const MAX_LITERALS: usize = 1 << 16;

/// The widest a 1-byte literal code lists.
const NARROW_LITERALS: usize = 1 << 8;

/// What derivation reads of a table definition.
#[derive(Clone, Copy, Debug)]
pub struct TableShape<'a> {
	/// The table.
	pub table: &'a TableName,
	/// Whether the table is `SCHEMAFULL`.
	pub schemafull: bool,
	/// Whether the table is `DROP`.
	pub drop: bool,
	/// Whether the table is a view.
	pub view: bool,
	/// The table's type.
	pub table_type: &'a TableType,
}

impl<'a> TableShape<'a> {
	/// The shape of `stored`, the definition of `table`.
	pub fn of(table: &'a TableName, stored: &'a StoredTableDefinition) -> Self {
		Self {
			table,
			schemafull: stored.schemafull,
			drop: stored.drop,
			view: stored.view.is_some(),
			table_type: &stored.table_type,
		}
	}

	/// Whether rows of the table may be packed: it is `SCHEMAFULL`, not `DROP`, not a view and not
	/// a lightweight relation.
	pub fn may_pack(&self) -> bool {
		self.schemafull
			&& !self.drop
			&& !self.view
			&& !matches!(self.table_type, TableType::Relation(relation) if relation.lightweight)
	}
}

/// The layout the rows of `table` are written in at a generation minted now, derived from the
/// table's definition and its compiled field definitions alone.
///
/// The result is deterministic in its inputs whatever their order, and it always passes
/// [`LayoutDescriptor::validate`]: a table that cannot be described by a valid packed descriptor
/// derives the document layout.
pub fn derive_layout(table: TableShape<'_>, fields: &[&FieldCandidate]) -> LayoutDescriptor {
	if !table.may_pack() {
		return LayoutDescriptor::document(table.table.clone());
	}
	let Some(packed) = assemble(draft_columns(table, fields)) else {
		return LayoutDescriptor::document(table.table.clone());
	};
	let descriptor = LayoutDescriptor {
		table: table.table.clone(),
		layout: RowLayout::Packed(packed),
		migration: None,
		migrated: None,
	};
	match descriptor.validate() {
		Ok(()) => descriptor,
		Err(refusal) => {
			debug_assert!(false, "a derived layout fails its validation: {refusal:?}");
			LayoutDescriptor::document(table.table.clone())
		}
	}
}

/// How many presence bits a column takes, and which states they distinguish.
#[derive(Clone, Copy, Debug)]
enum Shape {
	/// No bit.
	Required,
	/// One bit: `NONE`.
	Optional,
	/// One bit: `NULL`.
	Nullable,
	/// Two bits: `NONE` and `NULL`.
	Both,
}

impl Shape {
	/// The number of presence bits.
	const fn bits(self) -> u32 {
		match self {
			Self::Required => 0,
			Self::Optional | Self::Nullable => 1,
			Self::Both => 2,
		}
	}
}

/// A column before its bits, offsets and indexes are assigned.
struct Draft {
	path: Vec<String>,
	kind: PhysicalKind,
	presence: Shape,
	literals: Vec<Value>,
	tables: Vec<TableName>,
}

impl Draft {
	fn new(path: &[&str], kind: PhysicalKind, presence: Shape) -> Self {
		Self {
			path: path.iter().map(|segment| (*segment).to_owned()).collect(),
			kind,
			presence,
			literals: Vec::new(),
			tables: Vec::new(),
		}
	}
}

/// A field definition with the leading run of named parts of its path.
struct Placed<'a> {
	/// The names of the path's leading parts, raw.
	run: Vec<&'a str>,
	/// Whether the path is made of those names alone.
	full: bool,
	candidate: &'a FieldCandidate,
}

impl<'a> Placed<'a> {
	fn of(candidate: &'a FieldCandidate) -> Self {
		let parts = &candidate.idiom().0;
		let run: Vec<&'a str> = parts
			.iter()
			.map_while(|part| match part {
				Part::Field(name) => Some(name.as_str()),
				_ => None,
			})
			.collect();
		let full = run.len() == parts.len();
		Self {
			run,
			full,
			candidate,
		}
	}

	/// Whether the field's value is not held in the record under its own path: the record's id,
	/// and a computed value.
	fn is_unstored(&self) -> bool {
		!self.run.is_empty() && (self.run.first() == Some(&"id") || self.candidate.computed())
	}

	/// Whether the field names a path a column can have: it starts with a name and no name is
	/// empty.
	fn is_placeable(&self) -> bool {
		!self.run.is_empty() && self.run.iter().all(|segment| !segment.is_empty())
	}
}

/// A column's source: a field definition, or the key column of a relation's `in` or `out`.
enum Source<'a> {
	Field(&'a FieldCandidate),
	Link(&'a [TableName]),
}

/// A source and the path it defines.
struct Entry<'a> {
	path: Vec<&'a str>,
	source: Source<'a>,
}

/// A field kind with its unions flattened: the members that are neither `none` nor `null`, and
/// which of those two the kind admits.
struct Flat<'k> {
	members: Vec<&'k Kind>,
	none: bool,
	null: bool,
	any: bool,
}

impl<'k> Flat<'k> {
	/// The flattened `kind`. A missing kind is `any`, and `any` admits `none` and `null`.
	fn of(kind: Option<&'k Kind>) -> Self {
		let mut flat = Self {
			members: Vec::new(),
			none: false,
			null: false,
			any: false,
		};
		let Some(kind) = kind else {
			flat.any = true;
			flat.none = true;
			flat.null = true;
			return flat;
		};
		// Members keep declaration order: the stack is filled in reverse.
		let mut stack: Vec<&'k Kind> = vec![kind];
		while let Some(kind) = stack.pop() {
			match kind {
				Kind::Any => {
					flat.any = true;
					flat.none = true;
					flat.null = true;
				}
				Kind::None => flat.none = true,
				Kind::Null => flat.null = true,
				Kind::Either(kinds) => stack.extend(kinds.iter().rev()),
				other => flat.members.push(other),
			}
		}
		flat
	}

	/// Whether the kind is exactly `object`, with `none` and `null` allowed.
	fn is_object(&self) -> bool {
		!self.any && matches!(self.members.as_slice(), [Kind::Object])
	}

	fn presence(&self) -> Shape {
		match (self.none, self.null) {
			(false, false) => Shape::Required,
			(true, false) => Shape::Optional,
			(false, true) => Shape::Nullable,
			(true, true) => Shape::Both,
		}
	}
}

/// The columns of `table`, in column order, with presence and kind decided.
fn draft_columns(table: TableShape<'_>, fields: &[&FieldCandidate]) -> Vec<Draft> {
	let placed: Vec<Placed<'_>> = fields.iter().copied().map(Placed::of).collect();
	let skipped: Vec<&[&str]> = placed
		.iter()
		.filter(|field| field.is_unstored())
		.map(|field| field.run.as_slice())
		.collect();
	let kept: Vec<&Placed<'_>> = placed
		.iter()
		.filter(|field| {
			field.is_placeable() && !skipped.iter().any(|prefix| field.run.starts_with(prefix))
		})
		.collect();
	// The paths a definition of an element names (`P.*`, `P[0]`): none of them is a node.
	let blocked: HashSet<Vec<&str>> =
		kept.iter().filter(|field| !field.full).map(|field| field.run.clone()).collect();
	let mut entries: Vec<Entry<'_>> = kept
		.iter()
		.filter(|field| field.full)
		.map(|field| Entry {
			path: field.run.clone(),
			source: Source::Field(field.candidate),
		})
		.collect();
	if let TableType::Relation(relation) = table.table_type {
		for (name, tables) in [("in", &relation.from), ("out", &relation.to)] {
			if !entries.iter().any(|entry| entry.path.first() == Some(&name)) {
				entries.push(Entry {
					path: vec![name],
					source: Source::Link(tables),
				});
			}
		}
	}
	// A stable sort: definitions of one path stay adjacent, and any order of them derives the same
	// column.
	entries.sort_by(|a, b| a.path.cmp(&b.path));
	let mut drafts = Vec::new();
	build(&entries, 0, &blocked, &mut drafts);
	drafts
}

/// Appends the columns of the entries at `depth`, which share their first `depth` names and are in
/// path order: one column per distinct next name, then, for a node, the columns under it.
///
/// A node's depth is below [`MAX_PATH_DEPTH`], which bounds the recursion.
fn build<'a>(
	entries: &[Entry<'a>],
	depth: usize,
	blocked: &HashSet<Vec<&'a str>>,
	out: &mut Vec<Draft>,
) {
	let mut rest = entries;
	while let Some(first) = rest.first() {
		let Some(&name) = first.path.get(depth) else {
			return;
		};
		let size = rest.iter().take_while(|entry| entry.path.get(depth) == Some(&name)).count();
		let Some((group, after)) = rest.split_at_checked(size) else {
			return;
		};
		rest = after;
		let Some(path) = first.path.get(..depth.saturating_add(1)) else {
			return;
		};
		// A shorter path sorts first, so the definitions of `path` itself lead the group.
		let own =
			group.iter().take_while(|entry| entry.path.len() == depth.saturating_add(1)).count();
		let Some((own_entries, deeper)) = group.split_at_checked(own) else {
			return;
		};
		match own_entries {
			[only] => match &only.source {
				Source::Link(tables) => out.push(link_column(path, tables)),
				Source::Field(candidate) => {
					let flat = Flat::of(candidate.kind());
					let presence = flat.presence();
					let is_node = flat.is_object()
						&& !candidate.flexible()
						&& path.len() < MAX_PATH_DEPTH
						&& !blocked.contains(path)
						&& deeper.iter().any(|entry| entry.path.len() == depth.saturating_add(2));
					if is_node {
						out.push(Draft::new(path, PhysicalKind::Object, presence));
						build(deeper, depth.saturating_add(1), blocked, out);
					} else {
						out.push(leaf_column(path, &flat, presence));
					}
				}
			},
			// No definition of the path, or several: the column holds whatever the key holds.
			_ => out.push(Draft::new(path, PhysicalKind::Value, Shape::Both)),
		}
	}
}

/// The column of a relation's `in` or `out` that has no definition: required, and a key column
/// when the relation names the tables.
fn link_column(path: &[&str], tables: &[TableName]) -> Draft {
	let mut draft = Draft::new(path, PhysicalKind::Value, Shape::Required);
	let mut named: Vec<TableName> = Vec::new();
	if add_tables(&mut named, tables) && !named.is_empty() {
		draft.kind = PhysicalKind::RecordKey;
		draft.tables = named;
	}
	draft
}

/// Appends the tables of `named` that `into` does not hold, in order. Answers whether `into` is
/// still within what a `RecordKey` column names.
fn add_tables(into: &mut Vec<TableName>, named: &[TableName]) -> bool {
	for table in named {
		if !into.contains(table) {
			into.push(table.clone());
		}
		if into.len() > MAX_KEY_TABLES {
			return false;
		}
	}
	true
}

/// The column of a field that is not a node: the narrowest physical kind that holds every value
/// of the field's kind, or a `Value` column.
fn leaf_column(path: &[&str], flat: &Flat<'_>, presence: Shape) -> Draft {
	let mut draft = Draft::new(path, PhysicalKind::Value, presence);
	if flat.any {
		return draft;
	}
	let single = match flat.members.as_slice() {
		[Kind::Int] => Some(PhysicalKind::I64),
		[Kind::Float] => Some(PhysicalKind::F64),
		[Kind::Bool] => Some(PhysicalKind::Bool),
		[Kind::Decimal] => Some(PhysicalKind::DecimalVar),
		[Kind::Datetime] => Some(PhysicalKind::Datetime),
		[Kind::Duration] => Some(PhysicalKind::Duration),
		[Kind::Uuid] => Some(PhysicalKind::Uuid),
		[Kind::String] => Some(PhysicalKind::String),
		[Kind::Bytes] => Some(PhysicalKind::Bytes),
		[Kind::Geometry(kinds)] if matches!(kinds.as_slice(), [GeometryKind::Point]) => {
			Some(PhysicalKind::Point)
		}
		_ => None,
	};
	if let Some(kind) = single {
		draft.kind = kind;
	} else if let Some(tables) = record_tables(&flat.members) {
		draft.kind = PhysicalKind::RecordKey;
		draft.tables = tables;
	} else if let Some(literals) = literal_values(&flat.members) {
		draft.kind = PhysicalKind::LiteralCode(if literals.len() <= NARROW_LITERALS {
			1
		} else {
			2
		});
		draft.literals = literals;
	}
	draft
}

/// The tables of a union of record links, in declaration order without repeats, when every member
/// names its tables and they fit one `RecordKey` column.
fn record_tables(members: &[&Kind]) -> Option<Vec<TableName>> {
	let mut tables: Vec<TableName> = Vec::new();
	for member in members {
		let Kind::Record(named) = member else {
			return None;
		};
		// A link to any table has no list to key on.
		if named.is_empty() || !add_tables(&mut tables, named) {
			return None;
		}
	}
	(!tables.is_empty()).then_some(tables)
}

/// The values of a union of scalar literals, in declaration order without repeats, when they fit
/// one `LiteralCode` column.
///
/// Two literals are the same when their stored encodings are: the grammar keeps a float's bits
/// and a decimal's scale, so equal encodings are one value under `same_value`.
fn literal_values(members: &[&Kind]) -> Option<Vec<Value>> {
	let mut values: Vec<Value> = Vec::new();
	let mut encodings: HashSet<Vec<u8>> = HashSet::new();
	for member in members {
		let Kind::Literal(literal) = member else {
			return None;
		};
		let value = match literal {
			KindLiteral::String(text) => Value::String(text.clone()),
			KindLiteral::Integer(int) => Value::Number(Number::Int(*int)),
			KindLiteral::Float(float) => Value::Number(Number::Float(*float)),
			KindLiteral::Decimal(decimal) => Value::Number(Number::Decimal(*decimal)),
			KindLiteral::Duration(duration) => Value::Duration(*duration),
			KindLiteral::Bool(flag) => Value::Bool(*flag),
			KindLiteral::Array(_) | KindLiteral::Object(_) => return None,
		};
		let encoding = crate::val::stored::encode(&value).ok()?;
		if encodings.insert(encoding) {
			if values.len() >= MAX_LITERALS {
				return None;
			}
			values.push(value);
		}
	}
	(!values.is_empty()).then_some(values)
}

/// Assigns presence bits, bool bits, fixed offsets and variable indexes densely in column order.
///
/// `None` when the columns exceed what a descriptor holds.
fn assemble(drafts: Vec<Draft>) -> Option<PackedDescriptor> {
	if drafts.len() > MAX_COLUMNS {
		return None;
	}
	let mut presence_bits: u32 = 0;
	let mut bool_bits: u32 = 0;
	for draft in &drafts {
		presence_bits = presence_bits.checked_add(draft.presence.bits())?;
		if draft.kind == PhysicalKind::Bool {
			bool_bits = bool_bits.checked_add(1)?;
		}
	}
	let header = presence_bits.div_ceil(8).checked_add(bool_bits.div_ceil(8))?;
	let mut presence_next: u32 = 0;
	let mut bool_next: u32 = 0;
	let mut fixed_next: u32 = header;
	let mut var_next: u32 = 0;
	let mut columns: Vec<Column> = Vec::with_capacity(drafts.len());
	for draft in drafts {
		let bit = presence_next;
		presence_next = presence_next.checked_add(draft.presence.bits())?;
		let presence = match draft.presence {
			Shape::Required => Presence::Required,
			Shape::Optional => Presence::Optional {
				bit,
			},
			Shape::Nullable => Presence::Nullable {
				bit,
			},
			Shape::Both => Presence::OptionalNullable {
				bit,
			},
		};
		let location = if draft.kind == PhysicalKind::Object {
			Location::None
		} else if draft.kind == PhysicalKind::Bool {
			let at = bool_next;
			bool_next = bool_next.checked_add(1)?;
			Location::Bit(at)
		} else if let Some(width) = draft.kind.fixed_width() {
			let at = fixed_next;
			fixed_next = fixed_next.checked_add(width).filter(|end| *end <= MAX_FIXED_SIZE)?;
			Location::Fixed(at)
		} else {
			let at = var_next;
			var_next = var_next.checked_add(1)?;
			Location::Var(at)
		};
		columns.push(Column {
			path: draft.path,
			kind: draft.kind,
			presence,
			location,
			role: ColumnRole::Field,
			literals: draft.literals,
			tables: draft.tables,
		});
	}
	Some(PackedDescriptor {
		columns,
		presence_bits,
		bool_bits,
		fixed_size: fixed_next,
		var_count: var_next,
	})
}

#[cfg(test)]
mod tests {
	use surrealdb_strand::Strand;

	use super::*;
	use crate::conformance::tests::definition;
	use crate::generation::tests::check_frozen;
	use crate::{ExprText, Relation, StoredFieldDefinition};

	fn t() -> TableName {
		TableName::new("t")
	}

	fn schemafull(table_type: &TableType) -> TableShape<'static> {
		// The shape borrows its table and type: both live for the test.
		let table: &'static TableName = Box::leak(Box::new(t()));
		let table_type: &'static TableType = Box::leak(Box::new(table_type.clone()));
		TableShape {
			table,
			schemafull: true,
			drop: false,
			view: false,
			table_type,
		}
	}

	fn derive(shape: TableShape<'_>, definitions: &[StoredFieldDefinition]) -> LayoutDescriptor {
		let candidates: Vec<FieldCandidate> = definitions
			.iter()
			.map(|definition| {
				FieldCandidate::compile(definition.name.as_str(), definition)
					.unwrap_or_else(|| panic!("{} does not compile", definition.name.as_str()))
			})
			.collect();
		let candidates: Vec<&FieldCandidate> = candidates.iter().collect();
		derive_layout(shape, &candidates)
	}

	fn packed_of(descriptor: &LayoutDescriptor) -> &PackedDescriptor {
		match &descriptor.layout {
			RowLayout::Packed(packed) => packed,
			RowLayout::Document => panic!("a document layout"),
		}
	}

	fn relation(from: &[&str], to: &[&str], lightweight: bool) -> TableType {
		TableType::Relation(Relation {
			from: from.iter().map(|table| TableName::new(*table)).collect(),
			to: to.iter().map(|table| TableName::new(*table)).collect(),
			enforced: false,
			lightweight,
		})
	}

	/// One definition of every shape a column takes.
	fn every_shape() -> Vec<StoredFieldDefinition> {
		let mut flexible = definition("flex", Some("object"));
		flexible.flexible = true;
		let mut computed = definition("c", Some("int"));
		computed.computed = Some(ExprText::from_raw("1"));
		vec![
			definition("name", Some("string")),
			definition("age", Some("option<int>")),
			definition("active", Some("bool")),
			definition("score", Some("float | null")),
			definition("both", Some("option<int | null>")),
			definition("address", Some("object")),
			definition("address.city", Some("string")),
			definition("meta", Some("object")),
			flexible,
			definition("flex.x", Some("string")),
			definition("tags", Some("array<string>")),
			definition("tags[*]", Some("string")),
			definition("status", Some("'a' | 'b'")),
			definition("owner", Some("record<user | org>")),
			definition("created", Some("datetime")),
			definition("orphan.x", Some("string")),
			definition("loose", None),
			definition("id", Some("string")),
			computed,
			definition("c.under", Some("string")),
		]
	}

	fn col(path: &[&str], kind: PhysicalKind, presence: Presence, location: Location) -> Column {
		Column {
			path: path.iter().map(|segment| (*segment).to_owned()).collect(),
			kind,
			presence,
			location,
			role: ColumnRole::Field,
			literals: Vec::new(),
			tables: Vec::new(),
		}
	}

	fn expected_every_shape() -> PackedDescriptor {
		let mut owner =
			col(&["owner"], PhysicalKind::RecordKey, Presence::Required, Location::Var(6));
		owner.tables = vec![TableName::new("user"), TableName::new("org")];
		let mut status =
			col(&["status"], PhysicalKind::LiteralCode(1), Presence::Required, Location::Fixed(34));
		status.literals = vec![Value::String(Strand::new("a")), Value::String(Strand::new("b"))];
		PackedDescriptor {
			columns: vec![
				col(&["active"], PhysicalKind::Bool, Presence::Required, Location::Bit(0)),
				col(&["address"], PhysicalKind::Object, Presence::Required, Location::None),
				col(
					&["address", "city"],
					PhysicalKind::String,
					Presence::Required,
					Location::Var(0),
				),
				col(
					&["age"],
					PhysicalKind::I64,
					Presence::Optional {
						bit: 0,
					},
					Location::Fixed(2),
				),
				col(
					&["both"],
					PhysicalKind::I64,
					Presence::OptionalNullable {
						bit: 1,
					},
					Location::Fixed(10),
				),
				col(&["created"], PhysicalKind::Datetime, Presence::Required, Location::Fixed(18)),
				col(&["flex"], PhysicalKind::Value, Presence::Required, Location::Var(1)),
				col(
					&["loose"],
					PhysicalKind::Value,
					Presence::OptionalNullable {
						bit: 3,
					},
					Location::Var(2),
				),
				col(&["meta"], PhysicalKind::Value, Presence::Required, Location::Var(3)),
				col(&["name"], PhysicalKind::String, Presence::Required, Location::Var(4)),
				col(
					&["orphan"],
					PhysicalKind::Value,
					Presence::OptionalNullable {
						bit: 5,
					},
					Location::Var(5),
				),
				owner,
				col(
					&["score"],
					PhysicalKind::F64,
					Presence::Nullable {
						bit: 7,
					},
					Location::Fixed(26),
				),
				status,
				col(&["tags"], PhysicalKind::Value, Presence::Required, Location::Var(7)),
			],
			presence_bits: 8,
			bool_bits: 1,
			fixed_size: 35,
			var_count: 8,
		}
	}

	#[test]
	fn a_table_that_may_not_pack_derives_the_document_layout() {
		let fields = vec![definition("name", Some("string"))];
		let document = LayoutDescriptor::document(t());
		let normal = TableType::Normal;
		let mut shape = schemafull(&normal);
		shape.schemafull = false;
		assert_eq!(derive(shape, &fields), document);
		let mut shape = schemafull(&normal);
		shape.drop = true;
		assert_eq!(derive(shape, &fields), document);
		let mut shape = schemafull(&normal);
		shape.view = true;
		assert_eq!(derive(shape, &fields), document);
		let lightweight = relation(&["a"], &["b"], true);
		assert!(!schemafull(&lightweight).may_pack());
		assert_eq!(derive(schemafull(&lightweight), &fields), document);
		assert!(schemafull(&relation(&["a"], &["b"], false)).may_pack());
	}

	#[test]
	fn a_schemafull_table_with_no_field_packs_no_columns() {
		let derived = derive(schemafull(&TableType::Normal), &[]);
		assert_eq!(
			derived.layout,
			RowLayout::Packed(PackedDescriptor {
				columns: Vec::new(),
				presence_bits: 0,
				bool_bits: 0,
				fixed_size: 0,
				var_count: 0,
			})
		);
	}

	#[test]
	fn every_field_shape_derives_its_documented_column() {
		let derived = derive(schemafull(&TableType::Normal), &every_shape());
		assert_eq!(derived.table, t());
		assert_eq!(packed_of(&derived), &expected_every_shape());
		assert_eq!(derived.validate(), Ok(()));
	}

	#[test]
	fn the_derivation_does_not_depend_on_the_order_of_the_definitions() {
		let fields = every_shape();
		let expected = derive(schemafull(&TableType::Normal), &fields);
		let mut reversed = fields.clone();
		reversed.reverse();
		assert_eq!(derive(schemafull(&TableType::Normal), &reversed), expected);
		for shift in 1..fields.len() {
			let mut rotated = fields.clone();
			rotated.rotate_left(shift);
			assert_eq!(
				derive(schemafull(&TableType::Normal), &rotated),
				expected,
				"rotated by {shift}"
			);
		}
	}

	/// The bytes of the layout [`every_shape`] derives: a release that derives other bytes from
	/// the same definitions mints a generation another release would not.
	const FROZEN_EVERY_SHAPE: &[u8] = &[
		254, 0, 128, 128, 16, 39, 2, 0, 0, 0, 3, 0, 2, 7, 116, 4, 39, 1, 0, 0, 0, 2, 39, 5, 0, 0,
		0, 190, 2, 192, 2, 194, 2, 196, 2, 2, 21, 15, 0, 0, 0, 42, 0, 85, 0, 134, 0, 177, 0, 221,
		0, 8, 1, 48, 1, 93, 1, 133, 1, 173, 1, 219, 1, 19, 2, 64, 2, 116, 2, 38, 5, 0, 11, 17, 23,
		29, 2, 20, 1, 0, 7, 97, 99, 116, 105, 118, 101, 4, 38, 1, 0, 1, 0, 6, 38, 1, 0, 1, 0, 8,
		38, 1, 0, 1, 128, 10, 38, 1, 0, 1, 0, 38, 5, 0, 12, 18, 24, 30, 2, 20, 1, 0, 7, 97, 100,
		100, 114, 101, 115, 115, 4, 38, 1, 0, 34, 0, 6, 38, 1, 0, 1, 0, 8, 38, 1, 0, 4, 0, 10, 38,
		1, 0, 1, 0, 38, 5, 0, 18, 24, 30, 36, 2, 20, 2, 0, 8, 7, 97, 100, 100, 114, 101, 115, 115,
		7, 99, 105, 116, 121, 4, 38, 1, 0, 23, 0, 6, 38, 1, 0, 1, 0, 8, 38, 1, 0, 3, 128, 10, 38,
		1, 0, 1, 0, 38, 5, 0, 8, 14, 24, 30, 2, 20, 1, 0, 7, 97, 103, 101, 4, 38, 1, 0, 5, 0, 6,
		38, 1, 0, 2, 38, 1, 0, 2, 128, 8, 38, 1, 0, 2, 130, 10, 38, 1, 0, 1, 0, 38, 5, 0, 9, 15,
		25, 31, 2, 20, 1, 0, 7, 98, 111, 116, 104, 4, 38, 1, 0, 5, 0, 6, 38, 1, 0, 4, 38, 1, 0, 2,
		129, 8, 38, 1, 0, 2, 138, 10, 38, 1, 0, 1, 0, 38, 5, 0, 12, 18, 24, 30, 2, 20, 1, 0, 7, 99,
		114, 101, 97, 116, 101, 100, 4, 38, 1, 0, 17, 0, 6, 38, 1, 0, 1, 0, 8, 38, 1, 0, 2, 146,
		10, 38, 1, 0, 1, 0, 38, 5, 0, 9, 15, 21, 27, 2, 20, 1, 0, 7, 102, 108, 101, 120, 4, 38, 1,
		0, 27, 0, 6, 38, 1, 0, 1, 0, 8, 38, 1, 0, 3, 129, 10, 38, 1, 0, 1, 0, 38, 5, 0, 10, 16, 26,
		32, 2, 20, 1, 0, 7, 108, 111, 111, 115, 101, 4, 38, 1, 0, 27, 0, 6, 38, 1, 0, 4, 38, 1, 0,
		2, 131, 8, 38, 1, 0, 3, 130, 10, 38, 1, 0, 1, 0, 38, 5, 0, 9, 15, 21, 27, 2, 20, 1, 0, 7,
		109, 101, 116, 97, 4, 38, 1, 0, 27, 0, 6, 38, 1, 0, 1, 0, 8, 38, 1, 0, 3, 131, 10, 38, 1,
		0, 1, 0, 38, 5, 0, 9, 15, 21, 27, 2, 20, 1, 0, 7, 110, 97, 109, 101, 4, 38, 1, 0, 23, 0, 6,
		38, 1, 0, 1, 0, 8, 38, 1, 0, 3, 132, 10, 38, 1, 0, 1, 0, 38, 5, 0, 11, 17, 27, 33, 2, 20,
		1, 0, 7, 111, 114, 112, 104, 97, 110, 4, 38, 1, 0, 27, 0, 6, 38, 1, 0, 4, 38, 1, 0, 2, 133,
		8, 38, 1, 0, 3, 133, 10, 38, 1, 0, 1, 0, 38, 6, 0, 10, 16, 22, 28, 34, 2, 20, 1, 0, 7, 111,
		119, 110, 101, 114, 4, 38, 1, 0, 25, 0, 6, 38, 1, 0, 1, 0, 8, 38, 1, 0, 3, 134, 10, 38, 1,
		0, 1, 0, 14, 20, 2, 0, 5, 7, 117, 115, 101, 114, 7, 111, 114, 103, 38, 5, 0, 10, 16, 26,
		32, 2, 20, 1, 0, 7, 115, 99, 111, 114, 101, 4, 38, 1, 0, 13, 0, 6, 38, 1, 0, 3, 38, 1, 0,
		2, 135, 8, 38, 1, 0, 2, 154, 10, 38, 1, 0, 1, 0, 38, 6, 0, 11, 17, 23, 29, 35, 2, 20, 1, 0,
		7, 115, 116, 97, 116, 117, 115, 4, 38, 1, 0, 22, 129, 6, 38, 1, 0, 1, 0, 8, 38, 1, 0, 2,
		162, 10, 38, 1, 0, 1, 0, 12, 20, 2, 0, 2, 7, 97, 7, 98, 38, 5, 0, 9, 15, 21, 27, 2, 20, 1,
		0, 7, 116, 97, 103, 115, 4, 38, 1, 0, 27, 0, 6, 38, 1, 0, 1, 0, 8, 38, 1, 0, 3, 135, 10,
		38, 1, 0, 1, 0, 4, 136, 6, 129, 8, 163, 10, 136,
	];

	#[test]
	fn the_derived_layout_is_its_frozen_bytes() {
		check_frozen(&derive(schemafull(&TableType::Normal), &every_shape()), FROZEN_EVERY_SHAPE);
	}

	#[test]
	fn a_path_defined_twice_holds_any_value() {
		let fields = vec![definition("name", Some("string")), definition("name", Some("int"))];
		let derived = derive(schemafull(&TableType::Normal), &fields);
		assert_eq!(
			packed_of(&derived).columns,
			[col(
				&["name"],
				PhysicalKind::Value,
				Presence::OptionalNullable {
					bit: 0
				},
				Location::Var(0)
			)]
		);
	}

	#[test]
	fn the_ends_of_a_relation_are_key_columns_unless_defined() {
		let derived = derive(schemafull(&relation(&["a"], &["b", "c", "b"], false)), &[]);
		let mut from = col(&["in"], PhysicalKind::RecordKey, Presence::Required, Location::Var(0));
		from.tables = vec![TableName::new("a")];
		let mut to = col(&["out"], PhysicalKind::RecordKey, Presence::Required, Location::Var(1));
		to.tables = vec![TableName::new("b"), TableName::new("c")];
		assert_eq!(packed_of(&derived).columns, [from, to.clone()]);

		let derived = derive(schemafull(&relation(&[], &[], false)), &[]);
		assert_eq!(
			packed_of(&derived).columns,
			[
				col(&["in"], PhysicalKind::Value, Presence::Required, Location::Var(0)),
				col(&["out"], PhysicalKind::Value, Presence::Required, Location::Var(1)),
			]
		);

		let fields = vec![definition("in", Some("string"))];
		let derived = derive(schemafull(&relation(&["a"], &["b", "c"], false)), &fields);
		assert_eq!(
			packed_of(&derived).columns,
			[col(&["in"], PhysicalKind::String, Presence::Required, Location::Var(0)), to]
		);
	}

	#[test]
	fn a_literal_union_wider_than_a_byte_takes_a_two_byte_code() {
		let union: Vec<String> = (0..=256).map(|i| i.to_string()).collect();
		let fields = vec![definition("n", Some(&union.join(" | ")))];
		let derived = derive(schemafull(&TableType::Normal), &fields);
		let [column] = packed_of(&derived).columns.as_slice() else {
			panic!("one column");
		};
		assert_eq!(column.kind, PhysicalKind::LiteralCode(2));
		assert_eq!(column.literals.len(), 257);
	}

	#[test]
	fn a_literal_repeated_in_a_union_is_listed_once() {
		let fields = vec![definition("n", Some("1 | 2 | 1"))];
		let derived = derive(schemafull(&TableType::Normal), &fields);
		let [column] = packed_of(&derived).columns.as_slice() else {
			panic!("one column");
		};
		assert_eq!(column.kind, PhysicalKind::LiteralCode(1));
		assert_eq!(column.literals.len(), 2);
	}

	#[test]
	fn a_union_of_mixed_kinds_holds_any_value() {
		for kind in [
			"int | string",
			"record | record<a>",
			"'a' | int",
			"array<int>",
			"geometry<point | line>",
		] {
			let fields = vec![definition("v", Some(kind))];
			let derived = derive(schemafull(&TableType::Normal), &fields);
			assert_eq!(packed_of(&derived).columns[0].kind, PhysicalKind::Value, "{kind}");
		}
		let fields = vec![definition("p", Some("geometry<point>"))];
		let derived = derive(schemafull(&TableType::Normal), &fields);
		assert_eq!(packed_of(&derived).columns[0].kind, PhysicalKind::Point);
	}
}
