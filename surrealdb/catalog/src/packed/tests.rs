//! Tests of the packed-row writer and its two readers.
//!
//! * A seeded generator of descriptors and records covers every physical kind and presence, nested
//!   object nodes, open nodes and their remainders, the datetime and duration escapes, and every
//!   end-offset width. Each record [`pack`] writes decodes whole to itself, packs again to the same
//!   bytes, and reads field by field, through [`PackedRow::field`] and through a [`FieldPlan`], to
//!   what the whole decode holds.
//! * Mutations of every generated row hold the readers to one contract: a row that decodes whole
//!   reads field by field to the same values, passes [`check_packed`], and is the one packed form
//!   of what it decodes to. No field read refuses what the whole decode accepts, or answers
//!   differently.
//! * Targeted corruptions are refused by the whole decode and by every read of a field whose bytes
//!   they corrupt.
//! * [`fixtures`] freezes the bytes of a handful of rows and of their descriptors.
//!
//! A [`FieldRef::Stored`] or [`FieldRef::RecordId`] carries bytes that the reader that walks them
//! validates, so a field read here is the read and that walk: [`read`].

mod fixtures;

use std::sync::Arc;

use chrono::{DateTime, NaiveDate, Utc};
use surrealdb_types::Decimal;

use super::kind::widen_f32;
use super::{
	DATETIME_ABOVE, DATETIME_BELOW, DURATION_ESCAPE, FieldPlan, FieldRef, PackedError, PackedRow,
	Packing, check_packed, pack,
};
use crate::Generation;
use crate::layout::{
	Column, ColumnRole, LayoutDescriptor, Location, PackedDescriptor, PhysicalKind, Presence,
	RowLayout,
};
use crate::record::{RowFlags, StoredRecord};
use crate::val::stored::{self, varint};
use crate::val::{self, Array, Number, Object, RecordId, RecordIdKey, Strand, TableName, Value};

// A deterministic generator

/// A xorshift64* generator: the same seed gives the same descriptors and records on every run.
pub(super) struct Rng(u64);

impl Rng {
	pub(super) fn new(seed: u64) -> Self {
		Self((seed ^ 0x9E37_79B9_7F4A_7C15) | 1)
	}

	pub(super) fn next(&mut self) -> u64 {
		let mut x = self.0;
		x ^= x >> 12;
		x ^= x << 25;
		x ^= x >> 27;
		self.0 = x;
		x.wrapping_mul(0x2545_F491_4F6C_DD1D)
	}

	/// A number below `n`, which is not zero.
	pub(super) fn below(&mut self, n: u64) -> u64 {
		self.next() % n
	}

	pub(super) fn index(&mut self, n: usize) -> usize {
		(self.next() % n as u64) as usize
	}

	pub(super) fn one_in(&mut self, n: u64) -> bool {
		self.below(n) == 0
	}
}

/// The seeds the generated tests run.
const SEEDS: &[u64] = &[1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 0xDEAD_BEEF];

// Building descriptors

/// A column's presence, before its bits are assigned.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Pres {
	Required,
	Optional,
	Nullable,
	Both,
}

impl Pres {
	const ALL: [Self; 4] = [Self::Required, Self::Optional, Self::Nullable, Self::Both];

	fn absent(self) -> bool {
		matches!(self, Self::Optional | Self::Both)
	}

	fn null(self) -> bool {
		matches!(self, Self::Nullable | Self::Both)
	}
}

/// A column of a descriptor, or an object node and the columns under it.
#[derive(Clone, Debug)]
pub(super) enum Spec {
	Leaf {
		kind: PhysicalKind,
		presence: Pres,
		literals: Vec<Value>,
		tables: Vec<TableName>,
	},
	Node {
		presence: Pres,
		open: bool,
		children: Vec<(String, Spec)>,
	},
}

impl Spec {
	fn presence(&self) -> Pres {
		match self {
			Self::Leaf {
				presence,
				..
			}
			| Self::Node {
				presence,
				..
			} => *presence,
		}
	}
}

/// A leaf of `kind`, with no literals and no tables.
pub(super) fn leaf(kind: PhysicalKind, presence: Pres) -> Spec {
	Spec::Leaf {
		kind,
		presence,
		literals: Vec::new(),
		tables: Vec::new(),
	}
}

/// An object node over `children`, which must ascend by name.
pub(super) fn node(presence: Pres, open: bool, children: Vec<(&str, Spec)>) -> Spec {
	Spec::Node {
		presence,
		open,
		children: named(children),
	}
}

pub(super) fn named(children: Vec<(&str, Spec)>) -> Vec<(String, Spec)> {
	children.into_iter().map(|(name, spec)| (name.to_owned(), spec)).collect()
}

#[derive(Default)]
struct Builder {
	columns: Vec<Column>,
	presence: u32,
	bools: u32,
	vars: u32,
}

impl Builder {
	fn presence(&mut self, presence: Pres) -> Presence {
		let bit = self.presence;
		match presence {
			Pres::Required => Presence::Required,
			Pres::Optional => {
				self.presence += 1;
				Presence::Optional {
					bit,
				}
			}
			Pres::Nullable => {
				self.presence += 1;
				Presence::Nullable {
					bit,
				}
			}
			Pres::Both => {
				self.presence += 2;
				Presence::OptionalNullable {
					bit,
				}
			}
		}
	}

	fn var(&mut self) -> Location {
		self.vars += 1;
		Location::Var(self.vars - 1)
	}

	fn node(&mut self, path: &[String], open: bool, children: &[(String, Spec)]) {
		assert!(children.windows(2).all(|pair| pair[0].0 < pair[1].0), "children out of order");
		if open {
			let presence = self.presence(Pres::Optional);
			let location = self.var();
			self.columns.push(Column {
				path: path.to_vec(),
				kind: PhysicalKind::Value,
				presence,
				location,
				role: ColumnRole::Remainder,
				literals: Vec::new(),
				tables: Vec::new(),
			});
		}
		for (name, spec) in children {
			let mut child = path.to_vec();
			child.push(name.clone());
			match spec {
				Spec::Leaf {
					kind,
					presence,
					literals,
					tables,
				} => {
					let presence = self.presence(*presence);
					let location = if *kind == PhysicalKind::Bool {
						self.bools += 1;
						Location::Bit(self.bools - 1)
					} else if kind.is_variable() {
						self.var()
					} else {
						// Assigned once the bit regions' sizes are known.
						Location::Fixed(0)
					};
					self.columns.push(Column {
						path: child,
						kind: *kind,
						presence,
						location,
						role: ColumnRole::Field,
						literals: literals.clone(),
						tables: tables.clone(),
					});
				}
				Spec::Node {
					presence,
					open,
					children,
				} => {
					let presence = self.presence(*presence);
					self.columns.push(Column {
						path: child.clone(),
						kind: PhysicalKind::Object,
						presence,
						location: Location::None,
						role: ColumnRole::Field,
						literals: Vec::new(),
						tables: Vec::new(),
					});
					self.node(&child, *open, children);
				}
			}
		}
	}
}

/// The canonical descriptor of a root over `children`, open when `open`: bits, slots and var
/// indexes dense in column order.
pub(super) fn descriptor(open: bool, children: &[(String, Spec)]) -> PackedDescriptor {
	let mut builder = Builder::default();
	builder.node(&[], open, children);
	let mut offset = builder.presence.div_ceil(8) + builder.bools.div_ceil(8);
	for column in &mut builder.columns {
		if let Location::Fixed(at) = &mut column.location {
			*at = offset;
			offset += column.kind.fixed_width().expect("a fixed kind");
		}
	}
	let descriptor = PackedDescriptor {
		columns: builder.columns,
		presence_bits: builder.presence,
		bool_bits: builder.bools,
		fixed_size: offset,
		var_count: builder.vars,
	};
	descriptor.validate().expect("a built descriptor is valid");
	descriptor
}

pub(super) fn layout(packed: PackedDescriptor) -> Arc<LayoutDescriptor> {
	Arc::new(LayoutDescriptor {
		table: TableName::new("t"),
		layout: RowLayout::Packed(packed),
		migration: None,
		migrated: None,
	})
}

pub(super) fn packed_of(layout: &LayoutDescriptor) -> &PackedDescriptor {
	match &layout.layout {
		RowLayout::Packed(packed) => packed,
		RowLayout::Document => panic!("a document descriptor"),
	}
}

// Building records

pub(super) fn object(entries: Vec<(&str, Value)>) -> Object {
	let mut object = Object::default();
	for (key, value) in entries {
		object.0.insert(Strand::new(key), value);
	}
	object
}

pub(super) fn int(v: i64) -> Value {
	Value::Number(Number::Int(v))
}

pub(super) fn float(v: f64) -> Value {
	Value::Number(Number::Float(v))
}

pub(super) fn decimal(v: Decimal) -> Value {
	Value::Number(Number::Decimal(v))
}

pub(super) fn string(v: &str) -> Value {
	Value::String(Strand::new(v))
}

pub(super) fn bytes(v: &[u8]) -> Value {
	Value::Bytes(val::Bytes::from(v.to_vec()))
}

pub(super) fn datetime(v: DateTime<Utc>) -> Value {
	Value::Datetime(val::Datetime(v))
}

pub(super) fn duration(v: std::time::Duration) -> Value {
	Value::Duration(val::Duration(v))
}

pub(super) fn uuid(v: u128) -> Value {
	Value::Uuid(val::Uuid(uuid::Uuid::from_u128(v)))
}

pub(super) fn point(x: f64, y: f64) -> Value {
	Value::Geometry(val::Geometry::Point((x, y).into()))
}

pub(super) fn record(table: &str, key: RecordIdKey) -> Value {
	Value::RecordId(RecordId {
		table: TableName::new(table),
		key,
	})
}

/// A datetime from whole seconds and nanoseconds of the second.
pub(super) fn at(seconds: i64, nanoseconds: u32) -> DateTime<Utc> {
	DateTime::from_timestamp(seconds, nanoseconds).expect("a datetime in range")
}

/// The leap second at the end of 2016, whose sub-second part is a second or more.
pub(super) fn leap_second() -> DateTime<Utc> {
	NaiveDate::from_ymd_opt(2016, 12, 31)
		.and_then(|date| date.and_hms_nano_opt(23, 59, 59, 1_500_000_000))
		.expect("a leap second")
		.and_utc()
}

/// `data` without the root `id`, which no row holds.
fn without_id(data: &Object) -> Object {
	let mut kept = data.clone();
	kept.0.remove("id");
	kept
}

pub(super) fn same(a: &Object, b: &Object) -> bool {
	val::same_value(&Value::Object(a.clone()), &Value::Object(b.clone()))
}

pub(super) fn hex(bytes: &[u8]) -> String {
	bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

// Reading fields

/// A field as a read answers it, with every byte it carries walked into a value.
#[derive(Clone, Debug)]
pub(super) enum Read {
	Absent,
	Null,
	Object,
	Value(Value),
}

/// Reads `index` through `field` and walks what it carries. Any refusal, of the read or the walk,
/// is an `Err`.
pub(super) fn read(
	column: &Column,
	field: Result<FieldRef<'_>, PackedError>,
) -> Result<Read, String> {
	let field = field.map_err(|error| format!("{error:?}"))?;
	let walk = |error: stored::Error| format!("walking the field's bytes: {error:?}");
	Ok(match field {
		FieldRef::Absent => Read::Absent,
		FieldRef::Null => Read::Null,
		FieldRef::Object => Read::Object,
		FieldRef::Bool(v) => Read::Value(Value::Bool(v)),
		FieldRef::Int(v) => Read::Value(int(v)),
		FieldRef::Float(v) => Read::Value(float(v)),
		FieldRef::Decimal(v) => Read::Value(decimal(v)),
		FieldRef::Uuid(v) => Read::Value(Value::Uuid(val::Uuid(v))),
		FieldRef::Datetime(v) => Read::Value(datetime(v)),
		FieldRef::Duration(v) => Read::Value(duration(v)),
		FieldRef::Point(x, y) => Read::Value(point(x, y)),
		FieldRef::String(v) => Read::Value(string(v)),
		FieldRef::Bytes(v) => Read::Value(bytes(v)),
		FieldRef::Literal(v) => Read::Value(v.clone()),
		FieldRef::RecordId(table, key) => {
			let key = stored::decode_key_nested(key, column.path.len().saturating_add(1))
				.map_err(walk)?;
			Read::Value(Value::RecordId(RecordId {
				table: table.clone(),
				key,
			}))
		}
		FieldRef::Stored(v) => Read::Value(v.decode().map_err(walk)?),
	})
}

/// The names of the direct child fields of the node at `path`.
fn child_names<'d>(packed: &'d PackedDescriptor, path: &[String]) -> Vec<&'d str> {
	packed
		.columns
		.iter()
		.filter(|column| {
			column.role == ColumnRole::Field
				&& column.path.len() == path.len() + 1
				&& column.path.starts_with(path)
		})
		.filter_map(|column| column.path.last().map(String::as_str))
		.collect()
}

/// What a whole decode, `decoded`, holds of column `index`: the value at a field's path, or the
/// keys of a remainder's node that have no column of their own. `None` is absent.
pub(super) fn expected(packed: &PackedDescriptor, decoded: &Object, index: usize) -> Option<Value> {
	let column = &packed.columns[index];
	let (walk, last) = match column.role {
		ColumnRole::Field => {
			let (last, walk) = column.path.split_last().expect("a field has a path");
			(walk, Some(last))
		}
		ColumnRole::Remainder => (column.path.as_slice(), None),
	};
	let mut node = decoded;
	for key in walk {
		match node.get(key.as_str()) {
			Some(Value::Object(inner)) => node = inner,
			_ => return None,
		}
	}
	match last {
		Some(key) => node.get(key.as_str()).cloned(),
		None => {
			let names = child_names(packed, &column.path);
			let mut rest = Object::default();
			for (key, value) in node.iter() {
				if !names.contains(&key.as_str()) {
					rest.0.insert(key.clone(), value.clone());
				}
			}
			(!rest.is_empty()).then_some(Value::Object(rest))
		}
	}
}

/// Whether a read agrees with what the whole decode holds.
pub(super) fn agrees(read: &Read, expected: Option<&Value>) -> bool {
	match (read, expected) {
		(Read::Absent, None) => true,
		(Read::Null, Some(Value::Null)) => true,
		(Read::Object, Some(Value::Object(_))) => true,
		(Read::Value(value), Some(expected)) => val::same_value(value, expected),
		_ => false,
	}
}

/// Reads every column of `row` both ways, and checks each read against the whole decode.
pub(super) fn check_fields(
	layout: &Arc<LayoutDescriptor>,
	row: &PackedRow<'_>,
	decoded: &Object,
	bytes: &[u8],
) {
	check_fields_but(layout, row, decoded, bytes, &[]);
}

/// [`check_fields`], but for the columns `skip`.
fn check_fields_but(
	layout: &Arc<LayoutDescriptor>,
	row: &PackedRow<'_>,
	decoded: &Object,
	bytes: &[u8],
	skip: &[usize],
) {
	let packed = packed_of(layout);
	for (index, column) in packed.columns.iter().enumerate() {
		if skip.contains(&index) {
			continue;
		}
		let want = expected(packed, decoded, index);
		let plan = FieldPlan::new(layout, index).expect("a plan of a packed descriptor");
		assert_eq!(plan.column(), index);
		for (how, got) in [
			("field", read(column, row.field(index))),
			("field_with", read(column, row.field_with(&plan))),
		] {
			match got {
				Ok(got) if agrees(&got, want.as_ref()) => {}
				got => panic!(
					"{how}({index}) of {:?} disagrees with the whole decode\nread     {got:?}\n\
					 expected {want:?}\nrow      {}\ndecoded  {decoded:?}",
					column.path,
					hex(bytes)
				),
			}
		}
	}
}

// The generator

/// Field names, in byte order. `id` is not a field of the root.
const NAMES: &[&str] = &["a", "b", "c", "dd", "e", "f", "id", "k", "m", "q", "s", "x", "z", "é"];

/// Keys no generated field has, for remainders.
const EXTRA: &[&str] = &["extra", "o", "u", "w", "ä"];

/// Every physical kind but `Object`, with a placeholder width where the kind takes one.
const LEAF_KINDS: &[PhysicalKind] = &[
	PhysicalKind::Bool,
	PhysicalKind::I8,
	PhysicalKind::I16,
	PhysicalKind::I32,
	PhysicalKind::I64,
	PhysicalKind::U8,
	PhysicalKind::U16,
	PhysicalKind::U32,
	PhysicalKind::U64,
	PhysicalKind::F32,
	PhysicalKind::F64,
	PhysicalKind::Decimal16,
	PhysicalKind::Fixed(0),
	PhysicalKind::Uuid,
	PhysicalKind::Datetime,
	PhysicalKind::DatetimeWide,
	PhysicalKind::Duration,
	PhysicalKind::Point,
	PhysicalKind::LiteralCode(0),
	PhysicalKind::String,
	PhysicalKind::Bytes,
	PhysicalKind::RecordKey,
	PhysicalKind::DecimalVar,
	PhysicalKind::Value,
];

/// A name for each kind, by an exhaustive match, so that a kind added to [`PhysicalKind`] does not
/// compile here until the coverage of [`LEAF_KINDS`] names it.
fn kind_name(kind: PhysicalKind) -> &'static str {
	match kind {
		PhysicalKind::Bool => "Bool",
		PhysicalKind::I8 => "I8",
		PhysicalKind::I16 => "I16",
		PhysicalKind::I32 => "I32",
		PhysicalKind::I64 => "I64",
		PhysicalKind::U8 => "U8",
		PhysicalKind::U16 => "U16",
		PhysicalKind::U32 => "U32",
		PhysicalKind::U64 => "U64",
		PhysicalKind::F32 => "F32",
		PhysicalKind::F64 => "F64",
		PhysicalKind::Decimal16 => "Decimal16",
		PhysicalKind::Fixed(_) => "Fixed",
		PhysicalKind::Uuid => "Uuid",
		PhysicalKind::Datetime => "Datetime",
		PhysicalKind::DatetimeWide => "DatetimeWide",
		PhysicalKind::Duration => "Duration",
		PhysicalKind::Point => "Point",
		PhysicalKind::LiteralCode(_) => "LiteralCode",
		PhysicalKind::String => "String",
		PhysicalKind::Bytes => "Bytes",
		PhysicalKind::RecordKey => "RecordKey",
		PhysicalKind::DecimalVar => "DecimalVar",
		PhysicalKind::Value => "Value",
		PhysicalKind::Object => "Object",
	}
}

/// Scalars a literal union may hold, all distinct.
fn literal_pool() -> Vec<Value> {
	vec![
		string("red"),
		string("green"),
		int(1),
		float(1.0),
		int(-2),
		Value::Bool(true),
		Value::Bool(false),
		duration(std::time::Duration::from_secs(1)),
		decimal(Decimal::new(150, 2)),
		decimal(Decimal::new(15, 1)),
	]
}

/// The 300 literals of a 2-byte code: more than one byte holds.
pub(super) fn wide_literals() -> Vec<Value> {
	(0..300).map(|i| string(&format!("l{i}"))).collect()
}

const TABLES: &[&str] = &["a", "b", "person"];

fn gen_leaf(rng: &mut Rng, presence: Pres) -> Spec {
	let mut kind = LEAF_KINDS[rng.index(LEAF_KINDS.len())];
	let mut literals = Vec::new();
	let mut tables = Vec::new();
	match &mut kind {
		PhysicalKind::Fixed(width) => {
			*width = match rng.below(4) {
				0 => 1,
				1 => 16,
				2 => 4096,
				_ => 1 + rng.below(40) as u32,
			}
		}
		PhysicalKind::LiteralCode(width) => {
			if rng.one_in(3) {
				*width = 2;
				literals = wide_literals();
			} else {
				*width = 1 + rng.below(2) as u32;
				let mut pool = literal_pool();
				let n = 1 + rng.index(pool.len());
				while pool.len() > n {
					pool.remove(rng.index(pool.len()));
				}
				literals = pool;
			}
		}
		PhysicalKind::RecordKey => {
			for table in TABLES {
				if tables.is_empty() || rng.one_in(2) {
					tables.push(TableName::new(*table));
				}
			}
		}
		_ => {}
	}
	Spec::Leaf {
		kind,
		presence,
		literals,
		tables,
	}
}

fn gen_children(rng: &mut Rng, depth: usize) -> Vec<(String, Spec)> {
	let most = if depth == 0 {
		10
	} else {
		5
	};
	let mut names: Vec<&str> =
		NAMES.iter().copied().filter(|name| depth > 0 || *name != "id").collect();
	let count = rng.index(most + 1).min(names.len());
	let mut chosen = Vec::new();
	for _ in 0..count {
		chosen.push(names.remove(rng.index(names.len())));
	}
	chosen.sort_unstable();
	chosen
		.into_iter()
		.map(|name| {
			let presence = Pres::ALL[rng.index(4)];
			let spec = if depth < 3 && rng.one_in(5) {
				Spec::Node {
					presence,
					open: rng.one_in(2),
					children: gen_children(rng, depth + 1),
				}
			} else {
				gen_leaf(rng, presence)
			};
			(name.to_owned(), spec)
		})
		.collect()
}

fn gen_int_in(rng: &mut Rng, lo: i64, hi: i64) -> i64 {
	match rng.below(5) {
		0 => lo,
		1 => hi,
		2 => 0,
		_ => {
			let span = (hi as i128 - lo as i128 + 1) as u128;
			(lo as i128 + (u128::from(rng.next()) % span) as i128) as i64
		}
	}
}

fn gen_decimal(rng: &mut Rng) -> Decimal {
	let hi = match rng.below(3) {
		0 => 0,
		1 => u32::MAX,
		_ => rng.next() as u32,
	};
	Decimal::from_parts(
		rng.next() as u32,
		rng.next() as u32,
		hi,
		rng.one_in(2),
		rng.below(29) as u32,
	)
}

fn gen_string(rng: &mut Rng) -> String {
	const CHARS: &[char] = &['a', 'z', ' ', 'é', '名', '🦀', '\0', '"'];
	let length = match rng.below(60) {
		0 => 300,
		1 => 70_000,
		_ => rng.index(12),
	};
	if length > 12 {
		return "x".repeat(length);
	}
	(0..length).map(|_| CHARS[rng.index(CHARS.len())]).collect()
}

fn gen_bytes(rng: &mut Rng, length: usize) -> Vec<u8> {
	(0..length).map(|_| rng.next() as u8).collect()
}

fn gen_datetime(rng: &mut Rng, wide: bool) -> DateTime<Utc> {
	match rng.below(10) {
		0 => DateTime::from_timestamp_nanos(DATETIME_BELOW),
		1 => DateTime::from_timestamp_nanos(DATETIME_ABOVE),
		2 => {
			at(-10_000_000_000 - rng.below(200_000_000_000) as i64, rng.below(1_000_000_000) as u32)
		}
		3 => {
			at(10_000_000_000 + rng.below(200_000_000_000) as i64, rng.below(1_000_000_000) as u32)
		}
		4 => DateTime::<Utc>::MIN_UTC,
		5 => DateTime::<Utc>::MAX_UTC,
		6 if wide => leap_second(),
		_ => DateTime::from_timestamp_nanos(rng.next() as i64),
	}
}

fn gen_duration(rng: &mut Rng) -> std::time::Duration {
	match rng.below(6) {
		0 => std::time::Duration::from_nanos(DURATION_ESCAPE),
		1 => std::time::Duration::MAX,
		2 => std::time::Duration::from_secs(rng.next()),
		_ => std::time::Duration::from_nanos(rng.next() >> rng.below(64)),
	}
}

fn gen_key(rng: &mut Rng) -> RecordIdKey {
	match rng.below(4) {
		0 => RecordIdKey::Number(rng.next() as i64),
		1 => RecordIdKey::String(Strand::new(gen_string(rng))),
		2 => RecordIdKey::Uuid(val::Uuid(uuid::Uuid::from_u64_pair(rng.next(), rng.next()))),
		_ => RecordIdKey::Array(Array(vec![int(rng.below(10) as i64), string("k")])),
	}
}

/// Any value but `NONE`, nested at most `depth` more levels.
fn gen_any(rng: &mut Rng, depth: usize) -> Value {
	match rng.below(if depth == 0 {
		12
	} else {
		14
	}) {
		0 => Value::Null,
		1 => Value::Bool(rng.one_in(2)),
		2 => int(rng.next() as i64),
		3 => float(f64::from_bits(rng.next())),
		4 => decimal(gen_decimal(rng)),
		5 => string(&gen_string(rng)),
		6 => {
			let length = rng.index(6);
			bytes(&gen_bytes(rng, length))
		}
		7 => duration(gen_duration(rng)),
		8 => datetime(gen_datetime(rng, true)),
		9 => uuid(u128::from(rng.next()) << 64 | u128::from(rng.next())),
		10 => record(TABLES[rng.index(TABLES.len())], gen_key(rng)),
		11 => point(f64::from_bits(rng.next()), f64::from_bits(rng.next())),
		12 => Value::Array(Array((0..rng.index(4)).map(|_| gen_any(rng, depth - 1)).collect())),
		_ => {
			let mut inner = Object::default();
			for _ in 0..rng.index(4) {
				let key = NAMES[rng.index(NAMES.len())];
				inner.0.insert(Strand::new(key), gen_any(rng, depth - 1));
			}
			Value::Object(inner)
		}
	}
}

/// A present value of a leaf. One in fifty does not fit the column.
fn gen_value(rng: &mut Rng, kind: PhysicalKind, literals: &[Value], tables: &[TableName]) -> Value {
	if rng.one_in(50) {
		return match kind {
			PhysicalKind::Bool => int(1),
			PhysicalKind::I8 => int(1_000),
			PhysicalKind::U8 | PhysicalKind::U16 | PhysicalKind::U32 | PhysicalKind::U64 => int(-1),
			PhysicalKind::F32 => float(0.1),
			PhysicalKind::Datetime => datetime(leap_second()),
			PhysicalKind::LiteralCode(_) => string("not a literal"),
			PhysicalKind::RecordKey => record("elsewhere", RecordIdKey::Number(1)),
			PhysicalKind::Value => Value::None,
			_ => Value::Bool(true),
		};
	}
	match kind {
		PhysicalKind::Bool => Value::Bool(rng.one_in(2)),
		PhysicalKind::I8 => int(gen_int_in(rng, i8::MIN.into(), i8::MAX.into())),
		PhysicalKind::I16 => int(gen_int_in(rng, i16::MIN.into(), i16::MAX.into())),
		PhysicalKind::I32 => int(gen_int_in(rng, i32::MIN.into(), i32::MAX.into())),
		PhysicalKind::I64 => int(gen_int_in(rng, i64::MIN, i64::MAX)),
		PhysicalKind::U8 => int(gen_int_in(rng, 0, u8::MAX.into())),
		PhysicalKind::U16 => int(gen_int_in(rng, 0, u16::MAX.into())),
		PhysicalKind::U32 => int(gen_int_in(rng, 0, u32::MAX.into())),
		PhysicalKind::U64 => int(gen_int_in(rng, 0, i64::MAX)),
		PhysicalKind::F32 => float(widen_f32(rng.next() as u32)),
		PhysicalKind::F64 => float(f64::from_bits(rng.next())),
		PhysicalKind::Decimal16 | PhysicalKind::DecimalVar => decimal(gen_decimal(rng)),
		PhysicalKind::Fixed(width) => bytes(&gen_bytes(rng, width as usize)),
		PhysicalKind::Uuid => uuid(u128::from(rng.next()) << 64 | u128::from(rng.next())),
		PhysicalKind::Datetime => datetime(gen_datetime(rng, false)),
		PhysicalKind::DatetimeWide => datetime(gen_datetime(rng, true)),
		PhysicalKind::Duration => duration(gen_duration(rng)),
		PhysicalKind::Point => point(f64::from_bits(rng.next()), f64::from_bits(rng.next())),
		PhysicalKind::LiteralCode(_) => literals[rng.index(literals.len())].clone(),
		PhysicalKind::String => string(&gen_string(rng)),
		PhysicalKind::Bytes => {
			let length = rng.index(20);
			bytes(&gen_bytes(rng, length))
		}
		PhysicalKind::RecordKey => {
			let table = &tables[rng.index(tables.len())];
			Value::RecordId(RecordId {
				table: table.clone(),
				key: gen_key(rng),
			})
		}
		PhysicalKind::Value => match rng.below(4) {
			0 => Value::Null,
			_ => gen_any(rng, 2),
		},
		PhysicalKind::Object => unreachable!("an object node is not a leaf"),
	}
}

/// A record for a node over `children`: each key absent, `NULL` or present as its presence
/// allows, one in forty in a state it does not, and, in an open node, keys that have no column.
fn gen_object(rng: &mut Rng, children: &[(String, Spec)], open: bool, root: bool) -> Object {
	let mut object = Object::default();
	for (name, spec) in children {
		let presence = spec.presence();
		let mut states = vec![2u8];
		if presence.absent() {
			states.push(0);
		}
		if presence.null() {
			states.push(1);
		}
		let state = if rng.one_in(40) {
			rng.below(3) as u8
		} else {
			states[rng.index(states.len())]
		};
		let value = match state {
			0 => continue,
			1 => Value::Null,
			_ => match spec {
				Spec::Leaf {
					kind,
					literals,
					tables,
					..
				} => gen_value(rng, *kind, literals, tables),
				Spec::Node {
					open,
					children,
					..
				} => Value::Object(gen_object(rng, children, *open, false)),
			},
		};
		object.0.insert(Strand::new(name), value);
	}
	if open && rng.one_in(2) {
		for _ in 0..1 + rng.index(3) {
			let key = EXTRA[rng.index(EXTRA.len())];
			object.0.insert(Strand::new(key), gen_any(rng, 2));
		}
	}
	if root && rng.one_in(3) {
		object.0.insert(Strand::new("id"), record("t", RecordIdKey::Number(1)));
	}
	object
}

/// What a generated run reached, so that a generator change that stops reaching a path fails.
#[derive(Default)]
struct Coverage {
	rows: usize,
	misfits: usize,
	kinds: std::collections::BTreeSet<&'static str>,
	remainders: usize,
	escapes: usize,
	widths: [usize; 3],
	mutants: usize,
	mutants_decoded: usize,
	mutants_misfit: usize,
}

/// Packs `data` as a row of `layout`, and checks every way of reading it back. Answers the row, or
/// `None` when the descriptor cannot hold the record.
fn round_trip(
	layout: &Arc<LayoutDescriptor>,
	data: &Object,
	generation: Generation,
	flags: RowFlags,
) -> Option<Vec<u8>> {
	let packed = packed_of(layout);
	let bytes = match pack(packed, generation, flags, data).expect("a storable record packs") {
		Packing::Packed(bytes) => bytes,
		Packing::Misfit => return None,
	};
	let stamp = generation.stamp();
	check_packed(&bytes, bytes[1], generation, packed).expect("a packed row checks");
	let record = StoredRecord::parse(&bytes).expect("parses").expect("in the v1 envelope");
	assert!(record.is_packed());
	assert_eq!(record.stamp(), stamp);
	assert_eq!(record.is_edge(), flags.is_edge());
	let row = PackedRow::new(&record, packed).expect("a packed row has its shape");
	assert_eq!(row.generation(), stamp);
	assert_eq!(row.is_edge(), flags.is_edge());
	let decoded = row.decode(None).unwrap_or_else(|error| {
		panic!("a packed row does not decode: {error:?}\nrow  {}\ndata {data:?}", hex(&bytes))
	});
	let kept = without_id(data);
	assert!(
		same(&decoded, &kept),
		"a packed row decodes to another record\nrow     {}\ndata    {kept:?}\ndecoded {decoded:?}",
		hex(&bytes)
	);
	// The id is merged in at its place in key order.
	let id = RecordId {
		table: TableName::new("t"),
		key: RecordIdKey::Number(1),
	};
	let mut with_id = kept;
	with_id.0.insert(Strand::new("id"), Value::RecordId(id.clone()));
	let merged = row.decode(Some(Value::RecordId(id.clone()))).expect("decodes with its id");
	assert!(same(&merged, &with_id), "{merged:?}\n{with_id:?}");
	let whole = record.decode_packed(packed, Some(id)).expect("decodes as a record");
	assert!(val::same_value(&whole.data, &Value::Object(with_id)));
	assert_eq!(whole.is_edge(), flags.is_edge());
	// One record has one packed form.
	match pack(packed, generation, flags, &decoded).expect("packs again") {
		Packing::Packed(again) => assert_eq!(hex(&again), hex(&bytes), "packed two ways"),
		Packing::Misfit => panic!("a decoded row does not pack again"),
	}
	check_fields(layout, &row, &decoded, &bytes);
	Some(bytes)
}

/// Checks a mutated row: when it decodes whole, every field reads to what the decode holds, it
/// passes [`check_packed`], and it is the packed form of what it decodes to.
fn check_mutant(layout: &Arc<LayoutDescriptor>, bytes: &[u8], coverage: &mut Coverage) {
	coverage.mutants += 1;
	let packed = packed_of(layout);
	let Ok(Some(record)) = StoredRecord::parse(bytes) else {
		return;
	};
	let Ok(row) = PackedRow::new(&record, packed) else {
		assert!(record.decode_packed(packed, None).is_err());
		return;
	};
	let Ok(decoded) = row.decode(None) else {
		return;
	};
	coverage.mutants_decoded += 1;
	check_fields(layout, &row, &decoded, bytes);
	let generation = record
		.generation()
		.expect("a stamp naming a generation")
		.expect("the generation the row was packed at");
	check_packed(bytes, bytes[1], generation, packed).unwrap_or_else(|error| {
		panic!("a row that decodes fails its check: {error:?}\nrow {}", hex(bytes))
	});
	match pack(packed, generation, RowFlags::edge(record.is_edge()), &decoded)
		.expect("a decoded record packs")
	{
		Packing::Packed(again) => assert_eq!(
			hex(&again),
			hex(bytes),
			"a row that decodes is not the packed form of what it decodes to\ndecoded {decoded:?}"
		),
		// A `U64` slot above `i64::MAX` reads as a decimal, which the writer does not narrow. No
		// other row that decodes is a record the descriptor cannot hold.
		Packing::Misfit => {
			let wide = packed.columns.iter().enumerate().any(|(index, column)| {
				column.kind == PhysicalKind::U64
					&& matches!(
						expected(packed, &decoded, index),
						Some(Value::Number(Number::Decimal(_)))
					)
			});
			assert!(
				wide,
				"a row that decodes does not pack again\nrow     {}\ndecoded {decoded:?}",
				hex(bytes)
			);
			coverage.mutants_misfit += 1;
		}
	}
}

fn mutate(rng: &mut Rng, layout: &Arc<LayoutDescriptor>, bytes: &[u8], coverage: &mut Coverage) {
	// The marker, the header and a generation below 128.
	let body = 3;
	for _ in 0..12 {
		let mut mutant = bytes.to_vec();
		match rng.below(8) {
			0..=3 if mutant.len() > body => {
				let at = body + rng.index(mutant.len() - body);
				mutant[at] ^= 1 << rng.below(8);
			}
			4 if mutant.len() > body => {
				let at = body + rng.index(mutant.len() - body);
				mutant[at] = rng.next() as u8;
			}
			5 => {
				mutant.truncate(rng.index(mutant.len()));
			}
			6 => {
				mutant.push(rng.next() as u8);
			}
			_ => {
				mutant[1] ^= match rng.below(3) {
					0 => StoredRecord::ESCAPES,
					1 => 1,
					_ => 2,
				};
			}
		}
		check_mutant(layout, &mutant, coverage);
	}
}

#[test]
fn generated_rows_round_trip_and_read_field_by_field() {
	let mut coverage = Coverage::default();
	for &seed in SEEDS {
		let mut rng = Rng::new(seed);
		for _ in 0..24 {
			let open = rng.one_in(2);
			let children = gen_children(&mut rng, 0);
			let layout = layout(descriptor(open, &children));
			let packed = packed_of(&layout);
			for _ in 0..6 {
				let data = gen_object(&mut rng, &children, open, true);
				let generation = Generation::new(1 + rng.below(100) as u16).expect("not zero");
				let flags = RowFlags::edge(rng.one_in(4));
				let Some(bytes) = round_trip(&layout, &data, generation, flags) else {
					coverage.misfits += 1;
					continue;
				};
				coverage.rows += 1;
				let record = StoredRecord::parse(&bytes).expect("parses").expect("v1");
				coverage.widths[usize::from(record.layout()) - 1] += 1;
				if record.has_escapes() {
					coverage.escapes += 1;
				}
				let row = PackedRow::new(&record, packed).expect("a row");
				for (index, column) in packed.columns.iter().enumerate() {
					let present =
						!matches!(row.field(index), Ok(FieldRef::Absent | FieldRef::Null));
					if present && column.role == ColumnRole::Remainder {
						coverage.remainders += 1;
					}
					if present && column.role == ColumnRole::Field {
						coverage.kinds.insert(kind_name(column.kind));
					}
				}
				// Mutating only the short rows bounds the time the test takes.
				if bytes.len() < 300 {
					mutate(&mut rng, &layout, &bytes, &mut coverage);
				}
			}
		}
	}
	let mut all: Vec<&str> = LEAF_KINDS.iter().map(|kind| kind_name(*kind)).collect();
	all.push("Object");
	for kind in all {
		assert!(coverage.kinds.contains(kind), "no generated row holds a present {kind}");
	}
	assert!(coverage.rows > 1_000, "only {} rows packed", coverage.rows);
	assert!(coverage.misfits > 0, "no generated record misfits");
	assert!(coverage.remainders > 100, "{} remainders", coverage.remainders);
	assert!(coverage.escapes > 100, "{} rows with a trailer", coverage.escapes);
	assert!(coverage.widths.iter().all(|&rows| rows > 0), "end widths {:?}", coverage.widths);
	assert!(coverage.mutants_decoded > 1_000, "{} mutants decoded", coverage.mutants_decoded);
	assert!(coverage.mutants > coverage.mutants_decoded);
}

// Hand-built rows

/// A descriptor that exercises the corruption tests: an open root, every slot shape, a nested
/// open node and the two escaping kinds.
///
/// Columns: 0 root remainder, 1 `a` I32, 2 `b` Bool, 3 `c` Datetime, 4 `d` Datetime, 5 `e`
/// Duration, 6 `node`, 7 `node` remainder, 8 `node.x` I64, 9 `node.y` String, 10 `s` String, 11
/// `v` Value. All optional.
fn corrupt_layout() -> Arc<LayoutDescriptor> {
	let o = Pres::Optional;
	layout(descriptor(
		true,
		&named(vec![
			("a", leaf(PhysicalKind::I32, o)),
			("b", leaf(PhysicalKind::Bool, o)),
			("c", leaf(PhysicalKind::Datetime, o)),
			("d", leaf(PhysicalKind::Datetime, o)),
			("e", leaf(PhysicalKind::Duration, o)),
			(
				"node",
				node(
					o,
					true,
					vec![("x", leaf(PhysicalKind::I64, o)), ("y", leaf(PhysicalKind::String, o))],
				),
			),
			("s", leaf(PhysicalKind::String, o)),
			("v", leaf(PhysicalKind::Value, o)),
		]),
	))
}

const A: usize = 1;
const B: usize = 2;
const C: usize = 3;
const D: usize = 4;
const E: usize = 5;
const NODE: usize = 6;
const NODE_X: usize = 8;
const NODE_Y: usize = 9;
const S: usize = 10;
const V: usize = 11;
const ROOT_REMAINDER: usize = 0;

/// Where the body of a row of generation 1 starts.
const BODY: usize = 3;

fn packed_row(layout: &Arc<LayoutDescriptor>, data: &Object) -> Vec<u8> {
	round_trip(layout, data, Generation::FIRST, RowFlags::NONE).expect("the record packs")
}

/// The index in a row's bytes of the fixed slot of `column`.
fn slot_at(layout: &LayoutDescriptor, column: usize) -> usize {
	match packed_of(layout).columns[column].location {
		Location::Fixed(offset) => BODY + offset as usize,
		location => panic!("column {column} is at {location:?}"),
	}
}

/// Flips presence bit `bit` of a row.
fn flip_presence(bytes: &mut [u8], bit: u32) {
	bytes[BODY + bit as usize / 8] ^= 1 << (bit % 8);
}

fn presence_bit(layout: &LayoutDescriptor, column: usize) -> u32 {
	match packed_of(layout).columns[column].presence {
		Presence::Optional {
			bit,
		}
		| Presence::Nullable {
			bit,
		}
		| Presence::OptionalNullable {
			bit,
		} => bit,
		Presence::Required => panic!("column {column} has no presence bit"),
	}
}

/// Where `needle` is in `haystack`, once.
fn find(haystack: &[u8], needle: &[u8]) -> usize {
	let found: Vec<usize> = haystack
		.windows(needle.len())
		.enumerate()
		.filter(|(_, window)| *window == needle)
		.map(|(at, _)| at)
		.collect();
	assert_eq!(found.len(), 1, "{needle:?} is in {} {} times", hex(haystack), found.len());
	found[0]
}

/// Asserts that `bytes` decode whole to a refusal, and that a read of each of `columns` refuses.
/// Answers the whole decode's refusal.
fn refused(layout: &Arc<LayoutDescriptor>, bytes: &[u8], columns: &[usize]) -> PackedError {
	let packed = packed_of(layout);
	let record = StoredRecord::parse(bytes).expect("parses").expect("v1");
	let row = PackedRow::new(&record, packed).expect("the row has its shape");
	let whole = row.decode(None).expect_err("the whole decode accepts a corrupt row");
	assert!(record.decode_packed(packed, None).is_err());
	for &column in columns {
		let plan = FieldPlan::new(layout, column).expect("a plan");
		let c = &packed.columns[column];
		let direct = read(c, row.field(column));
		let planned = read(c, row.field_with(&plan));
		assert!(
			direct.is_err(),
			"a read of column {column} accepts it: {direct:?}\nrow {}",
			hex(bytes)
		);
		assert!(planned.is_err(), "a planned read of column {column} accepts it: {planned:?}");
	}
	whole
}

/// Asserts that `bytes` are refused before any field is read.
fn refused_shape(layout: &Arc<LayoutDescriptor>, bytes: &[u8]) -> PackedError {
	let packed = packed_of(layout);
	let record = StoredRecord::parse(bytes).expect("parses").expect("v1");
	assert!(record.decode_packed(packed, None).is_err());
	PackedRow::new(&record, packed).expect_err("a row of the wrong shape is accepted")
}

#[test]
fn a_stray_byte_in_an_unused_fixed_slot_is_refused() {
	let layout = corrupt_layout();
	let mut bytes = packed_row(&layout, &object(vec![]));
	bytes[slot_at(&layout, A) + 2] = 1;
	let column = A as u32;
	assert_eq!(
		refused(&layout, &bytes, &[A]),
		PackedError::Unused {
			column
		}
	);
}

#[test]
fn a_stray_bool_bit_is_refused() {
	let layout = corrupt_layout();
	let packed = packed_of(&layout);
	let mut bytes = packed_row(&layout, &object(vec![("b", Value::Bool(false))]));
	// `b` is present and false: clear its presence bit and set its bool bit.
	flip_presence(&mut bytes, presence_bit(&layout, B));
	let Location::Bit(bit) = packed.columns[B].location else {
		panic!("a bool column");
	};
	let bools = BODY + (packed.presence_bits as usize).div_ceil(8);
	bytes[bools + bit as usize / 8] |= 1 << (bit % 8);
	let column = B as u32;
	assert_eq!(
		refused(&layout, &bytes, &[B]),
		PackedError::Unused {
			column
		}
	);
}

#[test]
fn a_stray_variable_slice_is_refused() {
	let layout = corrupt_layout();
	for column in [S, V] {
		let data = match column {
			S => object(vec![("s", string("hi"))]),
			_ => object(vec![("v", int(3))]),
		};
		let mut bytes = packed_row(&layout, &data);
		flip_presence(&mut bytes, presence_bit(&layout, column));
		assert_eq!(
			refused(&layout, &bytes, &[column]),
			PackedError::Unused {
				column: column as u32
			}
		);
	}
}

#[test]
fn a_remainder_with_its_bit_clear_and_a_slice_is_refused() {
	let layout = corrupt_layout();
	let mut bytes = packed_row(&layout, &object(vec![("extra", int(1))]));
	flip_presence(&mut bytes, presence_bit(&layout, ROOT_REMAINDER));
	// A read of the remainder, and the lookup of an absent column through it.
	assert_eq!(
		refused(&layout, &bytes, &[ROOT_REMAINDER, A]),
		PackedError::Unused {
			column: ROOT_REMAINDER as u32
		}
	);
}

#[test]
fn a_node_between_an_absent_ancestor_and_the_column_is_checked() {
	let o = Pres::Optional;
	let layout = layout(descriptor(
		true,
		&named(vec![(
			"o",
			node(o, true, vec![("m", node(o, true, vec![("z", leaf(PhysicalKind::I64, o))]))]),
		)]),
	));
	let packed = packed_of(&layout);
	let at = |path: &[&str]| {
		packed
			.columns
			.iter()
			.position(|c| c.path.iter().map(String::as_str).eq(path.iter().copied()))
			.expect("a column at the path")
	};
	let (outer, middle, leaf_column) = (at(&["o"]), at(&["o", "m"]), at(&["o", "m", "z"]));
	// `o` and `m` are present and `z` is absent: clearing `o`'s bit leaves `m` set under it.
	let mut bytes = packed_row(
		&layout,
		&object(vec![("o", Value::Object(object(vec![("m", Value::Object(object(vec![])))])))]),
	);
	flip_presence(&mut bytes, presence_bit(&layout, outer));
	assert_eq!(
		refused(&layout, &bytes, &[leaf_column]),
		PackedError::UnderAbsent {
			column: middle as u32
		}
	);
}

#[test]
fn a_column_under_an_absent_node_is_refused() {
	let layout = corrupt_layout();
	// The node is present and holds `x`: clearing the node's bit leaves `x` set under it.
	let mut bytes =
		packed_row(&layout, &object(vec![("node", Value::Object(object(vec![("x", int(5))])))]));
	flip_presence(&mut bytes, presence_bit(&layout, NODE));
	assert_eq!(
		refused(&layout, &bytes, &[NODE_X]),
		PackedError::UnderAbsent {
			column: NODE_X as u32
		}
	);
	// `x`'s bit clear, but its slot still holds the value.
	flip_presence(&mut bytes, presence_bit(&layout, NODE_X));
	assert_eq!(
		refused(&layout, &bytes, &[NODE_X]),
		PackedError::Unused {
			column: NODE_X as u32
		}
	);
	// A slice under an absent node.
	let mut bytes = packed_row(
		&layout,
		&object(vec![("node", Value::Object(object(vec![("y", string("in"))])))]),
	);
	flip_presence(&mut bytes, presence_bit(&layout, NODE));
	refused(&layout, &bytes, &[NODE_Y]);
}

#[test]
fn both_presence_bits_set_is_refused() {
	let layout =
		layout(descriptor(false, &named(vec![("a", leaf(PhysicalKind::I64, Pres::Both))])));
	let mut bytes = packed_row(&layout, &object(vec![("a", int(1))]));
	flip_presence(&mut bytes, 1);
	assert_eq!(
		refused(&layout, &bytes, &[0]),
		PackedError::Presence {
			column: 0
		}
	);
}

/// The trailer of a row: the bytes after its variable region, which hold no escaping column's
/// slice here.
fn trailer_start(layout: &LayoutDescriptor, bytes: &[u8]) -> usize {
	let packed = packed_of(layout);
	let record = StoredRecord::parse(bytes).expect("parses").expect("v1");
	assert_eq!(record.layout(), StoredRecord::PACKED_U8);
	let ends = BODY + packed.fixed_size as usize;
	let var = ends + packed.var_count as usize;
	var + bytes[var - 1] as usize
}

/// A trailer entry: the column's LEB128 index, then a 12-byte payload of seconds and nanoseconds.
fn entry(column: usize, seconds: i64, nanoseconds: u32) -> Vec<u8> {
	let mut buf = [0u8; varint::MAX_LEN];
	let mut out = varint::write_u64(column as u64, &mut buf).to_vec();
	out.extend_from_slice(&seconds.to_le_bytes());
	out.extend_from_slice(&nanoseconds.to_le_bytes());
	out
}

const FAR_PAST: i64 = -20_000_000_000;
const FAR_FUTURE: i64 = 20_000_000_000;

#[test]
fn an_escape_value_with_no_trailer_entry_is_refused() {
	let layout = corrupt_layout();
	let mut bytes = packed_row(&layout, &object(vec![("c", datetime(at(1_600_000_000, 5)))]));
	let at_c = slot_at(&layout, C);
	bytes[at_c..at_c + 8].copy_from_slice(&DATETIME_BELOW.to_le_bytes());
	assert_eq!(
		refused(&layout, &bytes, &[C]),
		PackedError::Escape {
			column: C as u32
		}
	);
	let mut bytes =
		packed_row(&layout, &object(vec![("e", duration(std::time::Duration::from_secs(5)))]));
	let at_e = slot_at(&layout, E);
	bytes[at_e..at_e + 8].copy_from_slice(&DURATION_ESCAPE.to_le_bytes());
	assert_eq!(
		refused(&layout, &bytes, &[E]),
		PackedError::Escape {
			column: E as u32
		}
	);
}

#[test]
fn an_escape_payload_that_is_not_the_slots_is_refused() {
	let layout = corrupt_layout();
	let row = packed_row(&layout, &object(vec![("c", datetime(at(FAR_PAST, 7)))]));
	let trailer = trailer_start(&layout, &row);
	assert_eq!(&row[trailer..], entry(C, FAR_PAST, 7).as_slice());
	let column = C as u32;
	// A payload the 8-byte slot holds, one on the other side of the range, and a leap second.
	for (seconds, nanoseconds) in
		[(1_600_000_000, 0), (FAR_FUTURE, 0), (1_483_228_799, 1_500_000_000)]
	{
		let mut bytes = row[..trailer].to_vec();
		bytes.extend(entry(C, seconds, nanoseconds));
		assert_eq!(
			refused(&layout, &bytes, &[C]),
			PackedError::Escape {
				column
			},
			"{seconds}s {nanoseconds}ns"
		);
	}
	// The slot moved to the other side.
	let mut bytes = row;
	let at_c = slot_at(&layout, C);
	bytes[at_c..at_c + 8].copy_from_slice(&DATETIME_ABOVE.to_le_bytes());
	assert_eq!(
		refused(&layout, &bytes, &[C]),
		PackedError::Escape {
			column
		}
	);
	// A duration payload the slot holds.
	let row = packed_row(&layout, &object(vec![("e", duration(std::time::Duration::MAX))]));
	let trailer = trailer_start(&layout, &row);
	let mut bytes = row[..trailer].to_vec();
	bytes.extend(entry(E, 1, 0));
	assert_eq!(
		refused(&layout, &bytes, &[E]),
		PackedError::Escape {
			column: E as u32
		}
	);
}

#[test]
fn a_trailer_that_escapes_disagrees_with_is_refused() {
	let layout = corrupt_layout();
	let row = packed_row(
		&layout,
		&object(vec![("c", datetime(at(FAR_PAST, 1))), ("d", datetime(at(FAR_FUTURE, 2)))]),
	);
	let trailer = trailer_start(&layout, &row);
	let c = entry(C, FAR_PAST, 1);
	let d = entry(D, FAR_FUTURE, 2);
	assert_eq!(row[trailer..], [c.clone(), d.clone()].concat());
	let head = &row[..trailer];
	// Every read of an escaped slot, and of an empty column whose kind escapes, checks the whole
	// trailer, as the whole decode consumes all of it.
	let readers = [C, D, E];
	// Out of order.
	let bytes = [head, &d, &c].concat();
	refused(&layout, &bytes, &readers);
	// Repeated, right after its first entry and after another.
	let bytes = [head, &c, &c, &d].concat();
	assert_eq!(refused(&layout, &bytes, &readers), PackedError::Trailer);
	let bytes = [head, &c, &d, &c].concat();
	assert_eq!(refused(&layout, &bytes, &readers), PackedError::Trailer);
	// Truncated.
	let bytes = &row[..row.len() - 1];
	refused(&layout, bytes, &readers);
	// An entry for a column whose kind does not escape, before `c`.
	let bytes = [head, &entry(A, 0, 0), &c, &d].concat();
	assert_eq!(refused(&layout, &bytes, &readers), PackedError::Trailer);
	// An entry for a column past the descriptor.
	let bytes = [head, &c, &d, &entry(99, 0, 0)].concat();
	assert_eq!(refused(&layout, &bytes, &readers), PackedError::Trailer);
	// A read of a column whose kind cannot escape does not read the trailer.
	assert!(matches!(
		read(&packed_of(&layout).columns[A], {
			let record = StoredRecord::parse(&bytes).expect("parses").expect("v1");
			PackedRow::new(&record, packed_of(&layout)).expect("a row").field(A)
		}),
		Ok(Read::Absent)
	));
}

#[test]
fn a_trailer_entry_for_an_absent_column_is_refused() {
	let layout = corrupt_layout();
	// `c` is absent and its slot is zero; `d` escapes.
	let row = packed_row(&layout, &object(vec![("d", datetime(at(FAR_FUTURE, 2)))]));
	let trailer = trailer_start(&layout, &row);
	let bytes = [&row[..trailer], &entry(C, FAR_PAST, 1), &row[trailer..]].concat();
	// The decode, the read of `c`, and the read of `d`, which walks past the entry, refuse it.
	assert_eq!(refused(&layout, &bytes, &[C, D, E]), PackedError::Trailer);
	assert!(check_packed(&bytes, bytes[1], Generation::FIRST, packed_of(&layout)).is_err());
	// The entry with `c`'s slot at the escape value is a stray byte in an unused slot.
	let mut bytes = bytes;
	let at_c = slot_at(&layout, C);
	bytes[at_c..at_c + 8].copy_from_slice(&DATETIME_BELOW.to_le_bytes());
	assert_eq!(
		refused(&layout, &bytes, &[C]),
		PackedError::Unused {
			column: C as u32
		}
	);
}

#[test]
fn escapes_must_agree_with_the_trailer() {
	let layout = corrupt_layout();
	// ESCAPES with no trailer.
	let mut bytes = packed_row(&layout, &object(vec![("c", datetime(at(5, 0)))]));
	bytes[1] |= StoredRecord::ESCAPES;
	assert_eq!(refused_shape(&layout, &bytes), PackedError::Trailer);
	// A trailer with no ESCAPES.
	let mut bytes = packed_row(&layout, &object(vec![("c", datetime(at(FAR_PAST, 0)))]));
	bytes[1] &= !StoredRecord::ESCAPES;
	assert_eq!(refused_shape(&layout, &bytes), PackedError::Length);
}

#[test]
fn a_row_of_the_wrong_shape_is_refused() {
	let layout = corrupt_layout();
	let packed = packed_of(&layout);
	let row = packed_row(&layout, &object(vec![("s", string("abc")), ("a", int(1))]));
	// A bit past the presence count.
	let mut bytes = row.clone();
	let bits = packed.presence_bits;
	assert_ne!(bits % 8, 0);
	flip_presence(&mut bytes, bits);
	assert_eq!(refused_shape(&layout, &bytes), PackedError::Padding);
	// End offsets wider than the variable region needs.
	let mut bytes = row;
	bytes[1] = (bytes[1] & !StoredRecord::LAYOUT) | StoredRecord::PACKED_U16;
	assert!(matches!(
		refused_shape(&layout, &bytes),
		PackedError::Width | PackedError::Length | PackedError::Short
	));
	// Every proper prefix.
	let row = packed_row(
		&layout,
		&object(vec![
			("c", datetime(at(FAR_PAST, 0))),
			("s", string("abc")),
			("v", Value::Object(object(vec![("k", int(1))]))),
		]),
	);
	for length in 0..row.len() {
		let prefix = &row[..length];
		match StoredRecord::parse(prefix) {
			Ok(Some(record)) => {
				assert!(record.decode_packed(packed, None).is_err(), "a prefix of {length} decodes")
			}
			Ok(None) => assert_eq!(length, 0),
			Err(_) => {}
		}
	}
	// A byte past the row's end.
	let mut bytes = row;
	bytes.push(0);
	assert!(matches!(
		StoredRecord::parse(&bytes).expect("parses").expect("v1").decode_packed(packed, None),
		Err(PackedError::Trailer)
	));
}

/// Swaps the bytes `x` and `y` within `bytes[at..at + length]`.
fn swap_within(bytes: &mut [u8], at: usize, length: usize, x: u8, y: u8) {
	for byte in &mut bytes[at..at + length] {
		if *byte == x {
			*byte = y;
		} else if *byte == y {
			*byte = x;
		}
	}
}

#[test]
fn object_keys_out_of_order_are_refused() {
	let layout = corrupt_layout();
	let pair = || Value::Object(object(vec![("ka", int(1)), ("kb", int(1))]));
	// In a value column, and nested in one.
	for value in [pair(), Value::Object(object(vec![("o", pair())]))] {
		let mut bytes = packed_row(&layout, &object(vec![("v", value)]));
		let at = find(&bytes, b"ka");
		swap_within(&mut bytes, at, 5, b'a', b'b');
		refused(&layout, &bytes, &[V]);
	}
	// In the root remainder: its read, and the lookup of an absent column through it.
	let mut bytes = packed_row(&layout, &object(vec![("extra", pair())]));
	let at = find(&bytes, b"ka");
	swap_within(&mut bytes, at, 5, b'a', b'b');
	refused(&layout, &bytes, &[ROOT_REMAINDER]);
	let mut bytes = packed_row(&layout, &object(vec![("wa", int(1)), ("wb", int(2))]));
	let at = find(&bytes, b"wa");
	swap_within(&mut bytes, at, 5, b'a', b'b');
	refused(&layout, &bytes, &[ROOT_REMAINDER, A, NODE, NODE_X, S]);
	// In a nested remainder, through the lookup of the node's absent column.
	let node = Value::Object(object(vec![("qa", int(1)), ("qb", int(2))]));
	let mut bytes = packed_row(&layout, &object(vec![("node", node)]));
	let at = find(&bytes, b"qa");
	swap_within(&mut bytes, at, 5, b'a', b'b');
	refused(&layout, &bytes, &[NODE + 1, NODE_X, NODE_Y]);
}

#[test]
fn a_key_in_a_present_column_and_the_remainder_is_refused() {
	let layout = corrupt_layout();
	// `t` has no column; renamed `s`, it repeats the present column `s`.
	let mut bytes = packed_row(&layout, &object(vec![("s", string("col")), ("t", int(1))]));
	let at = find(&bytes, b"t\x81");
	bytes[at] = b's';
	assert_eq!(
		refused(&layout, &bytes, &[]),
		PackedError::KeyInColumnAndRemainder {
			column: ROOT_REMAINDER as u32
		}
	);
}

#[test]
fn an_empty_column_reads_its_key_from_the_remainder() {
	let layout = corrupt_layout();
	let packed = packed_of(&layout);
	// `nodf` has no column; renamed `node`, it is the remainder's entry for the absent node.
	for (value, x) in [
		(Value::Object(object(vec![("x", int(7)), ("z", Value::Null)])), Some(int(7))),
		(int(5), None),
		(Value::Null, None),
	] {
		let mut bytes = packed_row(&layout, &object(vec![("nodf", value.clone())]));
		let at = find(&bytes, b"nodf");
		bytes[at + 3] = b'e';
		let record = StoredRecord::parse(&bytes).expect("parses").expect("v1");
		let row = PackedRow::new(&record, packed).expect("a row");
		let decoded = row.decode(None).expect("a key in the remainder of an empty column decodes");
		let held = object(vec![("node", value.clone())]);
		assert!(same(&decoded, &held), "{decoded:?}");
		// The root remainder reads as it is held, with the key its column could hold, and the
		// absent node's own remainder reads absent.
		check_fields_but(&layout, &row, &decoded, &bytes, &[ROOT_REMAINDER, NODE + 1]);
		let got = read(&packed.columns[NODE + 1], row.field(NODE + 1)).expect("reads");
		assert!(matches!(got, Read::Absent), "{got:?}");
		let got = read(&packed.columns[ROOT_REMAINDER], row.field(ROOT_REMAINDER)).expect("reads");
		assert!(agrees(&got, Some(&Value::Object(held))), "{got:?}");
		let got = read(&packed.columns[NODE_X], row.field(NODE_X)).expect("reads");
		assert!(agrees(&got, x.as_ref()), "{got:?}");
		let got = read(&packed.columns[NODE], row.field(NODE)).expect("reads");
		assert!(agrees(&got, Some(&value)), "{got:?}");
	}
	// The same for a leaf: `t` has no column; renamed `s`, it is the absent `s`'s value.
	let mut bytes = packed_row(&layout, &object(vec![("t", int(1))]));
	let at = find(&bytes, b"t\x81");
	bytes[at] = b's';
	let record = StoredRecord::parse(&bytes).expect("parses").expect("v1");
	let row = PackedRow::new(&record, packed).expect("a row");
	let decoded = row.decode(None).expect("decodes");
	let held = object(vec![("s", int(1))]);
	assert!(same(&decoded, &held));
	check_fields_but(&layout, &row, &decoded, &bytes, &[ROOT_REMAINDER]);
	let got = read(&packed.columns[ROOT_REMAINDER], row.field(ROOT_REMAINDER)).expect("reads");
	assert!(agrees(&got, Some(&Value::Object(held))), "{got:?}");
}

/// A row of generation 1 in parts.
struct Parts {
	header: u8,
	fixed: Vec<u8>,
	slices: Vec<Vec<u8>>,
	trailer: Vec<u8>,
}

fn split(layout: &LayoutDescriptor, bytes: &[u8]) -> Parts {
	let packed = packed_of(layout);
	assert_eq!(bytes[2], 1, "generation 1");
	let width = match bytes[1] & StoredRecord::LAYOUT {
		StoredRecord::PACKED_U8 => 1,
		StoredRecord::PACKED_U16 => 2,
		_ => 4,
	};
	let body = &bytes[BODY..];
	let fixed = packed.fixed_size as usize;
	let count = packed.var_count as usize;
	let var = fixed + count * width;
	let mut slices = Vec::new();
	let mut start = 0;
	for index in 0..count {
		let at = fixed + index * width;
		let mut raw = [0u8; 8];
		raw[..width].copy_from_slice(&body[at..at + width]);
		let end = u64::from_le_bytes(raw) as usize;
		slices.push(body[var + start..var + end].to_vec());
		start = end;
	}
	Parts {
		header: bytes[1],
		fixed: body[..fixed].to_vec(),
		slices,
		trailer: body[var + start..].to_vec(),
	}
}

/// The row of `parts`, at the narrowest end width, with ESCAPES set when it has a trailer.
fn join(parts: &Parts) -> Vec<u8> {
	let length: usize = parts.slices.iter().map(Vec::len).sum();
	let (layout, width) = match length {
		0..=255 => (StoredRecord::PACKED_U8, 1),
		256..=65_535 => (StoredRecord::PACKED_U16, 2),
		_ => (StoredRecord::PACKED_U32, 4),
	};
	let mut header = (parts.header & !StoredRecord::LAYOUT & !StoredRecord::ESCAPES) | layout;
	if !parts.trailer.is_empty() {
		header |= StoredRecord::ESCAPES;
	}
	let mut out = vec![StoredRecord::MARKER, header, 1];
	out.extend_from_slice(&parts.fixed);
	let mut end = 0u64;
	for slice in &parts.slices {
		end += slice.len() as u64;
		out.extend_from_slice(&end.to_le_bytes()[..width]);
	}
	for slice in &parts.slices {
		out.extend_from_slice(slice);
	}
	out.extend_from_slice(&parts.trailer);
	out
}

/// `bytes` with variable slice `var` replaced by the stored encoding of `value`.
fn with_slice(layout: &LayoutDescriptor, bytes: &[u8], var: usize, value: &Value) -> Vec<u8> {
	let mut parts = split(layout, bytes);
	parts.slices[var] = stored::encode(value).expect("encodes");
	join(&parts)
}

#[test]
fn split_and_join_are_inverse() {
	let layout = corrupt_layout();
	let row = packed_row(
		&layout,
		&object(vec![("c", datetime(at(FAR_PAST, 0))), ("s", string(&"x".repeat(300)))]),
	);
	assert_eq!(join(&split(&layout, &row)), row);
}

#[test]
fn a_remainder_is_a_non_empty_object_of_values_without_a_root_id() {
	let layout = corrupt_layout();
	let row = packed_row(&layout, &object(vec![("extra", int(1))]));
	let none = Value::Object(object(vec![("k", Value::None)]));
	for (value, refusal) in [
		(Value::Array(Array(vec![int(1)])), "not an object"),
		(Value::Object(Object::default()), "no keys"),
		(Value::Object(object(vec![("id", int(1))])), "an id"),
		(none, "a NONE"),
	] {
		let bytes = with_slice(&layout, &row, 0, &value);
		// The remainder's own read, and the lookups of absent columns through it.
		let whole = refused(&layout, &bytes, &[ROOT_REMAINDER, A, NODE, NODE_X, S]);
		assert!(
			matches!(
				whole,
				PackedError::Slot {
					column: 0,
					..
				} | PackedError::RemainderId
			),
			"{refusal}: {whole:?}"
		);
	}
	// A nested remainder may hold an `id`.
	let node = Value::Object(object(vec![("extra", int(1))]));
	let row = packed_row(&layout, &object(vec![("node", node)]));
	let id = Value::Object(object(vec![("id", int(1))]));
	let bytes = with_slice(&layout, &row, 1, &id);
	let record = StoredRecord::parse(&bytes).expect("parses").expect("v1");
	let row = PackedRow::new(&record, packed_of(&layout)).expect("a row");
	let decoded = row.decode(None).expect("a nested remainder holds an id");
	assert!(same(&decoded, &object(vec![("node", id)])));
	check_fields(&layout, &row, &decoded, &bytes);
}

#[test]
fn a_plan_reads_only_rows_of_its_own_descriptor() {
	let layout = corrupt_layout();
	let bytes = packed_row(&layout, &object(vec![("a", int(1))]));
	let record = StoredRecord::parse(&bytes).expect("parses").expect("v1");
	let row = PackedRow::new(&record, packed_of(&layout)).expect("a row");
	// An equal descriptor in another allocation.
	let other = Arc::new((*layout).clone());
	let plan = FieldPlan::new(&other, A).expect("a plan");
	assert_eq!(plan.descriptor(), &other);
	assert!(matches!(row.field_with(&plan), Err(PackedError::ForeignPlan)));
	let plan = FieldPlan::new(&layout, A).expect("a plan");
	assert!(matches!(row.field_with(&plan), Ok(FieldRef::Int(1))));
	let count = packed_of(&layout).columns.len();
	assert!(matches!(FieldPlan::new(&layout, count), Err(PackedError::NoColumn { .. })));
	assert!(matches!(row.field(count), Err(PackedError::NoColumn { .. })));
	let document = Arc::new(LayoutDescriptor::document(TableName::new("t")));
	assert!(matches!(FieldPlan::new(&document, 0), Err(PackedError::NotPackedDescriptor)));
}

#[test]
fn a_document_is_not_a_packed_row() {
	let layout = corrupt_layout();
	let bytes = [StoredRecord::MARKER, StoredRecord::DOCUMENT, 1, stored::tag::NULL];
	let record = StoredRecord::parse(&bytes).expect("parses").expect("v1");
	assert!(matches!(PackedRow::new(&record, packed_of(&layout)), Err(PackedError::NotPacked)));
}

#[test]
fn kinds_read_the_bytes_no_writer_writes() {
	// A `U64` slot above `i64::MAX` reads as the decimal it is, and does not pack again.
	let l = layout(descriptor(false, &named(vec![("u", leaf(PhysicalKind::U64, Pres::Required))])));
	let packed = packed_of(&l);
	let mut bytes = packed_row(&l, &object(vec![("u", int(0))]));
	let at = slot_at(&l, 0);
	bytes[at..at + 8].copy_from_slice(&u64::MAX.to_le_bytes());
	let stored = StoredRecord::parse(&bytes).expect("parses").expect("v1");
	let row = PackedRow::new(&stored, packed).expect("a row");
	let decoded = row.decode(None).expect("decodes");
	let want = decimal(Decimal::from(u64::MAX));
	assert!(same(&decoded, &object(vec![("u", want)])));
	check_fields(&l, &row, &decoded, &bytes);
	assert_eq!(pack(packed, Generation::FIRST, RowFlags::NONE, &decoded), Ok(Packing::Misfit));
	// A `Decimal16` slot with a flag bit no decimal sets, or a scale past 28.
	let l = layout(descriptor(
		false,
		&named(vec![("d", leaf(PhysicalKind::Decimal16, Pres::Required))]),
	));
	let row = packed_row(&l, &object(vec![("d", decimal(Decimal::new(5, 1)))]));
	let at = slot_at(&l, 0);
	for (byte, value) in [(0, 1), (2, 29), (3, 0x40)] {
		let mut bytes = row.clone();
		bytes[at + byte] = value;
		assert!(matches!(
			refused(&l, &bytes, &[0]),
			PackedError::Slot {
				column: 0,
				..
			}
		));
	}
	// A literal code past the list.
	let literals = vec![string("x"), string("y")];
	let l = layout_of_literals(literals);
	let mut bytes = packed_row(&l, &object(vec![("l", string("y"))]));
	let at = slot_at(&l, 0);
	bytes[at] = 2;
	assert_eq!(
		refused(&l, &bytes, &[0]),
		PackedError::Code {
			column: 0
		}
	);
	// A record key of a table code past the list, and one with no key bytes.
	let l = layout(descriptor(
		false,
		&named(vec![(
			"r",
			Spec::Leaf {
				kind: PhysicalKind::RecordKey,
				presence: Pres::Required,
				literals: Vec::new(),
				tables: vec![TableName::new("a"), TableName::new("b")],
			},
		)]),
	));
	let row = packed_row(&l, &object(vec![("r", record("b", RecordIdKey::Number(1)))]));
	let at = BODY + packed_of(&l).fixed_size as usize + 1;
	assert_eq!(row[at], 1, "the table code");
	let mut bytes = row;
	bytes[at] = 2;
	assert_eq!(
		refused(&l, &bytes, &[0]),
		PackedError::Code {
			column: 0
		}
	);
	// A string that is not UTF-8.
	let l = layout_of(PhysicalKind::String);
	let mut bytes = packed_row(&l, &object(vec![("l", string("ab"))]));
	let len = bytes.len();
	bytes[len - 1] = 0xFF;
	refused(&l, &bytes, &[0]);
	// A `NONE` in a value column, and a `NULL` its presence bits could have said.
	let l =
		layout(descriptor(false, &named(vec![("l", leaf(PhysicalKind::Value, Pres::Nullable))])));
	let mut bytes = packed_row(&l, &object(vec![("l", Value::Bool(true))]));
	let len = bytes.len();
	for tag in [stored::tag::NONE, stored::tag::NULL] {
		bytes[len - 1] = tag;
		assert!(matches!(
			refused(&l, &bytes, &[0]),
			PackedError::Slot {
				column: 0,
				..
			}
		));
	}
}

fn layout_of(kind: PhysicalKind) -> Arc<LayoutDescriptor> {
	layout(descriptor(false, &named(vec![("l", leaf(kind, Pres::Required))])))
}

fn layout_of_literals(literals: Vec<Value>) -> Arc<LayoutDescriptor> {
	layout(descriptor(
		false,
		&named(vec![(
			"l",
			Spec::Leaf {
				kind: PhysicalKind::LiteralCode(1),
				presence: Pres::Required,
				literals,
				tables: Vec::new(),
			},
		)]),
	))
}

#[test]
fn a_variable_region_past_u16_takes_four_byte_ends() {
	let layout = layout_of(PhysicalKind::String);
	for (length, width) in [
		(255, StoredRecord::PACKED_U8),
		(256, StoredRecord::PACKED_U16),
		(65_535, StoredRecord::PACKED_U16),
		(65_536, StoredRecord::PACKED_U32),
	] {
		let bytes = packed_row(&layout, &object(vec![("l", string(&"x".repeat(length)))]));
		let record = StoredRecord::parse(&bytes).expect("parses").expect("v1");
		assert_eq!(record.layout(), width, "{length}");
	}
}
