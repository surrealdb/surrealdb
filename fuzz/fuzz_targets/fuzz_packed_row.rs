//! Fuzzing of packed rows and the descriptors they are read against.
//!
//! An input's first byte chooses how the rest is used.
//!
//! * `0`: the rest drives a generator of a descriptor, and the bytes it leaves are a row of that
//!   descriptor, after the envelope. Reading the row, whole or checked, must not panic. The check
//!   accepts exactly the rows the whole decode accepts and refuses the others with its error. A
//!   row that decodes whole reads field by field, as a checked row, to what the decode holds,
//!   passes `check_packed`, and packs again to a row that decodes to the same data.
//! * `1`: the rest drives a generator of a descriptor and of a record for it. A record that packs
//!   is checked as a row that decodes is, and decodes to itself.
//! * `2`: the rest is a descriptor's stored bytes, then a row after a 2-byte length. A descriptor
//!   that decodes and passes its validation is written again to bytes that decode to it, and the
//!   row is read as in `0`.

#![no_main]

use std::sync::Arc;

use libfuzzer_sys::arbitrary::Unstructured;
use libfuzzer_sys::fuzz_target;
use surrealdb_catalog::packed::{CheckedPackedRow, FieldRef, Packing, check_packed, pack};
use surrealdb_catalog::stored::{canonical, decode_entry};
use surrealdb_catalog::{
	Column, ColumnRole, Generation, LayoutDescriptor, Location, PackedDescriptor, PhysicalKind,
	Presence, RowFlags, RowLayout, StoredRecord,
};
use surrealdb_expr::val::{
	Bytes, Datetime, Duration, Geometry, Number, Object, RecordId, Strand, TableName, Uuid, Value,
	stored,
};

mod stored_value;

use stored_value::{Gen, hex, identical};

fuzz_target!(|data: &[u8]| {
	let Some((&mode, rest)) = data.split_first() else {
		return;
	};
	match mode % 3 {
		0 => {
			let mut u = Unstructured::new(rest);
			let layout = {
				let mut g = Gen::new(&mut u);
				generated_layout(&mut g)
			};
			let header = u.arbitrary::<u8>().unwrap_or(1);
			let body = u.take_rest();
			read_row(&layout, &envelope(header, 1, body));
		}
		1 => generated_record(&mut Unstructured::new(rest)),
		_ => stored_descriptor(rest),
	}
});

/// The marker, `header` and `generation` before `body`.
fn envelope(header: u8, generation: u8, body: &[u8]) -> Vec<u8> {
	let mut row = vec![StoredRecord::MARKER, header, generation & 0x7F];
	row.extend_from_slice(body);
	row
}

fn packed_of(layout: &LayoutDescriptor) -> &PackedDescriptor {
	match &layout.layout {
		RowLayout::Packed(packed) => packed,
		RowLayout::Document => panic!("a document descriptor"),
	}
}

// Descriptors

/// Columns being laid out: bits, slots and var indexes are dense in column order.
#[derive(Default)]
struct Builder {
	columns: Vec<Column>,
	presence: u32,
	bools: u32,
	vars: u32,
}

const NAMES: &[&str] = &["a", "b", "c", "id", "x", "z", "é"];
const TABLES: &[&str] = &["a", "b", "t"];

fn literals() -> Vec<Value> {
	vec![
		Value::String(Strand::new("red")),
		Value::Number(Number::Int(1)),
		Value::Number(Number::Float(1.0)),
		Value::Bool(true),
	]
}

impl Builder {
	fn presence(&mut self, g: &mut Gen<'_, '_>) -> Presence {
		let bit = self.presence;
		match g.below(4) {
			0 => Presence::Required,
			1 => {
				self.presence += 1;
				Presence::Optional {
					bit,
				}
			}
			2 => {
				self.presence += 1;
				Presence::Nullable {
					bit,
				}
			}
			_ => {
				self.presence += 2;
				Presence::OptionalNullable {
					bit,
				}
			}
		}
	}

	fn node(&mut self, g: &mut Gen<'_, '_>, path: &[String], depth: usize) {
		if g.below(2) == 0 {
			let bit = self.presence;
			self.presence += 1;
			self.vars += 1;
			self.columns.push(column(
				path.to_vec(),
				PhysicalKind::Value,
				Presence::Optional {
					bit,
				},
				Location::Var(self.vars - 1),
				ColumnRole::Remainder,
			));
		}
		let mut names: Vec<&str> = NAMES
			.iter()
			.copied()
			.filter(|name| !path.is_empty() || *name != "id")
			.filter(|_| g.below(3) == 0)
			.collect();
		names.sort_unstable();
		for name in names {
			let mut child = path.to_vec();
			child.push(name.to_owned());
			let presence = self.presence(g);
			if depth < 3 && g.below(5) == 0 {
				self.columns.push(column(
					child.clone(),
					PhysicalKind::Object,
					presence,
					Location::None,
					ColumnRole::Field,
				));
				self.node(g, &child, depth + 1);
				continue;
			}
			let kind = leaf_kind(g);
			let location = if kind == PhysicalKind::Bool {
				self.bools += 1;
				Location::Bit(self.bools - 1)
			} else if kind.is_variable() {
				self.vars += 1;
				Location::Var(self.vars - 1)
			} else {
				Location::Fixed(0)
			};
			let mut c = column(child, kind, presence, location, ColumnRole::Field);
			match kind {
				PhysicalKind::LiteralCode(_) => {
					c.literals = literals().into_iter().take(1 + g.below(4) as usize).collect()
				}
				PhysicalKind::RecordKey => {
					c.tables = TABLES
						.iter()
						.take(1 + g.below(3) as usize)
						.map(|table| TableName::new(*table))
						.collect()
				}
				_ => {}
			}
			self.columns.push(c);
		}
	}
}

fn column(
	path: Vec<String>,
	kind: PhysicalKind,
	presence: Presence,
	location: Location,
	role: ColumnRole,
) -> Column {
	Column {
		path,
		kind,
		presence,
		location,
		role,
		literals: Vec::new(),
		tables: Vec::new(),
	}
}

fn leaf_kind(g: &mut Gen<'_, '_>) -> PhysicalKind {
	match g.below(26) {
		0 => PhysicalKind::Bool,
		1 => PhysicalKind::I8,
		2 => PhysicalKind::I16,
		3 => PhysicalKind::I32,
		4 => PhysicalKind::I64,
		5 => PhysicalKind::U8,
		6 => PhysicalKind::U16,
		7 => PhysicalKind::U32,
		8 => PhysicalKind::U64,
		9 => PhysicalKind::F16,
		10 => PhysicalKind::F32,
		11 => PhysicalKind::F64,
		12 => PhysicalKind::Decimal16,
		13 => PhysicalKind::Fixed(1 + g.below(24) as u32),
		14 => PhysicalKind::Uuid,
		15 => PhysicalKind::Datetime,
		16 => PhysicalKind::DatetimeWide,
		17 => PhysicalKind::Duration,
		18 => PhysicalKind::Point,
		19 => PhysicalKind::LiteralCode(1 + g.below(2) as u32),
		20 => PhysicalKind::String,
		21 => PhysicalKind::Bytes,
		22 => PhysicalKind::RecordKey,
		23 => PhysicalKind::DecimalVar,
		24 => PhysicalKind::BigDecimal,
		_ => PhysicalKind::Value,
	}
}

/// A valid descriptor of a generated shape.
fn generated_layout(g: &mut Gen<'_, '_>) -> Arc<LayoutDescriptor> {
	let mut builder = Builder::default();
	builder.node(g, &[], 0);
	let mut offset = builder.presence.div_ceil(8) + builder.bools.div_ceil(8);
	for column in &mut builder.columns {
		if let Location::Fixed(at) = &mut column.location {
			*at = offset;
			offset += column.kind.fixed_width().expect("a fixed kind");
		}
	}
	let packed = PackedDescriptor {
		columns: builder.columns,
		presence_bits: builder.presence,
		bool_bits: builder.bools,
		fixed_size: offset,
		var_count: builder.vars,
	};
	packed.validate().expect("a generated descriptor is valid");
	Arc::new(LayoutDescriptor {
		table: TableName::new("t"),
		layout: RowLayout::Packed(packed),
	})
}

/// A descriptor's stored bytes, then a row. A descriptor that is read is written again to bytes
/// that read back as it.
fn stored_descriptor(bytes: &[u8]) {
	let [high, low, rest @ ..] = bytes else {
		return;
	};
	let length = usize::from(u16::from_le_bytes([*low, *high])).min(rest.len());
	let (descriptor, row) = rest.split_at(length);
	let Ok(layout) = decode_entry::<LayoutDescriptor>(descriptor) else {
		return;
	};
	if layout.validate().is_err() {
		return;
	}
	let written = canonical(&layout).expect("a valid descriptor is written");
	let again = decode_entry::<LayoutDescriptor>(&written).expect("a written descriptor is read");
	assert_eq!(again, layout, "a descriptor is read back as another");
	assert_eq!(canonical(&again).expect("written"), written, "a descriptor is written two ways");
	if matches!(layout.layout, RowLayout::Packed(_)) {
		read_row(&Arc::new(layout), row);
	}
}

// Records

/// A record for a generated descriptor: every key absent, `NULL` or present as the column's
/// presence allows, or not, a value that fits its column's kind, or not, and keys with no column.
fn generated_record(u: &mut Unstructured<'_>) {
	let mut g = Gen::new(u);
	let layout = generated_layout(&mut g);
	let packed = packed_of(&layout);
	let mut at = 0;
	let data = node_record(&mut g, packed, &mut at, &[]);
	let generation = Generation::new(1 + g.below(127) as u16).expect("not zero");
	let flags = RowFlags::edge(g.below(2) == 0);
	let bytes = match pack(packed, generation, flags, &data) {
		Ok(Packing::Packed(bytes)) => bytes,
		Ok(Packing::Misfit) | Err(_) => return,
	};
	let decoded = read_row(&layout, &bytes).expect("a packed row decodes");
	let mut kept = data;
	kept.0.remove("id");
	assert!(
		identical(&Value::Object(decoded.clone()), &Value::Object(kept.clone())),
		"packed {kept:?}\nas     {}\nread   {decoded:?}",
		hex(&bytes)
	);
}

fn node_record(
	g: &mut Gen<'_, '_>,
	packed: &PackedDescriptor,
	at: &mut usize,
	path: &[String],
) -> Object {
	let mut object = Object::default();
	while let Some(column) = packed.columns.get(*at) {
		if column.path.len() < path.len() || !column.path.starts_with(path) {
			break;
		}
		*at += 1;
		if column.role == ColumnRole::Remainder {
			for _ in 0..g.below(3) {
				object.0.insert(Strand::new(g.field()), g.value(1));
			}
			continue;
		}
		let name = column.path.last().expect("a field has a name").clone();
		let value = match g.below(8) {
			0 => None,
			1 => Some(Value::Null),
			_ if column.kind == PhysicalKind::Object => {
				Some(Value::Object(node_record(g, packed, at, &column.path)))
			}
			_ => Some(fitting(g, column)),
		};
		if column.kind == PhysicalKind::Object && !matches!(value, Some(Value::Object(_))) {
			// The columns under an absent or NULL node have no keys of their own.
			while packed.columns.get(*at).is_some_and(|c| c.path.starts_with(&column.path)) {
				*at += 1;
			}
		}
		if let Some(value) = value {
			object.0.insert(Strand::new(name), value);
		}
	}
	if path.is_empty() && g.below(3) == 0 {
		object.0.insert(Strand::new("id"), g.value(1));
	}
	object
}

/// A value of the column's kind, usually one the column holds.
fn fitting(g: &mut Gen<'_, '_>, column: &Column) -> Value {
	if g.below(16) == 0 {
		return g.value(1);
	}
	match column.kind {
		PhysicalKind::Bool => Value::Bool(g.below(2) == 0),
		PhysicalKind::I8 => Value::Number(Number::I8(g.bits() as i8)),
		PhysicalKind::I16 => Value::Number(Number::I16(g.bits() as i16)),
		PhysicalKind::I32 => Value::Number(Number::I32(g.bits() as i32)),
		PhysicalKind::I64 => Value::Number(Number::Int(g.int())),
		PhysicalKind::U8 => Value::Number(Number::U8(g.bits() as u8)),
		PhysicalKind::U16 => Value::Number(Number::U16(g.bits() as u16)),
		PhysicalKind::U32 => Value::Number(Number::U32(g.bits() as u32)),
		PhysicalKind::U64 => Value::Number(Number::U64(g.bits())),
		PhysicalKind::F16 => {
			Value::Number(Number::F16(surrealdb_types::f16::from_bits(g.bits() as u16)))
		}
		PhysicalKind::F32 => {
			Value::Number(Number::F32(f32::from_bits(g.bits() as u32)))
		}
		PhysicalKind::F64 => Value::Number(Number::Float(g.float())),
		PhysicalKind::Decimal16 | PhysicalKind::DecimalVar => {
			Value::Number(Number::Decimal(g.decimal()))
		}
		PhysicalKind::BigDecimal => Value::Number(Number::BigDecimal(Box::new(g.bigdecimal()))),
		PhysicalKind::Fixed(width) => {
			Value::Bytes(Bytes::from((0..width).map(|_| g.bits() as u8).collect::<Vec<u8>>()))
		}
		PhysicalKind::Uuid => {
			let mut raw = [0u8; 16];
			raw[..8].copy_from_slice(&g.bits().to_le_bytes());
			raw[8..].copy_from_slice(&g.bits().to_le_bytes());
			Value::Uuid(Uuid::from_slice(&raw).expect("16 bytes"))
		}
		PhysicalKind::Datetime | PhysicalKind::DatetimeWide => Value::Datetime(g.datetime()),
		PhysicalKind::Duration => Value::Duration(Duration(g.duration())),
		PhysicalKind::Point => Value::Geometry(Geometry::Point(
			(f64::from_bits(g.bits()), f64::from_bits(g.bits())).into(),
		)),
		PhysicalKind::LiteralCode(_) => {
			column.literals[g.below(column.literals.len() as u64) as usize].clone()
		}
		PhysicalKind::String => Value::String(Strand::new(g.string())),
		PhysicalKind::Bytes => {
			Value::Bytes(Bytes::from((0..g.below(40)).map(|_| g.bits() as u8).collect::<Vec<u8>>()))
		}
		PhysicalKind::RecordKey => Value::RecordId(RecordId {
			table: column.tables[g.below(column.tables.len() as u64) as usize].clone(),
			key: g.key(1),
		}),
		PhysicalKind::Value | PhysicalKind::Object => g.value(1),
	}
}

// Reading

/// A field as a read answers it, with every byte it carries walked into a value.
#[derive(Debug)]
enum Read {
	Absent,
	Null,
	Object,
	Value(Value),
}

fn owned(column: &Column, field: FieldRef<'_>) -> Result<Read, stored::Error> {
	let number = |n: Number| Read::Value(Value::Number(n));
	Ok(match field {
		FieldRef::Absent => Read::Absent,
		FieldRef::Null => Read::Null,
		FieldRef::Object => Read::Object,
		FieldRef::Bool(v) => Read::Value(Value::Bool(v)),
		FieldRef::I8(v) => number(Number::I8(v)),
		FieldRef::I16(v) => number(Number::I16(v)),
		FieldRef::I32(v) => number(Number::I32(v)),
		FieldRef::Int(v) => number(Number::Int(v)),
		FieldRef::U8(v) => number(Number::U8(v)),
		FieldRef::U16(v) => number(Number::U16(v)),
		FieldRef::U32(v) => number(Number::U32(v)),
		FieldRef::U64(v) => number(Number::U64(v)),
		FieldRef::F16(v) => number(Number::F16(v)),
		FieldRef::F32(v) => number(Number::F32(v)),
		FieldRef::Float(v) => number(Number::Float(v)),
		FieldRef::Decimal(v) => number(Number::Decimal(v)),
		FieldRef::Uuid(v) => Read::Value(Value::Uuid(Uuid(v))),
		FieldRef::Datetime(v) => Read::Value(Value::Datetime(Datetime(v))),
		FieldRef::Duration(v) => Read::Value(Value::Duration(Duration(v))),
		FieldRef::Point(x, y) => Read::Value(Value::Geometry(Geometry::Point((x, y).into()))),
		FieldRef::String(v) => Read::Value(Value::String(Strand::new(v))),
		FieldRef::Bytes(v) => Read::Value(Value::Bytes(Bytes::from(v.to_vec()))),
		FieldRef::Literal(v) => Read::Value(v.clone()),
		FieldRef::RecordId(table, key) => Read::Value(Value::RecordId(RecordId {
			table: table.clone(),
			key: stored::decode_key_nested(key, column.path.len() + 1)?,
		})),
		FieldRef::Stored(v) => Read::Value(v.decode()?),
	})
}

/// What the whole decode holds of column `index`: the value at a field's path, or the keys of a
/// remainder's node that have no column of their own, the root's `id` aside.
fn expected(packed: &PackedDescriptor, decoded: &Object, index: usize) -> Option<Value> {
	let column = &packed.columns[index];
	let (walk, last) = match column.role {
		ColumnRole::Field => {
			let (last, walk) = column.path.split_last()?;
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
	if let Some(key) = last {
		return node.get(key.as_str()).cloned();
	}
	let names: Vec<&str> = packed
		.columns
		.iter()
		.filter(|c| {
			c.role == ColumnRole::Field
				&& c.path.len() == column.path.len() + 1
				&& c.path.starts_with(&column.path)
		})
		.filter_map(|c| c.path.last().map(String::as_str))
		.collect();
	let mut rest = Object::default();
	for (key, value) in node.iter() {
		if !names.contains(&key.as_str()) {
			rest.0.insert(key.clone(), value.clone());
		}
	}
	(!rest.is_empty()).then_some(Value::Object(rest))
}

fn agrees(read: &Read, expected: Option<&Value>) -> bool {
	match (read, expected) {
		(Read::Absent, None) => true,
		(Read::Null, Some(Value::Null)) => true,
		(Read::Object, Some(Value::Object(_))) => true,
		(Read::Value(value), Some(expected)) => identical(value, expected),
		_ => false,
	}
}

/// Reads `bytes` as a row of `layout`, whole and checked. The check accepts exactly the rows the
/// whole decode accepts and refuses the others with its error. A row that decodes whole reads
/// field by field, as a checked row, to the same values, passes `check_packed`, and packs again to
/// a row that decodes to the same data. Answers what it decodes to.
fn read_row(layout: &Arc<LayoutDescriptor>, bytes: &[u8]) -> Option<Object> {
	let packed = packed_of(layout);
	let Ok(Some(record)) = StoredRecord::parse(bytes) else {
		return None;
	};
	let decoded = record.decode_packed(packed, None);
	let checked = record.checked_packed(packed);
	assert_eq!(
		checked.is_ok(),
		decoded.is_ok(),
		"the check and the whole decode disagree on {}",
		hex(bytes)
	);
	let (decoded, checked) = match (decoded, checked) {
		(Ok(decoded), Ok(checked)) => (decoded, checked),
		(Err(decode), Err(check)) => {
			assert_eq!(check, decode, "the check's error is not the decode's");
			return None;
		}
		_ => return None,
	};
	let Value::Object(data) = decoded.data else {
		panic!("a packed row decodes to an object");
	};
	check_reads(packed, &data, &checked, bytes);
	let generation = match record.generation() {
		Ok(Some(generation)) => generation,
		// A stamp that names no generation has no descriptor to check the row against.
		Ok(None) | Err(_) => return Some(data),
	};
	check_packed(bytes, bytes[1], generation, packed)
		.expect("a row that decodes passes its check");
	match pack(packed, generation, RowFlags::edge(record.is_edge()), &data) {
		Ok(Packing::Packed(again)) => {
			// A key a remainder holds although its column is absent is merged into the node as
			// the column's, and the writer puts it in the column: the bytes differ, the data
			// does not.
			if again != bytes {
				let record = StoredRecord::parse(&again).ok().flatten().expect("a written row");
				let repacked = record.decode_packed(packed, None).expect("a written row decodes");
				assert!(
					identical(&repacked.data, &Value::Object(data.clone())),
					"a row that decodes packs again to other data\nrow   {}\nagain {}",
					hex(bytes),
					hex(&again)
				);
			}
		}
		Ok(Packing::Misfit) => {
			panic!("a row that decodes does not pack again\nrow {}", hex(bytes));
		}
		Err(error) => panic!("a decoded row does not pack: {error}"),
	}
	Some(data)
}

/// Every field of a checked row reads to what the whole decode holds, by its path, and by its name
/// for a field of the root.
fn check_reads(
	packed: &PackedDescriptor,
	data: &Object,
	checked: &CheckedPackedRow<'_>,
	bytes: &[u8],
) {
	for (index, column) in packed.columns.iter().enumerate() {
		if column.role != ColumnRole::Field {
			continue;
		}
		let want = expected(packed, data, index);
		let read = checked
			.read_path(column.path.iter().map(String::as_str))
			.expect("a read of a checked row");
		let got = owned(column, read).expect("the bytes of a checked field read");
		assert!(
			agrees(&got, want.as_ref()),
			"column {index} reads {got:?}, and the whole decode holds {want:?}\nrow     {}\n\
			 decoded {data:?}",
			hex(bytes)
		);
		if column.kind == PhysicalKind::Object && matches!(read, FieldRef::Object) {
			let node = checked.decode_node(index).expect("a present node decodes alone");
			assert!(
				matches!(&want, Some(held) if identical(&Value::Object(node), held)),
				"object node {index} decodes alone to another value than the whole decode holds"
			);
		}
		if let [name] = column.path.as_slice() {
			let rooted = checked.read_root(name).expect("a read of a checked row");
			let got = owned(column, rooted).expect("the bytes of a checked field read");
			assert!(agrees(&got, want.as_ref()), "read_root({name:?}) disagrees with read_path");
		}
	}
}
