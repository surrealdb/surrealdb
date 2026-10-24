//! Tests of [`PackedRow::visit`] against [`PackedRow::decode`].
//!
//! * Every generated row, visited and rebuilt, is the record its whole decode builds, and every
//!   object the visit closes holds the entry count it closes with.
//! * Every bit flip and every truncation of the short generated rows is visited without a panic. A
//!   mutant the whole decode accepts is visited to the same record; one it refuses is refused by
//!   the visit or visited to some record, and the refusals the visit does not make are tallied by
//!   the decode's error. Every truncation is refused: by the shape check, or, for a cut inside the
//!   trailer, by the visit's read of the escape it cut.

use std::collections::BTreeMap;

use super::tests::{
	Pres, Rng, SEEDS, at, datetime, descriptor, duration, gen_children, gen_object, hex, layout,
	leaf, named, object, packed_of, same,
};
use super::{FieldRef, PackedError, PackedRow, PackedVisitor, Packing, pack};
use crate::Generation;
use crate::record::{RowFlags, StoredRecord};
use crate::val::{Number, Object, RecordId, RecordIdKey, Strand, TableName, Value, stored};

/// The record's own id in these tests.
fn id() -> Value {
	Value::RecordId(RecordId {
		table: TableName::new("t"),
		key: RecordIdKey::Number(1),
	})
}

/// An object being rebuilt: its entries, and the key awaiting a value.
type Building = (Vec<(String, Value)>, Option<String>);

/// A visitor that rebuilds the record, checking each object's count as it closes.
#[derive(Default)]
struct Rebuild {
	/// The open objects, innermost last.
	open: Vec<Building>,
	done: Option<Object>,
}

impl Rebuild {
	fn put(&mut self, value: Value) -> Result<(), PackedError> {
		let (entries, key) = self.open.last_mut().expect("a value inside an object");
		let key = key.take().expect("a value after its key");
		entries.push((key, value));
		Ok(())
	}
}

impl<'a> PackedVisitor<'a> for Rebuild {
	type Error = PackedError;

	fn open(&mut self) -> Result<usize, PackedError> {
		self.open.push((Vec::new(), None));
		Ok(self.open.len())
	}

	fn key(&mut self, at: usize, key: &str) -> Result<(), PackedError> {
		let (entries, pending) = self.open.last_mut().expect("a key inside an object");
		assert_eq!(entries.len(), at, "the entry index");
		assert!(pending.is_none(), "a key before the last key's value");
		*pending = Some(key.to_owned());
		Ok(())
	}

	fn id(&mut self, at: usize) -> Result<(), PackedError> {
		self.key(at, "id")?;
		self.put(id())
	}

	fn field(&mut self, field: FieldRef<'a>) -> Result<(), PackedError> {
		let depth = self.open.len();
		let bad = |error| PackedError::Stored {
			column: u32::MAX,
			error,
		};
		let value = match field {
			FieldRef::Absent | FieldRef::Object => panic!("a visit hands no {field:?}"),
			FieldRef::Null => Value::Null,
			FieldRef::Bool(v) => Value::Bool(v),
			FieldRef::I8(v) => Value::Number(Number::I8(v)),
			FieldRef::I16(v) => Value::Number(Number::I16(v)),
			FieldRef::I32(v) => Value::Number(Number::I32(v)),
			FieldRef::Int(v) => Value::from(v),
			FieldRef::U8(v) => Value::Number(Number::U8(v)),
			FieldRef::U16(v) => Value::Number(Number::U16(v)),
			FieldRef::U32(v) => Value::Number(Number::U32(v)),
			FieldRef::U64(v) => Value::Number(Number::U64(v)),
			FieldRef::F16(v) => Value::Number(Number::F16(v)),
			FieldRef::F32(v) => Value::Number(Number::F32(v)),
			FieldRef::Float(v) => Value::from(v),
			FieldRef::Decimal(v) => Value::from(v),
			FieldRef::Uuid(v) => Value::Uuid(crate::val::Uuid(v)),
			FieldRef::Datetime(v) => Value::Datetime(crate::val::Datetime(v)),
			FieldRef::Duration(v) => Value::Duration(crate::val::Duration(v)),
			FieldRef::Point(x, y) => Value::Geometry(crate::val::Geometry::Point((x, y).into())),
			FieldRef::String(v) => Value::String(Strand::new(v)),
			FieldRef::Bytes(v) => Value::Bytes(crate::val::Bytes::from(v.to_vec())),
			FieldRef::Literal(v) => v.clone(),
			FieldRef::RecordId(table, key) => Value::RecordId(RecordId {
				table: table.clone(),
				key: stored::decode_key_nested(key, depth.saturating_add(1)).map_err(bad)?,
			}),
			FieldRef::Stored(v) => v.decode().map_err(bad)?,
		};
		self.put(value)
	}

	fn close(&mut self, mark: usize, len: usize) -> Result<(), PackedError> {
		assert_eq!(mark, self.open.len(), "a close of the object open last");
		let (entries, pending) = self.open.pop().expect("a close of an open object");
		assert!(pending.is_none(), "a key with no value");
		assert_eq!(entries.len(), len, "an object holds the count it closes with");
		let mut object = Object::default();
		for (key, value) in entries {
			object.0.insert(Strand::new(&key), value);
		}
		if self.open.is_empty() {
			self.done = Some(object);
			Ok(())
		} else {
			self.put(Value::Object(object))
		}
	}
}

/// The record a visit of `row` rebuilds.
fn visited(row: &PackedRow<'_>) -> Result<Object, PackedError> {
	let mut rebuild = Rebuild::default();
	row.visit(&mut rebuild)?;
	Ok(rebuild.done.expect("a visit closes the root"))
}

/// Visits mutant `bytes`, which must not panic, and holds it to the decode: a mutant the decode
/// accepts is visited to the same record. Answers the decode's refusal of a mutant the visit
/// accepts.
fn check_mutant(packed: &crate::layout::PackedDescriptor, bytes: &[u8]) -> Option<String> {
	let Ok(Some(record)) = StoredRecord::parse(bytes) else {
		return None;
	};
	let Ok(row) = PackedRow::new(&record, packed) else {
		return None;
	};
	let visit = visited(&row);
	match row.decode(Some(id())) {
		Ok(decoded) => {
			let visit = visit.unwrap_or_else(|error| {
				panic!("a row that decodes is refused by its visit: {error:?}\nrow {}", hex(bytes))
			});
			assert!(
				same(&visit, &decoded),
				"{}\nvisit   {visit:?}\ndecoded {decoded:?}",
				hex(bytes)
			);
			None
		}
		Err(error) => visit.is_ok().then(|| {
			let name = format!("{error:?}");
			name.split([' ', '{', '(']).next().unwrap_or_default().to_owned()
		}),
	}
}

#[test]
fn a_visit_rebuilds_the_record_the_decode_builds() {
	let (mut rows, mut mutants, mut truncations) = (0usize, 0usize, 0usize);
	let mut accepted: BTreeMap<String, usize> = BTreeMap::new();
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
				let Packing::Packed(bytes) =
					pack(packed, generation, flags, &data).expect("a storable record packs")
				else {
					continue;
				};
				rows += 1;
				let record = StoredRecord::parse(&bytes).expect("parses").expect("v1");
				let row = PackedRow::new(&record, packed).expect("a packed row has its shape");
				let decoded = row.decode(Some(id())).expect("decodes");
				let visit = visited(&row).unwrap_or_else(|error| {
					panic!("a packed row is refused by its visit: {error:?}\nrow {}", hex(&bytes))
				});
				assert!(
					same(&visit, &decoded),
					"row     {}\nvisit   {visit:?}\ndecoded {decoded:?}",
					hex(&bytes)
				);
				// Every flip of every bit, and every truncation, of the short rows.
				if bytes.len() < 160 {
					for at in 0..bytes.len() {
						for bit in 0..8 {
							let mut mutant = bytes.clone();
							mutant[at] ^= 1 << bit;
							mutants += 1;
							if let Some(name) = check_mutant(packed, &mutant) {
								*accepted.entry(name).or_default() += 1;
							}
						}
						truncations += 1;
						let truncated = &bytes[..at];
						if let Ok(Some(record)) = StoredRecord::parse(truncated)
							&& let Ok(row) = PackedRow::new(&record, packed)
						{
							assert!(
								visited(&row).is_err(),
								"a truncated row is visited\nrow {} cut at {at}",
								hex(&bytes)
							);
						}
					}
				}
			}
		}
	}
	assert!(rows > 1_000, "only {rows} rows packed");
	assert!(mutants > 10_000, "only {mutants} mutants");
	eprintln!(
		"{rows} rows, {mutants} bit flips, {truncations} truncations; flips the decode refuses and the visit accepts, by the decode's refusal: {accepted:?}"
	);
}

/// A row whose every column holds an escape value is visited to the record its decode builds, and
/// the visit reads each trailer entry once: its reads grow with the escaped columns, not with
/// their square.
#[test]
fn a_row_of_many_escapes_reads_its_trailer_once() {
	use crate::layout::PhysicalKind;
	for count in [1usize, 8, 64, 256] {
		let names: Vec<String> = (0..count).map(|i| format!("c{i:04}")).collect();
		let mut children = Vec::new();
		let mut entries = Vec::new();
		for (i, name) in names.iter().enumerate() {
			// A datetime past the 8-byte slot's range, or a duration of more than `u64::MAX`
			// nanoseconds: both live in the trailer.
			if i % 2 == 0 {
				children.push((name.as_str(), leaf(PhysicalKind::Datetime, Pres::Required)));
				entries.push((name.as_str(), datetime(at(10_000_000_000 + i as i64, 7))));
			} else {
				children.push((name.as_str(), leaf(PhysicalKind::Duration, Pres::Required)));
				entries.push((
					name.as_str(),
					duration(std::time::Duration::new(20_000_000_000 + i as u64, 0)),
				));
			}
		}
		let layout = layout(descriptor(false, &named(children)));
		let packed = packed_of(&layout);
		let generation = Generation::new(1).expect("not zero");
		let Packing::Packed(bytes) =
			pack(packed, generation, RowFlags::edge(false), &object(entries)).expect("packs")
		else {
			panic!("{count} escaped columns misfit");
		};
		let record = StoredRecord::parse(&bytes).expect("parses").expect("v1");
		assert!(record.has_escapes(), "{count}: the row has a trailer");
		let row = PackedRow::new(&record, packed).expect("a packed row has its shape");
		let before = super::row::trailer_reads();
		let visit = visited(&row).expect("visits");
		let reads = super::row::trailer_reads() - before;
		assert!(same(&visit, &row.decode(Some(id())).expect("decodes")), "{count}: {visit:?}");
		// One read finds each entry, and one more finds the end of the trailer at most.
		assert!(reads <= count + 1, "{count} escaped columns read {reads} trailer entries");
	}
}
