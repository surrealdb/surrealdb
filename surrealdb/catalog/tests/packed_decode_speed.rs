//! The time the whole decode of a packed row takes, over a nested row of thirty-odd columns and a
//! flat row of forty.
//!
//! The decode of a packed row is the one walk every read of a packed table that does not read
//! its fields in place goes through, so its speed is the floor of a scan that decodes whole rows.
//! The measurement is ignored by default because it states a time, not a property: run it with
//! `cargo test --release -p surrealdb-catalog --test packed_decode_speed -- --ignored --nocapture`.
//! It uses only the public decode, so one file measures any revision of the decode.

use std::sync::atomic::AtomicU32;

use chrono::{DateTime, Utc};
use common::time::Instant;
use surrealdb_catalog::Generation;
use surrealdb_catalog::layout::{
	Column, ColumnRole, Location, PackedDescriptor, PhysicalKind, Presence,
};
use surrealdb_catalog::packed::{CheckedPackedRow, FieldRef, Packing, PathRead, pack};
use surrealdb_catalog::record::{RowFlags, StoredRecord};
use surrealdb_catalog::val::{self, Number, Object, Strand};

/// Lays out the columns of a closed or open record as `(path, kind, presence)` in key order, with
/// the slots assigned in column order.
struct Layout {
	columns: Vec<Column>,
	presence_bits: u32,
	bool_bits: u32,
	vars: u32,
}

impl Layout {
	fn new() -> Self {
		Self {
			columns: Vec::new(),
			presence_bits: 0,
			bool_bits: 0,
			vars: 0,
		}
	}

	fn presence(&mut self, optional: bool) -> Presence {
		if !optional {
			return Presence::Required;
		}
		let bit = self.presence_bits;
		self.presence_bits += 1;
		Presence::Optional {
			bit,
		}
	}

	fn push(&mut self, path: &[&str], kind: PhysicalKind, optional: bool, role: ColumnRole) {
		let presence = self.presence(optional);
		let location = match &kind {
			PhysicalKind::Bool => {
				self.bool_bits += 1;
				Location::Bit(self.bool_bits - 1)
			}
			PhysicalKind::String | PhysicalKind::Value => {
				self.vars += 1;
				Location::Var(self.vars - 1)
			}
			PhysicalKind::Object => Location::None,
			_ => Location::Fixed(0),
		};
		self.columns.push(Column {
			path: path.iter().map(|segment| (*segment).to_owned()).collect(),
			kind,
			presence,
			location,
			role,
			literals: Vec::new(),
			tables: Vec::new(),
		});
	}

	fn field(&mut self, path: &[&str], kind: PhysicalKind, optional: bool) {
		self.push(path, kind, optional, ColumnRole::Field);
	}

	fn remainder(&mut self, path: &[&str]) {
		self.push(path, PhysicalKind::Value, true, ColumnRole::Remainder);
	}

	fn finish(mut self) -> PackedDescriptor {
		let mut offset = self.presence_bits.div_ceil(8) + self.bool_bits.div_ceil(8);
		for column in &mut self.columns {
			if let Location::Fixed(at) = &mut column.location {
				*at = offset;
				offset += column.kind.fixed_width().expect("a fixed kind");
			}
		}
		let descriptor = PackedDescriptor {
			columns: self.columns,
			presence_bits: self.presence_bits,
			bool_bits: self.bool_bits,
			fixed_size: offset,
			var_count: self.vars,
		};
		descriptor.validate().expect("the descriptor is valid");
		descriptor
	}
}

/// A closed record of the fields of a `SCHEMAFULL` table of people and places: strings, integers,
/// a float, a bool, a datetime, a uuid, an optional string, an optional far datetime, an array,
/// a nested object three deep and an open object.
fn descriptor() -> PackedDescriptor {
	people(false)
}

/// The columns of [`descriptor`], with a root that is open when `open`.
fn people(open: bool) -> PackedDescriptor {
	use PhysicalKind::{Bool, Datetime, F64, I64, Object, String, Uuid, Value};
	let mut l = Layout::new();
	if open {
		l.remainder(&[]);
	}
	l.field(&["active"], Bool, false);
	l.field(&["age"], I64, false);
	l.field(&["city"], String, false);
	l.field(&["created_at"], Datetime, false);
	l.field(&["description"], String, false);
	l.field(&["extra"], Object, true);
	l.remainder(&["extra"]);
	l.field(&["far"], Datetime, true);
	l.field(&["geography"], Object, false);
	l.field(&["geography", "code"], String, false);
	l.field(&["geography", "description"], String, false);
	l.field(&["geography", "location"], String, false);
	l.field(&["geography", "region"], Object, false);
	l.field(&["geography", "region", "details"], Object, false);
	l.field(&["geography", "region", "details", "continent"], String, false);
	l.field(&["geography", "region", "details", "timezone"], String, false);
	l.field(&["geography", "region", "name"], String, false);
	l.field(&["geography", "text"], String, false);
	l.field(&["http_status"], I64, false);
	l.field(&["integer"], I64, false);
	l.field(&["name"], String, false);
	l.field(&["note"], String, true);
	l.field(&["number"], I64, false);
	l.field(&["request_id"], Uuid, false);
	l.field(&["score"], F64, false);
	l.field(&["status"], String, false);
	l.field(&["tags"], Value, false);
	l.field(&["text"], String, false);
	l.field(&["tier"], I64, false);
	l.field(&["words"], String, false);
	l.finish()
}

fn int(v: i64) -> val::Value {
	val::Value::Number(Number::Int(v))
}

fn text(v: &str) -> val::Value {
	val::Value::String(Strand::new(v))
}

fn object(entries: Vec<(&str, val::Value)>) -> Object {
	let mut object = Object::default();
	for (key, value) in entries {
		object.0.insert(Strand::new(key), value);
	}
	object
}

/// The record of row `i`: half the rows hold the optional string, a third an `extra` key.
fn record(i: i64) -> Object {
	let when: DateTime<Utc> = DateTime::from_timestamp(1_700_000_000 + i * 997, 5).expect("a time");
	let mut entries = vec![
		("active", val::Value::Bool(i % 3 == 0)),
		("age", int(18 + i % 60)),
		("city", text(["Tokyo", "Paris", "Lima", "Oslo", "Cairo"][(i % 5) as usize])),
		("created_at", val::Value::Datetime(val::Datetime(when))),
		("description", text("A description of the record, long enough to be heard as prose.")),
		(
			"geography",
			val::Value::Object(object(vec![
				("code", text("JP")),
				("description", text("A place on the map")),
				("location", text("35.6,139.6")),
				(
					"region",
					val::Value::Object(object(vec![
						(
							"details",
							val::Value::Object(object(vec![
								("continent", text("Asia")),
								("timezone", text("Asia/Tokyo")),
							])),
						),
						("name", text("Kanto")),
					])),
				),
				("text", text("Tokyo, Japan")),
			])),
		),
		("http_status", int(200 + i % 5)),
		("integer", int(i * 7)),
		("name", text("A name of a record")),
		("number", int(1 + i % 5000)),
		("request_id", val::Value::Uuid(val::Uuid(uuid::Uuid::from_u128(i as u128 + 1)))),
		("score", val::Value::Number(Number::Float(i as f64 * 0.37))),
		("status", text(["new", "open", "closed"][(i % 3) as usize])),
		("tags", val::Value::Array(vec![text("a"), text("b")].into())),
		("text", text("Some text")),
		("tier", int(i % 4)),
		("words", text("one two three four")),
	];
	if i % 2 == 0 {
		entries.push(("note", text("a note")));
	}
	if i % 3 == 0 {
		entries.push(("extra", val::Value::Object(object(vec![("k", int(1))]))));
	}
	if i % 100 == 0 {
		let far: DateTime<Utc> = DateTime::from_timestamp(20_000_000_000, 5).expect("a time");
		entries.push(("far", val::Value::Datetime(val::Datetime(far))));
	}
	object(entries)
}

/// A closed record of forty scalar columns: twenty integers and twenty strings.
fn flat_descriptor() -> PackedDescriptor {
	let mut l = Layout::new();
	let names: Vec<String> = (0..20).map(|i| format!("{i:02}")).collect();
	for name in &names {
		l.field(&[&format!("i{name}")], PhysicalKind::I64, false);
	}
	for name in &names {
		l.field(&[&format!("s{name}")], PhysicalKind::String, false);
	}
	l.finish()
}

fn flat_record(i: i64) -> Object {
	let mut object = Object::default();
	for n in 0..20 {
		object.0.insert(Strand::new(format!("i{n:02}")), int(i + n));
		object.0.insert(Strand::new(format!("s{n:02}")), text("a string of some length"));
	}
	object
}

/// Prints the minimum, median and maximum time in milliseconds of the whole decode of ten thousand
/// rows of `record` against `descriptor`.
fn decode_time(label: &str, descriptor: &PackedDescriptor, record: impl Fn(i64) -> Object) {
	let rows: Vec<Vec<u8>> = (0..10_000)
		.map(|i| {
			match pack(descriptor, Generation::FIRST, RowFlags::NONE, &record(i))
				.expect("a storable record")
			{
				Packing::Packed(bytes) => bytes,
				Packing::Misfit => panic!("row {i} does not fit the descriptor"),
			}
		})
		.collect();
	let run = || {
		let started = Instant::now();
		let mut keys = 0usize;
		for bytes in &rows {
			let stored = StoredRecord::parse(bytes).expect("parses").expect("v1");
			let record = stored.decode_packed(descriptor, None).expect("decodes");
			if let val::Value::Object(object) = &record.data {
				keys += object.len();
			}
		}
		assert!(keys > 0);
		started.elapsed()
	};
	for _ in 0..5 {
		run();
	}
	let mut samples: Vec<f64> = (0..15).map(|_| run().as_secs_f64() * 1e3).collect();
	samples.sort_by(f64::total_cmp);
	println!(
		"decode_packed {label} over {} rows: min {:.3} ms, median {:.3} ms, max {:.3} ms",
		rows.len(),
		samples[0],
		samples[samples.len() / 2],
		samples[samples.len() - 1]
	);
}

#[test]
#[ignore = "states a time; run it by hand with --release --nocapture"]
fn decoding_a_packed_row_whole() {
	decode_time("nested", &descriptor(), record);
	decode_time("flat", &flat_descriptor(), flat_record);
}

/// [`descriptor`] with an open root: the root's remainder holds the keys no column declares.
fn open_descriptor() -> PackedDescriptor {
	people(true)
}

/// [`record`] with six keys of its open root's remainder.
fn open_record(i: i64) -> Object {
	let mut object = record(i);
	for key in ["aa1", "aa2", "aa3", "mm1", "zz1", "zz2"] {
		object.0.insert(Strand::new(key), int(i));
	}
	object
}

/// Prints the minimum and median time in nanoseconds per row of `read` over ten thousand rows.
fn per_row<'a>(
	label: &str,
	rows: &[CheckedPackedRow<'a>],
	mut read: impl FnMut(&CheckedPackedRow<'a>) -> usize,
) {
	let mut run = || {
		let started = Instant::now();
		let mut seen = 0usize;
		for row in rows {
			seen = seen.wrapping_add(read(row));
		}
		std::hint::black_box(seen);
		started.elapsed()
	};
	for _ in 0..5 {
		run();
	}
	let mut samples: Vec<f64> =
		(0..25).map(|_| run().as_secs_f64() * 1e9 / rows.len() as f64).collect();
	samples.sort_by(f64::total_cmp);
	println!(
		"{label}: min {:.1} ns/row, median {:.1} ns/row",
		samples[0],
		samples[samples.len() / 2]
	);
}

/// The reads of a checked row the in-place readers make, in nanoseconds per row: the check, a root
/// column, a root name no column declares, and nested paths walked as the pre-decode filter walks
/// them.
#[test]
#[ignore = "states a time; run it by hand with --release --nocapture"]
fn reading_a_checked_packed_row_in_place() {
	let descriptor = open_descriptor();
	let bytes: Vec<Vec<u8>> = (0..10_000)
		.map(|i| match pack(&descriptor, Generation::FIRST, RowFlags::NONE, &open_record(i)) {
			Ok(Packing::Packed(bytes)) => bytes,
			other => panic!("row {i} does not pack: {other:?}"),
		})
		.collect();
	let records: Vec<StoredRecord<'_>> =
		bytes.iter().map(|b| StoredRecord::parse(b).expect("parses").expect("v1")).collect();
	let check = || {
		let started = Instant::now();
		for record in &records {
			std::hint::black_box(record.checked_packed(&descriptor).expect("checks"));
		}
		started.elapsed().as_secs_f64() * 1e9 / records.len() as f64
	};
	let mut samples: Vec<f64> = (0..25).map(|_| check()).collect();
	samples.sort_by(f64::total_cmp);
	println!("check: min {:.1} ns/row, median {:.1} ns/row", samples[0], samples[12]);
	let rows: Vec<CheckedPackedRow<'_>> =
		records.iter().map(|r| r.checked_packed(&descriptor).expect("checks")).collect();
	let present =
		|field: Result<FieldRef<'_>, _>| usize::from(!matches!(field, Ok(FieldRef::Absent)));
	let hint = AtomicU32::new(u32::MAX);
	per_row("root column", &rows, |row| {
		let index = row.root_column("score", &hint).expect("a column");
		present(row.read_column(index))
	});
	let missing = AtomicU32::new(u32::MAX);
	per_row("root name without a column", &rows, |row| match row.root_column("mm1", &missing) {
		Some(index) => present(row.read_column(index)),
		None => present(row.read_root_remainder("mm1")),
	});
	let reached =
		|read: Result<PathRead<'_>, _>| usize::from(matches!(read, Ok(PathRead::Field { .. })));
	per_row("nested column, by path", &rows, |row| {
		reached(row.read_path(["geography", "region", "details", "timezone"].into_iter()))
	});
	per_row("key of an open node's remainder, by path", &rows, |row| {
		reached(row.read_path(["extra", "k"].into_iter()))
	});
	per_row("key of the root's remainder, by path", &rows, |row| {
		reached(row.read_path(["zz2"].into_iter()))
	});
}
