//! The check of a packed row allocates nothing for the kinds whose check builds no value.
//!
//! A reader that takes a packed row in place pays the check once per row, so the check must not
//! add a heap allocation per row to a read that otherwise allocates none. The counting allocator
//! of this binary counts only the allocations of the thread that asks to count.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use chrono::{DateTime, Utc};
use surrealdb_catalog::Generation;
use surrealdb_catalog::layout::{
	Column, ColumnRole, Location, PackedDescriptor, PhysicalKind, Presence,
};
use surrealdb_catalog::packed::{Packing, pack};
use surrealdb_catalog::record::{RowFlags, StoredRecord};
use surrealdb_catalog::val::{self, Number, Object, Strand, Value};

thread_local! {
	static COUNTING: Cell<bool> = const { Cell::new(false) };
	static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}

struct Counting;

// SAFETY: every call is forwarded to the system allocator, and the counters are thread-local
// cells that allocate nothing.
unsafe impl GlobalAlloc for Counting {
	unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
		if COUNTING.try_with(Cell::get).unwrap_or(false) {
			let _ = ALLOCATIONS.try_with(|count| count.set(count.get() + 1));
		}
		unsafe { System.alloc(layout) }
	}

	unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
		unsafe { System.dealloc(ptr, layout) }
	}

	unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
		if COUNTING.try_with(Cell::get).unwrap_or(false) {
			let _ = ALLOCATIONS.try_with(|count| count.set(count.get() + 1));
		}
		unsafe { System.realloc(ptr, layout, new_size) }
	}
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// The allocations `f` makes on this thread.
fn allocations<R>(f: impl FnOnce() -> R) -> (R, usize) {
	ALLOCATIONS.with(|count| count.set(0));
	COUNTING.with(|counting| counting.set(true));
	let result = f();
	COUNTING.with(|counting| counting.set(false));
	(result, ALLOCATIONS.with(Cell::get))
}

fn column(
	path: &[&str],
	kind: PhysicalKind,
	presence: Presence,
	location: Location,
	role: ColumnRole,
) -> Column {
	Column {
		path: path.iter().map(|segment| (*segment).to_owned()).collect(),
		kind,
		presence,
		location,
		role,
		literals: Vec::new(),
		tables: Vec::new(),
	}
}

/// An open root over every kind whose check builds nothing: integers, a float, a bool, a
/// datetime, a duration, a uuid, a string, a literal union and a nested open node, all but the
/// required ones optional.
///
/// Presence bits: 0 root remainder, 1 `b`, 2 `d`, 3 `f`, 4 `n`, 5 `n` remainder, 6 `n.x`, 7 `s`.
/// Fixed region: 1 presence byte, 1 bool byte, then `a`, `d`, `dur`, `f`, `l`, `n.x` and `u`.
fn descriptor() -> PackedDescriptor {
	let optional = |bit| Presence::Optional {
		bit,
	};
	let mut literal = column(
		&["l"],
		PhysicalKind::LiteralCode(1),
		Presence::Required,
		Location::Fixed(26),
		ColumnRole::Field,
	);
	literal.literals = vec![Value::String(Strand::new("red")), Value::String(Strand::new("green"))];
	let descriptor = PackedDescriptor {
		columns: vec![
			column(&[], PhysicalKind::Value, optional(0), Location::Var(0), ColumnRole::Remainder),
			column(
				&["a"],
				PhysicalKind::I64,
				Presence::Required,
				Location::Fixed(2),
				ColumnRole::Field,
			),
			column(&["b"], PhysicalKind::Bool, optional(1), Location::Bit(0), ColumnRole::Field),
			column(
				&["d"],
				PhysicalKind::Datetime,
				optional(2),
				Location::Fixed(10),
				ColumnRole::Field,
			),
			column(
				&["dur"],
				PhysicalKind::Duration,
				Presence::Required,
				Location::Fixed(18),
				ColumnRole::Field,
			),
			column(
				&["f"],
				PhysicalKind::F64,
				Presence::Nullable {
					bit: 3,
				},
				Location::Fixed(19),
				ColumnRole::Field,
			),
			literal,
			column(&["n"], PhysicalKind::Object, optional(4), Location::None, ColumnRole::Field),
			column(
				&["n"],
				PhysicalKind::Value,
				optional(5),
				Location::Var(1),
				ColumnRole::Remainder,
			),
			column(
				&["n", "x"],
				PhysicalKind::I32,
				optional(6),
				Location::Fixed(0),
				ColumnRole::Field,
			),
			column(&["s"], PhysicalKind::String, optional(7), Location::Var(2), ColumnRole::Field),
			column(
				&["u"],
				PhysicalKind::Uuid,
				Presence::Required,
				Location::Fixed(0),
				ColumnRole::Field,
			),
		],
		presence_bits: 8,
		bool_bits: 1,
		fixed_size: 0,
		var_count: 3,
	};
	layout_offsets(descriptor)
}

/// `descriptor` with its fixed slots laid out densely in column order after the bit regions, as a
/// valid descriptor has them.
fn layout_offsets(mut descriptor: PackedDescriptor) -> PackedDescriptor {
	let mut offset = descriptor.presence_bits.div_ceil(8) + descriptor.bool_bits.div_ceil(8);
	for column in &mut descriptor.columns {
		if let Location::Fixed(at) = &mut column.location {
			*at = offset;
			offset += column.kind.fixed_width().expect("a fixed kind");
		}
	}
	descriptor.fixed_size = offset;
	descriptor.validate().expect("the descriptor is valid");
	descriptor
}

fn object(entries: Vec<(&str, Value)>) -> Object {
	let mut object = Object::default();
	for (key, value) in entries {
		object.0.insert(Strand::new(key), value);
	}
	object
}

fn int(v: i64) -> Value {
	Value::Number(Number::Int(v))
}

fn row(descriptor: &PackedDescriptor, data: &Object) -> Vec<u8> {
	match pack(descriptor, Generation::FIRST, RowFlags::NONE, data).expect("a storable record") {
		Packing::Packed(bytes) => bytes,
		Packing::Misfit => panic!("the record does not fit the descriptor: {data:?}"),
	}
}

#[test]
fn checking_a_packed_row_allocates_nothing() {
	let descriptor = descriptor();
	let when: DateTime<Utc> = DateTime::from_timestamp(1_700_000_000, 5).expect("a datetime");
	let full = object(vec![
		("a", int(42)),
		("b", Value::Bool(true)),
		("d", Value::Datetime(val::Datetime(when))),
		("dur", Value::Duration(val::Duration(std::time::Duration::from_secs(90)))),
		("f", Value::Number(Number::Float(1.5))),
		("l", Value::String(Strand::new("green"))),
		(
			"n",
			Value::Object(object(vec![
				("x", Value::Number(Number::I32(7))),
				("extra", Value::String(Strand::new("e"))),
			])),
		),
		("s", Value::String(Strand::new("a string that is longer than twelve bytes"))),
		("u", Value::Uuid(val::Uuid(uuid::Uuid::from_u128(9)))),
		("extra", int(1)),
		("zz", Value::String(Strand::new("z"))),
	]);
	let sparse = object(vec![
		("a", int(1)),
		("dur", Value::Duration(val::Duration(std::time::Duration::from_secs(1)))),
		("f", Value::Null),
		("l", Value::String(Strand::new("red"))),
		("u", Value::Uuid(val::Uuid(uuid::Uuid::from_u128(1)))),
	]);
	// A datetime past the 8-byte slot is held in the trailer, which the check reads too.
	let far = object(vec![
		("a", int(1)),
		(
			"d",
			Value::Datetime(val::Datetime(
				DateTime::from_timestamp(20_000_000_000, 5).expect("a datetime"),
			)),
		),
		("dur", Value::Duration(val::Duration(std::time::Duration::from_secs(20_000_000_000)))),
		("f", Value::Null),
		("l", Value::String(Strand::new("red"))),
		("u", Value::Uuid(val::Uuid(uuid::Uuid::from_u128(1)))),
	]);
	for data in [full, sparse, far] {
		let bytes = row(&descriptor, &data);
		let record = StoredRecord::parse(&bytes).expect("parses").expect("v1");
		// One check outside the count, so a lazily built table is not charged to the row.
		record.checked_packed(&descriptor).expect("the row checks");
		let (checked, count) = allocations(|| {
			let mut checked = 0usize;
			for _ in 0..1_000 {
				checked += usize::from(record.checked_packed(&descriptor).is_ok());
			}
			checked
		});
		assert_eq!(checked, 1_000);
		assert_eq!(count, 0, "1000 checks allocated {count} times");
		// The counter sees the allocations of a decode of the same row, which builds its values.
		let (_, built) = allocations(|| record.decode_packed(&descriptor, None).expect("decodes"));
		assert!(built > 0, "the counting allocator counted no allocation of a decode");
	}
}
