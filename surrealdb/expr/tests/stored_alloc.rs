//! The in-place readers of the stored-value grammar allocate nothing.
//!
//! [`ValueRef`] and the views it hands out borrow the encoded bytes: a field
//! looked up by name, a scalar compared, an element indexed or a typed array
//! iterated costs no heap allocation on the success path. A scan that reads one
//! field of every row depends on that, so it is a property of the readers and
//! not an optimisation. Only [`ValueRef::decode`] and the owned conversions
//! allocate.
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::collections::BTreeMap;

use surrealdb_expr::val::stored::{ValueRef, encode};
use surrealdb_expr::val::{Array, Number, Object, RecordId, RecordIdKey, TableName, Value};
use surrealdb_strand::Strand;

struct CountingAllocator;

thread_local! {
	static COUNT: Cell<Option<usize>> = const { Cell::new(None) };
}

fn count() {
	let _ = COUNT.try_with(|c| {
		if let Some(n) = c.get() {
			c.set(Some(n + 1));
		}
	});
}

unsafe impl GlobalAlloc for CountingAllocator {
	unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
		count();
		unsafe { System.alloc(layout) }
	}
	unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
		count();
		unsafe { System.alloc_zeroed(layout) }
	}
	unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
		count();
		unsafe { System.realloc(ptr, layout, size) }
	}
	unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
		unsafe { System.dealloc(ptr, layout) }
	}
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

/// The result of `f` and the allocations this thread made while it ran.
fn allocations<T>(f: impl FnOnce() -> T) -> (T, usize) {
	COUNT.with(|c| c.set(Some(0)));
	let value = f();
	let count = COUNT.with(|c| c.replace(None).expect("count enabled"));
	(value, count)
}

fn int(i: i64) -> Value {
	Value::Number(Number::Int(i))
}

fn float(f: f64) -> Value {
	Value::Number(Number::Float(f))
}

fn string(s: &str) -> Value {
	Value::String(Strand::new(s))
}

fn row() -> Value {
	let entries: BTreeMap<&str, Value> = BTreeMap::from([
		(
			"id",
			Value::RecordId(RecordId {
				table: TableName::new("person"),
				key: RecordIdKey::String(Strand::new("tobie")),
			}),
		),
		("age", int(41)),
		("name", string("Tobie, whose name is longer than any inline string")),
		("score", float(9.5)),
		("floats", Value::Array(Array((0..16_i32).map(|i| float(f64::from(i) + 0.25)).collect()))),
		("ints", Value::Array(Array((0..16_i64).map(|i| int(i * 1000)).collect()))),
		("nested", Value::Object(Object::from(BTreeMap::from([("depth", int(2))])))),
	]);
	Value::Object(Object::from(entries))
}

#[test]
fn fields_and_scalars_are_read_without_allocating() {
	let bytes = encode(&row()).expect("encodes");
	let (answers, n) = allocations(|| {
		let obj = ValueRef::new(&bytes).unwrap().as_ordered_object().unwrap().unwrap();
		let mut hint = u32::MAX;
		let age = obj.get_hinted("age", &mut hint).unwrap().unwrap().as_int().unwrap();
		let name_len = obj.get("name").unwrap().unwrap().as_str().unwrap().map(str::len);
		let score = obj.get("score").unwrap().unwrap().as_float().unwrap();
		let absent = obj.get("missing").unwrap().is_none();
		let (table, key) = obj.get("id").unwrap().unwrap().as_record_id().unwrap().unwrap();
		let table_len = table.len();
		let key_len = key.as_str().unwrap().map(str::len);
		let depth = obj
			.get("nested")
			.unwrap()
			.unwrap()
			.as_ordered_object()
			.unwrap()
			.unwrap()
			.get("depth")
			.unwrap()
			.unwrap()
			.as_int()
			.unwrap();
		let names = obj.iter().map(|e| e.unwrap().0.len()).sum::<usize>();
		(age, name_len, score, absent, table_len, key_len, depth, names)
	});
	assert_eq!(n, 0, "in-place field reads allocated");
	assert_eq!(answers.0, Some(41));
	assert_eq!(answers.1, Some("Tobie, whose name is longer than any inline string".len()));
	assert_eq!(answers.2, Some(9.5));
	assert!(answers.3);
	assert_eq!(answers.4, "person".len());
	assert_eq!(answers.5, Some("tobie".len()));
	assert_eq!(answers.6, Some(2));
	assert!(answers.7 > 0);
}

#[test]
fn typed_arrays_are_indexed_and_iterated_without_allocating() {
	let bytes = encode(&row()).expect("encodes");
	let (answers, n) = allocations(|| {
		let obj = ValueRef::new(&bytes).unwrap().as_ordered_object().unwrap().unwrap();
		let floats = obj.get("floats").unwrap().unwrap().as_array().unwrap().unwrap();
		let ints = obj.get("ints").unwrap().unwrap().as_array().unwrap().unwrap();
		let float_sum: f64 = floats.floats().unwrap().sum();
		let int_sum: i64 = ints.ints().unwrap().sum();
		let third = ints.get(3).unwrap().unwrap().as_int().unwrap();
		let past_the_end = ints.get(ints.len()).unwrap().is_none();
		(float_sum, int_sum, third, past_the_end)
	});
	assert_eq!(n, 0, "in-place array reads allocated");
	assert_eq!(answers.0, (0..16_i32).map(|i| f64::from(i) + 0.25).sum::<f64>());
	assert_eq!(answers.1, (0..16_i64).map(|i| i * 1000).sum::<i64>());
	assert_eq!(answers.2, Some(3000));
	assert!(answers.3);
}

#[test]
fn the_guard_sees_an_allocation_when_there_is_one() {
	let bytes = encode(&row()).expect("encodes");
	let (decoded, n) = allocations(|| ValueRef::new(&bytes).unwrap().decode().unwrap());
	assert!(n > 0, "a full decode allocates, or the guard counts nothing");
	assert!(matches!(decoded, Value::Object(_)));
}
