use std::collections::HashSet;
use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use surrealdb_core::val::{Array, Number, Value};

// Current implementation as of https://github.com/surrealdb/surrealdb/pull/6047
// crates/core/src/expr/array.rs#L439
#[allow(clippy::mutable_key_type)]
fn array_uniq(array: Array) -> Array {
	let mut set = HashSet::with_capacity(array.len());
	let mut to_return = Array::with_capacity(array.len());
	for i in array.iter() {
		if set.insert(i) {
			to_return.push(i.clone());
		}
	}
	to_return
}

fn criterion_benchmark(c: &mut Criterion) {
	let mut array = Array::new();
	for i in 0..100000 {
		array.push(Value::Number(Number::Int(i)));
		array.push(i.to_string().into());
	}
	for i in (0..100000).rev() {
		array.push(Value::Number(Number::Int(i)));
		array.push(i.to_string().into());
	}
	c.bench_function("array_uniq", |b| b.iter(|| array_uniq(black_box(array.clone()))));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
