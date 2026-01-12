#![allow(clippy::unwrap_used)]

use std::collections::BTreeSet;
use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use surrealdb_types::{Array, Number, Value};

// Current implementation as of https://github.com/surrealdb/surrealdb/pull/6047
// surrealdb/core/src/expr/array.rs
#[allow(clippy::mutable_key_type)]
fn array_complement(first: Array, other: Array) -> Array {
	let mut out = Array::with_capacity(first.len());
	let mut set = BTreeSet::new();
	for i in other.iter() {
		set.insert(i);
	}
	for v in first {
		if !set.contains(&v) {
			out.push(v)
		}
	}
	out
}

fn criterion_benchmark(c: &mut Criterion) {
	let mut first = Array::new();
	for i in 0..5000 {
		first.push(Value::Number(Number::Int(i)));
		first.push(Value::String(i.to_string()));
	}
	let mut second = Array::new();
	for i in 0..2500 {
		second.push(Value::Number(Number::Int(i)));
		second.push(Value::String(i.to_string()));
	}
	c.bench_function("array_complement", |b| {
		b.iter(|| array_complement(black_box(first.clone()), black_box(second.clone())))
	});
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
