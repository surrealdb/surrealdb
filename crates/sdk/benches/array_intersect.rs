use std::collections::VecDeque;
use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use rand::Rng;
use surrealdb_core::val::{Array, Number, Value};

// Current implementation as of https://github.com/surrealdb/surrealdb/pull/6047
// crates/core/src/expr/array.rs
#[allow(clippy::mutable_key_type)]
fn array_intersect(first: Array, other: Array) -> Array {
	let len = match (first.len(), other.len()) {
		(first_len, other_len) if first_len > other_len => first_len,
		(_, other_len) => other_len,
	};
	let mut out = Array::with_capacity(len);
	let mut other = VecDeque::from(other.0);
	for v in first.into_iter() {
		if let Some(pos) = other.iter().position(|w| v == *w) {
			other.remove(pos);
			out.push(v);
		}
	}
	out.append(&mut Vec::from(other));
	out
}

fn criterion_benchmark(c: &mut Criterion) {
	let mut first = Array::new();
	let mut rng = rand::thread_rng();
	for _ in 0..5000 {
		first.push(Value::Number(Number::Int(rng.gen_range(0..=5000))));
		first.push(char::from_u32(rng.gen_range(0..=5000)).unwrap().to_string().into());
	}
	let mut second = Array::new();
	for _ in 0..5000 {
		second.push(Value::Number(Number::Int(rng.gen_range(0..=5000))));
		second.push(char::from_u32(rng.gen_range(0..=5000)).unwrap().to_string().into());
	}
	c.bench_function("array_intersect", |b| {
		b.iter(|| array_intersect(black_box(first.clone()), black_box(second.clone())))
	});
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
