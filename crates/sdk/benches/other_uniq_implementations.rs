use criterion::{Criterion, criterion_group, criterion_main};
use std::{
	collections::{BTreeSet, HashSet},
	hint::black_box,
};
use surrealdb_core::expr::{Array, Number, Value};

// Current implementation as of https://github.com/surrealdb/surrealdb/pull/6047
#[allow(clippy::mutable_key_type)]
fn current_uniq(mut array: Array) -> Array {
	let mut set: HashSet<&Value> = HashSet::new();
	let mut to_remove: Vec<usize> = Vec::new();
	for (i, item) in array.iter().enumerate() {
		if !set.insert(item) {
			to_remove.push(i);
		}
	}
	for i in to_remove.iter().rev() {
		array.remove(*i);
	}
	array
}

// About 30% faster than current_uniq
#[allow(clippy::mutable_key_type)]
fn uniq_hashset(array: Array) -> Array {
	let mut set = HashSet::with_capacity(array.len());
	let mut to_return = Array::with_capacity(array.len());
	for i in array.iter() {
		if set.insert(i) {
			to_return.push(i.clone());
		}
	}
	to_return
}

// Much slower, only included to compare
#[allow(clippy::mutable_key_type)]
fn uniq_btreeset(array: Array) -> Array {
	let mut set = BTreeSet::new();
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
	c.bench_function("current_uniq", |b| b.iter(|| current_uniq(black_box(array.clone()))));
	c.bench_function("uniq_hashset", |b| b.iter(|| uniq_hashset(black_box(array.clone()))));
	c.bench_function("uniq_btreeset", |b| b.iter(|| uniq_btreeset(black_box(array.clone()))));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
