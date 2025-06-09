use criterion::{Criterion, criterion_group, criterion_main};
use std::{
	collections::{BTreeMap, HashMap, HashSet},
	hint::black_box,
};
use surrealdb_core::expr::{Array, Number, Value};

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
fn uniq_hashmap(array: Array) -> Array {
	let mut map: HashMap<&Value, ()> = HashMap::with_capacity(array.len());
	let mut to_return = Array::with_capacity(array.len());
	for i in array.iter() {
		if map.insert(i, ()).is_some() {
			to_return.push(i.clone());
		}
	}
	to_return
}

// Much slower, only included to compare
fn uniq_btreemap(array: Array) -> Array {
	let mut map: BTreeMap<&Value, ()> = BTreeMap::new();
	let mut to_return = Array::with_capacity(array.len());
	for i in array.iter() {
		if map.insert(i, ()).is_some() {
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
	for i in 100000..0 {
		array.push(Value::Number(Number::Int(i)));
		array.push(i.to_string().into());
	}
	c.bench_function("current_uniq", |b| b.iter(|| current_uniq(black_box(array.clone()))));
	c.bench_function("uniq_hashmap", |b| b.iter(|| uniq_hashmap(black_box(array.clone()))));
	c.bench_function("uniq_btreemap", |b| b.iter(|| uniq_btreemap(black_box(array.clone()))));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
