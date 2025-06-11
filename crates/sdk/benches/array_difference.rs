use criterion::{Criterion, criterion_group, criterion_main};
use std::{collections::VecDeque, hint::black_box};
use surrealdb_core::expr::{Array, Number, Value};

fn old_array_difference(first: Array, mut other: Array) -> Array {
	let mut out = Array::new();
	for v in first.into_iter() {
		if let Some(pos) = other.iter().position(|w| v == *w) {
			other.remove(pos);
		} else {
			out.push(v);
		}
	}
	out.append(&mut other);
	out
}

fn array_difference(first: Array, other: Array) -> Array {
	let mut out = Array::with_capacity(first.len() + other.len());
	let mut other = VecDeque::from(other.0);
	for v in first.into_iter() {
		if let Some(pos) = other.iter().position(|w| v == *w) {
			other.remove(pos);
		} else {
			out.push(v);
		}
	}
	out.append(&mut Vec::from(other));
	out
}

fn criterion_benchmark(c: &mut Criterion) {
	let mut first = Array::new();
	for i in 0..5000 {
		first.push(Value::Number(Number::Int(i)));
		first.push(i.to_string().into());
	}
	let mut second = Array::new();
	for i in 0..2500 {
		second.push(Value::Number(Number::Int(i)));
		second.push(i.to_string().into());
	}
	c.bench_function("old_array_difference", |b| {
		b.iter(|| old_array_difference(black_box(first.clone()), black_box(second.clone())))
	});
	c.bench_function("array_difference", |b| {
		b.iter(|| array_difference(black_box(first.clone()), black_box(second.clone())))
	});
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
