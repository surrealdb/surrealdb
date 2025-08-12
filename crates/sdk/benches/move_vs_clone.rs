use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};

const ITERATIONS: u32 = 1_000_000;

fn bench_move() {
	let mut value = Arc::new(AtomicU32::new(0));
	for _ in 0..ITERATIONS {
		value = do_something_with_move(value);
	}
	assert_eq!(value.load(Ordering::Relaxed), ITERATIONS);
}

fn do_something_with_move(value: Arc<AtomicU32>) -> Arc<AtomicU32> {
	value.fetch_add(1, Ordering::Relaxed);
	value
}

fn bench_clone() {
	let value = Arc::new(AtomicU32::new(0));
	for _ in 0..ITERATIONS {
		do_something_with_clone(value.clone());
	}
	assert_eq!(value.load(Ordering::Relaxed), ITERATIONS);
}

fn do_something_with_clone(value: Arc<AtomicU32>) {
	value.fetch_add(1, Ordering::Relaxed);
}

fn bench_move_vs_clone(c: &mut Criterion) {
	let mut group = c.benchmark_group("move_vs_clone");
	group.throughput(Throughput::Elements(ITERATIONS as u64));
	group.sample_size(10);
	group.measurement_time(Duration::from_secs(10));

	group.bench_function("move", |b| {
		b.iter(bench_move);
	});

	group.bench_function("clone", |b| {
		b.iter(bench_clone);
	});

	group.finish();
}

criterion_group!(benches, bench_move_vs_clone);
criterion_main!(benches);
