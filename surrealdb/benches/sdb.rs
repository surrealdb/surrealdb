#![allow(clippy::unwrap_used)]

mod sdb_benches;

use criterion::{Criterion, criterion_group, criterion_main};

fn bench(c: &mut Criterion) {
	let target = std::env::var("BENCH_DATASTORE_TARGET").unwrap_or("lib-mem".to_string());

	sdb_benches::benchmark_group(c, target);
}

criterion_group!(benches, bench);
criterion_main!(benches);
