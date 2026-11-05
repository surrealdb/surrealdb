#![allow(clippy::unwrap_used)]
#![recursion_limit = "256"]
// TODO: Remove once this crate no longer needs it; see `absolute_paths` in the workspace lints.
#![allow(clippy::absolute_paths)]

mod sdb_benches;

use criterion::{Criterion, criterion_group, criterion_main};

fn bench(c: &mut Criterion) {
	let target = std::env::var("BENCH_DATASTORE_TARGET").unwrap_or("sdk-mem".to_string());

	sdb_benches::benchmark_group(c, target);
}

criterion_group!(benches, bench);
criterion_main!(benches);
