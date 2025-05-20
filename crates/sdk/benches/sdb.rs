mod sdb_benches;

use criterion::{Criterion, criterion_group, criterion_main};
use pprof::criterion::{Output, PProfProfiler};

fn bench(c: &mut Criterion) {
	let target = std::env::var("BENCH_DATASTORE_TARGET").unwrap_or("lib-mem".to_string());

	sdb_benches::benchmark_group(c, target);
}

criterion_group!(
	name = benches;
	config = Criterion::default().with_profiler(PProfProfiler::new(1000, Output::Flamegraph(None)));
	targets = bench
);
criterion_main!(benches);
