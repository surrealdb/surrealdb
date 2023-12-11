use criterion::Criterion;
use once_cell::sync::Lazy;
use std::sync::OnceLock;
use tokio::runtime::Runtime;

mod lib;
mod sdk;

static NUM_OPS: Lazy<usize> =
	Lazy::new(|| std::env::var("BENCH_NUM_OPS").unwrap_or("1000".to_string()).parse().unwrap());
static DURATION_SECS: Lazy<u64> =
	Lazy::new(|| std::env::var("BENCH_DURATION").unwrap_or("30".to_string()).parse().unwrap());
static SAMPLE_SIZE: Lazy<usize> =
	Lazy::new(|| std::env::var("BENCH_SAMPLE_SIZE").unwrap_or("30".to_string()).parse().unwrap());
static WORKER_THREADS: Lazy<usize> =
	Lazy::new(|| std::env::var("BENCH_WORKER_THREADS").unwrap_or("1".to_string()).parse().unwrap());
static RUNTIME: OnceLock<Runtime> = OnceLock::new();

fn rt() -> &'static Runtime {
	RUNTIME.get_or_init(|| {
		tokio::runtime::Builder::new_multi_thread()
			.worker_threads(*WORKER_THREADS)
			.enable_all()
			.build()
			.unwrap()
	})
}

/// Create a benchmark group for the given target.
pub(super) fn benchmark_group(c: &mut Criterion, target: String) {
	println!("### Benchmark config: target={}, num_ops={}, duration={}, sample_size={}, worker_threads={} ###", target, *NUM_OPS, *DURATION_SECS, *SAMPLE_SIZE, *WORKER_THREADS);

	match &target {
		t if t.starts_with("lib") => lib::benchmark_group(c, target),
		t if t.starts_with("sdk") => sdk::benchmark_group(c, target),
		t => panic!("Target '{}' not supported.", t),
	}
}
