//! Benchmarks the datastore driven directly, without the SDK in the way.
//!
//! The backend is selected at runtime from `BENCH_DATASTORE_TARGET` (one of
//! `lib-mem`, `lib-rocksdb`, `lib-surrealkv`); the matching `kv-*` feature must
//! be enabled for that target to be available. The SDK-facing half of the same
//! comparison lives in the `surrealdb` crate's `sdb` benchmark.

#![allow(clippy::unwrap_used)]
#![recursion_limit = "256"]

use std::sync::{Arc, LazyLock, OnceLock};
use std::time::Duration;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
#[cfg(any(feature = "kv-mem", feature = "kv-rocksdb", feature = "kv-surrealkv"))]
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use tokio::runtime::Runtime;

mod routines;

static NUM_OPS: LazyLock<usize> =
	LazyLock::new(|| std::env::var("BENCH_NUM_OPS").unwrap_or("1000".to_string()).parse().unwrap());
static DURATION_SECS: LazyLock<u64> =
	LazyLock::new(|| std::env::var("BENCH_DURATION").unwrap_or("30".to_string()).parse().unwrap());
static SAMPLE_SIZE: LazyLock<usize> = LazyLock::new(|| {
	std::env::var("BENCH_SAMPLE_SIZE").unwrap_or("30".to_string()).parse().unwrap()
});
static WORKER_THREADS: LazyLock<usize> = LazyLock::new(|| {
	std::env::var("BENCH_WORKER_THREADS").unwrap_or("1".to_string()).parse().unwrap()
});
static RUNTIME: OnceLock<Runtime> = OnceLock::new();

static DB: OnceLock<Arc<Datastore>> = OnceLock::new();

fn rt() -> &'static Runtime {
	RUNTIME.get_or_init(|| {
		tokio::runtime::Builder::new_multi_thread()
			.worker_threads(*WORKER_THREADS)
			.enable_all()
			.build()
			.unwrap()
	})
}

async fn init(target: &str) {
	match target {
		#[cfg(feature = "kv-mem")]
		"lib-mem" => {
			let ds = Datastore::builder()
				.without_maintenance_tasks()
				.build_with_path("memory")
				.await
				.unwrap();
			// Define namespace and database for benchmarks
			ds.execute("DEFINE NAMESPACE test", &Session::owner(), None)
				.await
				.expect("Unable to define namespace");
			ds.execute("DEFINE DATABASE test", &Session::owner().with_ns("test"), None)
				.await
				.expect("Unable to define database");
			let _ = DB.set(ds);
		}
		#[cfg(feature = "kv-rocksdb")]
		"lib-rocksdb" => {
			let path = format!(
				"rocksdb://lib-rocksdb-{}.db",
				web_time::SystemTime::now()
					.duration_since(web_time::UNIX_EPOCH)
					.unwrap()
					.as_millis()
			);
			println!("\n### Using path: {} ###\n", path);
			let ds = Datastore::builder()
				.without_maintenance_tasks()
				.build_with_path(&path)
				.await
				.unwrap();
			// Define namespace and database for benchmarks
			ds.execute("DEFINE NAMESPACE test", &Session::owner(), None)
				.await
				.expect("Unable to define namespace");
			ds.execute("DEFINE DATABASE test", &Session::owner().with_ns("test"), None)
				.await
				.expect("Unable to define database");
			let _ = DB.set(ds);
		}
		#[cfg(feature = "kv-surrealkv")]
		"lib-surrealkv" => {
			let path = format!(
				"surrealkv://lib-surrealkv-{}.db",
				web_time::SystemTime::now()
					.duration_since(web_time::UNIX_EPOCH)
					.unwrap()
					.as_millis()
			);
			println!("\n### Using path: {} ###\n", path);
			let ds = Datastore::builder()
				.without_maintenance_tasks()
				.build_with_path(&path)
				.await
				.unwrap();
			// Define namespace and database for benchmarks
			ds.execute("DEFINE NAMESPACE test", &Session::owner(), None)
				.await
				.expect("Unable to define namespace");
			ds.execute("DEFINE DATABASE test", &Session::owner().with_ns("test"), None)
				.await
				.expect("Unable to define database");
			let _ = DB.set(ds);
		}
		t if t.starts_with("sdk") => panic!(
			"Target '{t}' drives the SDK, not the datastore. Run it from the `surrealdb` crate: `cargo bench --package surrealdb --bench sdb`."
		),
		_ => panic!("Unknown target: {}", target),
	}
}

fn bench(c: &mut Criterion) {
	let target = std::env::var("BENCH_DATASTORE_TARGET").unwrap_or("lib-mem".to_string());

	println!(
		"### Benchmark config: target={}, num_ops={}, duration={}, sample_size={}, worker_threads={} ###",
		target, *NUM_OPS, *DURATION_SECS, *SAMPLE_SIZE, *WORKER_THREADS
	);

	let num_ops = *NUM_OPS;
	let runtime = rt();

	runtime.block_on(async { init(&target).await });

	let mut group = c.benchmark_group(target);

	group.measurement_time(Duration::from_secs(*DURATION_SECS));
	group.sample_size(*SAMPLE_SIZE);
	group.throughput(Throughput::Elements(1));

	group.bench_function("reads", |b| {
		let read = routines::Read::new(rt());
		routines::bench_routine(b, DB.get().unwrap(), &read, num_ops)
	});
	group.bench_function("creates", |b| {
		let create = routines::Create::new(rt());
		routines::bench_routine(b, DB.get().unwrap(), &create, num_ops)
	});
	group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
