#![allow(clippy::unwrap_used)]

mod common;

use common::{block_on, setup_datastore_with_records};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use futures::StreamExt;
use surrealdb_core::kvs::Direction::Forward;
use surrealdb_core::kvs::LockType::Optimistic;
use surrealdb_core::kvs::ScanLimit;
use surrealdb_core::kvs::TransactionType::Read;
use surrealdb_core::kvs::{Key, Scanner, Val};

// ============================================================================
// Benchmark: Scanner (Keys/KeyVal, Prefetch On/Off)
// ============================================================================

fn bench_scanner(c: &mut Criterion) {
	// Create the benchmark group
	let mut group = c.benchmark_group("scanner");
	// Setup the datastore with a query
	let (dbs, _ses) = block_on(setup_datastore_with_records(100_000));

	for count in [1, 10, 100, 1_000, 100_000, 1_000_000] {
		// Configure throughput for the benchmark
		group.throughput(Throughput::Elements(count));
		// Test all combinations of prefetch and scanner type
		for (prefetch_name, prefetch) in [("no_prefetch", false), ("prefetch", true)] {
			for (keysvals, keyval) in [("keys", false), ("keyvals", true)] {
				// Benchmark the query with the given parameter
				group.bench_with_input(
					BenchmarkId::new(format!("{}_{}", keysvals, prefetch_name), count),
					&count,
					|b, _cnt| {
						// Create a multithreaded runtime for async benchmarking
						let runtime = common::create_runtime();
						// Benchmark the query with the given parameter
						b.to_async(&runtime).iter(|| async {
							// Get the scanner range
							let range = vec![0x00]..vec![0xff];
							// Get the scanner limit
							let limit = Some(count as usize);
							// Create a new read transaction
							let tx = dbs.transaction(Read, Optimistic).await.unwrap();
							// Run the appropriate scanner
							if keyval {
								let scanner: Scanner<(Key, Val)> =
									Scanner::new(&tx, range, limit, Forward).prefetch(prefetch);
								std::hint::black_box(scanner.collect::<Vec<_>>().await);
							} else {
								let scanner: Scanner<Key> =
									Scanner::new(&tx, range, limit, Forward).prefetch(prefetch);
								std::hint::black_box(scanner.collect::<Vec<_>>().await);
							}
						});
					},
				);
			}
		}
	}

	group.finish();
}

// ============================================================================
// Benchmark: Scanner Batch Sizes (Initial/Subsequent, Count/Bytes)
// ============================================================================

fn bench_scanner_batch_sizes(c: &mut Criterion) {
	// Create the benchmark group
	let mut group = c.benchmark_group("scanner_batch_sizes");
	// Setup the datastore with a query
	let (dbs, _ses) = block_on(setup_datastore_with_records(1_000_000));
	// Use a fixed scan count for batch size benchmarks
	let count: u64 = 1_000_000;
	// Configure throughput for the benchmark
	group.throughput(Throughput::Elements(count));

	// Test different initial batch sizes (count-based)
	for initial_count in [100, 500, 1_000] {
		// Test different subsequent batch sizes (bytes-based)
		for (name, subsequent_batch_size) in [
			("100", ScanLimit::Count(100)),
			("1000", ScanLimit::Count(1_000)),
			("10000", ScanLimit::Count(10_000)),
			("100000", ScanLimit::Count(100_000)),
			("1MiB", ScanLimit::Bytes(1024 * 1024)),
			("4MiB", ScanLimit::Bytes(4 * 1024 * 1024)),
			("16MiB", ScanLimit::Bytes(16 * 1024 * 1024)),
			("64MiB", ScanLimit::Bytes(64 * 1024 * 1024)),
		] {
			// Configure the initial batch size
			let initial_batch_size = ScanLimit::Count(initial_count);
			// Benchmark with key-value scanning and prefetch enabled
			group.bench_with_input(
				BenchmarkId::new(format!("{initial_count}_{name}"), count),
				&count,
				|b, _cnt| {
					let runtime = common::create_runtime();
					b.to_async(&runtime).iter(|| async {
						let range = vec![0x00]..vec![0xff];
						let limit = Some(count as usize);
						let tx = dbs.transaction(Read, Optimistic).await.unwrap();
						let scanner: Scanner<(Key, Val)> = Scanner::new(&tx, range, limit, Forward)
							.initial_batch_size(initial_batch_size)
							.subsequent_batch_size(subsequent_batch_size)
							.prefetch(false);
						std::hint::black_box(scanner.collect::<Vec<_>>().await);
					});
				},
			);
		}
	}

	group.finish();
}

criterion_group!(
	name = benches;
	config = Criterion::default();
	targets = bench_scanner, bench_scanner_batch_sizes,
);
criterion_main!(benches);
