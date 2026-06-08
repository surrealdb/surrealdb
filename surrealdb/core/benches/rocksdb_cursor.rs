//! RocksDB-direct cursor benchmarks.
//!
//! The existing `scanner` bench uses the in-memory backend, which is too
//! fast to expose the per-batch lock costs we're trying to optimise.
//! These benches construct a real RocksDB datastore on a temp dir and
//! drive the cursor API directly. Two workloads:
//!
//! * **Nested edge** — open many cursors back-to-back, each over a small prefix (simulates `SELECT
//!   ->knows FROM person`). Measures cursor open + 1-batch advance + drop, repeated N times.
//!   Per-cursor overhead dominates.
//! * **Bulk scan** — one cursor over the entire range. Measures per-batch advance overhead
//!   amortised across many items.

#![allow(clippy::unwrap_used)]

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use surrealdb_core::CommunityComposer;
use surrealdb_core::kvs::Direction::Forward;
use surrealdb_core::kvs::LockType::Optimistic;
use surrealdb_core::kvs::{Datastore, ScanLimit, TransactionType, Transactor};
use temp_dir::TempDir;
use tokio::runtime::Runtime;

fn runtime() -> Runtime {
	tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap()
}

/// Seed a rocksdb-backed datastore with `prefix_count` prefixes, each
/// containing `per_prefix` keys (raw KV, not SurrealQL). Returns the
/// datastore and the temp dir (keep alive for the bench's lifetime).
fn setup(prefix_count: usize, per_prefix: usize) -> (Datastore, TempDir) {
	let tmp = TempDir::new().unwrap();
	let path = format!("rocksdb:{}", tmp.path().to_string_lossy());
	let ds = runtime().block_on(async {
		let ds =
			Datastore::builder().build_with_factory_path(&path, CommunityComposer()).await.unwrap();
		let tx = ds.transaction(TransactionType::Write, Optimistic).await.unwrap();
		// Insert raw bytes via the Transactor (`tx.set` on the typed
		// Transaction wrapper now requires a `KVKey`; we want raw byte
		// keys for the bench, so go one level lower).
		let tr: &Transactor = &tx;
		for p in 0..prefix_count {
			for k in 0..per_prefix {
				let key = format!("p_{p:06}/k_{k:06}");
				tr.set(key.into_bytes(), vec![0u8; 8]).await.unwrap();
			}
		}
		tx.commit().await.unwrap();
		ds
	});
	(ds, tmp)
}

fn prefix_range(prefix: &str) -> std::ops::Range<Vec<u8>> {
	let start = prefix.as_bytes().to_vec();
	let mut end = start.clone();
	end.push(0xff);
	start..end
}

/// Nested edge: N small cursors. Each opens, takes one batch of small
/// size, drops. Per-cursor-open cost dominates — this is the
/// "`SELECT ->knows FROM person` with N outer rows" hot path.
fn bench_nested_edge(c: &mut Criterion) {
	let mut group = c.benchmark_group("rocksdb_nested_edge");
	for prefix_count in [100usize, 1_000, 10_000] {
		// 5 keys per prefix — the user's stated case (5 edges per row).
		let (ds, _tmp) = setup(prefix_count, 5);
		group.throughput(Throughput::Elements(prefix_count as u64));
		group.bench_with_input(
			BenchmarkId::new("open+1batch+drop", prefix_count),
			&prefix_count,
			|b, &prefix_count| {
				let rt = runtime();
				b.to_async(&rt).iter(|| async {
					let tx = ds.transaction(TransactionType::Read, Optimistic).await.unwrap();
					let tr: &Transactor = &tx;
					let mut count = 0u64;
					for p in 0..prefix_count {
						let rng = prefix_range(&format!("p_{p:06}/"));
						let mut cursor =
							tr.open_keys_cursor(rng, Forward, 0u32, None).await.unwrap();
						let batch = cursor.next_batch(ScanLimit::Count(10)).await.unwrap();
						count += batch.len() as u64;
						drop(cursor);
					}
					assert!(count > 0);
					tx.cancel().await.unwrap();
				});
			},
		);
	}
	group.finish();
}

/// Bulk scan: one cursor, many keys. Amortises per-cursor cost across
/// many batches. Measures per-batch advance cost.
fn bench_bulk_scan(c: &mut Criterion) {
	let mut group = c.benchmark_group("rocksdb_bulk_scan");
	let (ds, _tmp) = setup(1, 1_000_000);
	for count in [100_000u64, 1_000_000] {
		group.throughput(Throughput::Elements(count));
		group.bench_with_input(BenchmarkId::new("next_batch_2000", count), &count, |b, &count| {
			let rt = runtime();
			b.to_async(&rt).iter(|| async {
				let tx = ds.transaction(TransactionType::Read, Optimistic).await.unwrap();
				let tr: &Transactor = &tx;
				let rng = prefix_range("p_000000/");
				let mut cursor = tr.open_keys_cursor(rng, Forward, 0u32, None).await.unwrap();
				let mut total = 0u64;
				while total < count {
					let batch = cursor.next_batch(ScanLimit::Count(2000)).await.unwrap();
					if batch.is_empty() {
						break;
					}
					total += batch.len() as u64;
				}
				drop(cursor);
				assert_eq!(total, count.min(1_000_000));
				tx.cancel().await.unwrap();
			});
		});
		// `borrowed_iter_2000`: drive the same cursor as `next_batch_2000`
		// but iterate the borrowed `Vec<&[u8]>` directly without copying
		// each key into an owned `Vec<u8>`. With the per-batch allocation
		// now a single `Vec<&[u8]>` (not N `Vec<u8>`s), this should be
		// strictly faster than the legacy owned-batch shape.
		group.bench_with_input(
			BenchmarkId::new("borrowed_iter_2000", count),
			&count,
			|b, &count| {
				let rt = runtime();
				b.to_async(&rt).iter(|| async {
					let tx = ds.transaction(TransactionType::Read, Optimistic).await.unwrap();
					let tr: &Transactor = &tx;
					let rng = prefix_range("p_000000/");
					let mut cursor = tr.open_keys_cursor(rng, Forward, 0u32, None).await.unwrap();
					let mut total = 0u64;
					while total < count {
						let batch = cursor.next_batch(ScanLimit::Count(2000)).await.unwrap();
						if batch.is_empty() {
							break;
						}
						total += batch.len() as u64;
						for k in &batch {
							std::hint::black_box(k);
						}
					}
					drop(cursor);
					assert_eq!(total, count.min(1_000_000));
					tx.cancel().await.unwrap();
				});
			},
		);
	}
	group.finish();
}

criterion_group!(benches, bench_nested_edge, bench_bulk_scan);
criterion_main!(benches);
