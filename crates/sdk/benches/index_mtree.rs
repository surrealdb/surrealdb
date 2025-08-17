use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use criterion::measurement::WallTime;
use criterion::{BenchmarkGroup, Criterion, Throughput, criterion_group, criterion_main};
use futures::future::join_all;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use reblessive::TreeStack;
use surrealdb_core::catalog::{Distance, MTreeParams, VectorType};
use surrealdb_core::ctx::MutableContext;
use surrealdb_core::idx::IndexKeyBase;
use surrealdb_core::idx::planner::checker::MTreeConditionChecker;
use surrealdb_core::idx::trees::mtree::MTreeIndex;
use surrealdb_core::kvs::LockType::Optimistic;
use surrealdb_core::kvs::TransactionType::{Read, Write};
use surrealdb_core::kvs::{Datastore, Transaction, TransactionType};
use surrealdb_core::val::{Number, RecordId, RecordIdKey, Value};
use tokio::runtime::{Builder, Runtime};
use tokio::task;

fn bench_index_mtree_combinations(c: &mut Criterion) {
	for (samples, dimension, cache) in [
		(1000, 3, 100),
		(1000, 3, 1000),
		(1000, 3, 0),
		(300, 50, 100),
		(300, 50, 300),
		(300, 50, 0),
		(150, 300, 50),
		(150, 300, 150),
		(150, 300, 0),
		(75, 1024, 25),
		(75, 1024, 75),
		(75, 1024, 0),
		(50, 2048, 20),
		(50, 2048, 50),
		(50, 2048, 0),
	] {
		bench_index_mtree(c, samples, dimension, cache);
	}
}

async fn mtree_index(
	tx: &Transaction,
	dimension: usize,
	cache_size: usize,
	tt: TransactionType,
) -> MTreeIndex {
	let p = MTreeParams {
		dimension: dimension as u16,
		distance: Distance::Euclidean,
		vector_type: VectorType::F64,
		capacity: 40,
		doc_ids_order: 100,
		doc_ids_cache: cache_size as u32,
		mtree_cache: cache_size as u32,
	};
	MTreeIndex::new(tx, IndexKeyBase::new(0, 0, "test", "test"), &p, tt).await.unwrap()
}

fn runtime() -> Runtime {
	Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap()
}

fn bench_index_mtree(
	c: &mut Criterion,
	samples_len: usize,
	vector_dimension: usize,
	cache_size: usize,
) {
	let samples_len = if cfg!(debug_assertions) {
		samples_len / 10 // Debug is slow
	} else {
		samples_len // Release is fast
	};

	// Both benchmark groups are sharing the same datastore
	let b = Builder::new_multi_thread().worker_threads(1).enable_all().build().unwrap();
	let ds = b.block_on(Datastore::new("memory")).unwrap();

	// Indexing benchmark group
	{
		let mut group = get_group(c, "index_mtree_insert", samples_len);
		let id = format!("len_{}_dim_{}_cache_{}", samples_len, vector_dimension, cache_size);
		group.bench_function(id, |b| {
			b.to_async(runtime())
				.iter(|| insert_objects(&ds, samples_len, vector_dimension, cache_size));
		});
		group.finish();
	}

	// Knn lookup benchmark group
	{
		let mut group = get_group(c, "index_mtree_lookup", samples_len);
		for knn in [1, 10] {
			let id = format!(
				"knn_{}_len_{}_dim_{}_cache_{}",
				knn, samples_len, vector_dimension, cache_size
			);
			group.bench_function(id, |b| {
				b.to_async(runtime()).iter(|| {
					knn_lookup_objects(&ds, samples_len / 5, vector_dimension, cache_size, knn)
				});
			});
		}
		group.finish();
	}
}

fn get_group<'a>(
	c: &'a mut Criterion,
	group_name: &str,
	samples_len: usize,
) -> BenchmarkGroup<'a, WallTime> {
	let mut group = c.benchmark_group(group_name);
	group.throughput(Throughput::Elements(samples_len as u64));
	group.sample_size(10);
	group
}
fn random_object(rng: &mut StdRng, vector_size: usize) -> Vec<Number> {
	let mut vec = Vec::with_capacity(vector_size);
	for _ in 0..vector_size {
		vec.push(rng.gen_range(-1.0..=1.0).into());
	}
	vec
}

async fn insert_objects(
	ds: &Datastore,
	samples_size: usize,
	vector_size: usize,
	cache_size: usize,
) {
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	let mut mt = mtree_index(&tx, vector_size, cache_size, Write).await;
	let mut stack = TreeStack::new();
	let mut rng = StdRng::from_entropy();
	stack
		.enter(|stk| async {
			for i in 0..samples_size {
				let vector: Vec<Number> = random_object(&mut rng, vector_size);
				// Insert the sample
				let rid = RecordId::new("test".to_string(), RecordIdKey::from(i as i64));
				mt.index_document(
					stk,
					&tx,
					&rid,
					&[vector.into_iter().map(Value::from).collect::<Value>()],
				)
				.await
				.unwrap();
			}
		})
		.finish()
		.await;
	mt.finish(&tx).await.unwrap();
	tx.commit().await.unwrap();
}

async fn knn_lookup_objects(
	ds: &Datastore,
	samples_size: usize,
	vector_size: usize,
	cache_size: usize,
	knn: usize,
) {
	let txn = ds.transaction(Read, Optimistic).await.unwrap();
	let db = txn.ensure_ns_db("myns", "mydb", false).await.unwrap();
	let mt = Arc::new(mtree_index(&txn, vector_size, cache_size, Read).await);
	let ctx = Arc::new(MutableContext::from(txn));

	let counter = Arc::new(AtomicUsize::new(0));

	let mut consumers = Vec::with_capacity(4);
	for _ in 0..4 {
		let (ctx, mt, counter, db) = (ctx.clone(), mt.clone(), counter.clone(), db.clone());
		let c = task::spawn(async move {
			let mut rng = StdRng::from_entropy();
			let mut stack = TreeStack::new();
			stack
				.enter(|stk| async {
					while counter.fetch_add(1, Ordering::Relaxed) < samples_size {
						let object = random_object(&mut rng, vector_size);
						let chk = MTreeConditionChecker::new(&ctx);
						let r = mt.knn_search(&db, stk, &ctx, &object, knn, chk).await.unwrap();
						assert_eq!(r.len(), knn);
					}
				})
				.finish()
				.await;
		});
		consumers.push(c);
	}
	for c in join_all(consumers).await {
		c.unwrap();
	}
}

criterion_group!(benches, bench_index_mtree_combinations);
criterion_main!(benches);
