use criterion::measurement::WallTime;
use criterion::{criterion_group, criterion_main, BenchmarkGroup, Criterion, Throughput};
use futures::executor::block_on;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use reblessive::TreeStack;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use surrealdb::kvs::Datastore;
use surrealdb::kvs::LockType::Optimistic;
use surrealdb::kvs::TransactionType::{Read, Write};
use surrealdb_core::ctx::Context;
use surrealdb_core::idx::planner::checker::MTreeConditionChecker;
use surrealdb_core::idx::trees::mtree::MTreeIndex;
use surrealdb_core::idx::IndexKeyBase;
use surrealdb_core::kvs::{Transaction, TransactionType};
use surrealdb_core::sql::index::{Distance, MTreeParams, VectorType};
use surrealdb_core::sql::{Id, Number, Thing, Value};
use tokio::runtime::{Builder, Runtime};
use tokio::task;

fn bench_index_mtree_dim_3(c: &mut Criterion) {
	bench_index_mtree(c, 250, 2500, 3, 100);
}

fn bench_index_mtree_dim_3_full_cache(c: &mut Criterion) {
	bench_index_mtree(c, 250, 2500, 3, 0);
}

fn bench_index_mtree_dim_50(c: &mut Criterion) {
	bench_index_mtree(c, 100, 1000, 50, 100);
}

fn bench_index_mtree_dim_50_full_cache(c: &mut Criterion) {
	bench_index_mtree(c, 100, 1000, 50, 0);
}

fn bench_index_mtree_dim_300(c: &mut Criterion) {
	bench_index_mtree(c, 50, 500, 300, 100);
}

fn bench_index_mtree_dim_300_full_cache(c: &mut Criterion) {
	bench_index_mtree(c, 50, 500, 300, 0);
}

fn bench_index_mtree_dim_2048(c: &mut Criterion) {
	bench_index_mtree(c, 10, 100, 2048, 100);
}

fn bench_index_mtree_dim_2048_full_cache(c: &mut Criterion) {
	bench_index_mtree(c, 10, 100, 2048, 0);
}

async fn mtree_index(
	ds: &Datastore,
	tx: &Transaction,
	dimension: usize,
	cache_size: usize,
	tt: TransactionType,
) -> MTreeIndex {
	let p = MTreeParams::new(
		dimension as u16,
		Distance::Euclidean,
		VectorType::F64,
		40,
		100,
		cache_size as u32,
		cache_size as u32,
	);
	MTreeIndex::new(ds.index_store(), tx, IndexKeyBase::default(), &p, tt).await.unwrap()
}

fn runtime() -> Runtime {
	Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap()
}

fn bench_index_mtree(
	c: &mut Criterion,
	debug_samples_len: usize,
	release_samples_len: usize,
	vector_dimension: usize,
	cache_size: usize,
) {
	let samples_len = if cfg!(debug_assertions) {
		debug_samples_len // Debug is slow
	} else {
		release_samples_len // Release is fast
	};

	// Both benchmark groups are sharing the same datastore
	let ds = block_on(Datastore::new("memory")).unwrap();

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
					knn_lookup_objects(&ds, samples_len, vector_dimension, cache_size, knn)
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
	let mut mt = mtree_index(ds, &tx, vector_size, cache_size, Write).await;
	let mut stack = TreeStack::new();
	let mut rng = StdRng::from_entropy();
	stack
		.enter(|stk| async {
			for i in 0..samples_size {
				let vector: Vec<Number> = random_object(&mut rng, vector_size);
				// Insert the sample
				let rid = Thing::from(("test", Id::from(i as i64)));
				mt.index_document(stk, &tx, &rid, &vec![Value::from(vector)]).await.unwrap();
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
	let mt = Arc::new(mtree_index(ds, &txn, vector_size, cache_size, Read).await);
	let ctx = Arc::new(Context::from(txn));

	let counter = Arc::new(AtomicUsize::new(0));

	let mut consumers = Vec::with_capacity(4);
	for _ in 0..4 {
		let (ctx, mt, counter) = (ctx.clone(), mt.clone(), counter.clone());
		let c = task::spawn(async move {
			let mut rng = StdRng::from_entropy();
			while counter.fetch_add(1, Ordering::Relaxed) < samples_size {
				let object = random_object(&mut rng, vector_size);
				knn_lookup_object(mt.as_ref(), &ctx, object, knn).await;
			}
		});
		consumers.push(c);
	}
	for c in consumers {
		c.await.unwrap();
	}
}

async fn knn_lookup_object(mt: &MTreeIndex, ctx: &Context<'_>, object: Vec<Number>, knn: usize) {
	let mut stack = TreeStack::new();
	stack
		.enter(|stk| async {
			let chk = MTreeConditionChecker::new(ctx);
			let r = mt.knn_search(stk, ctx, &object, knn, chk).await.unwrap();
			assert_eq!(r.len(), knn);
		})
		.finish()
		.await;
}

criterion_group!(
	benches,
	bench_index_mtree_dim_3,
	bench_index_mtree_dim_3_full_cache,
	bench_index_mtree_dim_50,
	bench_index_mtree_dim_50_full_cache,
	bench_index_mtree_dim_300,
	bench_index_mtree_dim_300_full_cache,
	bench_index_mtree_dim_2048,
	bench_index_mtree_dim_2048_full_cache
);
criterion_main!(benches);
