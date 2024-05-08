use criterion::measurement::WallTime;
use criterion::{criterion_group, criterion_main, BenchmarkGroup, Criterion, Throughput};
use futures::executor::block_on;
use rand::prelude::ThreadRng;
use rand::{thread_rng, Rng};
use reblessive::TreeStack;
use std::time::Duration;
use surrealdb::idx::docids::DocId;
use surrealdb::idx::trees::mtree::{MState, MTree};
use surrealdb::idx::trees::store::TreeNodeProvider;
use surrealdb::idx::trees::vector::Vector;
use surrealdb::kvs::Datastore;
use surrealdb::kvs::LockType::Optimistic;
use surrealdb::kvs::TransactionType::{Read, Write};
use surrealdb::sql::index::Distance;
use tokio::runtime::Runtime;

fn bench_index_mtree_dim_3(c: &mut Criterion) {
	bench_index_mtree(c, 1_000, 100_000, 3, 120, 100);
}

fn bench_index_mtree_dim_3_full_cache(c: &mut Criterion) {
	bench_index_mtree(c, 1_000, 100_000, 3, 120, 0);
}

fn bench_index_mtree_dim_50(c: &mut Criterion) {
	bench_index_mtree(c, 100, 10_000, 50, 20, 100);
}

fn bench_index_mtree_dim_50_full_cache(c: &mut Criterion) {
	bench_index_mtree(c, 100, 10_000, 50, 20, 0);
}

fn bench_index_mtree_dim_300(c: &mut Criterion) {
	bench_index_mtree(c, 50, 5_000, 300, 40, 100);
}

fn bench_index_mtree_dim_300_full_cache(c: &mut Criterion) {
	bench_index_mtree(c, 50, 5_000, 300, 40, 0);
}

fn bench_index_mtree_dim_2048(c: &mut Criterion) {
	bench_index_mtree(c, 10, 1_000, 2048, 60, 100);
}

fn bench_index_mtree_dim_2048_full_cache(c: &mut Criterion) {
	bench_index_mtree(c, 10, 1_000, 2048, 60, 0);
}

fn bench_index_mtree(
	c: &mut Criterion,
	debug_samples_len: usize,
	release_samples_len: usize,
	vector_dimension: usize,
	measurement_secs: u64,
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
		let mut group = get_group(c, "index_mtree_insert", samples_len, measurement_secs);
		let id = format!("len_{}_dim_{}_cache_{}", samples_len, vector_dimension, cache_size);
		group.bench_function(id, |b| {
			b.to_async(Runtime::new().unwrap())
				.iter(|| insert_objects(&ds, samples_len, vector_dimension, cache_size));
		});
		group.finish();
	}

	// Knn lookup benchmark group
	{
		let mut group = get_group(c, "index_mtree_lookup", samples_len, 10);
		for knn in [1, 10] {
			let id = format!(
				"knn_{}_len_{}_dim_{}_cache_{}",
				knn, samples_len, vector_dimension, cache_size
			);
			group.bench_function(id, |b| {
				b.to_async(Runtime::new().unwrap()).iter(|| {
					knn_lookup_objects(&ds, samples_len, vector_dimension, knn, cache_size)
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
	measurement_secs: u64,
) -> BenchmarkGroup<'a, WallTime> {
	let mut group = c.benchmark_group(group_name);
	group.throughput(Throughput::Elements(samples_len as u64));
	group.sample_size(10);
	group.measurement_time(Duration::from_secs(measurement_secs));
	group
}
fn random_object(rng: &mut ThreadRng, vector_size: usize) -> Vector {
	let mut vec = Vec::with_capacity(vector_size);
	for _ in 0..vector_size {
		vec.push(rng.gen_range(-1.0..=1.0));
	}
	Vector::F32(vec.into())
}

fn mtree() -> MTree {
	MTree::new(MState::new(40), Distance::Euclidean)
}

async fn insert_objects(
	ds: &Datastore,
	samples_size: usize,
	vector_size: usize,
	cache_size: usize,
) {
	let mut rng = thread_rng();
	let mut t = mtree();
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
	let mut s =
		ds.index_store().get_store_mtree(TreeNodeProvider::Debug, 0, Write, cache_size).await;

	let mut stack = TreeStack::new();
	stack
		.enter(|stk| async {
			for i in 0..samples_size {
				let object = random_object(&mut rng, vector_size).into();
				// Insert the sample
				t.insert(stk, &mut tx, &mut s, object, i as DocId).await.unwrap();
			}
		})
		.finish()
		.await;

	if let Some(new_cache) = s.finish(&mut tx).await.unwrap() {
		ds.index_store().advance_store_mtree(new_cache);
	}
	tx.commit().await.unwrap();
}

async fn knn_lookup_objects(
	ds: &Datastore,
	samples_size: usize,
	vector_size: usize,
	knn: usize,
	cache_size: usize,
) {
	let mut rng = thread_rng();
	let t = mtree();
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap();
	let s = ds.index_store().get_store_mtree(TreeNodeProvider::Debug, 0, Read, cache_size).await;
	for _ in 0..samples_size {
		let object = random_object(&mut rng, vector_size).into();
		// Insert the sample
		t.knn_search(&mut tx, &s, &object, knn).await.unwrap();
	}
	tx.rollback_with_panic();
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
