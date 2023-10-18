use criterion::async_executor::FuturesExecutor;
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use rand::prelude::ThreadRng;
use rand::{thread_rng, Rng};
use std::time::Duration;
use surrealdb::idx::docids::DocId;
use surrealdb::idx::trees::mtree::{MState, MTree, Vector};
use surrealdb::idx::trees::store::{TreeNodeProvider, TreeNodeStore, TreeStoreType};
use surrealdb::kvs::Datastore;
use surrealdb::kvs::LockType::Optimistic;
use surrealdb::kvs::TransactionType::Write;
use surrealdb::sql::index::Distance;
use surrealdb::sql::Number;

fn bench_index_mtree_insert_dim_50(c: &mut Criterion) {
	let samples_len = if cfg!(debug_assertions) {
		100 // debug is much slower!
	} else {
		10_000
	};
	bench_index_mtree_insert(c, "index_mtree_insert_dim_50", samples_len, 50, 20);
}

fn bench_index_mtree_insert_dim_300(c: &mut Criterion) {
	let samples_len = if cfg!(debug_assertions) {
		50 // debug is much slower!
	} else {
		5_000
	};
	bench_index_mtree_insert(c, "index_mtree_insert_dim_300", samples_len, 300, 40);
}

fn bench_index_mtree_insert_dim_2048(c: &mut Criterion) {
	let samples_len = if cfg!(debug_assertions) {
		10 // debug is much slower!
	} else {
		1_000
	};
	bench_index_mtree_insert(c, "index_mtree_insert_dim_2048", samples_len, 2048, 60);
}

fn bench_index_mtree_insert(
	c: &mut Criterion,
	group_name: &str,
	samples_len: usize,
	vector_dimension: usize,
	measurement_secs: u64,
) {
	let mut group = c.benchmark_group(group_name);
	group.throughput(Throughput::Elements(samples_len as u64));
	group.sample_size(10);
	group.measurement_time(Duration::from_secs(measurement_secs));
	group.bench_function(group_name, |b| {
		b.to_async(FuturesExecutor).iter(|| bench(samples_len, vector_dimension));
	});
	group.finish();
}

fn get_vector(rng: &mut ThreadRng, vector_size: usize) -> Vector {
	let mut vec = Vec::with_capacity(vector_size);
	for _ in 0..vector_size {
		let v: f32 = rng.gen_range(-1.0..=1.0);
		vec.push(Number::from(v));
	}
	vec
}

async fn bench(samples_size: usize, vector_size: usize) {
	let mut rng = thread_rng();
	let ds = Datastore::new("memory").await.unwrap();
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
	let mut t = MTree::new(MState::new(40), Distance::Euclidean);
	let s = TreeNodeStore::new(TreeNodeProvider::Debug, TreeStoreType::Write, 20);
	let mut s = s.lock().await;
	for i in 0..samples_size {
		let object = get_vector(&mut rng, vector_size);
		// Insert the sample
		t.insert(&mut tx, &mut s, object, i as DocId).await.unwrap();
	}
	tx.commit().await.unwrap();
}

criterion_group!(
	benches,
	bench_index_mtree_insert_dim_50,
	bench_index_mtree_insert_dim_300,
	bench_index_mtree_insert_dim_2048
);
criterion_main!(benches);
