use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use rand::prelude::SliceRandom;
use rand::thread_rng;
use std::fmt::Debug;
use std::time::Duration;
use surrealdb::idx::trees::bkeys::{BKeys, FstKeys, TrieKeys};
use surrealdb::idx::trees::btree::{BState, BTree, Payload};
use surrealdb::idx::trees::store::cache::TreeCache;
use surrealdb::idx::trees::store::{TreeNodeProvider, TreeStore};
use surrealdb::kvs::{Datastore, Key, LockType::*, TransactionType::*};
use tokio::runtime::Runtime;
macro_rules! get_key_value {
	($idx:expr) => {{
		(format!("{}", $idx).into(), ($idx * 10) as Payload)
	}};
}

fn bench_index_btree(c: &mut Criterion) {
	let (samples_len, samples) = setup();

	let mut group = c.benchmark_group("index_btree");
	group.throughput(Throughput::Elements(1));
	group.sample_size(10);
	group.measurement_time(Duration::from_secs(30));

	group.bench_function("trees-insertion-fst", |b| {
		b.to_async(Runtime::new().unwrap())
			.iter(|| bench::<_, FstKeys>(samples_len, 100, |i| get_key_value!(samples[i])))
	});

	group.bench_function("trees-insertion-trie", |b| {
		b.to_async(Runtime::new().unwrap())
			.iter(|| bench::<_, TrieKeys>(samples_len, 100, |i| get_key_value!(samples[i])))
	});

	group.bench_function("trees-insertion-fst-fullcache", |b| {
		b.to_async(Runtime::new().unwrap())
			.iter(|| bench::<_, FstKeys>(samples_len, 0, |i| get_key_value!(samples[i])))
	});

	group.bench_function("trees-insertion-trie-fullcache", |b| {
		b.to_async(Runtime::new().unwrap())
			.iter(|| bench::<_, TrieKeys>(samples_len, 0, |i| get_key_value!(samples[i])))
	});

	group.finish();
}

fn setup() -> (usize, Vec<usize>) {
	let samples_len = if cfg!(debug_assertions) {
		1000 // debug is much slower!
	} else {
		100_000
	};
	let mut samples: Vec<usize> = (0..samples_len).collect();
	let mut rng = thread_rng();
	samples.shuffle(&mut rng);
	(samples_len, samples)
}

async fn bench<F, BK>(samples_size: usize, cache_size: usize, sample_provider: F)
where
	F: Fn(usize) -> (Key, Payload),
	BK: BKeys + Clone + Default + Debug,
{
	let ds = Datastore::new("memory").await.unwrap();
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
	let mut t = BTree::<BK>::new(BState::new(100));
	let c = TreeCache::new(0, TreeNodeProvider::Debug, cache_size);
	let mut s = TreeStore::new(TreeNodeProvider::Debug, c, Write).await;
	for i in 0..samples_size {
		let (key, payload) = sample_provider(i);
		// Insert the sample
		t.insert(&mut tx, &mut s, key.clone(), payload).await.unwrap();
		// Search for it
		black_box(t.search_mut(&mut tx, &mut s, &key).await.unwrap());
	}
	s.finish(&mut tx).await.unwrap();
	tx.commit().await.unwrap();
}

criterion_group!(benches, bench_index_btree);
criterion_main!(benches);
