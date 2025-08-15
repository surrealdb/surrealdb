use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;
use std::time::Duration;

use criterion::measurement::WallTime;
use criterion::{BenchmarkGroup, Criterion, Throughput, criterion_group, criterion_main};
use radix_trie::{Trie, TrieCommon, TrieKey};
use surrealdb_core::syn;
use surrealdb_core::val::{Array, RecordId};

// Common use case: VectorSearch
fn bench_hash_trie_btree_large_vector(c: &mut Criterion) {
	const N: usize = 10_000;
	let mut samples = Vec::with_capacity(N);
	for i in 0..N {
		let key = vec![i as u64; 1536];
		samples.push((key, i));
	}

	let mut g = new_group(c, "bench_hash_trie_btree_large_vector", N);
	bench_hash(&mut g, &samples);
	bench_trie(&mut g, &samples);
	bench_btree(&mut g, &samples);
	g.finish();
}

fn bench_hash_trie_btree_ix_key(c: &mut Criterion) {
	const N: usize = 100_000;
	let mut samples = Vec::with_capacity(N);
	for i in 0..N {
		let mut key = b"/*test\0*test\0*test\0!ixtest".to_vec();
		key.append(&mut i.to_be_bytes().to_vec());
		samples.push((key.clone(), i));
	}

	let mut g = new_group(c, "bench_hash_trie_btree_ix_key", N);
	bench_hash(&mut g, &samples);
	bench_trie(&mut g, &samples);
	bench_btree(&mut g, &samples);
	g.finish();
}

fn bench_hash_trie_btree_small_string(c: &mut Criterion) {
	const N: usize = 100_000;
	let mut samples = Vec::with_capacity(N);
	for i in 0..N {
		let key = format!("test{i}");
		samples.push((key, i));
	}

	let mut g = new_group(c, "bench_hash_trie_btree_string", N);
	bench_hash(&mut g, &samples);
	bench_trie(&mut g, &samples);
	bench_btree(&mut g, &samples);
	g.finish();
}

fn bench_hash_trie_btree_value(c: &mut Criterion) {
	const N: usize = 100_000;
	let mut samples = Vec::with_capacity(N);
	for i in 0..N {
		let key = syn::value(&format!(
			"{{ test: {{ something: [1, 'two', null, test:{i}, {{ trueee: false, noneee: nulll }}] }} }}"
		))
		.unwrap();
		samples.push((key, i));
	}

	let mut g = new_group(c, "bench_hash_trie_btree_value", N);
	bench_hash(&mut g, &samples);
	bench_btree(&mut g, &samples);
	g.finish();
}

fn bench_hash_trie_btree_thing(c: &mut Criterion) {
	const N: usize = 50_000;
	let mut samples = Vec::with_capacity(N);
	for i in 0..N {
		let key = RecordId::new("test".to_owned(), Array::from(vec![i as i32; 5]));
		samples.push((key, i));
	}

	let mut g = new_group(c, "bench_hash_trie_btree_thing", N);
	bench_hash(&mut g, &samples);
	bench_btree(&mut g, &samples);
	g.finish();
}

fn new_group<'a>(c: &'a mut Criterion, group: &str, n: usize) -> BenchmarkGroup<'a, WallTime> {
	let mut group = c.benchmark_group(group);
	group.throughput(Throughput::Elements(n as u64));
	group.sample_size(10);
	group.measurement_time(Duration::from_secs(10));
	group
}

fn bench_hash<K: Hash + Eq + Clone, V: Clone>(
	group: &mut BenchmarkGroup<WallTime>,
	samples: &[(K, V)],
) {
	group.bench_function("hash_insert", |b| {
		b.iter(|| bench_hash_insert(samples));
	});
	group.bench_function("hash_get", |b| {
		let map = build_hash(samples);
		b.iter(|| bench_hash_get(samples, &map));
	});
}

fn bench_trie<K: TrieKey + Clone, V: Clone>(
	group: &mut BenchmarkGroup<WallTime>,
	samples: &[(K, V)],
) {
	group.bench_function("trie_insert", |b| {
		b.iter(|| bench_trie_insert(samples));
	});

	group.bench_function("trie_get", |b| {
		let map = build_trie(samples);
		b.iter(|| bench_trie_get(samples, &map));
	});
}

fn bench_btree<K: Eq + Ord + Clone, V: Clone>(
	group: &mut BenchmarkGroup<WallTime>,
	samples: &[(K, V)],
) {
	group.bench_function("btree_insert", |b| {
		b.iter(|| bench_btree_insert(samples));
	});

	group.bench_function("btree_get", |b| {
		let map = build_btree(samples);
		b.iter(|| bench_btree_get(samples, &map));
	});
}

fn build_hash<K: Hash + Eq + Clone, V: Clone>(samples: &[(K, V)]) -> HashMap<K, V> {
	let mut map = HashMap::default();
	for (key, val) in samples {
		map.insert(key.clone(), val.clone());
	}
	map
}
fn bench_hash_insert<K: Hash + Eq + Clone, V: Clone>(samples: &[(K, V)]) {
	let map = build_hash(samples);
	assert_eq!(map.len(), samples.len());
}

fn bench_hash_get<K: Hash + Eq, V>(samples: &[(K, V)], map: &HashMap<K, V>) {
	for (key, _) in samples {
		assert!(map.get(key).is_some());
	}
	assert_eq!(map.len(), samples.len());
}

fn build_trie<K: TrieKey + Clone, V: Clone>(samples: &[(K, V)]) -> Trie<K, V> {
	let mut map = Trie::default();
	for (key, val) in samples {
		map.insert(key.clone(), val.clone());
	}
	map
}

fn bench_trie_insert<K: TrieKey + Clone, V: Clone>(samples: &[(K, V)]) {
	let map = build_trie(samples);
	assert_eq!(map.len(), samples.len());
}

fn bench_trie_get<K: TrieKey, V>(samples: &[(K, V)], map: &Trie<K, V>) {
	for (key, _) in samples {
		assert!(map.get(key).is_some());
	}
	assert_eq!(map.len(), samples.len());
}

fn build_btree<K: Ord + Clone, V: Clone>(samples: &[(K, V)]) -> BTreeMap<K, V> {
	let mut map = BTreeMap::default();
	for (key, val) in samples {
		map.insert(key.clone(), val.clone());
	}
	map
}

fn bench_btree_insert<K: Ord + Clone, V: Clone>(samples: &[(K, V)]) {
	let map = build_btree(samples);
	assert_eq!(map.len(), samples.len());
}

fn bench_btree_get<K: Ord, V>(samples: &[(K, V)], map: &BTreeMap<K, V>) {
	for (key, _) in samples {
		assert!(map.get(key).is_some());
	}
	assert_eq!(map.len(), samples.len());
}

criterion_group!(
	benches,
	bench_hash_trie_btree_large_vector,
	bench_hash_trie_btree_ix_key,
	bench_hash_trie_btree_small_string,
	bench_hash_trie_btree_thing,
	bench_hash_trie_btree_value
);
criterion_main!(benches);
