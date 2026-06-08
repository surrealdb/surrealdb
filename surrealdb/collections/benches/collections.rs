//! Criterion benchmarks for [`VecMap`] and [`VecSet`].
//!
//! Map and set types use `String` keys/elements. Tiers include **tiny1** (1 entry), **tiny2** (2),
//! then small / medium / large. Small / **tiny\*** use linear search for lookups; medium and
//! large use binary search.
//!
//! `HashMap` / `HashSet` are compared for in-memory work. Storekey has no `BorrowDecode` for
//! `HashMap` or `HashSet`, so the `hashmap_*` / `hashset_*` storekey cases decode a `VecMap` or
//! `VecSet` then `HashMap`/`HashSet::from_iter`.
//!
//! Run: `cargo bench -p surrealdb-collections --bench collections`

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::hint::black_box;
use std::io::Cursor;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use revision::prelude::{DeserializeRevisioned, SerializeRevisioned};
use storekey::{decode_borrow, encode_vec};
use surrealdb_collections::{VecMap, VecSet};

// Must match `LINEAR_SEARCH_THRESHOLD` in `search.rs` (64) for the small vs medium label split.
const LINEAR_SCAN_KEY_MAX: usize = 64;

const MAP_TINY1: usize = 1;
const MAP_TINY2: usize = 2;
const MAP_SMALL: usize = 16;
const MAP_MEDIUM: usize = 256;
const MAP_LARGE: usize = 16_384;

/// `(label, n)` for all map-like size tiers, including 1- and 2-field tiny cases.
const MAP_SIZE_TIERS: [(&str, usize); 5] = [
	("tiny1", MAP_TINY1),
	("tiny2", MAP_TINY2),
	("small", MAP_SMALL),
	("medium", MAP_MEDIUM),
	("large", MAP_LARGE),
];

fn assert_tier_linearity_makes_sense(label: &str, n: usize) {
	let linear = ["tiny1", "tiny2", "small"].contains(&label);
	let binary = label == "medium" || label == "large";
	assert_eq!(linear, n <= LINEAR_SCAN_KEY_MAX, "size label vs LINEAR_SCAN_KEY_MAX mismatch");
	assert_eq!(binary, n > LINEAR_SCAN_KEY_MAX, "size label vs range mismatch");
}

#[inline]
fn key_str(i: usize) -> String {
	format!("k{i:05}")
}

#[inline]
fn val_str(i: usize) -> String {
	format!("v{i}")
}

fn build_vec_map_sequential(n: usize) -> VecMap<String, String> {
	let mut m = VecMap::new();
	for i in 0..n {
		m.insert(key_str(i), val_str(i));
	}
	m
}

fn build_btree_sequential(n: usize) -> BTreeMap<String, String> {
	(0..n).map(|i| (key_str(i), val_str(i))).collect()
}

fn build_hashmap_sequential(n: usize) -> HashMap<String, String> {
	let mut m = HashMap::new();
	for i in 0..n {
		m.insert(key_str(i), val_str(i));
	}
	m
}

/// Sorted `(key, value)` pairs for [`VecMap::from_sorted_vec_unchecked`].
fn sorted_map_entries(n: usize) -> Vec<(String, String)> {
	(0..n).map(|i| (key_str(i), val_str(i))).collect()
}

/// Sorted string keys for [`VecSet::from_sorted_vec_unchecked`].
fn sorted_set_keys(n: usize) -> Vec<String> {
	(0..n).map(key_str).collect()
}

fn ordered_vecmap_storekey_bytes(n: usize) -> Vec<u8> {
	let vm = build_vec_map_sequential(n);
	encode_vec(&vm).expect("encode_vec VecMap")
}

fn ordered_vecset_storekey_bytes(n: usize) -> Vec<u8> {
	let vs = build_vec_set_sequential(n);
	encode_vec(&vs).expect("encode_vec VecSet")
}

fn revision_bytes_from_vecmap(n: usize) -> Vec<u8> {
	let vm = build_vec_map_sequential(n);
	let mut w = Vec::new();
	vm.serialize_revisioned(&mut w).expect("serialize_revisioned VecMap");
	w
}

fn revision_bytes_from_vecset(n: usize) -> Vec<u8> {
	let vs = build_vec_set_sequential(n);
	let mut w = Vec::new();
	vs.serialize_revisioned(&mut w).expect("serialize_revisioned VecSet");
	w
}

// --- map (string keys/values) ---

fn bench_map_select(c: &mut Criterion) {
	let mut group = c.benchmark_group("map_select");
	for (label, n) in MAP_SIZE_TIERS {
		assert_tier_linearity_makes_sense(label, n);
		group.throughput(Throughput::Elements(1));
		let vm = build_vec_map_sequential(n);
		let bt = build_btree_sequential(n);
		let hm = build_hashmap_sequential(n);
		let look = key_str(n / 2);
		group.bench_with_input(BenchmarkId::new("vecmap", label), &n, |b, _| {
			b.iter(|| black_box(vm.get(look.as_str())));
		});
		group.bench_with_input(BenchmarkId::new("btreemap", label), &n, |b, _| {
			b.iter(|| black_box(bt.get(look.as_str())));
		});
		group.bench_with_input(BenchmarkId::new("hashmap", label), &n, |b, _| {
			b.iter(|| black_box(hm.get(look.as_str())));
		});
	}
	group.finish();
}

fn bench_map_insert(c: &mut Criterion) {
	let mut group = c.benchmark_group("map_insert");
	for (label, n) in MAP_SIZE_TIERS {
		group.throughput(Throughput::Elements(n as u64));
		group.bench_with_input(BenchmarkId::new("vecmap", label), &n, |b, &n| {
			b.iter_batched(
				VecMap::new,
				|mut m: VecMap<String, String>| {
					for i in 0..n {
						m.insert(black_box(key_str(i)), val_str(i));
					}
					black_box(m);
				},
				BatchSize::SmallInput,
			);
		});
		group.bench_with_input(BenchmarkId::new("btreemap", label), &n, |b, &n| {
			b.iter_batched(
				BTreeMap::new,
				|mut m: BTreeMap<String, String>| {
					for i in 0..n {
						m.insert(black_box(key_str(i)), val_str(i));
					}
					black_box(m);
				},
				BatchSize::SmallInput,
			);
		});
		group.bench_with_input(BenchmarkId::new("hashmap", label), &n, |b, &n| {
			b.iter_batched(
				HashMap::new,
				|mut m: HashMap<String, String>| {
					for i in 0..n {
						m.insert(black_box(key_str(i)), val_str(i));
					}
					black_box(m);
				},
				BatchSize::SmallInput,
			);
		});
	}
	group.finish();
}

/// Build from pre-sorted entry vectors: [`VecMap::from_sorted_vec_unchecked`] vs `collect`.
fn bench_map_from_sorted_vec(c: &mut Criterion) {
	let mut group = c.benchmark_group("map_from_sorted_vec");
	for (label, n) in MAP_SIZE_TIERS {
		group.throughput(Throughput::Elements(n as u64));
		group.bench_with_input(BenchmarkId::new("vecmap_unchecked", label), &n, |b, &n| {
			b.iter_batched(
				|| sorted_map_entries(n),
				|entries| {
					black_box(VecMap::from_sorted_vec_unchecked(entries));
				},
				BatchSize::SmallInput,
			);
		});
		group.bench_with_input(BenchmarkId::new("btreemap_from_iter", label), &n, |b, &n| {
			b.iter_batched(
				|| sorted_map_entries(n),
				|entries: Vec<(String, String)>| {
					black_box(BTreeMap::from_iter(entries));
				},
				BatchSize::SmallInput,
			);
		});
		group.bench_with_input(BenchmarkId::new("hashmap_from_iter", label), &n, |b, &n| {
			b.iter_batched(
				|| sorted_map_entries(n),
				|entries: Vec<(String, String)>| {
					black_box(HashMap::<String, String>::from_iter(entries));
				},
				BatchSize::SmallInput,
			);
		});
	}
	group.finish();
}

fn bench_map_revision_deserialize(c: &mut Criterion) {
	let mut group = c.benchmark_group("map_revision_deserialize");
	for (label, n) in MAP_SIZE_TIERS {
		let bytes = revision_bytes_from_vecmap(n);
		group.throughput(Throughput::Elements(n as u64));
		group.bench_with_input(BenchmarkId::new("vecmap", label), &bytes, |b, bytes| {
			b.iter(|| {
				let mut r = Cursor::new(bytes.as_slice());
				black_box(
					VecMap::<String, String>::deserialize_revisioned(&mut r).expect("VecMap"),
				);
			});
		});
		group.bench_with_input(BenchmarkId::new("btreemap", label), &bytes, |b, bytes| {
			b.iter(|| {
				let mut r = Cursor::new(bytes.as_slice());
				black_box(
					BTreeMap::<String, String>::deserialize_revisioned(&mut r).expect("BTreeMap"),
				);
			});
		});
		group.bench_with_input(BenchmarkId::new("hashmap", label), &bytes, |b, bytes| {
			b.iter(|| {
				let mut r = Cursor::new(bytes.as_slice());
				black_box(
					HashMap::<String, String>::deserialize_revisioned(&mut r).expect("HashMap"),
				);
			});
		});
	}
	group.finish();
}

fn bench_map_clone(c: &mut Criterion) {
	let mut group = c.benchmark_group("map_clone");
	for (label, n) in MAP_SIZE_TIERS {
		group.throughput(Throughput::Elements(n as u64));
		let vm = build_vec_map_sequential(n);
		let bt = build_btree_sequential(n);
		let hm = build_hashmap_sequential(n);
		group.bench_with_input(BenchmarkId::new("vecmap", label), &n, |b, _| {
			b.iter(|| black_box(vm.clone()));
		});
		group.bench_with_input(BenchmarkId::new("btreemap", label), &n, |b, _| {
			b.iter(|| black_box(bt.clone()));
		});
		group.bench_with_input(BenchmarkId::new("hashmap", label), &n, |b, _| {
			b.iter(|| black_box(hm.clone()));
		});
	}
	group.finish();
}

fn bench_map_storekey_decode_ordered(c: &mut Criterion) {
	let mut group = c.benchmark_group("map_storekey_decode_ordered");
	for (label, n) in MAP_SIZE_TIERS {
		let bytes = ordered_vecmap_storekey_bytes(n);
		group.throughput(Throughput::Elements(n as u64));
		group.bench_with_input(BenchmarkId::new("vecmap", label), &bytes, |b, bytes| {
			b.iter(|| {
				black_box(decode_borrow::<VecMap<String, String>>(bytes).expect("decode VecMap"))
			});
		});
		group.bench_with_input(BenchmarkId::new("btreemap", label), &bytes, |b, bytes| {
			b.iter(|| {
				black_box(
					decode_borrow::<BTreeMap<String, String>>(bytes).expect("decode BTreeMap"),
				);
			});
		});
		group.bench_with_input(
			BenchmarkId::new("hashmap_from_vecmap_decode", label),
			&bytes,
			|b, bytes| {
				b.iter(|| {
					let vm: VecMap<String, String> = decode_borrow(bytes).expect("decode VecMap");
					black_box(HashMap::<String, String>::from_iter(vm));
				});
			},
		);
	}
	group.finish();
}

/// `remove` one existing key, `retain` half, `VecMap::append`, and `entry` + `or_insert_with` for a
/// new key.
fn bench_map_mutation(c: &mut Criterion) {
	let mut g_remove = c.benchmark_group("map_remove_one");
	for (label, n) in MAP_SIZE_TIERS {
		if n < 1 {
			continue;
		}
		let remove_key = key_str(n / 2);
		g_remove.throughput(Throughput::Elements(1));
		g_remove.bench_with_input(BenchmarkId::new("vecmap", label), &n, |b, &n| {
			b.iter_batched(
				|| build_vec_map_sequential(n),
				|mut m| {
					black_box(m.remove(remove_key.as_str()));
					black_box(m);
				},
				BatchSize::SmallInput,
			);
		});
		g_remove.bench_with_input(BenchmarkId::new("btreemap", label), &n, |b, &n| {
			b.iter_batched(
				|| build_btree_sequential(n),
				|mut m| {
					black_box(m.remove(remove_key.as_str()));
					black_box(m);
				},
				BatchSize::SmallInput,
			);
		});
		g_remove.bench_with_input(BenchmarkId::new("hashmap", label), &n, |b, &n| {
			b.iter_batched(
				|| build_hashmap_sequential(n),
				|mut m| {
					black_box(m.remove(remove_key.as_str()));
					black_box(m);
				},
				BatchSize::SmallInput,
			);
		});
	}
	g_remove.finish();

	let mut g_retain = c.benchmark_group("map_retain_half");
	for (label, n) in MAP_SIZE_TIERS {
		// with 1 key, "half" is a degenerate but still valid
		g_retain.throughput(Throughput::Elements(n as u64));
		g_retain.bench_with_input(BenchmarkId::new("vecmap", label), &n, |b, &n| {
			b.iter_batched(
				|| build_vec_map_sequential(n),
				|mut m| {
					m.retain(|k, _v| {
						let idx: usize = k[1..].parse().expect("key");
						idx.is_multiple_of(2)
					});
					black_box(m);
				},
				BatchSize::SmallInput,
			);
		});
		g_retain.bench_with_input(BenchmarkId::new("btreemap", label), &n, |b, &n| {
			b.iter_batched(
				|| build_btree_sequential(n),
				|mut m| {
					m.retain(|k, _v| {
						let idx: usize = k[1..].parse().expect("key");
						idx.is_multiple_of(2)
					});
					black_box(m);
				},
				BatchSize::SmallInput,
			);
		});
		g_retain.bench_with_input(BenchmarkId::new("hashmap", label), &n, |b, &n| {
			b.iter_batched(
				|| build_hashmap_sequential(n),
				|mut m| {
					m.retain(|k, _v| {
						let idx: usize = k[1..].parse().expect("key");
						idx.is_multiple_of(2)
					});
					black_box(m);
				},
				BatchSize::SmallInput,
			);
		});
	}
	g_retain.finish();

	let mut g_append = c.benchmark_group("map_append_disjoint");
	// n keys in first map, n keys in second (0..n and n..2n)
	for (label, n) in MAP_SIZE_TIERS {
		if n < 1 {
			continue;
		}
		g_append.throughput(Throughput::Elements((2 * n) as u64));
		g_append.bench_with_input(BenchmarkId::new("vecmap", label), &n, |b, &n| {
			b.iter_batched(
				|| {
					let a = build_vec_map_sequential(n);
					let mut b = VecMap::new();
					for i in n..(2 * n) {
						b.insert(key_str(i), val_str(i));
					}
					(a, b)
				},
				|mut a: (VecMap<String, String>, VecMap<String, String>)| {
					a.0.append(&mut a.1);
					black_box(a.0);
				},
				BatchSize::SmallInput,
			);
		});
		g_append.bench_with_input(BenchmarkId::new("btreemap", label), &n, |b, &n| {
			b.iter_batched(
				|| {
					let a = build_btree_sequential(n);
					let b: BTreeMap<_, _> =
						(n..(2 * n)).map(|i| (key_str(i), val_str(i))).collect();
					(a, b)
				},
				|mut a: (BTreeMap<_, _>, BTreeMap<_, _>)| {
					a.0.append(&mut a.1);
					black_box(a.0);
				},
				BatchSize::SmallInput,
			);
		});
	}
	g_append.finish();

	let mut g_entry = c.benchmark_group("map_entry_vacant_insert");
	// one fresh insert at key `k{n}` (not in 0..n)
	for (label, n) in MAP_SIZE_TIERS {
		let new_key = key_str(n);
		g_entry.throughput(Throughput::Elements(1));
		g_entry.bench_with_input(BenchmarkId::new("vecmap", label), &n, |b, &n| {
			b.iter_batched(
				|| build_vec_map_sequential(n),
				|mut m: VecMap<String, String>| {
					m.entry(black_box(new_key.clone()))
						.or_insert_with(|| black_box("new".to_string()));
					black_box(m);
				},
				BatchSize::SmallInput,
			);
		});
		g_entry.bench_with_input(BenchmarkId::new("btreemap", label), &n, |b, &n| {
			b.iter_batched(
				|| build_btree_sequential(n),
				|mut m: BTreeMap<_, _>| {
					m.entry(black_box(new_key.clone()))
						.or_insert_with(|| black_box("new".to_string()));
					black_box(m);
				},
				BatchSize::SmallInput,
			);
		});
		g_entry.bench_with_input(BenchmarkId::new("hashmap", label), &n, |b, &n| {
			b.iter_batched(
				|| build_hashmap_sequential(n),
				|mut m: HashMap<_, _>| {
					m.entry(black_box(new_key.clone()))
						.or_insert_with(|| black_box("new".to_string()));
					black_box(m);
				},
				BatchSize::SmallInput,
			);
		});
	}
	g_entry.finish();
}

// --- set (String elements) ---

fn build_vec_set_sequential(n: usize) -> VecSet<String> {
	let mut s = VecSet::new();
	for i in 0..n {
		s.insert(key_str(i));
	}
	s
}

fn build_btreeset_sequential(n: usize) -> BTreeSet<String> {
	(0..n).map(key_str).collect()
}

fn build_hashset_sequential(n: usize) -> HashSet<String> {
	(0..n).map(key_str).collect()
}

fn bench_set_contains(c: &mut Criterion) {
	let mut group = c.benchmark_group("set_contains");
	for (label, n) in MAP_SIZE_TIERS {
		assert_tier_linearity_makes_sense(label, n);
		group.throughput(Throughput::Elements(1));
		let vs = build_vec_set_sequential(n);
		let bt = build_btreeset_sequential(n);
		let hs = build_hashset_sequential(n);
		// Tiers are n >= 1; n/2 is always a present key for lookup.
		let look = key_str(n / 2);
		group.bench_with_input(BenchmarkId::new("vecset", label), &n, |b, _| {
			b.iter(|| black_box(vs.contains(look.as_str())));
		});
		group.bench_with_input(BenchmarkId::new("btreeset", label), &n, |b, _| {
			b.iter(|| black_box(bt.contains(look.as_str())));
		});
		group.bench_with_input(BenchmarkId::new("hashset", label), &n, |b, _| {
			b.iter(|| black_box(hs.contains(look.as_str())));
		});
	}
	group.finish();
}

/// Pre-sorted keys: [`VecSet::from_sorted_vec_unchecked`] vs set `collect`.
fn bench_set_from_sorted_vec(c: &mut Criterion) {
	let mut group = c.benchmark_group("set_from_sorted_vec");
	for (label, n) in MAP_SIZE_TIERS {
		group.throughput(Throughput::Elements(n as u64));
		group.bench_with_input(BenchmarkId::new("vecset_unchecked", label), &n, |b, &n| {
			b.iter_batched(
				|| sorted_set_keys(n),
				|elems| {
					black_box(VecSet::from_sorted_vec_unchecked(elems));
				},
				BatchSize::SmallInput,
			);
		});
		group.bench_with_input(BenchmarkId::new("btreeset_from_iter", label), &n, |b, &n| {
			b.iter_batched(
				|| sorted_set_keys(n),
				|elems: Vec<String>| {
					black_box(BTreeSet::from_iter(elems));
				},
				BatchSize::SmallInput,
			);
		});
		group.bench_with_input(BenchmarkId::new("hashset_from_iter", label), &n, |b, &n| {
			b.iter_batched(
				|| sorted_set_keys(n),
				|elems: Vec<String>| {
					black_box(HashSet::<String>::from_iter(elems));
				},
				BatchSize::SmallInput,
			);
		});
	}
	group.finish();
}

fn bench_set_revision_deserialize(c: &mut Criterion) {
	let mut group = c.benchmark_group("set_revision_deserialize");
	for (label, n) in MAP_SIZE_TIERS {
		let bytes = revision_bytes_from_vecset(n);
		group.throughput(Throughput::Elements(n as u64));
		group.bench_with_input(BenchmarkId::new("vecset", label), &bytes, |b, bytes| {
			b.iter(|| {
				let mut r = Cursor::new(bytes.as_slice());
				black_box(VecSet::<String>::deserialize_revisioned(&mut r).expect("VecSet"));
			});
		});
		group.bench_with_input(BenchmarkId::new("btreeset", label), &bytes, |b, bytes| {
			b.iter(|| {
				let mut r = Cursor::new(bytes.as_slice());
				black_box(BTreeSet::<String>::deserialize_revisioned(&mut r).expect("BTreeSet"));
			});
		});
		group.bench_with_input(BenchmarkId::new("hashset", label), &bytes, |b, bytes| {
			b.iter(|| {
				let mut r = Cursor::new(bytes.as_slice());
				black_box(HashSet::<String>::deserialize_revisioned(&mut r).expect("HashSet"));
			});
		});
	}
	group.finish();
}

fn bench_set_storekey_decode_ordered(c: &mut Criterion) {
	let mut group = c.benchmark_group("set_storekey_decode_ordered");
	for (label, n) in MAP_SIZE_TIERS {
		let bytes = ordered_vecset_storekey_bytes(n);
		group.throughput(Throughput::Elements(n as u64));
		group.bench_with_input(BenchmarkId::new("vecset", label), &bytes, |b, bytes| {
			b.iter(|| black_box(decode_borrow::<VecSet<String>>(bytes).expect("decode VecSet")));
		});
		group.bench_with_input(BenchmarkId::new("btreeset", label), &bytes, |b, bytes| {
			b.iter(|| {
				black_box(decode_borrow::<BTreeSet<String>>(bytes).expect("decode BTreeSet"))
			});
		});
		group.bench_with_input(
			BenchmarkId::new("hashset_from_vecset_decode", label),
			&bytes,
			|b, bytes| {
				b.iter(|| {
					let s: VecSet<String> = decode_borrow(bytes).expect("VecSet");
					black_box(HashSet::<String>::from_iter(s));
				});
			},
		);
	}
	group.finish();
}

fn bench_set_clone(c: &mut Criterion) {
	let mut group = c.benchmark_group("set_clone");
	for (label, n) in MAP_SIZE_TIERS {
		let vs = build_vec_set_sequential(n);
		let bt = build_btreeset_sequential(n);
		let hs = build_hashset_sequential(n);
		group.throughput(Throughput::Elements(n as u64));
		group.bench_with_input(BenchmarkId::new("vecset", label), &n, |b, _| {
			b.iter(|| black_box(vs.clone()));
		});
		group.bench_with_input(BenchmarkId::new("btreeset", label), &n, |b, _| {
			b.iter(|| black_box(bt.clone()));
		});
		group.bench_with_input(BenchmarkId::new("hashset", label), &n, |b, _| {
			b.iter(|| black_box(hs.clone()));
		});
	}
	group.finish();
}

/// `retain` half, `remove` one element, same overlapping pairs for [`VecSet`], [`BTreeSet`],
/// [`HashSet`].
fn bench_set_mutation(c: &mut Criterion) {
	let mut g_retain = c.benchmark_group("set_retain_half");
	for (label, n) in MAP_SIZE_TIERS {
		g_retain.throughput(Throughput::Elements(n as u64));
		g_retain.bench_with_input(BenchmarkId::new("vecset", label), &n, |b, &n| {
			b.iter_batched(
				|| build_vec_set_sequential(n),
				|mut s: VecSet<String>| {
					s.retain(|k| {
						let idx: usize = k[1..].parse().expect("key");
						idx.is_multiple_of(2)
					});
					black_box(s);
				},
				BatchSize::SmallInput,
			);
		});
		g_retain.bench_with_input(BenchmarkId::new("btreeset", label), &n, |b, &n| {
			b.iter_batched(
				|| build_btreeset_sequential(n),
				|mut s: BTreeSet<String>| {
					s.retain(|k| {
						let idx: usize = k[1..].parse().expect("key");
						idx.is_multiple_of(2)
					});
					black_box(s);
				},
				BatchSize::SmallInput,
			);
		});
		g_retain.bench_with_input(BenchmarkId::new("hashset", label), &n, |b, &n| {
			b.iter_batched(
				|| build_hashset_sequential(n),
				|mut s: HashSet<String>| {
					s.retain(|k| {
						let idx: usize = k[1..].parse().expect("key");
						idx.is_multiple_of(2)
					});
					black_box(s);
				},
				BatchSize::SmallInput,
			);
		});
	}
	g_retain.finish();

	let mut g_remove = c.benchmark_group("set_remove_one");
	for (label, n) in MAP_SIZE_TIERS {
		if n < 1 {
			continue;
		}
		let rm = key_str(n / 2);
		g_remove.throughput(Throughput::Elements(1));
		g_remove.bench_with_input(BenchmarkId::new("vecset", label), &n, |b, &n| {
			b.iter_batched(
				|| build_vec_set_sequential(n),
				|mut s| {
					black_box(s.remove(rm.as_str()));
					black_box(s);
				},
				BatchSize::SmallInput,
			);
		});
		g_remove.bench_with_input(BenchmarkId::new("btreeset", label), &n, |b, &n| {
			b.iter_batched(
				|| build_btreeset_sequential(n),
				|mut s: BTreeSet<String>| {
					black_box(s.remove(rm.as_str()));
					black_box(s);
				},
				BatchSize::SmallInput,
			);
		});
		g_remove.bench_with_input(BenchmarkId::new("hashset", label), &n, |b, &n| {
			b.iter_batched(
				|| build_hashset_sequential(n),
				|mut s: HashSet<String>| {
					black_box(s.remove(&rm));
					black_box(s);
				},
				BatchSize::SmallInput,
			);
		});
	}
	g_remove.finish();
}

/// Two sets of `n` string elements, overlapping on the upper half of `a` and the lower of `b`.
fn build_overlapping_pair(n: u64) -> (VecSet<String>, VecSet<String>) {
	let n = n as usize;
	let a = build_vec_set_sequential(n);
	let mut b = VecSet::new();
	for i in (n / 2)..(n + n / 2) {
		b.insert(key_str(i));
	}
	(a, b)
}

fn bench_set_algebra(c: &mut Criterion) {
	// Overlapping pair sizes (same as prior vecset-only benches).
	const SIZES: [u64; 4] = [64, 512, 4096, 32_768];
	let mut group = c.benchmark_group("set_algebra");
	for n in SIZES {
		group.throughput(Throughput::Elements(2 * n));

		let (va, vb) = build_overlapping_pair(n);
		group.bench_with_input(
			BenchmarkId::new("vecset_union", n),
			&(va, vb),
			|bencher, (a, b): &(VecSet<String>, VecSet<String>)| {
				bencher.iter(|| black_box(a.union(b)));
			},
		);
		let (va, vb) = build_overlapping_pair(n);
		group.bench_with_input(
			BenchmarkId::new("vecset_intersection", n),
			&(va, vb),
			|bencher, (a, b): &(VecSet<String>, VecSet<String>)| {
				bencher.iter(|| black_box(a.intersection(b)));
			},
		);
		let (va, vb) = build_overlapping_pair(n);
		group.bench_with_input(
			BenchmarkId::new("vecset_difference", n),
			&(va, vb),
			|bencher, (a, b): &(VecSet<String>, VecSet<String>)| {
				bencher.iter(|| black_box(a.difference(b)));
			},
		);

		let (va, vb) = build_overlapping_pair(n);
		let a_bt: BTreeSet<String> = va.iter().cloned().collect();
		let b_bt: BTreeSet<String> = vb.iter().cloned().collect();
		group.bench_with_input(
			BenchmarkId::new("btreeset_union", n),
			&(a_bt, b_bt),
			|bencher, (a, b): &(BTreeSet<String>, BTreeSet<String>)| {
				bencher.iter(|| black_box(a.union(b).cloned().collect::<BTreeSet<String>>()));
			},
		);
		let (va, vb) = build_overlapping_pair(n);
		let a_bt: BTreeSet<String> = va.iter().cloned().collect();
		let b_bt: BTreeSet<String> = vb.iter().cloned().collect();
		group.bench_with_input(
			BenchmarkId::new("btreeset_intersection", n),
			&(a_bt, b_bt),
			|bencher, (a, b): &(BTreeSet<String>, BTreeSet<String>)| {
				bencher
					.iter(|| black_box(a.intersection(b).cloned().collect::<BTreeSet<String>>()));
			},
		);
		let (va, vb) = build_overlapping_pair(n);
		let a_bt: BTreeSet<String> = va.iter().cloned().collect();
		let b_bt: BTreeSet<String> = vb.iter().cloned().collect();
		group.bench_with_input(
			BenchmarkId::new("btreeset_difference", n),
			&(a_bt, b_bt),
			|bencher, (a, b): &(BTreeSet<String>, BTreeSet<String>)| {
				bencher.iter(|| black_box(a.difference(b).cloned().collect::<BTreeSet<String>>()));
			},
		);

		let (va, vb) = build_overlapping_pair(n);
		let a_hs: HashSet<String> = va.iter().cloned().collect();
		let b_hs: HashSet<String> = vb.iter().cloned().collect();
		group.bench_with_input(
			BenchmarkId::new("hashset_union", n),
			&(a_hs, b_hs),
			|bencher, (a, b): &(HashSet<String>, HashSet<String>)| {
				bencher.iter(|| black_box(a.union(b).cloned().collect::<HashSet<String>>()));
			},
		);
		let (va, vb) = build_overlapping_pair(n);
		let a_hs: HashSet<String> = va.iter().cloned().collect();
		let b_hs: HashSet<String> = vb.iter().cloned().collect();
		group.bench_with_input(
			BenchmarkId::new("hashset_intersection", n),
			&(a_hs, b_hs),
			|bencher, (a, b): &(HashSet<String>, HashSet<String>)| {
				bencher.iter(|| black_box(a.intersection(b).cloned().collect::<HashSet<String>>()));
			},
		);
		let (va, vb) = build_overlapping_pair(n);
		let a_hs: HashSet<String> = va.iter().cloned().collect();
		let b_hs: HashSet<String> = vb.iter().cloned().collect();
		group.bench_with_input(
			BenchmarkId::new("hashset_difference", n),
			&(a_hs, b_hs),
			|bencher, (a, b): &(HashSet<String>, HashSet<String>)| {
				bencher.iter(|| black_box(a.difference(b).cloned().collect::<HashSet<String>>()));
			},
		);
	}
	group.finish();
}

criterion_group!(
	benches,
	bench_map_select,
	bench_map_insert,
	bench_map_from_sorted_vec,
	bench_map_revision_deserialize,
	bench_map_clone,
	bench_map_storekey_decode_ordered,
	bench_map_mutation,
	bench_set_contains,
	bench_set_from_sorted_vec,
	bench_set_revision_deserialize,
	bench_set_storekey_decode_ordered,
	bench_set_clone,
	bench_set_mutation,
	bench_set_algebra
);
criterion_main!(benches);
