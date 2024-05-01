use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use hashbrown::HashSet as HashBrownSet;
use smallvec::{Array, SmallVec};
use std::collections::HashSet;
use std::time::{Duration, SystemTime};
use surrealdb_core::idx::trees::dynamicset::{DynamicSet, DynamicSetImpl};

fn bench_hashset(samples_vec: &Vec<Vec<u64>>) {
	for samples in samples_vec {
		let mut h = HashSet::with_capacity(samples.len());
		for &s in samples {
			h.insert(s);
		}
		for s in samples {
			assert!(h.contains(s));
		}
		assert_eq!(h.len(), samples.len());
	}
}

fn bench_hashbrown(samples_vec: &Vec<Vec<u64>>) {
	for samples in samples_vec {
		let mut h = HashBrownSet::with_capacity(samples.len());
		for &s in samples {
			h.insert(s);
		}
		for s in samples {
			assert!(h.contains(s));
		}
		assert_eq!(h.len(), samples.len());
	}
}

fn bench_vector(samples_vec: &Vec<Vec<u64>>) {
	for samples in samples_vec {
		let mut v = Vec::with_capacity(samples.len());
		for &s in samples {
			// Same behaviour than Hash
			if !v.contains(&s) {
				v.push(s);
			}
		}
		for s in samples {
			assert!(v.contains(s));
		}
		assert_eq!(v.len(), samples.len());
	}
}

fn bench_small_vec<A: Array<Item = u64>, F: Fn() -> SmallVec<A>>(
	new_vec: F,
	samples_vec: &Vec<Vec<u64>>,
) {
	for samples in samples_vec {
		let mut v = new_vec();
		for &s in samples {
			// Same behaviour than Hash
			if !v.contains(&s) {
				v.push(s);
			}
		}
		for s in samples {
			assert!(v.contains(s));
		}
		assert_eq!(v.len(), samples.len());
	}
}

fn bench_dynamic_set(samples_vec: &Vec<Vec<u64>>) {
	for samples in samples_vec {
		let mut v = DynamicSet::with_capacity(samples.len());
		for &s in samples {
			v.insert(s);
		}
		for s in samples {
			assert!(v.contains(s));
		}
		assert_eq!(v.len(), samples.len());
	}
}

fn create_samples(capacity: usize, num_samples: usize) -> Vec<Vec<u64>> {
	let mut s = SystemTime::now().elapsed().unwrap().as_secs();
	let mut res = Vec::with_capacity(num_samples);
	for _ in 0..num_samples {
		let mut samples = Vec::with_capacity(capacity);
		for _ in 0..capacity {
			s += 1;
			samples.push(s);
		}
		res.push(samples);
	}
	res
}

/// This bench compares the performance of insert and search for small size HashSet collections.
/// It compares HashSet, HashBrown, Vector and SmallVec.
/// It is used to help choosing the best options for the UndirectedGraph used for the HNSW index.
/// The ultimate goal is to be sure that the DynamicSet use the best option based on the expected capacity.
fn bench_hashset_vs_vector(c: &mut Criterion) {
	const ITERATIONS: usize = 1_000_000;

	let mut group = c.benchmark_group("hashset_vs_vector");
	group.throughput(Throughput::Elements(ITERATIONS as u64));
	group.sample_size(10);
	group.measurement_time(Duration::from_secs(10));

	{
		let samples = create_samples(4, ITERATIONS);

		group.bench_function("hashset_4", |b| {
			b.iter(|| bench_hashset(&samples));
		});

		group.bench_function("hashbrown_4", |b| {
			b.iter(|| bench_hashbrown(&samples));
		});

		group.bench_function("vector_4", |b| {
			b.iter(|| bench_vector(&samples));
		});

		group.bench_function("smallvec_4", |b| {
			b.iter(|| bench_small_vec(|| SmallVec::<[u64; 4]>::new(), &samples));
		});

		group.bench_function("dynamic_set_4", |b| {
			b.iter(|| bench_dynamic_set(&samples));
		});
	}

	{
		let samples = create_samples(8, ITERATIONS);

		group.bench_function("hashset_8", |b| {
			b.iter(|| bench_hashset(&samples));
		});

		group.bench_function("hashbrown_8", |b| {
			b.iter(|| bench_hashbrown(&samples));
		});

		group.bench_function("vector_8", |b| {
			b.iter(|| bench_vector(&samples));
		});

		group.bench_function("smallvec_8", |b| {
			b.iter(|| bench_small_vec(|| SmallVec::<[u64; 8]>::new(), &samples));
		});

		group.bench_function("dynamic_set_8", |b| {
			b.iter(|| bench_dynamic_set(&samples));
		});
	}

	{
		let samples = create_samples(16, ITERATIONS);

		group.bench_function("hashset_16", |b| {
			b.iter(|| bench_hashset(&samples));
		});

		group.bench_function("hashbrown_16", |b| {
			b.iter(|| bench_hashbrown(&samples));
		});

		group.bench_function("vector_16", |b| {
			b.iter(|| bench_vector(&samples));
		});

		group.bench_function("smallvec_16", |b| {
			b.iter(|| bench_small_vec(|| SmallVec::<[u64; 16]>::new(), &samples));
		});

		group.bench_function("dynamic_set_16", |b| {
			b.iter(|| bench_dynamic_set(&samples));
		});
	}

	{
		let samples = create_samples(24, ITERATIONS);

		group.bench_function("hashset_24", |b| {
			b.iter(|| bench_hashset(&samples));
		});

		group.bench_function("hashbrown_24", |b| {
			b.iter(|| bench_hashbrown(&samples));
		});

		group.bench_function("vector_24", |b| {
			b.iter(|| bench_vector(&samples));
		});

		group.bench_function("smallvec_24", |b| {
			b.iter(|| bench_small_vec(|| SmallVec::<[u64; 24]>::new(), &samples));
		});

		group.bench_function("dynamic_set_24", |b| {
			b.iter(|| bench_dynamic_set(&samples));
		});
	}

	{
		let samples = create_samples(36, ITERATIONS);

		group.bench_function("hashset_36", |b| {
			b.iter(|| bench_hashset(&samples));
		});

		group.bench_function("hashbrown_36", |b| {
			b.iter(|| bench_hashbrown(&samples));
		});

		group.bench_function("vector_36", |b| {
			b.iter(|| bench_vector(&samples));
		});

		group.bench_function("smallvec_36", |b| {
			b.iter(|| bench_small_vec(|| SmallVec::<[u64; 36]>::new(), &samples));
		});

		group.bench_function("dynamic_set_36", |b| {
			b.iter(|| bench_dynamic_set(&samples));
		});
	}

	{
		let samples = create_samples(48, ITERATIONS);

		group.bench_function("hashset_48", |b| {
			b.iter(|| bench_hashset(&samples));
		});

		group.bench_function("hashbrown_48", |b| {
			b.iter(|| bench_hashbrown(&samples));
		});

		group.bench_function("vector_48", |b| {
			b.iter(|| bench_vector(&samples));
		});

		group.bench_function("smallvec_48", |b| {
			// There is no 48 implementation for Array/SmallVec,
			// so we just up to the next implementation, which is 0x40 / 64.
			b.iter(|| bench_small_vec(|| SmallVec::<[u64; 64]>::new(), &samples));
		});

		group.bench_function("dynamic_set_48", |b| {
			b.iter(|| bench_dynamic_set(&samples));
		});
	}

	group.finish();
}

criterion_group!(benches, bench_hashset_vs_vector);
criterion_main!(benches);
