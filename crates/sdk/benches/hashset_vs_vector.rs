use std::collections::HashSet;
use std::time::{Duration, SystemTime};

use criterion::measurement::WallTime;
use criterion::{BenchmarkGroup, Criterion, Throughput, criterion_group, criterion_main};
use surrealdb_core::idx::trees::dynamicset::{AHashSet, ArraySet, DynamicSet};

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
		let mut h = AHashSet::with_capacity(samples.len());
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

fn bench_array<const N: usize>(samples_vec: &Vec<Vec<u64>>) {
	for samples in samples_vec {
		let mut v = ArraySet::<N>::with_capacity(samples.len());
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

/// This bench compares the performance of insert and search for small size
/// HashSet collections. It compares HashSet, HashBrown, Vector and SmallVec.
/// It is used to help choosing the best options for the UndirectedGraph used
/// for the HNSW index. The ultimate goal is to be sure that the DynamicSet use
/// the best option based on the expected capacity.
fn bench_hashset_vs_vector(c: &mut Criterion) {
	const ITERATIONS: usize = 1_000_000;

	let mut group = c.benchmark_group("hashset_vs_vector");
	group.throughput(Throughput::Elements(ITERATIONS as u64));
	group.sample_size(10);
	group.measurement_time(Duration::from_secs(10));

	group_test::<4>(&mut group, ITERATIONS);
	group_test::<8>(&mut group, ITERATIONS);
	group_test::<16>(&mut group, ITERATIONS);
	group_test::<24>(&mut group, ITERATIONS);
	group_test::<28>(&mut group, ITERATIONS);
	group_test::<30>(&mut group, ITERATIONS);
	group_test::<32>(&mut group, ITERATIONS);

	group.finish();
}

fn group_test<const N: usize>(group: &mut BenchmarkGroup<WallTime>, iterations: usize) {
	let samples = create_samples(N, iterations);

	group.bench_function(format!("hashset_{N}"), |b| {
		b.iter(|| bench_hashset(&samples));
	});

	group.bench_function(format!("hashbrown_{N}"), |b| {
		b.iter(|| bench_hashbrown(&samples));
	});

	group.bench_function(format!("vector_{N}"), |b| {
		b.iter(|| bench_vector(&samples));
	});

	group.bench_function(format!("array_{N}"), |b| {
		b.iter(|| bench_array::<N>(&samples));
	});
}

criterion_group!(benches, bench_hashset_vs_vector);
criterion_main!(benches);
