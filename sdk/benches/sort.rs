use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use std::time::Duration;
use surrealdb::dbs::Session;
use surrealdb::kvs::Datastore;
use surrealdb_core::sql::Value;
use tokio::runtime::{Builder, Runtime};

/// When ordering a query, the sort method can choose between
/// single-threaded sorting or parallel sorting (Rayon::par_sort_unstable_by).
/// Following several tests, a value of 10000 has been selected to decide when we use the parallel sort.
/// This benchmark ensures that we start seeing a performance improvement.
/// 9999 = sort_unstable_by
/// >=10000 = par_sort_unstable_by
fn bench_sort(c: &mut Criterion) {
	let rt = Runtime::new().unwrap();

	let mut group = c.benchmark_group("sort");
	group.throughput(Throughput::Elements(1));
	group.sample_size(10);
	group.measurement_time(Duration::from_secs(15));

	let i = rt.block_on(prepare_data(9999));

	group.bench_function("sort 9.999 (Vec::sort_unstable_by)", |b| {
		b.to_async(Builder::new_multi_thread().build().unwrap())
			.iter(|| run(&i, "SELECT * FROM i ORDER BY v", 9999))
	});

	let i = rt.block_on(prepare_data(10000));

	group.bench_function("sort 10.000 (Rayon::par_sort_unstable_by)", |b| {
		b.to_async(Builder::new_multi_thread().build().unwrap())
			.iter(|| run(&i, "SELECT * FROM i ORDER BY v", 10000))
	});

	group.bench_function("sort 10.000 (concurrent/incremental)", |b| {
		b.to_async(Builder::new_multi_thread().build().unwrap())
			.iter(|| run(&i, "SELECT * FROM i ORDER BY v PARALLEL", 10000))
	});

	let i = rt.block_on(prepare_data(1000000));

	group.bench_function("sort 1m (Rayon::par_sort_unstable_by)", |b| {
		b.to_async(Builder::new_multi_thread().build().unwrap())
			.iter(|| run(&i, "SELECT * FROM i ORDER BY v", 1000000))
	});

	group.bench_function("sort 1m (concurrent/incremental)", |b| {
		b.to_async(Builder::new_multi_thread().build().unwrap())
			.iter(|| run(&i, "SELECT * FROM i ORDER BY v PARALLEL", 100000))
	});

	group.bench_function("random 1m (Vec::shuffle)", |b| {
		b.to_async(Builder::new_multi_thread().build().unwrap())
			.iter(|| run(&i, "SELECT * FROM i ORDER BY RAND()", 1000000))
	});

	group.bench_function("random 1m (concurrent/incremental)", |b| {
		b.to_async(Builder::new_multi_thread().build().unwrap())
			.iter(|| run(&i, "SELECT * FROM i ORDER BY RAND() PARALLEL", 100000))
	});

	group.finish();
}

struct Input {
	dbs: Datastore,
	ses: Session,
}

async fn prepare_data(n: usize) -> Input {
	let dbs = Datastore::new("memory").await.unwrap();
	let ses = Session::owner().with_ns("bench").with_db("bench");
	let sql = format!(" CREATE |i:{n}| SET v = rand::guid() RETURN NONE");
	let res = &mut dbs.execute(&sql, &ses, None).await.unwrap();
	let _ = res.remove(0).result.is_ok();
	Input {
		dbs,
		ses,
	}
}

async fn run(i: &Input, q: &str, expected: usize) {
	let mut r = i.dbs.execute(black_box(q), &i.ses, None).await.unwrap();
	if cfg!(debug_assertions) {
		assert_eq!(r.len(), 1);
		if let Value::Array(a) = r.remove(0).result.unwrap() {
			assert_eq!(a.len(), expected);
		} else {
			panic!("Fail");
		}
	}
	black_box(r);
}

criterion_group!(benches, bench_sort);
criterion_main!(benches);
