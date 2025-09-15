use std::time::Duration;

use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_core::val::Value;
use tokio::runtime::{Builder, Runtime};

fn bench_order(c: &mut Criterion) {
	bench_group(c, 1000, 1);
	bench_group(c, 10000, 1);
	bench_group(c, 100000, 1);
	bench_group(c, 1000000, 1);
	bench_group(c, 1000000, 5);
}

fn bench_group(c: &mut Criterion, samples: usize, n_value: usize) {
	let mut group = c.benchmark_group(format!("{samples} - {n_value}"));
	group.sample_size(10);
	group.measurement_time(Duration::from_secs(15));

	let rt = Runtime::new().unwrap();

	let i = rt.block_on(prepare_data(samples, n_value));

	group.throughput(Throughput::Elements(samples as u64));

	group.bench_function("ORDER BY v", |b| {
		b.to_async(Builder::new_multi_thread().build().unwrap())
			.iter(|| run(&i, "SELECT * FROM i ORDER BY v LIMIT 1", samples))
	});

	group.bench_function("ORDER BY v PARALLEL", |b| {
		b.to_async(Builder::new_multi_thread().build().unwrap())
			.iter(|| run(&i, "SELECT * FROM i ORDER BY v LIMIT 1 PARALLEL", samples))
	});

	group.bench_function("ORDER BY RAND()", |b| {
		b.to_async(Builder::new_multi_thread().build().unwrap())
			.iter(|| run(&i, "SELECT * FROM i ORDER BY RAND() LIMIT 1", samples))
	});

	group.bench_function("ORDER BY RAND() PARALLEL", |b| {
		b.to_async(Builder::new_multi_thread().build().unwrap())
			.iter(|| run(&i, "SELECT * FROM i ORDER BY RAND() LIMIT 1 PARALLEL", samples))
	});

	group.finish();

	rt.block_on(async { drop(i) });
}

struct Input {
	dbs: Datastore,
	ses: Session,
}

async fn prepare_data(n: usize, n_value: usize) -> Input {
	let value = (0..n_value).map(|_| "rand::guid()").collect::<Vec<_>>().join(" + ");
	let dbs = Datastore::new("memory").await.unwrap();
	let ses = Session::owner().with_ns("bench").with_db("bench");
	let sql = format!(" CREATE |i:{n}| SET v = rand::guid(), d = {value} RETURN NONE");
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
		match r.remove(0).result.unwrap() {
			Value::Array(a) => {
				assert_eq!(a.len(), expected);
			}
			_ => {
				panic!("Fail");
			}
		}
	}
	black_box(r);
}

criterion_group!(benches, bench_order);
criterion_main!(benches);
