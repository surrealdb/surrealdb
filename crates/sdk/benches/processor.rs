use std::time::Duration;

use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_core::val::Value;
use tokio::runtime::Runtime;

fn bench_processor(c: &mut Criterion) {
	let rt = Runtime::new().unwrap();
	let i = rt.block_on(prepare_data());

	let mut group = c.benchmark_group("processor");
	group.throughput(Throughput::Elements(1));
	group.sample_size(10);
	group.measurement_time(Duration::from_secs(15));

	group.bench_function("table-iterator", |b| {
		b.to_async(Runtime::new().unwrap()).iter(|| run(&i, "SELECT * FROM item", i.count * 5))
	});

	group.bench_function("table-iterator-parallel", |b| {
		b.to_async(Runtime::new().unwrap())
			.iter(|| run(&i, "SELECT * FROM item PARALLEL", i.count * 5))
	});

	group.bench_function("non-uniq-index-iterator", |b| {
		b.to_async(Runtime::new().unwrap())
			.iter(|| run(&i, "SELECT * FROM item WHERE number=4", i.count))
	});

	group.bench_function("non-uniq-index-iterator-parallel", |b| {
		b.to_async(Runtime::new().unwrap())
			.iter(|| run(&i, "SELECT * FROM item WHERE number=4 PARALLEL", i.count))
	});

	group.bench_function("full-text-index-iterator", |b| {
		b.to_async(Runtime::new().unwrap())
			.iter(|| run(&i, "SELECT * FROM item WHERE label @@ 'charlie'", i.count))
	});

	group.bench_function("full-text-index-iterator-parallel", |b| {
		b.to_async(Runtime::new().unwrap())
			.iter(|| run(&i, "SELECT * FROM item WHERE label @@ 'charlie' PARALLEL", i.count))
	});

	group.finish();

	rt.block_on(async { drop(i) });
}

struct Input {
	dbs: Datastore,
	ses: Session,
	count: usize,
}

async fn prepare_data() -> Input {
	let dbs = Datastore::new("memory").await.unwrap();
	let ses = Session::owner().with_ns("bench").with_db("bench");
	let sql = r"DEFINE INDEX number ON item FIELDS number;
		DEFINE ANALYZER simple TOKENIZERS blank,class;
		DEFINE INDEX search ON item FIELDS label SEARCH ANALYZER simple BM25"
		.to_owned();
	let res = &mut dbs.execute(&sql, &ses, None).await.unwrap();
	for _ in 0..3 {
		res.remove(0).result.unwrap();
	}

	let count = if cfg!(debug_assertions) {
		100 // debug is much slower!
	} else {
		10_000
	};

	for i in 0..count {
		let j = i * 5;
		let a = j;
		let b = j + 1;
		let c = j + 2;
		let d = j + 3;
		let e = j + 4;
		let sql = format!(
			r"CREATE item SET id = {a}, name = '{a}', number = 0, label='alpha';
		CREATE item SET id = {b}, name = '{b}', number = 1, label='bravo';
		CREATE item SET id = {c}, name = '{c}', number = 2, label='charlie';
		CREATE item SET id = {d}, name = '{d}', number = 3, label='delta';
		CREATE item SET id = {e}, name = '{e}', number = 4, label='echo';",
		);
		let res = &mut dbs.execute(&sql, &ses, None).await.unwrap();
		for _ in 0..5 {
			res.remove(0).result.unwrap();
		}
	}
	Input {
		dbs,
		ses,
		count,
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

criterion_group!(benches, bench_processor);
criterion_main!(benches);
