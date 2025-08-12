use std::collections::BTreeMap;
use std::time::Duration;

use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use surrealdb_core::dbs::capabilities::{FuncTarget, Targets};
use surrealdb_core::dbs::{Capabilities, Session};
use surrealdb_core::kvs::Datastore;
use surrealdb_core::val::{Array, Number, Object, Value};
use tokio::runtime::Runtime;

fn bench_with_or_without_index(c: &mut Criterion) {
	let rt = Runtime::new().unwrap();
	let i = rt.block_on(prepare_data());

	let mut group = c.benchmark_group("with_or_without_index");
	group.throughput(Throughput::Elements(50_000));
	group.sample_size(10);
	group.measurement_time(Duration::from_secs(15));

	group.bench_function("count/filter without index", |b| {
		b.to_async(Runtime::new().unwrap()).iter(|| {
			run(&i, "SELECT count() FROM t WITH NOINDEX WHERE n > 49999 GROUP ALL", 50_000)
		})
	});

	group.bench_function("count/filter with index", |b| {
		b.to_async(Runtime::new().unwrap())
			.iter(|| run(&i, "SELECT count() FROM t WHERE n > 49999 GROUP ALL", 50_000))
	});

	group.finish();

	rt.block_on(async { drop(i) });
}

struct Input {
	dbs: Datastore,
	ses: Session,
}

async fn prepare_data() -> Input {
	#[cfg(not(feature = "kv-rocksdb"))]
	let path = "memory";
	#[cfg(feature = "kv-rocksdb")]
	let path = format!(
		"rocksdb:///tmp/bench-rocksdb-{}.db",
		std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis()
	);
	let dbs = Datastore::new(&path).await.unwrap().with_capabilities(
		Capabilities::default().with_functions(Targets::<FuncTarget>::All).with_scripting(true),
	);
	//
	let ses = Session::owner().with_ns("bench").with_db("bench");
	let sql = "DEFINE INDEX idx ON TABLE t COLUMNS n";
	let res = &mut dbs.execute(sql, &ses, None).await.unwrap();
	res.remove(0).result.unwrap();
	//
	for i in 0..100_000 {
		let sql = format!("CREATE t CONTENT {{ n: {i} }}");
		let res = &mut dbs.execute(&sql, &ses, None).await.unwrap();
		res.remove(0).result.unwrap();
	}
	Input {
		dbs,
		ses,
	}
}

async fn run(i: &Input, q: &str, expected: usize) {
	let mut r = i.dbs.execute(black_box(q), &i.ses, None).await.unwrap();
	if cfg!(debug_assertions) {
		assert_eq!(r.len(), 1);
		let val = r.remove(0).result.unwrap();
		let expected = Value::Array(Array::from(vec![Value::Object(Object::from(
			BTreeMap::from([("count", Value::Number(Number::Int(expected as i64)))]),
		))]));
		assert_eq!(format!("{val:#}"), format!("{expected:#}"));
	}
	black_box(r);
}

criterion_group!(benches, bench_with_or_without_index);
criterion_main!(benches);
