use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use std::collections::BTreeMap;
use std::time::Duration;
use surrealdb::dbs::Session;
use surrealdb::kvs::Datastore;
use surrealdb_core::dbs::capabilities::{FuncTarget, Targets};
use surrealdb_core::dbs::Capabilities;
use surrealdb_core::sql::{Array, Number, Object, Value};
use tokio::runtime::Runtime;

fn bench_with_or_without_index(c: &mut Criterion) {
	let rt = Runtime::new().unwrap();
	let i = rt.block_on(prepare_data());

	let mut group = c.benchmark_group("with_or_without_index");
	group.throughput(Throughput::Elements(7500));
	group.sample_size(10);
	group.measurement_time(Duration::from_secs(15));

	group.bench_function("without index", |b| {
		b.to_async(Runtime::new().unwrap())
			.iter(|| run(&i, "SELECT count() FROM t WITH NOINDEX WHERE n > 7499 GROUP ALL", 7500))
	});

	group.bench_function("with index", |b| {
		b.to_async(Runtime::new().unwrap())
			.iter(|| run(&i, "SELECT count() FROM t WHERE n > 7499 GROUP ALL", 7500))
	});

	group.finish();
}

struct Input {
	dbs: Datastore,
	ses: Session,
}

async fn prepare_data() -> Input {
	#[cfg(feature = "kv-mem")]
	let path = "memory";
	#[cfg(feature = "kv-rocksdb")]
	let path = format!(
		"rocksdb:///tmp/bench-rocksdb-{}.db",
		std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis()
	);
	let dbs = Datastore::new(&path).await.unwrap().with_capabilities(
		Capabilities::default().with_functions(Targets::<FuncTarget>::All).with_scripting(true),
	);
	let ses = Session::owner().with_ns("bench").with_db("bench");
	let sql = r"
		DEFINE INDEX idx ON TABLE t COLUMNS n;
		FOR $i IN function() { return new Array(15000).fill(0).map((_, i)=>i) } {
			CREATE t CONTENT { n: $i };
		};"
	.to_owned();
	let res = &mut dbs.execute(&sql, &ses, None).await.unwrap();
	assert_eq!(res.len(), 2);
	for _ in 0..2 {
		let r = res.remove(0);
		assert!(r.result.is_ok(), "{r:?}");
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
