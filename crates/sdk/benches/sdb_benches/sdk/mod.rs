use std::sync::LazyLock;
use std::time::Duration;

use criterion::{Criterion, Throughput};
use serde::{Deserialize, Serialize};
use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use surrealdb_core::val::RecordIdKey;

mod routines;

static DB: LazyLock<Surreal<Any>> = LazyLock::new(Surreal::init);

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Record {
	field: RecordIdKey,
}

pub(super) async fn init(target: &str) {
	match target {
		#[cfg(feature = "kv-mem")]
		"sdk-mem" => {
			DB.connect("memory").await.unwrap();
		}
		#[cfg(feature = "kv-rocksdb")]
		"sdk-rocksdb" => {
			let path = format!(
				"rocksdb://sdk-rocksdb-{}.db",
				std::time::SystemTime::now()
					.duration_since(std::time::UNIX_EPOCH)
					.unwrap()
					.as_millis()
			);
			println!("\n### Using path: {} ###\n", path);
			DB.connect(&path).await.unwrap();
		}
		#[cfg(feature = "kv-surrealkv")]
		"sdk-surrealkv" => {
			let path = format!(
				"surrealkv://sdk-surrealkv-{}.db",
				std::time::SystemTime::now()
					.duration_since(std::time::UNIX_EPOCH)
					.unwrap()
					.as_millis()
			);
			println!("\n### Using path: {} ###\n", path);
			DB.connect(&path).await.unwrap();
		}
		#[cfg(feature = "protocol-ws")]
		"sdk-ws" => {
			DB.connect("ws://localhost:8000").await.unwrap();
		}
		_ => panic!("Unknown target: {}", target),
	};

	DB.use_ns("test").use_db("test").await.unwrap();
}

pub(super) fn benchmark_group(c: &mut Criterion, target: String) {
	let num_ops = *super::NUM_OPS;
	let runtime = super::rt();

	runtime.block_on(async { init(&target).await });

	let mut group = c.benchmark_group(target);

	group.measurement_time(Duration::from_secs(*super::DURATION_SECS));
	group.sample_size(*super::SAMPLE_SIZE);
	group.throughput(Throughput::Elements(1));

	group.bench_function("reads", |b| {
		routines::bench_routine(b, &DB, routines::Read::new(super::rt()), num_ops)
	});
	group.bench_function("creates", |b| {
		routines::bench_routine(b, &DB, routines::Create::new(super::rt()), num_ops)
	});
	group.finish();
}
