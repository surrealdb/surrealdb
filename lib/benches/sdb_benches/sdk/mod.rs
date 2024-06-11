use criterion::{Criterion, Throughput};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use surrealdb::{engine::any::Any, sql::Id, Surreal};

mod routines;

static DB: Lazy<Surreal<Any>> = Lazy::new(Surreal::init);

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Record {
	field: Id,
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
