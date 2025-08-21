use std::sync::{Arc, OnceLock};
use std::time::Duration;

use criterion::{Criterion, Throughput};
#[cfg(any(feature = "kv-rocksdb", feature = "kv-surrealkv"))]
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;

mod routines;

static DB: OnceLock<Arc<Datastore>> = OnceLock::new();

pub(super) async fn init(target: &str) {
	match target {
		#[cfg(feature = "kv-mem")]
		"lib-mem" => {
			let _ = DB.set(Arc::new(Datastore::new("memory").await.unwrap()));
		}
		#[cfg(feature = "kv-rocksdb")]
		"lib-rocksdb" => {
			let path = format!(
				"rocksdb://lib-rocksdb-{}.db",
				std::time::SystemTime::now()
					.duration_since(std::time::UNIX_EPOCH)
					.unwrap()
					.as_millis()
			);
			println!("\n### Using path: {} ###\n", path);
			let ds = Datastore::new(&path).await.unwrap();
			ds.execute("INFO FOR DB", &Session::owner().with_ns("ns").with_db("db"), None)
				.await
				.expect("Unable to execute the query");
			let _ = DB.set(Arc::new(ds));
		}
		#[cfg(feature = "kv-surrealkv")]
		"lib-surrealkv" => {
			let path = format!(
				"surrealkv://lib-surrealkv-{}.db",
				std::time::SystemTime::now()
					.duration_since(std::time::UNIX_EPOCH)
					.unwrap()
					.as_millis()
			);
			println!("\n### Using path: {} ###\n", path);
			let ds = Datastore::new(&path).await.unwrap();
			ds.execute("INFO FOR DB", &Session::owner().with_ns("ns").with_db("db"), None)
				.await
				.expect("Unable to execute the query");
			let _ = DB.set(Arc::new(ds));
		}

		_ => panic!("Unknown target: {}", target),
	}
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
		routines::bench_routine(
			b,
			DB.get().unwrap().clone(),
			routines::Read::new(super::rt()),
			num_ops,
		)
	});
	group.bench_function("creates", |b| {
		routines::bench_routine(
			b,
			DB.get().unwrap().clone(),
			routines::Create::new(super::rt()),
			num_ops,
		)
	});
	group.finish();
}
