//! Wall time of large `CREATE` statements on the in-memory and SurrealKV backends.
//!
//! Ignored by default: run in release, once with `SURREAL_HOLD_CREATES=false` (each create checks
//! its key immediately) and once without, and compare.
//!
//! ```text
//! cargo test --release -p surrealdb-core --features kv-surrealkv held_creates_bench -- --ignored --nocapture
//! ```
#![allow(clippy::unwrap_used)]

use web_time::Instant;

use crate::dbs::Session;
use crate::kvs::Datastore;

async fn timed(ds: &Datastore, ses: &Session, label: &str, sql: &str) {
	let started = Instant::now();
	let results = ds.execute(sql, ses, None).await.unwrap();
	let elapsed = started.elapsed();
	for result in results {
		result.result.unwrap();
	}
	println!("{label:<36} {:>10.1} ms", elapsed.as_secs_f64() * 1000.0);
}

async fn bench(path: &str, backend: &str) {
	let ds = Datastore::builder().without_maintenance_tasks().build_with_path(path).await.unwrap();
	let ses = Session::owner().with_ns("t").with_db("t");
	ds.execute("DEFINE NAMESPACE t; DEFINE DATABASE t;", &ses, None).await.unwrap();
	let held = surrealdb_datastore::absence::enabled();
	println!("-- {backend} (held creates: {held})");
	timed(&ds, &ses, "CREATE |a:100000|", "CREATE |a:100000| RETURN NONE").await;
	timed(&ds, &ses, "CREATE |b:1000000|", "CREATE |b:1000000| RETURN NONE").await;
	timed(&ds, &ses, "CREATE |c:100000| into 1M rows", "CREATE |b:100000| RETURN NONE").await;
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread")]
#[ignore = "a measurement, run in release"]
async fn held_creates_bench_memory() {
	bench("memory", "kv-mem").await;
}

#[cfg(feature = "kv-surrealkv")]
#[tokio::test(flavor = "multi_thread")]
#[ignore = "a measurement, run in release"]
async fn held_creates_bench_surrealkv() {
	let dir = temp_dir::TempDir::new().unwrap();
	bench(&format!("surrealkv:{}", dir.path().to_string_lossy()), "surrealkv").await;
}
