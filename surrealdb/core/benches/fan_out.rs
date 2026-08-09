#![allow(clippy::unwrap_used)]
#![recursion_limit = "256"]

//! What overlapping the rows of a batch is worth.
//!
//! The expression layer evaluates some work once per row and can overlap those
//! rows (`exec::fan_out`). Whether that pays depends on what the rows do, which
//! is why the shapes below cover every site rather than one of them.
//!
//! Rows that dereference a record link have little to gain: they share one
//! transaction and every backend guards it, so they queue on the guard rather
//! than overlap their reads. `kv-mem` and `kv-surrealkv` take a read lock and
//! then do synchronous work under it, and `kv-rocksdb` takes an exclusive
//! `Mutex` for every operation.
//!
//! Rows that each run an operator plan are a different case: those plans buffer
//! through spawned tasks of their own, so overlapping the rows reaches more than
//! one core rather than just interleaving on one.
//!
//! Each query below is run against two datastores that differ in exactly one
//! setting, `fan_out_row_threshold`: one low enough that every batch overlaps,
//! one above any batch size so none do. Everything else — data, plan, backend —
//! is identical, so the difference is the schedule.
//!
//! ```bash
//! # kv-mem (the default)
//! cargo bench -p surrealdb-core --bench fan_out
//!
//! # another backend
//! cargo bench -p surrealdb-core --no-default-features --features kv-rocksdb --bench fan_out
//!
//! # narrow to one shape
//! cargo bench -p surrealdb-core --bench fan_out -- field_dereference
//! ```

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::create_runtime;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use surrealdb_cnf::ConfigMap;
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_rpc::capabilities::Capabilities;
use temp_dir::TempDir;
use tokio::runtime::Runtime;

/// Rows per source table. Large enough that a batch reaches the scan batch size
/// and the fan-out has something to overlap.
const ROWS: usize = 2000;

/// A threshold no batch can reach, so nothing overlaps. Comfortably above
/// `scan_batch_size`, which is what bounds a batch (default 1000). Kept well
/// short of `usize::MAX` so it parses as a `usize` on any target rather than
/// failing and silently leaving the default in place.
const NEVER: &str = "1000000";

/// A threshold every batch reaches, so everything overlaps.
const ALWAYS: &str = "2";

#[cfg(not(any(feature = "kv-mem", feature = "kv-rocksdb", feature = "kv-surrealkv")))]
compile_error!("benches/fan_out.rs needs one of kv-mem, kv-rocksdb or kv-surrealkv");

/// The datastore path for the backend this bench was built against.
///
/// Only the selected `kv-*` feature's backend is compiled in, so the path has
/// to match it: `memory` is not loadable by a build that has only RocksDB. The
/// on-disk backends take a subdirectory of `dir` named after the arm, since the
/// two datastores are live at the same time and must not share storage.
#[cfg(feature = "kv-mem")]
fn datastore_path(_dir: &TempDir, _arm: &str) -> String {
	"memory".to_owned()
}

#[cfg(all(feature = "kv-rocksdb", not(feature = "kv-mem")))]
fn datastore_path(dir: &TempDir, arm: &str) -> String {
	format!("rocksdb:{}", dir.path().join(arm).display())
}

#[cfg(all(feature = "kv-surrealkv", not(any(feature = "kv-mem", feature = "kv-rocksdb"))))]
fn datastore_path(dir: &TempDir, arm: &str) -> String {
	format!("surrealkv:{}", dir.path().join(arm).display())
}

/// The shapes that reach a fan-out, one per site in `exec::fan_out`.
///
/// Each is a whole statement rather than a microbenchmark of the helper, so
/// what is measured is the cost the engine actually pays.
const SHAPES: &[(&str, &str)] = &[
	// FieldPart::evaluate_batch — dereference a link, read one field.
	("field_dereference", "SELECT link.title FROM holder"),
	// AllPart::evaluate_batch — dereference a link, read the whole record.
	("all_dereference", "SELECT link.* FROM holder"),
	// batch_fetch_records — the FETCH clause resolves the links as a batch.
	("fetch_clause", "SELECT * FROM holder FETCH link"),
	// LookupPart::evaluate_batch — one graph traversal plan per row.
	("graph_lookup", "SELECT ->wrote->doc FROM holder"),
	// ScalarSubquery::evaluate_batch — one subquery plan per row.
	("scalar_subquery", "SELECT (SELECT title FROM doc WHERE id = $parent.link) AS t FROM holder"),
];

/// A datastore seeded with `ROWS` linked records under the given threshold.
async fn seeded(dir: &TempDir, arm: &str, threshold: &str) -> Arc<Datastore> {
	let ds = Datastore::builder()
		.with_capabilities(Capabilities::all())
		.with_config(ConfigMap::empty().with_key_value("fan_out_row_threshold", threshold))
		.build_with_path(&datastore_path(dir, arm))
		.await
		.unwrap();
	// A freshly built datastore has no namespace or database, and `DEFINE TABLE`
	// will not create them, so make them explicitly before anything else.
	ds.execute("DEFINE NAMESPACE bench", &Session::owner(), None).await.unwrap();
	ds.execute("DEFINE DATABASE bench", &Session::owner().with_ns("bench"), None).await.unwrap();

	let ses = Session::owner().with_ns("bench").with_db("bench");
	ds.execute("DEFINE TABLE doc SCHEMALESS; DEFINE TABLE holder SCHEMALESS;", &ses, None)
		.await
		.unwrap();

	// One INSERT per table keeps setup out of the measured section.
	let docs: Vec<String> = (0..ROWS)
		.map(|i| format!("{{ id: doc:{i}, title: 'title {i}', body: 'body text for {i}' }}"))
		.collect();
	ds.execute(&format!("INSERT INTO doc [{}]", docs.join(",")), &ses, None).await.unwrap();

	let holders: Vec<String> =
		(0..ROWS).map(|i| format!("{{ id: holder:{i}, link: doc:{i} }}")).collect();
	ds.execute(&format!("INSERT INTO holder [{}]", holders.join(",")), &ses, None).await.unwrap();

	let edges: Vec<String> =
		(0..ROWS).map(|i| format!("RELATE holder:{i}->wrote->doc:{i};")).collect();
	ds.execute(&edges.join(""), &ses, None).await.unwrap();

	ds
}

/// Run `sql` to completion, failing loudly rather than timing an error.
async fn run(ds: &Datastore, ses: &Session, sql: &str) {
	for response in ds.execute(sql, ses, None).await.unwrap() {
		response.result.unwrap();
	}
}

fn bench_fan_out(c: &mut Criterion) {
	let runtime: Runtime = create_runtime();
	let ses = Session::owner().with_ns("bench").with_db("bench");

	// Two datastores differing only in the threshold. Built once and shared by
	// every shape, so the comparison is not paying for setup.
	let dir = TempDir::new().unwrap();
	let overlapped = runtime.block_on(seeded(&dir, "overlapped", ALWAYS));
	let sequential = runtime.block_on(seeded(&dir, "sequential", NEVER));

	// Every shape must actually reach a fan-out; a query that stopped doing so
	// would otherwise report a flat ratio and read as "overlap does not matter".
	for (name, sql) in SHAPES {
		let rows = runtime.block_on(async {
			let mut responses = overlapped.execute(sql, &ses, None).await.unwrap();
			responses.remove(0).result.unwrap().into_array().map(|a| a.len()).unwrap_or(0)
		});
		assert_eq!(rows, ROWS, "{name} returned {rows} rows, expected {ROWS}");
	}

	let mut group = c.benchmark_group("fan_out");
	group.throughput(Throughput::Elements(ROWS as u64));
	group.sample_size(20);
	group.measurement_time(Duration::from_secs(10));

	for (name, sql) in SHAPES {
		group.bench_with_input(BenchmarkId::new(*name, "overlapped"), sql, |b, sql| {
			b.to_async(&runtime).iter(|| run(&overlapped, &ses, sql));
		});
		group.bench_with_input(BenchmarkId::new(*name, "sequential"), sql, |b, sql| {
			b.to_async(&runtime).iter(|| run(&sequential, &ses, sql));
		});
	}

	group.finish();
}

criterion_group!(benches, bench_fan_out);
criterion_main!(benches);
