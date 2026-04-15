#![allow(clippy::unwrap_used)]

//! Import load test benchmark.
//!
//! Streams `INSERT INTO person [... 1000 records ...]` statements through
//! `Datastore::import_stream()` to validate correctness and measure throughput.
//!
//! Configure the target data size via the `IMPORT_BENCH_SIZE_GB` environment
//! variable (default: 100 MB).
//!
//! ```bash
//! # Default 100 MB (fast dev cycle)
//! cargo bench --bench import_load
//!
//! # 1 GiB
//! IMPORT_BENCH_SIZE_GB=1 cargo bench --bench import_load
//!
//! # 5 GiB (needs ~16 GB RAM)
//! IMPORT_BENCH_SIZE_GB=5 cargo bench --bench import_load
//! ```

mod common;

use std::time::{Duration, Instant};

use bytes::Bytes;
use common::create_runtime;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use futures::StreamExt;
use surrealdb_core::dbs::{Capabilities, Session};
use surrealdb_core::kvs::Datastore;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Default target size in bytes (100 MB — safe for CI and local dev).
const DEFAULT_SIZE_BYTES: u64 = 100 * 1024 * 1024;

/// Number of records per INSERT statement.
const RECORDS_PER_STATEMENT: usize = 1000;

/// Approximate target size per record in the generated SQL (bytes).
const TARGET_RECORD_SIZE: usize = 700;

/// Read the target size from `IMPORT_BENCH_SIZE_GB`, or fall back to
/// `DEFAULT_SIZE_BYTES`.
fn target_size_bytes() -> u64 {
	std::env::var("IMPORT_BENCH_SIZE_GB")
		.ok()
		.and_then(|s| s.parse::<f64>().ok())
		.map(|gb| (gb * 1024.0 * 1024.0 * 1024.0) as u64)
		.unwrap_or(DEFAULT_SIZE_BYTES)
}

// ---------------------------------------------------------------------------
// Data generation
// ---------------------------------------------------------------------------

/// Generate a single `INSERT INTO person [...]` statement containing
/// [`RECORDS_PER_STATEMENT`] records.
///
/// Each record is approximately [`TARGET_RECORD_SIZE`] bytes and looks like:
///
/// ```surql
/// { name: 'Person 00042371', email: 'user_00042371@example.com',
///   age: 42, active: true,
///   address: { street: '42371 Main St', city: 'Springfield',
///              state: 'IL', zip: '62701' },
///   bio: 'abcdefghij...(padding)...' }
/// ```
fn generate_insert_statement(statement_index: usize) -> String {
	let mut sql = String::with_capacity(RECORDS_PER_STATEMENT * TARGET_RECORD_SIZE + 256);
	sql.push_str("INSERT INTO person [");

	for i in 0..RECORDS_PER_STATEMENT {
		let global_id = statement_index * RECORDS_PER_STATEMENT + i;
		if i > 0 {
			sql.push_str(", ");
		}

		let base = format!(
			"{{ name: 'Person {global_id:08}', \
			   email: 'user_{global_id:08}@example.com', \
			   age: {age}, \
			   active: {active}, \
			   address: {{ street: '{global_id} Main St', \
			               city: 'Springfield', \
			               state: 'IL', \
			               zip: '62701' }}, \
			   bio: '",
			age = 18 + (global_id % 62),
			active = if global_id.is_multiple_of(2) {
				"true"
			} else {
				"false"
			},
		);
		sql.push_str(&base);

		// Pad the bio field so the total record reaches TARGET_RECORD_SIZE.
		let closing = "' }";
		let current = base.len() + closing.len();
		if current < TARGET_RECORD_SIZE {
			let padding_len = TARGET_RECORD_SIZE - current;
			// Repeating ASCII pattern — no single-quotes or backslashes.
			for (j, _) in (0..padding_len).enumerate() {
				sql.push((b'a' + (j % 26) as u8) as char);
			}
		}
		sql.push_str(closing);
	}

	sql.push_str("];\n");
	sql
}

/// Compute how many INSERT statements are needed to reach `target_bytes`.
fn statements_needed(target_bytes: u64) -> usize {
	let sample = generate_insert_statement(0);
	let stmt_bytes = sample.len() as u64;
	target_bytes.div_ceil(stmt_bytes) as usize
}

// ---------------------------------------------------------------------------
// Benchmark
// ---------------------------------------------------------------------------

fn bench_import_throughput(c: &mut Criterion) {
	let target = target_size_bytes();
	let num_stmts = statements_needed(target);
	let total_records = (num_stmts * RECORDS_PER_STATEMENT) as u64;
	let stmt_bytes = generate_insert_statement(0).len() as u64;
	let approx_bytes = stmt_bytes * num_stmts as u64;

	eprintln!("=== Import Load Benchmark ===");
	eprintln!(
		"  Target size:       {:.2} GiB ({} bytes)",
		target as f64 / (1024.0 * 1024.0 * 1024.0),
		target
	);
	eprintln!("  Statements:        {num_stmts}");
	eprintln!("  Records/statement: {RECORDS_PER_STATEMENT}");
	eprintln!("  Total records:     {total_records}");
	eprintln!("  SQL payload:       {:.2} GiB", approx_bytes as f64 / (1024.0 * 1024.0 * 1024.0));
	eprintln!("  Bytes/statement:   {stmt_bytes}");
	eprintln!("=============================");

	let mut group = c.benchmark_group("import_load");
	group.throughput(Throughput::Bytes(approx_bytes));
	group.sample_size(10);

	// Scale measurement time with data size.
	if target >= 1024 * 1024 * 1024 {
		group.measurement_time(Duration::from_secs(600));
		group.warm_up_time(Duration::from_secs(5));
	} else if target >= 500 * 1024 * 1024 {
		group.measurement_time(Duration::from_secs(120));
		group.warm_up_time(Duration::from_secs(5));
	} else {
		group.measurement_time(Duration::from_secs(60));
	}

	let size_label = if target >= 1024 * 1024 * 1024 {
		format!("{:.1}GiB", target as f64 / (1024.0 * 1024.0 * 1024.0))
	} else {
		format!("{:.0}MB", target as f64 / (1024.0 * 1024.0))
	};

	group.bench_function(BenchmarkId::new("import_stream", &size_label), |b| {
		let runtime = create_runtime();
		b.to_async(&runtime).iter_custom(|iters| async move {
			let mut total_elapsed = Duration::ZERO;

			for iteration in 0..iters {
				// Fresh datastore per iteration so records don't accumulate.
				let dbs = Datastore::new("memory").await.unwrap();
				let dbs = dbs.with_capabilities(Capabilities::all());
				let ses = Session::owner().with_ns("test").with_db("test");
				dbs.execute("USE NAMESPACE test DATABASE test", &ses, None).await.unwrap();

				// Lazy stream — OPTION IMPORT header followed by one statement at a time.
				let header = futures::stream::once(async {
					Ok::<Bytes, anyhow::Error>(Bytes::from_static(b"OPTION IMPORT;\n"))
				});
				let inserts = futures::stream::iter((0..num_stmts).map(|i| {
					Ok::<Bytes, anyhow::Error>(Bytes::from(generate_insert_statement(i)))
				}));
				let stream = header.chain(inserts);

				let start = Instant::now();
				let results = dbs.import_stream(&ses, stream).await.unwrap();
				let elapsed = start.elapsed();
				total_elapsed += elapsed;

				// Verify no errors.
				let errors: Vec<_> = results.iter().filter(|r| r.result.is_err()).collect();
				assert!(
					errors.is_empty(),
					"Import had {} errors: {:?}",
					errors.len(),
					errors.first().map(|e| &e.result)
				);

				// Verify all records persisted.
				let verify =
					dbs.execute("SELECT count() FROM person GROUP ALL", &ses, None).await.unwrap();
				let count_result = verify[0].result.as_ref().unwrap();

				eprintln!(
					"  [{}/{}] {:.2}s | {:.0} records/sec | {:.2} MiB/sec | count: {:?}",
					iteration + 1,
					iters,
					elapsed.as_secs_f64(),
					total_records as f64 / elapsed.as_secs_f64(),
					(approx_bytes as f64 / (1024.0 * 1024.0)) / elapsed.as_secs_f64(),
					count_result,
				);
			}

			total_elapsed
		});
	});

	group.finish();
}

// ---------------------------------------------------------------------------
// Criterion registration
// ---------------------------------------------------------------------------

criterion_group! {
	name = benches;
	config = Criterion::default();
	targets = bench_import_throughput
}
criterion_main!(benches);
