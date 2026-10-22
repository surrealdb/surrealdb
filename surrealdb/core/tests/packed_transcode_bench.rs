#![recursion_limit = "256"]
#![deny(clippy::wildcard_enum_match_arm)]
//! Nanoseconds per row of `SELECT *` over packed (SCHEMAFULL) rows written into frames, beside the
//! same rows in a schemaless twin. A measurement, `#[ignore]`d; run it with an optimised profile:
//!
//! ```text
//! cargo test --locked --profile profiling -p surrealdb-core --test packed_transcode_bench \
//!     -- --ignored --nocapture --test-threads 1
//! ```
//!
//! On an in-memory datastore it prints, per table, the median and the least over several runs of:
//!
//! * `Scan`, `Decoded`, `Transcoded`, `Shaped`: every stored record read and admitted, then nothing
//!   more, decoded whole and encoded, transcoded from its bytes, or written shaped by its copy
//!   plan, into one reused frame (`exec::row_frame_probe::scan_table`): the floor below the
//!   executor;
//! * `select cbor` / `select json` / `select shaped`: a streamed `SELECT *` with rows encoded for
//!   the wire, the items drained as a transport drains them;
//! * `bytes`: the rows of `SELECT *` encoded, per row, in bytes;
//! * `limit`: a streamed `SELECT * FROM table LIMIT 1000`, per statement, in µs, in each format;
//! * `point`: a streamed `SELECT * FROM table:id` in CBOR, per statement, in µs.
//!
//! `BENCH_ROWS` sets the row count (default 200 000) and `BENCH_RUNS` the runs (default 7);
//! `BENCH_TABLE=packed` (or `plain`) measures that table alone;
//! `BENCH_ONLY=Transcoded` (or `Scan`, `Decoded`) runs that probe alone, for a profiler.

use std::sync::Arc;

use surrealdb_core::dbs::Session;
use surrealdb_core::exec::row_frame_probe::{Probe, scan_table};
use surrealdb_core::kvs::{Datastore, QueryRequest};
use surrealdb_rpc::{QUERY_STREAM_BUFFER, RowFormat};
use web_time::Instant;

fn env(name: &str, default: usize) -> usize {
	std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// The fields of each table, as the streaming baselines define them, and a few more kinds.
const FIELDS: &str = "score int, cat int, name string, flag bool, body string, at datetime, \
	ratio float, note option<string>, tags array<string>";

async fn datastore(rows: usize) -> (Arc<Datastore>, Session) {
	let ds = Datastore::builder()
		.without_maintenance_tasks()
		.build_with_path("memory")
		.await
		.expect("datastore");
	let session = Session::owner().with_ns("test").with_db("test");
	ds.execute("DEFINE NS test; USE NS test; DEFINE DB test;", &Session::owner(), None)
		.await
		.expect("namespace and database defined");
	let mut schema = String::from("DEFINE TABLE packed SCHEMAFULL; DEFINE TABLE plain SCHEMALESS;");
	for field in FIELDS.split(", ") {
		let (name, kind) = field.split_once(' ').expect("a field and its type");
		schema.push_str(&format!("DEFINE FIELD {name} ON packed TYPE {kind};"));
	}
	for response in ds.execute(&schema, &session, None).await.expect("schema runs") {
		response.result.expect("schema succeeds");
	}
	for start in (0..rows).step_by(10_000) {
		let end = (start + 10_000).min(rows);
		let sql = format!(
			"FOR $i IN {start}..{end} {{
				LET $row = {{ score: $i * 7, cat: $i % 13, name: 'name ' + <string> $i,
					flag: $i % 2 = 0, body: string::repeat('b', 40),
					at: d'2024-01-01T00:00:00Z' + duration::from_secs($i), ratio: $i / 3f,
					note: IF $i % 2 = 0 THEN 'even' ELSE NONE END, tags: ['x', 'y'] }};
				CREATE type::record('packed', $i) CONTENT $row;
				CREATE type::record('plain', $i) CONTENT $row;
			}};"
		);
		for response in ds.execute(&sql, &session, None).await.expect("load runs") {
			response.result.expect("load succeeds");
		}
	}
	(ds, session)
}

/// Run `sql` streamed with rows encoded in `format`, draining the items: the bytes of the rows
/// they carry, encoded.
async fn stream(ds: &Arc<Datastore>, session: &Session, sql: &str, format: RowFormat) -> usize {
	let (items, received) = async_channel::bounded(QUERY_STREAM_BUFFER);
	let job = ds
		.run_streaming(QueryRequest::new(sql, session).with_row_format(Some(format)), items)
		.expect("the query starts");
	let drain = async {
		let mut bytes = 0usize;
		while let Ok(item) = received.recv().await {
			if let surrealdb_rpc::QueryStreamItem::Frame {
				rows,
				..
			} = &item
			{
				bytes += rows.bytes().len();
			}
			drop(item);
		}
		bytes
	};
	let (ran, bytes) = futures::join!(job.run, drain);
	ran.expect("the query runs");
	bytes
}

/// The formats a transport writes rows in that this build serves: CBOR, JSON, and shaped frames
/// where the `shaped` feature builds them.
fn wire_formats() -> Vec<surrealdb_rpc::RowFormat> {
	let mut formats = vec![surrealdb_rpc::RowFormat::Cbor, surrealdb_rpc::RowFormat::Json];
	if cfg!(feature = "shaped") {
		formats.push(surrealdb_rpc::RowFormat::Shaped);
	}
	formats
}

fn least(samples: &[f64]) -> f64 {
	samples.iter().copied().fold(f64::INFINITY, f64::min)
}

fn median(mut samples: Vec<f64>) -> f64 {
	samples.sort_by(f64::total_cmp);
	samples[samples.len() / 2]
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "a measurement, not a check"]
async fn packed_select_star_ns_per_row() {
	let rows = env("BENCH_ROWS", 200_000);
	let runs = env("BENCH_RUNS", 7);
	let (ds, session) = datastore(rows).await;
	println!("rows {rows}, runs {runs}; ns/row (point: µs/statement), median then least");
	let tables = std::env::var("BENCH_TABLE").ok();
	for table in ["packed", "plain"] {
		if tables.as_deref().is_some_and(|only| only != table) {
			continue;
		}
		let only = std::env::var("BENCH_ONLY").ok();
		let probes: Vec<Probe> = [
			Probe::Scan,
			Probe::Decoded,
			Probe::Transcoded,
			#[cfg(feature = "shaped")]
			Probe::Shaped,
		]
		.into_iter()
		.filter(|probe| only.as_deref().is_none_or(|only| only == format!("{probe:?}")))
		.collect();
		// The probes take turns run by run, so a slower stretch of a shared machine falls on each.
		let mut samples = vec![Vec::new(); probes.len()];
		for _ in 0..runs {
			for (at, probe) in probes.iter().enumerate() {
				let start = Instant::now();
				let (n, _) = scan_table(&ds, "test", "test", table, *probe).await.expect("scans");
				assert_eq!(n, rows);
				samples[at].push(start.elapsed().as_nanos() as f64 / rows as f64);
			}
		}
		for (probe, samples) in probes.iter().zip(samples) {
			let (min, median) = (least(&samples), median(samples));
			println!("{table:<7} {:<12}{median:>8.1} {min:>8.1}", format!("{probe:?}"));
		}
		if only.is_some() {
			continue;
		}
		for format in wire_formats() {
			let sql = format!("SELECT * FROM {table}");
			stream(&ds, &session, &sql, format).await;
			let mut samples = Vec::new();
			for _ in 0..runs {
				let start = Instant::now();
				stream(&ds, &session, &sql, format).await;
				samples.push(start.elapsed().as_nanos() as f64 / rows as f64);
			}
			let (min, median) = (least(&samples), median(samples));
			println!("{table:<7} select {:<5}{median:>8.1} {min:>8.1}", format!("{format:?}"));
		}
		// The rows' encoded bytes; a shaped block adds its shape once and a 13-byte header per
		// frame of up to 256 rows, which this leaves out.
		for format in wire_formats() {
			let bytes = stream(&ds, &session, &format!("SELECT * FROM {table}"), format).await;
			println!(
				"{table:<7} bytes {:<6}{:>8.1}",
				format!("{format:?}"),
				bytes as f64 / rows as f64
			);
		}
		for format in wire_formats() {
			let sql = format!("SELECT * FROM {table} LIMIT 1000");
			let queries = 200;
			let mut samples = Vec::new();
			for _ in 0..runs {
				let start = Instant::now();
				for _ in 0..queries {
					stream(&ds, &session, &sql, format).await;
				}
				samples.push(start.elapsed().as_nanos() as f64 / queries as f64 / 1_000.0);
			}
			let (min, median) = (least(&samples), median(samples));
			println!("{table:<7} limit {:<6}{median:>8.1} {min:>8.1}", format!("{format:?}"));
		}
		let points = 2_000;
		let mut samples = Vec::new();
		for _ in 0..runs {
			let start = Instant::now();
			for i in 0..points {
				let sql = format!("SELECT * FROM {table}:{}", (i * 7919) % rows);
				stream(&ds, &session, &sql, RowFormat::Cbor).await;
			}
			samples.push(start.elapsed().as_nanos() as f64 / points as f64 / 1_000.0);
		}
		let (min, median) = (least(&samples), median(samples));
		println!("{table:<7} point       {median:>8.1} {min:>8.1}");
	}
}
