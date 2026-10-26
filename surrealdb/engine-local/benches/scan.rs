//! Times the table scans of crud-bench's scan legs through the datastore, over a SurrealKV store
//! on disk, one client at a time and with many clients at once.
//!
//! The store holds `BENCH_RECORDS` records (default 1,000,000) shaped like crud-bench's (about
//! 1.6 KB each, random-looking 26-character ids), each written `BENCH_VERSIONS` times (default
//! 1; 2 is crud-bench's create then update). It is loaded once into `BENCH_PATH` (default
//! `surrealdb-scan-bench` in the temporary directory) and reused by
//! later runs, which then read it from its tables; `BENCH_FRESH=1` loads it again, and the scans
//! then also read what the load left in the memtables. For each case and each client count in
//! `BENCH_CLIENTS` (default `1,64`), the clients run the statement until `BENCH_QUERIES` of them
//! have run (scaled per case), and the line printed is the wall time per statement and the rows
//! of the table scanned per second.
//!
//! `BENCH_WIRE=1` instead times what a remote client's `SELECT * ... LIMIT 100` costs end to end
//! on one thread, per statement: run to values, encoded as flatbuffers (the Rust SDK's WebSocket
//! format) and decoded as the client does; and streamed as CBOR rows written from the stored
//! records, and decoded.

#![allow(clippy::unwrap_used)]
#![recursion_limit = "256"]
// TODO: Remove once this crate no longer needs it; see `absolute_paths` in the workspace lints.
#![allow(clippy::absolute_paths)]
#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Instant;

use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::{Datastore, QueryRequest};

fn env(name: &str, default: usize) -> usize {
	std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// Writes records `$from..$to`, each id a random-looking 26 characters derived from its number, so
/// that a later pass (`$verb` UPSERT) rewrites the same records.
const LOAD: &str = "FOR $i IN $from..$to { $verb type::record('record', string::slice(crypto::md5(<string> $i), 0, 26)) CONTENT {
	text: rand::string(50),
	name: rand::string(20),
	age: rand::int(1, 100),
	city: rand::enum('London', 'Paris', 'Berlin', 'Tokyo', 'New York', 'Sydney', 'Madrid', 'Rome'),
	score: rand::float(0, 100),
	number: rand::int(1, 5000),
	integer: rand::int(),
	active: rand::bool(),
	created_at: time::now(),
	http_status: rand::enum(200, 201, 400, 404, 500),
	request_id: rand::uuid(),
	status: rand::enum('draft', 'published', 'archived'),
	description: rand::string(100, 400),
	tier: rand::enum(0, 1, 2, 3),
	words: rand::string(600),
	tags: [rand::enum('alpha', 'beta'), rand::enum('theta', 'iota'), rand::enum('nu', 'xi')],
	geography: {
		code: rand::string(3),
		text: rand::string(15),
		location: rand::enum('Africa', 'Asia', 'Europe', 'Oceania'),
		description: rand::string(600),
	},
} }";

async fn open(path: &str) -> (Arc<Datastore>, Session) {
	let ds = Datastore::builder()
		.without_maintenance_tasks()
		.build_with_path(&format!("surrealkv://{path}"))
		.await
		.unwrap();
	let session = Session::owner().with_ns("bench").with_db("bench");
	ds.execute(
		"DEFINE NAMESPACE IF NOT EXISTS bench; USE NS bench; DEFINE DATABASE IF NOT EXISTS bench",
		&Session::owner(),
		None,
	)
	.await
	.unwrap();
	(ds, session)
}

/// The records the store holds.
async fn count(ds: &Datastore, session: &Session) -> usize {
	let text = "RETURN (SELECT VALUE count() FROM record GROUP ALL)[0] ?? 0";
	let mut res = ds.execute(text, session, None).await.unwrap();
	if let Ok(surrealdb_types::Value::Number(n)) = res.remove(0).result {
		n.to_int().map_or(0, |n| n as usize)
	} else {
		0
	}
}

/// Writes every record `versions` times: created, then rewritten whole, as crud-bench's update
/// leg rewrites every record before its scans.
async fn load(ds: &Datastore, session: &Session, records: usize, versions: usize) {
	let start = Instant::now();
	let batch = 1000;
	for version in 0..versions {
		let verb = if version == 0 {
			"CREATE"
		} else {
			"UPSERT"
		};
		let text = LOAD.replace("$verb", verb);
		for at in (0..records).step_by(batch) {
			let mut vars = surrealdb_types::Variables::new();
			vars.insert("from", at as i64);
			vars.insert("to", (at + batch).min(records) as i64);
			for res in ds.execute(&text, session, Some(vars)).await.unwrap() {
				res.result.unwrap();
			}
		}
	}
	println!("loaded {records} records x{versions} in {:.1?}", start.elapsed());
}

/// Runs `text` `queries` times over `clients` clients; returns seconds per statement (wall time
/// over all clients) and the first result.
async fn run(
	ds: &Arc<Datastore>,
	session: &Session,
	text: &'static str,
	clients: usize,
	queries: usize,
) -> (f64, String) {
	let first = {
		let mut res = ds.execute(text, session, None).await.unwrap();
		let value = res.remove(0).result.unwrap();
		let s = format!("{value:?}");
		s.chars().take(60).collect::<String>()
	};
	let left = Arc::new(std::sync::atomic::AtomicUsize::new(queries));
	let start = Instant::now();
	let tasks: Vec<_> = (0..clients)
		.map(|_| {
			let ds = Arc::clone(ds);
			let session = session.clone();
			let left = Arc::clone(&left);
			tokio::spawn(async move {
				while left
					.fetch_update(
						std::sync::atomic::Ordering::Relaxed,
						std::sync::atomic::Ordering::Relaxed,
						|n| n.checked_sub(1),
					)
					.is_ok()
				{
					let mut res = ds.execute(text, &session, None).await.unwrap();
					res.remove(0).result.unwrap();
				}
			})
		})
		.collect();
	for task in tasks {
		task.await.unwrap();
	}
	(start.elapsed().as_secs_f64() / queries as f64, first)
}

/// One thread's cost of `text` as a remote client sees it, in µs per statement: see the module
/// docs (`BENCH_WIRE`).
async fn wire(ds: &Arc<Datastore>, session: &Session, text: &str, iters: usize) {
	let (mut exec, mut enc, mut dec, mut size) = (0f64, 0f64, 0f64, 0usize);
	for _ in 0..iters {
		let t = Instant::now();
		let mut res = ds.execute(text, session, None).await.unwrap();
		let value = res.remove(0).result.unwrap();
		exec += t.elapsed().as_secs_f64();
		let t = Instant::now();
		let bytes = surrealdb_types::encode(&value).unwrap();
		enc += t.elapsed().as_secs_f64();
		size = bytes.len();
		let t = Instant::now();
		let back: surrealdb_types::Value = surrealdb_types::decode(&bytes).unwrap();
		dec += t.elapsed().as_secs_f64();
		assert_eq!(back, value);
	}
	let us = |s: f64| s * 1e6 / iters as f64;
	println!(
		"{text:<48} flatbuffers: run {:8.1}  encode {:8.1}  decode {:8.1}  total {:8.1} us  ({size} B)",
		us(exec),
		us(enc),
		us(dec),
		us(exec + enc + dec)
	);
	let (mut run, mut dec, mut size) = (0f64, 0f64, 0usize);
	for _ in 0..iters {
		let t = Instant::now();
		let (items, received) = async_channel::bounded(surrealdb_rpc::QUERY_STREAM_BUFFER);
		let job = ds
			.run_streaming(
				QueryRequest::new(text, session)
					.with_row_format(Some(surrealdb_rpc::RowFormat::Cbor)),
				items,
			)
			.unwrap();
		let drain = async {
			let mut frames = Vec::new();
			while let Ok(item) = received.recv().await {
				if let surrealdb_rpc::QueryStreamItem::Frame {
					rows,
					..
				} = item
				{
					frames.push(rows);
				}
			}
			frames
		};
		let (ran, frames) = futures::join!(job.run, drain);
		ran.unwrap();
		run += t.elapsed().as_secs_f64();
		let t = Instant::now();
		size = 0;
		for rows in &frames {
			let mut start = 0usize;
			for &end in rows.ends() {
				let row = &rows.bytes()[start..end as usize];
				std::hint::black_box(surrealdb_core::rpc::format::cbor::decode(row, 100).unwrap());
				start = end as usize;
			}
			size += rows.bytes().len();
		}
		dec += t.elapsed().as_secs_f64();
	}
	println!(
		"{text:<48} cbor rows:   run {:8.1}  decode {:8.1}  total {:8.1} us  ({size} B)",
		us(run),
		us(dec),
		us(run + dec)
	);
}

fn main() {
	let path = std::env::var("BENCH_PATH").unwrap_or_else(|_| {
		std::env::temp_dir().join("surrealdb-scan-bench").to_string_lossy().into_owned()
	});
	let records = env("BENCH_RECORDS", 1_000_000);
	let fresh = std::env::var_os("BENCH_FRESH").is_some();
	let versions = env("BENCH_VERSIONS", 1);
	let clients: Vec<usize> = std::env::var("BENCH_CLIENTS")
		.unwrap_or_else(|_| "1,64".into())
		.split(',')
		.map(|c| c.parse().unwrap())
		.collect();
	let queries = env("BENCH_QUERIES", 64);
	let only = std::env::var("BENCH_ONLY").ok();
	let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
	rt.block_on(async {
		// One marker per store, naming what it was loaded with: a store loaded with other
		// settings, or one that does not hold the records its marker names, is loaded again.
		let marker = format!("{path}.loaded");
		let loaded = format!("{records}x{versions}");
		let current = std::fs::read_to_string(&marker).is_ok_and(|held| held == loaded)
			&& std::path::Path::new(&path).is_dir();
		let reused = if fresh || !current {
			None
		} else {
			let (ds, session) = open(&path).await;
			if count(&ds, &session).await == records {
				Some((ds, session))
			} else {
				ds.shutdown().await.unwrap();
				None
			}
		};
		let (ds, session) = if let Some(reused) = reused {
			reused
		} else {
			let _ = std::fs::remove_dir_all(&path);
			let _ = std::fs::remove_file(&marker);
			let (ds, session) = open(&path).await;
			load(&ds, &session, records, versions).await;
			if fresh {
				(ds, session)
			} else {
				ds.shutdown().await.unwrap();
				drop(ds);
				std::fs::write(&marker, &loaded).unwrap();
				open(&path).await
			}
		};
		if std::env::var_os("BENCH_WIRE").is_some() {
			let iters = env("BENCH_ITERS", 2000);
			for text in [
				"SELECT * FROM record LIMIT 100",
				"SELECT id FROM record LIMIT 100",
				"SELECT * FROM record START 5000 LIMIT 100",
			] {
				wire(&ds, &session, text, iters).await;
			}
			ds.shutdown().await.unwrap();
			return;
		}
		// (label, statement, whole-table scans per statement, queries per `BENCH_QUERIES`)
		let cases: [(&str, &'static str, bool, usize); 6] = [
			("count()", "SELECT count() FROM record GROUP ALL", true, 1),
			(
				"count() where number = 21",
				"SELECT count() FROM record WHERE number = 21 GROUP ALL",
				true,
				1,
			),
			("select(*) where number = 21", "SELECT * FROM record WHERE number = 21", true, 1),
			("select(id) limit 100", "SELECT id FROM record LIMIT 100", false, 100),
			("select(*) limit 100", "SELECT * FROM record LIMIT 100", false, 100),
			(
				"select(*) start 5000 limit 100",
				"SELECT * FROM record START 5000 LIMIT 100",
				false,
				100,
			),
		];
		for (label, text, whole, scale) in cases {
			if only.as_deref().is_some_and(|only| !label.contains(only)) {
				continue;
			}
			for &c in &clients {
				let n = (queries * scale).max(c);
				let n = if c == 1 && whole {
					n.min(5)
				} else {
					n
				};
				let (secs, first) = run(&ds, &session, text, c, n).await;
				let rate = if whole {
					format!("{:7.1} M rows/s", records as f64 / secs / 1e6)
				} else {
					String::new()
				};
				println!(
					"{label:<32} clients {c:>3}  {:9.3} ms/stmt  {rate}   {first}",
					secs * 1e3
				);
			}
		}
		ds.shutdown().await.unwrap();
	});
}
