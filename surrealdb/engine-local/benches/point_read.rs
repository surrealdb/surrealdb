//! Times a point read by record id through the datastore, one request at a time on one thread.
//!
//! Each case runs one statement shape with a different record every iteration, so it measures the
//! cost of a whole request — parse, plan, run — against a warm in-memory store. The record is
//! bound as a variable (`$id`, `$n`), written as a literal (`person:7`), or the same literal every
//! time (`person:1`). Output is one line per case: mean and median microseconds per request.
//! `BENCH_ITERS` sets the requests per case (default 20000), `BENCH_WARMUP` the requests before
//! timing (default 2000), and `BENCH_STATEMENT_CACHE` the `statement_cache_size` the datastore is
//! built with (default: the datastore's own default; `0` keeps no compiled statement).

#![allow(clippy::unwrap_used)]
#![recursion_limit = "256"]
// TODO: Remove once this crate no longer needs it; see `absolute_paths` in the workspace lints.
#![allow(clippy::absolute_paths)]
#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Instant;

use surrealdb_cnf::ConfigMap;
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_types::{RecordId, Variables};

const RECORDS: i64 = 1000;

fn env(name: &str, default: usize) -> usize {
	std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

async fn store(schema: &str) -> (Arc<Datastore>, Session) {
	let mut config = ConfigMap::empty();
	if let Ok(size) = std::env::var("BENCH_STATEMENT_CACHE") {
		config = config.with_key_value("statement_cache_size", &size);
	}
	let ds = Datastore::builder()
		.without_maintenance_tasks()
		.with_config(config)
		.build_with_path("memory")
		.await
		.unwrap();
	let session = Session::owner().with_ns("bench").with_db("bench");
	ds.execute(
		"DEFINE NAMESPACE bench; USE NS bench; DEFINE DATABASE bench",
		&Session::owner(),
		None,
	)
	.await
	.unwrap();
	let define = match schema {
		"schemafull" => {
			"DEFINE TABLE person SCHEMAFULL; DEFINE FIELD name ON person TYPE string; \
			 DEFINE FIELD age ON person TYPE int; DEFINE FIELD tags ON person TYPE array<string>"
		}
		_ => "DEFINE TABLE person SCHEMALESS",
	};
	for res in ds.execute(define, &session, None).await.unwrap() {
		res.result.unwrap();
	}
	for res in ds
		.execute(
			&format!(
				"FOR $i IN 0..{RECORDS} {{ CREATE type::record('person', $i) \
				 SET name = 'n' + <string> $i, age = $i, tags = ['a', 'b'] }}"
			),
			&session,
			None,
		)
		.await
		.unwrap()
	{
		res.result.unwrap();
	}
	(ds, session)
}

/// How a case names the record it reads.
#[derive(Clone, Copy)]
enum Record {
	/// `$id`, bound to the record.
	Id,
	/// `$n`, bound to the record's key.
	Key,
	/// The statement text holds the record, a different one each request.
	Literal,
	/// The statement text holds `person:1` every request.
	Fixed,
}

async fn case(ds: &Datastore, session: &Session, label: &str, text: &str, record: Record) {
	let iters = env("BENCH_ITERS", 20_000);
	let warmup = env("BENCH_WARMUP", 2_000);
	let mut samples = Vec::with_capacity(iters);
	for i in 0..warmup + iters {
		let n = match record {
			Record::Fixed => 1,
			_ => (i as i64 * 7919) % RECORDS,
		};
		let mut vars = Variables::new();
		let literal;
		let text = match record {
			Record::Id => {
				vars.insert("id", RecordId::new("person", n));
				text
			}
			Record::Key => {
				vars.insert("n", n);
				text
			}
			Record::Literal | Record::Fixed => {
				literal = text.replace("{n}", &n.to_string());
				literal.as_str()
			}
		};
		let start = Instant::now();
		let mut res = ds.execute(text, session, Some(vars)).await.unwrap();
		let value = res.remove(0).result.unwrap();
		let took = start.elapsed();
		if i == 0 {
			assert!(
				matches!(&value, surrealdb_types::Value::Array(rows) if rows.len() == 1),
				"{label}: no record for {n}: {value:?}"
			);
		}
		if i >= warmup {
			samples.push(took.as_nanos() as f64 / 1000.0);
		}
	}
	samples.sort_by(f64::total_cmp);
	let mean = samples.iter().sum::<f64>() / samples.len() as f64;
	let median = samples[samples.len() / 2];
	println!("{label:<48} mean {mean:7.1} us   median {median:7.1} us");
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
	for schema in ["schemaless", "schemafull"] {
		let (ds, session) = store(schema).await;
		for (shape, text, record) in [
			("SELECT * FROM $id", "SELECT * FROM $id", Record::Id),
			(
				"SELECT * FROM type::record('person', $n)",
				"SELECT * FROM type::record('person', $n)",
				Record::Key,
			),
			("SELECT * FROM person:<n>", "SELECT * FROM person:{n}", Record::Literal),
			("SELECT * FROM person:1", "SELECT * FROM person:{n}", Record::Fixed),
		] {
			case(&ds, &session, &format!("{schema}: {shape}"), text, record).await;
		}
	}
}
