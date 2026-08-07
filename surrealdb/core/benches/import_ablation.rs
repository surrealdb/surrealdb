#![allow(clippy::unwrap_used)]
#![allow(clippy::print_stderr)]
#![recursion_limit = "256"]

//! Cost attribution for the `.surql` import path.
//!
//! Runs the same record corpus through `Datastore::import_stream()` under a
//! series of ablations, so the wall-clock difference between two runs isolates
//! one stage of the pipeline:
//!
//! | case          | what it adds over the case above           |
//! |---------------|-------------------------------------------|
//! | `parse`       | tokenise + parse + `sql` -> `expr` convert |
//! | `drop`        | statement dispatch, document pipeline, one commit per statement (a `DROP` table discards writes) |
//! | `idx0`        | record KV writes                          |
//! | `idxN_before` | incremental maintenance of N indexes, defined ahead of the data (what `surreal export` emits today) |
//! | `idxN_after`  | one batched build per index, defined after the data |
//!
//! Also sweeps records-per-statement to price the per-commit overhead, prices
//! `INSERT` against `CREATE` to separate the statement form from the work it
//! shares, and reports gzip/zstd ratio and throughput for the generated corpus.
//!
//! ```bash
//! cargo bench -p surrealdb-core --bench import_ablation --features kv-rocksdb
//!
//! ABL_RECORDS=200000 ABL_BACKEND=rocksdb \
//!   cargo bench -p surrealdb-core --bench import_ablation --features kv-rocksdb
//! ```

use std::fmt::Write as _;
use std::io::Write as _;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use surrealdb_core::dbs::{Capabilities, Session};
use surrealdb_core::kvs::Datastore;
use surrealdb_core::syn::parser::{ParserSettings, StatementStream};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

fn env_usize(key: &str, default: usize) -> usize {
	std::env::var(key).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

fn env_str(key: &str, default: &str) -> String {
	std::env::var(key).unwrap_or_else(|_| default.to_string())
}

/// Approximate size of one generated record in the SQL text.
static TARGET_RECORD_SIZE: std::sync::LazyLock<usize> =
	std::sync::LazyLock::new(|| env_usize("ABL_RECORD_SIZE", 700));

/// A vocabulary of pseudo-words, large enough that text drawn from it has
/// roughly the entropy of prose rather than of a repeating cycle.
static VOCAB: std::sync::LazyLock<Vec<String>> = std::sync::LazyLock::new(|| {
	let mut state = 0x9E37_79B9_7F4A_7C15u64;
	(0..2048)
		.map(|_| {
			state ^= state << 13;
			state ^= state >> 7;
			state ^= state << 17;
			let len = 3 + (state % 8) as usize;
			(0..len).map(|i| (b'a' + ((state >> (i * 5)) % 26) as u8) as char).collect::<String>()
		})
		.collect()
});

// ---------------------------------------------------------------------------
// Corpus generation
// ---------------------------------------------------------------------------

/// Record shape. `wide` carries several scalar fields plus a nested object;
/// `narrow` carries the same byte count in a single string field. Comparing the
/// two prices the per-field cost of turning a parsed literal into a `Value`.
static SHAPE: std::sync::LazyLock<String> =
	std::sync::LazyLock::new(|| env_str("ABL_SHAPE", "wide"));

/// Extra scalar fields appended to each record. Holding the byte count fixed
/// while varying this isolates the per-field cost of literal evaluation.
static EXTRA_FIELDS: std::sync::LazyLock<usize> =
	std::sync::LazyLock::new(|| env_usize("ABL_EXTRA_FIELDS", 0));

/// Elements in each record's `tags` array. A plain or unique index over an
/// array-valued field expands the array into one index entry per element, so
/// this is the knob that prices index fan-out. Zero leaves the field off
/// entirely, which is what keeps every other measurement comparable.
static ARRAY_TAGS: std::sync::LazyLock<usize> =
	std::sync::LazyLock::new(|| env_usize("ABL_ARRAY_TAGS", 0));

/// One record, shaped like a document a real export would carry: a few scalar
/// fields, a nested object, and a text blob that dominates the byte size.
fn record(id: usize) -> String {
	let mut base = if *SHAPE == "narrow" {
		format!("{{ id: person:{id}, ")
	} else {
		format!(
			"{{ id: person:{id}, \
		   name: 'Person {id:08}', \
		   email: 'user_{id:08}@example.com', \
		   age: {age}, \
		   active: {active}, \
		   address: {{ street: '{id} Main St', city: 'City{city:03}', \
		               state: 'ST{state:02}', zip: '{zip:05}' }}, ",
			age = 18 + (id % 62),
			active = if id.is_multiple_of(2) {
				"true"
			} else {
				"false"
			},
			city = id % 500,
			state = id % 50,
			zip = 10000 + (id % 80000),
		)
	};
	for f in 0..*EXTRA_FIELDS {
		let _ = write!(base, "f{f}: {}, ", id + f);
	}
	if *ARRAY_TAGS > 0 {
		base.push_str("tags: [");
		for t in 0..*ARRAY_TAGS {
			if t > 0 {
				base.push_str(", ");
			}
			let _ = write!(base, "'tag{:04}'", (id + t) % 1000);
		}
		base.push_str("], ");
	}
	// The bio is always the last field, and always the one that absorbs the
	// remaining byte budget, so record size stays fixed as field count varies.
	base.push_str("bio: '");
	// Fill the bio with prose-like text: words drawn pseudo-randomly from a
	// large vocabulary, so the blob has the entropy of real text rather than
	// of a short repeating cycle (which would flatter the compression numbers).
	let closing = "' }";
	let mut state = (id as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
	while base.len() + closing.len() < *TARGET_RECORD_SIZE {
		state ^= state << 13;
		state ^= state >> 7;
		state ^= state << 17;
		base.push_str(&VOCAB[(state % VOCAB.len() as u64) as usize]);
		base.push(' ');
	}
	base.push_str(closing);
	base
}

/// One `INSERT` statement carrying `batch` records, in exactly the
/// `INSERT [ … ];` form `export_regular_data` emits (the table comes from each
/// record's embedded `id`, not from an `INTO` clause).
fn insert_statement(first_id: usize, batch: usize) -> String {
	let mut sql = String::with_capacity(batch * *TARGET_RECORD_SIZE + 64);
	sql.push_str("INSERT [");
	for i in 0..batch {
		if i > 0 {
			sql.push_str(", ");
		}
		sql.push_str(&record(first_id + i));
	}
	sql.push_str("];\n");
	sql
}

/// One `CREATE … CONTENT { … }` statement carrying a single record.
///
/// The record's `id` rides in the content, as `CREATE person CONTENT { id:
/// person:1, … }`, so the statement carries exactly the same bytes as the
/// one-record `INSERT` form and differs only in the statement kind.
fn create_statement(id: usize) -> String {
	format!("CREATE person CONTENT {};\n", record(id))
}

/// The index set, ordered so that taking the first N gives a sensible
/// N-index configuration. Names are stable so `INFO FOR INDEX` can find them.
fn index_defs(table: &str) -> Vec<String> {
	vec![
		format!("DEFINE INDEX ix_email ON {table} FIELDS email UNIQUE"),
		format!("DEFINE INDEX ix_age ON {table} FIELDS age"),
		format!("DEFINE INDEX ix_city ON {table} FIELDS address.city"),
		format!("DEFINE INDEX ix_active ON {table} FIELDS active"),
		format!("DEFINE INDEX ix_name ON {table} FIELDS name"),
		format!("DEFINE INDEX ix_zip ON {table} FIELDS address.zip"),
		format!("DEFINE INDEX ix_state_age ON {table} FIELDS address.state, age"),
		format!("DEFINE INDEX ix_email_active ON {table} FIELDS email, active"),
	]
}

/// An index over the array-valued `tags` field, which fans out to one entry per
/// element rather than one per record.
fn array_index_defs(table: &str) -> Vec<String> {
	vec![format!("DEFINE INDEX ix_tags ON {table} FIELDS tags")]
}

/// A full-text index needs an analyzer, so it comes as a pair.
fn fulltext_defs(table: &str) -> Vec<String> {
	vec![
		"DEFINE ANALYZER ab TOKENIZERS blank,class FILTERS lowercase".to_string(),
		format!("DEFINE INDEX ix_bio ON {table} FIELDS bio FULLTEXT ANALYZER ab BM25"),
	]
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

struct Case {
	name: &'static str,
	/// Table definition statements applied before any data.
	schema: Vec<String>,
	/// Index statements applied before the data.
	before: Vec<String>,
	/// Index statements applied after the data.
	after: Vec<String>,
}

#[derive(Default)]
struct Timing {
	schema: Duration,
	insert: Duration,
	build: Duration,
}

impl Timing {
	fn total(&self) -> Duration {
		self.schema + self.insert + self.build
	}
}

/// The datastore builder does not read the environment, so the write-key guard
/// has to be handed in explicitly. Setting it lets a run be used to count the
/// keys one record writes: the smallest limit that still succeeds is that count.
fn builder() -> surrealdb_core::kvs::Builder {
	let mut b = Datastore::builder().with_capabilities(Capabilities::all());
	let limit = env_usize("ABL_MAX_WRITE_KEYS", 0);
	if limit > 0 {
		b = b.with_config(
			surrealdb_core::cnf::ConfigMap::empty()
				.with_key_value("transaction_max_write_keys", limit.to_string()),
		);
	}
	b
}

async fn open(backend: &str) -> (Datastore, Option<temp_dir::TempDir>) {
	match backend {
		"memory" => (builder().build_with_path("memory").await.unwrap(), None),
		"rocksdb" => {
			let dir = temp_dir::TempDir::new().unwrap();
			let path = format!("rocksdb:{}", dir.path().display());
			let ds = builder().build_with_path(&path).await.unwrap();
			(ds, Some(dir))
		}
		other => panic!("unknown ABL_BACKEND: {other}"),
	}
}

/// Feed `statements` through `import_stream`, panicking on any statement error.
///
/// A statement that does not already terminate itself is terminated here, so
/// the case tables can hold bare `DEFINE …` text.
async fn run_import(ds: &Datastore, ses: &Session, statements: Vec<String>) {
	let stream = futures::stream::iter(
		std::iter::once(Ok::<Bytes, anyhow::Error>(Bytes::from_static(b"OPTION IMPORT;\n"))).chain(
			statements.into_iter().map(|mut s| {
				if !s.ends_with(";\n") {
					s.push_str(";\n");
				}
				Ok(Bytes::from(s))
			}),
		),
	);
	let results = ds.import_stream(ses, stream).await.unwrap();
	let errors: Vec<_> = results.iter().filter(|r| r.result.is_err()).collect();
	assert!(errors.is_empty(), "import errors: {:?}", errors.first().map(|e| &e.result));
}

/// Parse and convert every statement without executing any of them.
///
/// Mirrors `Datastore::execute_import`'s incremental buffering so the measured
/// work is the same tokenise/parse/convert the import performs.
fn run_parse_only(statements: &[String]) -> usize {
	let mut stream = StatementStream::new_with_settings(ParserSettings::default());
	let mut buffer = BytesMut::new();
	let mut parsed = 0usize;
	for s in statements {
		buffer.extend_from_slice(s.as_bytes());
		while let Some(stmt) = stream.parse_partial(&mut buffer).unwrap() {
			// The import path converts every parsed statement into the
			// execution AST before dispatch, so price that here too.
			std::hint::black_box(surrealdb_core::expr::TopLevelExpr::from(stmt));
			parsed += 1;
		}
	}
	while let Some(stmt) = stream.parse_complete(&mut buffer).unwrap() {
		std::hint::black_box(surrealdb_core::expr::TopLevelExpr::from(stmt));
		parsed += 1;
	}
	parsed
}

async fn run_case(backend: &str, case: &Case, inserts: &[String], records: usize) -> Timing {
	let (ds, _dir) = open(backend).await;
	let ses = Session::owner().with_ns("test").with_db("test");
	ds.execute("USE NAMESPACE test DATABASE test", &ses, None).await.unwrap();

	let mut t = Timing::default();

	let start = Instant::now();
	let mut schema = case.schema.clone();
	schema.extend(case.before.iter().cloned());
	if !schema.is_empty() {
		run_import(&ds, &ses, schema).await;
	}
	t.schema = start.elapsed();

	let start = Instant::now();
	run_import(&ds, &ses, inserts.to_vec()).await;
	t.insert = start.elapsed();

	let start = Instant::now();
	if !case.after.is_empty() {
		run_import(&ds, &ses, case.after.clone()).await;
	}
	t.build = start.elapsed();

	// A case that discards its writes has nothing to verify.
	let dropped = case.schema.iter().any(|s| s.contains("DROP"));
	if !dropped {
		let res = ds
			.execute("SELECT count() FROM person GROUP ALL", &ses, None)
			.await
			.unwrap()
			.remove(0)
			.result
			.unwrap();
		let json = res.into_json_value();
		let got = json
			.get(0)
			.and_then(|v| v.get("count"))
			.and_then(|v| v.as_u64())
			.unwrap_or_else(|| panic!("unexpected count shape: {json}"));
		assert_eq!(got as usize, records, "{}: wrong record count", case.name);
	}

	// An index that silently failed to backfill would make the whole "after"
	// ordering meaningless (a `DEFINE INDEX` sharing a transaction with the
	// data it indexes backfills nothing), so confirm every index reached the
	// terminal healthy state and that a post-data build actually scanned the
	// records.
	for (def, is_after) in
		case.before.iter().map(|d| (d, false)).chain(case.after.iter().map(|d| (d, true)))
	{
		if !def.starts_with("DEFINE INDEX") {
			continue;
		}
		let Some(name) = def.split_whitespace().nth(2) else {
			continue;
		};
		let sql = format!("INFO FOR INDEX {name} ON person");
		let res = ds.execute(&sql, &ses, None).await.unwrap().remove(0).result.unwrap();
		let json = res.into_json_value();
		let building = json.get("building").cloned().unwrap_or(serde_json::Value::Null);
		let status = building.get("status").and_then(|v| v.as_str()).unwrap_or("<missing>");
		assert_eq!(
			status, "ready",
			"{}: index {name} is not ready, building={building}",
			case.name
		);
		if is_after {
			let scanned = building.get("initial").and_then(|v| v.as_u64()).unwrap_or(0)
				+ building.get("updated").and_then(|v| v.as_u64()).unwrap_or(0);
			assert_eq!(
				scanned as usize, records,
				"{}: index {name} backfilled {scanned} of {records} records, building={building}",
				case.name
			);
		}
	}

	t
}

fn build_cases() -> Vec<Case> {
	let plain = vec!["DEFINE TABLE person SCHEMALESS".to_string()];
	let ixs = index_defs("person");
	let ft = fulltext_defs("person");

	let mut cases = vec![
		Case {
			name: "drop",
			schema: vec!["DEFINE TABLE person DROP SCHEMALESS".to_string()],
			before: vec![],
			after: vec![],
		},
		Case {
			name: "idx0",
			schema: plain.clone(),
			before: vec![],
			after: vec![],
		},
	];

	for n in [1usize, 4, 8] {
		cases.push(Case {
			name: match n {
				1 => "idx1_before",
				4 => "idx4_before",
				_ => "idx8_before",
			},
			schema: plain.clone(),
			before: ixs[..n].to_vec(),
			after: vec![],
		});
		cases.push(Case {
			name: match n {
				1 => "idx1_after",
				4 => "idx4_after",
				_ => "idx8_after",
			},
			schema: plain.clone(),
			before: vec![],
			after: ixs[..n].to_vec(),
		});
	}

	// An index over an array-valued field: the same index kind as the b-tree
	// cases above, but its per-record entry count follows the array's length
	// rather than being one.
	cases.push(Case {
		name: "arr1_before",
		schema: vec!["DEFINE TABLE person SCHEMALESS".to_string()],
		before: array_index_defs("person"),
		after: vec![],
	});

	// Full-text is a different cost class, so it gets its own pair. The
	// analyzer is schema, not an index, so it stays ahead of the data in both.
	cases.push(Case {
		name: "ft1_before",
		schema: {
			let mut s = plain.clone();
			s.push(ft[0].clone());
			s
		},
		before: vec![ft[1].clone()],
		after: vec![],
	});
	cases.push(Case {
		name: "ft1_after",
		schema: {
			let mut s = plain;
			s.push(ft[0].clone());
			s
		},
		before: vec![],
		after: vec![ft[1].clone()],
	});

	cases
}

// ---------------------------------------------------------------------------
// Export
// ---------------------------------------------------------------------------

/// Measures the backup/restore round trip: seed a datastore under `schema`,
/// export it, then import the export's own bytes into a fresh datastore.
///
/// Restore time is what a user experiences, and it is governed by how the
/// exporter grouped records into `INSERT` statements, because an import runs one
/// statement per transaction. Importing the generated corpus instead would
/// measure a grouping the exporter never chose.
async fn run_roundtrip(backend: &str, inserts: &[String], records: usize, schema: &[String]) {
	let (ds, _dir) = open(backend).await;
	let ses = Session::owner().with_ns("test").with_db("test");
	ds.execute("USE NAMESPACE test DATABASE test", &ses, None).await.unwrap();
	let mut setup = vec!["DEFINE TABLE person SCHEMALESS".to_string()];
	setup.extend(schema.iter().cloned());
	run_import(&ds, &ses, setup).await;
	run_import(&ds, &ses, inserts.to_vec()).await;

	let (tx, rx) = surrealdb_core::channel::bounded::<Vec<u8>>(64);
	let task = ds.export(&ses, tx).await.unwrap();
	let collector = tokio::spawn(async move {
		let mut out = Vec::new();
		while let Ok(chunk) = rx.recv().await {
			out.extend_from_slice(&chunk);
		}
		out
	});
	task.await.unwrap();
	let exported = collector.await.unwrap();
	let text = String::from_utf8(exported).unwrap();

	// Every fixture record carries its own `id`, and the wide shape nests an
	// `address` object, so brace counting would over-count; the id field is what
	// occurs exactly once per record.
	let groups: Vec<usize> = text
		.lines()
		.filter(|l| l.starts_with("INSERT [ "))
		.map(|l| l.matches("id: person:").count())
		.collect();
	let largest = groups.iter().copied().max().unwrap_or(0);

	// Restore into a fresh store, importing exactly what the export produced.
	let (fresh, _dir2) = open(backend).await;
	let ses2 = Session::owner().with_ns("test").with_db("test");
	fresh.execute("USE NAMESPACE test DATABASE test", &ses2, None).await.unwrap();
	let start = Instant::now();
	let stream = futures::stream::iter(vec![Ok::<Bytes, anyhow::Error>(Bytes::from(text))]);
	let results = fresh.import_stream(&ses2, stream).await.unwrap();
	let errors: Vec<_> = results.iter().filter(|r| r.result.is_err()).collect();
	assert!(errors.is_empty(), "restore errors: {:?}", errors.first().map(|e| &e.result));
	let elapsed = start.elapsed();

	let res = fresh
		.execute("SELECT count() FROM person GROUP ALL", &ses2, None)
		.await
		.unwrap()
		.remove(0)
		.result
		.unwrap();
	let json = res.into_json_value();
	let got = json.get(0).and_then(|v| v.get("count")).and_then(|v| v.as_u64()).unwrap_or(0);
	assert_eq!(got as usize, records, "restore lost records");

	eprintln!(
		"{:<10} {:>10} {:>12} {:>10.2}s {:>12.0}",
		if schema.is_empty() {
			"none"
		} else {
			"schema"
		},
		groups.len(),
		largest,
		elapsed.as_secs_f64(),
		records as f64 / elapsed.as_secs_f64()
	);
}

/// Populate a datastore with the corpus, then run a real export through
/// `Datastore::export_with_config` and return its bytes.
///
/// Reports the export's wall clock and, because the core export sends one
/// channel message per emitted line, the message count -- which is what the
/// gRPC transport turns into one frame each.
async fn run_export(backend: &str, inserts: &[String], records: usize) -> Vec<u8> {
	let (ds, _dir) = open(backend).await;
	let ses = Session::owner().with_ns("test").with_db("test");
	ds.execute("USE NAMESPACE test DATABASE test", &ses, None).await.unwrap();
	run_import(&ds, &ses, vec!["DEFINE TABLE person SCHEMALESS".to_string()]).await;
	run_import(&ds, &ses, inserts.to_vec()).await;

	let (tx, rx) = surrealdb_core::channel::bounded::<Vec<u8>>(1);
	let task = ds.export(&ses, tx).await.unwrap();
	let collector = tokio::spawn(async move {
		let mut out = Vec::new();
		let mut messages = 0usize;
		let mut largest = 0usize;
		while let Ok(chunk) = rx.recv().await {
			messages += 1;
			largest = largest.max(chunk.len());
			out.extend_from_slice(&chunk);
		}
		(out, messages, largest)
	});
	let start = Instant::now();
	task.await.unwrap();
	let (out, messages, largest) = collector.await.unwrap();
	let elapsed = start.elapsed();

	eprintln!("\n=== Export ({backend}, {records} records) ===");
	eprintln!(
		"  {:.2}s  {:.0} records/s  {:.2} MiB  {:.1} MiB/s",
		elapsed.as_secs_f64(),
		records as f64 / elapsed.as_secs_f64(),
		out.len() as f64 / (1024.0 * 1024.0),
		(out.len() as f64 / (1024.0 * 1024.0)) / elapsed.as_secs_f64(),
	);
	// One channel message per line the exporter writes, so its size follows the
	// records a line carries. A transport frames these itself rather than
	// forwarding them one for one, so this is the shape the framing is handed,
	// not the shape that reaches a client.
	eprintln!(
		"  channel messages: {messages}, largest {:.2} MiB",
		largest as f64 / (1024.0 * 1024.0)
	);
	out
}

// ---------------------------------------------------------------------------
// Compression
// ---------------------------------------------------------------------------

fn compression_report(label: &str, corpus: &[u8]) {
	eprintln!("\n=== Compression: {label} ===");
	eprintln!("  raw: {:.2} MiB", corpus.len() as f64 / (1024.0 * 1024.0));

	for level in [1u32, 6] {
		let start = Instant::now();
		let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::new(level));
		enc.write_all(corpus).unwrap();
		let out = enc.finish().unwrap();
		let elapsed = start.elapsed();
		eprintln!(
			"  gzip -{level}: {:.2} MiB  ratio {:.2}x  {:.0} MiB/s compress",
			out.len() as f64 / (1024.0 * 1024.0),
			corpus.len() as f64 / out.len() as f64,
			(corpus.len() as f64 / (1024.0 * 1024.0)) / elapsed.as_secs_f64(),
		);
	}

	for level in [1i32, 3, 9] {
		let start = Instant::now();
		let out = zstd::encode_all(corpus, level).unwrap();
		let elapsed = start.elapsed();
		let dstart = Instant::now();
		let back = zstd::decode_all(&out[..]).unwrap();
		let delapsed = dstart.elapsed();
		assert_eq!(back.len(), corpus.len());
		eprintln!(
			"  zstd -{level}: {:.2} MiB  ratio {:.2}x  {:.0} MiB/s compress  {:.0} MiB/s decompress",
			out.len() as f64 / (1024.0 * 1024.0),
			corpus.len() as f64 / out.len() as f64,
			(corpus.len() as f64 / (1024.0 * 1024.0)) / elapsed.as_secs_f64(),
			(corpus.len() as f64 / (1024.0 * 1024.0)) / delapsed.as_secs_f64(),
		);
	}

	// A gRPC transport compresses each message on its own, so the ratio it
	// achieves is not the whole-stream ratio above: every frame's compressor
	// starts without the history of the ones before it. Pricing the export at
	// the frame size the transport actually uses is what says whether framing
	// erodes the win.
	for chunk in [64usize * 1024, 256 * 1024, 1024 * 1024] {
		let start = Instant::now();
		let total: usize =
			corpus.chunks(chunk).map(|c| zstd::encode_all(c, 3).unwrap().len()).sum();
		let elapsed = start.elapsed();
		eprintln!(
			"  zstd -3 per {:>4} KiB frame: {:.2} MiB  ratio {:.2}x  {:.0} MiB/s compress",
			chunk / 1024,
			total as f64 / (1024.0 * 1024.0),
			corpus.len() as f64 / total as f64,
			(corpus.len() as f64 / (1024.0 * 1024.0)) / elapsed.as_secs_f64(),
		);
	}
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

fn main() {
	let records = env_usize("ABL_RECORDS", 50_000);
	let batch = env_usize("ABL_BATCH", 1000);
	let backend = env_str("ABL_BACKEND", "memory");
	let only = env_str("ABL_CASES", "");
	// Which sections to run. Narrowing this is what makes the binary usable
	// under a sampling profiler: one section, one kind of work in the profile.
	let phases = env_str("ABL_PHASES", "parse,ladder,stmtform,sweep,export,compress");
	let phase = |name: &str| phases.split(',').any(|p| p.trim() == name);
	let sweep = phase("sweep") && env_str("ABL_SWEEP", "1") == "1";

	let statements = records.div_ceil(batch);
	// The tail statement carries only what is left, so the corpus holds exactly
	// `records` records however the two divide. Emitting a full final batch
	// instead would import more than asked for, trip the per-case count
	// assertion, and divide every reported rate by the wrong denominator.
	let inserts: Vec<String> = (0..statements)
		.map(|i| {
			let first = i * batch;
			insert_statement(first, batch.min(records - first))
		})
		.collect();
	let corpus_bytes: usize = inserts.iter().map(|s| s.len()).sum();

	eprintln!("=== Import ablation ===");
	eprintln!(
		"  size_of: Value={} Object={} Array={} Number={} RecordId={}",
		std::mem::size_of::<surrealdb_core::val::Value>(),
		std::mem::size_of::<surrealdb_core::val::Object>(),
		std::mem::size_of::<surrealdb_core::val::Array>(),
		std::mem::size_of::<surrealdb_core::val::Number>(),
		std::mem::size_of::<surrealdb_core::val::RecordId>(),
	);
	eprintln!(
		"  size_of: expr::Expr={} expr::Literal={} sql::Expr={}",
		std::mem::size_of::<surrealdb_core::expr::Expr>(),
		std::mem::size_of::<surrealdb_core::expr::Literal>(),
		std::mem::size_of::<surrealdb_core::sql::Expr>(),
	);
	eprintln!("  shape:              {}", *SHAPE);
	eprintln!("  backend:            {backend}");
	eprintln!("  records:            {records}");
	eprintln!("  records/statement:  {batch}");
	eprintln!("  statements:         {statements}");
	eprintln!(
		"  SQL corpus:         {:.2} MiB ({:.0} B/record)",
		corpus_bytes as f64 / (1024.0 * 1024.0),
		corpus_bytes as f64 / records as f64
	);

	let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();

	eprintln!(
		"\n{:<14} {:>9} {:>9} {:>9} {:>9} {:>12}",
		"case", "schema", "insert", "build", "total", "records/s"
	);

	// --- parse only -------------------------------------------------------
	if phase("parse") {
		let start = Instant::now();
		let parsed = run_parse_only(&inserts);
		let parse_elapsed = start.elapsed();
		assert_eq!(parsed, statements);
		eprintln!(
			"{:<14} {:>9} {:>9} {:>9} {:>8.2}s {:>12.0}",
			"parse",
			"-",
			"-",
			"-",
			parse_elapsed.as_secs_f64(),
			records as f64 / parse_elapsed.as_secs_f64()
		);
	}

	// --- ablation ladder --------------------------------------------------
	let mut baseline: Option<Duration> = None;
	for case in build_cases() {
		if !phase("ladder") {
			break;
		}
		if !only.is_empty() && !only.split(',').any(|c| c == case.name) {
			continue;
		}
		let t = rt.block_on(run_case(&backend, &case, &inserts, records));
		let total = t.total();
		if case.name == "idx0" {
			baseline = Some(total);
		}
		let vs = match baseline {
			Some(b) if case.name != "idx0" => {
				format!("  ({:.2}x idx0)", total.as_secs_f64() / b.as_secs_f64())
			}
			_ => String::new(),
		};
		eprintln!(
			"{:<14} {:>8.2}s {:>8.2}s {:>8.2}s {:>8.2}s {:>12.0}{}",
			case.name,
			t.schema.as_secs_f64(),
			t.insert.as_secs_f64(),
			t.build.as_secs_f64(),
			total.as_secs_f64(),
			records as f64 / total.as_secs_f64(),
			vs,
		);
	}

	// --- statement form ---------------------------------------------------
	// `INSERT` takes a savepoint per record and carries the `ON DUPLICATE KEY
	// UPDATE` conflict path; `CREATE` does neither. Both forms are priced at one
	// record per statement, which holds the transaction count equal, so the
	// difference between the two rows is the statement form and nothing else.
	//
	// The delta is an upper bound on what dropping the savepoint could recover,
	// since the two statements differ in more than that one step.
	if phase("stmtform") {
		let n = records.min(20_000);
		eprintln!("\n=== statement form (idx0, {backend}, 1 record/statement) ===");
		eprintln!("{:<10} {:>10} {:>10} {:>12}", "form", "records", "total", "records/s");
		let forms: [(&str, Vec<String>); 2] = [
			("insert", (0..n).map(|i| insert_statement(i, 1)).collect()),
			("create", (0..n).map(create_statement).collect()),
		];
		for (name, data) in forms {
			let case = Case {
				name: "stmtform",
				schema: vec!["DEFINE TABLE person SCHEMALESS".to_string()],
				before: vec![],
				after: vec![],
			};
			let t = rt.block_on(run_case(&backend, &case, &data, n));
			eprintln!(
				"{:<10} {:>10} {:>9.2}s {:>12.0}",
				name,
				n,
				t.total().as_secs_f64(),
				n as f64 / t.total().as_secs_f64()
			);
		}
	}

	// --- commit-granularity sweep ----------------------------------------
	// One statement is one transaction, so varying records-per-statement varies
	// commits-per-record with everything else held constant.
	if sweep {
		// The index set the sweep runs against. Records-per-statement only sets
		// transaction size in *records*; what a transaction actually costs is the
		// keys those records write, which the index set decides. Sweeping under
		// each index set is what separates the two.
		let sweep_schema = env_str("ABL_SWEEP_SCHEMA", "none");
		let sweep_before = match sweep_schema.as_str() {
			"none" => vec![],
			"idx8" => index_defs("person"),
			"ft1" => fulltext_defs("person"),
			other => panic!("unknown ABL_SWEEP_SCHEMA: {other} (none|idx8|ft1)"),
		};
		eprintln!("\n=== records/statement sweep ({sweep_schema}, {backend}) ===");
		eprintln!(
			"{:<10} {:>10} {:>12} {:>10} {:>10} {:>12}",
			"batch", "records", "statements", "parse", "total", "records/s"
		);
		// The small batches pay a commit per handful of records, so cap their
		// corpus; the rate is what is being compared, not the total.
		for b in [1usize, 10, 100, 500, 1000, 2000, 5000, 10_000] {
			let cap = if b <= 10 {
				records.min(20_000)
			} else {
				records
			};
			if b > cap {
				continue;
			}
			let n = cap / b;
			let recs = n * b;
			let stmts: Vec<String> = (0..n).map(|i| insert_statement(i * b, b)).collect();
			let case = Case {
				name: "sweep",
				schema: vec!["DEFINE TABLE person SCHEMALESS".to_string()],
				before: sweep_before.clone(),
				after: vec![],
			};
			// Parse the same corpus separately: a very large single statement
			// forces the import's parse buffer to grow, so the sweep needs to
			// show how much of any change is parsing rather than committing.
			let pstart = Instant::now();
			run_parse_only(&stmts);
			let parse = pstart.elapsed();
			let t = rt.block_on(run_case(&backend, &case, &stmts, recs));
			eprintln!(
				"{:<10} {:>10} {:>12} {:>9.2}s {:>9.2}s {:>12.0}",
				b,
				recs,
				n,
				parse.as_secs_f64(),
				t.total().as_secs_f64(),
				recs as f64 / t.total().as_secs_f64()
			);
		}
	}

	// --- export/restore round trip ---------------------------------------
	// The exporter chooses the `INSERT` grouping, and an import runs one
	// statement per transaction, so restore time is set by that choice rather
	// than by the corpus the ladder above imports.
	if phase("roundtrip") {
		eprintln!("\n=== export/restore round trip ({backend}) ===");
		eprintln!(
			"{:<10} {:>10} {:>12} {:>11} {:>12}",
			"schema", "INSERTs", "largest", "restore", "records/s"
		);
		rt.block_on(run_roundtrip(&backend, &inserts, records, &[]));
		rt.block_on(run_roundtrip(&backend, &inserts, records, &index_defs("person")));
		rt.block_on(run_roundtrip(&backend, &inserts, records, &fulltext_defs("person")));
	}

	// --- export + compression --------------------------------------------
	//
	// Compression is measured on a real export rather than the generated
	// corpus, so the ratio reflects what the wire actually carries.
	if !phase("export") && !phase("compress") {
		return;
	}
	let exported = rt.block_on(run_export(&backend, &inserts, records));
	if !phase("compress") {
		return;
	}
	compression_report("real .surql export", &exported);

	// The generated bio padding is drawn from a small word list, which
	// compresses better than arbitrary user text. Repeat the measurement on a
	// high-entropy variant to bracket the answer for real data.
	let high_entropy: Vec<u8> = {
		let mut out = String::new();
		let mut state = 0x2545_F491_4F6C_DD1Du64;
		for i in 0..records.min(20_000) {
			let mut blob = String::new();
			while blob.len() < TARGET_RECORD_SIZE.saturating_sub(120) {
				state ^= state << 13;
				state ^= state >> 7;
				state ^= state << 17;
				let _ = write!(blob, "{state:016x}");
			}
			let _ = writeln!(
				out,
				"INSERT [ {{ id: person:{i}, name: 'Person {i:08}', \
				 email: 'user_{i:08}@example.com', bio: '{blob}' }} ];"
			);
		}
		out.into_bytes()
	};
	compression_report("high-entropy variant (lower bound)", &high_entropy);
}
