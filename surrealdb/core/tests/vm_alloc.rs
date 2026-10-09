#![recursion_limit = "256"]
// The crate's `allocator` feature installs the crate's own global allocator,
// and a binary has one, so with it this harness compiles to nothing.
#![cfg(not(feature = "allocator"))]
//! Heap allocations per row on the executor's bench shapes.
//!
//! This binary installs a counting global allocator, so it is its own
//! integration-test target: a global allocator is per binary. The measurements
//! are `#[ignore]`d; run them with a profile that keeps line tables, so the
//! sampled backtraces symbolise:
//!
//! ```text
//! cargo test --locked --profile profiling -p surrealdb-core --test vm_alloc \
//!     -- --ignored --nocapture --test-threads 1
//! ```
//!
//! `per_row_allocations` prints, per shape, the allocations and bytes each row
//! adds. Each shape runs over two datastores that differ only in their row
//! count (`SMALL` and `LARGE`); every query runs once to warm up and once
//! counted, and the per-row figure is the difference of the two counted runs
//! over the difference in rows. Parsing, planning and compiling cost the same
//! at both sizes, so the difference holds only what execution spends per row,
//! including the conversion of each result row to the public value type.
//!
//! With `ALLOC_SAMPLE=K` set, every `K`th counted allocation captures a
//! backtrace. The innermost frame inside a `surrealdb` crate names the site;
//! sites are again differenced across the two sizes and scaled by `K`, which
//! gives an estimate of each site's allocations per row. `ALLOC_SHAPES` takes
//! a comma-separated list of shape names to run a subset.

use std::alloc::{GlobalAlloc, Layout, System};
use std::backtrace::Backtrace;
use std::cell::Cell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use surrealdb_core::dbs::Session;
use surrealdb_core::exec::row_frame_probe::transcode_table;
use surrealdb_core::kvs::Datastore;
use surrealdb_types::{Array, Number, Object, Value, Variables};
use tokio::sync::Mutex as AsyncMutex;

// --------------------------------------------------
// The counting allocator
// --------------------------------------------------

/// Counts the allocations (`alloc`, `alloc_zeroed` and `realloc` calls) and
/// the bytes they request while a [`Region`] is open, on every thread.
/// Deallocations are not counted.
struct Counting;

static ENABLED: AtomicBool = AtomicBool::new(false);
static ALLOCS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);
/// Capture a backtrace every this many counted allocations; zero captures none.
static SAMPLE_EVERY: AtomicU64 = AtomicU64::new(0);
static SAMPLES: Mutex<Vec<Backtrace>> = Mutex::new(Vec::new());
/// The bytes allocated and not yet freed, on every thread, counted always.
static LIVE: AtomicI64 = AtomicI64::new(0);
/// Held by every check for its whole run: the counts and the peak are
/// process-wide, so a check measures only its own work while no other runs.
static MEASURING: AsyncMutex<()> = AsyncMutex::const_new(());
/// The most [`LIVE`] has reached since [`reset_peak`].
static PEAK: AtomicI64 = AtomicI64::new(0);

#[inline]
fn live(delta: i64) {
	let now = LIVE.fetch_add(delta, Ordering::Relaxed) + delta;
	PEAK.fetch_max(now, Ordering::Relaxed);
}

/// Start a peak measurement from the bytes live now.
fn reset_peak() -> i64 {
	let now = LIVE.load(Ordering::SeqCst);
	PEAK.store(now, Ordering::SeqCst);
	now
}

/// The most bytes live at once since [`reset_peak`] answered `base`, above
/// `base`.
fn peak_since(base: i64) -> i64 {
	PEAK.load(Ordering::SeqCst) - base
}

thread_local! {
	/// Set while this thread captures a backtrace, so the allocations the
	/// capture makes are neither counted nor sampled.
	static IN_HOOK: Cell<bool> = const { Cell::new(false) };
}

#[inline]
fn record(size: usize) {
	if !ENABLED.load(Ordering::Relaxed) || IN_HOOK.with(Cell::get) {
		return;
	}
	let n = ALLOCS.fetch_add(1, Ordering::Relaxed);
	BYTES.fetch_add(size as u64, Ordering::Relaxed);
	let every = SAMPLE_EVERY.load(Ordering::Relaxed);
	if every != 0 && n.is_multiple_of(every) {
		IN_HOOK.with(|h| h.set(true));
		let trace = Backtrace::force_capture();
		if let Ok(mut samples) = SAMPLES.lock() {
			samples.push(trace);
		}
		IN_HOOK.with(|h| h.set(false));
	}
}

unsafe impl GlobalAlloc for Counting {
	unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
		record(layout.size());
		live(layout.size() as i64);
		// SAFETY: forwarded unchanged from the caller's contract.
		unsafe { System.alloc(layout) }
	}

	unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
		record(layout.size());
		live(layout.size() as i64);
		// SAFETY: forwarded unchanged from the caller's contract.
		unsafe { System.alloc_zeroed(layout) }
	}

	unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
		live(-(layout.size() as i64));
		// SAFETY: forwarded unchanged from the caller's contract.
		unsafe { System.dealloc(ptr, layout) }
	}

	unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
		record(new_size);
		live(new_size as i64 - layout.size() as i64);
		// SAFETY: forwarded unchanged from the caller's contract.
		unsafe { System.realloc(ptr, layout, new_size) }
	}
}

#[global_allocator]
static GLOBAL: Counting = Counting;

/// What a [`Region`] counted.
#[derive(Clone, Copy, Debug, Default)]
struct Counts {
	allocs: u64,
	bytes: u64,
}

/// A scope whose allocations are counted, from [`Region::start`] to
/// [`Region::finish`]. Regions do not nest, and allocations on every thread
/// count, so nothing else may run while one is open.
struct Region;

impl Region {
	/// Open a region, capturing a backtrace every `sample_every` allocations
	/// (zero for none).
	fn start(sample_every: u64) -> Self {
		ALLOCS.store(0, Ordering::SeqCst);
		BYTES.store(0, Ordering::SeqCst);
		SAMPLE_EVERY.store(sample_every, Ordering::SeqCst);
		ENABLED.store(true, Ordering::SeqCst);
		Region
	}

	/// Close the region: its counts and the backtraces it sampled.
	fn finish(self) -> (Counts, Vec<Backtrace>) {
		ENABLED.store(false, Ordering::SeqCst);
		let counts = Counts {
			allocs: ALLOCS.load(Ordering::SeqCst),
			bytes: BYTES.load(Ordering::SeqCst),
		};
		let samples = std::mem::take(&mut *SAMPLES.lock().expect("samples lock"));
		(counts, samples)
	}
}

/// The allocations `fut` makes while it runs.
async fn count<F: Future>(sample_every: u64, fut: F) -> (F::Output, Counts, Vec<Backtrace>) {
	let region = Region::start(sample_every);
	let out = fut.await;
	let (counts, samples) = region.finish();
	(out, counts, samples)
}

// --------------------------------------------------
// Site attribution
// --------------------------------------------------

/// The site a backtrace names: the innermost two frames, in distinct
/// functions, whose source lies in this workspace (outside this harness), each
/// as `function (file:line)`. Inlined frames carry only a function's bare name,
/// so frames are placed by their file rather than their symbol path.
/// `ALLOC_DEPTH` sets how many frames name a site.
fn site(trace: &Backtrace) -> String {
	let depth: usize = std::env::var("ALLOC_DEPTH").ok().and_then(|s| s.parse().ok()).unwrap_or(2);
	let text = trace.to_string();
	let mut lines = text.lines().peekable();
	let mut frames: Vec<String> = Vec::new();
	while let Some(line) = lines.next() {
		let Some((_, symbol)) = line.trim().split_once(": ") else {
			continue;
		};
		let Some(at) = lines.peek().and_then(|next| next.trim().strip_prefix("at ")) else {
			continue;
		};
		// The standard library's sources sit under `/rustc/`, registry crates'
		// under `.cargo/`; everything else is this workspace's.
		let local = !at.starts_with("/rustc/") && !at.contains("/.cargo/");
		if !local || at.contains("tests/vm_alloc.rs") {
			continue;
		}
		let at = at.rsplit_once(':').map_or(at, |(head, _column)| head);
		let at = at.find("surrealdb/").map_or(at, |i| &at[i + "surrealdb/".len()..]);
		let frame = format!("{} ({at})", strip_generics(trim_symbol(symbol)));
		// Consecutive frames of one function in one file (inlined helpers,
		// closures) name the same site.
		let function = |f: &str| f.rsplit_once(':').map(|(head, _line)| head.to_owned());
		if frames.last().is_some_and(|last| function(last) == function(&frame)) {
			continue;
		}
		frames.push(frame);
		if frames.len() == depth {
			break;
		}
	}
	if frames.is_empty() {
		// A build whose workspace frames carry no line tables still names
		// their symbols: take the innermost workspace ones.
		let symbols: Vec<String> = text
			.lines()
			.filter_map(|line| line.trim().split_once(": ").map(|(_, symbol)| symbol))
			.filter(|symbol| symbol.contains("surrealdb") && !symbol.contains("vm_alloc"))
			.map(|symbol| strip_generics(trim_symbol(symbol)))
			.take(depth)
			.collect();
		if symbols.is_empty() {
			return "<outside the workspace>".to_owned();
		}
		return symbols.join("\n           <- ");
	}
	frames.join("\n           <- ")
}

/// A symbol without its trailing hash.
fn trim_symbol(symbol: &str) -> &str {
	match symbol.rsplit_once("::h") {
		Some((head, hash)) if hash.len() == 16 && hash.chars().all(|c| c.is_ascii_hexdigit()) => {
			head
		}
		_ => symbol,
	}
}

/// A symbol with its generic arguments elided.
fn strip_generics(symbol: &str) -> String {
	let mut out = String::new();
	let mut depth = 0usize;
	for c in symbol.chars() {
		match c {
			'<' => {
				if depth == 0 {
					out.push_str("<..>");
				}
				depth += 1;
			}
			'>' if depth > 0 => depth -= 1,
			_ if depth == 0 => out.push(c),
			_ => {}
		}
	}
	out
}

fn tally(samples: &[Backtrace]) -> HashMap<String, i64> {
	// `ALLOC_DUMP=text` prints the first sampled backtrace that mentions `text`.
	if let Ok(needle) = std::env::var("ALLOC_DUMP")
		&& let Some(trace) =
			samples.iter().map(ToString::to_string).find(|trace| trace.contains(&needle))
	{
		println!("{trace}");
	}
	let mut sites = HashMap::new();
	for trace in samples {
		*sites.entry(site(trace)).or_insert(0) += 1;
	}
	sites
}

// --------------------------------------------------
// Shapes
// --------------------------------------------------

const SMALL: usize = 10_000;
const LARGE: usize = 20_000;

/// The schema and data a datastore of `n` rows holds: a table `t` of `n`
/// records, two `knows` edges out of each, an empty indexed table `ix`, an
/// empty table `c`, and a function `fn::tag`.
fn setup(n: usize) -> String {
	format!(
		"
		DEFINE TABLE t SCHEMALESS;
		FOR $i IN 0..{n} {{
			CREATE type::record('t', $i) SET a = $i, b = $i % 7, s = 'row', lo = $i % 10,
				hi = math::floor($i / 2), friend = type::record('t', ($i + 1) % {n});
		}};
		FOR $i IN 0..{n} {{
			RELATE (type::record('t', $i))->knows->(type::record('t', ($i + 1) % {n}));
			RELATE (type::record('t', $i))->knows->(type::record('t', ($i * 7 + 3) % {n}));
		}};
		DEFINE TABLE ix SCHEMALESS;
		DEFINE INDEX ix_k ON ix FIELDS k;
		DEFINE TABLE c SCHEMALESS;
		DEFINE FUNCTION fn::tag($s: string) {{ RETURN $s + '!'; }};
		"
	)
}

/// A bench shape: its name, its query for a datastore of `n` rows, and the
/// number of rows it processes there.
struct Shape {
	name: &'static str,
	query: fn(usize) -> String,
	rows: fn(usize) -> usize,
	/// Whether the query takes `$rows`, `rows(n)` objects to insert.
	takes_rows: bool,
}

fn all(n: usize) -> usize {
	n
}

fn tenth(n: usize) -> usize {
	n / 10
}

const SHAPES: &[Shape] = &[
	Shape {
		name: "scan",
		query: |_| "SELECT * FROM t".into(),
		rows: all,
		takes_rows: false,
	},
	Shape {
		name: "filter",
		query: |_| "SELECT * FROM t WHERE b = 3".into(),
		rows: all,
		takes_rows: false,
	},
	Shape {
		name: "project3",
		query: |_| "SELECT a, b, s FROM t".into(),
		rows: all,
		takes_rows: false,
	},
	Shape {
		name: "order",
		query: |_| "SELECT * FROM t ORDER BY a DESC".into(),
		rows: all,
		takes_rows: false,
	},
	Shape {
		name: "order_limit10",
		query: |_| "SELECT * FROM t ORDER BY a DESC LIMIT 10".into(),
		rows: all,
		takes_rows: false,
	},
	Shape {
		name: "order_limit5000",
		query: |_| "SELECT * FROM t ORDER BY a DESC LIMIT 5000".into(),
		rows: all,
		takes_rows: false,
	},
	Shape {
		name: "group_lo",
		query: |_| "SELECT lo, count() AS c FROM t GROUP BY lo".into(),
		rows: all,
		takes_rows: false,
	},
	Shape {
		name: "group_hi",
		query: |_| "SELECT hi, count() AS c FROM t GROUP BY hi".into(),
		rows: all,
		takes_rows: false,
	},
	Shape {
		name: "count_all",
		query: |_| "SELECT count() FROM t GROUP ALL".into(),
		rows: all,
		takes_rows: false,
	},
	Shape {
		name: "graph_1hop",
		query: |_| "SELECT ->knows->t AS f FROM t".into(),
		rows: all,
		takes_rows: false,
	},
	Shape {
		name: "graph_2hop",
		query: |_| "SELECT ->knows->t->knows->t AS f FROM t".into(),
		rows: all,
		takes_rows: false,
	},
	Shape {
		name: "fetch",
		query: |_| "SELECT * FROM t FETCH friend".into(),
		rows: all,
		takes_rows: false,
	},
	// Each call binds a string argument, which a call's frame accounts for.
	Shape {
		name: "fn_call",
		query: |_| "SELECT fn::tag(s) AS r FROM t".into(),
		rows: all,
		takes_rows: false,
	},
	Shape {
		name: "create_loop",
		query: |n| format!("FOR $i IN 0..{} {{ CREATE c SET n = $i }}", n / 10),
		rows: tenth,
		takes_rows: false,
	},
	Shape {
		name: "insert_btree",
		query: |_| "INSERT INTO ix $rows".into(),
		rows: tenth,
		takes_rows: true,
	},
	Shape {
		name: "update",
		query: |_| "UPDATE t SET a = a + 1".into(),
		rows: all,
		takes_rows: false,
	},
];

/// `$rows`: `n` objects for `INSERT INTO ix`.
fn insert_rows(n: usize) -> Variables {
	let rows: Vec<Value> = (0..n)
		.map(|i| {
			let mut object = Object::new();
			object.insert("k", Value::Number(Number::Int(i as i64)));
			object.insert("v", Value::String("row".to_owned()));
			Value::Object(object)
		})
		.collect();
	let mut vars = Variables::new();
	vars.insert("rows", Value::Array(Array::from(rows)));
	vars
}

async fn datastore(n: usize) -> (Arc<Datastore>, Session) {
	let ds = Datastore::builder()
		.without_maintenance_tasks()
		.build_with_path("memory")
		.await
		.expect("datastore");
	let session = Session::owner().with_ns("test").with_db("test");
	ds.execute("DEFINE NS test; USE NS test; DEFINE DB test;", &Session::owner(), None)
		.await
		.expect("namespace and database defined");
	for response in ds.execute(&setup(n), &session, None).await.expect("setup runs") {
		response.result.expect("setup succeeds");
	}
	(ds, session)
}

/// Run `sql` once to warm up, then once counted.
async fn measure(
	ds: &Datastore,
	session: &Session,
	sql: &str,
	vars: Option<Variables>,
	sample_every: u64,
) -> (Counts, Vec<Backtrace>) {
	let run = || async {
		let responses = ds.execute(sql, session, vars.clone()).await.expect("query runs");
		for response in &responses {
			if let Err(e) = &response.result {
				panic!("`{sql}` failed: {e}");
			}
		}
		responses
	};
	drop(run().await);
	let (responses, counts, samples) = count(sample_every, run()).await;
	drop(responses);
	// The counted run clones `vars`; that clone is the harness's, not the query's.
	let (_, clone_counts, _) = count(0, async { drop(vars.clone()) }).await;
	let counts = Counts {
		allocs: counts.allocs.saturating_sub(clone_counts.allocs),
		bytes: counts.bytes.saturating_sub(clone_counts.bytes),
	};
	(counts, samples)
}

fn selected() -> Vec<&'static Shape> {
	let only = std::env::var("ALLOC_SHAPES").ok();
	let only: Option<Vec<&str>> = only.as_deref().map(|s| s.split(',').collect());
	SHAPES.iter().filter(|s| only.as_ref().is_none_or(|o| o.contains(&s.name))).collect()
}

#[tokio::test]
#[ignore = "a measurement, not a check"]
async fn per_row_allocations() {
	let sample_every: u64 =
		std::env::var("ALLOC_SAMPLE").ok().and_then(|s| s.parse().ok()).unwrap_or(0);
	let shapes = selected();
	let (small, session) = datastore(SMALL).await;
	let (large, _) = datastore(LARGE).await;
	println!("{:<14} {:>12} {:>12} {:>14}", "shape", "allocs/row", "bytes/row", "fixed allocs");
	let mut attributions = Vec::new();
	for shape in shapes {
		let (rows_small, rows_large) = ((shape.rows)(SMALL), (shape.rows)(LARGE));
		let vars = |rows| shape.takes_rows.then(|| insert_rows(rows));
		let (a, sa) =
			measure(&small, &session, &(shape.query)(SMALL), vars(rows_small), sample_every).await;
		let (b, sb) =
			measure(&large, &session, &(shape.query)(LARGE), vars(rows_large), sample_every).await;
		let rows = (rows_large - rows_small) as f64;
		let per_row = (b.allocs as f64 - a.allocs as f64) / rows;
		let bytes_per_row = (b.bytes as f64 - a.bytes as f64) / rows;
		let fixed = a.allocs as f64 - per_row * rows_small as f64;
		println!("{:<14} {per_row:>12.2} {bytes_per_row:>12.1} {fixed:>14.0}", shape.name);
		if sample_every != 0 {
			attributions.push((shape.name, per_row, rows, tally(&sa), tally(&sb)));
		}
	}
	for (name, per_row, rows, small_sites, large_sites) in attributions {
		println!("\n== {name}: {per_row:.2} allocs/row; sites by estimated allocs/row");
		let mut sites: Vec<(String, f64)> = large_sites
			.iter()
			.map(|(site, &n)| {
				let m = small_sites.get(site).copied().unwrap_or(0);
				(site.clone(), (n - m) as f64 * sample_every as f64 / rows)
			})
			.filter(|(_, per)| *per > 0.05)
			.collect();
		sites.sort_by(|a, b| b.1.total_cmp(&a.1));
		let top: usize = std::env::var("ALLOC_TOP").ok().and_then(|s| s.parse().ok()).unwrap_or(25);
		for (site, per) in sites.iter().take(top) {
			println!("{per:>7.2}  {site}");
		}
	}
}

/// Allocations per row of transcoding table `t`'s stored records straight
/// into an output frame (`exec::row_frame_probe`), against `SELECT * FROM t`
/// through the executor and the public value conversion.
#[tokio::test]
#[ignore = "a measurement, not a check"]
async fn per_row_frame_allocations() {
	let sample_every: u64 =
		std::env::var("ALLOC_SAMPLE").ok().and_then(|s| s.parse().ok()).unwrap_or(0);
	let (small, _) = datastore(SMALL).await;
	let (large, _) = datastore(LARGE).await;
	let probe = |ds: Arc<Datastore>| async move {
		let run = || transcode_table(&ds, "test", "test", "t");
		run().await.expect("transcodes");
		let ((rows, bytes), counts, samples) =
			count(sample_every, async { run().await.expect("transcodes") }).await;
		(rows, bytes, counts, samples)
	};
	let (rows_a, _, a, sa) = probe(small).await;
	let (rows_b, bytes_b, b, sb) = probe(large).await;
	let rows = (rows_b - rows_a) as f64;
	let per_row = (b.allocs as f64 - a.allocs as f64) / rows;
	println!(
		"scan->frame    allocs/row {per_row:.2}  alloc bytes/row {:.1}  frame bytes/row {:.1}",
		(b.bytes as f64 - a.bytes as f64) / rows,
		bytes_b as f64 / rows_b as f64,
	);
	if sample_every != 0 {
		let (small_sites, large_sites) = (tally(&sa), tally(&sb));
		let mut sites: Vec<(String, f64)> = large_sites
			.iter()
			.map(|(site, &n)| {
				let m = small_sites.get(site).copied().unwrap_or(0);
				(site.clone(), (n - m) as f64 * sample_every as f64 / rows)
			})
			.filter(|(_, per)| *per > 0.05)
			.collect();
		sites.sort_by(|a, b| b.1.total_cmp(&a.1));
		for (site, per) in sites.iter().take(15) {
			println!("{per:>7.2}  {site}");
		}
	}
}

/// Allocations per row of the read shapes as a CBOR or JSON transport runs
/// them: on the streaming executor with rows encoded for the wire
/// (`QueryRequest::with_row_format`), the items drained as they arrive. The
/// response envelope around the rows costs the same at both sizes.
#[tokio::test]
#[ignore = "a measurement, not a check"]
async fn per_row_wire_allocations() {
	use surrealdb_core::kvs::QueryRequest;
	use surrealdb_rpc::{QUERY_STREAM_BUFFER, RowFormat};

	let sample_every: u64 =
		std::env::var("ALLOC_SAMPLE").ok().and_then(|s| s.parse().ok()).unwrap_or(0);
	let (small, session) = datastore(SMALL).await;
	let (large, _) = datastore(LARGE).await;
	let run = |ds: Arc<Datastore>, sql: String, format: RowFormat| {
		let session = session.clone();
		async move {
			let (items, received) = async_channel::bounded(QUERY_STREAM_BUFFER);
			let job = ds
				.run_streaming(
					QueryRequest::new(sql.as_str(), &session).with_row_format(Some(format)),
					items,
				)
				.expect("the query starts");
			let drain = async {
				let mut n = 0usize;
				while let Ok(item) = received.recv().await {
					n += 1;
					drop(item);
				}
				n
			};
			let (ran, _) = futures::join!(job.run, drain);
			ran.expect("the query runs");
		}
	};
	println!("{:<14} {:>6} {:>12} {:>12}", "shape", "format", "allocs/row", "bytes/row");
	for shape in selected()
		.into_iter()
		.filter(|s| !s.takes_rows && !matches!(s.name, "update" | "create_loop" | "fn_call"))
	{
		for format in [RowFormat::Cbor, RowFormat::Json] {
			let measure = |ds: Arc<Datastore>, n: usize| {
				let sql = (shape.query)(n);
				async move {
					run(Arc::clone(&ds), sql.clone(), format).await;
					let ((), counts, samples) = count(sample_every, run(ds, sql, format)).await;
					(counts, samples)
				}
			};
			let (a, sa) = measure(Arc::clone(&small), SMALL).await;
			let (b, sb) = measure(Arc::clone(&large), LARGE).await;
			let rows = ((shape.rows)(LARGE) - (shape.rows)(SMALL)) as f64;
			let per_row = (b.allocs as f64 - a.allocs as f64) / rows;
			let bytes = (b.bytes as f64 - a.bytes as f64) / rows;
			println!(
				"{:<14} {:>6} {per_row:>12.2} {bytes:>12.1}",
				shape.name,
				format!("{format:?}")
			);
			if sample_every != 0 {
				let (small_sites, large_sites) = (tally(&sa), tally(&sb));
				let mut sites: Vec<(String, f64)> = large_sites
					.iter()
					.map(|(site, &n)| {
						let m = small_sites.get(site).copied().unwrap_or(0);
						(site.clone(), (n - m) as f64 * sample_every as f64 / rows)
					})
					.filter(|(_, per)| *per > 0.05)
					.collect();
				sites.sort_by(|a, b| b.1.total_cmp(&a.1));
				for (site, per) in sites.iter().take(10) {
					println!("{per:>7.2}  {site}");
				}
			}
		}
	}
}

// --------------------------------------------------
// The response path
// --------------------------------------------------

/// How a response is produced, from the query to the bytes a transport sends.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Path {
	/// The `query` RPC and `/sql` over public values: buffered execution,
	/// every row converted to the public value, the response encoded whole.
	Values,
	/// The `query` RPC and `/sql` over frames: streaming execution with rows
	/// encoded for the wire, the response written around them.
	Frames,
	/// `query_stream` over public values: rows framed as values, each frame
	/// encoded whole.
	StreamValues,
	/// `query_stream` over frames: encoded rows framed and written into each
	/// frame's message as they are.
	StreamFrames,
}

/// Produce the response to `sql` along `path` in `format`, as the server's
/// response encoders write it (`rpc::format::encoded::encode_response`, which
/// encodes a response of values exactly as `cbor::encode(res.into_value())`
/// does). Returns the bytes sent.
async fn respond(
	ds: &Arc<Datastore>,
	session: &Session,
	sql: &str,
	path: Path,
	format: surrealdb_rpc::RowFormat,
) -> usize {
	use surrealdb_core::kvs::QueryRequest;
	use surrealdb_core::rpc::format::encoded::{
		encode_response, encode_stream_rows, execute_encoded,
	};
	use surrealdb_rpc::{DbResponse, DbResult, OutgoingFrame, QUERY_STREAM_BUFFER, StreamFrames};

	let id = Some(Value::Number(Number::Int(1)));
	match path {
		Path::Values => {
			let results = ds.execute(sql, session, None).await.expect("the query runs");
			let response = DbResponse::success(id, None, DbResult::Query(results));
			encode_response(format, response).expect("encodes").len()
		}
		Path::Frames => {
			let results =
				execute_encoded(ds, sql, session, None, format).await.expect("the query runs");
			let response = DbResponse::success(id, None, DbResult::Encoded(results));
			encode_response(format, response).expect("encodes").len()
		}
		Path::StreamValues | Path::StreamFrames => {
			let encoded = path == Path::StreamFrames;
			let (items, received) = async_channel::bounded(QUERY_STREAM_BUFFER);
			let job = ds
				.run_streaming(
					QueryRequest::new(sql, session).with_row_format(encoded.then_some(format)),
					items,
				)
				.expect("the query starts");
			let mut frames = if encoded {
				StreamFrames::keeping_encoded_rows()
			} else {
				StreamFrames::new()
			};
			// Frames are encoded as items arrive, as the WebSocket driver
			// sends them.
			let drain = async {
				let mut sent = 0;
				while let Ok(item) = received.recv().await {
					frames.absorb(item);
					while let Some(frame) = frames.pop_outgoing() {
						sent += match frame {
							OutgoingFrame::Frame(frame) => encode_response(
								format,
								DbResponse::success(
									id.clone(),
									None,
									DbResult::Other(frame.into_value()),
								),
							),
							OutgoingFrame::Rows {
								index,
								rows,
							} => encode_stream_rows(format, id.clone(), None, index, &rows),
						}
						.expect("encodes")
						.len();
					}
				}
				sent
			};
			let (ran, sent) = futures::join!(job.run, drain);
			ran.expect("the query runs");
			sent
		}
	}
}

/// The read shapes the response path serves.
fn read_shapes() -> impl Iterator<Item = &'static Shape> {
	selected()
		.into_iter()
		.filter(|s| !s.takes_rows && !matches!(s.name, "update" | "create_loop" | "fn_call"))
}

/// Allocations per row of `shape` produced along `path`, between datastores
/// of `small_n` and `large_n` rows.
async fn response_allocations(
	(small, small_n): (&Arc<Datastore>, usize),
	(large, large_n): (&Arc<Datastore>, usize),
	session: &Session,
	shape: &Shape,
	path: Path,
	format: surrealdb_rpc::RowFormat,
) -> (f64, f64) {
	let counted = |ds: Arc<Datastore>, n: usize| {
		let sql = (shape.query)(n);
		async move {
			respond(&ds, session, &sql, path, format).await;
			let (_, counts, _) = count(0, respond(&ds, session, &sql, path, format)).await;
			counts
		}
	};
	let a = counted(Arc::clone(small), small_n).await;
	let b = counted(Arc::clone(large), large_n).await;
	let rows = ((shape.rows)(large_n) - (shape.rows)(small_n)) as f64;
	((b.allocs as f64 - a.allocs as f64) / rows, (b.bytes as f64 - a.bytes as f64) / rows)
}

/// Allocations per row of the read shapes from the query to the encoded
/// response bytes, along each [`Path`], in CBOR and JSON.
///
/// ```text
/// cargo test --locked --release -p surrealdb-core --test vm_alloc \
///     per_row_response_allocations -- --ignored --nocapture --test-threads 1
/// ```
#[tokio::test]
#[ignore = "a measurement, not a check"]
async fn per_row_response_allocations() {
	use surrealdb_rpc::RowFormat;
	let (small, session) = datastore(SMALL).await;
	let (large, _) = datastore(LARGE).await;
	println!(
		"{:<16} {:>6} {:>10} {:>10} {:>12} {:>12}",
		"shape", "format", "values", "frames", "stream vals", "stream frms"
	);
	for shape in read_shapes() {
		for format in [RowFormat::Cbor, RowFormat::Json] {
			let mut per_row = Vec::new();
			for path in [Path::Values, Path::Frames, Path::StreamValues, Path::StreamFrames] {
				let (allocs, _) = response_allocations(
					(&small, SMALL),
					(&large, LARGE),
					&session,
					shape,
					path,
					format,
				)
				.await;
				per_row.push(allocs);
			}
			println!(
				"{:<16} {:>6} {:>10.2} {:>10.2} {:>12.2} {:>12.2}",
				shape.name,
				format!("{format:?}"),
				per_row[0],
				per_row[1],
				per_row[2],
				per_row[3]
			);
		}
	}
}

/// The most allocations per row each read shape may make on the frame paths,
/// from the query to the response bytes. A shape not listed may make none
/// beyond rounding. Each budget is the sum of the sites that remain, and why.
const RESPONSE_BUDGETS: &[(&str, f64, &str)] = &[
	// The sort holds each row's key vector beside its stored bytes.
	("order", 1.5, "sort key vector per held row"),
	// A top-k keeps each held row as a value with its key: at the check's
	// sizes every row is held.
	("order_limit5000", 2.5, "top-k heap entry (row value and key) per held row"),
	// The aggregate builds each group's output object as a value.
	("group_hi", 1.0, "group output object built as a value, then encoded"),
	// Graph and FETCH rows are built as values and encoded by `push_value`:
	// the KV cursor setup and range-key encoding per lookup remain.
	("graph_1hop", 60.0, "kv-mem cursor and range key per lookup; rows built as values"),
	("graph_2hop", 120.0, "kv-mem cursor and range key per lookup; rows built as values"),
	("fetch", 40.0, "the fetched record decoded whole; rows built as values"),
];

/// Rows on the frame paths of the response cost no more allocations than
/// their shape's budget. Smaller datastores than the measurement's, so the
/// check is quick; the per-row difference is unchanged.
#[tokio::test]
async fn response_allocations_stay_within_budget() {
	use surrealdb_rpc::RowFormat;
	let _measuring = MEASURING.lock().await;
	const CHECK_SMALL: usize = 2_000;
	const CHECK_LARGE: usize = 4_000;
	let (small, session) = datastore(CHECK_SMALL).await;
	let (large, _) = datastore(CHECK_LARGE).await;
	let mut over = Vec::new();
	for shape in read_shapes() {
		let budget = RESPONSE_BUDGETS
			.iter()
			.find(|(name, _, _)| *name == shape.name)
			.map_or(0.1, |(_, budget, _)| *budget);
		for format in [RowFormat::Cbor, RowFormat::Json] {
			for path in [Path::Frames, Path::StreamFrames] {
				let (allocs, _) = response_allocations(
					(&small, CHECK_SMALL),
					(&large, CHECK_LARGE),
					&session,
					shape,
					path,
					format,
				)
				.await;
				if allocs > budget {
					over.push(format!(
						"{} {format:?} {path:?}: {allocs:.2} allocs/row > {budget}",
						shape.name
					));
				}
			}
		}
	}
	assert!(over.is_empty(), "over budget:\n{}", over.join("\n"));
}

// --------------------------------------------------
// Memory ceilings
// --------------------------------------------------

/// Runs go to files, so the heap holds only what the breakers keep in memory
/// and the parts they read and write.
/// Rows whose payload makes the table several MiB, sorted and grouped by it.
const CEILING_ROWS: usize = 12_000;

async fn ceiling_datastore(bounded: bool) -> (Arc<Datastore>, Session) {
	use std::sync::Arc;

	use surrealdb_core::exec::memory::{FairGovernor, FileTempStore};
	let mut builder = Datastore::builder().without_maintenance_tasks();
	if bounded {
		builder = builder
			.with_memory_governor(Arc::new(FairGovernor::new(1 << 20)))
			.with_temp_store(Arc::new(
				FileTempStore::open(
					&std::env::temp_dir().join("surrealdb-vm-alloc"),
					64 << 10,
					None,
				)
				.expect("a temporary store"),
			))
			.with_config(
				surrealdb_cnf::ConfigMap::empty()
					.with_key_value("spill_part_size", "65536")
					.with_key_value("spill_merge_fan_in", "4")
					.with_key_value("query_memory_minimum", "262144"),
			);
	}
	let ds = builder.build_with_path("memory").await.expect("datastore");
	let session = Session::owner().with_ns("test").with_db("test");
	ds.execute("DEFINE NS test; USE NS test; DEFINE DB test;", &Session::owner(), None)
		.await
		.expect("namespace and database defined");
	// Written a transaction of a hundred rows at a time: a transaction's
	// writes count against its query's memory too.
	let define = ds.execute("DEFINE TABLE t", &session, None).await.expect("table defined");
	define.into_iter().for_each(|response| drop(response.result.expect("table defined")));
	for start in (0..CEILING_ROWS).step_by(100) {
		let setup = format!(
			"FOR $i IN {start}..{} {{ CREATE type::record('t', $i) SET n = $i, \
			 pad = string::concat(<string> ($i % 1999), string::repeat('x', 2048)) }};",
			start + 100
		);
		for response in ds.execute(&setup, &session, None).await.expect("setup runs") {
			response.result.expect("setup succeeds");
		}
	}
	(ds, session)
}

/// The answer to `sql` and the most bytes live at once while it ran.
async fn peak_of(ds: &Datastore, session: &Session, sql: &str) -> (Vec<Value>, i64) {
	let base = reset_peak();
	let mut answers = Vec::new();
	for response in ds.execute(sql, session, None).await.expect("query runs") {
		answers.push(response.result.expect("statement succeeds"));
	}
	(answers, peak_since(base))
}

/// A sort and an aggregate over a table several times the size of a 1 MiB
/// budget answer what they answer unbounded, while the heap they hold at
/// once stays a fraction of what they hold unbounded. The statements answer
/// little, so what they hold is what their breakers hold.
#[tokio::test(flavor = "current_thread")]
async fn breakers_over_a_small_budget_hold_a_bounded_heap() {
	let _measuring = MEASURING.lock().await;
	let statements = [
		"SELECT VALUE n FROM t ORDER BY pad, n",
		"SELECT n % 100 AS b, array::len(array::group(pad)) AS c FROM t GROUP BY b",
	];
	let (free, session) = ceiling_datastore(false).await;
	let (bounded, _) = ceiling_datastore(true).await;
	for sql in statements {
		let (expected, unbounded_peak) = peak_of(&free, &session, sql).await;
		let (got, bounded_peak) = peak_of(&bounded, &session, sql).await;
		assert_eq!(got, expected, "`{sql}` under a small budget");
		eprintln!(
			"{sql}: unbounded peak {unbounded_peak} bytes, bounded peak {bounded_peak} bytes"
		);
		assert!(
			bounded_peak * 2 < unbounded_peak,
			"`{sql}` held {bounded_peak} bytes under a 1 MiB budget, {unbounded_peak} unbounded"
		);
	}
}
