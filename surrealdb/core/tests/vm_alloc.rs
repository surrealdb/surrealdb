#![recursion_limit = "256"]
#![deny(clippy::wildcard_enum_match_arm)]
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
/// The formats a transport writes rows in that this build serves: CBOR, JSON, and shaped frames
/// where the `shaped` feature builds them.
fn wire_formats() -> Vec<surrealdb_rpc::RowFormat> {
	let mut formats = vec![surrealdb_rpc::RowFormat::Cbor, surrealdb_rpc::RowFormat::Json];
	if cfg!(feature = "shaped") {
		formats.push(surrealdb_rpc::RowFormat::Shaped);
	}
	formats
}

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
/// records, two `knows` edges out of each, a `SCHEMAFULL` table `p` of `n`
/// packed records with the values of `t`'s, tables of `n` records whose ids are
/// strings (`ps`, `ts`), strings that hold a byte the key encoding escapes (`pe`, `te`), arrays of
/// integers and strings (`pa`, `ta`), arrays of integers, floats, decimals, strings and uuids
/// (`pn`, `tn`), uuids (`pu`, `tu`) and objects of such values (`po`, `to`), packed and as
/// documents, an empty indexed table `ix`, an empty table `c`, and a function `fn::tag`.
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
		DEFINE TABLE p SCHEMAFULL;
		DEFINE FIELD a ON p TYPE int;
		DEFINE FIELD b ON p TYPE int;
		DEFINE FIELD s ON p TYPE string;
		DEFINE FIELD lo ON p TYPE int;
		FOR $i IN 0..{n} {{
			CREATE type::record('p', $i) SET a = $i, b = $i % 7, s = 'row', lo = $i % 10;
		}};
		DEFINE TABLE ps SCHEMAFULL;
		DEFINE FIELD b ON ps TYPE int;
		DEFINE TABLE ts SCHEMALESS;
		FOR $i IN 0..{n} {{
			CREATE type::record('ps', 'a_string_key_longer_than_23_bytes_' + <string> $i) SET b = $i % 7;
			CREATE type::record('ts', 'a_string_key_longer_than_23_bytes_' + <string> $i) SET b = $i % 7;
		}};
		DEFINE TABLE pa SCHEMAFULL;
		DEFINE FIELD b ON pa TYPE int;
		DEFINE TABLE ta SCHEMALESS;
		FOR $i IN 0..{n} {{
			CREATE type::record('pa', [$i, 'an_element_longer_than_23_bytes']) SET b = $i % 7;
			CREATE type::record('ta', [$i, 'an_element_longer_than_23_bytes']) SET b = $i % 7;
		}};
		DEFINE TABLE pe SCHEMAFULL;
		DEFINE FIELD b ON pe TYPE int;
		DEFINE TABLE te SCHEMALESS;
		FOR $i IN 0..{n} {{
			CREATE type::record('pe', 'a_key_with_a_nul_byte_\\u{{0}}_longer_than_23_bytes_' + <string> $i) SET b = $i % 7;
			CREATE type::record('te', 'a_key_with_a_nul_byte_\\u{{0}}_longer_than_23_bytes_' + <string> $i) SET b = $i % 7;
		}};
		DEFINE TABLE pn SCHEMAFULL;
		DEFINE FIELD b ON pn TYPE int;
		DEFINE TABLE tn SCHEMALESS;
		FOR $i IN 0..{n} {{
			CREATE type::record('pn', [$i, 1.5f + $i, 2.5dec + $i, 'an_element_longer_than_23_bytes', rand::uuid::v4()]) SET b = $i % 7;
			CREATE type::record('tn', [$i, 1.5f + $i, 2.5dec + $i, 'an_element_longer_than_23_bytes', rand::uuid::v4()]) SET b = $i % 7;
		}};
		DEFINE TABLE pu SCHEMAFULL;
		DEFINE FIELD b ON pu TYPE int;
		DEFINE TABLE tu SCHEMALESS;
		FOR $i IN 0..{n} {{
			CREATE type::record('pu', rand::uuid::v4()) SET b = $i % 7;
			CREATE type::record('tu', rand::uuid::v4()) SET b = $i % 7;
		}};
		DEFINE TABLE po SCHEMAFULL;
		DEFINE FIELD b ON po TYPE int;
		DEFINE TABLE to SCHEMALESS;
		FOR $i IN 0..{n} {{
			CREATE type::record('po', {{ n: $i, f: 1.5f + $i, d: 2.5dec + $i, s: 'an_entry_longer_than_23_bytes', u: rand::uuid::v4() }}) SET b = $i % 7;
			CREATE type::record('to', {{ n: $i, f: 1.5f + $i, d: 2.5dec + $i, s: 'an_entry_longer_than_23_bytes', u: rand::uuid::v4() }}) SET b = $i % 7;
		}};
		DEFINE TABLE pw SCHEMAFULL;
		DEFINE FIELD score ON pw TYPE int; DEFINE FIELD cat ON pw TYPE int;
		DEFINE FIELD name ON pw TYPE string; DEFINE FIELD flag ON pw TYPE bool;
		DEFINE FIELD body ON pw TYPE string; DEFINE FIELD at ON pw TYPE datetime;
		DEFINE FIELD ratio ON pw TYPE float; DEFINE FIELD note ON pw TYPE option<string>;
		DEFINE FIELD tags ON pw TYPE array<string>; DEFINE FIELD link ON pw TYPE option<record<pw>>;
		FOR $i IN 0..{n} {{
			CREATE type::record('pw', $i) SET score = $i * 7, cat = $i % 13, name = 'name ' + <string> $i,
				flag = $i % 2 = 0, body = string::repeat('b', 40), at = d'2024-01-01T00:00:00Z' + duration::from_secs($i),
				ratio = $i / 3f, note = IF $i % 2 = 0 THEN 'even' ELSE NONE END, tags = ['x', 'y'],
				link = type::record('pw', ($i + 1) % {n});
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
	Shape {
		name: "fetch_page",
		query: |n| format!("SELECT * FROM t LIMIT {} FETCH friend", n / 10),
		rows: tenth,
		takes_rows: false,
	},
	Shape {
		name: "deref",
		query: |_| "SELECT friend.a FROM t".into(),
		rows: all,
		takes_rows: false,
	},
	Shape {
		name: "deref_page",
		query: |n| format!("SELECT friend.a FROM t LIMIT {}", n / 10),
		rows: tenth,
		takes_rows: false,
	},
	// A filter between the scan and the projection leaves the dereferences no window to read
	// through: the shape `deref` is measured against.
	Shape {
		name: "deref_unwindowed",
		query: |_| "SELECT friend.a FROM t WHERE a >= 0".into(),
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

/// A shape over the `SCHEMAFULL` table `p` and its twin over the `SCHEMALESS` table `t`, which
/// holds the same values as documents.
struct Pair {
	packed: Shape,
	twin: Shape,
}

/// The shapes of a packed table, kept apart from [`SHAPES`]: the response budget set holds none of
/// them.
///
/// A scan reads a packed row where it lies when the body that runs on it holds only filters over
/// fields and emits the row or a field of it (`packed_filter`, `packed_in_place_reject`,
/// the `*_keys` shapes, `packed_in_place_value`). The aggregates and the sort
/// (`packed_filter_reject`, `packed_group_lo`, the `packed_*order*` shapes) run on the row too,
/// after the top-k probe has judged it from its columns where it can. `packed_order_limit10` reads
/// its rows best last, so every row enters the sort and none is refused ([`DECODED_INTO_SORT`]).
/// Which shapes read in place is proved from their compiled programs by
/// `the_allocation_shapes_compile_to_the_path_they_measure` in `exec::vm::tests_packed_reads`.
const PACKED_SHAPES: &[Pair] = &[
	Pair {
		packed: Shape {
			name: "packed_filter_reject",
			query: |_| "SELECT count() FROM p WHERE b = 99 GROUP ALL".into(),
			rows: all,
			takes_rows: false,
		},
		twin: Shape {
			name: "filter_reject",
			query: |_| "SELECT count() FROM t WHERE b = 99 GROUP ALL".into(),
			rows: all,
			takes_rows: false,
		},
	},
	Pair {
		packed: Shape {
			name: "packed_filter",
			query: |_| "SELECT * FROM p WHERE b = 3".into(),
			rows: all,
			takes_rows: false,
		},
		twin: Shape {
			name: "filter",
			query: |_| "SELECT * FROM t WHERE b = 3".into(),
			rows: all,
			takes_rows: false,
		},
	},
	Pair {
		packed: Shape {
			name: "packed_group_lo",
			query: |_| "SELECT lo, count() AS c FROM p GROUP BY lo".into(),
			rows: all,
			takes_rows: false,
		},
		twin: Shape {
			name: "group_lo",
			query: |_| "SELECT lo, count() AS c FROM t GROUP BY lo".into(),
			rows: all,
			takes_rows: false,
		},
	},
	Pair {
		packed: Shape {
			name: "packed_order_limit10",
			query: |_| "SELECT * FROM p ORDER BY a DESC LIMIT 10".into(),
			rows: all,
			takes_rows: false,
		},
		twin: Shape {
			name: "order_limit10",
			query: |_| "SELECT * FROM t ORDER BY a DESC LIMIT 10".into(),
			rows: all,
			takes_rows: false,
		},
	},
	// The rows arrive best first, so once the sort holds its ten the probe refuses every row it
	// reads: the first from the columns of the packed row, the second above a filter the
	// prefilter states whole.
	Pair {
		packed: Shape {
			name: "packed_order_asc_limit10",
			query: |_| "SELECT * FROM p ORDER BY a ASC LIMIT 10".into(),
			rows: all,
			takes_rows: false,
		},
		twin: Shape {
			name: "order_asc_limit10",
			query: |_| "SELECT * FROM t ORDER BY a ASC LIMIT 10".into(),
			rows: all,
			takes_rows: false,
		},
	},
	Pair {
		packed: Shape {
			name: "packed_filter_order_asc_limit10",
			query: |_| "SELECT * FROM p WHERE b < 7 ORDER BY a ASC LIMIT 10".into(),
			rows: all,
			takes_rows: false,
		},
		twin: Shape {
			name: "filter_order_asc_limit10",
			query: |_| "SELECT * FROM t WHERE b < 7 ORDER BY a ASC LIMIT 10".into(),
			rows: all,
			takes_rows: false,
		},
	},
	Pair {
		packed: Shape {
			name: "packed_in_place_reject",
			query: |_| "SELECT * FROM p WHERE b = 99".into(),
			rows: all,
			takes_rows: false,
		},
		twin: Shape {
			name: "in_place_reject",
			query: |_| "SELECT * FROM t WHERE b = 99".into(),
			rows: all,
			takes_rows: false,
		},
	},
	// Keys of every shape whose validation on open builds nothing (`ZERO_ON_OPEN`): strings longer
	// than a strand holds inline, with and without a byte the key encoding escapes, and arrays and
	// objects of numbers, strings and uuids, and uuids. A packed row's key is validated whole when
	// the row is opened and its id is built only when it is read, so no id is built for a row the
	// body rejects.
	Pair {
		packed: Shape {
			name: "packed_string_keys",
			query: |_| "SELECT * FROM ps WHERE b = 99".into(),
			rows: all,
			takes_rows: false,
		},
		twin: Shape {
			name: "string_keys",
			query: |_| "SELECT * FROM ts WHERE b = 99".into(),
			rows: all,
			takes_rows: false,
		},
	},
	Pair {
		packed: Shape {
			name: "packed_composite_keys",
			query: |_| "SELECT * FROM pa WHERE b = 99".into(),
			rows: all,
			takes_rows: false,
		},
		twin: Shape {
			name: "composite_keys",
			query: |_| "SELECT * FROM ta WHERE b = 99".into(),
			rows: all,
			takes_rows: false,
		},
	},
	Pair {
		packed: Shape {
			name: "packed_escaped_string_keys",
			query: |_| "SELECT * FROM pe WHERE b = 99".into(),
			rows: all,
			takes_rows: false,
		},
		twin: Shape {
			name: "escaped_string_keys",
			query: |_| "SELECT * FROM te WHERE b = 99".into(),
			rows: all,
			takes_rows: false,
		},
	},
	Pair {
		packed: Shape {
			name: "packed_number_keys",
			query: |_| "SELECT * FROM pn WHERE b = 99".into(),
			rows: all,
			takes_rows: false,
		},
		twin: Shape {
			name: "number_keys",
			query: |_| "SELECT * FROM tn WHERE b = 99".into(),
			rows: all,
			takes_rows: false,
		},
	},
	Pair {
		packed: Shape {
			name: "packed_uuid_keys",
			query: |_| "SELECT * FROM pu WHERE b = 99".into(),
			rows: all,
			takes_rows: false,
		},
		twin: Shape {
			name: "uuid_keys",
			query: |_| "SELECT * FROM tu WHERE b = 99".into(),
			rows: all,
			takes_rows: false,
		},
	},
	Pair {
		packed: Shape {
			name: "packed_object_keys",
			query: |_| "SELECT * FROM po WHERE b = 99".into(),
			rows: all,
			takes_rows: false,
		},
		twin: Shape {
			name: "object_keys",
			query: |_| "SELECT * FROM to WHERE b = 99".into(),
			rows: all,
			takes_rows: false,
		},
	},
	Pair {
		packed: Shape {
			name: "packed_in_place_value",
			query: |_| "SELECT VALUE s FROM p WHERE b = 3".into(),
			rows: all,
			takes_rows: false,
		},
		twin: Shape {
			name: "in_place_value",
			query: |_| "SELECT VALUE s FROM t WHERE b = 3".into(),
			rows: all,
			takes_rows: false,
		},
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

/// The shapes of [`PACKED_SHAPES`] and their twins that `ALLOC_SHAPES` selects, packed shapes
/// first of each pair; all of them when it is not set.
fn packed_selected() -> Vec<&'static Shape> {
	let only = std::env::var("ALLOC_SHAPES").ok();
	let wanted = |shape: &Shape| {
		only.as_deref().is_none_or(|only| only.split(',').any(|name| name == shape.name))
	};
	PACKED_SHAPES
		.iter()
		.filter(|pair| wanted(&pair.packed) || wanted(&pair.twin))
		.flat_map(|pair| [&pair.packed, &pair.twin])
		.collect()
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
	for shape in shapes.into_iter().chain(packed_selected()) {
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
	use surrealdb_types::shaped::ShapeScope;

	let id = Some(Value::Number(Number::Int(1)));
	match path {
		Path::Values => {
			let results = ds.execute(sql, session, None).await.expect("the query runs");
			let response = DbResponse::success(id, None, DbResult::Query(results));
			encode_response(format, response).expect("encodes").len()
		}
		Path::Frames => {
			let results = execute_encoded(ds, sql, session, None, format, None)
				.await
				.expect("the query runs");
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
			// sends them, shaped rows in the stream's one shape scope.
			let mut scope = ShapeScope::new();
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
								trace,
							} => encode_stream_rows(
								format,
								id.clone(),
								None,
								index,
								&rows,
								trace,
								&mut scope,
							),
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
	("fetch_page", 10.0, "the fetched record decoded whole; rows built as values"),
	// A dereferenced record is decoded whole to read one field of it.
	("deref", 26.0, "the dereferenced record decoded whole for one field"),
	("deref_page", 26.0, "the dereferenced record decoded whole for one field"),
	(
		"deref_unwindowed",
		30.0,
		"the dereferenced record decoded whole for one field, read for itself",
	),
];

/// Rows on the frame paths of the response cost no more allocations than
/// their shape's budget. Smaller datastores than the measurement's, so the
/// check is quick; the per-row difference is unchanged.
#[tokio::test]
async fn response_allocations_stay_within_budget() {
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
		for format in wire_formats() {
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

/// Allocations per row of `shape` between datastores of `small_n` and `large_n` rows.
async fn per_row(
	(small, small_n): (&Datastore, usize),
	(large, large_n): (&Datastore, usize),
	session: &Session,
	shape: &Shape,
) -> f64 {
	let (a, _) = measure(small, session, &(shape.query)(small_n), None, 0).await;
	let (b, _) = measure(large, session, &(shape.query)(large_n), None, 0).await;
	let rows = ((shape.rows)(large_n) - (shape.rows)(small_n)) as f64;
	(b.allocs as f64 - a.allocs as f64) / rows
}

/// The shapes of [`PACKED_SHAPES`] that refuse every row without decoding it, each from the
/// columns of the packed row: the filter of a count judges it before the decode
/// (`packed_filter_reject`), the body refuses it in place (`packed_in_place_reject` and the
/// shapes over ids of every kind), or the top-k probe refuses it (`packed_order_asc_limit10`, and
/// the same above a filter). Each allocates nothing for a row beyond [`BATCH_FLOOR`], whatever its
/// twin costs. The ids of the key shapes are those whose components the open of a packed row reads
/// without building anything: numbers of every kind, strings of any length, uuids, and arrays and
/// objects of those.
///
/// An id that holds a set, geometry, bytes, file, table name, regex, closure, range or record id is
/// not in the set: reading such a component builds it, so opening a row whose id holds one
/// allocates.
const ZERO_PER_ROW: &[&str] = &[
	"packed_filter_reject",
	"packed_order_asc_limit10",
	"packed_filter_order_asc_limit10",
	"packed_in_place_reject",
	"packed_string_keys",
	"packed_escaped_string_keys",
	"packed_composite_keys",
	"packed_number_keys",
	"packed_uuid_keys",
	"packed_object_keys",
];

/// The per-row allowance for allocations that do not depend on the id's shape, which every twin
/// here makes as well.
const BATCH_FLOOR: f64 = 0.02;

/// The shapes whose rows all enter a sort that keeps a bounded number of rows. A document enters
/// such a sort as its stored bytes and is decoded only if it keeps its place, while a packed row is
/// decoded as it enters, so a sort holds no packed bytes and reads no layouts: these shapes cost a
/// packed row its decode, [`DECODED_ROW`] at most, rather than what the twin's documents cost.
const DECODED_INTO_SORT: &[&str] = &["packed_order_limit10"];

/// The allocations a packed row's whole decode makes, per row, at most.
const DECODED_ROW: f64 = 2.1;

/// A packed row costs no more allocations than the same row as a document, in every shape of
/// [`PACKED_SHAPES`]: the read of a packed row in place, the judgement of the filter and the
/// probe from its columns, and the check that makes them safe (the whole row's decode run to build
/// nothing), allocate nothing beyond what the twin's read allocates, and a shape that decodes the
/// row decodes it as the twin's document is decoded. A shape of [`ZERO_PER_ROW`] allocates nothing
/// per row beyond [`BATCH_FLOOR`]. Every shape is printed beside its twin. That each shape takes
/// the path it measures is proved from its compiled program by
/// `the_allocation_shapes_compile_to_the_path_they_measure` in `exec::vm::tests_packed_reads`.
#[tokio::test]
async fn packed_rows_read_in_place_allocate_no_more_than_documents() {
	let _measuring = MEASURING.lock().await;
	const CHECK_SMALL: usize = 2_000;
	const CHECK_LARGE: usize = 4_000;
	let (small, session) = datastore(CHECK_SMALL).await;
	let (large, _) = datastore(CHECK_LARGE).await;
	let mut over = Vec::new();
	println!("{:<32} {:>12} {:>12}", "shape", "packed/row", "twin/row");
	for pair in PACKED_SHAPES {
		let sizes = ((&*small, CHECK_SMALL), (&*large, CHECK_LARGE));
		let packed = per_row(sizes.0, sizes.1, &session, &pair.packed).await;
		let twin = per_row(sizes.0, sizes.1, &session, &pair.twin).await;
		println!("{:<32} {packed:>12.3} {twin:>12.3}", pair.packed.name);
		let bound = if DECODED_INTO_SORT.contains(&pair.packed.name) {
			DECODED_ROW
		} else {
			twin + 0.01
		};
		if packed > bound {
			over.push(format!(
				"{}: {packed:.2} allocs/row > {bound:.2} (the twin's {twin:.2})",
				pair.packed.name
			));
		}
		if ZERO_PER_ROW.contains(&pair.packed.name) && packed > BATCH_FLOOR {
			over.push(format!("{}: {packed:.3} allocs/row > {BATCH_FLOOR}", pair.packed.name));
		}
	}
	assert!(
		over.is_empty(),
		"packed rows cost more allocations than the documents:\n{}",
		over.join("\n")
	);
}

/// The most allocations per row a packed `SELECT *` into frames may make: the frame buffers grow
/// by doubling, which is amortised to a few hundredths of an allocation per row.
const FRAME_PER_ROW: f64 = 0.02;

/// Packed rows written into frames allocate nothing per row beyond the growth of the frame's own
/// buffers: the transcode from stored bytes into a frame (`exec::row_frame_probe`), and a streamed
/// `SELECT *` with rows encoded for the wire, in CBOR, JSON and shaped, over packed tables of int,
/// string and uuid keys and of a wide row of every common field kind. An array key (`pa`) is
/// decoded to be written, as a document's is, and is printed beside them without a bound.
#[tokio::test]
async fn packed_rows_into_frames_allocate_nothing_per_row() {
	use surrealdb_core::kvs::QueryRequest;
	use surrealdb_rpc::{QUERY_STREAM_BUFFER, RowFormat};

	let _measuring = MEASURING.lock().await;
	let sample_every: u64 =
		std::env::var("ALLOC_SAMPLE").ok().and_then(|s| s.parse().ok()).unwrap_or(0);
	const CHECK_SMALL: usize = 2_000;
	const CHECK_LARGE: usize = 4_000;
	let (small, session) = datastore(CHECK_SMALL).await;
	let (large, _) = datastore(CHECK_LARGE).await;
	let rows = (CHECK_LARGE - CHECK_SMALL) as f64;
	let stream = |ds: Arc<Datastore>, sql: String, format: RowFormat| {
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
				while let Ok(item) = received.recv().await {
					drop(item);
				}
			};
			let (ran, ()) = futures::join!(job.run, drain);
			ran.expect("the query runs");
		}
	};
	let mut over = Vec::new();
	println!("{:<24} {:>12}", "shape", "allocs/row");
	for table in ["pw", "p", "ps", "pu", "pa"] {
		let bounded = table != "pa";
		let floor = |ds: Arc<Datastore>| async move {
			let run = || transcode_table(&ds, "test", "test", table);
			run().await.expect("transcodes");
			count(0, async { run().await.expect("transcodes") }).await.1
		};
		let (a, b) = (floor(Arc::clone(&small)).await, floor(Arc::clone(&large)).await);
		let per_row = (b.allocs as f64 - a.allocs as f64) / rows;
		println!("{:<24} {per_row:>12.3}", format!("transcode {table}"));
		if bounded && per_row > FRAME_PER_ROW {
			over.push(format!("transcode {table}: {per_row:.3} allocs/row"));
		}
		for format in wire_formats() {
			let sql = format!("SELECT * FROM {table}");
			let measure = |ds: Arc<Datastore>| {
				let sql = sql.clone();
				async move {
					stream(Arc::clone(&ds), sql.clone(), format).await;
					let ((), counts, samples) = count(sample_every, stream(ds, sql, format)).await;
					(counts, samples)
				}
			};
			let ((a, sa), (b, sb)) =
				(measure(Arc::clone(&small)).await, measure(Arc::clone(&large)).await);
			let per_row = (b.allocs as f64 - a.allocs as f64) / rows;
			let name = format!("{sql} {format:?}");
			println!("{name:<24} {per_row:>12.3}");
			if bounded && per_row > FRAME_PER_ROW {
				over.push(format!("{name}: {per_row:.3} allocs/row"));
			}
			if sample_every != 0 {
				let (small_sites, large_sites) = (tally(&sa), tally(&sb));
				for (site, &n) in &large_sites {
					let m = small_sites.get(site).copied().unwrap_or(0);
					let per = (n - m) as f64 * sample_every as f64 / rows;
					if per > 0.05 {
						println!("{per:>7.2}  {site}");
					}
				}
			}
		}
	}
	assert!(over.is_empty(), "packed rows allocate per row into frames:\n{}", over.join("\n"));
}

// --------------------------------------------------
// Generated-id creates
// --------------------------------------------------

/// The bulk creates of generated ids whose per-record cost is checked: a name,
/// the table's definition, the statement (over table `$t` and count `$n`), the
/// most allocations and the most bytes allocated per record, and what they are.
///
/// The byte budget holds a record's write step to its own work: a future the
/// step builds for every record is allocated whole however little of it a
/// record reaches, so a stage that awaits a large body inline, or a step that
/// builds one it does not run, shows here as kilobytes per record.
const CREATE_BUDGETS: &[(&str, &str, &str, f64, f64, &str)] = &[
	(
		"schemaless",
		"DEFINE TABLE $t SCHEMALESS;",
		"CREATE |$t:$n| RETURN NONE",
		18.0,
		16_384.0,
		"the generated row, its key and document encodings, the kv-mem write, the transaction cache entry, and the record's boxed write step",
	),
	(
		"schemaless_set",
		"DEFINE TABLE $t SCHEMALESS;",
		"CREATE |$t:$n| SET score = rand::int(0, 100) RETURN NONE",
		28.0,
		18_432.0,
		"as `schemaless`, with the data clause's evaluation, `$input`, and the assignment",
	),
	(
		"schemafull",
		"DEFINE TABLE $t SCHEMAFULL; DEFINE FIELD score ON $t TYPE option<int>; DEFINE FIELD name ON $t TYPE option<string>;",
		"CREATE |$t:$n| RETURN NONE",
		31.0,
		18_432.0,
		"as `schemaless`, with each field's frame and the places it walks",
	),
	(
		"schemafull_set",
		"DEFINE TABLE $t SCHEMAFULL; DEFINE FIELD score ON $t TYPE option<int>; DEFINE FIELD name ON $t TYPE option<string>;",
		"CREATE |$t:$n| SET score = rand::int(0, 100) RETURN NONE",
		40.0,
		20_480.0,
		"as `schemafull`, with the data clause's evaluation, `$input`, and the assignment",
	),
];

/// Each shape of [`CREATE_BUDGETS`] allocates no more per record than its
/// budget, counted as the difference between two creates of different sizes
/// into tables of their own, so the statement's planning and commit cancel.
#[tokio::test]
async fn generated_id_creates_stay_within_budget() {
	let _measuring = MEASURING.lock().await;
	const CHECK_SMALL: usize = 2_000;
	const CHECK_LARGE: usize = 4_000;
	let ds = Datastore::builder()
		.without_maintenance_tasks()
		.build_with_path("memory")
		.await
		.expect("datastore");
	let session = Session::owner().with_ns("test").with_db("test");
	ds.execute("DEFINE NS test; USE NS test; DEFINE DB test;", &Session::owner(), None)
		.await
		.expect("namespace and database defined");
	let run = async |sql: String| {
		for response in ds.execute(&sql, &session, None).await.expect("query runs") {
			if let Err(e) = &response.result {
				panic!("`{sql}` failed: {e}");
			}
		}
	};
	let mut over = Vec::new();
	for (name, define, create, allocs_budget, bytes_budget, _) in CREATE_BUDGETS {
		let mut counted = Vec::new();
		for n in [CHECK_SMALL, CHECK_LARGE] {
			let table = format!("{name}_{n}");
			run(define.replace("$t", &table)).await;
			let sql = create.replace("$t", &table).replace("$n", &n.to_string());
			let ((), counts, _) = count(0, run(sql)).await;
			counted.push(counts);
		}
		let rows = (CHECK_LARGE - CHECK_SMALL) as f64;
		let allocs = (counted[1].allocs as f64 - counted[0].allocs as f64) / rows;
		let bytes = (counted[1].bytes as f64 - counted[0].bytes as f64) / rows;
		println!("{name:<16} {allocs:>8.2} allocs/record {bytes:>10.0} bytes/record");
		if allocs > *allocs_budget {
			over.push(format!("{name}: {allocs:.2} allocs/record > {allocs_budget}"));
		}
		if bytes > *bytes_budget {
			over.push(format!("{name}: {bytes:.0} bytes/record > {bytes_budget}"));
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
	ceiling_datastore_over(bounded, "memory", &[]).await
}

/// [`ceiling_datastore`] over the store at `path`, with the configuration keys `extra` beside the
/// ones a bounded datastore holds.
async fn ceiling_datastore_over(
	bounded: bool,
	path: &str,
	extra: &[(&str, &str)],
) -> (Arc<Datastore>, Session) {
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
				extra.iter().fold(
					surrealdb_cnf::ConfigMap::empty()
						.with_key_value("spill_part_size", "65536")
						.with_key_value("spill_merge_fan_in", "4")
						.with_key_value("query_memory_minimum", "262144"),
					|config, &(key, value)| config.with_key_value(key, value),
				),
			);
	} else if !extra.is_empty() {
		builder = builder.with_config(
			extra.iter().fold(surrealdb_cnf::ConfigMap::empty(), |config, &(key, value)| {
				config.with_key_value(key, value)
			}),
		);
	}
	let ds = builder.build_with_path(path).await.expect("datastore");
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

/// The statements the memory-ceiling tests run. A sort over fields holds nothing the interpreter
/// hands the host, so its morsels read key ranges. A grouping by a computed key, and the
/// statements that call a function, hold a region or a step of the host, so their morsels are
/// batches of rows the coordinator read.
const BREAKERS: [&str; 4] = [
	"SELECT VALUE n FROM t ORDER BY pad, n",
	"SELECT n % 100 AS b, array::len(array::group(pad)) AS c FROM t GROUP BY b",
	"SELECT string::len(pad) AS l, n FROM t ORDER BY l, n",
	"SELECT math::abs(n % 100) AS b, array::len(array::group(pad)) AS c FROM t GROUP BY b",
];

/// Each of [`BREAKERS`] answers over `bounded` what it answers over `free`, while the heap it holds
/// at once stays under half of what it holds over `free`.
async fn assert_breakers_stay_bounded(free: &Datastore, bounded: &Datastore, session: &Session) {
	for sql in BREAKERS {
		let (expected, unbounded_peak) = peak_of(free, session, sql).await;
		let (got, bounded_peak) = peak_of(bounded, session, sql).await;
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

/// A sort and an aggregate over a table several times the size of a 1 MiB
/// budget answer what they answer unbounded, while the heap they hold at
/// once stays a fraction of what they hold unbounded. The statements answer
/// little, so what they hold is what their breakers hold. The in-memory store gives
/// workers no read view, so nothing here reaches a worker: the case below runs the same
/// statements where morsels do.
#[tokio::test(flavor = "current_thread")]
async fn breakers_over_a_small_budget_hold_a_bounded_heap() {
	let _measuring = MEASURING.lock().await;
	let (free, session) = ceiling_datastore(false).await;
	let (bounded, _) = ceiling_datastore(true).await;
	assert_breakers_stay_bounded(&free, &bounded, &session).await;
}

/// The rows a statement's `EXPLAIN ANALYZE` reports workers read in its `Morsels:` block, or
/// `None` where it renders none.
#[cfg(feature = "kv-surrealkv")]
async fn worker_rows(ds: &Datastore, session: &Session, sql: &str) -> Option<u64> {
	let responses =
		ds.execute(&format!("EXPLAIN ANALYZE {sql}"), session, None).await.expect("explains");
	let Ok(Value::String(text)) = &responses[0].result else {
		panic!("`{sql}` answers {:?}", responses[0].result);
	};
	let (_, block) = text.split_once("Morsels:")?;
	let (_, workers) = block.split_once("workers: rows: ")?;
	let digits: String = workers.chars().take_while(char::is_ascii_digit).collect();
	Some(digits.parse().unwrap_or_else(|_| panic!("`{sql}` renders {block}")))
}

/// Over SurrealKV, whose workers read through a view, the sort over fields runs its key ranges on
/// workers under the 1 MiB budget, and the heap the statements hold at once still stays under half
/// of what they hold unbounded. Workers read rows, so the bound is not the bound of a serial run.
#[cfg(feature = "kv-surrealkv")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn breakers_over_a_small_budget_hold_a_bounded_heap_with_workers_on_surrealkv() {
	let _measuring = MEASURING.lock().await;
	let (free_dir, bounded_dir, observed_dir) = (
		temp_dir::TempDir::new().unwrap(),
		temp_dir::TempDir::new().unwrap(),
		temp_dir::TempDir::new().unwrap(),
	);
	let path = |dir: &temp_dir::TempDir| format!("surrealkv://{}", dir.path().display());
	let (free, session) = ceiling_datastore_over(false, &path(&free_dir), &[]).await;
	let (bounded, _) = ceiling_datastore_over(true, &path(&bounded_dir), &[]).await;
	assert_breakers_stay_bounded(&free, &bounded, &session).await;
	// A datastore that renders what the morsels did, under the same budget, shows the workers read
	// rows: a statement that renders a `Morsels:` block shows workers in it, and the sort does.
	let (observed, _) =
		ceiling_datastore_over(true, &path(&observed_dir), &[("morsel_explain_analyze", "true")])
			.await;
	let mut rendered = Vec::new();
	for sql in BREAKERS {
		if let Some(rows) = worker_rows(&observed, &session, sql).await {
			assert!(rows > 0, "`{sql}`: no worker read a row under a 1 MiB budget");
			rendered.push(sql);
		}
	}
	assert!(
		rendered.contains(&BREAKERS[0]),
		"the sort over fields renders no morsels: {rendered:?}"
	);
}

/// The allocations of whole statements that read a fixed page of linked rows, in total: a page of
/// a fixed size has no per-row slope between datastore sizes, so the statement is counted once on
/// the small datastore.
#[tokio::test]
#[ignore = "a measurement, not a check"]
async fn link_page_statement_allocations() {
	let (ds, session) = datastore(SMALL).await;
	println!("{:<44} {:>10} {:>12}", "statement", "allocs", "bytes");
	for sql in [
		"SELECT * FROM t LIMIT 20 FETCH friend",
		"SELECT friend.a FROM t LIMIT 20",
		"SELECT * FROM t LIMIT 1000 FETCH friend",
		"SELECT friend.a FROM t LIMIT 1000",
	] {
		let (counts, _) = measure(&ds, &session, sql, None, 0).await;
		println!("{sql:<44} {:>10} {:>12}", counts.allocs, counts.bytes);
	}
}

/// The dereferences of a statement whose plan reads them through a window cost no more allocations
/// per row than those of a statement it gives none: the window holds the stored bytes of the
/// records, which the dereference decodes as a read of its own does. The in-memory datastore
/// places both serially, which is the placement a window is read in here. The statement given no
/// window differs by a filter, which the plan gives no window to, so it also judges a predicate per
/// row: the bound is loose, an upper bound on what the window may add.
#[tokio::test]
async fn a_dereference_window_adds_no_allocation_per_row() {
	let _measuring = MEASURING.lock().await;
	const CHECK_SMALL: usize = 2_000;
	const CHECK_LARGE: usize = 4_000;
	let (small, session) = datastore(CHECK_SMALL).await;
	let (large, _) = datastore(CHECK_LARGE).await;
	let shape = |name: &str| SHAPES.iter().find(|shape| shape.name == name).expect("a shape");
	let windowed =
		per_row((&*small, CHECK_SMALL), (&*large, CHECK_LARGE), &session, shape("deref")).await;
	let unwindowed = per_row(
		(&*small, CHECK_SMALL),
		(&*large, CHECK_LARGE),
		&session,
		shape("deref_unwindowed"),
	)
	.await;
	println!("deref allocs/row: windowed {windowed:.2}, no window {unwindowed:.2}");
	assert!(
		windowed <= unwindowed + 0.5,
		"a window adds allocations per row: {windowed:.2} against {unwindowed:.2}"
	);
}
