#![allow(clippy::unwrap_used)]
#![recursion_limit = "256"]
// TODO: Remove once this crate no longer needs it; see `absolute_paths` in the workspace lints.
#![allow(clippy::absolute_paths)]

//! Compile-path benchmark: the cost of turning statement text into a running program.
//!
//! Every case is measured through `Datastore::execute` alone, so the numbers cover the whole
//! path (parse, bind, plan, lower, run) and no planner internals are touched. The corpus runs in
//! two groups:
//!
//! - `compile_arrival`: each iteration sends text that no earlier iteration sent, so nothing is
//!   served from a plan or program cache.
//! - `compile_served`: every iteration sends the same text repeated, so a plan or program cache can
//!   serve every iteration after the first.
//!
//! The difference between the groups is the cost a cache hit removes.
//!
//! Two reports are built only with the `compile-timers` feature:
//!
//! - `compile_allocations` (built only without the `allocator` feature, which installs its own
//!   global allocator): allocations and bytes per execution of each arrival text, from a counting
//!   `#[global_allocator]`, then timed by criterion like the arrival group.
//! - `compile_phases`: the mean time per planning phase, each phase's share of the total, the share
//!   of the request's compile time the phases cover, the catalog reads per compile and the e-graph
//!   share against its budget, from the `surrealdb::compile::*` events the planning phases emit.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use common::{create_runtime, setup_datastore};
use criterion::measurement::WallTime;
use criterion::{BatchSize, BenchmarkGroup, Criterion, criterion_group, criterion_main};
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_types::Variables;
use tokio::runtime::Runtime;

/// One corpus entry: the statements that prepare the datastore, and the text generator.
///
/// `text(salt)` must return text whose parse differs for every distinct `salt`, and must succeed
/// when the same text is executed repeatedly. `vars(salt)`, when present, returns the variables
/// the text reads; they are sent with the text on every execution and must bind for every `salt`.
struct Case {
	name: &'static str,
	setup: String,
	text: fn(u64) -> String,
	vars: Option<fn(u64) -> Variables>,
}

/// Graph and aggregate fixtures shared by the cases that read them.
const FIXTURE: &str = "
	DEFINE TABLE person SCHEMALESS;
	DEFINE TABLE knows SCHEMALESS;
	FOR $i IN 0..20 {
		CREATE type::record('person', $i) SET name = 'p' + <string> $i;
	};
	FOR $i IN 0..20 {
		FOR $j IN 1..4 {
			RELATE (type::record('person', $i))->knows->(type::record('person', ($i + $j) % 20))
				SET weight = $j;
		};
	};
	DEFINE TABLE item SCHEMALESS;
	FOR $i IN 0..200 {
		CREATE type::record('item', $i) SET cat = $i % 5, v = $i;
	};
	DEFINE TABLE doc SCHEMALESS;
	DEFINE INDEX doc_a ON doc FIELDS a;
	DEFINE EVENT doc_changed ON doc WHEN $event = 'UPDATE' THEN (
		CREATE doc_log SET at = time::now(), doc = $after.id
	);
	CREATE doc:1 SET a = 0, b = 0, c = 0;
	DEFINE TABLE bulk SCHEMALESS;
";

fn point_select(salt: u64) -> String {
	format!("SELECT * FROM item:{} WHERE v >= {salt};", salt % 200)
}

fn point_create(salt: u64) -> String {
	format!("CREATE point_new SET v = {salt};")
}

fn point_select_plain(salt: u64) -> String {
	format!("SELECT * FROM item:{salt};")
}

/// A point-sized `CREATE` with parameter assignments. A write with a `SET` clause is not handled
/// by the fast path, so this case runs the full planner, e-graph included, and its e-graph share
/// is not zero.
fn point_create_params(salt: u64) -> String {
	format!("CREATE point_param SET a = $a, b = $b, n = {salt};")
}

fn point_create_params_vars(salt: u64) -> Variables {
	let mut vars = Variables::new();
	vars.insert("a", i64::try_from(salt).unwrap());
	vars.insert("b", format!("b{salt}"));
	vars
}

fn indexed_update(salt: u64) -> String {
	format!("UPDATE doc:1 SET a = {salt}, b = {}, c = {};", salt + 1, salt + 2)
}

fn block_of_ten(salt: u64) -> String {
	let mut text = String::from("BEGIN;\n");
	for i in 0..8 {
		text.push_str(&format!("UPSERT block_row:{i} SET v = {salt}, i = {i};\n"));
	}
	text.push_str(&format!("SELECT * FROM block_row WHERE v = {salt};\n"));
	text.push_str("COMMIT;");
	text
}

fn graph_five_way(salt: u64) -> String {
	format!(
		"SELECT ->knows->person->knows->person->knows->person->knows->person->knows->person \
		 FROM person:{} WHERE name != 'salt{salt}';",
		salt % 20
	)
}

fn group_by_four(salt: u64) -> String {
	format!(
		"SELECT cat, count() AS n, math::sum(v) AS total, math::mean(v) AS mean, \
		 math::max(v) AS top FROM item WHERE v >= {} GROUP BY cat;",
		salt % 100
	)
}

fn insert_literal_1000(salt: u64) -> String {
	let mut text = String::from("INSERT IGNORE INTO bulk [");
	for i in 0..1_000 {
		if i > 0 {
			text.push(',');
		}
		text.push_str(&format!("{{ id: {i}, a: {i}, b: 'row_{i}', salt: {salt} }}"));
	}
	text.push_str("];");
	text
}

/// Fixture of the planning-shape cases: a small indexed table, and a schemafull table with
/// defaults, an assertion and an index, so that execution is negligible next to planning.
const SHAPES_FIXTURE: &str = "
	DEFINE TABLE emp SCHEMALESS;
	DEFINE INDEX emp_dept ON emp FIELDS dept;
	CREATE emp:1 SET name = 'a', dept = 1, age = 5;
	CREATE emp:2 SET name = 'b', dept = 2, age = 7;
	DEFINE TABLE sw SCHEMAFULL;
	DEFINE FIELD name ON sw TYPE string ASSERT string::len($value) > 0;
	DEFINE FIELD age ON sw TYPE int DEFAULT 0;
	DEFINE FIELD tags ON sw TYPE array<string> DEFAULT [];
	DEFINE FIELD created ON sw TYPE datetime DEFAULT time::now();
	DEFINE INDEX sw_age ON sw FIELDS age;
";

fn shape_return_literal(salt: u64) -> String {
	format!("RETURN {salt};")
}

fn shape_simple_select(salt: u64) -> String {
	format!("SELECT * FROM emp LIMIT {};", salt % 1000 + 1)
}

fn shape_filtered_indexed_select(salt: u64) -> String {
	format!(
		"SELECT name, dept FROM emp WHERE dept = {} AND age > {salt} ORDER BY name LIMIT 10;",
		salt % 3
	)
}

fn shape_five_subqueries(salt: u64) -> String {
	format!(
		"SELECT name, \
		 (SELECT count() FROM emp WHERE dept = {salt} GROUP ALL) AS a, \
		 (SELECT * FROM emp WHERE age > {salt} LIMIT 1) AS b, \
		 (SELECT name FROM emp WHERE dept = 2 LIMIT 3) AS c, \
		 (SELECT math::max(age) AS m FROM emp GROUP ALL) AS d, \
		 (SELECT * FROM emp WHERE id = emp:1) AS e \
		 FROM emp LIMIT 1;"
	)
}

fn shape_schema_write(salt: u64) -> String {
	format!("CREATE sw SET name = 'n{salt}', age = {salt}, tags = ['a', 'b'];")
}

fn corpus() -> Vec<Case> {
	vec![
		Case {
			name: "point_select_by_id",
			setup: FIXTURE.into(),
			text: point_select,
			vars: None,
		},
		Case {
			name: "point_create",
			setup: FIXTURE.into(),
			text: point_create,
			vars: None,
		},
		Case {
			name: "indexed_update_three_fields_one_event",
			setup: FIXTURE.into(),
			text: indexed_update,
			vars: None,
		},
		Case {
			name: "block_ten_statements_begin_commit",
			setup: FIXTURE.into(),
			text: block_of_ten,
			vars: None,
		},
		Case {
			name: "graph_five_way",
			setup: FIXTURE.into(),
			text: graph_five_way,
			vars: None,
		},
		Case {
			name: "group_by_four_aggregates",
			setup: FIXTURE.into(),
			text: group_by_four,
			vars: None,
		},
		Case {
			name: "insert_literal_1000_objects",
			setup: FIXTURE.into(),
			text: insert_literal_1000,
			vars: None,
		},
		Case {
			name: "point_select_by_id_plain",
			setup: FIXTURE.into(),
			text: point_select_plain,
			vars: None,
		},
		Case {
			name: "point_create_params",
			setup: FIXTURE.into(),
			text: point_create_params,
			vars: Some(point_create_params_vars),
		},
		Case {
			name: "shape_return_literal",
			setup: SHAPES_FIXTURE.into(),
			text: shape_return_literal,
			vars: None,
		},
		Case {
			name: "shape_simple_select",
			setup: SHAPES_FIXTURE.into(),
			text: shape_simple_select,
			vars: None,
		},
		Case {
			name: "shape_filtered_indexed_select",
			setup: SHAPES_FIXTURE.into(),
			text: shape_filtered_indexed_select,
			vars: None,
		},
		Case {
			name: "shape_five_subqueries",
			setup: SHAPES_FIXTURE.into(),
			text: shape_five_subqueries,
			vars: None,
		},
		Case {
			name: "shape_schema_write",
			setup: SHAPES_FIXTURE.into(),
			text: shape_schema_write,
			vars: None,
		},
	]
}

/// The text and variables for the arrival of `salt`.
fn arrival(case: &Case, salt: u64) -> (String, Option<Variables>) {
	((case.text)(salt), case.vars.map(|vars| vars(salt)))
}

/// A datastore prepared for `case`, after proving that the case runs cleanly.
fn prepare(runtime: &Runtime, case: &Case) -> (Arc<Datastore>, Session) {
	// One datastore per case, so one case's writes never shape another's
	let (dbs, ses) = runtime.block_on(setup_datastore());
	runtime.block_on(dbs.execute(&case.setup, &ses, None)).unwrap();
	// The case must run cleanly before it is timed
	let (text, vars) = arrival(case, 0);
	let probe = runtime.block_on(dbs.execute(&text, &ses, vars)).unwrap();
	for response in &probe {
		assert!(response.result.is_ok(), "{} failed: {:?}", case.name, response.result);
	}
	(dbs, ses)
}

/// Time `case` in `group`. `fresh` selects arrival (new text per iteration) over served (one text
/// repeated).
fn time_case(
	group: &mut BenchmarkGroup<'_, WallTime>,
	runtime: &Runtime,
	case: &Case,
	dbs: &Datastore,
	ses: &Session,
	fresh: bool,
) {
	let salt = AtomicU64::new(1);
	let served = arrival(case, 0);
	group.bench_function(case.name, |b| {
		b.to_async(runtime).iter_batched(
			|| {
				// Text generation is excluded from the measurement
				if fresh {
					arrival(case, salt.fetch_add(1, Ordering::Relaxed))
				} else {
					served.clone()
				}
			},
			|(query, vars)| async move {
				std::hint::black_box(dbs.execute(&query, ses, vars).await).unwrap()
			},
			BatchSize::SmallInput,
		)
	});
}

/// Run one group over the whole corpus. `fresh` selects arrival (new text per iteration) over
/// served (one text repeated).
fn run_group(c: &mut Criterion, group_name: &str, fresh: bool) {
	let mut group = c.benchmark_group(group_name);
	let runtime = create_runtime();
	for case in corpus() {
		let (dbs, ses) = prepare(&runtime, &case);
		time_case(&mut group, &runtime, &case, &dbs, &ses, fresh);
	}
	group.finish();
}

fn bench_arrival(c: &mut Criterion) {
	run_group(c, "compile_arrival", true);
}

fn bench_served(c: &mut Criterion) {
	run_group(c, "compile_served", false);
}

/// Executions per case in the allocation count and the phase report.
#[cfg(feature = "compile-timers")]
const REPORT_EXECUTIONS: u64 = 100;

/// Executions per case that run before counting starts, so one-time initialisation is excluded.
#[cfg(feature = "compile-timers")]
const WARMUP_EXECUTIONS: u64 = 5;

/// Run `executions` distinct arrivals of `case`, numbered from `first`, one after another.
#[cfg(feature = "compile-timers")]
fn execute_arrivals(
	runtime: &Runtime,
	case: &Case,
	dbs: &Datastore,
	ses: &Session,
	first: u64,
	executions: u64,
) {
	// Text generation is excluded from whatever the caller measures around this call
	let inputs: Vec<_> = (first..first + executions).map(|salt| arrival(case, salt)).collect();
	runtime.block_on(async {
		for (query, vars) in inputs {
			std::hint::black_box(dbs.execute(&query, ses, vars).await).unwrap();
		}
	});
}

/// Counts the allocations and requested bytes of every allocation request made by the process.
///
/// Frees are not counted: the figures are totals requested, not live memory. A reallocation counts
/// as one allocation of its new size.
#[cfg(all(feature = "compile-timers", not(feature = "allocator")))]
mod counting {
	use std::alloc::{GlobalAlloc, Layout, System};
	use std::sync::atomic::{AtomicU64, Ordering};

	pub struct CountingAlloc;

	static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
	static BYTES: AtomicU64 = AtomicU64::new(0);

	fn count(size: usize) {
		ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
		BYTES.fetch_add(size as u64, Ordering::Relaxed);
	}

	/// The `(allocations, bytes)` requested so far.
	pub fn totals() -> (u64, u64) {
		(ALLOCATIONS.load(Ordering::Relaxed), BYTES.load(Ordering::Relaxed))
	}

	// SAFETY: every method forwards to `System` with the arguments it received, so the allocator
	// contract is upheld by `System`; the counters have no effect on the returned pointers.
	unsafe impl GlobalAlloc for CountingAlloc {
		unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
			count(layout.size());
			unsafe { System.alloc(layout) }
		}

		unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
			count(layout.size());
			unsafe { System.alloc_zeroed(layout) }
		}

		unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
			unsafe { System.dealloc(ptr, layout) }
		}

		unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
			count(new_size);
			unsafe { System.realloc(ptr, layout, new_size) }
		}
	}
}

#[cfg(all(feature = "compile-timers", not(feature = "allocator")))]
#[global_allocator]
static ALLOCATOR: counting::CountingAlloc = counting::CountingAlloc;

/// Print the allocations and bytes per execution of each case's arrival text, then time the same
/// executions so criterion records them next to the arrival group.
#[cfg(all(feature = "compile-timers", not(feature = "allocator")))]
fn bench_allocations(c: &mut Criterion) {
	let mut group = c.benchmark_group("compile_allocations");
	let runtime = create_runtime();
	for case in corpus() {
		let (dbs, ses) = prepare(&runtime, &case);
		execute_arrivals(&runtime, &case, &dbs, &ses, 1, WARMUP_EXECUTIONS);
		let (allocations, bytes) = counting::totals();
		execute_arrivals(&runtime, &case, &dbs, &ses, 1_000, REPORT_EXECUTIONS);
		let (allocations_after, bytes_after) = counting::totals();
		println!(
			"compile_allocations/{}: {} allocations, {} bytes per execution ({} executions)",
			case.name,
			(allocations_after - allocations) / REPORT_EXECUTIONS,
			(bytes_after - bytes) / REPORT_EXECUTIONS,
			REPORT_EXECUTIONS,
		);
		time_case(&mut group, &runtime, &case, &dbs, &ses, true);
	}
	group.finish();
}

/// Without the feature, or with the `allocator` feature, there is no counting allocator.
#[cfg(not(all(feature = "compile-timers", not(feature = "allocator"))))]
fn bench_allocations(_c: &mut Criterion) {}

/// Collection of the `surrealdb::compile::*` events emitted under `compile-timers`.
#[cfg(feature = "compile-timers")]
mod phases {
	use std::collections::HashMap;
	use std::sync::atomic::{AtomicBool, Ordering};
	use std::sync::{LazyLock, Mutex, Once};

	use tracing::field::{Field, Visit};
	use tracing::subscriber::Interest;
	use tracing::{Event, Metadata, Subscriber};
	use tracing_subscriber::layer::{Context, SubscriberExt};
	use tracing_subscriber::{Layer, registry};

	const TARGET_PREFIX: &str = "surrealdb::compile::";

	/// The phase names the timers report, in the order a compile runs them. The list is the
	/// one the emitter checks its names against.
	pub use surrealdb_core::COMPILE_PHASES as PHASES;

	/// What the layer collected since the last `take`.
	#[derive(Default)]
	pub struct Collected {
		/// Per phase name: total nanoseconds and number of events.
		pub phases: HashMap<&'static str, (u64, u64)>,
		/// Total nanoseconds the request compiles took, and how many request events said so.
		pub request_nanos: u64,
		pub requests: u64,
		/// Every compile event reported, whatever its target.
		pub events: u64,
		/// Total catalog reads reported.
		pub catalog_reads: u64,
		/// E-graph events reported, with their total nodes and iterations.
		pub egraph_events: u64,
		pub egraph_nodes: u64,
		pub egraph_iterations: u64,
	}

	static COLLECTED: LazyLock<Mutex<Collected>> = LazyLock::new(Mutex::default);

	/// Events are built and collected only while this is set, so the timed groups and the
	/// allocation counts run without the layer's work.
	static ACTIVE: AtomicBool = AtomicBool::new(false);

	/// Install the collecting subscriber as the process default; later calls do nothing.
	pub fn install() {
		static INSTALLED: Once = Once::new();
		INSTALLED.call_once(|| {
			let subscriber = registry().with(PhaseLayer);
			// Another default already installed leaves the report empty rather than failing
			let _ = tracing::subscriber::set_global_default(subscriber);
		});
	}

	/// Collect events until `stop`; discards anything collected before.
	pub fn start() {
		*COLLECTED.lock().unwrap() = Collected::default();
		ACTIVE.store(true, Ordering::SeqCst);
	}

	/// The nanoseconds one stretch costs under this layer: the two clock reads that bound it and
	/// the delivery of its event. That time passes between the stretches a compile reports, not
	/// inside them.
	pub fn stretch_cost_nanos() -> f64 {
		const STRETCHES: u32 = 100_000;
		start();
		let begun = web_time::Instant::now();
		for _ in 0..STRETCHES {
			let started = web_time::Instant::now();
			let nanos = started.elapsed().as_nanos() as u64;
			tracing::event!(
				target: "surrealdb::compile::phase",
				tracing::Level::DEBUG,
				phase = PHASES[0],
				nanos
			);
		}
		let spent = begun.elapsed();
		stop();
		spent.as_nanos() as f64 / f64::from(STRETCHES)
	}

	/// Stop collecting and return what was collected since `start`.
	pub fn stop() -> Collected {
		ACTIVE.store(false, Ordering::SeqCst);
		std::mem::take(&mut *COLLECTED.lock().unwrap())
	}

	struct PhaseLayer;

	impl<S: Subscriber> Layer<S> for PhaseLayer {
		fn register_callsite(&self, metadata: &'static Metadata<'static>) -> Interest {
			if metadata.target().starts_with(TARGET_PREFIX) {
				Interest::sometimes()
			} else {
				Interest::never()
			}
		}

		fn enabled(&self, metadata: &Metadata<'_>, _ctx: Context<'_, S>) -> bool {
			ACTIVE.load(Ordering::Relaxed) && metadata.target().starts_with(TARGET_PREFIX)
		}

		fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
			let mut fields = Fields::default();
			event.record(&mut fields);
			let mut collected = COLLECTED.lock().unwrap();
			collected.events += 1;
			match event.metadata().target() {
				"surrealdb::compile::phase" => {
					if let Some(name) = fields.phase {
						let entry = collected.phases.entry(name).or_default();
						entry.0 += fields.nanos;
						entry.1 += 1;
					}
				}
				"surrealdb::compile::request" => {
					collected.request_nanos += fields.nanos;
					collected.requests += 1;
				}
				"surrealdb::compile::catalog_reads" => collected.catalog_reads += fields.count,
				"surrealdb::compile::egraph" => {
					collected.egraph_events += 1;
					collected.egraph_nodes += fields.nodes;
					collected.egraph_iterations += fields.iterations;
				}
				_ => {}
			}
		}
	}

	/// The fields of one compile event; a field the event does not carry stays at its default.
	#[derive(Default)]
	struct Fields {
		phase: Option<&'static str>,
		nanos: u64,
		count: u64,
		nodes: u64,
		iterations: u64,
	}

	impl Visit for Fields {
		fn record_u64(&mut self, field: &Field, value: u64) {
			match field.name() {
				"nanos" => self.nanos = value,
				"count" => self.count = value,
				"nodes" => self.nodes = value,
				"iterations" => self.iterations = value,
				_ => {}
			}
		}

		fn record_str(&mut self, field: &Field, value: &str) {
			if field.name() == "phase" {
				// Interned against the known names so collecting allocates nothing
				self.phase = PHASES.iter().copied().find(|name| *name == value);
			}
		}

		fn record_debug(&mut self, _field: &Field, _value: &dyn std::fmt::Debug) {}
	}
}

/// The share of the compile time the e-graph may take. A case above it is flagged.
#[cfg(feature = "compile-timers")]
const EGRAPH_BUDGET_PERCENT: f64 = 15.0;

/// The share of the request's compile time the phases must cover, net of the timers' own cost.
#[cfg(feature = "compile-timers")]
const COVERAGE_FLOOR_PERCENT: f64 = 90.0;

/// Print, per case, the mean time per planning phase, each phase's share of the total, how much of
/// the request's compile time the phases cover, the catalog reads per compile and the e-graph
/// share against its budget.
///
/// A mean is the phase's total over the executions, so a phase that runs once per statement of a
/// block counts all of its runs. The total is the sum of the phase totals. The coverage is the
/// phases' total, parse excluded, over the time the request compiles took, which spans every
/// statement the request compiles at its arrival. The timers' own cost passes between the
/// stretches and does not exist without them, so the coverage measured against
/// [`COVERAGE_FLOOR_PERCENT`] is the one net of that cost. The catalog reads are the sum of the
/// legacy bind's distinct entries and the memo's issued reads, as
/// `surrealdb::compile::catalog_reads` reports them; the figure is not a count of datastore reads.
/// A coverage whose request time is not positive is reported as `n/a`. Exceeding the e-graph
/// budget or falling below the coverage floor is reported, never a failure.
/// Without call sites for the timers the report is empty.
#[cfg(feature = "compile-timers")]
fn bench_phases(_c: &mut Criterion) {
	phases::install();
	let stretch_cost = phases::stretch_cost_nanos();
	println!("compile_phases: one stretch costs {stretch_cost:.0} ns under the collecting layer");
	let runtime = create_runtime();
	for case in corpus() {
		let (dbs, ses) = prepare(&runtime, &case);
		execute_arrivals(&runtime, &case, &dbs, &ses, 1, WARMUP_EXECUTIONS);
		phases::start();
		let started = web_time::Instant::now();
		execute_arrivals(&runtime, &case, &dbs, &ses, 1_000, REPORT_EXECUTIONS);
		let wall = started.elapsed();
		let collected = phases::stop();
		let executions = REPORT_EXECUTIONS as f64;
		let total: u64 = collected.phases.values().map(|(nanos, _)| nanos).sum();
		println!("compile_phases/{} ({} executions)", case.name, REPORT_EXECUTIONS);
		if collected.phases.is_empty() {
			println!("  no compile events collected");
			continue;
		}
		for name in phases::PHASES.iter().copied() {
			let Some((nanos, events)) = collected.phases.get(name) else {
				continue;
			};
			println!(
				"  {name:<9} mean {:>10.1} us  share {:>5.1}%  events/execution {:.2}",
				*nanos as f64 / executions / 1_000.0,
				*nanos as f64 / total as f64 * 100.0,
				*events as f64 / executions,
			);
		}
		// Parsing runs before the request is compiled, so the request's compile time holds every
		// phase but it. The timers' own cost passes between the stretches, and does not exist
		// without them, so coverage is also stated net of it.
		let parse_nanos = collected.phases.get("parse").map_or(0, |(nanos, _)| *nanos);
		let covered = (total - parse_nanos) as f64;
		let request = collected.request_nanos as f64;
		// The parse event and the request event are delivered outside the request's compile time
		let parse_events = collected.phases.get("parse").map_or(0, |(_, events)| *events);
		let inside = (collected.events - parse_events - collected.requests) as f64;
		let timers = inside * stretch_cost;
		// A case that records phase events but no request event, or whose timer cost exceeds its
		// request time, has no coverage to state
		let coverage = |whole: f64| (whole > 0.0).then(|| covered / whole * 100.0);
		let gross_coverage = coverage(request);
		let net_coverage = coverage(request - timers);
		let percent =
			|coverage: Option<f64>| coverage.map_or("n/a".to_owned(), |c| format!("{c:.1}%"));
		let coverage_verdict = match net_coverage {
			Some(c) if c < COVERAGE_FLOOR_PERCENT => "BELOW FLOOR",
			Some(_) => "meets floor",
			None => "no verdict",
		};
		println!(
			"  phases cover {} of the {:.1} us per execution the request compile took (parse \
			 excluded), {} net of the {:.1} us per execution the {:.1} events cost \
			 ({coverage_verdict} of {COVERAGE_FLOOR_PERCENT:.0}%); {:.1} us per execution end to \
			 end",
			percent(gross_coverage),
			request / executions / 1_000.0,
			percent(net_coverage),
			timers / executions / 1_000.0,
			inside / executions,
			wall.as_nanos() as f64 / executions / 1_000.0,
		);
		println!(
			"  catalog reads per compile (bind entries plus memo reads): {:.2}",
			collected.catalog_reads as f64 / executions
		);
		let egraph_nanos = collected.phases.get("egraph").map_or(0, |(nanos, _)| *nanos);
		let egraph_share = egraph_nanos as f64 / total as f64 * 100.0;
		let verdict = if egraph_share > EGRAPH_BUDGET_PERCENT {
			"OVER BUDGET"
		} else {
			"within budget"
		};
		println!(
			"  egraph share {egraph_share:.1}% of {EGRAPH_BUDGET_PERCENT:.0}% budget: {verdict} \
			 ({} nodes, {} iterations over {} runs)",
			collected.egraph_nodes, collected.egraph_iterations, collected.egraph_events,
		);
	}
}

/// Without the feature the timers emit nothing, so there is nothing to report.
#[cfg(not(feature = "compile-timers"))]
fn bench_phases(_c: &mut Criterion) {}

criterion_group!(
	name = benches;
	config = Criterion::default().sample_size(20);
	targets = bench_arrival, bench_served, bench_phases, bench_allocations,
);
criterion_main!(benches);
