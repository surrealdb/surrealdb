//! Times statements that read zero or one record, one at a time on one thread, to expose the
//! fixed cost every statement pays.
//!
//! `BENCH_ONLY=<case>` repeats one case for `BENCH_SECS`, for a profiler. `BENCH_PARSE=1` also
//! times parsing alone for each case's text.

#![allow(clippy::unwrap_used)]
#![recursion_limit = "256"]
#![allow(clippy::absolute_paths)]
#![allow(clippy::print_stdout)]

#[cfg(not(feature = "allocator"))]
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_types::{Number, RecordId, Value, Variables};
use web_time::Instant;

/// Counts allocations while [`COUNTING`] is set; installed unless the crate's own allocator is.
#[cfg(not(feature = "allocator"))]
struct Counting;
static COUNTING: AtomicBool = AtomicBool::new(false);
static ALLOCS: AtomicU64 = AtomicU64::new(0);

// SAFETY: forwards to `System` unchanged.
#[cfg(not(feature = "allocator"))]
unsafe impl GlobalAlloc for Counting {
	unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
		if COUNTING.load(Ordering::Relaxed) {
			ALLOCS.fetch_add(1, Ordering::Relaxed);
		}
		// SAFETY: caller upholds the contract.
		unsafe { System.alloc(layout) }
	}
	unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
		if COUNTING.load(Ordering::Relaxed) {
			ALLOCS.fetch_add(1, Ordering::Relaxed);
		}
		// SAFETY: caller upholds the contract.
		unsafe { System.alloc_zeroed(layout) }
	}
	unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
		if COUNTING.load(Ordering::Relaxed) {
			ALLOCS.fetch_add(1, Ordering::Relaxed);
		}
		// SAFETY: caller upholds the contract.
		unsafe { System.realloc(ptr, layout, new_size) }
	}
	unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
		// SAFETY: caller upholds the contract.
		unsafe { System.dealloc(ptr, layout) }
	}
}

#[cfg(not(feature = "allocator"))]
#[global_allocator]
static GLOBAL: Counting = Counting;

// --------------------------------------------------
// Compile phase events (`surrealdb-core/compile-timers`)
// --------------------------------------------------

mod phases {
	use std::collections::BTreeMap;
	use std::sync::Mutex;
	use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

	use tracing::field::{Field, Visit};
	use tracing::span::{Attributes, Id, Record};
	use tracing::{Event, Metadata, Subscriber};

	pub static ACTIVE: AtomicBool = AtomicBool::new(false);
	pub static PHASES: Mutex<BTreeMap<String, (u64, u64)>> = Mutex::new(BTreeMap::new());
	pub static REQUEST_NANOS: AtomicU64 = AtomicU64::new(0);
	pub static REQUESTS: AtomicU64 = AtomicU64::new(0);
	pub static CATALOG: AtomicU64 = AtomicU64::new(0);
	pub static EGRAPH: AtomicU64 = AtomicU64::new(0);

	pub struct Collector;

	#[derive(Default)]
	struct Fields {
		phase: Option<String>,
		nanos: u64,
		count: u64,
	}
	impl Visit for Fields {
		fn record_u64(&mut self, f: &Field, v: u64) {
			match f.name() {
				"nanos" => self.nanos = v,
				"count" => self.count = v,
				_ => {}
			}
		}
		fn record_str(&mut self, f: &Field, v: &str) {
			if f.name() == "phase" {
				self.phase = Some(v.to_string());
			}
		}
		fn record_debug(&mut self, f: &Field, v: &dyn std::fmt::Debug) {
			if f.name() == "phase" {
				self.phase = Some(format!("{v:?}").trim_matches('"').to_string());
			}
		}
	}

	impl Subscriber for Collector {
		fn enabled(&self, m: &Metadata<'_>) -> bool {
			m.target().starts_with("surrealdb::compile::")
		}
		fn new_span(&self, _: &Attributes<'_>) -> Id {
			Id::from_u64(1)
		}
		fn record(&self, _: &Id, _: &Record<'_>) {}
		fn record_follows_from(&self, _: &Id, _: &Id) {}
		fn event(&self, e: &Event<'_>) {
			if !ACTIVE.load(Ordering::Relaxed) {
				return;
			}
			let mut f = Fields::default();
			e.record(&mut f);
			match e.metadata().target() {
				"surrealdb::compile::phase" => {
					let mut p = PHASES.lock().unwrap();
					let x = p.entry(f.phase.unwrap_or_default()).or_default();
					x.0 += f.nanos;
					x.1 += 1;
				}
				"surrealdb::compile::request" => {
					REQUEST_NANOS.fetch_add(f.nanos, Ordering::Relaxed);
					REQUESTS.fetch_add(1, Ordering::Relaxed);
				}
				"surrealdb::compile::catalog_reads" => {
					CATALOG.fetch_add(f.count, Ordering::Relaxed);
				}
				"surrealdb::compile::egraph" => {
					EGRAPH.fetch_add(1, Ordering::Relaxed);
				}
				_ => {}
			}
		}
		fn enter(&self, _: &Id) {}
		fn exit(&self, _: &Id) {}
	}

	pub fn reset() {
		PHASES.lock().unwrap().clear();
		for a in [&REQUEST_NANOS, &REQUESTS, &CATALOG, &EGRAPH] {
			a.store(0, Ordering::Relaxed);
		}
	}

	pub fn report(name: &str, n: f64) {
		let p = PHASES.lock().unwrap();
		if p.is_empty() && REQUESTS.load(Ordering::Relaxed) == 0 {
			return;
		}
		let mut line = format!("  phases {name}:");
		let mut sum = 0u64;
		for (k, (ns, ev)) in p.iter() {
			sum += ns;
			line += &format!(" {k}={:.2}us/{:.1}", *ns as f64 / n / 1000.0, *ev as f64 / n);
		}
		line += &format!(
			" | sum={:.2}us request={:.2}us x{:.2} catalog={:.2} egraph={:.2}",
			sum as f64 / n / 1000.0,
			REQUEST_NANOS.load(Ordering::Relaxed) as f64 / n / 1000.0,
			REQUESTS.load(Ordering::Relaxed) as f64 / n,
			CATALOG.load(Ordering::Relaxed) as f64 / n,
			EGRAPH.load(Ordering::Relaxed) as f64 / n,
		);
		println!("{line}");
	}
}

// --------------------------------------------------
// Wake probe (`BENCH_WAKES=1`)
// --------------------------------------------------

mod probe {
	use std::future::Future;
	use std::pin::Pin;
	use std::sync::Arc;
	use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
	use std::task::{Context, Poll, Wake, Waker};

	pub static POLLS: AtomicU64 = AtomicU64::new(0);
	pub static PENDING: AtomicU64 = AtomicU64::new(0);
	pub static SELF_WAKES: AtomicU64 = AtomicU64::new(0);
	pub static OTHER_WAKES: AtomicU64 = AtomicU64::new(0);
	static IN_POLL: AtomicBool = AtomicBool::new(false);
	static TRACES: AtomicU64 = AtomicU64::new(0);

	struct Counting(Waker);
	impl Wake for Counting {
		fn wake(self: Arc<Self>) {
			self.wake_by_ref()
		}
		fn wake_by_ref(self: &Arc<Self>) {
			if IN_POLL.load(Ordering::Relaxed) {
				SELF_WAKES.fetch_add(1, Ordering::Relaxed);
				if TRACES.fetch_add(1, Ordering::Relaxed) < 2 {
					eprintln!("self-wake:\n{}", std::backtrace::Backtrace::force_capture());
				}
			} else {
				OTHER_WAKES.fetch_add(1, Ordering::Relaxed);
			}
			self.0.wake_by_ref()
		}
	}

	pub struct Probe<F>(pub Pin<Box<F>>);
	impl<F: Future> Future for Probe<F> {
		type Output = F::Output;
		fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
			let waker = Waker::from(Arc::new(Counting(cx.waker().clone())));
			let mut inner = Context::from_waker(&waker);
			POLLS.fetch_add(1, Ordering::Relaxed);
			IN_POLL.store(true, Ordering::Relaxed);
			let r = self.0.as_mut().poll(&mut inner);
			IN_POLL.store(false, Ordering::Relaxed);
			if r.is_pending() {
				PENDING.fetch_add(1, Ordering::Relaxed);
			}
			r
		}
	}

	pub fn report(n: f64) {
		println!(
			"  per request: polls {:.3}, pending {:.3}, self-wakes {:.3}, other wakes {:.3}",
			POLLS.swap(0, Ordering::Relaxed) as f64 / n,
			PENDING.swap(0, Ordering::Relaxed) as f64 / n,
			SELF_WAKES.swap(0, Ordering::Relaxed) as f64 / n,
			OTHER_WAKES.swap(0, Ordering::Relaxed) as f64 / n,
		);
	}
}

/// Nanoseconds of CPU time on `clock` (`clock_gettime`), or 0 where this bench does not read the
/// CPU clocks: CPU columns then read 0.
#[cfg(all(unix, target_pointer_width = "64"))]
fn cpu_clock_ns(clock: u32) -> u64 {
	#[repr(C)]
	struct Timespec {
		sec: i64,
		nsec: i64,
	}
	unsafe extern "C" {
		fn clock_gettime(clock: u32, tp: *mut Timespec) -> i32;
	}
	let mut ts = Timespec {
		sec: 0,
		nsec: 0,
	};
	// SAFETY: `ts` is a valid out-pointer for the call.
	unsafe { clock_gettime(clock, &mut ts) };
	ts.sec as u64 * 1_000_000_000 + ts.nsec as u64
}

#[cfg(not(all(unix, target_pointer_width = "64")))]
fn cpu_clock_ns(_clock: u32) -> u64 {
	0
}

/// Nanoseconds of CPU this thread has used (`CLOCK_THREAD_CPUTIME_ID`, 16 on macOS, 3 on Linux).
fn thread_cpu_ns() -> u64 {
	cpu_clock_ns(if cfg!(target_os = "macos") {
		16
	} else {
		3
	})
}

/// Nanoseconds of CPU the whole process has used (`CLOCK_PROCESS_CPUTIME_ID`, 12 on macOS, 2 on
/// Linux): on a multi-thread runtime a request's work runs on any worker.
fn process_cpu_ns() -> u64 {
	cpu_clock_ns(if cfg!(target_os = "macos") {
		12
	} else {
		2
	})
}

fn cpu_ns() -> u64 {
	if MULTI.load(Ordering::Relaxed) {
		process_cpu_ns()
	} else {
		thread_cpu_ns()
	}
}

static MULTI: AtomicBool = AtomicBool::new(false);

fn env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
	std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

const ROWS: u64 = 10_000;

const CASES: [&str; 11] = [
	"return_lit",
	"return_param",
	"let_return",
	"k0_lit",
	"k0_param",
	"k1_lit",
	"k1_param",
	"k1_only_param",
	"k1_typerecord",
	"k1_ls_param",
	"k1_where_id",
];

fn request(case: &str, n: u64) -> (String, Variables) {
	let mut vars = Variables::new();
	let text = match case {
		"return_lit" => "RETURN 1".to_string(),
		"return_param" => {
			vars.insert("n", Value::Number(Number::Int(n as i64)));
			"RETURN $n".to_string()
		}
		"let_return" => "LET $x = 1; RETURN $x".to_string(),
		"k0_lit" => "SELECT * FROM []".to_string(),
		"k0_param" => {
			vars.insert("ids", Value::Array(Vec::<Value>::new().into()));
			"SELECT * FROM $ids".to_string()
		}
		"k1_lit" => format!("SELECT * FROM pk:{n}"),
		"k1_param" => {
			vars.insert("id", RecordId::new("pk", n as i64));
			"SELECT * FROM $id".to_string()
		}
		"k1_only_param" => {
			vars.insert("id", RecordId::new("pk", n as i64));
			"SELECT * FROM ONLY $id".to_string()
		}
		"k1_typerecord" => {
			vars.insert("n", Value::Number(Number::Int(n as i64)));
			"SELECT * FROM type::record('pk', $n)".to_string()
		}
		"k1_ls_param" => {
			vars.insert("id", RecordId::new("ls", n as i64));
			"SELECT * FROM $id".to_string()
		}
		"k1_where_id" => {
			vars.insert("id", RecordId::new("pk", n as i64));
			"SELECT * FROM pk WHERE id = $id".to_string()
		}
		other => panic!("unknown case {other}"),
	};
	(text, vars)
}

async fn run(ds: &Datastore, session: &Session, text: &str, vars: Option<Variables>) {
	for res in ds.execute(text, session, vars).await.unwrap() {
		res.result.unwrap_or_else(|e| panic!("{text}: {e}"));
	}
}

async fn load(ds: &Datastore, session: &Session) {
	run(ds, &Session::owner(), "DEFINE NAMESPACE bench; USE NS bench; DEFINE DATABASE bench", None)
		.await;
	run(
		ds,
		session,
		"DEFINE TABLE ls SCHEMALESS;
		 DEFINE TABLE pk SCHEMAFULL;
		 DEFINE FIELD name ON pk TYPE string;
		 DEFINE FIELD age ON pk TYPE int;
		 DEFINE FIELD score ON pk TYPE float;
		 DEFINE FIELD active ON pk TYPE bool;
		 DEFINE FIELD tags ON pk TYPE array<string>;",
		None,
	)
	.await;
	for table in ["ls", "pk"] {
		let batch: Vec<Value> = (0..ROWS)
			.map(|i| {
				let mut row = surrealdb_types::Object::new();
				row.insert("id".to_string(), Value::RecordId(RecordId::new(table, i as i64)));
				row.insert("name".to_string(), Value::String(format!("n{i}")));
				row.insert("age".to_string(), Value::Number(Number::Int((i % 100) as i64)));
				row.insert("score".to_string(), Value::Number(Number::Float(i as f64 / 7.0)));
				row.insert("active".to_string(), Value::Bool(i % 2 == 0));
				row.insert(
					"tags".to_string(),
					Value::Array(
						vec![Value::String("a".to_string()), Value::String("b".to_string())].into(),
					),
				);
				Value::Object(row)
			})
			.collect();
		let mut vars = Variables::new();
		vars.insert("rows", Value::Array(batch.into()));
		run(ds, session, &format!("INSERT INTO {table} $rows RETURN NONE"), Some(vars)).await;
	}
}

async fn case(
	ds: &std::sync::Arc<Datastore>,
	session: &Session,
	name: &str,
	secs: f64,
) -> (f64, f64, f64, f64) {
	let mut samples = Vec::new();
	let mut cpu = Vec::new();
	let mut allocs = 0u64;
	let budget = std::time::Duration::from_secs_f64(secs);
	let mut timed = std::time::Duration::ZERO;
	let mut i = 0u64;
	while i < 200 || timed < budget {
		let (text, vars) = request(name, (i * 7919) % ROWS);
		let counting = i >= 200;
		phases::ACTIVE.store(counting, Ordering::Relaxed);
		let before = ALLOCS.load(Ordering::Relaxed);
		COUNTING.store(counting, Ordering::Relaxed);
		let cpu0 = cpu_ns();
		let start = Instant::now();
		let res = if env_or("BENCH_ENCODED", 0u8) != 0 {
			let encoded = surrealdb_core::rpc::format::encoded::execute_encoded(
				ds,
				&text,
				session,
				Some(vars),
				surrealdb_rpc::RowFormat::Cbor,
				None,
			)
			.await
			.unwrap();
			std::hint::black_box(&encoded);
			Vec::new()
		} else if env_or("BENCH_WAKES", 0u8) != 0 {
			probe::Probe(Box::pin(ds.execute(&text, session, Some(vars)))).await.unwrap()
		} else if env_or("BENCH_UNCONSTRAINED", 0u8) != 0 {
			tokio::task::unconstrained(ds.execute(&text, session, Some(vars))).await.unwrap()
		} else {
			ds.execute(&text, session, Some(vars)).await.unwrap()
		};
		let took = start.elapsed();
		let cpu1 = cpu_ns();
		COUNTING.store(false, Ordering::Relaxed);
		if i == 0 {
			for r in &res {
				if let Err(e) = &r.result {
					panic!("{name}: {e}");
				}
			}
		}
		drop(res);
		if counting {
			allocs += ALLOCS.load(Ordering::Relaxed) - before;
			samples.push(took.as_nanos() as f64 / 1000.0);
			cpu.push((cpu1 - cpu0) as f64 / 1000.0);
			timed += took;
		}
		i += 1;
	}
	phases::ACTIVE.store(false, Ordering::Relaxed);
	let n = samples.len() as f64;
	phases::report(name, n);
	if env_or("BENCH_WAKES", 0u8) != 0 {
		probe::report(i as f64);
	}
	phases::reset();
	samples.sort_by(f64::total_cmp);
	cpu.sort_by(f64::total_cmp);
	(
		samples.iter().sum::<f64>() / n,
		samples[samples.len() / 2],
		allocs as f64 / n,
		cpu[cpu.len() / 2],
	)
}

fn parse_us(name: &str) -> f64 {
	let (text, _) = request(name, 4242);
	let iters = 20_000;
	let start = Instant::now();
	for _ in 0..iters {
		std::hint::black_box(surrealdb_core::syn::parse(std::hint::black_box(&text)).unwrap());
	}
	start.elapsed().as_nanos() as f64 / 1000.0 / iters as f64
}

fn main() {
	let multi = std::env::var("BENCH_RT").is_ok_and(|rt| rt == "multi");
	MULTI.store(multi, Ordering::Relaxed);
	let runtime = if multi {
		tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build()
	} else {
		tokio::runtime::Builder::new_current_thread().enable_all().build()
	}
	.unwrap();
	runtime.block_on(async {
		if multi {
			// The server runs each connection's requests on a task of the runtime, not on the
			// thread that blocks on it.
			tokio::spawn(run_main()).await.unwrap()
		} else {
			run_main().await
		}
	});
}

async fn run_main() {
	let secs: f64 = env_or("BENCH_SECS", 2.0);
	if env_or("BENCH_PHASES", 0u8) != 0 {
		tracing::subscriber::set_global_default(phases::Collector).unwrap();
	}
	let ds = std::sync::Arc::new(
		Datastore::builder().without_maintenance_tasks().build_with_path("memory").await.unwrap(),
	);
	let session = Session::owner().with_ns("bench").with_db("bench");
	load(&ds, &session).await;
	if env_or("BENCH_SIZES", 0u8) != 0 {
		let (text, vars) = request("k1_param", 1);
		let fut = ds.execute(&text, &session, Some(vars));
		println!("execute future: {} bytes", std::mem::size_of_val(&fut));
		drop(fut);
	}
	if let Ok(only) = std::env::var("BENCH_ONLY") {
		let (mean, _, a, c) = case(&ds, &session, &only, secs).await;
		println!("{only}: mean {mean:.1} us, cpu median {c:.1} us, {a:.0} allocs");
		return;
	}
	let parse = env_or("BENCH_PARSE", 0u8) != 0;
	println!(
		"{:<16} {:>9} {:>9} {:>9} {:>8} {:>9}",
		"case", "mean us", "median", "cpu med", "allocs", "parse us"
	);
	for c in CASES {
		let (mean, median, a, cpu) = case(&ds, &session, c, secs).await;
		let p = if parse {
			parse_us(c)
		} else {
			f64::NAN
		};
		println!("{c:<16} {mean:>9.1} {median:>9.1} {cpu:>9.1} {a:>8.0} {p:>9.2}");
	}
}
