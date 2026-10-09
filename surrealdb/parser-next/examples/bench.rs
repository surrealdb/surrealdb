//! Measures the new parser against `surrealdb-syn` (the parser in use) and
//! `surrealdb-parser` (the arena-based successor in the tree) on the corpus
//! statements all three parse and that lower to `surrealdb-syn`'s tree.
//!
//! ```text
//! cargo run --locked --release -p surrealdb-parser-next --example bench
//! ```
//!
//! Reports throughput, heap allocations per statement (a counting global
//! allocator), and the native stack each parser needs on deeply nested input
//! (found by bisecting the stack size of a child process).

// The successor parser's async state machines need it when monomorphised here.
#![recursion_limit = "256"]

use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;

use surrealdb_parser_next::{Ast, Limits, Scratch, Step, StreamParser, Tag, parse_script};
use surrealdb_parser_next_lower::{CHECK_NAMES, Lower};
use surrealdb_syn::ParserSettings;
use surrealdb_syn::config::ParserConfig;
use web_time::Instant;

struct Counting;

static ALLOCS: AtomicUsize = AtomicUsize::new(0);
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

// SAFETY: forwards to the system allocator and only adds counters.
unsafe impl GlobalAlloc for Counting {
	unsafe fn alloc(&self, l: Layout) -> *mut u8 {
		ALLOCS.fetch_add(1, Relaxed);
		let live = LIVE.fetch_add(l.size(), Relaxed) + l.size();
		PEAK.fetch_max(live, Relaxed);
		// SAFETY: same contract as the caller's.
		unsafe { System.alloc(l) }
	}
	unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
		LIVE.fetch_sub(l.size(), Relaxed);
		// SAFETY: same contract as the caller's.
		unsafe { System.dealloc(p, l) }
	}
	unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
		ALLOCS.fetch_add(1, Relaxed);
		let live = LIVE.fetch_add(n, Relaxed) + n - l.size();
		LIVE.fetch_sub(l.size(), Relaxed);
		PEAK.fetch_max(live, Relaxed);
		// SAFETY: same contract as the caller's.
		unsafe { System.realloc(p, l, n) }
	}
}

#[global_allocator]
static GLOBAL: Counting = Counting;

fn collect(dir: &Path, out: &mut Vec<PathBuf>) {
	let Ok(rd) = std::fs::read_dir(dir) else {
		return;
	};
	for e in rd.flatten() {
		let p = e.path();
		if p.is_dir() {
			collect(&p, out);
		} else if p.extension().is_some_and(|x| x == "surql") {
			out.push(p);
		}
	}
}

fn old(s: &str) -> bool {
	surrealdb_syn::parse(black_box(s)).is_ok()
}

struct V3 {
	ast: ast::Ast,
}

impl V3 {
	fn new() -> Self {
		V3 {
			ast: ast::Ast::empty(),
		}
	}
	fn parse(&mut self, s: &str, depth_limit: usize) -> bool {
		let config = parser::Config {
			depth_limit,
			..Default::default()
		};
		parser::Parser::enter_parse_reuse::<ast::Query>(black_box(s), &mut self.ast, config).is_ok()
	}
}

/// The statements every parser accepts and the new parser lowers to the
/// same tree as `surrealdb-syn`.
fn corpus() -> Vec<String> {
	let root = concat!(env!("CARGO_MANIFEST_DIR"), "/../../language-tests/tests");
	let mut files = Vec::new();
	collect(Path::new(root), &mut files);
	files.sort();
	let mut out = Vec::new();
	let mut ast = Ast::new();
	let mut scratch = Scratch::default();
	let mut v3 = V3::new();
	for f in files {
		let Ok(src) = std::fs::read_to_string(&f) else {
			continue;
		};
		parse_script(&src, Limits::default(), &mut ast, &mut scratch);
		for (i, &s) in ast.stmts.iter().enumerate() {
			if ast.node(s).tag == Tag::Error {
				continue;
			}
			let (a, b) = ast.spans[i];
			let text = &src[a as usize..b as usize];
			let new = Lower::new(&ast, &src).top(s);
			let Ok(old_ast) = surrealdb_syn::parse(text) else {
				continue;
			};
			let same = matches!((&new, old_ast.expressions.first()), (Ok(n), Some(o)) if n == o);
			if same && v3.parse(text, 1024) {
				out.push(text.to_owned());
			}
		}
	}
	out
}

fn time<F: FnMut(&str)>(stmts: &[String], rounds: u32, mut f: F) -> f64 {
	for s in stmts {
		f(s);
	}
	let t = Instant::now();
	for _ in 0..rounds {
		for s in stmts {
			f(s);
		}
	}
	t.elapsed().as_secs_f64() / rounds as f64
}

fn allocs<F: FnMut(&str)>(stmts: &[String], mut f: F) -> f64 {
	for s in stmts {
		f(s);
	}
	let before = ALLOCS.load(Relaxed);
	for s in stmts {
		f(s);
	}
	(ALLOCS.load(Relaxed) - before) as f64 / stmts.len() as f64
}

/// Nested inputs for the stack probe, at nesting depth `d`.
fn nested(family: &str, d: usize) -> String {
	match family {
		"parens" => format!("{}1{}", "(".repeat(d), ")".repeat(d)),
		"arrays" => format!("{}1{}", "[".repeat(d), "]".repeat(d)),
		"objects" => format!("{}1{}", "{a:".repeat(d), "}".repeat(d)),
		"prefix" => format!("{}1", "-(".repeat(d)) + &")".repeat(d),
		"calls" => format!("{}1{}", "math::abs(".repeat(d), ")".repeat(d)),
		"subqueries" => format!("{}1{}", "(SELECT * FROM ".repeat(d), ")".repeat(d)),
		"lookups" => format!("a{}{}", "->(b WHERE a".repeat(d), ")".repeat(d)),
		"lookup-aliases" => format!("a{}{}", "->(b AS c".repeat(d), ")".repeat(d)),
		"destructures" => format!("a{}{}", ".{a".repeat(d), "}".repeat(d)),
		"if-blocks" => format!("{}1{}", "IF true {".repeat(d), "}".repeat(d)),
		"defines" => format!("{}1{}", "DEFINE FUNCTION fn::f() {".repeat(d), "}".repeat(d)),
		_ => unreachable!(),
	}
}

/// Runs one parse in this process on a thread with `stack` bytes of stack.
/// Exits 0 on success, 1 on a parse error; an overflow kills the process.
fn probe(parser: &str, family: &str, depth: usize, stack: usize) -> ! {
	let src = nested(family, depth);
	let parser = parser.to_owned();
	let ok = std::thread::Builder::new()
		.stack_size(stack)
		.spawn(move || match parser.as_str() {
			"new-default" | "new-stored" => {
				let mut ast = Ast::new();
				let mut scratch = Scratch::default();
				let limits = if parser == "new-default" {
					Limits::default()
				} else {
					Limits::stored_text()
				};
				parse_script(&src, limits, &mut ast, &mut scratch);
				scratch.diags.is_empty()
			}
			"new" => {
				let mut ast = Ast::new();
				let mut scratch = Scratch::default();
				let limits = Limits {
					expr_depth: u16::MAX,
					query_depth: u16::MAX,
					object_depth: u16::MAX,
					strict_record_ids: false,
				};
				parse_script(&src, limits, &mut ast, &mut scratch);
				scratch.diags.is_empty()
			}
			"syn" => {
				let settings = ParserSettings {
					object_recursion_limit: 1 << 20,
					query_recursion_limit: 1 << 20,
					expr_recursion_limit: 1 << 20,
					..ParserSettings::all_features(&ParserConfig::default())
				};
				surrealdb_syn::parse_with_settings(src.as_bytes(), settings, |p| p.parse_query())
					.is_ok()
			}
			"v3" => V3::new().parse(&src, 1 << 20),
			_ => unreachable!(),
		})
		.expect("spawn")
		.join()
		.unwrap_or(false);
	std::process::exit(if ok {
		0
	} else {
		1
	})
}

/// The smallest stack, in KiB, on which `parser` parses `family` at `depth`.
fn min_stack_kib(parser: &str, family: &str, depth: usize) -> Option<usize> {
	let exe = std::env::current_exe().expect("exe");
	let run = |kib: usize| {
		Command::new(&exe)
			.args(["probe", parser, family, &depth.to_string(), &(kib * 1024).to_string()])
			.output()
			.map(|o| o.status.code())
			.ok()
			.flatten()
	};
	// 16 KiB is the smallest thread stack the platform grants.
	let (mut lo, mut hi) = (16usize, 64 * 1024usize);
	match run(hi) {
		Some(0) => {}
		_ => return None,
	}
	if run(lo) == Some(0) {
		return Some(lo);
	}
	while hi - lo > 4 {
		let mid = (lo + hi) / 2;
		if run(mid) == Some(0) {
			hi = mid;
		} else {
			lo = mid;
		}
	}
	Some(hi)
}

/// Statements of the corpus the new parser parses, terminated by `;\n`: the
/// material the synthetic import repeats.
fn import_material() -> Vec<u8> {
	let root = concat!(env!("CARGO_MANIFEST_DIR"), "/../../language-tests/tests");
	let mut files = Vec::new();
	collect(Path::new(root), &mut files);
	files.sort();
	let mut out = Vec::new();
	let mut ast = Ast::new();
	let mut scratch = Scratch::default();
	for f in files {
		let Ok(src) = std::fs::read_to_string(&f) else {
			continue;
		};
		parse_script(&src, Limits::default(), &mut ast, &mut scratch);
		for (i, &s) in ast.stmts.iter().enumerate() {
			if ast.node(s).tag != Tag::Error {
				let (a, b) = ast.spans[i];
				out.extend_from_slice(&src.as_bytes()[a as usize..b as usize]);
				out.extend_from_slice(b";\n");
			}
		}
	}
	out
}

/// Streams a synthetic import of `mb` MiB, generated on the fly in 64 KiB
/// chunks, then parses a smaller in-memory copy whole, reporting throughput
/// and peak live heap for both.
fn import(mb: usize, whole: bool) {
	let material = import_material();
	let total = mb << 20;
	let chunk = 64 << 10;
	let mut sp = StreamParser::new(Limits::default(), 64 << 20);
	PEAK.store(LIVE.load(Relaxed), Relaxed);
	let base = LIVE.load(Relaxed);
	let (mut fed, mut at, mut stmts, mut failed) = (0usize, 0usize, 0u64, 0u64);
	let t = Instant::now();
	loop {
		if fed < total {
			let mut n = chunk.min(total - fed);
			let mut buf = [0u8; 64 << 10];
			let mut filled = 0;
			while filled < n {
				let k = (n - filled).min(material.len() - at);
				buf[filled..filled + k].copy_from_slice(&material[at..at + k]);
				filled += k;
				at = (at + k) % material.len();
			}
			// Stop the last chunk on a character boundary.
			while fed + n == total && n > 0 && (buf[n - 1] & 0xC0) == 0x80 {
				n -= 1;
			}
			sp.feed(&buf[..n]);
			fed += chunk.min(total - fed);
		} else {
			sp.finish();
		}
		loop {
			match sp.next_statement() {
				Ok(Step::Statement(s)) => {
					stmts += 1;
					failed += (s.ast.node(s.root).tag == Tag::Error) as u64;
				}
				Ok(Step::NeedMore) => break,
				Ok(Step::End) | Err(_) => {
					let secs = t.elapsed().as_secs_f64();
					println!(
						"streamed import: {mb} MiB, {stmts} statements ({failed} failed, from cut ends) in {secs:.2}s: {:.0} MB/s, {:.2}M statements/s, peak live heap {} KiB",
						total as f64 / secs / 1e6,
						stmts as f64 / secs / 1e6,
						(PEAK.load(Relaxed) - base) >> 10
					);
					if !whole {
						return;
					}
					let whole_mb = mb.min(256);
					let mut text = Vec::with_capacity(whole_mb << 20);
					while text.len() < whole_mb << 20 {
						text.extend_from_slice(&material);
					}
					let text = String::from_utf8(text).expect("utf-8");
					let mut ast = Ast::new();
					let mut scratch = Scratch::default();
					let base = LIVE.load(Relaxed);
					PEAK.store(base, Relaxed);
					let t = Instant::now();
					parse_script(&text, Limits::default(), &mut ast, &mut scratch);
					let secs = t.elapsed().as_secs_f64();
					println!(
						"whole-input parse: {} MiB, {} statements in {secs:.2}s: {:.0} MB/s, peak live heap {} KiB (excluding the input)",
						text.len() >> 20,
						ast.stmts.len(),
						text.len() as f64 / secs / 1e6,
						(PEAK.load(Relaxed) - base) >> 10
					);
					return;
				}
			}
		}
	}
}

/// The native stack the new parser needs at the deepest input its budgets
/// accept, for the constructs that recurse natively.
fn stack_only() {
	println!("\nminimum native stack (KiB) at the deepest input the budgets accept");
	println!("{:16} {:>6} {:>10} {:>6} {:>10}", "family", "depth", "default", "depth", "stored");
	for (family, deep, stored) in [
		("subqueries", 10, 200),
		("lookups", 20, 400),
		("lookup-aliases", 127, 511),
		("destructures", 126, 510),
		("if-blocks", 20, 400),
		("defines", 20, 400),
	] {
		let cell = |p: &str, d: usize| match min_stack_kib(p, family, d) {
			Some(k) => k.to_string(),
			None => "fail".to_owned(),
		};
		println!(
			"{family:16} {deep:>6} {:>10} {stored:>6} {:>10}",
			cell("new-default", deep),
			cell("new-stored", stored)
		);
	}
}

fn main() {
	let args: Vec<String> = std::env::args().skip(1).collect();
	if args.first().map(String::as_str) == Some("probe") {
		probe(&args[1], &args[2], args[3].parse().expect("depth"), args[4].parse().expect("stack"));
	}
	if args.first().map(String::as_str) == Some("stack") {
		stack_only();
		return;
	}
	if args.first().map(String::as_str) == Some("import") {
		let whole = !args.iter().any(|a| a == "--stream-only");
		import(args.get(1).and_then(|a| a.parse().ok()).unwrap_or(1024), whole);
		return;
	}

	let stmts = corpus();
	let bytes: usize = stmts.iter().map(String::len).sum();
	println!("corpus: {} statements, {} bytes", stmts.len(), bytes);

	CHECK_NAMES.with(|c| c.set(false));
	let rounds = 20;
	let mut ast = Ast::new();
	let mut scratch = Scratch::default();
	let mut v3 = V3::new();
	let rows: [(&str, f64); 4] = [
		(
			"surrealdb-syn parse",
			time(&stmts, rounds, |s| {
				black_box(old(s));
			}),
		),
		(
			"surrealdb-parser (v3) parse",
			time(&stmts, rounds, |s| {
				black_box(v3.parse(s, 1024));
			}),
		),
		(
			"parser-next parse",
			time(&stmts, rounds, |s| {
				parse_script(black_box(s), Limits::default(), &mut ast, &mut scratch);
			}),
		),
		(
			"parser-next parse + lower to sql AST",
			time(&stmts, rounds, |s| {
				parse_script(black_box(s), Limits::default(), &mut ast, &mut scratch);
				let l = Lower::new(&ast, s);
				black_box(l.top(ast.stmts[0]).is_ok());
			}),
		),
	];
	println!("\n{:40} {:>10} {:>12} {:>9}", "throughput", "MB/s", "stmts/s", "vs syn");
	let base = rows[0].1;
	for (name, secs) in rows {
		println!(
			"{name:40} {:>10.1} {:>12.0} {:>8.2}x",
			bytes as f64 / secs / 1e6,
			stmts.len() as f64 / secs,
			base / secs
		);
	}

	println!("\n{:40} {:>10}", "heap allocations per statement", "allocs");
	let a_old = allocs(&stmts, |s| {
		black_box(old(s));
	});
	let a_v3_cold = allocs(&stmts, |s| {
		black_box(V3::new().parse(s, 1024));
	});
	let a_v3 = allocs(&stmts, |s| {
		black_box(v3.parse(s, 1024));
	});
	let a_cold = allocs(&stmts, |s| {
		let mut ast = Ast::new();
		let mut scratch = Scratch::default();
		parse_script(black_box(s), Limits::default(), &mut ast, &mut scratch);
	});
	let a_warm = allocs(&stmts, |s| {
		parse_script(black_box(s), Limits::default(), &mut ast, &mut scratch);
	});
	let a_lower = allocs(&stmts, |s| {
		parse_script(black_box(s), Limits::default(), &mut ast, &mut scratch);
		let l = Lower::new(&ast, s);
		black_box(l.top(ast.stmts[0]).is_ok());
	});
	for (name, v) in [
		("surrealdb-syn parse (incl. drop)", a_old),
		("surrealdb-parser (v3), fresh arena", a_v3_cold),
		("surrealdb-parser (v3), reused arena", a_v3),
		("parser-next, fresh arena", a_cold),
		("parser-next, reused arena", a_warm),
		("parser-next + lower to sql AST", a_lower),
	] {
		println!("{name:40} {v:>10.2}");
	}

	println!("\nminimum native stack (KiB) on nested input, limits lifted");
	println!("{:12} {:>6} {:>10} {:>10} {:>10}", "family", "depth", "syn", "v3", "next");
	for family in ["parens", "arrays", "objects", "prefix", "calls", "subqueries"] {
		for depth in [100usize, 1_000, 10_000] {
			if family == "subqueries" && depth > 1_000 {
				continue;
			}
			let cell = |p: &str| match min_stack_kib(p, family, depth) {
				Some(k) => k.to_string(),
				None => "fail".to_owned(),
			};
			println!(
				"{family:12} {depth:>6} {:>10} {:>10} {:>10}",
				cell("syn"),
				cell("v3"),
				cell("new")
			);
		}
	}

	stack_only();

	println!("\npeak heap bytes on nested parens, limits lifted");
	for depth in [1_000usize, 10_000] {
		let src = nested("parens", depth);
		let mut row = Vec::new();
		for p in ["syn", "v3", "new"] {
			PEAK.store(LIVE.load(Relaxed), Relaxed);
			let base = LIVE.load(Relaxed);
			let src = src.clone();
			let p2 = p.to_owned();
			std::thread::Builder::new()
				.stack_size(64 << 20)
				.spawn(move || match p2.as_str() {
					"syn" => {
						let settings = ParserSettings {
							object_recursion_limit: 1 << 20,
							query_recursion_limit: 1 << 20,
							expr_recursion_limit: 1 << 20,
							..ParserSettings::default()
						};
						let _ = black_box(surrealdb_syn::parse_with_settings(
							src.as_bytes(),
							settings,
							|p| p.parse_query(),
						));
					}
					"v3" => {
						black_box(V3::new().parse(&src, 1 << 20));
					}
					_ => {
						let mut ast = Ast::new();
						let mut scratch = Scratch::default();
						let limits = Limits {
							expr_depth: u16::MAX,
							query_depth: u16::MAX,
							object_depth: u16::MAX,
							strict_record_ids: false,
						};
						parse_script(&src, limits, &mut ast, &mut scratch);
						black_box(&ast);
					}
				})
				.expect("spawn")
				.join()
				.expect("join");
			row.push(format!("{p}={}", PEAK.load(Relaxed).saturating_sub(base)));
		}
		println!("  depth {depth:>6}: {}", row.join("  "));
	}
}
