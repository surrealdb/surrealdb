//! The golden corpus check: every entry point's outcome on the corpus, frozen.
//!
//! Inputs are every statement of the language-test corpus and of
//! `tests/corpus{,_quirk}`, every suffix of each that starts at a token, and
//! one deterministic mutant of each. Each input goes through every entry
//! point the engine calls; the tree (as SurrealQL) or the rendered error is
//! folded into one digest per corpus file. `tests/golden/entries.txt` holds
//! the digests of the trees printed plainly and `tests/golden/entries-pretty.txt`
//! those of the trees pretty-printed; a file whose digest changed has changed
//! the behaviour of the parser, or of its printer, on some input drawn from it.
//!
//! ```text
//! cargo run --locked --profile golden -p surrealdb-expr --example golden [--bless] [--pretty] [--show FILE] [--check TEXT…]
//! ```
//!
//! With no flags it checks both digest files and fails if either differs.
//! `--bless` rewrites both. `--show FILE` prints every output for the inputs
//! drawn from `FILE`, to compare against a build from before a change.
//! `--check TEXT…` prints every entry point's output for each `TEXT`.
//! `--pretty` makes `--show` and `--check` print the pretty-printed trees.
//!
//! Corpus files are digested in parallel, one worker per available core.
//! Each file's digest depends only on its own inputs, taken in order, so the
//! digests do not depend on the number of workers or on the order they run.
//!
//! The digests were first taken from the previous parser's outputs, which
//! parser-next matched on these inputs except for those listed in
//! `tests/golden/known_differences.txt`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};

use surrealdb_expr::parser::entry::{self, Settings};
use surrealdb_expr::parser::{ParserConfig, ParserSettings, render, value};
use surrealdb_parser_next::lex::{Lexer, T};
use surrealdb_parser_next::{Limits, Scratch, parse_script};
use surrealdb_types::ToSql;

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

/// Replaces the random keys minted for access methods defined without one.
fn mask_keys(sql: &str) -> String {
	let mut out = String::with_capacity(sql.len());
	let mut rest = sql;
	while let Some(i) = rest.find('\'') {
		let after = &rest[i + 1..];
		let key = after.len() > 128
			&& after.as_bytes()[128] == b'\''
			&& after.as_bytes()[..128].iter().all(|b| b.is_ascii_alphanumeric());
		if key {
			out.push_str(&rest[..i]);
			out.push_str("'random'");
			rest = &after[129..];
		} else {
			out.push_str(&rest[..i + 1]);
			rest = after;
		}
	}
	out.push_str(rest);
	out
}

fn settings(s: &ParserSettings) -> Settings {
	s.next()
}

/// The outcome of one entry point on one input: its tree as SurrealQL, plain
/// and pretty-printed, or its rendered error, which both modes share.
struct Outcome {
	plain: String,
	pretty: Option<String>,
}

impl Outcome {
	fn get(&self, pretty: bool) -> &str {
		match (&self.pretty, pretty) {
			(Some(p), true) => p,
			_ => &self.plain,
		}
	}
}

/// The outcome of one entry point on `src`.
fn outcome<V: ToSql>(src: &str, r: Result<V, entry::Error>) -> Outcome {
	match r {
		Ok(v) => Outcome {
			plain: format!("ok {}", mask_keys(&v.to_sql())),
			pretty: Some(format!("ok {}", mask_keys(&v.to_sql_pretty()))),
		},
		Err(e) => Outcome {
			plain: format!("err {}", render(src, &e)),
			pretty: None,
		},
	}
}

type Run = fn(&str) -> Outcome;

/// Each entry point the engine calls, with the settings it calls it with.
fn entry_points() -> Vec<(&'static str, Run)> {
	fn all() -> ParserSettings {
		ParserSettings::all_features(&ParserConfig::default())
	}
	fn legacy() -> ParserSettings {
		ParserSettings {
			object_recursion_limit: usize::MAX,
			query_recursion_limit: usize::MAX,
			expr_recursion_limit: usize::MAX,
			legacy_strands: true,
			..Default::default()
		}
	}
	fn stored() -> ParserSettings {
		ParserSettings::STORED_TEXT
	}
	fn json(legacy_strands: bool) -> ParserSettings {
		ParserSettings {
			object_recursion_limit: if legacy_strands {
				0
			} else {
				ParserSettings::default().object_recursion_limit
			},
			query_recursion_limit: if legacy_strands {
				0
			} else {
				ParserSettings::default().query_recursion_limit
			},
			legacy_strands,
			json_string_escapes: true,
			import: false,
			..Default::default()
		}
	}
	vec![
		("expr", |s| outcome(s, entry::expr(s, &settings(&all()), false))),
		("expr_for_definition", |s| outcome(s, entry::expr(s, &settings(&stored()), true))),
		("expr_legacy_strand", |s| outcome(s, entry::expr(s, &settings(&legacy()), false))),
		("block_for_definition", |s| outcome(s, entry::block(s, &settings(&stored()), true))),
		("block", |s| outcome(s, entry::block(s, &settings(&all()), false))),
		("idiom", |s| outcome(s, entry::idiom(s, &settings(&ParserSettings::default()), false))),
		("idiom_for_definition", |s| outcome(s, entry::idiom(s, &settings(&stored()), true))),
		("kind", |s| outcome(s, entry::kind(s, &settings(&ParserSettings::default()), false))),
		("kind_for_definition", |s| outcome(s, entry::kind(s, &settings(&stored()), true))),
		("fields_for_definition", |s| outcome(s, entry::fields(s, &settings(&stored()), true))),
		("value", |s| outcome(s, value::value(s, &settings(&ParserSettings::default())))),
		("value_legacy_strand", |s| {
			let c = ParserConfig::default();
			let set = ParserSettings {
				object_recursion_limit: c.max_object_parsing_depth as usize,
				query_recursion_limit: c.max_query_parsing_depth as usize,
				legacy_strands: true,
				..Default::default()
			};
			outcome(s, value::value(s, &settings(&set)))
		}),
		("json", |s| outcome(s, value::json(s, &settings(&json(false))))),
		("json_legacy_strand", |s| outcome(s, value::json(s, &settings(&json(true))))),
		("record_id", |s| outcome(s, value::record_id(s, &settings(&ParserSettings::default())))),
		("query_gates_off", |s| {
			let set = ParserSettings::from_config(&ParserConfig::default());
			outcome(s, entry::query(s, &settings(&set)))
		}),
		("query_strict_ids", |s| {
			let set = ParserSettings {
				flexible_record_id: false,
				..all()
			};
			outcome(s, entry::query(s, &settings(&set)))
		}),
	]
}

/// FNV-1a, stable across builds and platforms.
fn fnv(h: &mut u64, bytes: &[u8]) {
	for &b in bytes {
		*h ^= b as u64;
		*h = h.wrapping_mul(0x0100_0000_01b3);
	}
}

/// One mutant of `text`, chosen by its own digest: one or two pieces of
/// syntax inserted, or a few bytes replaced or deleted.
fn mutant(text: &str) -> String {
	const PIECES: &[&str] = &[
		"(", ")", "[", "]", "{", "}", "<", ">", ";", ":", "::", ",", ".", "..", "'", "\"", "`",
		"$", "-", "+", "*", "/", "|", "=", "!", "?", "->", "<-", "0", "1.5", "1d", "x", " ",
		"NONE", "VALUE", "AS", "IF", "THEN", "END", "WHERE", "option<", "record", "s'", "u'",
	];
	let mut seed = 0xcbf2_9ce4_8422_2325;
	fnv(&mut seed, text.as_bytes());
	seed |= 1;
	let mut next = move || {
		seed ^= seed << 13;
		seed ^= seed >> 7;
		seed ^= seed << 17;
		seed
	};
	let mut t = text.to_owned();
	for _ in 0..1 + next() % 2 {
		let mut at = next() as usize % (t.len() + 1);
		while !t.is_char_boundary(at) {
			at -= 1;
		}
		let piece = PIECES[next() as usize % PIECES.len()];
		let mut end = (at + 1 + next() as usize % 4).min(t.len());
		while !t.is_char_boundary(end) {
			end += 1;
		}
		match next() % 3 {
			0 => t.insert_str(at, piece),
			1 => t.replace_range(at..end, piece),
			_ => t.replace_range(at..end, ""),
		}
	}
	t
}

/// Calls `f` with every suffix of every statement of the corpus file `src`
/// that starts at a token, in order.
fn suffixes(src: &str, mut f: impl FnMut(&str)) {
	let mut ast = surrealdb_parser_next::Ast::new();
	let mut scratch = Scratch::default();
	parse_script(src, Limits::default(), &mut ast, &mut scratch);
	for &(a, b) in &ast.spans {
		let text = &src[a as usize..b as usize];
		let mut lex = Lexer::new(text.as_bytes());
		loop {
			let t = lex.next_token();
			if t.kind == T::Eof {
				break;
			}
			if let Some(rest) = text.get(t.start as usize..) {
				f(rest);
			}
		}
	}
}

/// The inputs drawn from the corpus file `src`: each suffix and its mutant.
fn inputs(src: &str) -> Vec<String> {
	let mut out = Vec::new();
	suffixes(src, |rest| {
		out.push(rest.to_owned());
		out.push(mutant(rest));
	});
	out
}

/// An estimate of the time the inputs of `src` take: their total length.
fn cost(src: &str) -> usize {
	let mut n = 0;
	suffixes(src, |rest| n += rest.len());
	n
}

/// Runs `job(i)` for every `i` in `0..n` on up to `workers` threads, or on
/// the calling thread when `workers` is 1.
fn par_for(workers: usize, n: usize, job: impl Fn(usize) + Sync) {
	if workers <= 1 {
		(0..n).for_each(job);
		return;
	}
	let next = AtomicUsize::new(0);
	std::thread::scope(|s| {
		for _ in 0..workers.min(n) {
			// The legacy entry points parse without a depth limit, so a worker
			// gets a stack well above the main thread's.
			std::thread::Builder::new()
				.stack_size(64 << 20)
				.spawn_scoped(s, || {
					loop {
						let i = next.fetch_add(1, Ordering::Relaxed);
						if i >= n {
							break;
						}
						job(i);
					}
				})
				.expect("spawn a worker");
		}
	});
}

/// The plain and pretty digests of the corpus file `src`. Its inputs run on
/// `workers` threads, a batch at a time, and their outputs are folded in input
/// order on the calling thread. `show` prints each output as it is folded in.
fn digest(
	src: &str,
	points: &[(&'static str, Run)],
	workers: usize,
	show: Option<bool>,
) -> (u64, u64) {
	let mut plain = 0xcbf2_9ce4_8422_2325;
	let mut pretty = plain;
	// A batch's outputs are held until they are folded, so a batch stays small.
	for batch in inputs(src).chunks(workers * 16) {
		let outs: Vec<OnceLock<Vec<Outcome>>> = batch.iter().map(|_| OnceLock::new()).collect();
		par_for(workers, batch.len(), |i| {
			let set = outs[i].set(points.iter().map(|(_, run)| run(&batch[i])).collect());
			assert!(set.is_ok(), "par_for runs each input once");
		});
		for (input, outs) in batch.iter().zip(outs) {
			for ((name, _), out) in points.iter().zip(outs.into_inner().expect("ran every input")) {
				if let Some(p) = show {
					println!("{name} {input:?}\n  {:?}", out.get(p));
				}
				for (h, p) in [(&mut plain, false), (&mut pretty, true)] {
					fnv(h, name.as_bytes());
					fnv(h, out.get(p).as_bytes());
				}
			}
		}
	}
	(plain, pretty)
}

/// Compares `digests` against the golden file `name`, or rewrites it when
/// `bless`. Returns how many recorded files changed.
fn compare(name: &str, digests: &BTreeMap<String, u64>, bless: bool) -> usize {
	let golden =
		Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../parser-next/tests/golden/")).join(name);
	if bless {
		let mut text = String::new();
		for (f, h) in digests {
			text.push_str(&format!("{h:016x} {f}\n"));
		}
		std::fs::create_dir_all(golden.parent().expect("a parent")).expect("golden dir");
		std::fs::write(&golden, text).expect("write golden");
		println!("{name}: blessed {} files", digests.len());
		return 0;
	}
	let expected: BTreeMap<String, u64> = std::fs::read_to_string(&golden)
		.unwrap_or_else(|e| panic!("tests/golden/{name}: {e}; run with --bless to create it"))
		.lines()
		.filter_map(|l| {
			let (h, f) = l.split_once(' ')?;
			Some((f.to_owned(), u64::from_str_radix(h, 16).ok()?))
		})
		.collect();
	let mut changed = 0;
	for (f, h) in digests {
		match expected.get(f) {
			Some(e) if e == h => {}
			Some(_) => {
				changed += 1;
				println!("{name}: changed: {f}");
			}
			None => println!("{name}: new (bless to record): {f}"),
		}
	}
	println!("{name}: {} files, {changed} changed", digests.len());
	changed
}

fn main() {
	let args: Vec<String> = std::env::args().skip(1).collect();
	let pretty = args.iter().any(|arg| arg == "--pretty");
	if let Some(i) = args.iter().position(|a| a == "--check") {
		for text in &args[i + 1..] {
			for (name, run) in entry_points() {
				println!("{name} {text:?}\n  {:?}", run(text).get(pretty));
			}
		}
		return;
	}
	let bless = args.iter().any(|a| a == "--bless");
	let show = args.iter().position(|a| a == "--show").map(|i| args[i + 1].clone());
	let root = Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../.."));
	let mut files = Vec::new();
	for dir in [
		"language-tests/tests",
		"surrealdb/parser-next/tests/corpus",
		"surrealdb/parser-next/tests/corpus_quirk",
	] {
		collect(&root.join(dir), &mut files);
	}
	files.sort();
	let rel = |f: &Path| f.strip_prefix(root).unwrap_or(f).to_string_lossy().replace('\\', "/");
	if let Some(show) = &show {
		files.retain(|f| rel(f) == *show);
	}
	let sources: Vec<(String, String)> =
		files.iter().filter_map(|f| Some((rel(f), std::fs::read_to_string(f).ok()?))).collect();
	let costs: Vec<usize> = sources.iter().map(|(_, src)| cost(src)).collect();
	let total: usize = costs.iter().sum();
	let workers = std::thread::available_parallelism().map_or(1, |n| n.get());
	// A few files cost far more than the rest (every suffix of a long statement
	// is an input). Each of those has its inputs spread over every worker in
	// turn; the rest run a file per worker, the costliest first, so that no
	// single file is left running alone at the end.
	let mut order: Vec<usize> = (0..sources.len()).collect();
	order.sort_by_key(|&i| std::cmp::Reverse(costs[i]));
	let split = order.partition_point(|&i| costs[i] * workers * 8 > total);
	let (heavy, light) = order.split_at(split);
	let points = entry_points();
	let show = show.as_ref().map(|_| pretty);
	let digests = Mutex::new(BTreeMap::new());
	for &i in heavy {
		let d = digest(&sources[i].1, &points, workers, show);
		digests.lock().expect("digests").insert(sources[i].0.clone(), d);
	}
	par_for(workers, light.len(), |j| {
		let (rel, src) = &sources[light[j]];
		let d = digest(src, &points, 1, show);
		digests.lock().expect("digests").insert(rel.clone(), d);
	});
	if show.is_some() {
		return;
	}
	let digests = digests.into_inner().expect("digests");
	let plain = digests.iter().map(|(f, d)| (f.clone(), d.0)).collect();
	let pretty = digests.iter().map(|(f, d)| (f.clone(), d.1)).collect();
	let changed =
		compare("entries.txt", &plain, bless) + compare("entries-pretty.txt", &pretty, bless);
	if changed > 0 {
		std::process::exit(1);
	}
}
