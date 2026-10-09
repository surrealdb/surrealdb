//! The golden corpus check: every entry point's outcome on the corpus, frozen.
//!
//! Inputs are every statement of the language-test corpus and of
//! `tests/corpus{,_quirk}`, every suffix of each that starts at a token, and
//! one deterministic mutant of each. Each input goes through every entry
//! point the engine calls; the tree (as SurrealQL) or the rendered error is
//! folded into one digest per corpus file. `tests/golden/entries.txt` holds
//! the digests; a file whose digest changed has changed the behaviour of the
//! parser on some input drawn from it.
//!
//! ```text
//! cargo run --locked --release -p surrealdb-parser-next --example golden [--bless] [--show FILE] [--check TEXT…]
//! ```
//!
//! `--bless` rewrites the digests. `--show FILE` prints every output for the
//! inputs drawn from `FILE`, to compare against a build from before a change.
//! `--check TEXT…` prints every entry point's output for each `TEXT`.
//!
//! The digests were first taken from the previous parser's outputs, which
//! parser-next matched on these inputs except for those listed in
//! `tests/golden/known_differences.txt`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use surrealdb_parser_next::lex::{Lexer, T};
use surrealdb_parser_next::{Limits, Scratch, parse_script};
use surrealdb_parser_next_lower::entry::{self, Settings};
use surrealdb_parser_next_lower::{ParserConfig, ParserSettings, render, value};
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

/// The outcome of one entry point on `src`: its tree as SurrealQL, or its
/// rendered error.
fn outcome<V: ToSql>(src: &str, r: Result<V, entry::Error>) -> String {
	match r {
		Ok(v) => format!("ok {}", mask_keys(&v.to_sql())),
		Err(e) => format!("err {}", render(src, &e)),
	}
}

type Run = fn(&str) -> String;

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

/// The inputs drawn from the corpus file `src`.
fn inputs(src: &str) -> Vec<String> {
	let mut ast = surrealdb_parser_next::Ast::new();
	let mut scratch = Scratch::default();
	parse_script(src, Limits::default(), &mut ast, &mut scratch);
	let mut out = Vec::new();
	for &(a, b) in &ast.spans {
		let text = &src[a as usize..b as usize];
		let mut lex = Lexer::new(text.as_bytes());
		loop {
			let t = lex.next_token();
			if t.kind == T::Eof {
				break;
			}
			if let Some(rest) = text.get(t.start as usize..) {
				out.push(rest.to_owned());
				out.push(mutant(rest));
			}
		}
	}
	out
}

fn main() {
	let args: Vec<String> = std::env::args().skip(1).collect();
	if let Some(i) = args.iter().position(|a| a == "--check") {
		for text in &args[i + 1..] {
			for (name, run) in entry_points() {
				println!("{name} {text:?}\n  {:?}", run(text));
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
	let points = entry_points();
	let mut digests = BTreeMap::new();
	for f in &files {
		let Ok(src) = std::fs::read_to_string(f) else {
			continue;
		};
		let rel = f.strip_prefix(root).unwrap_or(f).to_string_lossy().replace('\\', "/");
		let mut h = 0xcbf2_9ce4_8422_2325;
		for input in inputs(&src) {
			for (name, run) in &points {
				let out = run(&input);
				if show.as_deref() == Some(rel.as_str()) {
					println!("{name} {input:?}\n  {out:?}");
				}
				fnv(&mut h, name.as_bytes());
				fnv(&mut h, out.as_bytes());
			}
		}
		digests.insert(rel, h);
	}
	if show.is_some() {
		return;
	}
	let golden = Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/golden/entries.txt"));
	if bless {
		let mut text = String::new();
		for (f, h) in &digests {
			text.push_str(&format!("{h:016x} {f}\n"));
		}
		std::fs::create_dir_all(golden.parent().expect("a parent")).expect("golden dir");
		std::fs::write(golden, text).expect("write golden");
		println!("blessed {} files", digests.len());
		return;
	}
	let expected: BTreeMap<String, u64> = std::fs::read_to_string(golden)
		.expect("tests/golden/entries.txt; run with --bless to create it")
		.lines()
		.filter_map(|l| {
			let (h, f) = l.split_once(' ')?;
			Some((f.to_owned(), u64::from_str_radix(h, 16).ok()?))
		})
		.collect();
	let mut changed = 0;
	for (f, h) in &digests {
		match expected.get(f) {
			Some(e) if e == h => {}
			Some(_) => {
				changed += 1;
				println!("changed: {f}");
			}
			None => println!("new (bless to record): {f}"),
		}
	}
	println!("{} files, {changed} changed", digests.len());
	if changed > 0 {
		std::process::exit(1);
	}
}
