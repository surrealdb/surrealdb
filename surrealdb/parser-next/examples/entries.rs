//! Differential test of `syn`'s entry points other than whole queries, and of
//! the parser settings, against parser-next and its lowering.
//!
//! Inputs are every corpus statement and every suffix of it that starts at a
//! token, so each entry point sees realistic text at many positions. For
//! each input and entry point both sides must produce equal trees or both
//! reject.
//!
//! ```text
//! cargo run --locked --release -p surrealdb-parser-next --example entries [-v] [--only NAME] [--fuzz N]
//! ```

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use surrealdb_parser_next::lex::{Lexer, T};
use surrealdb_parser_next::{Limits, Scratch, parse_script};
use surrealdb_parser_next_lower::entry::{self, Settings};
use surrealdb_parser_next_lower::{Options, value};
use surrealdb_syn::{ParserConfig, ParserSettings};
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

/// Replaces the random keys `syn` mints for access methods defined without
/// one.
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

#[derive(Debug)]
#[allow(dead_code, reason = "fields are printed through Debug")]
enum Outcome {
	Equal,
	BothReject,
	Mismatch(String, String),
	NewAcceptsInvalid(String),
	NewRejectsValid(String),
}

fn judge<T: PartialEq + ToSql>(
	new: Result<T, entry::Error>,
	old: Result<T, surrealdb_syn::ParseError>,
) -> Outcome {
	match (new, old) {
		(Ok(n), Ok(o)) => {
			if n == o {
				return Outcome::Equal;
			}
			let (ns, os) = (mask_keys(&n.to_sql()), mask_keys(&o.to_sql()));
			if ns == os {
				Outcome::Equal
			} else {
				Outcome::Mismatch(os, ns)
			}
		}
		(Err(_), Err(_)) => Outcome::BothReject,
		(Ok(n), Err(_)) => Outcome::NewAcceptsInvalid(n.to_sql()),
		(Err(e), Ok(o)) => Outcome::NewRejectsValid(format!("{e:?} / syn: {}", o.to_sql())),
	}
}

fn limits(s: &ParserSettings) -> Limits {
	let c = |v: usize| v.min(u16::MAX as usize) as u16;
	Limits {
		expr_depth: c(s.expr_recursion_limit),
		query_depth: c(s.query_recursion_limit),
		object_depth: c(s.object_recursion_limit),
		strict_record_ids: !s.flexible_record_id,
	}
}

fn settings(s: &ParserSettings) -> Settings {
	Settings {
		limits: limits(s),
		opts: Options {
			legacy_strands: s.legacy_strands,
			files: s.files_enabled,
			surrealism: s.surrealism_enabled,
		},
	}
}

type Check = fn(&str) -> Outcome;

/// Each entry point `syn` exposes, as the engine calls it.
fn checks() -> Vec<(&'static str, Check)> {
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
	vec![
		("expr", |s| judge(entry::expr(s, &settings(&all()), false), surrealdb_syn::expr(s))),
		("expr_for_definition", |s| {
			judge(
				entry::expr(s, &settings(&ParserSettings::STORED_TEXT), true),
				surrealdb_syn::expr_for_definition(s),
			)
		}),
		("expr_legacy_strand", |s| {
			judge(entry::expr(s, &settings(&legacy()), false), surrealdb_syn::expr_legacy_strand(s))
		}),
		("block_for_definition", |s| {
			judge(
				entry::block(s, &settings(&ParserSettings::STORED_TEXT), true),
				surrealdb_syn::block_for_definition(s),
			)
		}),
		("block", |s| {
			judge(
				entry::block(s, &settings(&all()), false),
				surrealdb_syn::block_with_settings(s, all(), false),
			)
		}),
		("idiom", |s| {
			judge(
				entry::idiom(s, &settings(&ParserSettings::default()), false),
				surrealdb_syn::idiom(s),
			)
		}),
		("idiom_for_definition", |s| {
			judge(
				entry::idiom(s, &settings(&ParserSettings::STORED_TEXT), true),
				surrealdb_syn::idiom_for_definition(s),
			)
		}),
		("kind", |s| {
			judge(
				entry::kind(s, &settings(&ParserSettings::default()), false),
				surrealdb_syn::kind(s),
			)
		}),
		("kind_for_definition", |s| {
			judge(
				entry::kind(s, &settings(&ParserSettings::STORED_TEXT), true),
				surrealdb_syn::kind_for_definition(s),
			)
		}),
		("fields_for_definition", |s| {
			judge(
				entry::fields(s, &settings(&ParserSettings::STORED_TEXT), true),
				surrealdb_syn::fields_for_definition(s),
			)
		}),
		("value", |s| {
			judge(value::value(s, &settings(&ParserSettings::default())), surrealdb_syn::value(s))
		}),
		("value_legacy_strand", |s| {
			let c = ParserConfig::default();
			let set = ParserSettings {
				object_recursion_limit: c.max_object_parsing_depth as usize,
				query_recursion_limit: c.max_query_parsing_depth as usize,
				legacy_strands: true,
				..Default::default()
			};
			judge(value::value(s, &settings(&set)), surrealdb_syn::value_legacy_strand(s, &c))
		}),
		("json", |s| {
			let set = ParserSettings {
				json_string_escapes: true,
				..Default::default()
			};
			judge(
				value::json(s, &settings(&set)),
				surrealdb_syn::parse_with_settings(s.as_bytes(), set, |p| p.parse_json()),
			)
		}),
		("json_legacy_strand", |s| {
			let set = ParserSettings {
				object_recursion_limit: 0,
				query_recursion_limit: 0,
				legacy_strands: true,
				json_string_escapes: true,
				..Default::default()
			};
			judge(
				value::json(s, &settings(&set)),
				surrealdb_syn::parse_with_settings(s.as_bytes(), set, |p| p.parse_json()),
			)
		}),
		("record_id", |s| {
			judge(
				value::record_id(s, &settings(&ParserSettings::default())),
				surrealdb_syn::record_id(s),
			)
		}),
		("query_gates_off", |s| {
			let set = ParserSettings::from_config(&ParserConfig::default());
			judge(
				entry::query(s, &settings(&set)),
				surrealdb_syn::parse_with_settings(s.as_bytes(), set, |p| p.parse_query()),
			)
		}),
		("query_strict_ids", |s| {
			let set = ParserSettings {
				flexible_record_id: false,
				..all()
			};
			judge(
				entry::query(s, &settings(&set)),
				surrealdb_syn::parse_with_settings(s.as_bytes(), set, |p| p.parse_query()),
			)
		}),
	]
}

/// `n` mutants of each input: one or two pieces of syntax inserted, or a
/// few bytes replaced or deleted. `FUZZ_SEED` picks the sequence.
fn mutants(inputs: &[String], n: usize) -> Vec<String> {
	const PIECES: &[&str] = &[
		"(", ")", "[", "]", "{", "}", "<", ">", ";", ":", "::", ",", ".", "..", "'", "\"", "`",
		"$", "-", "+", "*", "/", "|", "=", "!", "?", "->", "<-", "0", "1.5", "1d", "x", " ",
		"NONE", "VALUE", "AS", "IF", "THEN", "END", "WHERE", "option<", "record", "s'", "u'",
	];
	let mut seed: u64 = std::env::var("FUZZ_SEED")
		.ok()
		.and_then(|v| v.parse().ok())
		.unwrap_or(0x1234_5678_9abc_def1)
		| 1;
	let mut next = move || {
		seed ^= seed << 13;
		seed ^= seed >> 7;
		seed ^= seed << 17;
		seed
	};
	let mut out = Vec::with_capacity(inputs.len() * n);
	for text in inputs {
		for _ in 0..n {
			let mut t = text.clone();
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
			out.push(t);
		}
	}
	out
}

fn main() {
	let args: Vec<String> = std::env::args().skip(1).collect();
	let verbose = args.iter().any(|a| a == "-v");
	let only = args.iter().position(|a| a == "--only").map(|i| args[i + 1].clone());
	if let Some(i) = args.iter().position(|a| a == "--check") {
		for text in &args[i + 1..] {
			for (name, check) in checks() {
				if only.as_deref().is_none_or(|o| o == name) {
					println!("{name}: {text:?}: {:?}", check(text));
				}
			}
		}
		return;
	}
	let root = concat!(env!("CARGO_MANIFEST_DIR"), "/../../language-tests/tests");
	let mut files = Vec::new();
	collect(Path::new(root), &mut files);
	files.sort();

	// Statements, then every token-start suffix of each.
	let mut inputs: Vec<String> = Vec::new();
	let mut ast = surrealdb_parser_next::Ast::new();
	let mut scratch = Scratch::default();
	for f in &files {
		let Ok(src) = std::fs::read_to_string(f) else {
			continue;
		};
		parse_script(&src, Limits::default(), &mut ast, &mut scratch);
		for &(a, b) in &ast.spans {
			let text = &src[a as usize..b as usize];
			let mut lex = Lexer::new(text.as_bytes());
			loop {
				let t = lex.next_token();
				if t.kind == T::Eof {
					break;
				}
				if let Some(rest) = text.get(t.start as usize..) {
					inputs.push(rest.to_owned());
				}
			}
		}
	}
	// Strings `legacy_strands` reinterprets, and constructs behind feature
	// gates, which the corpus has few of.
	for e in [
		"'a:b'",
		"\"a:1\"",
		"'a:1..2'",
		"'a:⟨x y⟩'",
		"'a:[1, 2]'",
		"'a:{b: 1}'",
		"'a:1 '",
		"' a:1'",
		"'a:b:c'",
		"'2020-01-01'",
		"'2020-01-01T00:00:00Z'",
		"'2020-13-01'",
		"\"8c54161f-d4fe-4a74-9409-ed1e137040c1\"",
		"'8c54161f-d4fe-4a74-9409-ed1e137040c'",
		"{ a: '2020-01-01', b: ['a:1', \"x\"] }",
		"<string> 'a:1'",
		"f'a:/b'",
		"mod::a::b(1)",
		"silo::a::b<1.2.3>(1)",
		"DEFINE BUCKET b BACKEND 'memory'",
		"DEFINE MODULE mod::m AS f'a:/b'",
		"REMOVE MODULE mod::m",
		"DEFINE ANALYZER a FUNCTION mod::m::f",
		"a:1b",
		"a:1_000",
		"a:1e5",
		"a:123abc..456",
		"{ a: 1, b: [1, 2.5, -3dec, 4d], c: { type: 'Point', coordinates: [1, 2] } }",
		"{1, 2, 3}",
		"{1}",
		"{,}",
		"(1, 2)",
		"(1d)",
		"1..2",
		"1>..=2",
		"..=5",
		"a:1..",
		"a:+5",
		"a:-5",
		"a:-5x",
		"a:-1_000",
		"a:1_000",
		"a:⟨x⟩",
		"a:u'8c54161f-d4fe-4a74-9409-ed1e137040c1'",
		"a:[1, 'x', { b: 2 }]",
		"{ \"emoji\": \"\\uD83D\\uDE00\" }",
		"\"\\uD83D\"",
		"[null, true, false, 1e5, -0.5, \"x\"]",
		"foo",
		"-Infinity",
		"/a.b/",
		"d'2020-01-01T00:00:00Z'",
		"b'00ff'",
		"[[[[1]]]]",
	] {
		inputs.push(e.to_owned());
	}
	inputs.sort();
	inputs.dedup();
	if let Some(i) = args.iter().position(|a| a == "--fuzz") {
		let n: usize = args.get(i + 1).and_then(|v| v.parse().ok()).unwrap_or(1);
		inputs = mutants(&inputs, n);
	}
	println!("inputs: {}", inputs.len());

	let mut failed = false;
	for (name, check) in checks() {
		if only.as_deref().is_some_and(|o| o != name) {
			continue;
		}
		let mut tally: BTreeMap<&'static str, u32> = BTreeMap::new();
		let mut shown = 0;
		let trace = args.iter().any(|a| a == "--trace");
		for s in &inputs {
			if trace {
				eprintln!("{s:?}");
			}
			let Ok(o) = std::panic::catch_unwind(|| check(s)) else {
				println!("  {name}: {s:?}\n    panicked");
				*tally.entry("panic").or_default() += 1;
				failed = true;
				continue;
			};
			let key = match &o {
				Outcome::Equal => "equal",
				Outcome::BothReject => "both reject",
				Outcome::Mismatch(..) => "mismatch",
				Outcome::NewAcceptsInvalid(_) => "new accepts invalid",
				Outcome::NewRejectsValid(_) => "new rejects valid",
			};
			*tally.entry(key).or_default() += 1;
			if !matches!(o, Outcome::Equal | Outcome::BothReject) {
				failed = true;
				if verbose || shown < 5 {
					shown += 1;
					println!("  {name}: {s:?}\n    {o:?}");
				}
			}
		}
		println!("{name}: {tally:?}");
	}
	if failed {
		std::process::exit(1);
	}
}
