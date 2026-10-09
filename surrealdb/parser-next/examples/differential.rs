//! Differential test against `surrealdb-syn` over the language-test corpus.
//!
//! Every `.surql` file is split into statements by the new parser. Each
//! statement's text is parsed again by `surrealdb-syn`, and the new parser's
//! tree, lowered into the `surrealdb_sql` AST, must equal the old parser's, or
//! both must reject it.
//!
//! ```text
//! cargo run --locked --release -p surrealdb-parser-next --example differential [DIR] [-v]
//!     [--v3 | --fuzz-diff | --depth-probe | --stream | --stream-fuzz | --fuzz | --check SCRIPT…]
//! ```

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use surrealdb_parser_next::{
	Ast, Limits, Scratch, StatementEnd, Step, StreamParser, Tag, parse_script, parse_statement,
};
use surrealdb_parser_next_lower::Lower;
use surrealdb_sql::TopLevelExpr;
use surrealdb_types::ToSql;

#[derive(Default, Debug)]
struct Stats {
	files: u32,
	statements: u32,
	equal: u32,
	mismatch: u32,
	both_reject: u32,
	new_rejects_valid: u32,
	new_accepts_invalid: u32,
}

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
/// one, so that two parses of the same definition compare equal.
fn mask_random_keys(e: &mut TopLevelExpr) {
	use surrealdb_sql::access_type::{JwtAccess, JwtAccessVerify};
	use surrealdb_sql::statements::define::DefineStatement;
	use surrealdb_sql::{AccessType, Expr, Literal};
	fn key(e: &mut Expr) {
		if let Expr::Literal(Literal::String(s)) = e
			&& s.len() == 128
			&& s.chars().all(|c| c.is_ascii_alphanumeric())
		{
			*s = "random".into();
		}
	}
	fn jwt(j: &mut JwtAccess) {
		if let JwtAccessVerify::Key(k) = &mut j.verify {
			key(&mut k.key);
		}
		if let Some(i) = &mut j.issue {
			key(&mut i.key);
		}
	}
	fn expr(e: &mut Expr) {
		match e {
			Expr::Block(b) => b.0.iter_mut().for_each(expr),
			Expr::Define(d) => {
				let DefineStatement::Access(a) = &mut **d else {
					return;
				};
				match &mut a.access_type {
					AccessType::Jwt(j) => jwt(j),
					AccessType::Record(r) => {
						jwt(&mut r.jwt);
						if let Some(b) = &mut r.bearer {
							jwt(&mut b.jwt);
						}
					}
					AccessType::Bearer(b) => jwt(&mut b.jwt),
				}
			}
			_ => {}
		}
	}
	if let TopLevelExpr::Expr(e) = e {
		expr(e);
	}
}

/// Replaces 128-character alphanumeric string literals (random access keys
/// anywhere in a statement) in rendered SurrealQL.
fn mask_keys_in_sql(sql: &str) -> String {
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

/// A lowering rejection's message, when it is static.
fn reject_msg(r: &surrealdb_parser_next_lower::Reject) -> &'static str {
	match r.msg {
		std::borrow::Cow::Borrowed(m) => m,
		std::borrow::Cow::Owned(_) => "lowering check failed",
	}
}

fn old_parse(text: &str) -> Option<TopLevelExpr> {
	let ast = surrealdb_syn::parse(text).ok()?;
	let mut it = ast.expressions.into_iter();
	match (it.next(), it.next()) {
		(Some(e), None) => Some(e),
		_ => None,
	}
}

/// Parses truncations and byte mutations of every corpus file and checks
/// the parser neither panics nor breaks its output invariants.
fn fuzz(files: &[PathBuf]) {
	const PUNCT: &[u8] = b"()[]{}<>;:,.'\"`$-+*/|&=!?@#~0aZ \n";
	let mut ast = Ast::new();
	let mut scratch = Scratch::default();
	let mut seed: u64 = 0x9e37_79b9_7f4a_7c15;
	let mut next = move || {
		seed ^= seed << 13;
		seed ^= seed >> 7;
		seed ^= seed << 17;
		seed
	};
	let (mut parses, mut panics) = (0u64, 0u64);
	for f in files {
		let Ok(src) = std::fs::read_to_string(f) else {
			continue;
		};
		let mut inputs = Vec::new();
		for i in 0..100 {
			let mut cut = src.len() * i / 100;
			while !src.is_char_boundary(cut) {
				cut -= 1;
			}
			inputs.push(src[..cut].to_owned());
		}
		for _ in 0..100 {
			let mut b = src.clone().into_bytes();
			if b.is_empty() {
				break;
			}
			for _ in 0..3 {
				let at = next() as usize % b.len();
				if b[at].is_ascii() {
					b[at] = PUNCT[next() as usize % PUNCT.len()];
				}
			}
			if let Ok(s) = String::from_utf8(b) {
				inputs.push(s);
			}
		}
		for input in inputs {
			parses += 1;
			// Release builds abort on panic, so a parser panic ends the run;
			// the invariants are checked without panicking.
			parse_script(&input, Limits::default(), &mut ast, &mut scratch);
			let errors = ast.stmts.iter().filter(|&&s| ast.node(s).tag == Tag::Error).count();
			let mut ok = errors == scratch.diags.len() && ast.stmts.len() == ast.spans.len();
			let mut last = 0;
			for &(a, b) in &ast.spans {
				ok &= last <= a && a <= b && b as usize <= input.len();
				last = b;
			}
			if !ok {
				panics += 1;
				if panics < 3 {
					eprintln!(
						"BROKEN INVARIANT on input derived from {}:\n{input}\n{:?}",
						f.display(),
						ast.spans
					);
				}
			}
		}
	}
	println!("fuzz: {parses} parses, {panics} broken invariants, no panics");
}

/// One statement's parse, comparable across arenas and windows: spans are
/// made absolute.
#[derive(PartialEq, Debug)]
struct Snap {
	offset: u64,
	span: (u32, u32),
	nodes: Vec<(u8, u8, u16, u32, u32, u32, u32)>,
	extra: Vec<u32>,
	root: u32,
	diags: Vec<(u32, u32, &'static str)>,
}

fn snap(offset: u64, ast: &Ast, diags: &[surrealdb_parser_next::Diag]) -> Snap {
	let o = offset as u32;
	Snap {
		offset,
		span: (ast.spans[0].0 + o, ast.spans[0].1 + o),
		nodes: ast
			.nodes
			.iter()
			.map(|n| (n.tag as u8, n.op, n.depth, n.a, n.b, n.start + o, n.end + o))
			.collect(),
		extra: ast.extra.clone(),
		root: ast.stmts[0],
		diags: diags.iter().map(|d| (d.start + o, d.end + o, d.msg)).collect(),
	}
}

/// The statements of `src` parsed one at a time over the whole input: what a
/// stream must reproduce for any chunking.
fn reference(src: &str, ast: &mut Ast, scratch: &mut Scratch) -> Vec<Snap> {
	let mut out = Vec::new();
	let mut pos = 0usize;
	loop {
		match parse_statement(&src[pos..], Limits::default(), ast, scratch) {
			StatementEnd::Empty {
				..
			} => return out,
			StatementEnd::Statement {
				consumed,
				..
			} => {
				out.push(snap(pos as u64, ast, &scratch.diags));
				pos += consumed as usize;
			}
		}
	}
}

/// Feeds `src` in the given chunk lengths and collects the statements.
fn streamed(src: &[u8], chunks: &mut dyn FnMut() -> usize, sp: &mut StreamParser) -> Vec<Snap> {
	let mut out = Vec::new();
	let mut at = 0;
	loop {
		if at < src.len() {
			let n = chunks().clamp(1, src.len() - at);
			sp.feed(&src[at..at + n]);
			at += n;
		} else {
			sp.finish();
		}
		loop {
			match sp.next_statement().expect("corpus statements fit the stream limits") {
				Step::Statement(s) => out.push(snap(s.offset, s.ast, s.diags)),
				Step::NeedMore => break,
				Step::End => return out,
			}
		}
	}
}

/// Checks that a streamed parse equals the whole-input parse for every
/// chunking tried: every split point of small files and a dense sample of
/// large ones, or (`random`) random chunk sizes over mutated inputs.
fn stream_check(files: &[PathBuf], random: bool) {
	const PUNCT: &[u8] = b"()[]{}<>;:,.'\"`$-+*/|&=!?@#~0aZ \n";
	let mut ast = Ast::new();
	let mut scratch = Scratch::default();
	let mut seed: u64 = 0x2545_f491_4f6c_dd1d;
	let mut rng = move || {
		seed ^= seed << 13;
		seed ^= seed >> 7;
		seed ^= seed << 17;
		seed
	};
	let (mut streams, mut failures, mut whole_mismatch) = (0u64, 0u64, 0u64);
	for f in files {
		let Ok(orig) = std::fs::read_to_string(f) else {
			continue;
		};
		let mut inputs = vec![orig.clone()];
		if random {
			for _ in 0..20 {
				let mut b = orig.clone().into_bytes();
				if b.is_empty() {
					break;
				}
				for _ in 0..3 {
					let at = rng() as usize % b.len();
					if b[at].is_ascii() {
						b[at] = PUNCT[rng() as usize % PUNCT.len()];
					}
				}
				if let Ok(s) = String::from_utf8(b) {
					inputs.push(s);
				}
			}
		}
		for src in inputs {
			let want = reference(&src, &mut ast, &mut scratch);
			// The one-at-a-time reference must split and parse exactly as the
			// whole-input parse does.
			parse_script(&src, Limits::default(), &mut ast, &mut scratch);
			let spans: Vec<_> = want.iter().map(|s| s.span).collect();
			let tags: Vec<_> = want.iter().map(|s| s.nodes[s.root as usize].0).collect();
			let whole_tags: Vec<_> = ast.stmts.iter().map(|&r| ast.node(r).tag as u8).collect();
			if spans != ast.spans || tags != whole_tags {
				whole_mismatch += 1;
				if whole_mismatch < 4 {
					eprintln!(
						"STATEMENT-WISE PARSE DIFFERS FROM WHOLE PARSE: {}\n {spans:?} {tags:?}\n {:?} {whole_tags:?}",
						f.display(),
						ast.spans
					);
				}
				continue;
			}
			let bytes = src.as_bytes();
			let splits: Vec<usize> = if random {
				(0..8).collect()
			} else if bytes.len() <= 2048 {
				(1..bytes.len()).collect()
			} else {
				(1..2048).map(|i| i * bytes.len() / 2048).collect()
			};
			for p in splits {
				streams += 1;
				let mut sp = StreamParser::new(Limits::default(), 1 << 26);
				let got = if random {
					let max = [1usize, 3, 16, 64, 4096][p % 5];
					let mut r = || (rng() as usize % max) + 1;
					streamed(bytes, &mut r, &mut sp)
				} else {
					let mut first = true;
					let mut c = || {
						if std::mem::take(&mut first) {
							p
						} else {
							usize::MAX
						}
					};
					streamed(bytes, &mut c, &mut sp)
				};
				// Window offsets depend on where empty statements were dropped;
				// the absolute content may not.
				let same = got.len() == want.len()
					&& got.iter().zip(&want).all(|(a, b)| {
						(&a.span, &a.nodes, &a.extra, a.root, &a.diags)
							== (&b.span, &b.nodes, &b.extra, b.root, &b.diags)
					});
				if !same {
					failures += 1;
					if failures < 4 {
						let i = got.iter().zip(&want).position(|(a, b)| a != b).unwrap_or(0);
						eprintln!(
							"STREAM MISMATCH {} split {p}: {} vs {} statements, first difference at #{i}\n got: {:?}\nwant: {:?}",
							f.display(),
							got.len(),
							want.len(),
							got.get(i).map(|s| (s.offset, &s.diags)),
							want.get(i).map(|s| (s.offset, &s.diags)),
						);
					}
				}
			}
		}
	}
	println!(
		"stream: {streams} streamed parses, {failures} differ from the whole-input parse, {whole_mismatch} statement-wise splits differ"
	);
}

fn main() {
	let args: Vec<String> = std::env::args().skip(1).collect();
	let verbose = args.iter().any(|a| a == "-v");
	let root = args.iter().find(|a| !a.starts_with('-')).cloned().unwrap_or_else(|| {
		concat!(env!("CARGO_MANIFEST_DIR"), "/../../language-tests/tests").into()
	});
	let mut files = Vec::new();
	collect(Path::new(&root), &mut files);
	files.sort();
	if args.iter().any(|a| a == "--fuzz") {
		fuzz(&files);
		return;
	}
	if args.iter().any(|a| a == "--stream") {
		stream_check(&files, false);
		return;
	}
	if args.iter().any(|a| a == "--stream-fuzz") {
		stream_check(&files, true);
		return;
	}

	if let Some(i) = args.iter().position(|a| a == "--check") {
		// Compares each following argument as a script.
		for text in &args[i + 1..] {
			println!("{text:?}: {:?}", compare_script(text));
		}
		return;
	}
	if args.iter().any(|a| a == "--depth-probe") {
		depth_probe();
		return;
	}
	if args.iter().any(|a| a == "--v3") {
		let root = concat!(env!("CARGO_MANIFEST_DIR"), "/../parser/src/test");
		let mut v3 = Vec::new();
		collect(Path::new(root), &mut v3);
		v3.sort();
		let mut tally = BTreeMap::new();
		for f in &v3 {
			let Ok(src) = std::fs::read_to_string(f) else {
				continue;
			};
			// The input is everything before the expected-output block.
			let input = src.split("/* ===== result =====").next().unwrap_or(&src);
			let outcome = compare_script(input);
			if verbose && !matches!(outcome, Outcome::Equal | Outcome::BothReject) {
				println!("--- {} ({}):\n{input}", outcome.name(), f.display());
				if let Outcome::Mismatch(old, new) = &outcome {
					println!("  old: {old}\n  new: {new}");
				}
			}
			*tally.entry(outcome.name()).or_insert(0u32) += 1;
		}
		println!("v3 snapshot inputs: {} files: {tally:?}", v3.len());
		return;
	}
	if args.iter().any(|a| a == "--fuzz-diff") {
		fuzz_diff(&files, verbose);
		return;
	}

	let mut st = Stats::default();
	let mut shown = 0;
	let mut ast = Ast::new();
	let mut scratch = Scratch::default();
	for f in &files {
		let Ok(src) = std::fs::read_to_string(f) else {
			continue;
		};
		st.files += 1;
		parse_script(&src, Limits::default(), &mut ast, &mut scratch);
		let mut diags = scratch.diags.iter();
		for (i, &s) in ast.stmts.iter().enumerate() {
			st.statements += 1;
			let n = ast.node(s);
			let (a, b) = ast.spans[i];
			let text = &src[a as usize..b as usize];
			let new = if n.tag == Tag::Error {
				Err(diags.next().expect("one diagnostic per failed statement").msg)
			} else {
				Lower::new(&ast, &src).top(s).map_err(|r| reject_msg(&r))
			};
			let outcome = judge(new, old_parse(text));
			match &outcome {
				Outcome::Equal => st.equal += 1,
				Outcome::BothReject => st.both_reject += 1,
				Outcome::Mismatch(..) => st.mismatch += 1,
				Outcome::NewAcceptsInvalid(_) => st.new_accepts_invalid += 1,
				Outcome::NewRejectsValid(_) => st.new_rejects_valid += 1,
			}
			if verbose && shown < 60 && !matches!(outcome, Outcome::Equal | Outcome::BothReject) {
				shown += 1;
				println!("--- {} ({}):\n{text}\n  {outcome:?}", outcome.name(), f.display());
			}
		}
	}
	println!("{st:#?}");
	let agree = st.equal + st.both_reject;
	println!(
		"statements: {}, equal to syn: {} ({:.1}%), both reject: {}, agreement {:.2}%",
		st.statements,
		st.equal,
		100.0 * st.equal as f64 / st.statements as f64,
		st.both_reject,
		100.0 * agree as f64 / st.statements as f64,
	);
}

/// How the new parser's result for a statement or script compares with
/// `syn`'s. The payloads are read through `Debug` in verbose output.
#[derive(Debug)]
#[allow(dead_code, reason = "payloads are printed with Debug")]
enum Outcome {
	Equal,
	BothReject,
	Mismatch(String, String),
	NewAcceptsInvalid(String),
	NewRejectsValid(&'static str),
}

impl Outcome {
	fn name(&self) -> &'static str {
		match self {
			Outcome::Equal => "equal",
			Outcome::BothReject => "both reject",
			Outcome::Mismatch(..) => "mismatch",
			Outcome::NewAcceptsInvalid(_) => "new accepts invalid",
			Outcome::NewRejectsValid(_) => "new rejects valid",
		}
	}
}

fn judge(new: Result<TopLevelExpr, &'static str>, old: Option<TopLevelExpr>) -> Outcome {
	match (new, old) {
		(Ok(mut new), Some(mut old)) => {
			mask_random_keys(&mut new);
			mask_random_keys(&mut old);
			if new == old {
				Outcome::Equal
			} else {
				Outcome::Mismatch(old.to_sql(), new.to_sql())
			}
		}
		(Ok(new), None) => Outcome::NewAcceptsInvalid(new.to_sql()),
		(Err(_), None) => Outcome::BothReject,
		(Err(m), Some(_)) => Outcome::NewRejectsValid(m),
	}
}

/// Compares a whole script: both parsers must accept it with equal
/// statements, or both reject it.
fn compare_script(text: &str) -> Outcome {
	let (ast, diags) = surrealdb_parser_next::parse(text);
	let new: Result<Vec<TopLevelExpr>, &'static str> = if let Some(d) = diags.first() {
		Err(d.msg)
	} else {
		ast.stmts
			.iter()
			.map(|&s| Lower::new(&ast, text).top(s).map_err(|r| reject_msg(&r)))
			.collect()
	};
	let old = surrealdb_syn::parse(text).ok().map(|a| a.expressions);
	match (new, old) {
		(Ok(mut new), Some(mut old)) => {
			new.iter_mut().for_each(mask_random_keys);
			old.iter_mut().for_each(mask_random_keys);
			let masked = |v: &[TopLevelExpr]| {
				v.iter().map(|e| mask_keys_in_sql(&e.to_sql())).collect::<Vec<_>>()
			};
			if new == old || masked(&new) == masked(&old) {
				Outcome::Equal
			} else {
				let debug = std::env::var_os("DIFF_DEBUG").is_some();
				let p = |v: &[TopLevelExpr]| {
					v.iter()
						.map(|e| {
							if debug {
								format!("{e:?}")
							} else {
								e.to_sql()
							}
						})
						.collect::<Vec<_>>()
						.join("; ")
				};
				Outcome::Mismatch(p(&old), p(&new))
			}
		}
		(Ok(new), None) => Outcome::NewAcceptsInvalid(
			new.iter().map(|e| e.to_sql()).collect::<Vec<_>>().join("; "),
		),
		(Err(_), None) => Outcome::BothReject,
		(Err(m), Some(_)) => Outcome::NewRejectsValid(m),
	}
}

/// For each nesting template, finds the deepest nesting each parser
/// accepts. The two must agree: deeper input that `syn` rejects must be
/// rejected.
fn depth_probe() {
	type Template = (&'static str, fn(usize) -> String);
	let templates: &[Template] = &[
		("parens", |n| format!("{}1{}", "(".repeat(n), ")".repeat(n))),
		("arrays", |n| format!("{}1{}", "[".repeat(n), "]".repeat(n))),
		("objects", |n| format!("{}1{}", "{a:".repeat(n), "}".repeat(n))),
		("sets", |n| format!("{}1{}", "{1, ".repeat(n), "}".repeat(n))),
		("blocks", |n| format!("{}1{}", "{1;".repeat(n), "}".repeat(n))),
		("calls", |n| format!("{}1{}", "math::abs(".repeat(n), ")".repeat(n))),
		("custom calls", |n| format!("{}1{}", "fn::f(".repeat(n), ")".repeat(n))),
		("negation", |n| format!("{}1", "-".repeat(n))),
		("not", |n| format!("{}1", "!".repeat(n))),
		("casts", |n| format!("{}1", "<int>".repeat(n))),
		("sum chain", |n| format!("1{}", " + 1".repeat(n))),
		("power chain", |n| format!("1{}", " ** 1".repeat(n))),
		("and chain", |n| format!("a{}", " AND a".repeat(n))),
		("field parts", |n| format!("a{}", ".a".repeat(n))),
		("index parts", |n| format!("a{}", "[0]".repeat(n))),
		("nested index", |n| format!("{}0{}", "a[".repeat(n), "]".repeat(n))),
		("graph parts", |n| format!("a{}", "->b".repeat(n))),
		("method calls", |n| format!("a{}", ".b()".repeat(n))),
		("nested methods", |n| format!("{}1{}", "a.b(".repeat(n), ")".repeat(n))),
		("subqueries", |n| format!("{}1{}", "(SELECT * FROM ".repeat(n), ")".repeat(n))),
		("returns", |n| format!("{}1", "RETURN ".repeat(n))),
		("if blocks", |n| format!("{}1{}", "IF true {".repeat(n), "}".repeat(n))),
		("closures", |n| format!("{}1", "|$a| ".repeat(n))),
		("array kinds", |n| format!("<{}int{}>1", "array<".repeat(n), ">".repeat(n))),
		("option kinds", |n| format!("<{}int{}>1", "option<".repeat(n), ">".repeat(n))),
		("object kinds", |n| format!("<{}int{}>1", "{a:".repeat(n), "}".repeat(n))),
		("where parts", |n| format!("{}true{}", "a[WHERE ".repeat(n), "]".repeat(n))),
		("record keys", |n| format!("a:{}1{}", "[".repeat(n), "]".repeat(n))),
		("destructures", |n| format!("a{}{}", ".{a".repeat(n), "}".repeat(n))),
		("group by parts", |n| format!("SELECT * FROM t GROUP BY a{}", ".a".repeat(n))),
		("set targets", |n| format!("UPDATE t SET a{} = 1", ".a".repeat(n))),
		("let chain", |n| format!("{}1{}", "{LET $a = ".repeat(n), "}".repeat(n))),
		("lookups", |n| format!("a{}{}", "->(b WHERE a".repeat(n), ")".repeat(n))),
		("lookup aliases", |n| format!("a{}{}", "->(b AS c".repeat(n), ")".repeat(n))),
		("worded ifs", |n| format!("{}1{}", "IF true THEN ".repeat(n), " END".repeat(n))),
		("for loops", |n| format!("{}1{}", "FOR $a IN [] {".repeat(n), "}".repeat(n))),
		("arrow closures", |n| format!("{}1{}", "|$a| -> int {".repeat(n), "}".repeat(n))),
		("literal kinds", |n| format!("<{}int{}>1", "[".repeat(n), "]".repeat(n))),
		("record objects", |n| format!("a:{}1{}", "{a:".repeat(n), "}".repeat(n))),
		("mixed nesting", |n| format!("{}1{}", "[(".repeat(n), ")]".repeat(n))),
		("set in object", |n| format!("{}1{}", "{a:{1,".repeat(n), "}}".repeat(n))),
		("functions", |n| format!("{}1{}", "function(".repeat(n), ") {}".repeat(n))),
		("defines", |n| format!("{}1{}", "DEFINE FUNCTION fn::f() {".repeat(n), "}".repeat(n))),
		("prefix ranges", |n| format!("{}1", "..".repeat(n))),
		("casts of parens", |n| format!("{}1{}", "<int>(".repeat(n), ")".repeat(n))),
		("unary in calls", |n| format!("{}1{}", "-math::abs(".repeat(n), ")".repeat(n))),
		("matches chain", |n| format!("a{}", " @@ a AND a".repeat(n))),
		("inserts", |n| format!("{}1{}", "INSERT INTO t (".repeat(n), ")".repeat(n))),
	];
	let max_ok = |text: &dyn Fn(usize) -> String, new: bool| -> usize {
		let ok = |n: usize| {
			let t = text(n);
			if new {
				let (_, diags) = surrealdb_parser_next::parse(&t);
				diags.is_empty()
			} else {
				surrealdb_syn::parse(&t).is_ok()
			}
		};
		// The largest n in 0..=400 accepted, assuming acceptance is monotone.
		let (mut lo, mut hi) = (0usize, 401usize);
		if !ok(0) {
			return usize::MAX;
		}
		while hi - lo > 1 {
			let mid = (lo + hi) / 2;
			if ok(mid) {
				lo = mid;
			} else {
				hi = mid;
			}
		}
		lo
	};
	let mut differ = 0;
	for (name, f) in templates {
		let old = max_ok(f, false);
		let new = max_ok(f, true);
		let mark = if old == new {
			""
		} else {
			differ += 1;
			"  <-- differs"
		};
		println!("{name:16} syn {old:4}  new {new:4}{mark}");
	}
	println!("depth probe: {} templates, {differ} differ", templates.len());
}

/// Mutates every corpus statement and requires both parsers to agree on
/// each mutant: equal trees, or both rejecting.
fn fuzz_diff(files: &[PathBuf], verbose: bool) {
	const PIECES: &[&str] = &[
		"(", ")", "[", "]", "{", "}", "<", ">", ";", ":", "::", ",", ".", "..", "...", "'", "\"",
		"`", "$", "-", "+", "*", "/", "|", "&&", "=", "!", "?", "@", "->", "<-", "<|", "|>", "0",
		"1.5", "1d", "x", " ", "\n", "NONE", "SELECT", "FROM", "WHERE", "AS", "IF", "ELSE", "END",
		"VALUE", "TYPE", "ON", "FOR", "SET", "RETURN", "LET", "THEN", "AND", "IN",
	];
	let env = |k: &str, d: u64| std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d);
	let mut seed: u64 = env("FUZZ_SEED", 0x1234_5678_9abc_def1) | 1;
	let rounds = env("FUZZ_N", 10);
	let mut next = move || {
		seed ^= seed << 13;
		seed ^= seed >> 7;
		seed ^= seed << 17;
		seed
	};
	let mut tally: BTreeMap<&'static str, u64> = BTreeMap::new();
	let mut shown = 0;
	let mut ast = Ast::new();
	let mut scratch = Scratch::default();
	for f in files {
		let Ok(src) = std::fs::read_to_string(f) else {
			continue;
		};
		parse_script(&src, Limits::default(), &mut ast, &mut scratch);
		let texts: Vec<String> =
			ast.spans.iter().map(|&(a, b)| src[a as usize..b as usize].to_owned()).collect();
		for text in texts {
			for _ in 0..rounds {
				let mut t = text.clone();
				for _ in 0..1 + next() % 2 {
					let mut at = next() as usize % (t.len() + 1);
					while !t.is_char_boundary(at) {
						at -= 1;
					}
					let piece = PIECES[next() as usize % PIECES.len()];
					match next() % 3 {
						0 => t.insert_str(at, piece),
						1 => {
							let mut end = (at + 1 + next() as usize % 4).min(t.len());
							while !t.is_char_boundary(end) {
								end += 1;
							}
							t.replace_range(at..end, piece);
						}
						_ => {
							let mut end = (at + 1 + next() as usize % 4).min(t.len());
							while !t.is_char_boundary(end) {
								end += 1;
							}
							t.replace_range(at..end, "");
						}
					}
				}
				let outcome = compare_script(&t);
				if verbose && shown < 40 && !matches!(outcome, Outcome::Equal | Outcome::BothReject)
				{
					shown += 1;
					println!("--- {} ({}):\n{t}\n  {outcome:?}", outcome.name(), f.display());
				}
				*tally.entry(outcome.name()).or_default() += 1;
			}
		}
	}
	println!("fuzz-diff: {tally:?}");
}
