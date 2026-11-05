//! Properties of parser-next that need no reference parser, over the
//! language-test corpus.
//!
//! - `--fuzz` (the default): truncations and byte mutations of every file keep the tree's
//!   invariants (one diagnostic per failed statement, ordered statement spans).
//! - `--stream`, `--stream-fuzz`: parsing a file in chunks, at every split point or at random
//!   sizes, gives the statements a whole parse gives.
//! - `--depth-probe`: the deepest nesting accepted for each of 48 templates is the frozen ceiling.
//! - `--round-trip`: every accepted statement, printed and parsed again, gives the same tree.
//! - `--logical`: direct flat-tree lowering agrees with runtime-expression lowering.
//! - `--render`: rendering SurrealQL from the arena gives the text the lowered expression prints.
//!   `--render-fuzz` runs the same check over byte mutations of the corpus that still parse.
//!
//! The golden check (`--example golden`) pins every entry point's output.
//!
//! ```text
//! cargo run --locked --release -p surrealdb-expr --example invariants [DIR] [-v]
//!     [--fuzz | --stream | --stream-fuzz | --depth-probe | --round-trip | --logical | --render | --render-fuzz]
//! ```

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use surrealdb_expr::parser;
use surrealdb_parser_next::ast::Cl;
use surrealdb_parser_next::{
	Ast, Limits, Scratch, StatementEnd, Step, StreamParser, Tag, parse_script, parse_statement,
};
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
	let (mut requests, mut admission_differences) = (0u64, 0u64);
	let settings = parser::ParserSettings::all_features(&parser::ParserConfig::default());
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
			if parses.is_multiple_of(32) {
				requests += 1;
				let native = parser::query_with_settings(&input, settings)
					.map(|query| query.expressions.len())
					.map_err(|error| error.to_string());
				let direct = parser::execution_query_with_settings(&input, settings)
					.map(|query| query.len())
					.map_err(|error| error.to_string());
				if native != direct {
					admission_differences += 1;
					if admission_differences <= 5 {
						eprintln!("request admission differs on a mutation of {}", f.display());
					}
				}
			}
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
	println!(
		"request fuzz: {requests} sampled mutations, {admission_differences} admission differences"
	);
	if panics != 0 || admission_differences != 0 {
		std::process::exit(1);
	}
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
	// The deepest nesting earlier releases accepted for each template, frozen:
	// the depth budgets are charged where those releases charged them, so the
	// ceilings stay where queries found them.
	let expected: BTreeMap<&str, usize> = [
		("parens", 20),
		("arrays", 100),
		("objects", 100),
		("sets", 100),
		("blocks", 20),
		("calls", 20),
		("custom calls", 20),
		("negation", 400),
		("not", 127),
		("casts", 127),
		("sum chain", 126),
		("power chain", 126),
		("and chain", 126),
		("field parts", 127),
		("index parts", 126),
		("nested index", 20),
		("graph parts", 127),
		("method calls", 127),
		("nested methods", 20),
		("subqueries", 10),
		("returns", 20),
		("if blocks", 20),
		("closures", 20),
		("array kinds", 99),
		("option kinds", 1),
		("object kinds", 99),
		("where parts", 20),
		("record keys", 100),
		("destructures", 126),
		("group by parts", 127),
		("set targets", 127),
		("let chain", 10),
		("lookups", 20),
		("lookup aliases", 127),
		("worded ifs", 20),
		("for loops", 20),
		("arrow closures", 20),
		("literal kinds", 99),
		("record objects", 100),
		("mixed nesting", 20),
		("set in object", 50),
		("functions", 20),
		("defines", 20),
		("prefix ranges", 1),
		("casts of parens", 20),
		("unary in calls", 20),
		("matches chain", 124),
		("inserts", 10),
	]
	.into_iter()
	.collect();
	let max_ok = |text: &dyn Fn(usize) -> String| -> usize {
		let ok = |n: usize| parser::parse(&text(n)).is_ok();
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
		let want = expected[name];
		let got = max_ok(f);
		let mark = if want == got {
			""
		} else {
			differ += 1;
			"  <-- differs"
		};
		println!("{name:16} expected {want:4}  got {got:4}{mark}");
	}
	println!("depth probe: {} templates, {differ} differ", templates.len());
	if differ > 0 {
		std::process::exit(1);
	}
}

/// Request admission, logical forms, transaction modes and observer text
/// match native parsing and lowering across the corpus. Scalar entry points
/// also match independently of request dispatch.
fn logical_check(files: &[PathBuf], verbose: bool) {
	use surrealdb_expr::expr::logical::{lower, lower_expr};
	use surrealdb_expr::parser::{ParserConfig, ParserSettings};
	let settings = ParserSettings::all_features(&ParserConfig::default());
	let (mut checked, mut differ) = (0u64, 0u64);
	let (mut request_checked, mut request_differ) = (0u64, 0u64);
	let mask = |value: String| {
		value
			.split('"')
			.map(|part| {
				if part.len() == 128 && part.bytes().all(|b| b.is_ascii_alphanumeric()) {
					"[GENERATED KEY]"
				} else {
					part
				}
			})
			.collect::<Vec<_>>()
			.join("\"")
	};
	for file in files {
		let Ok(source) = std::fs::read_to_string(file) else {
			continue;
		};
		let native_query = parser::parse(&source);
		let direct_query = parser::execution_query_with_settings(&source, settings);
		match (&native_query, &direct_query) {
			(Err(native), Err(direct)) if native.to_string() == direct.to_string() => {}
			(Ok(native), Ok(direct)) if native.expressions.len() == direct.len() => {
				for (native, direct) in native.expressions.iter().zip(direct) {
					use surrealdb_expr::parser::ExecutionStatement;
					let matches = match direct {
						ExecutionStatement::Surface(surface) => {
							surface == native
								|| mask(format!("{surface:?}")) == mask(format!("{native:?}"))
						}
						ExecutionStatement::Logical(statement) => {
							let expected = if let parser::TopLevelExpr::Expr(expr) = native {
								lower(expr.clone()).ok()
							} else {
								None
							};
							request_checked += 1;
							{
								let plan = expected.as_ref() == Some(&statement.plan);
								let access = statement.plan.read_only() == native.read_only();
								let text = statement.text.to_sql() == native.to_redacted_sql();
								if !(plan && access && text) && (verbose || request_differ <= 5) {
									println!(
										"direct parity: plan={plan} read_only={access} text={text}"
									);
								}
								plan && access && text
							}
						}
					};
					if !matches {
						request_differ += 1;
						if verbose || request_differ <= 5 {
							println!("request lowering or metadata differs for {}", file.display());
						}
					}
				}
			}
			_ => {
				request_differ += 1;
				if verbose || request_differ <= 5 {
					println!("request admission differs for {}", file.display());
				}
			}
		}
		let Ok(query) = native_query else {
			continue;
		};
		for statement in query.expressions {
			let text = statement.to_sql();
			let Ok(native) = parser::expr_with_settings(&text, settings, true) else {
				continue;
			};
			let expected = lower_expr(native).map_err(|e| e.to_string());
			let direct = parser::logical_expr_with_settings(&text, settings, true)
				.map_err(|e| e.to_string());
			checked += 1;
			// Implicit bearer signing keys are generated independently by each
			// parse. Preserve structural comparison while masking those values.
			if direct != expected && mask(format!("{direct:?}")) != mask(format!("{expected:?}")) {
				differ += 1;
				if verbose || differ <= 5 {
					println!("logical lowering differs for {}", file.display());
				}
			}
		}
	}
	println!("logical lowering: {checked} expressions, {differ} differ");
	println!("request lowering: {request_checked} direct statements, {request_differ} differ");
	if differ != 0 || request_differ != 0 {
		std::process::exit(1);
	}
}

/// Replaces every quoted run of exactly 128 alphanumeric characters, the
/// shape of a generated signing key, in either quote style.
fn mask_generated_keys(sql: &str) -> String {
	let mut out = String::with_capacity(sql.len());
	let mut rest = sql;
	while let Some(i) = rest.find(['\'', '"']) {
		let quote = rest.as_bytes()[i];
		let after = &rest[i + 1..];
		let key = after.len() > 128
			&& after.as_bytes()[128] == quote
			&& after.as_bytes()[..128].iter().all(|b| b.is_ascii_alphanumeric());
		if key {
			out.push_str(&rest[..=i]);
			out.push_str("[GENERATED KEY]");
			out.push(quote as char);
			rest = &after[129..];
		} else {
			out.push_str(&rest[..=i]);
			rest = after;
		}
	}
	out.push_str(rest);
	out
}

/// The tags `Lower::expr` handles.
const EXPR_TAGS: &[Tag] = &[
	Tag::Array,
	Tag::Object,
	Tag::Prefix,
	Tag::Cast,
	Tag::Binary,
	Tag::Block,
	Tag::Set,
	Tag::None,
	Tag::Null,
	Tag::True,
	Tag::False,
	Tag::Int,
	Tag::Float,
	Tag::NaN,
	Tag::Infinity,
	Tag::UnboundedRange,
	Tag::Decimal,
	Tag::Regex,
	Tag::Path,
	Tag::Duration,
	Tag::Str,
	Tag::Param,
	Tag::Field,
	Tag::Table,
	Tag::RecordId,
	Tag::BinaryX,
	Tag::PostfixRange,
	Tag::Call,
	Tag::Script,
	Tag::Point,
	Tag::CallExpr,
	Tag::Idiom,
	Tag::Select,
	Tag::Create,
	Tag::Update,
	Tag::Upsert,
	Tag::Delete,
	Tag::Return,
	Tag::Let,
	Tag::If,
	Tag::For,
	Tag::Relate,
	Tag::Insert,
	Tag::Explain,
	Tag::Alter,
	Tag::Remove,
	Tag::Define,
	Tag::Closure,
	Tag::Mock,
	Tag::Sleep,
	Tag::Throw,
	Tag::Break,
	Tag::Continue,
	Tag::Info,
	Tag::Rebuild,
];

#[derive(Default)]
struct RenderTag {
	compared: u64,
	mismatched: u64,
	rejected: u64,
	panicked: u64,
	differs_from_logical: u64,
}

/// The outcome of a call that may panic: the panic is a mismatch, not an abort.
type Caught<T> = Result<T, ()>;

fn catch<T>(f: impl FnOnce() -> T) -> Caught<T> {
	std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)).map_err(|_| ())
}

/// The generator of the byte mutations `--render-fuzz` applies.
struct RenderRng(u64);

impl RenderRng {
	fn next(&mut self) -> u64 {
		self.0 ^= self.0 << 13;
		self.0 ^= self.0 >> 7;
		self.0 ^= self.0 << 17;
		self.0
	}
}

/// Byte mutations of `src` that parse without a diagnostic.
fn render_mutants(src: &str, rng: &mut RenderRng, probe: &mut (Ast, Scratch)) -> Vec<String> {
	const PUNCT: &[u8] = b"()[]{}<>;:,.'\"`$-+*/|&=!?@#~0aZ \n";
	const ATTEMPTS: usize = 8;
	let mut out = Vec::new();
	if src.is_empty() {
		return out;
	}
	for _ in 0..ATTEMPTS {
		let mut b = src.as_bytes().to_vec();
		for _ in 0..3 {
			let at = rng.next() as usize % b.len();
			if b[at].is_ascii() {
				b[at] = PUNCT[rng.next() as usize % PUNCT.len()];
			}
		}
		let Ok(mutant) = String::from_utf8(b) else {
			continue;
		};
		parse_script(&mutant, Limits::default(), &mut probe.0, &mut probe.1);
		if probe.1.diags.is_empty() && !probe.0.stmts.is_empty() {
			out.push(mutant);
		}
	}
	out
}

/// Rendering from the arena gives the text of the lowered expression, under
/// three option sets, for every expression, idiom, part and statement of the
/// corpus.
fn render_check(files: &[PathBuf], verbose: bool, fuzz: bool) {
	use surrealdb_expr::expr::logical::lower_expr;
	use surrealdb_expr::parser::{Lower, Options};
	let sets = [
		("ALL", Options::ALL),
		(
			"legacy_strands",
			Options {
				legacy_strands: true,
				..Options::ALL
			},
		),
		(
			"no surrealism",
			Options {
				surrealism: false,
				legacy_strands: false,
			},
		),
	];
	let mut failed = false;
	for (name, opts) in sets {
		let mut stats: BTreeMap<String, RenderTag> = BTreeMap::new();
		let mut shown: BTreeMap<String, u32> = BTreeMap::new();
		let (mut with_errors, mut gate_misses) = (0u64, 0u64);
		let mut ast = Ast::new();
		let mut scratch = Scratch::default();
		let mut probe = (Ast::new(), Scratch::default());
		let mut rng = RenderRng(0x9e37_79b9_7f4a_7c15);
		for file in files {
			let Ok(base) = std::fs::read_to_string(file) else {
				continue;
			};
			let sources = if fuzz {
				render_mutants(&base, &mut rng, &mut probe)
			} else {
				vec![base]
			};
			for src in sources {
				parse_script(&src, Limits::default(), &mut ast, &mut scratch);
				if !scratch.diags.is_empty() {
					with_errors += 1;
				}
				// Nodes a failed statement left behind may hold dangling indexes, so
				// only nodes inside a statement that parsed are visited.
				let good: Vec<(u32, u32)> = ast
					.stmts
					.iter()
					.zip(&ast.spans)
					.filter(|&(&s, _)| ast.node(s).tag != Tag::Error)
					.map(|(_, &span)| span)
					.collect();
				let inside = |start: u32, end: u32| {
					let i = good.partition_point(|&(a, _)| a <= start);
					i > 0 && end <= good[i - 1].1
				};
				// The `SELECT` of a table view is read by the definition's own
				// lowering; as an expression node it is outside `Lower::expr`'s domain.
				let views: std::collections::HashSet<u32> = ast
					.nodes
					.iter()
					.filter(|n| n.tag == Tag::Clause && n.op == Cl::View as u8)
					.map(|n| n.a)
					.collect();
				let lower = Lower {
					ast: &ast,
					src: &src,
					opts,
				};
				let mut report = |key: &str, id: u32, want: &str, got: &str| {
					let shown = shown.entry(key.to_owned()).or_default();
					*shown += 1;
					if verbose || *shown <= 5 {
						let n = ast.node(id);
						println!(
							"  {key} {}:{}..{}\n    src:  {}\n    want: {want}\n    got:  {got}",
							file.display(),
							n.start,
							n.end,
							&src[n.start as usize..n.end as usize]
						);
					}
				};
				let mut record = |key: &str,
				                  id: u32,
				                  want: Caught<Result<String, String>>,
				                  got: Caught<Result<String, String>>,
				                  logical: Option<String>,
				                  must_reject: bool| {
					let entry = stats.entry(key.to_owned()).or_default();
					let want = match want {
						Ok(Ok(w)) => w,
						Ok(Err(_)) => {
							entry.rejected += 1;
							if must_reject && matches!(got, Ok(Ok(_))) {
								entry.mismatched += 1;
								gate_misses += 1;
								report(key, id, "Err", "rendered a disabled grammar");
							}
							return;
						}
						Err(()) => {
							entry.panicked += 1;
							entry.mismatched += 1;
							report(key, id, "panic in Lower", "");
							return;
						}
					};
					entry.compared += 1;
					match got {
						Ok(Ok(g)) => {
							if mask_generated_keys(&g) != mask_generated_keys(&want) {
								entry.mismatched += 1;
								report(key, id, &want, &g);
							} else if logical
								.is_some_and(|l| mask_generated_keys(&l) != mask_generated_keys(&g))
							{
								entry.differs_from_logical += 1;
							}
						}
						Ok(Err(e)) => {
							entry.mismatched += 1;
							report(key, id, &want, &format!("Err({e})"));
						}
						Err(()) => {
							entry.panicked += 1;
							entry.mismatched += 1;
							report(key, id, &want, "panic in render");
						}
					}
				};
				for (&s, &(a, _)) in ast.stmts.iter().zip(&ast.spans) {
					if ast.node(s).tag == Tag::Error {
						continue;
					}
					let _ = a;
					let want =
						catch(|| lower.top(s).map(|t| t.to_sql()).map_err(|e| e.msg.to_string()));
					let got = catch(|| {
						let mut out = String::new();
						lower.render_top(s, &mut out).map(|()| out).map_err(|e| e.msg.to_string())
					});
					record("(statement)", s, want, got, None, false);
				}
				for id in 0..ast.nodes.len() as u32 {
					let n = ast.node(id);
					if !inside(n.start, n.end) {
						continue;
					}
					if matches!(n.tag, Tag::Field | Tag::Idiom) {
						let want = catch(|| {
							lower.idiom(id).map_err(|e| e.msg.to_string()).map(|i| {
								let parts: Vec<String> = i.0.iter().map(|p| p.to_sql()).collect();
								(i.to_sql(), parts)
							})
						});
						match want {
							Ok(Ok((text, parts))) => {
								let got = catch(|| {
									let mut out = String::new();
									lower
										.render_idiom(id, &mut out)
										.map(|()| out)
										.map_err(|e| e.msg.to_string())
								});
								record("(idiom)", id, Ok(Ok(text)), got, None, false);
								for (i, part) in parts.into_iter().enumerate() {
									let got = catch(|| {
										let mut out = String::new();
										lower
											.render_part(id, i, &mut out)
											.map(|()| out)
											.map_err(|e| e.msg.to_string())
									});
									record("(part)", id, Ok(Ok(part)), got, None, false);
								}
							}
							Ok(Err(e)) => record(
								"(idiom)",
								id,
								Ok(Err(e)),
								Ok(Err(String::new())),
								None,
								false,
							),
							Err(()) => record("(idiom)", id, Err(()), Err(()), None, false),
						}
					}
					if !EXPR_TAGS.contains(&n.tag) || views.contains(&id) {
						continue;
					}
					let key = format!("{:?}", n.tag);
					let want =
						catch(|| lower.expr(id).map(|e| e.to_sql()).map_err(|e| e.msg.to_string()));
					let got = catch(|| {
						let mut out = String::new();
						lower.render_expr(id, &mut out).map(|()| out).map_err(|e| e.msg.to_string())
					});
					let gated = !opts.surrealism
						&& n.tag == Tag::Call
						&& ast.nodes.get(n.a as usize).is_some_and(|c| matches!(c.op, 2 | 3));
					// The logical form of the lowered expression, which `--logical` shows
					// equals direct lowering; report-only, and a panic there counts as no form.
					let logical = if verbose {
						catch(|| {
							lower.expr(id).ok().and_then(|e| lower_expr(e).ok()).map(|l| l.to_sql())
						})
						.ok()
						.flatten()
					} else {
						None
					};
					record(&key, id, want, got, logical, gated);
				}
			}
		}
		let (mut compared, mut rejected, mut mismatches) = (0, 0, 0);
		for t in stats.values() {
			compared += t.compared;
			rejected += t.rejected;
			mismatches += t.mismatched;
		}
		let (statements, parts) = (stats.remove("(statement)"), stats.remove("(part)"));
		let idioms = stats.remove("(idiom)");
		compared -= [&statements, &parts, &idioms]
			.iter()
			.filter_map(|t| t.as_ref())
			.map(|t| t.compared)
			.sum::<u64>();
		println!(
			"render parity [opts={name}]: files {} (with parse errors: {with_errors}), nodes compared {compared}, lower-rejected {rejected}, mismatches {mismatches}",
			files.len()
		);
		for (label, t) in [("statements", &statements), ("idioms", &idioms), ("parts", &parts)] {
			if let Some(t) = t {
				println!(
					"  {label}: compared={} mismatched={} rejected={}",
					t.compared, t.mismatched, t.rejected
				);
			}
		}
		let mut tags: Vec<_> = stats.iter().filter(|(_, t)| t.compared > 0).collect();
		tags.sort_by(|a, b| b.1.mismatched.cmp(&a.1.mismatched).then(a.0.cmp(b.0)));
		println!("  per tag:");
		for (tag, t) in tags {
			println!(
				"    {tag} compared={} mismatched={}{}{}",
				t.compared,
				t.mismatched,
				if t.panicked > 0 {
					format!(" panicked={}", t.panicked)
				} else {
					String::new()
				},
				if verbose {
					format!(" differs_from_logical={}", t.differs_from_logical)
				} else {
					String::new()
				}
			);
		}
		if gate_misses > 0 {
			println!("  disabled grammar rendered: {gate_misses}");
		}
		failed |= mismatches > 0;
	}
	if failed {
		std::process::exit(1);
	}
}

fn round_trip(files: &[PathBuf], verbose: bool) {
	let (mut checked, mut differ) = (0u64, 0u64);
	for f in files {
		let Ok(src) = std::fs::read_to_string(f) else {
			continue;
		};
		let Ok(ast) = parser::parse(&src) else {
			continue;
		};
		for e in ast.expressions {
			checked += 1;
			let text = e.to_sql();
			let again = parser::parse(&text).map(|a| a.expressions);
			let same = match &again {
				Ok(v) => {
					v.len() == 1 && mask_keys_in_sql(&v[0].to_sql()) == mask_keys_in_sql(&text)
				}
				Err(_) => false,
			};
			if !same {
				differ += 1;
				if verbose || differ <= 5 {
					println!("--- {}: {text}\n  {again:?}", f.display());
				}
			}
		}
	}
	println!("round trip: {checked} statements, {differ} differ");
	if differ > 0 {
		std::process::exit(1);
	}
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
	if args.iter().any(|a| a == "--stream") {
		stream_check(&files, false);
	} else if args.iter().any(|a| a == "--stream-fuzz") {
		stream_check(&files, true);
	} else if args.iter().any(|a| a == "--depth-probe") {
		depth_probe();
	} else if args.iter().any(|a| a == "--round-trip") {
		round_trip(&files, verbose);
	} else if args.iter().any(|a| a == "--logical") {
		logical_check(&files, verbose);
	} else if args.iter().any(|a| a == "--render") {
		render_check(&files, verbose, false);
	} else if args.iter().any(|a| a == "--render-fuzz") {
		render_check(&files, verbose, true);
	} else {
		fuzz(&files);
	}
}
