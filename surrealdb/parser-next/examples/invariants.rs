//! Properties of parser-next that need no reference parser, over the
//! language-test corpus.
//!
//! - `--fuzz` (the default): truncations and byte mutations of every file keep the tree's
//!   invariants (one diagnostic per failed statement, ordered statement spans).
//! - `--stream`, `--stream-fuzz`: parsing a file in chunks, at every split point or at random
//!   sizes, gives the statements a whole parse gives.
//! - `--depth-probe`: the deepest nesting accepted for each of 48 templates is the frozen ceiling.
//! - `--round-trip`: every accepted statement, printed and parsed again, gives the same tree.
//!
//! The golden check (`--example golden`) pins every entry point's output.
//!
//! ```text
//! cargo run --locked --release -p surrealdb-parser-next --example invariants [DIR] [-v]
//!     [--fuzz | --stream | --stream-fuzz | --depth-probe | --round-trip]
//! ```

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

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
		let ok = |n: usize| surrealdb_parser_next_lower::parse(&text(n)).is_ok();
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

/// Every corpus statement the engine accepts, printed as SurrealQL and parsed
/// again, gives the same tree: the text a definition stores reads back as
/// what was defined.
fn round_trip(files: &[PathBuf], verbose: bool) {
	let (mut checked, mut differ) = (0u64, 0u64);
	for f in files {
		let Ok(src) = std::fs::read_to_string(f) else {
			continue;
		};
		let Ok(ast) = surrealdb_parser_next_lower::parse(&src) else {
			continue;
		};
		for e in ast.expressions {
			checked += 1;
			let text = e.to_sql();
			let again = surrealdb_parser_next_lower::parse(&text).map(|a| a.expressions);
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
	} else {
		fuzz(&files);
	}
}
