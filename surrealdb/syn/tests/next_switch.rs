//! The parser-next switch: each mode routes the entry points as documented
//! in `surrealdb_syn::next`. One test, because the mode is process-wide.

use bytes::BytesMut;
use surrealdb_syn::next::{Mode, mode as current_mode, set_mode};
use surrealdb_syn::parser::StatementStream;

#[test]
fn every_mode_parses_the_same_trees_and_errors() {
	let valid = [
		"SELECT * FROM person WHERE age > 18 ORDER BY name LIMIT 10;",
		"DEFINE FIELD name ON person TYPE option<string> DEFAULT 'x';",
		"LET $a = { b: [1, 2d, 3.5f, 4dec] }; RETURN $a.b[0];",
	];
	let invalid = ["SELECT * FROM;", "RETURN [1, 2;", "DEFINE TABLE"];
	let mut off = Vec::new();
	for mode in [Mode::Off, Mode::On, Mode::Check] {
		set_mode(mode);
		assert_eq!(current_mode(), mode);
		let mut out = Vec::new();
		for q in valid {
			out.push(format!("{:?}", surrealdb_syn::parse(q).expect("valid query parses")));
		}
		for q in invalid {
			out.push(surrealdb_syn::parse(q).expect_err("invalid query is rejected").to_string());
		}
		out.push(format!("{:?}", surrealdb_syn::expr_for_definition("a.b + 1")));
		out.push(format!("{:?}", surrealdb_syn::expr_for_definition("a.b bogus")));
		out.push(format!("{:?}", surrealdb_syn::kind_for_definition("option<record<a | b>>")));
		out.push(format!("{:?}", surrealdb_syn::idiom_for_definition("a[WHERE b > 1].c")));
		out.push(format!("{:?}", surrealdb_syn::fields_for_definition("VALUE a AS b")));
		out.push(format!("{:?}", surrealdb_syn::block_for_definition("{ LET $x = 1; $x }")));
		out.push(format!("{:?}", surrealdb_syn::value("{ a: [1, 2d, 'x'], b: a:{ c: 1 } }")));
		out.push(format!("{:?}", surrealdb_syn::value("1 +")));
		out.push(format!("{:?}", surrealdb_syn::record_id("person:⟨tobie⟩")));
		let mut stream = StatementStream::new();
		let mut buf = BytesMut::from("RETURN 1; RETURN 'a;b'; RETURN [");
		let mut streamed = Vec::new();
		while let Ok(Some(s)) = stream.parse_partial(&mut buf) {
			streamed.push(format!("{s:?}"));
		}
		buf.extend_from_slice(b"2];");
		while let Ok(Some(s)) = stream.parse_complete(&mut buf) {
			streamed.push(format!("{s:?}"));
		}
		assert_eq!(streamed.len(), 3, "{mode:?}: {streamed:?}");
		out.extend(streamed);
		if mode == Mode::Off {
			off = out;
		} else {
			assert_eq!(out, off, "{mode:?} differs from Off");
		}
	}
}
