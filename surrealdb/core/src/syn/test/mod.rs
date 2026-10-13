//! Parser tests that need core: the facade entry points (which translate
//! capabilities into parser settings) and, for the parity cases, the
//! greenfield parser. `surrealdb-syn` sits below both.

mod builtin_paths;
mod json;
mod limit;

use super::parse;

#[test]
fn test_error_in_lineterminator() {
	let q = r#"
select * from person
CREATE person CONTENT { foo:'bar'};
"#;
	parse(q).unwrap_err();
}

#[test]
fn test_excessive_size() {
	let mut q = String::new();
	q.reserve_exact(u32::MAX as usize + 40);
	for _ in 0..u32::MAX {
		q.push(' ');
	}
	q.push_str("RETURN 1;");
	parse(&q).unwrap_err();
}

#[test]
fn empty_thing() {
	super::record_id("").unwrap_err();
}

#[test]
fn empty_block() {
	super::block("").unwrap_err();
}

/*
#[test]
fn empty_range() {
	super::range("").unwrap_err();
}
*/

#[test]
fn empty_duration() {
	super::duration("").unwrap_err();
}

#[test]
fn empty_datetime() {
	super::datetime("").unwrap_err();
}

#[test]
fn empty_idiom() {
	super::idiom("").unwrap_err();
}

#[test]
fn empty_expr() {
	super::expr("").unwrap_err();
}

#[test]
fn empty_json() {
	super::json("").unwrap_err();
}
