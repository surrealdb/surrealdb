//! Round-trip and pretty-printing tests for the render helpers in
//! `surrealdb-common`.
//!
//! They live in core rather than beside the helpers because they need the
//! parser and the expr lowering, both of which sit above `surrealdb-common`.

use surrealdb_types::ToSql;

use crate::syn::{expr, parse};

pub fn ensure_formats(s: &str) {
	let parsed = crate::syn::parse(s).unwrap();
	let parsed_formated = crate::syn::parse(&parsed.to_sql()).unwrap();

	let plan: crate::expr::LogicalPlan = parsed.clone().into();
	let parsed_formated_plan = crate::syn::parse(&plan.to_sql()).unwrap();

	assert_eq!(parsed, parsed_formated, "formatting the sql type changed the query");
	assert_eq!(parsed, parsed_formated_plan, "formatting the expr type changed the query");
}

macro_rules! test_case {
	($name:ident => $source:literal) => {
		#[test]
		fn $name() {
			ensure_formats($source)
		}
	};
}

test_case!(idiom_after_select => "(SELECT foo FROM bar ORDER BY foo)[0]");
test_case!(idiom_after_create => "(CREATE foo:1 SET V = $a)[0]");
test_case!(idiom_after_closure => "(|$a: number| { $a })[0]");

test_case!(covered_expr => "(1 + 1) * 3");

#[test]
fn pretty_query() {
	let query = parse("SELECT * FROM {foo: [1, 2, 3]};").unwrap();
	assert_eq!(query.to_sql(), "SELECT * FROM { foo: [1, 2, 3] };");
	assert_eq!(query.to_sql_pretty(), "SELECT * FROM {\n\tfoo: [\n\t\t1,\n\t\t2,\n\t\t3\n\t]\n};");
}

#[test]
fn pretty_define_query() {
	let query = parse("DEFINE TABLE test SCHEMAFULL PERMISSIONS FOR create, update, delete NONE FOR select WHERE public = true;").unwrap();
	assert_eq!(
		query.to_sql(),
		"DEFINE TABLE test TYPE NORMAL SCHEMAFULL PERMISSIONS FOR select WHERE public = true, FOR create, update, delete NONE;"
	);
	assert_eq!(
		query.to_sql_pretty(),
		"DEFINE TABLE test TYPE NORMAL SCHEMAFULL\n\tPERMISSIONS\n\tFOR select WHERE public = true,\n\tFOR create, update, delete NONE;"
	);
}

#[test]
fn pretty_value() {
	let value = expr("{foo: [1, 2, 3]}").unwrap();
	assert_eq!(value.to_sql(), "{ foo: [1, 2, 3] }");
	assert_eq!(value.to_sql_pretty(), "{\n\tfoo: [\n\t\t1,\n\t\t2,\n\t\t3\n\t]\n}");
}

#[test]
fn pretty_array() {
	let array = expr("[1, 2, 3]").unwrap();
	assert_eq!(array.to_sql(), "[1, 2, 3]");
	assert_eq!(array.to_sql_pretty(), "[\n\t1,\n\t2,\n\t3\n]");
}
