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

// A path used as the start of another path keeps its parentheses: without
// them the trailing parts extend the inner path instead of applying to its
// result, which is a different value and is what gets stored for a guard,
// `VALUE`, `ASSERT` or view.
test_case!(idiom_after_idiom_index => "RETURN ([{ a: [1] }, { a: [2] }].a)[0]");
test_case!(idiom_after_idiom_method => "RETURN ([{ a: [1] }, { a: [2] }].a).len()");
test_case!(idiom_after_param_idiom => "RETURN ($x.a)[0]");
test_case!(idiom_stored_in_field_value => "DEFINE FIELD v ON t VALUE ([{ a: [1] }].a)[0]");
test_case!(idiom_stored_in_permission => "DEFINE TABLE t PERMISSIONS FOR select WHERE ([{ a: [1] }].a)[0] = 1");

// An action left out of a table's `PERMISSIONS FOR …` list reparses as NONE,
// so every clause has to survive the round trip. Fields carry no `delete`.
test_case!(table_permissions_delete_full => "DEFINE TABLE t PERMISSIONS FOR select, delete FULL");
test_case!(table_permissions_delete_only => "DEFINE TABLE t PERMISSIONS FOR delete FULL");
test_case!(table_permissions_mixed => "DEFINE TABLE t PERMISSIONS FOR select FULL, FOR create, update, delete NONE");
test_case!(alter_table_permissions_delete_full => "ALTER TABLE t PERMISSIONS FOR select, delete FULL");
test_case!(field_permissions_mixed => "DEFINE FIELD f ON t PERMISSIONS FOR select FULL, FOR create, update NONE");
test_case!(field_permissions_none => "DEFINE FIELD f ON t PERMISSIONS NONE");
test_case!(alter_field_permissions_none => "ALTER FIELD f ON t PERMISSIONS NONE");

// A `DURATION` clause is separated from the previous one, never terminated.
test_case!(alter_access_duration_token => "ALTER ACCESS a ON DATABASE DURATION FOR TOKEN 2h");
test_case!(alter_access_duration_grant => "ALTER ACCESS a ON DATABASE DURATION FOR GRANT 1d");
test_case!(alter_access_duration_all => "ALTER ACCESS a ON DATABASE DURATION FOR GRANT 1d, FOR TOKEN 2h, FOR SESSION 3h");
test_case!(alter_access_duration_drop => "ALTER ACCESS a ON DATABASE DURATION FOR GRANT NONE, FOR SESSION NONE");

// Clauses with no other syntax to hang off are the easiest ones to forget.
test_case!(select_tempfiles => "SELECT * FROM t TEMPFILES");
test_case!(select_tempfiles_ordered => "SELECT * FROM t FOR UPDATE TEMPFILES EXPLAIN");
test_case!(remove_database_expunge => "REMOVE DATABASE AND EXPUNGE d");
test_case!(remove_namespace_expunge => "REMOVE NAMESPACE AND EXPUNGE IF EXISTS n");
test_case!(remove_table_expunge => "REMOVE TABLE AND EXPUNGE t");

/// SECURITY: see `sql::AccessType::fmt_sql`. The render that produces a
/// definition's text also feeds export, which redacts symmetric keys first, so
/// a rendered bearer key is either a real secret written into a dump or
/// `[REDACTED]` installed as the signing secret of the database restored from
/// it. Not rendering the clause costs the round trip and is the safe side of
/// that trade.
#[test]
fn bearer_access_does_not_render_its_key() {
	let query = parse(
		"DEFINE ACCESS a ON DATABASE TYPE BEARER FOR USER WITH JWT ALGORITHM HS512 KEY 'topsecret'",
	)
	.unwrap();
	let text = query.to_sql();
	assert!(!text.contains("topsecret"), "a bearer access rendered its signing key: {text}");
	assert!(!text.contains("WITH JWT"), "a bearer access rendered a JWT clause: {text}");
}

/// `TYPE RECORD` is the statement's default and only the database level
/// accepts it, so an access defined higher up has to name a type.
#[test]
fn access_without_type_is_database_only() {
	parse("DEFINE ACCESS a ON DATABASE").unwrap();
	parse("DEFINE ACCESS a ON ROOT").unwrap_err();
	parse("DEFINE ACCESS a ON NAMESPACE").unwrap_err();
}

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
