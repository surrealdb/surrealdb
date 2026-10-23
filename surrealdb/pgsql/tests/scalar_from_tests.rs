use surrealdb_pgsql::ast::stmt::{Statement, TableRef};
use surrealdb_pgsql::{ParserSettings, parse_statement, parse_statement_with_settings};

#[test]
fn scalar_function_factors_keep_aliases_and_ordinality() {
	for sql in [
		"SELECT * FROM current_schema()",
		"SELECT * FROM pg_catalog.abs(-2) AS a(n)",
		"SELECT * FROM abs($1::int4) WITH ORDINALITY AS a(n, ord)",
	] {
		let Statement::Select(select) = parse_statement(sql).expect("scalar function table") else {
			panic!("SELECT");
		};
		assert!(matches!(&select.from[0], TableRef::Function { .. }), "{sql}");
	}
}

#[test]
fn function_factors_reject_window_calls_and_malformed_tails() {
	for sql in [
		"SELECT * FROM abs(1) OVER ()",
		"SELECT * FROM abs(1) WITH",
		"SELECT * FROM abs(1) WITH ORDINALITY AS a(",
	] {
		assert!(parse_statement(sql).is_err(), "{sql}");
	}
}

#[test]
fn nested_function_factors_obey_expression_depth_limits() {
	let sql = format!("SELECT * FROM {}1{}", "abs(".repeat(40), ")".repeat(40));
	let settings = ParserSettings {
		max_recursion_depth: 8,
		..ParserSettings::default()
	};
	assert!(parse_statement_with_settings(&sql, settings).is_err());
}
