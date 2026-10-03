//! Tests for parser recursion depth limits and stack overflow protection.

use surrealdb_pgsql::error::ErrorKind;
use surrealdb_pgsql::{
	ParserSettings, parse_expr, parse_expr_with_settings, parse_statement_with_settings,
};

#[test]
fn test_parentheses_within_default_limit() {
	let depth = 50;
	let sql = format!("{}1{}", "(".repeat(depth), ")".repeat(depth));
	let parsed = parse_expr(&sql);
	assert!(parsed.is_ok(), "expected successful parse at depth {depth}");
}

#[test]
fn test_parentheses_exceeding_default_limit() {
	let depth = 150;
	let sql = format!("{}1{}", "(".repeat(depth), ")".repeat(depth));
	let err = parse_expr(&sql).expect_err("expected recursion limit error");
	assert_eq!(err.kind, ErrorKind::RecursionLimitExceeded);
}

#[test]
fn test_custom_recursion_limit_expression() {
	let settings = ParserSettings {
		max_recursion_depth: 10,
	};

	let ok_sql = format!("{}1{}", "(".repeat(5), ")".repeat(5));
	assert!(parse_expr_with_settings(&ok_sql, settings).is_ok());

	let deep_sql = format!("{}1{}", "(".repeat(15), ")".repeat(15));
	let err = parse_expr_with_settings(&deep_sql, settings).expect_err("should exceed limit 10");
	assert_eq!(err.kind, ErrorKind::RecursionLimitExceeded);
}

#[test]
fn test_deep_unary_not_recursion_limit() {
	let settings = ParserSettings {
		max_recursion_depth: 20,
	};

	let ok_sql = format!("{}TRUE", "NOT ".repeat(10));
	assert!(parse_expr_with_settings(&ok_sql, settings).is_ok());

	let deep_sql = format!("{}TRUE", "NOT ".repeat(25));
	let err = parse_expr_with_settings(&deep_sql, settings).expect_err("should exceed limit 20");
	assert_eq!(err.kind, ErrorKind::RecursionLimitExceeded);
}

#[test]
fn test_deep_subquery_recursion_limit() {
	let settings = ParserSettings {
		max_recursion_depth: 5,
	};

	// 2 levels of SELECT subqueries
	let ok_sql = "SELECT * FROM (SELECT * FROM users AS u) AS sub";
	assert!(parse_statement_with_settings(ok_sql, settings).is_ok());

	// 8 levels of SELECT subqueries
	let mut deep_sql = String::from("users");
	for i in 0..8 {
		deep_sql = format!("(SELECT * FROM {deep_sql}) AS t{i}");
	}
	let full_sql = format!("SELECT * FROM {deep_sql}");
	let err = parse_statement_with_settings(&full_sql, settings).expect_err("should exceed limit");
	assert_eq!(err.kind, ErrorKind::RecursionLimitExceeded);
}
