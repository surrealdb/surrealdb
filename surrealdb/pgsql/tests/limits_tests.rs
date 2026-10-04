//! Tests for parser recursion depth limits, stack overflow protection, and input hardening.

use surrealdb_pgsql::error::ErrorKind;
use surrealdb_pgsql::{
	ParserSettings, parse_expr, parse_expr_with_settings, parse_statement,
	parse_statement_with_settings, parse_statements, parse_statements_with_settings,
};

fn run_on_2mib_stack<F, R>(f: F) -> R
where
	F: FnOnce() -> R + Send + 'static,
	R: Send + 'static,
{
	std::thread::Builder::new()
		.stack_size(2 * 1024 * 1024)
		.spawn(f)
		.expect("thread spawn failed")
		.join()
		.expect("thread join failed")
}

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
		..ParserSettings::default()
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
		..ParserSettings::default()
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
		..ParserSettings::default()
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

#[test]
fn test_flat_chains_on_2mib_stack() {
	run_on_2mib_stack(|| {
		println!("Testing add chain...");
		// 5000 addition chain
		let add_chain = (0..5000).map(|_| "1").collect::<Vec<_>>().join(" + ");
		let err = parse_expr(&add_chain).expect_err("5000 additions must hit recursion limit");
		assert_eq!(err.kind, ErrorKind::RecursionLimitExceeded);

		println!("Testing or chain...");
		// 5000 OR chain
		let or_chain = (0..5000).map(|_| "true").collect::<Vec<_>>().join(" OR ");
		let err = parse_expr(&or_chain).expect_err("5000 ORs must hit recursion limit");
		assert_eq!(err.kind, ErrorKind::RecursionLimitExceeded);

		println!("Testing concat chain...");
		// 5000 string concat chain
		let concat_chain = (0..5000).map(|_| "'a'").collect::<Vec<_>>().join(" || ");
		let err = parse_expr(&concat_chain).expect_err("5000 concats must hit recursion limit");
		assert_eq!(err.kind, ErrorKind::RecursionLimitExceeded);

		println!("Testing cast chain...");
		// 5000 typecast chain
		let cast_chain = format!("1{}", "::INT".repeat(5000));
		let err = parse_expr(&cast_chain).expect_err("5000 casts must hit recursion limit");
		assert_eq!(err.kind, ErrorKind::RecursionLimitExceeded);

		println!("Testing sub chain...");
		// 5000 subscript chain
		let sub_chain = format!("arr{}", "[1]".repeat(5000));
		let err = parse_expr(&sub_chain).expect_err("5000 subscripts must hit recursion limit");
		assert_eq!(err.kind, ErrorKind::RecursionLimitExceeded);

		println!("Testing join chain...");
		// 5000 JOIN chain
		let mut join_chain = String::from("SELECT * FROM t0");
		for i in 1..5000 {
			join_chain.push_str(&format!(" JOIN t{i} ON t0.id = t{i}.id"));
		}
		let err = parse_statement(&join_chain).expect_err("5000 joins must hit recursion limit");
		assert_eq!(err.kind, ErrorKind::RecursionLimitExceeded);

		println!("Testing union chain...");
		// 5000 UNION chain
		let union_chain =
			(0..5000).map(|i| format!("SELECT {i}")).collect::<Vec<_>>().join(" UNION ");
		let err = parse_statement(&union_chain).expect_err("5000 unions must hit recursion limit");
		assert_eq!(err.kind, ErrorKind::RecursionLimitExceeded);
	});
}

#[test]
fn test_input_byte_cap() {
	let settings = ParserSettings {
		max_input_bytes: 50,
		..ParserSettings::default()
	};
	let long_sql = "SELECT ".to_string() + &"1, ".repeat(30) + "1";
	assert!(long_sql.len() > 50);

	let err = parse_statement_with_settings(&long_sql, settings).expect_err("must exceed byte cap");
	assert!(matches!(
		err.kind,
		ErrorKind::InputSizeLimitExceeded {
			max: 50,
			..
		}
	));

	let err = parse_expr_with_settings(&long_sql, settings).expect_err("must exceed byte cap");
	assert!(matches!(
		err.kind,
		ErrorKind::InputSizeLimitExceeded {
			max: 50,
			..
		}
	));

	let err =
		parse_statements_with_settings(&long_sql, settings).expect_err("must exceed byte cap");
	assert!(matches!(
		err.kind,
		ErrorKind::InputSizeLimitExceeded {
			max: 50,
			..
		}
	));
}

#[test]
fn test_work_counter_limit() {
	let settings = ParserSettings {
		max_work_count: 20,
		..ParserSettings::default()
	};
	let sql = "SELECT 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10 + 11 + 12 + 13 + 14 + 15";
	let err = parse_statement_with_settings(sql, settings).expect_err("must exceed work count");
	assert_eq!(err.kind, ErrorKind::WorkLimitExceeded);
}

#[test]
fn test_statement_separator_required() {
	// Missing semicolon between statements must be rejected
	let err = parse_statements("SELECT 1 SELECT 2;").expect_err("missing semicolon must error");
	assert!(matches!(err.kind, ErrorKind::UnexpectedToken { .. }));

	let err = parse_statement("SELECT 1 SELECT 2;").expect_err("trailing statement must error");
	assert!(matches!(err.kind, ErrorKind::UnexpectedToken { .. }));

	// Expression must not leave trailing tokens
	let err = parse_expr("1 + 2 3").expect_err("trailing token in expr must error");
	assert!(matches!(err.kind, ErrorKind::UnexpectedToken { .. }));
}

#[test]
fn test_transaction_modes_rejected() {
	assert!(parse_statement("BEGIN READ ONLY;").is_err());
	assert!(parse_statement("BEGIN ISOLATION LEVEL SERIALIZABLE;").is_err());
	assert!(parse_statement("COMMIT AND CHAIN;").is_err());
	assert!(parse_statement("ROLLBACK AND NO CHAIN;").is_err());

	// Clean transactions succeed
	assert!(parse_statement("BEGIN;").is_ok());
	assert!(parse_statement("START TRANSACTION;").is_ok());
	assert!(parse_statement("COMMIT;").is_ok());
	assert!(parse_statement("ROLLBACK;").is_ok());
}

#[test]
fn test_set_local_rejected() {
	let err =
		parse_statement("SET LOCAL timezone = 'UTC';").expect_err("SET LOCAL must be rejected");
	assert!(matches!(err.kind, ErrorKind::InvalidStatement(_)));

	// Normal SET succeeds
	assert!(parse_statement("SET timezone = 'UTC';").is_ok());
	assert!(parse_statement("SET SESSION timezone = 'UTC';").is_ok());
}

#[test]
fn test_string_escapes_rejected() {
	// NUL byte rejection
	assert!(matches!(parse_expr(r#"E'\0'"#).unwrap_err().kind, ErrorKind::InvalidCharacter(_)));
	assert!(matches!(parse_expr(r#"E'\x00'"#).unwrap_err().kind, ErrorKind::InvalidCharacter(_)));
	assert!(matches!(parse_expr(r#"E'\u0000'"#).unwrap_err().kind, ErrorKind::InvalidCharacter(_)));
	assert!(matches!(
		parse_expr(r#"E'\U00000000'"#).unwrap_err().kind,
		ErrorKind::InvalidCharacter(_)
	));

	// Surrogate code points rejection (0xD800..=0xDFFF)
	assert!(matches!(parse_expr(r#"E'\uD800'"#).unwrap_err().kind, ErrorKind::InvalidCharacter(_)));
	assert!(matches!(parse_expr(r#"E'\uDFFF'"#).unwrap_err().kind, ErrorKind::InvalidCharacter(_)));

	// Bad hex
	assert!(matches!(parse_expr(r#"E'\xZZ'"#).unwrap_err().kind, ErrorKind::InvalidCharacter(_)));
}

#[test]
fn test_number_validation_rejected() {
	// Trailing letters on base prefixes
	assert!(parse_expr("0xZZ").is_err());
	assert!(parse_expr("0o88").is_err());
	assert!(parse_expr("0b22").is_err());

	// Trailing letters on numbers
	assert!(parse_expr("123abc").is_err());
	assert!(parse_expr("456$").is_err());
}

#[test]
fn test_not_in_and_not_between_roundtrip() {
	use surrealdb_pgsql::ast::expr::Expr;

	let parsed_not_in = parse_expr("x NOT IN (1, 2, 3)").expect("parse NOT IN");
	assert!(matches!(
		parsed_not_in,
		Expr::InList {
			negated: true,
			..
		}
	));

	let parsed_not_between = parse_expr("x NOT BETWEEN 1 AND 10").expect("parse NOT BETWEEN");
	assert!(matches!(
		parsed_not_between,
		Expr::Between {
			negated: true,
			..
		}
	));
}
