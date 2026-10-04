//! Tests for parser recursion depth limits, stack overflow protection, and input hardening in
//! MySQL.

use surrealdb_mysql::{
	ErrorKind, ParserSettings, parse_expr, parse_expr_with_settings, parse_statement,
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
fn test_flat_chains_on_2mib_stack() {
	run_on_2mib_stack(|| {
		println!("Testing add chain...");
		// 5000 addition chain
		let add_chain = (0..5000).map(|_| "1").collect::<Vec<_>>().join(" + ");
		let err = parse_expr(&add_chain).expect_err("5000 additions must hit recursion limit");
		assert_eq!(err.kind, ErrorKind::RecursionLimitExceeded);

		println!("Testing or chain...");
		// 5000 OR chain
		let or_chain = (0..5000).map(|_| "TRUE").collect::<Vec<_>>().join(" OR ");
		let err = parse_expr(&or_chain).expect_err("5000 ORs must hit recursion limit");
		assert_eq!(err.kind, ErrorKind::RecursionLimitExceeded);

		println!("Testing join chain...");
		// 5000 JOIN chain
		let mut join_chain = String::from("SELECT * FROM t0");
		for i in 1..5000 {
			join_chain.push_str(&format!(" JOIN t{i} ON t0.id = t{i}.id"));
		}
		let err = parse_statement(&join_chain).expect_err("5000 joins must hit recursion limit");
		assert_eq!(err.kind, ErrorKind::RecursionLimitExceeded);

		println!("Testing derived tables with settings depth 10...");
		let settings = ParserSettings {
			max_recursion_depth: 10,
			..ParserSettings::default()
		};
		let mut deep_sql = String::from("users");
		for i in 0..15 {
			deep_sql = format!("(SELECT * FROM {deep_sql}) AS t{i}");
		}
		let full_sql = format!("SELECT * FROM {deep_sql}");
		let err = parse_statement_with_settings(&full_sql, settings)
			.expect_err("deep derived tables must hit recursion limit");
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
fn test_number_validation_rejected() {
	assert!(parse_expr("0xZZ").is_err());
	assert!(parse_expr("123abc").is_err());
}
