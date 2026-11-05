//! Recursion limits on table references: every way a `FROM` clause nests is bounded by
//! `ParserSettings::max_recursion_depth`, so a deep one is refused with the ordinary
//! recursion-limit error instead of overflowing the stack of the thread that parses it.

use surrealdb_mysql::{
	ErrorKind, ParserSettings, parse_statement, parse_statement_with_settings,
	parse_statements_with_settings,
};

const LIMIT: usize = 16;

fn settings() -> ParserSettings {
	ParserSettings {
		max_recursion_depth: LIMIT,
		..ParserSettings::default()
	}
}

/// Parse `shape(n)` for every nesting `n` from 1 to four times the limit, and return the deepest
/// one that is accepted. Every deeper one must be refused with the recursion-limit error, so a
/// nesting that is not bounded, or that fails for any other reason, fails the test.
fn deepest_accepted(shape: impl Fn(usize) -> String) -> usize {
	let mut deepest = 0;
	let mut refused = false;
	for n in 1..=4 * LIMIT {
		let sql = shape(n);
		match parse_statement_with_settings(&sql, settings()) {
			Ok(_) => {
				assert!(!refused, "{sql}: accepted although a shallower nesting was refused");
				deepest = n;
			}
			Err(err) => {
				assert_eq!(err.kind, ErrorKind::RecursionLimitExceeded, "{sql}");
				refused = true;
			}
		}
	}
	assert!(refused, "a nesting of {} is not refused", 4 * LIMIT);
	assert!(deepest >= 2, "even a shallow nesting is refused");
	assert!(deepest <= LIMIT, "a nesting of {deepest} exceeds the limit of {LIMIT}");
	deepest
}

/// Run `f` on a thread whose stack holds the default recursion limit of a debug build, whatever
/// the stack size of the test runner's threads.
fn on_large_stack(f: impl FnOnce() + Send + 'static) {
	std::thread::Builder::new()
		.stack_size(32 * 1024 * 1024)
		.spawn(f)
		.expect("thread spawn failed")
		.join()
		.expect("a nesting at the default limit panicked");
}

/// `count` table names separated by commas.
fn table_list(count: usize) -> String {
	(0..count).map(|i| format!("t{i}")).collect::<Vec<_>>().join(", ")
}

#[test]
fn joins_chained_to_the_left() {
	deepest_accepted(|n| {
		let joins: String = (1..=n).map(|i| format!(" JOIN t{i} ON t0.id = t{i}.id")).collect();
		format!("SELECT * FROM t0{joins}")
	});
	deepest_accepted(|n| {
		let joins: String = (1..=n).map(|i| format!(" CROSS JOIN t{i}")).collect();
		format!("SELECT * FROM t0{joins}")
	});
	deepest_accepted(|n| {
		let joins: String = (1..=n).map(|i| format!(" STRAIGHT_JOIN t{i}")).collect();
		format!("SELECT * FROM t0{joins}")
	});
}

#[test]
fn references_listed_with_commas() {
	// A comma is a cross join, and a long list nests as deep as a chain of joins does.
	deepest_accepted(|n| format!("SELECT * FROM {}", table_list(n + 1)));
}

#[test]
fn subqueries_in_from() {
	deepest_accepted(|n| {
		let mut from = String::from("t");
		for i in 0..n {
			from = format!("(SELECT * FROM {from}) AS s{i}");
		}
		format!("SELECT * FROM {from}")
	});
}

#[test]
fn parenthesised_subqueries_in_from() {
	deepest_accepted(|n| format!("SELECT * FROM {}SELECT 1{} AS s", "(".repeat(n), ")".repeat(n)));
}

#[test]
fn subqueries_in_joins() {
	deepest_accepted(|n| {
		let mut from = String::from("t");
		for i in 0..n {
			from = format!("(SELECT * FROM {from}) AS s{i} JOIN u{i} ON s{i}.id = u{i}.id");
		}
		format!("SELECT * FROM {from}")
	});
}

#[test]
fn set_operations_in_from() {
	deepest_accepted(|n| {
		let chain: Vec<String> = (0..=n).map(|i| format!("SELECT {i}")).collect();
		format!("SELECT * FROM ({}) AS s", chain.join(" UNION ALL "))
	});
}

#[test]
fn common_table_expressions_in_from() {
	deepest_accepted(|n| {
		let mut query = String::from("SELECT 1");
		for i in 0..n {
			query = format!("WITH c{i} AS ({query}) SELECT * FROM c{i}");
		}
		query
	});
}

#[test]
fn levels_are_released_when_a_reference_is_complete() {
	// None of the levels taken for the parts of a statement outlives it, so a script of many
	// statements is never refused for its length.
	let one = "SELECT * FROM a JOIN b ON a.id = b.id, c, (SELECT * FROM (SELECT 1) AS d) AS e, \
	           (SELECT 2 UNION ALL SELECT 3) AS f;";
	assert!(parse_statement_with_settings(one, settings()).is_ok());
	let script = one.repeat(50);
	assert!(parse_statements_with_settings(&script, settings()).is_ok());
}

#[test]
fn the_default_limit_bounds_every_nesting() {
	on_large_stack(|| {
		let commas = |n: usize| format!("SELECT * FROM {}", table_list(n));
		assert!(parse_statement(&commas(100)).is_ok());
		let err = parse_statement(&commas(200)).expect_err("200 listed tables must hit the limit");
		assert_eq!(err.kind, ErrorKind::RecursionLimitExceeded);

		let parens =
			|n: usize| format!("SELECT * FROM {}SELECT 1{} AS s", "(".repeat(n), ")".repeat(n));
		assert!(parse_statement(&parens(100)).is_ok());
		let err = parse_statement(&parens(200)).expect_err("200 parentheses must hit the limit");
		assert_eq!(err.kind, ErrorKind::RecursionLimitExceeded);
	});
}
