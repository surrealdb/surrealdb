//! Recursion limits on table references: every way a `FROM` clause nests is bounded by
//! `ParserSettings::max_recursion_depth`, so a deep one is refused with the ordinary
//! recursion-limit error instead of overflowing the stack of the thread that parses it.

use surrealdb_pgsql::error::ErrorKind;
use surrealdb_pgsql::{
	ParserSettings, parse_statement, parse_statement_with_settings, parse_statements_with_settings,
};

const LIMIT: u32 = 16;

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
	for n in 1..=4 * LIMIT as usize {
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
	assert!(deepest <= LIMIT as usize, "a nesting of {deepest} exceeds the limit of {LIMIT}");
	deepest
}

fn join_on(i: usize) -> String {
	format!("t0.id = t{i}.id")
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
fn parenthesised_references() {
	// PostgreSQL requires a join inside the parentheses; the parser accepts a bare reference too,
	// and bounds it the same way.
	deepest_accepted(|n| format!("SELECT * FROM {}t0{}", "(".repeat(n), ")".repeat(n)));
	deepest_accepted(|n| {
		format!("SELECT * FROM {}t0 JOIN t1 ON t0.id = t1.id{}", "(".repeat(n), ")".repeat(n))
	});
}

#[test]
fn parenthesised_joins_nested_to_the_left() {
	deepest_accepted(|n| {
		let joins: String = (1..=n).map(|i| format!(" JOIN t{i} ON {})", join_on(i))).collect();
		format!("SELECT * FROM {}t0{joins}", "(".repeat(n))
	});
}

#[test]
fn parenthesised_joins_nested_to_the_right() {
	deepest_accepted(|n| {
		let head: String = (0..n).map(|i| format!("t{i} CROSS JOIN (")).collect();
		format!("SELECT * FROM {head}t{n} CROSS JOIN t{}{}", n + 1, ")".repeat(n))
	});
}

#[test]
fn joins_chained_to_the_left() {
	deepest_accepted(|n| {
		let joins: String = (1..=n).map(|i| format!(" JOIN t{i} ON {}", join_on(i))).collect();
		format!("SELECT * FROM t0{joins}")
	});
	deepest_accepted(|n| {
		let joins: String = (1..=n).map(|i| format!(" CROSS JOIN t{i}")).collect();
		format!("SELECT * FROM t0{joins}")
	});
}

#[test]
fn references_listed_with_commas() {
	// The binder joins a list one reference after another, so a long list nests as deep as a
	// chain of joins does.
	deepest_accepted(|n| format!("SELECT * FROM {}", table_list(n + 1)));
	deepest_accepted(|n| format!("UPDATE t SET a = 1 FROM {}", table_list(n + 1)));
	deepest_accepted(|n| format!("DELETE FROM t USING {}", table_list(n + 1)));
}

#[test]
fn a_list_of_joins_counts_both() {
	// A list of references that are each a join chain holds the chain of the list and the chain of
	// its deepest member at the same time.
	deepest_accepted(|n| {
		let member = "a JOIN b ON a.id = b.id JOIN c ON b.id = c.id";
		let list = vec![member; n + 1];
		format!("SELECT * FROM {}", list.join(", "))
	});
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
fn subqueries_in_parenthesised_joins() {
	deepest_accepted(|n| {
		let mut from = String::from("t");
		for i in 0..n {
			from = format!("((SELECT * FROM {from}) AS s{i} CROSS JOIN u{i})");
		}
		format!("SELECT * FROM {from}")
	});
}

#[test]
fn values_rows_in_from() {
	// The rows of a VALUES list become a chain of UNION ALL, one level per row.
	deepest_accepted(|n| {
		let rows: Vec<String> = (0..=n).map(|i| format!("({i})")).collect();
		format!("SELECT * FROM (VALUES {}) AS v", rows.join(", "))
	});
}

#[test]
fn values_nested_in_from() {
	deepest_accepted(|n| {
		let mut value = String::from("1");
		for i in 0..n {
			value = format!("(SELECT * FROM (VALUES ({value})) AS v{i})");
		}
		format!("SELECT {value}")
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
fn alias_column_lists_do_not_nest() {
	// A list of column names is flat: it takes no level, and its length is held by the work limit.
	let list = |n: usize| (0..n).map(|i| format!("c{i}")).collect::<Vec<_>>().join(", ");
	let sql = |n: usize| format!("SELECT * FROM (VALUES (1)) AS v({})", list(n));
	let columns = 8 * LIMIT as usize;
	assert!(parse_statement_with_settings(&sql(columns), settings()).is_ok());

	let tight = ParserSettings {
		max_work_count: 100,
		..ParserSettings::default()
	};
	let err = parse_statement_with_settings(&sql(1000), tight).expect_err("a long list is refused");
	assert_eq!(err.kind, ErrorKind::WorkLimitExceeded);
}

#[test]
fn levels_are_released_when_a_reference_is_complete() {
	// None of the levels taken for the parts of a statement outlives it, so a script of many
	// statements is never refused for its length.
	let one = "SELECT * FROM ((a JOIN b ON a.id = b.id) JOIN c ON b.id = c.id), \
	           (d JOIN e ON d.id = e.id), (SELECT * FROM (f JOIN g ON f.id = g.id)) AS h, \
	           (VALUES (1), (2)) AS i;";
	assert!(parse_statement_with_settings(one, settings()).is_ok());
	let script = one.repeat(50);
	assert!(parse_statements_with_settings(&script, settings()).is_ok());
}

#[test]
fn the_default_limit_bounds_every_nesting() {
	on_large_stack(|| {
		let parens = |n: usize| {
			format!("SELECT * FROM {}t0 JOIN t1 ON t0.id = t1.id{}", "(".repeat(n), ")".repeat(n))
		};
		assert!(parse_statement(&parens(100)).is_ok());
		let err = parse_statement(&parens(200)).expect_err("200 parentheses must hit the limit");
		assert_eq!(err.kind, ErrorKind::RecursionLimitExceeded);

		let commas = |n: usize| format!("SELECT * FROM {}", table_list(n));
		assert!(parse_statement(&commas(100)).is_ok());
		let err = parse_statement(&commas(200)).expect_err("200 listed tables must hit the limit");
		assert_eq!(err.kind, ErrorKind::RecursionLimitExceeded);
	});
}
