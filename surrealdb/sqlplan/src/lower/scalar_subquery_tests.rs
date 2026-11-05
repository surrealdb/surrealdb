//! A subquery used as a value: the rows it reads, the error it raises for more than one row or
//! column, and the quantified comparisons that are lowered through the same path.

use surrealdb_expr::expr::logical::LogicalOp;

use crate::catalog::EmptyCatalog;
use crate::dialect::DialectPolicy;
use crate::error::PlanError;
use crate::lower::link_tests::{predicate, projection};

const PG_MANY_ROWS: &str = "more than one row returned by a subquery used as an expression";
const MY_MANY_ROWS: &str = "Subquery returns more than 1 row";

fn pg(sql: &str) -> Result<LogicalOp, PlanError> {
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse");
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	crate::bind_and_lower_pgsql(select, &EmptyCatalog, &DialectPolicy::postgres())
}

fn my(sql: &str) -> Result<LogicalOp, PlanError> {
	let stmts = surrealdb_mysql::parse_statements(sql).expect("parse");
	let surrealdb_mysql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	crate::bind_and_lower_mysql(select, &EmptyCatalog, &DialectPolicy::mysql())
}

/// The item of the select list of `sql` in both dialects, with the message each raises for more
/// than one row.
fn item(sql: &str) -> [(String, &'static str); 2] {
	let item = |op: Result<LogicalOp, PlanError>| {
		let mut items = projection(&op.expect("lower"));
		assert_eq!(items.len(), 1, "{sql}");
		items.remove(0)
	};
	[(item(pg(sql)), PG_MANY_ROWS), (item(my(sql)), MY_MANY_ROWS)]
}

/// The position of `needle` in `text`.
fn at(text: &str, needle: &str) -> usize {
	text.find(needle).unwrap_or_else(|| panic!("no `{needle}` in {text}"))
}

#[test]
fn a_subquery_used_as_a_value_reads_two_rows_at_most_and_fails_on_the_second() {
	for (text, message) in item("SELECT (SELECT k FROM t)") {
		assert!(text.contains("Limit[LIMIT 2]"), "{text}");
		assert!(text.contains(&format!("THROW '{message}'")), "{text}");
		assert!(text.contains("array::len() > 1"), "{text}");
	}
}

#[test]
fn a_limit_of_its_own_is_kept() {
	for (text, _) in item("SELECT (SELECT k FROM t LIMIT 5)") {
		assert!(text.contains("Limit[LIMIT 5]"), "{text}");
		assert!(!text.contains("LIMIT 2"), "{text}");
	}
}

#[test]
fn the_check_applies_to_the_rows_a_limit_of_its_own_leaves() {
	for limit in [1, 2, 5, 100] {
		for (text, message) in item(&format!("SELECT (SELECT k FROM t ORDER BY k LIMIT {limit})")) {
			assert!(text.contains(&format!("Limit[LIMIT {limit}]")), "{text}");
			if limit != 2 {
				assert!(!text.contains("LIMIT 2"), "{text}");
			}
			assert!(at(&text, "Limit[") < at(&text, "Sort["), "{text}");
			assert!(text.contains("array::len() > 1"), "{text}");
			assert!(text.contains(&format!("THROW '{message}'")), "{text}");
		}
	}
	for (text, message) in item("SELECT (SELECT k FROM t ORDER BY k LIMIT 5 OFFSET 2)") {
		assert!(text.contains("Limit[LIMIT 5"), "{text}");
		assert!(text.contains("START 2"), "{text}");
		assert!(!text.contains("LIMIT 2"), "{text}");
		assert!(text.contains(&format!("THROW '{message}'")), "{text}");
	}
}

#[test]
fn a_limit_of_its_own_in_a_predicate_is_kept_and_checked() {
	for (op, message) in [
		(pg("SELECT a FROM t WHERE a = (SELECT b FROM u ORDER BY b LIMIT 1)"), PG_MANY_ROWS),
		(my("SELECT a FROM t WHERE a = (SELECT b FROM u ORDER BY b LIMIT 1)"), MY_MANY_ROWS),
	] {
		let text = predicate(&op.expect("lower"));
		assert!(text.contains("Limit[LIMIT 1]"), "{text}");
		assert!(!text.contains("LIMIT 2"), "{text}");
		assert!(text.contains(&format!("THROW '{message}'")), "{text}");
	}
	for (op, message) in [
		(pg("SELECT a FROM t WHERE a = (SELECT b FROM u ORDER BY b LIMIT 5)"), PG_MANY_ROWS),
		(my("SELECT a FROM t WHERE a = (SELECT b FROM u ORDER BY b LIMIT 5)"), MY_MANY_ROWS),
	] {
		let text = predicate(&op.expect("lower"));
		assert!(text.contains("Limit[LIMIT 5]"), "{text}");
		assert!(!text.contains("LIMIT 2"), "{text}");
		assert!(text.contains(&format!("THROW '{message}'")), "{text}");
	}
}

#[test]
fn an_offset_alone_is_read_with_room_for_two_rows() {
	// MySQL has no OFFSET without a LIMIT.
	let text = projection(&pg("SELECT (SELECT k FROM t OFFSET 1)").expect("lower")).remove(0);
	assert!(text.contains("LIMIT 2"), "{text}");
	assert!(text.contains("START 1"), "{text}");
}

#[test]
fn the_limit_applies_after_the_sort_and_to_a_set_operation_as_a_whole() {
	for (text, _) in item("SELECT (SELECT k FROM t ORDER BY k)") {
		assert!(at(&text, "Limit[LIMIT 2]") < at(&text, "Sort["), "{text}");
	}
	for (text, _) in item("SELECT (SELECT k FROM t UNION ALL SELECT k FROM u)") {
		assert!(at(&text, "Limit[LIMIT 2]") < at(&text, "UnionAll"), "{text}");
	}
	for (text, _) in item("SELECT (SELECT k FROM t UNION ALL SELECT k FROM u LIMIT 1)") {
		assert!(at(&text, "Limit[LIMIT 1]") < at(&text, "UnionAll"), "{text}");
		assert!(!text.contains("LIMIT 2"), "{text}");
	}
}

#[test]
fn an_aggregate_is_limited_after_it_answers() {
	for (text, _) in item("SELECT (SELECT max(k) FROM t)") {
		assert!(at(&text, "Limit[LIMIT 2]") < at(&text, "Aggregate["), "{text}");
	}
}

#[test]
fn a_subquery_in_a_predicate_is_read_the_same_way() {
	for (op, message) in [
		(pg("SELECT a FROM t WHERE a = (SELECT b FROM u)"), PG_MANY_ROWS),
		(my("SELECT a FROM t WHERE a = (SELECT b FROM u)"), MY_MANY_ROWS),
	] {
		let text = predicate(&op.expect("lower"));
		assert!(text.contains("Limit[LIMIT 2]"), "{text}");
		assert!(text.contains(&format!("THROW '{message}'")), "{text}");
	}
}

#[test]
fn a_membership_or_existence_test_is_not_a_value_and_does_not_fail_on_many_rows() {
	for op in [
		pg("SELECT a FROM t WHERE a IN (SELECT b FROM u)"),
		my("SELECT a FROM t WHERE a IN (SELECT b FROM u)"),
		pg("SELECT a FROM t WHERE a = ANY (SELECT b FROM u)"),
		my("SELECT a FROM t WHERE a = ANY (SELECT b FROM u)"),
		pg("SELECT a FROM t WHERE EXISTS (SELECT b FROM u)"),
		my("SELECT a FROM t WHERE EXISTS (SELECT b FROM u)"),
	] {
		let text = predicate(&op.expect("lower"));
		assert!(!text.contains("THROW"), "{text}");
	}
}

#[test]
fn a_subquery_used_as_a_value_with_several_columns_is_refused() {
	for sql in [
		"SELECT (SELECT a, b FROM t)",
		"SELECT a FROM t WHERE a = (SELECT a, b FROM u)",
		"SELECT (SELECT a, b FROM t UNION ALL SELECT a, b FROM u)",
		"SELECT (SELECT 1, 2)",
	] {
		let err = pg(sql).expect_err(sql).to_string();
		assert_eq!(err, "planning error: subquery must return only one column", "{sql}");
		let err = my(sql).expect_err(sql).to_string();
		assert_eq!(err, "planning error: Operand should contain 1 column(s)", "{sql}");
	}
}

#[test]
fn a_wildcard_is_not_refused_because_its_columns_are_read_with_the_rows() {
	let sql = "SELECT (SELECT * FROM t)";
	pg(sql).expect(sql);
	my(sql).expect(sql);
}

#[test]
fn a_quantified_comparison_over_a_plain_subquery_is_lowered_over_one_aggregate() {
	for sql in [
		"SELECT a FROM t WHERE a > ALL (SELECT b FROM u)",
		"SELECT a FROM t WHERE a < ANY (SELECT b FROM u WHERE b > 1)",
		"SELECT a FROM t WHERE a <= SOME (SELECT DISTINCT b FROM u ORDER BY b)",
	] {
		let text = predicate(&pg(sql).expect(sql));
		assert!(text.contains("array::m"), "{text}");
		assert!(text.contains("THROW"), "{text}");
		let text = predicate(&my(sql).expect(sql));
		assert!(text.contains("array::m"), "{text}");
	}
}

#[test]
fn a_quantified_comparison_over_a_subquery_that_groups_limits_or_combines_is_refused() {
	for subquery in [
		"SELECT b FROM u GROUP BY b",
		"SELECT b FROM u GROUP BY b HAVING max(b) > 1",
		"SELECT max(b) FROM u HAVING max(b) > 1",
		"SELECT b FROM u ORDER BY b LIMIT 1",
		"SELECT b FROM u UNION ALL SELECT b FROM v",
	] {
		let sql = format!("SELECT a FROM t WHERE a > ALL ({subquery})");
		for err in [pg(&sql).expect_err(&sql), my(&sql).expect_err(&sql)] {
			assert!(is_quantified_refusal(&err), "{sql}: {err}");
		}
	}
	// MySQL has no OFFSET without a LIMIT.
	let sql = "SELECT a FROM t WHERE a > ALL (SELECT b FROM u ORDER BY b OFFSET 1)";
	let err = pg(sql).expect_err(sql);
	assert!(is_quantified_refusal(&err), "{sql}: {err}");
}

fn is_quantified_refusal(err: &PlanError) -> bool {
	matches!(err, PlanError::UnsupportedExpression(what)
		if what.starts_with("a quantified comparison over a subquery with"))
}
