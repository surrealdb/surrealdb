//! Comparing the rows of a statement without regard to their order.
//!
//! A run that executes under parallel scans (`run --unordered-rows`) grades the rows of a
//! statement that nothing orders as a multiset: the rows a test expects, in any order. Only the
//! rows of the result are compared that way; a value nested in a row is compared exactly.

use surrealdb_expr::expr::Expr;
use surrealdb_expr::expr::literal::Literal;
use surrealdb_expr::parser::{ParserSettings, TopLevelExpr};
use surrealdb_types::Value;

use super::cmp::{RoughlyEq, RoughlyEqConfig};

/// For each statement of `source`, in the order the run answers them, whether its rows may come in
/// any order: a `SELECT` of a table or a record range with no `ORDER BY`, `LIMIT`, `START` or
/// `EXPLAIN`, whose rows nothing but the scan orders. `None` where the source does not parse, and
/// so nothing may be compared out of order.
///
/// A transaction's `BEGIN`, `COMMIT` and `CANCEL` each answer a result, `NONE`, like any other
/// statement, and their rows are never graded out of order.
pub fn unordered_statements(source: &str) -> Option<Vec<bool>> {
	let ast =
		surrealdb_expr::parser::query_with_settings(source, ParserSettings::default()).ok()?;
	Some(
		ast.expressions
			.iter()
			.map(|statement| match statement {
				TopLevelExpr::Expr(Expr::Select(select)) => {
					select.explain.is_none()
						&& select.order.is_none()
						&& select.limit.is_none()
						&& select.start.is_none()
						&& select.what.iter().all(|source| {
							matches!(source, Expr::Table(_) | Expr::Literal(Literal::RecordId(_)))
						})
				}
				_ => false,
			})
			.collect(),
	)
}

/// Whether `got` holds the rows of `expected`, in any order, when both are arrays. A row of
/// `got` answers at most one row of `expected`.
pub fn same_rows(expected: &Value, got: &Value, config: &RoughlyEqConfig) -> bool {
	let (Value::Array(expected), Value::Array(got)) = (expected, got) else {
		return false;
	};
	if expected.len() != got.len() {
		return false;
	}
	let mut unmatched: Vec<&Value> = got.iter().collect();
	expected.iter().all(|row| {
		match unmatched.iter().position(|candidate| row.roughly_equal(candidate, config)) {
			Some(at) => {
				unmatched.swap_remove(at);
				true
			}
			None => false,
		}
	})
}

#[cfg(test)]
mod tests {
	use surrealdb_types::Number;

	use super::*;

	fn int(number: i64) -> Value {
		Value::Number(Number::Int(number))
	}

	fn rows(numbers: &[i64]) -> Value {
		Value::Array(numbers.iter().copied().map(int).collect())
	}

	#[test]
	fn a_select_with_no_order_or_paging_is_unordered() {
		let statements = unordered_statements(
			"CREATE t:1; SELECT * FROM t; SELECT * FROM t ORDER BY id; SELECT * FROM t LIMIT 2; \
			 SELECT * FROM t START 1; SELECT * FROM t:1..3; SELECT * FROM (SELECT * FROM t); \
			 SELECT * FROM t EXPLAIN; SELECT * FROM t EXPLAIN FULL; RETURN 1;",
		);
		assert_eq!(
			statements,
			Some(vec![false, true, false, false, false, true, false, false, false, false])
		);
	}

	#[test]
	fn a_transaction_answers_a_result_for_each_of_its_brackets() {
		let statements = unordered_statements("BEGIN; SELECT * FROM t; COMMIT;");
		assert_eq!(statements, Some(vec![false, true, false]));
	}

	#[test]
	fn a_source_that_does_not_parse_compares_nothing_out_of_order() {
		assert_eq!(unordered_statements("SELECT FROM FROM"), None);
	}

	#[test]
	fn reordered_rows_are_the_same_rows() {
		let config = RoughlyEqConfig::all();
		assert!(same_rows(&rows(&[1, 2, 3]), &rows(&[3, 1, 2]), &config));
		assert!(same_rows(&rows(&[1, 1, 2]), &rows(&[2, 1, 1]), &config));
	}

	#[test]
	fn different_rows_are_not_the_same_rows() {
		let config = RoughlyEqConfig::all();
		assert!(!same_rows(&rows(&[1, 2, 3]), &rows(&[1, 2, 2]), &config), "a different multiset");
		assert!(!same_rows(&rows(&[1, 1, 2]), &rows(&[1, 2, 2]), &config), "a row repeated");
		assert!(!same_rows(&rows(&[1, 2, 3]), &rows(&[1, 2]), &config), "a row missing");
		assert!(!same_rows(&int(1), &int(1), &config), "not rows");
	}
}
