//! The condition of an equality join: a key that is NULL or missing pairs with nothing. The engine
//! pairs a NULL key with a NULL one, so the condition of the join holds for no pair whose key is
//! NULL or NONE.

use surrealdb_expr::expr::logical::{JoinKind, LogicalJoin, LogicalOp};

use crate::catalog::EmptyCatalog;
use crate::lower::link_tests::{join_keys, my_select_in, pg_select_in};

/// The first join of the pipeline.
fn first_join(op: &LogicalOp) -> Option<&LogicalJoin> {
	if let LogicalOp::Join(join) = op {
		return Some(join);
	}
	op.children().iter().find_map(first_join)
}

/// The condition of the first join of the select, in both dialects.
fn conditions(sql: &str) -> [Option<String>; 2] {
	let condition =
		|op: LogicalOp| first_join(&op).expect("a join").filter.as_ref().map(ToString::to_string);
	[condition(pg_select_in(sql, &EmptyCatalog)), condition(my_select_in(sql, &EmptyCatalog))]
}

/// The guard that holds for a key that is neither NULL nor NONE.
fn present(key: &str) -> String {
	format!("{key} != NULL AND {key} != NONE")
}

#[test]
fn the_key_of_an_equality_is_judged_not_to_be_null_for_each_pair() {
	for sql in [
		"SELECT 1 FROM ja JOIN jb ON ja.k = jb.k",
		"SELECT 1 FROM ja LEFT JOIN jb ON ja.k = jb.k",
		"SELECT 1 FROM ja JOIN jb ON jb.k = ja.k",
	] {
		for condition in conditions(sql) {
			assert_eq!(condition, Some(present("_join0.k")), "{sql}");
		}
	}
}

#[test]
fn the_probed_key_is_judged_when_the_other_key_is_not_a_field_of_the_right_row() {
	for condition in conditions("SELECT 1 FROM ja JOIN jb ON ja.k = jb.k + 1") {
		assert_eq!(condition, Some(present("k")));
	}
	for condition in conditions("SELECT 1 FROM ja JOIN jb ON ja.k + 1 = jb.j + 1") {
		let condition = condition.expect("a guard");
		assert!(condition.starts_with("(IF k = NULL"), "the key is an expression: {condition}");
		assert!(!condition.contains("_join0.j != NULL"), "{condition}");
	}
}

#[test]
fn a_key_built_from_a_record_link_is_not_judged_through_the_record() {
	let catalog = crate::lower::link_tests::catalog();
	for op in [
		pg_select_in("SELECT 1 FROM task JOIN emp ON task.dept = emp.dept", &catalog),
		pg_select_in("SELECT 1 FROM task JOIN dept ON task.dept = dept.id", &catalog),
		my_select_in("SELECT 1 FROM task JOIN emp ON task.dept = emp.dept", &catalog),
	] {
		let condition =
			first_join(&op).and_then(|join| join.filter.as_ref()).expect("a condition").to_string();
		assert!(!condition.contains("type::record"), "{condition}");
		assert!(condition.starts_with("_join0."), "{condition}");
	}
}

#[test]
fn each_key_of_a_composite_key_is_guarded_in_order_and_the_residual_follows() {
	let guards = format!("{} AND {}", present("_join0.k"), present("_join0.j"));
	for condition in conditions("SELECT 1 FROM ja JOIN jb ON ja.k = jb.k AND ja.j = jb.j") {
		assert_eq!(condition, Some(guards.clone()));
	}
	let sql = "SELECT 1 FROM ja LEFT JOIN jb ON ja.k = jb.k AND jb.w > 1 AND ja.j = jb.j";
	for condition in conditions(sql) {
		let condition = condition.expect("a condition");
		assert!(condition.starts_with(&guards), "{condition}");
		assert!(condition.ends_with("_join0.w > 1 END)"), "{condition}");
	}
}

#[test]
fn a_using_key_is_guarded() {
	for condition in conditions("SELECT 1 FROM ja JOIN jb USING (k, j)") {
		let guards = format!("{} AND {}", present("_join0.k"), present("_join0.j"));
		assert_eq!(condition, Some(guards));
	}
}

#[test]
fn a_join_without_an_equality_key_has_no_guard() {
	for condition in conditions("SELECT 1 FROM ja JOIN jb ON ja.k < jb.k") {
		assert!(!condition.expect("a condition").contains("!= NULL AND"));
	}
	for condition in conditions("SELECT 1 FROM ja CROSS JOIN jb") {
		assert_eq!(condition, None);
	}
	let sql = "SELECT 1 FROM ja JOIN jb ON ja.k IS NOT DISTINCT FROM jb.k";
	let op = pg_select_in(sql, &EmptyCatalog);
	assert_eq!(join_keys(&op), []);
	assert!(first_join(&op).is_some_and(|join| join.filter.is_some()));
	let op = my_select_in("SELECT 1 FROM ja JOIN jb ON ja.k <=> jb.k", &EmptyCatalog);
	assert_eq!(join_keys(&op), []);
}

#[test]
fn the_rows_a_set_operation_compares_are_paired_when_both_are_null() {
	for sql in
		["SELECT a FROM t1 INTERSECT SELECT a FROM t2", "SELECT a FROM t1 EXCEPT SELECT a FROM t2"]
	{
		let op = pg_select_in(sql, &EmptyCatalog);
		let join = first_join(&op).expect("a join");
		assert!(matches!(join.kind, JoinKind::Semi | JoinKind::Anti), "{sql}");
		assert_eq!(join.filter, None, "{sql}");
	}
}
