//! The keys of a select's columns, and the descriptor and plan that carry them.

use surrealdb_expr::expr::logical::{LogicalField, LogicalFields, LogicalOp};
use surrealdb_types::ToSql;

use super::output_keys;
use crate::catalog::EmptyCatalog;
use crate::dialect::DialectPolicy;
use crate::ir::BoundSelect;
use crate::{ResultDescriptor, binder};

fn pg_bound(sql: &str) -> BoundSelect {
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse");
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	binder::pgsql::bind_select(select, &EmptyCatalog, &DialectPolicy::postgres(), None)
		.expect("bind")
}

fn my_bound(sql: &str) -> BoundSelect {
	let stmts = surrealdb_mysql::parse_statements(sql).expect("parse");
	let surrealdb_mysql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	binder::mysql::bind_select(select, &EmptyCatalog, &DialectPolicy::mysql(), None).expect("bind")
}

fn pg_keys(sql: &str) -> Option<Vec<String>> {
	output_keys(&pg_bound(sql).targets)
}

fn my_keys(sql: &str) -> Option<Vec<String>> {
	output_keys(&my_bound(sql).targets)
}

fn describe(select: &BoundSelect, policy: &DialectPolicy) -> ResultDescriptor {
	select.describe(&EmptyCatalog, policy)
}

fn pg_plan(sql: &str) -> LogicalOp {
	let bound = pg_bound(sql);
	crate::lower_bound_select(&bound, &DialectPolicy::postgres()).expect("lower")
}

fn my_plan(sql: &str) -> LogicalOp {
	let bound = my_bound(sql);
	crate::lower_bound_select(&bound, &DialectPolicy::mysql()).expect("lower")
}

/// The aliases of the fields of a projection, or the field's own text where it has none.
fn fields(op: &LogicalOp) -> Vec<String> {
	let LogicalOp::Project(project) = op else {
		panic!("expected Project, got {op:?}");
	};
	let LogicalFields::Select(fields) = &project.fields else {
		panic!("expected Select fields");
	};
	fields
		.iter()
		.map(|field| match field {
			LogicalField::All => "*".to_string(),
			LogicalField::Single {
				expr,
				alias,
				..
			} => match alias {
				Some(alias) => format!("{expr} AS {}", alias.to_sql()),
				None => expr.to_string(),
			},
		})
		.collect()
}

fn strings(keys: &[&str]) -> Option<Vec<String>> {
	Some(keys.iter().map(|key| key.to_string()).collect())
}

#[test]
fn columns_that_share_a_name_are_keyed_by_position() {
	assert_eq!(
		pg_keys("SELECT 1 AS a, 2 AS a, 3 AS a"),
		strings(&["?column?1", "?column?2", "?column?3"])
	);
	assert_eq!(
		pg_keys("SELECT x.k, y.k, x.v FROM x JOIN y ON x.k = y.k"),
		strings(&["?column?1", "?column?2", "v"])
	);
	assert_eq!(pg_keys("SELECT count(*), count(v) FROM t"), strings(&["?column?1", "?column?2"]));
	assert_eq!(pg_keys("SELECT k, v, k FROM t"), strings(&["?column?1", "v", "?column?3"]));
}

#[test]
fn columns_without_a_name_are_keyed_by_position() {
	assert_eq!(
		pg_keys("SELECT 1, 'a', NULL, 2 + 3"),
		strings(&["?column?1", "?column?2", "?column?3", "?column?4"])
	);
	assert_eq!(pg_keys("SELECT k + 1, v FROM t"), strings(&["?column?1", "v"]));
}

#[test]
fn a_name_that_no_other_column_has_is_kept() {
	assert_eq!(
		pg_keys("SELECT k AS x, v, count(*) FROM t GROUP BY k, v"),
		strings(&["x", "v", "count"])
	);
}

#[test]
fn a_key_never_takes_the_name_of_another_column() {
	assert_eq!(pg_keys("SELECT 1, 2 AS \"?column?1\""), strings(&["?column?1_", "?column?1"]));
}

#[test]
fn variables_are_named_by_the_variable() {
	assert_eq!(my_keys("SELECT @a, @b"), strings(&["@a", "@b"]));
	assert_eq!(my_keys("SELECT @a, @a"), strings(&["?column?1", "?column?2"]));
	assert_eq!(
		my_keys("SELECT @@version, @@global.version"),
		strings(&["@@version", "@@global.version"])
	);
}

#[test]
fn a_select_with_a_wildcard_has_no_keys() {
	assert_eq!(pg_keys("SELECT * FROM t"), None);
	assert_eq!(pg_keys("SELECT *, 1 FROM t"), None);
	assert_eq!(my_keys("SELECT t.*, k FROM t"), None);
	// A column that is merely named `*` is an ordinary column.
	assert_eq!(my_keys("SELECT `*` FROM t"), strings(&["?column?1"]));
}

#[test]
fn the_descriptor_names_a_column_as_a_client_is_shown_it_and_keys_it_apart() {
	let policy = DialectPolicy::postgres();
	let descriptor =
		describe(&pg_bound("SELECT x.k, y.k, x.v, 1 FROM x JOIN y ON x.k = y.k"), &policy);
	let columns: Vec<_> = descriptor
		.columns
		.iter()
		.map(|column| (column.name.as_str(), column.key.as_deref()))
		.collect();
	assert_eq!(
		columns,
		[
			("k", Some("?column?1")),
			("k", Some("?column?2")),
			("v", Some("v")),
			("?column?", Some("?column?4"))
		]
	);
	let descriptor = describe(&pg_bound("SELECT *, 1 FROM t"), &policy);
	assert!(descriptor.columns.iter().all(|column| column.key.is_none()));
}

#[test]
fn the_plan_projects_each_column_under_its_key() {
	assert_eq!(
		fields(&pg_plan("SELECT 1, 'a', 2 AS \"?column?1\"")),
		["1 AS `?column?1_`", "'a' AS `?column?2`", "2 AS `?column?1`"]
	);
	let op = my_plan("SELECT @a, @a");
	assert_eq!(fields(&op), ["$user_var_a AS `?column?1`", "$user_var_a AS `?column?2`"]);
}

#[test]
fn a_wildcard_select_is_projected_under_the_names_the_engine_gives() {
	assert_eq!(fields(&pg_plan("SELECT *, 1 FROM t")), ["*", "record::id(id) AS id", "1"]);
}

#[test]
fn a_derived_table_is_read_by_the_names_its_columns_have() {
	let LogicalOp::Project(project) = pg_plan("SELECT s.x FROM (SELECT 1 AS x, 2) s") else {
		panic!("expected Project");
	};
	let LogicalOp::Subquery(subquery) = project.input.as_ref() else {
		panic!("expected Subquery, got {:?}", project.input);
	};
	assert_eq!(fields(&subquery.input), ["1 AS x", "2"]);
}

#[test]
fn the_later_queries_of_a_set_operation_are_projected_under_the_keys_of_the_first() {
	let LogicalOp::UnionAll(union) =
		pg_plan("SELECT 1, 2 UNION ALL SELECT 3, 4 UNION ALL SELECT 5, 6")
	else {
		panic!("expected UnionAll");
	};
	let expected = ["1 AS `?column?1`", "2 AS `?column?2`"];
	assert_eq!(union.inputs.len(), 3);
	assert_eq!(fields(&union.inputs[0]), expected);
	assert_eq!(fields(&union.inputs[1]), ["3 AS `?column?1`", "4 AS `?column?2`"]);
	assert_eq!(fields(&union.inputs[2]), ["5 AS `?column?1`", "6 AS `?column?2`"]);

	// A later column that is named takes the key the first query gives it.
	let LogicalOp::UnionAll(union) =
		pg_plan("SELECT a.k, b.k FROM x a, y b UNION ALL SELECT k, w FROM z")
	else {
		panic!("expected UnionAll");
	};
	assert_eq!(fields(&union.inputs[1]), ["k AS `?column?1`", "w AS `?column?2`"]);
}

#[test]
fn an_intersection_compares_the_keys_of_both_queries() {
	let LogicalOp::Distinct(distinct) = pg_plan("SELECT 1, 2 INTERSECT SELECT 3, 4") else {
		panic!("expected Distinct");
	};
	let LogicalOp::Join(join) = distinct.input.as_ref() else {
		panic!("expected Join, got {:?}", distinct.input);
	};
	// The columns are the one key of the join, an array of both of them on each side.
	let on: Vec<_> = join.on.iter().map(|(l, r)| (l.to_string(), r.to_string())).collect();
	assert_eq!(on.len(), 1);
	for side in [&on[0].0, &on[0].1] {
		assert!(side.contains("?column?1") && side.contains("?column?2"), "{on:?}");
	}
}

#[test]
fn the_clauses_of_an_aggregation_read_the_keys_of_its_columns() {
	let op = pg_plan(
		"SELECT k, count(*), count(*) FROM t GROUP BY k HAVING count(*) > 1 ORDER BY count(*)",
	);
	let LogicalOp::Project(project) = op else {
		panic!("expected Project");
	};
	let LogicalOp::Sort(sort) = *project.input else {
		panic!("expected Sort");
	};
	let LogicalOp::Filter(having) = *sort.input else {
		panic!("expected Filter");
	};
	let LogicalOp::Aggregate(aggregate) = *having.input else {
		panic!("expected Aggregate");
	};
	let LogicalFields::Select(columns) = &aggregate.fields else {
		panic!("expected Select fields");
	};
	let aliases: Vec<_> = columns
		.iter()
		.map(|column| column.output_path().expect("a named column").to_sql())
		.collect();
	assert_eq!(aliases, ["k", "`?column?2`", "`?column?3`"]);
	assert!(having.predicate.to_string().contains("?column?2"), "{}", having.predicate);
}
