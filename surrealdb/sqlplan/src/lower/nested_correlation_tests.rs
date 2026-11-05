//! A subquery reads the columns of the query directly above it through `$parent`. A column of a
//! query two or more levels above is read from a parameter that the query one level below the
//! column's query binds to the value when it is entered, so that every query nested in it reads
//! the value by name.

use surrealdb_expr::expr::logical::LogicalOp;

use crate::catalog::EmptyCatalog;
use crate::dialect::DialectPolicy;
use crate::error::PlanError;

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

/// The plan of `sql` as text, in both dialects.
fn plans(sql: &str) -> [(&'static str, String); 2] {
	[
		("postgres", pg(sql).unwrap_or_else(|err| panic!("postgres: {sql}: {err}")).to_string()),
		("mysql", my(sql).unwrap_or_else(|err| panic!("mysql: {sql}: {err}")).to_string()),
	]
}

/// The parameters the plan binds, each with the expression it is bound to, in the order the plan
/// first binds them. A three-valued `AND` or `NOT` names its operand more than once, so a subquery
/// in one is bound where each copy of it is evaluated: each distinct binding is listed once.
fn bindings(plan: &str) -> Vec<(String, String)> {
	let lines: Vec<&str> = plan.lines().map(str::trim).collect();
	let mut found: Vec<(String, String)> = Vec::new();
	for pair in lines.windows(2) {
		let (Some(name), Some(value)) = (
			pair[0].strip_prefix("Let[$").and_then(|name| name.strip_suffix(']')),
			pair[1].strip_prefix("Eval[").and_then(|value| value.strip_suffix(']')),
		) else {
			continue;
		};
		let binding = (name.to_owned(), value.to_owned());
		if !found.contains(&binding) {
			found.push(binding);
		}
	}
	found
}

/// How many times the plan names the parameter `name` other than where it binds it. A comparison
/// names an operand more than once, so this tells whether the parameter is read and not how often.
fn reads(plan: &str, name: &str) -> usize {
	plan.matches(&format!("${name}")).count() - plan.matches(&format!("Let[${name}]")).count()
}

fn bound(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
	pairs.iter().map(|(name, value)| (name.to_string(), value.to_string())).collect()
}

#[test]
fn a_column_of_the_query_directly_above_is_read_from_parent_and_binds_nothing() {
	for sql in [
		"SELECT a FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.b = t.a)",
		"SELECT a FROM t WHERE a IN (SELECT b FROM u WHERE u.c = t.d)",
		"SELECT a FROM t WHERE (SELECT max(b) FROM u WHERE u.c = t.d) > 1",
		// Each level reads the level above it, however deep the nesting.
		"SELECT a FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.b = t.a AND EXISTS (SELECT 1 FROM v WHERE v.c = u.b AND EXISTS (SELECT 1 FROM w WHERE w.d = v.c)))",
		"SELECT a FROM t WHERE (SELECT t.a) = 1",
	] {
		for (dialect, plan) in plans(sql) {
			assert!(plan.contains("$parent."), "{dialect}: {sql}: {plan}");
			assert!(bindings(&plan).is_empty(), "{dialect}: {sql}: {plan}");
			assert!(!plan.contains("__sql_outer_"), "{dialect}: {sql}: {plan}");
			assert!(!plan.contains("$parent.parent"), "{dialect}: {sql}: {plan}");
		}
	}
}

#[test]
fn a_column_two_levels_up_is_bound_by_the_query_below_its_own() {
	// `t.id` belongs to the outermost query: the `u` query is entered with its row as `$parent`
	// and binds the value, which the `v` query two levels down reads by name. The `v` query reads
	// `u.id`, a column of the query directly above it, from `$parent`.
	for (sql, outer, inner) in [
		(
			"SELECT t.id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.o = t.id AND EXISTS (SELECT 1 FROM v WHERE v.o = t.id AND v.id = u.id))",
			"$parent.id",
			"$parent.id",
		),
		(
			"SELECT t.id FROM t WHERE t.id IN (SELECT u.o FROM u WHERE u.id IN (SELECT v.id FROM v WHERE v.o = t.id))",
			"$parent.id",
			"",
		),
		(
			"SELECT t.id FROM t WHERE (SELECT (SELECT v.id FROM v WHERE v.o = t.id) FROM u WHERE u.id = 1) = 1",
			"$parent.id",
			"",
		),
		(
			"SELECT t.id FROM t WHERE NOT EXISTS (SELECT 1 FROM u WHERE NOT EXISTS (SELECT 1 FROM v WHERE v.o = t.id))",
			"$parent.id",
			"",
		),
		(
			"SELECT t.id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE 12 = ANY (SELECT v.id FROM v WHERE v.o = t.id))",
			"$parent.id",
			"",
		),
	] {
		for (dialect, plan) in plans(sql) {
			assert_eq!(
				bindings(&plan),
				bound(&[("__sql_outer_0_0", outer)]),
				"{dialect}: {sql}: {plan}"
			);
			assert!(reads(&plan, "__sql_outer_0_0") > 0, "{dialect}: {sql}: {plan}");
			assert!(!plan.contains("$parent.parent"), "{dialect}: {sql}: {plan}");
			if !inner.is_empty() {
				assert!(plan.contains(inner), "{dialect}: {sql}: {plan}");
			}
		}
	}
}

#[test]
fn a_column_is_bound_once_however_many_times_and_from_however_deep_it_is_read() {
	// `v` reads `t.id` twice, two levels up, and `w` reads it three levels up: all read the one
	// parameter the `u` query binds.
	let sql = "SELECT t.id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE EXISTS (SELECT 1 FROM v WHERE v.o = t.id AND v.p = t.id AND EXISTS (SELECT 1 FROM w WHERE w.o = t.id)))";
	for (dialect, plan) in plans(sql) {
		assert_eq!(
			bindings(&plan),
			bound(&[("__sql_outer_0_0", "$parent.id")]),
			"{dialect}: {plan}"
		);
		assert!(reads(&plan, "__sql_outer_0_0") > 0, "{dialect}: {plan}");
	}
}

#[test]
fn each_column_read_is_bound_separately_in_the_order_it_is_first_read() {
	let sql = "SELECT t.id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE EXISTS (SELECT 1 FROM v WHERE v.o = t.id AND v.q = t.name AND v.r = t.id))";
	for (dialect, plan) in plans(sql) {
		assert_eq!(
			bindings(&plan),
			bound(&[("__sql_outer_0_0", "$parent.id"), ("__sql_outer_0_1", "$parent.name")]),
			"{dialect}: {plan}"
		);
		assert!(reads(&plan, "__sql_outer_0_0") > 0, "{dialect}: {plan}");
		assert!(reads(&plan, "__sql_outer_0_1") > 0, "{dialect}: {plan}");
	}
}

#[test]
fn every_query_binds_the_columns_of_the_query_above_it_that_the_queries_nested_in_it_read() {
	// `w` reads `t.id` three levels up and `u.id` two levels up, and `x` reads `v.id` two levels up
	// and `t.id` four levels up: `u` binds `t.id`, `v` binds `u.id` and `w` binds `v.id`.
	let sql = "SELECT t.id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE EXISTS (SELECT 1 FROM v WHERE EXISTS (SELECT 1 FROM w WHERE w.o = t.id AND w.p = u.id AND EXISTS (SELECT 1 FROM x WHERE x.q = v.id AND x.r = t.id))))";
	for (dialect, plan) in plans(sql) {
		assert_eq!(
			bindings(&plan),
			bound(&[
				("__sql_outer_0_0", "$parent.id"),
				("__sql_outer_1_0", "$parent.id"),
				("__sql_outer_2_0", "$parent.id"),
			]),
			"{dialect}: {plan}"
		);
		assert!(!plan.contains("$parent.parent"), "{dialect}: {plan}");
	}
}

#[test]
fn sibling_subqueries_bind_what_each_of_them_reads() {
	let sql = "SELECT t.id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE EXISTS (SELECT 1 FROM v WHERE v.o = t.id)) AND EXISTS (SELECT 1 FROM u WHERE EXISTS (SELECT 1 FROM v WHERE v.o = t.a))";
	for (dialect, plan) in plans(sql) {
		assert_eq!(
			bindings(&plan),
			bound(&[("__sql_outer_0_0", "$parent.id"), ("__sql_outer_0_0", "$parent.a")]),
			"{dialect}: {plan}"
		);
	}
}

#[test]
fn a_query_without_a_from_clause_is_a_level_of_its_own() {
	// The middle query has a row, an empty one, so the column is two levels above the inner one.
	for (dialect, plan) in plans("SELECT t.id, (SELECT (SELECT t.id)) FROM t") {
		assert_eq!(
			bindings(&plan),
			bound(&[("__sql_outer_0_0", "$parent.id")]),
			"{dialect}: {plan}"
		);
		assert!(reads(&plan, "__sql_outer_0_0") > 0, "{dialect}: {plan}");
	}
}

#[test]
fn a_column_is_bound_wherever_it_stands_in_the_inner_query() {
	for clause in [
		"SELECT t.id FROM v",
		"SELECT 1 FROM v WHERE v.o = t.id",
		"SELECT 1 FROM v ORDER BY t.id",
		"SELECT v.o FROM v GROUP BY v.o HAVING v.o = t.id",
		"SELECT v.o FROM v GROUP BY t.id, v.o",
		"SELECT sum(t.id) FROM v",
	] {
		let sql =
			format!("SELECT t.id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE EXISTS ({clause}))");
		for (dialect, plan) in plans(&sql) {
			assert_eq!(
				bindings(&plan),
				bound(&[("__sql_outer_0_0", "$parent.id")]),
				"{dialect}: {sql}: {plan}"
			);
			assert!(reads(&plan, "__sql_outer_0_0") >= 1, "{dialect}: {sql}: {plan}");
			assert!(!plan.contains("$parent.parent"), "{dialect}: {sql}: {plan}");
		}
	}
}

#[test]
fn a_column_is_bound_for_a_derived_table_and_a_common_table_expression_inside_the_query_below() {
	// A derived table is bound against the scope of the query it stands in, so a column of the
	// outermost query read there is two levels up from the query above it.
	for sql in [
		"SELECT t.id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE EXISTS (SELECT 1 FROM (SELECT t.id AS x FROM v) d WHERE d.x = u.id))",
		"SELECT t.id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE EXISTS (WITH c AS (SELECT v.id FROM v WHERE v.o = t.id) SELECT 1 FROM c WHERE c.id = u.id))",
		"SELECT t.id FROM t WHERE EXISTS (WITH c AS (SELECT v.id FROM v WHERE v.o = t.id) SELECT 1 FROM c)",
	] {
		for (dialect, plan) in plans(sql) {
			assert_eq!(
				bindings(&plan),
				bound(&[("__sql_outer_0_0", "$parent.id")]),
				"{dialect}: {sql}: {plan}"
			);
			assert!(reads(&plan, "__sql_outer_0_0") >= 1, "{dialect}: {sql}: {plan}");
			assert!(!plan.contains("$parent.parent"), "{dialect}: {sql}: {plan}");
		}
	}
}

#[test]
fn a_column_of_a_joined_relation_is_bound_from_the_field_of_the_join_it_is_under() {
	// The row of the outermost query is the row of a join: the first relation's columns are the
	// row's own and a joined relation's are under its alias.
	let sql = "SELECT t.id FROM t JOIN j ON j.a = t.id WHERE EXISTS (SELECT 1 FROM u WHERE EXISTS (SELECT 1 FROM v WHERE v.o = j.id AND v.p = t.id))";
	for (dialect, plan) in plans(sql) {
		assert_eq!(
			bindings(&plan),
			bound(&[("__sql_outer_0_0", "$parent._join0.id"), ("__sql_outer_0_1", "$parent.id")]),
			"{dialect}: {plan}"
		);
	}
}
