//! The plan of `INTERSECT` and `EXCEPT`, and what it refuses.

use surrealdb_expr::expr::logical::{
	JoinKind, LogicalDistinct, LogicalExpr, LogicalFields, LogicalJoin, LogicalLiteral, LogicalOp,
	LogicalProject, LogicalValues,
};
use surrealdb_types::ToSql;

use crate::catalog::EmptyCatalog;
use crate::dialect::DialectPolicy;
use crate::error::PlanError;
use crate::{binder, lower_bound_select};

fn pg_lower(sql: &str) -> Result<LogicalOp, PlanError> {
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse");
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let policy = DialectPolicy::postgres();
	let bound = binder::pgsql::bind_select(select, &EmptyCatalog, &policy, None).expect("bind");
	lower_bound_select(&bound, &policy)
}

fn my_lower(sql: &str) -> Result<LogicalOp, PlanError> {
	let stmts = surrealdb_mysql::parse_statements(sql).expect("parse");
	let surrealdb_mysql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let policy = DialectPolicy::mysql();
	let bound = binder::mysql::bind_select(select, &EmptyCatalog, &policy, None).expect("bind");
	lower_bound_select(&bound, &policy)
}

/// The join an `INTERSECT` or an `EXCEPT` of `sql` is answered by, under the `Distinct` that
/// answers each row once.
fn join_of(plan: LogicalOp) -> LogicalJoin {
	let LogicalOp::Distinct(distinct) = plan else {
		panic!("expected Distinct, got {plan:?}");
	};
	let LogicalOp::Join(join) = *distinct.input else {
		panic!("expected Join, got {:?}", distinct.input);
	};
	join
}

fn refusal(result: Result<LogicalOp, PlanError>) -> String {
	match result {
		Err(PlanError::UnsupportedExpression(message)) => message,
		other => panic!("expected a refusal, got {other:?}"),
	}
}

#[test]
fn a_set_operation_is_a_join_of_two_subqueries() {
	for (sql, kind) in [
		("SELECT a FROM t INTERSECT SELECT b FROM u", JoinKind::Semi),
		("SELECT a FROM t EXCEPT SELECT b FROM u", JoinKind::Anti),
	] {
		for plan in [pg_lower(sql), my_lower(sql)] {
			let join = join_of(plan.expect("lower"));
			assert_eq!(join.kind, kind, "{sql}");
			assert!(join.filter.is_none(), "{sql}");
			assert!(
				join.inputs.iter().all(|input| matches!(input, LogicalOp::Subquery(_))),
				"{sql}: {:?}",
				join.inputs
			);
		}
	}
}

#[test]
fn a_column_is_the_key_and_reads_an_absent_field_as_null() {
	let join = join_of(pg_lower("SELECT a FROM t INTERSECT SELECT b FROM u").expect("lower"));
	let [(left, right)] = join.on.as_slice() else {
		panic!("expected one key, got {:?}", join.on);
	};
	assert_eq!(left.to_sql(), "a ?? NULL");
	assert!(right.to_sql().ends_with(".a ?? NULL"), "{}", right.to_sql());
}

#[test]
fn the_columns_of_a_query_are_one_array_key() {
	let join =
		join_of(pg_lower("SELECT a, b, c FROM t EXCEPT SELECT x, y, z FROM u").expect("lower"));
	let [(left, right)] = join.on.as_slice() else {
		panic!("expected one key, got {:?}", join.on);
	};
	for key in [left, right] {
		let LogicalExpr::Array(columns) = key else {
			panic!("expected an array key, got {key:?}");
		};
		assert_eq!(columns.len(), 3);
	}
	// The second query is read under the names of the first.
	assert!(right.to_sql().contains(".b ?? NULL") && right.to_sql().contains(".c ?? NULL"));
}

#[test]
fn an_all_form_is_refused() {
	for (sql, keyword) in [
		("SELECT a FROM t INTERSECT ALL SELECT a FROM u", "INTERSECT"),
		("SELECT a FROM t EXCEPT ALL SELECT a FROM u", "EXCEPT"),
	] {
		for result in [pg_lower(sql), my_lower(sql)] {
			assert_eq!(
				refusal(result),
				format!("{keyword} ALL is not supported; use {keyword} to answer each row once")
			);
		}
	}
}

#[test]
fn a_wildcard_in_either_query_is_refused() {
	for sql in [
		"SELECT * FROM t INTERSECT SELECT a FROM u",
		"SELECT a FROM t EXCEPT SELECT * FROM u",
		"SELECT t.* FROM t EXCEPT SELECT a FROM u",
	] {
		for result in [pg_lower(sql), my_lower(sql)] {
			assert!(refusal(result).contains("with * in either query"), "{sql}");
		}
	}
}

#[test]
fn a_column_without_a_name_in_a_derived_table_is_refused() {
	let sql = "SELECT s.x FROM (SELECT a + 1 FROM t EXCEPT SELECT b + 1 FROM u) s";
	for result in [pg_lower(sql), my_lower(sql)] {
		assert!(refusal(result).contains("needs a name for every column"), "{sql}");
	}
	// An alias names it.
	let sql = "SELECT s.x FROM (SELECT a + 1 AS x FROM t EXCEPT SELECT b + 1 FROM u) s";
	assert!(pg_lower(sql).is_ok());
	assert!(my_lower(sql).is_ok());
}

#[test]
fn two_columns_of_one_name_in_a_derived_table_are_refused() {
	let sql = "SELECT s.x FROM (SELECT a AS x, b AS x FROM t INTERSECT SELECT c, d FROM u) s";
	for result in [pg_lower(sql), my_lower(sql)] {
		assert!(refusal(result).contains("a different name"), "{sql}");
	}
}

#[test]
fn a_subquery_that_answers_a_single_value_is_refused() {
	for sql in [
		"SELECT a FROM t WHERE a IN (SELECT a FROM t INTERSECT SELECT a FROM u)",
		"SELECT a FROM t WHERE EXISTS (SELECT a FROM t EXCEPT SELECT a FROM u)",
		"SELECT (SELECT a FROM t EXCEPT SELECT a FROM u)",
	] {
		for result in [pg_lower(sql), my_lower(sql)] {
			assert!(
				refusal(result).ends_with("in a subquery that answers a single value"),
				"{sql}"
			);
		}
	}
}

#[test]
fn a_subquery_in_the_from_clause_is_a_set_operation_of_rows() {
	let sql = "SELECT s.a FROM (SELECT a FROM t INTERSECT SELECT a FROM u) s";
	assert!(pg_lower(sql).is_ok());
	assert!(my_lower(sql).is_ok());
}

#[test]
fn rows_are_objects_when_the_query_projects_fields_and_not_when_it_projects_a_value() {
	let values = || {
		Box::new(LogicalOp::Values(LogicalValues {
			expr: LogicalExpr::Array(Vec::new()),
			for_update: false,
			written: false,
		}))
	};
	let project = |fields| {
		LogicalOp::Project(LogicalProject {
			input: values(),
			fields,
			omit: Vec::new(),
		})
	};
	let distinct = |op| {
		LogicalOp::Distinct(LogicalDistinct {
			input: Box::new(op),
		})
	};
	assert!(super::answers_objects(&project(LogicalFields::all())));
	assert!(super::answers_objects(&distinct(project(LogicalFields::all()))));
	let value = LogicalFields::Value {
		expr: LogicalExpr::Literal(LogicalLiteral::Null),
		alias: None,
	};
	assert!(!super::answers_objects(&project(value.clone())));
	assert!(!super::answers_objects(&distinct(project(value))));
	assert!(!super::answers_objects(&values()));
}
