//! Unit tests for surrealdb-sqlplan binder and lowering.

// A catch-all arm in these tests reports the value a lowering produced instead of the one
// expected; naming every other variant would add nothing to that report.
#![allow(clippy::wildcard_enum_match_arm)]

use surrealdb_expr::expr::logical::{LogicalExpr, LogicalOp};

use crate::catalog::MemoryCatalog;
use crate::dialect::DialectPolicy;
use crate::ir::*;
use crate::*;

#[test]
fn test_single_table_alias_binding() {
	let sql = "SELECT u.id, u.name FROM users AS u;";
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};

	let mut catalog = MemoryCatalog::new();
	catalog.define_table(None, "users", &["id", "name", "email"]);
	let policy = DialectPolicy::postgres();

	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).expect("bind failed");
	assert_eq!(bound.targets.len(), 2);

	// First target: u.id -> relation 0, column "id"
	let BoundExpr::Column(col) = &bound.targets[0].expr else {
		panic!("expected BoundExpr::Column, got {:?}", &bound.targets[0].expr);
	};
	assert_eq!(col.relation_index, 0);
	assert_eq!(col.column, "id");

	// Lower to LogicalOp
	let op = lower_bound_select(&bound, &policy).expect("lower failed");
	let LogicalOp::Project(proj) = op else {
		panic!("expected LogicalOp::Project, got {op:?}");
	};
	let surrealdb_expr::expr::logical::LogicalFields::Select(fields) = &proj.fields else {
		panic!("expected Select fields");
	};
	assert_eq!(fields.len(), 2);
	// Under relation 0, lowering emits the key of the direct `id` field, not
	// Access(Field("u"), "id")
	let surrealdb_expr::expr::logical::LogicalField::Single {
		expr: expr0,
		..
	} = &fields[0]
	else {
		panic!("expected Single field");
	};
	assert_eq!(expr0.to_string(), "record::id(id)");

	let surrealdb_expr::expr::logical::LogicalField::Single {
		expr: expr1,
		..
	} = &fields[1]
	else {
		panic!("expected Single field");
	};
	assert_eq!(expr1, &LogicalExpr::Field("name".into()));
}

#[test]
fn test_join_table_alias_binding() {
	let sql = "SELECT u.id, o.total FROM users u JOIN orders o ON u.id = o.user_id;";
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};

	let mut catalog = MemoryCatalog::new();
	catalog.define_table(None, "users", &["id", "name"]);
	catalog.define_table(None, "orders", &["id", "user_id", "total"]);
	let policy = DialectPolicy::postgres();

	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).expect("bind failed");

	// Verify target 0 (u.id) binds to relation 0
	let BoundExpr::Column(col) = &bound.targets[0].expr else {
		panic!("expected BoundExpr::Column, got {:?}", &bound.targets[0].expr);
	};
	assert_eq!(col.relation_index, 0);
	assert_eq!(col.column, "id");

	// Verify target 1 (o.total) binds to relation 1
	let BoundExpr::Column(col) = &bound.targets[1].expr else {
		panic!("expected BoundExpr::Column, got {:?}", &bound.targets[1].expr);
	};
	assert_eq!(col.relation_index, 1);
	assert_eq!(col.column, "total");

	// Lower to LogicalOp
	let op = lower_bound_select(&bound, &policy).expect("lower failed");
	let LogicalOp::Project(proj) = op else {
		panic!("expected LogicalOp::Project, got {op:?}");
	};
	let surrealdb_expr::expr::logical::LogicalFields::Select(fields) = &proj.fields else {
		panic!("expected Select fields");
	};
	let surrealdb_expr::expr::logical::LogicalField::Single {
		expr: expr0,
		..
	} = &fields[0]
	else {
		panic!("expected Single field");
	};
	assert_eq!(expr0.to_string(), "record::id(id)");

	let surrealdb_expr::expr::logical::LogicalField::Single {
		expr: expr1,
		..
	} = &fields[1]
	else {
		panic!("expected Single field");
	};
	let LogicalExpr::Access {
		base,
		field,
	} = expr1
	else {
		panic!("expected LogicalExpr::Access to _join0, got {expr1:?}");
	};
	assert_eq!(base.as_ref(), &LogicalExpr::Field("_join0".into()));
	assert_eq!(field.as_str(), "total");

	// Verify join condition
	let LogicalOp::Join(join) = &*proj.input else {
		panic!("expected LogicalOp::Join, got {:?}", proj.input);
	};
	assert_eq!(join.on.len(), 1);
	let (left, right) = &join.on[0];
	// The stored id is compared with the record of `users` the foreign key names,
	// whether it holds a key or a link.
	assert_eq!(left, &LogicalExpr::Field("id".into()));
	assert_eq!(
		right.to_string(),
		"IF _join0.user_id = '' OR !(type::is_int(_join0.user_id) OR type::is_i32(_join0.user_id) OR type::is_i16(_join0.user_id) OR type::is_i8(_join0.user_id) OR type::is_u32(_join0.user_id) OR type::is_u16(_join0.user_id) OR type::is_u8(_join0.user_id) OR type::is_string(_join0.user_id) OR type::is_uuid(_join0.user_id) OR type::is_array(_join0.user_id) OR type::is_object(_join0.user_id) OR type::is_record(_join0.user_id)) THEN NULL ELSE type::record('users', _join0.user_id) END"
	);
}

#[test]
fn test_positional_group_by_and_order_by() {
	let sql = "SELECT category, count(id) FROM products GROUP BY 1 ORDER BY 2 DESC;";
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};

	let catalog = MemoryCatalog::new();
	let policy = DialectPolicy::postgres();

	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).expect("bind failed");
	assert_eq!(bound.group_by.len(), 1);

	// GROUP BY 1 should resolve to category (target 1 expr)
	let BoundExpr::Column(col) = &bound.group_by[0] else {
		panic!("expected BoundExpr::Column, got {:?}", &bound.group_by[0]);
	};
	assert_eq!(col.column, "category");

	// ORDER BY 2 DESC should resolve to count(id) output column
	assert_eq!(bound.order_by.len(), 1);
	let BoundExpr::Column(col) = &bound.order_by[0].expr else {
		panic!("expected count column, got {:?}", &bound.order_by[0].expr);
	};
	assert_eq!(col.column, "count");
	assert_eq!(bound.order_by[0].direction, BoundOrderDirection::Desc);

	// Lowering should produce an Aggregate node rather than failing or creating array literal
	let op = lower_bound_select(&bound, &policy).expect("lower failed");
	let LogicalOp::Project(proj) = op else {
		panic!("expected LogicalOp::Project, got {op:?}");
	};
	let LogicalOp::Sort(sort) = &*proj.input else {
		panic!("expected LogicalOp::Sort inside project, got {:?}", proj.input);
	};
	let LogicalOp::Aggregate(agg) = &*sort.input else {
		panic!("expected LogicalOp::Aggregate inside sort, got {:?}", sort.input);
	};
	assert_eq!(agg.group.len(), 1);
	assert_eq!(agg.group[0], LogicalExpr::Field("category".into()));
}

#[test]
fn test_an_aggregate_in_group_by_is_refused() {
	let catalog = MemoryCatalog::new();
	let policy = DialectPolicy::postgres();
	for sql in [
		"SELECT count(*) FROM t GROUP BY count(*);",
		"SELECT sum(max(x)) AS s FROM t GROUP BY sum(max(x));",
		"SELECT x FROM t GROUP BY x, max(x);",
	] {
		let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
		let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
			panic!("expected SELECT");
		};
		let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).unwrap();
		let refused = lower_bound_select(&bound, &policy);
		assert!(
			matches!(refused, Err(crate::error::PlanError::AggregateNotAllowed(ref place)) if place == "GROUP BY"),
			"{sql}: {refused:?}"
		);
	}
}

#[test]
fn test_order_by_target_alias() {
	let sql = "SELECT id AS user_id FROM users ORDER BY user_id DESC;";
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};

	let catalog = MemoryCatalog::new();
	let policy = DialectPolicy::postgres();

	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).expect("bind failed");
	assert_eq!(bound.order_by.len(), 1);
	let BoundExpr::Column(col) = &bound.order_by[0].expr else {
		panic!("expected user_id column, got {:?}", &bound.order_by[0].expr);
	};
	assert_eq!(col.column, "user_id");
}

#[test]
fn test_schema_qualified_table_and_column() {
	let sql = "SELECT public.users.id FROM public.users;";
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};

	let catalog = MemoryCatalog::new();
	let policy = DialectPolicy::postgres();

	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).expect("bind failed");
	let BoundExpr::Column(col) = &bound.targets[0].expr else {
		panic!("expected BoundExpr::Column, got {:?}", &bound.targets[0].expr);
	};
	assert_eq!(col.relation_index, 0);
	assert_eq!(col.column, "id");
}

#[test]
fn test_case_folding_pg() {
	let sql = "SELECT U.ID FROM USERS AS U;";
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};

	let catalog = MemoryCatalog::new();
	let policy = DialectPolicy::postgres();

	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).expect("bind failed");
	let BoundExpr::Column(col) = &bound.targets[0].expr else {
		panic!("expected BoundExpr::Column, got {:?}", &bound.targets[0].expr);
	};
	assert_eq!(col.relation_index, 0);
	assert_eq!(col.column, "id");
}

#[test]
fn test_wildcard_expansion_with_catalog() {
	let sql = "SELECT * FROM users;";
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};

	let mut catalog = MemoryCatalog::new();
	catalog.define_table(None, "users", &["id", "name", "email"]);
	let policy = DialectPolicy::postgres();

	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).expect("bind failed");
	assert_eq!(bound.targets.len(), 3);
	assert_eq!(bound.targets[0].alias.as_deref(), Some("id"));
	assert_eq!(bound.targets[1].alias.as_deref(), Some("name"));
	assert_eq!(bound.targets[2].alias.as_deref(), Some("email"));
}

#[test]
fn test_mysql_binding_with_param() {
	let sql = "SELECT u.id FROM users u WHERE u.id = ?;";
	let stmts = surrealdb_mysql::parse_statements(sql).expect("parse failed");
	let surrealdb_mysql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};

	let catalog = MemoryCatalog::new();
	let policy = DialectPolicy::mysql();

	let bound = binder::mysql::bind_select(select, &catalog, &policy, None).expect("bind failed");
	let Some(BoundExpr::Binary {
		left,
		op,
		right,
	}) = &bound.where_clause
	else {
		panic!("expected binary where clause, got {:?}", bound.where_clause);
	};
	assert_eq!(*op, BoundBinaryOp::Equal);
	let BoundExpr::Column(col) = &**left else {
		panic!("expected column, got {:?}", &**left);
	};
	assert_eq!(col.column, "id");
	let BoundExpr::Param {
		index,
		name,
	} = &**right
	else {
		panic!("expected param, got {:?}", &**right);
	};
	assert_eq!(*index, 1);
	assert_eq!(name, "_1");
}

#[test]
fn test_result_descriptor_basic_select() {
	let sql = "SELECT id, name, 42, count(id) AS total_count FROM users;";
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};

	let mut catalog = MemoryCatalog::new();
	catalog.add_table(
		TableSchema::new(
			"users",
			vec![
				ColumnInfo::new("id", 0).with_type("int8"),
				ColumnInfo::new("name", 1).with_type("text"),
			],
		)
		.with_schema("public"),
	);
	let policy = DialectPolicy::postgres();

	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).expect("bind failed");
	let desc = bound.describe(&catalog, &policy);

	assert_eq!(desc.columns.len(), 4);
	assert_eq!(desc.columns[0].name, "id");
	assert_eq!(desc.columns[0].ty, SqlType::Int8);
	assert_eq!(desc.columns[0].ty.to_pg_oid(), 20);

	assert_eq!(desc.columns[1].name, "name");
	assert_eq!(desc.columns[1].ty, SqlType::Text);
	assert_eq!(desc.columns[1].ty.to_pg_oid(), 25);

	assert_eq!(desc.columns[2].name, "?column?");
	assert_eq!(desc.columns[2].ty, SqlType::Int8);

	assert_eq!(desc.columns[3].name, "total_count");
	assert_eq!(desc.columns[3].ty, SqlType::Int8);
}

#[test]
fn test_result_descriptor_duplicate_columns() {
	let sql = "SELECT 1 AS a, 2 AS a, 'hello' AS a;";
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};

	let catalog = MemoryCatalog::new();
	let policy = DialectPolicy::postgres();

	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).expect("bind failed");
	let desc = bound.describe(&catalog, &policy);

	assert_eq!(desc.columns.len(), 3);
	assert_eq!(desc.columns[0].name, "a");
	assert_eq!(desc.columns[0].ty, SqlType::Int8);

	assert_eq!(desc.columns[1].name, "a");
	assert_eq!(desc.columns[1].ty, SqlType::Int8);

	assert_eq!(desc.columns[2].name, "a");
	assert_eq!(desc.columns[2].ty, SqlType::Text);
}

#[test]
fn test_parameter_type_inference() {
	let sql = "SELECT name FROM users WHERE id = $1 AND name = $2;";
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};

	let mut catalog = MemoryCatalog::new();
	catalog.add_table(
		TableSchema::new(
			"users",
			vec![
				ColumnInfo::new("id", 0).with_type("int8"),
				ColumnInfo::new("name", 1).with_type("text"),
			],
		)
		.with_schema("public"),
	);
	let policy = DialectPolicy::postgres();

	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).expect("bind failed");
	let params = bound.describe_parameters(&catalog);

	assert_eq!(params.param_types.len(), 2);
	assert_eq!(params.param_types[0], SqlType::Int8);
	assert_eq!(params.param_types[0].to_pg_oid(), 20);
	assert_eq!(params.param_types[1], SqlType::Text);
	assert_eq!(params.param_types[1].to_pg_oid(), 25);
}

#[test]
fn a_quantified_array_parameter_is_described_as_an_array_of_the_compared_type() {
	let mut catalog = MemoryCatalog::new();
	catalog.add_table(
		TableSchema::new(
			"users",
			vec![
				ColumnInfo::new("id", 0).with_type("int8"),
				ColumnInfo::new("name", 1).with_type("text"),
			],
		)
		.with_schema("public"),
	);
	let policy = DialectPolicy::postgres();
	for (sql, expected_oid) in [
		("SELECT 1 FROM users WHERE name = ANY ($1)", 1009),
		("SELECT 1 FROM users WHERE id = ANY ($1)", 1016),
		("SELECT 1 FROM users WHERE id <> ALL ($1)", 1016),
		("SELECT 1 FROM users AS u WHERE u.name = ANY ($1)", 1009),
	] {
		let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
		let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
			panic!("expected SELECT");
		};
		let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).unwrap();
		let params = bound.describe_parameters(&catalog);
		assert_eq!(params.param_types.len(), 1, "{sql}");
		assert_eq!(params.param_types[0].to_pg_oid(), expected_oid, "{sql}");
	}
}

#[test]
fn a_system_relation_column_keeps_its_type_under_an_alias_and_a_join() {
	let catalog = MemoryCatalog::new();
	let policy = DialectPolicy::postgres();
	for sql in [
		"SELECT relrowsecurity FROM pg_class",
		"SELECT tbl.relrowsecurity FROM pg_class AS tbl",
		"SELECT tbl.relrowsecurity FROM pg_class AS tbl JOIN pg_namespace AS ns ON ns.oid = tbl.relnamespace",
	] {
		let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
		let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
			panic!("expected SELECT");
		};
		let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).unwrap();
		let described = bound.describe_static(&catalog, &policy).unwrap();
		assert_eq!(described.columns[0].ty, SqlType::Bool, "{sql}");
	}
}

#[test]
fn a_cast_of_a_column_is_named_after_the_column() {
	let catalog = MemoryCatalog::new();
	let policy = DialectPolicy::postgres();
	let stmts = surrealdb_pgsql::parse_statements("SELECT relpages::int8 FROM pg_class").unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).unwrap();
	let described = bound.describe_static(&catalog, &policy).unwrap();
	assert_eq!(described.columns[0].name, "relpages");
}

#[test]
fn an_advisory_lock_inside_a_query_is_refused_as_unsupported() {
	let catalog = MemoryCatalog::new();
	for sql in [
		"SELECT pg_advisory_lock(1)",
		"SELECT pg_advisory_lock(1) FROM pg_class",
		"SELECT pg_try_advisory_lock(1), 2",
		"SELECT 1 WHERE pg_advisory_unlock(1, 2)",
		"SELECT pg_advisory_unlock_all() FROM pg_class",
	] {
		let err = lower_pgsql_select(&catalog, sql).unwrap_err();
		assert_eq!(err.pg_sqlstate(), "0A000", "{sql}");
	}
}

#[test]
fn test_3vl_constant_folding() {
	let catalog = MemoryCatalog::new();
	let policy = DialectPolicy::postgres();

	// false AND NULL -> false
	let sql = "SELECT false AND NULL;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let mut bound = binder::pgsql::bind_select(select, &catalog, &policy, None).unwrap();
	fold_select(&mut bound, &policy).unwrap();
	assert_eq!(bound.targets[0].expr, BoundExpr::Literal(BoundLiteral::Boolean(false)));

	// true AND NULL -> NULL
	let sql = "SELECT true AND NULL;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let mut bound = binder::pgsql::bind_select(select, &catalog, &policy, None).unwrap();
	fold_select(&mut bound, &policy).unwrap();
	assert_eq!(bound.targets[0].expr, BoundExpr::Literal(BoundLiteral::Null));

	// true OR NULL -> true
	let sql = "SELECT true OR NULL;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let mut bound = binder::pgsql::bind_select(select, &catalog, &policy, None).unwrap();
	fold_select(&mut bound, &policy).unwrap();
	assert_eq!(bound.targets[0].expr, BoundExpr::Literal(BoundLiteral::Boolean(true)));

	// false OR NULL -> NULL
	let sql = "SELECT false OR NULL;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let mut bound = binder::pgsql::bind_select(select, &catalog, &policy, None).unwrap();
	fold_select(&mut bound, &policy).unwrap();
	assert_eq!(bound.targets[0].expr, BoundExpr::Literal(BoundLiteral::Null));

	// NOT NULL -> NULL
	let sql = "SELECT NOT NULL;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let mut bound = binder::pgsql::bind_select(select, &catalog, &policy, None).unwrap();
	fold_select(&mut bound, &policy).unwrap();
	assert_eq!(bound.targets[0].expr, BoundExpr::Literal(BoundLiteral::Null));
}

#[test]
fn test_null_comparisons_and_distinct_from() {
	let catalog = MemoryCatalog::new();
	let policy = DialectPolicy::postgres();

	// 1 = NULL -> NULL
	let sql = "SELECT 1 = NULL;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let mut bound = binder::pgsql::bind_select(select, &catalog, &policy, None).unwrap();
	fold_select(&mut bound, &policy).unwrap();
	assert_eq!(bound.targets[0].expr, BoundExpr::Literal(BoundLiteral::Null));

	// NULL IS NOT DISTINCT FROM NULL -> true
	let sql = "SELECT NULL IS NOT DISTINCT FROM NULL;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let mut bound = binder::pgsql::bind_select(select, &catalog, &policy, None).unwrap();
	fold_select(&mut bound, &policy).unwrap();
	assert_eq!(bound.targets[0].expr, BoundExpr::Literal(BoundLiteral::Boolean(true)));

	// 1 IS NOT DISTINCT FROM NULL -> false
	let sql = "SELECT 1 IS NOT DISTINCT FROM NULL;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let mut bound = binder::pgsql::bind_select(select, &catalog, &policy, None).unwrap();
	fold_select(&mut bound, &policy).unwrap();
	assert_eq!(bound.targets[0].expr, BoundExpr::Literal(BoundLiteral::Boolean(false)));

	// NULL IS DISTINCT FROM NULL -> false
	let sql = "SELECT NULL IS DISTINCT FROM NULL;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let mut bound = binder::pgsql::bind_select(select, &catalog, &policy, None).unwrap();
	fold_select(&mut bound, &policy).unwrap();
	assert_eq!(bound.targets[0].expr, BoundExpr::Literal(BoundLiteral::Boolean(false)));
}

#[test]
fn test_union_branches_answer_under_the_names_of_the_first_branch() {
	let mut catalog = MemoryCatalog::new();
	catalog.add_table(
		TableSchema::new("t1", vec![ColumnInfo::new("a", 0).with_type("int4")])
			.with_schema("public"),
	);
	catalog.add_table(
		TableSchema::new("t2", vec![ColumnInfo::new("b", 0).with_type("int4")])
			.with_schema("public"),
	);
	let policy = DialectPolicy::postgres();
	let lower = |sql: &str| {
		let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
		let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
			panic!("expected SELECT");
		};
		let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).unwrap();
		lower_bound_select(&bound, &policy).unwrap()
	};
	let later_branch = |op: LogicalOp| {
		let LogicalOp::UnionAll(union) = op else {
			panic!("expected UnionAll, got {op:?}");
		};
		format!("{:?}", union.inputs[1])
	};

	// A later branch that names its column differently is renamed to the first branch's name.
	assert!(later_branch(lower("SELECT a FROM t1 UNION ALL SELECT b FROM t2;")).contains("\"a\""));
	assert!(later_branch(lower("SELECT a FROM t1 UNION ALL SELECT 2;")).contains("\"a\""));
	assert!(later_branch(lower("SELECT 1 AS n UNION ALL SELECT b FROM t2;")).contains("\"n\""));
	// A branch that already names its column alike is lowered as it is.
	assert_eq!(
		later_branch(lower("SELECT a FROM t1 UNION ALL SELECT a AS a FROM t1;")),
		format!("{:?}", lower("SELECT a AS a FROM t1;"))
	);
}

#[test]
fn test_a_chain_of_set_operations() {
	let mut catalog = MemoryCatalog::new();
	for (table, column) in [("t1", "a"), ("t2", "b")] {
		catalog.add_table(
			TableSchema::new(table, vec![ColumnInfo::new(column, 0).with_type("int4")])
				.with_schema("public"),
		);
	}
	let policy = DialectPolicy::postgres();
	let lower = |sql: &str| {
		let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
		let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
			panic!("expected SELECT");
		};
		let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).unwrap();
		lower_bound_select(&bound, &policy)
	};
	let inputs = |op: LogicalOp| {
		if let LogicalOp::UnionAll(union) = op {
			union.inputs.len()
		} else if let LogicalOp::Distinct(distinct) = op {
			let LogicalOp::UnionAll(union) = *distinct.input else {
				panic!("expected UnionAll under Distinct, got {:?}", distinct.input);
			};
			union.inputs.len()
		} else {
			panic!("expected a union, got {op:?}");
		}
	};

	// A chain of UNION ALL, or of UNION, is one union of all its queries.
	let sql = "SELECT a FROM t1 UNION ALL SELECT b FROM t2 UNION ALL SELECT 3 UNION ALL SELECT 4;";
	assert!(matches!(lower(sql).unwrap(), LogicalOp::UnionAll(u) if u.inputs.len() == 4));
	let sql = "SELECT a FROM t1 UNION SELECT b FROM t2 UNION SELECT 3;";
	assert_eq!(inputs(lower(sql).unwrap()), 3);

	let refusal = |sql: &str| match lower(sql) {
		Err(PlanError::UnsupportedExpression(message)) => message,
		other => panic!("expected a refusal of {sql}, got {other:?}"),
	};
	// A chain that mixes UNION and UNION ALL reads differently under different groupings, so it is
	// refused with advice to group it.
	for sql in [
		"SELECT a FROM t1 UNION ALL SELECT b FROM t2 UNION SELECT 3;",
		"SELECT a FROM t1 UNION SELECT b FROM t2 UNION ALL SELECT 3;",
	] {
		assert!(refusal(sql).contains("group a mix with subqueries"), "{sql}");
	}
	// A chain with an INTERSECT or EXCEPT link is refused as a chain, mixed or not.
	for sql in [
		"SELECT a FROM t1 UNION ALL SELECT b FROM t2 EXCEPT SELECT 3;",
		"SELECT a FROM t1 INTERSECT SELECT b FROM t2 INTERSECT SELECT 3;",
		"SELECT a FROM t1 EXCEPT SELECT b FROM t2 EXCEPT SELECT 3;",
	] {
		assert!(refusal(sql).ends_with("supported only with UNION or UNION ALL"), "{sql}");
	}
	// So is a query inside a chain that orders or limits only itself.
	let sql =
		"SELECT a FROM t1 UNION ALL (SELECT b FROM t2 UNION ALL SELECT 3 ORDER BY 1 LIMIT 1);";
	assert!(matches!(lower(sql), Err(PlanError::UnsupportedExpression(_))));
	// Queries of one set operation have the same number of columns, unless a wildcard hides it.
	let sql = "SELECT a FROM t1 UNION ALL SELECT b, b FROM t2;";
	assert!(matches!(lower(sql), Err(PlanError::Custom(_))));
	assert!(lower("SELECT * FROM t1 UNION ALL SELECT b FROM t2;").is_ok());
}

#[test]
fn test_a_set_operation_cannot_be_the_left_operand_of_another() {
	for sql in ["(SELECT 1 UNION SELECT 2) UNION SELECT 3;", "VALUES (1), (2) UNION ALL SELECT 3;"]
	{
		assert!(surrealdb_pgsql::parse_statements(sql).is_err(), "{sql}");
	}
	assert!(
		surrealdb_mysql::parse_statements("(SELECT 1 UNION SELECT 2) UNION SELECT 3;").is_err()
	);
}

#[test]
fn test_division_semantics() {
	let catalog = MemoryCatalog::new();
	let pg_policy = DialectPolicy::postgres();
	let my_policy = DialectPolicy::mysql();

	// PG: 1 / 0 -> DivisionByZero
	let sql = "SELECT 1 / 0;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let mut bound = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap();
	let err = fold_select(&mut bound, &pg_policy).unwrap_err();
	assert!(matches!(err, PlanError::DivisionByZero));

	// PG: 5 / 2 -> integer division 2
	let sql = "SELECT 5 / 2;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let mut bound = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap();
	fold_select(&mut bound, &pg_policy).unwrap();
	assert_eq!(bound.targets[0].expr, BoundExpr::Literal(BoundLiteral::Integer(2)));

	// MySQL: 1 / 0 -> NULL
	let sql = "SELECT 1 / 0;";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let surrealdb_mysql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let mut bound = binder::mysql::bind_select(select, &catalog, &my_policy, None).unwrap();
	fold_select(&mut bound, &my_policy).unwrap();
	assert_eq!(bound.targets[0].expr, BoundExpr::Literal(BoundLiteral::Null));

	// MySQL: 5 / 2 -> 2.5
	let sql = "SELECT 5 / 2;";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let surrealdb_mysql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let mut bound = binder::mysql::bind_select(select, &catalog, &my_policy, None).unwrap();
	fold_select(&mut bound, &my_policy).unwrap();
	assert_eq!(bound.targets[0].expr, BoundExpr::Literal(BoundLiteral::Float(2.5)));

	// MySQL: 5 DIV 2 -> 2
	let sql = "SELECT 5 DIV 2;";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let surrealdb_mysql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let mut bound = binder::mysql::bind_select(select, &catalog, &my_policy, None).unwrap();
	fold_select(&mut bound, &my_policy).unwrap();
	assert_eq!(bound.targets[0].expr, BoundExpr::Literal(BoundLiteral::Integer(2)));
}

/// The first output column of the PostgreSQL `SELECT` `sql` once its constants are folded, or the
/// error that folding raised.
fn folded_pgsql(sql: &str) -> Result<BoundLiteral, PlanError> {
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap_or_else(|err| panic!("{sql}: {err}"));
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let policy = DialectPolicy::postgres();
	let mut bound =
		binder::pgsql::bind_select(select, &MemoryCatalog::new(), &policy, None).unwrap();
	fold_select(&mut bound, &policy)?;
	match &bound.targets[0].expr {
		BoundExpr::Literal(lit) => Ok(lit.clone()),
		other @ BoundExpr::Column(_)
		| other @ BoundExpr::Access {
			..
		}
		| other @ BoundExpr::Param {
			..
		}
		| other @ BoundExpr::Binary {
			..
		}
		| other @ BoundExpr::Unary {
			..
		}
		| other @ BoundExpr::FunctionCall {
			..
		}
		| other @ BoundExpr::Case {
			..
		}
		| other @ BoundExpr::Between {
			..
		}
		| other @ BoundExpr::InList {
			..
		}
		| other @ BoundExpr::IsNull {
			..
		}
		| other @ BoundExpr::Cast {
			..
		}
		| other @ BoundExpr::ScalarSubquery(_)
		| other @ BoundExpr::Exists(_)
		| other @ BoundExpr::InSubquery {
			..
		}
		| other @ BoundExpr::ArraySubquery(_)
		| other @ BoundExpr::WindowFunction {
			..
		}
		| other @ BoundExpr::UserVar(_)
		| other @ BoundExpr::SystemVar {
			..
		} => panic!("{sql}: expected a folded literal, got {other:?}"),
	}
}

/// The first output column of the MySQL `SELECT` `sql` once its constants are folded, or the error
/// that folding raised.
fn folded_mysql(sql: &str) -> Result<BoundLiteral, PlanError> {
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap_or_else(|err| panic!("{sql}: {err}"));
	let surrealdb_mysql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let policy = DialectPolicy::mysql();
	let mut bound =
		binder::mysql::bind_select(select, &MemoryCatalog::new(), &policy, None).unwrap();
	fold_select(&mut bound, &policy)?;
	match &bound.targets[0].expr {
		BoundExpr::Literal(lit) => Ok(lit.clone()),
		other @ BoundExpr::Column(_)
		| other @ BoundExpr::Access {
			..
		}
		| other @ BoundExpr::Param {
			..
		}
		| other @ BoundExpr::Binary {
			..
		}
		| other @ BoundExpr::Unary {
			..
		}
		| other @ BoundExpr::FunctionCall {
			..
		}
		| other @ BoundExpr::Case {
			..
		}
		| other @ BoundExpr::Between {
			..
		}
		| other @ BoundExpr::InList {
			..
		}
		| other @ BoundExpr::IsNull {
			..
		}
		| other @ BoundExpr::Cast {
			..
		}
		| other @ BoundExpr::ScalarSubquery(_)
		| other @ BoundExpr::Exists(_)
		| other @ BoundExpr::InSubquery {
			..
		}
		| other @ BoundExpr::ArraySubquery(_)
		| other @ BoundExpr::WindowFunction {
			..
		}
		| other @ BoundExpr::UserVar(_)
		| other @ BoundExpr::SystemVar {
			..
		} => panic!("{sql}: expected a folded literal, got {other:?}"),
	}
}

#[test]
fn test_integer_arithmetic_that_fits_64_bits_is_exact() {
	let numeric = |text: &str| BoundLiteral::Numeric(text.to_owned());
	let two_pow_63 = 2f64.powi(63);
	for (sql, expected) in [
		("SELECT 9223372036854775806 + 1", BoundLiteral::Integer(i64::MAX)),
		("SELECT 9223372036854775807 - 1", BoundLiteral::Integer(i64::MAX - 1)),
		("SELECT -9223372036854775807 - 1", BoundLiteral::Integer(i64::MIN)),
		("SELECT -9223372036854775808 + 0", BoundLiteral::Integer(i64::MIN)),
		("SELECT -4611686018427387904 * 2", BoundLiteral::Integer(i64::MIN)),
		("SELECT 9223372036854775807 * 1", BoundLiteral::Integer(i64::MAX)),
		("SELECT -9223372036854775808 / 1", BoundLiteral::Integer(i64::MIN)),
		("SELECT 9223372036854775807 / -1", BoundLiteral::Integer(-i64::MAX)),
		("SELECT -9223372036854775808 % -1", BoundLiteral::Integer(0)),
		("SELECT 9223372036854775807 % -1", BoundLiteral::Integer(0)),
		("SELECT -7 % 3", BoundLiteral::Integer(-1)),
		("SELECT 9223372036854775807 + 1.0", numeric("9223372036854775808.0")),
		// A constant beyond 64 bits is numeric, and its sum is exact.
		("SELECT 10000000000000000000 + 1", numeric("10000000000000000001")),
	] {
		assert_eq!(folded_pgsql(sql), Ok(expected), "{sql}");
	}
	for (sql, expected) in [
		("SELECT 9223372036854775806 + 1", BoundLiteral::Integer(i64::MAX)),
		("SELECT -9223372036854775807 - 1", BoundLiteral::Integer(i64::MIN)),
		("SELECT -4611686018427387904 * 2", BoundLiteral::Integer(i64::MIN)),
		("SELECT -9223372036854775808 DIV 1", BoundLiteral::Integer(i64::MIN)),
		("SELECT 9223372036854775807 DIV -1", BoundLiteral::Integer(-i64::MAX)),
		("SELECT -9223372036854775808 % -1", BoundLiteral::Integer(0)),
		("SELECT -9223372036854775808 / -1", BoundLiteral::Float(two_pow_63)),
		("SELECT 9223372036854775807 + 1.0", numeric("9223372036854775808.0")),
	] {
		assert_eq!(folded_mysql(sql), Ok(expected), "{sql}");
	}
}

#[test]
fn test_integer_arithmetic_beyond_64_bits_is_out_of_range() {
	for sql in [
		"SELECT 9223372036854775807 + 1",
		"SELECT -9223372036854775808 - 1",
		"SELECT 9223372036854775807 * 2",
		"SELECT -9223372036854775808 * -1",
		"SELECT -9223372036854775808 / -1",
		"SELECT - -9223372036854775808",
	] {
		let err = folded_pgsql(sql).expect_err(sql);
		assert_eq!(err, PlanError::NumericValueOutOfRange, "{sql}");
		assert_eq!(err.pg_sqlstate(), "22003", "{sql}");
		assert_eq!(err.to_string(), "bigint out of range", "{sql}");
	}
	for sql in [
		"SELECT 9223372036854775807 + 1",
		"SELECT -9223372036854775808 - 1",
		"SELECT 9223372036854775807 * 2",
		"SELECT -9223372036854775808 * -1",
		"SELECT -9223372036854775808 DIV -1",
		"SELECT - -9223372036854775808",
		"SELECT 1e30 DIV 1",
		"SELECT 9223372036854775807 DIV 0.5",
	] {
		let err = folded_mysql(sql).expect_err(sql);
		assert_eq!(err, PlanError::NumericValueOutOfRange, "{sql}");
		assert_eq!(err.mysql_error_code(), 1690, "{sql}");
	}
}

#[test]
fn test_a_divisor_of_zero_is_division_by_zero_and_not_a_panic() {
	for sql in ["SELECT 1 / 0", "SELECT 1 % 0", "SELECT 1 / 0.0", "SELECT -9223372036854775808 % 0"]
	{
		assert_eq!(folded_pgsql(sql), Err(PlanError::DivisionByZero), "{sql}");
	}
	for sql in
		["SELECT 1 DIV 0", "SELECT 1 DIV 0.0", "SELECT 1 / 0", "SELECT 1 % 0", "SELECT 1 % 0.0"]
	{
		assert_eq!(folded_mysql(sql), Ok(BoundLiteral::Null), "{sql}");
	}
}

#[test]
fn test_div_with_a_divisor_that_is_not_an_integer_divides_before_it_truncates() {
	for (sql, expected) in [
		("SELECT 1 DIV 0.5", 2),
		("SELECT 7 DIV 2.5", 2),
		("SELECT -7 DIV 2.5", -2),
		("SELECT 7.5 DIV 2", 3),
		("SELECT 5 DIV 2", 2),
		("SELECT -5 DIV 2", -2),
	] {
		assert_eq!(folded_mysql(sql), Ok(BoundLiteral::Integer(expected)), "{sql}");
	}
}

#[test]
fn test_div_of_integral_values_is_exact_even_when_an_operand_is_a_float() {
	for (sql, expected) in [
		("SELECT 9223372036854775807 DIV 1.0", i64::MAX),
		("SELECT 9223372036854775806 DIV 1.0", i64::MAX - 1),
		("SELECT 9223372036854775807 DIV 2.0", 4611686018427387903),
		("SELECT 9223372036854775807 DIV -1.0", -i64::MAX),
		("SELECT -9223372036854775808 DIV 1.0", i64::MIN),
		("SELECT -9223372036854775808 DIV 2.0", -4611686018427387904),
		("SELECT -7 DIV 2.0", -3),
		("SELECT 7.0 DIV 2", 3),
		("SELECT 7.0 DIV 2.0", 3),
		("SELECT 4611686018427387904.0 DIV 3", 1537228672809129301),
		("SELECT 4611686018427387904 DIV 3.0", 1537228672809129301),
	] {
		assert_eq!(folded_mysql(sql), Ok(BoundLiteral::Integer(expected)), "{sql}");
	}
	assert_eq!(
		folded_mysql("SELECT -9223372036854775808 DIV -1.0"),
		Err(PlanError::NumericValueOutOfRange)
	);
}

#[test]
fn test_mysql_string_coercion() {
	let catalog = MemoryCatalog::new();
	let policy = DialectPolicy::mysql();

	// '100abc' + 20 -> 120
	let sql = "SELECT '100abc' + 20;";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let surrealdb_mysql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let mut bound = binder::mysql::bind_select(select, &catalog, &policy, None).unwrap();
	fold_select(&mut bound, &policy).unwrap();
	assert_eq!(bound.targets[0].expr, BoundExpr::Literal(BoundLiteral::Integer(120)));

	// '2.5' * 2 -> 5.0
	let sql = "SELECT '2.5' * 2;";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let surrealdb_mysql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let mut bound = binder::mysql::bind_select(select, &catalog, &policy, None).unwrap();
	fold_select(&mut bound, &policy).unwrap();
	assert_eq!(bound.targets[0].expr, BoundExpr::Literal(BoundLiteral::Float(5.0)));
}

#[test]
fn test_mysql_sql_mode_pipes_as_concat_and_introduced_strings() {
	let catalog = MemoryCatalog::new();
	let policy = DialectPolicy::mysql();

	// Parse with PIPES_AS_CONCAT
	let settings = surrealdb_mysql::ParserSettings::default()
		.with_sql_mode(surrealdb_mysql::SqlMode::empty().with_pipes_as_concat(true));
	let sql = "SELECT 'hello ' || 'world';";
	let stmts = surrealdb_mysql::parse_statements_with_settings(sql, settings).unwrap();
	let surrealdb_mysql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let bound = binder::mysql::bind_select(select, &catalog, &policy, None).unwrap();
	assert_eq!(
		bound.targets[0].expr,
		BoundExpr::Binary {
			left: Box::new(BoundExpr::Literal(BoundLiteral::String("hello ".to_string()))),
			op: BoundBinaryOp::Concat,
			right: Box::new(BoundExpr::Literal(BoundLiteral::String("world".to_string()))),
		}
	);

	// Introduced strings
	let sql = "SELECT _utf8mb4'hello', _binary 0x6162;";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let surrealdb_mysql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let bound = binder::mysql::bind_select(select, &catalog, &policy, None).unwrap();
	assert_eq!(
		bound.targets[0].expr,
		BoundExpr::Literal(BoundLiteral::String("hello".to_string()))
	);
	assert_eq!(bound.targets[1].expr, BoundExpr::Literal(BoundLiteral::String("ab".to_string())));
}

#[test]
fn test_string_collation() {
	let catalog = MemoryCatalog::new();

	// MySQL: 'abc' = 'ABC' -> true
	let my_policy = DialectPolicy::mysql();
	let sql = "SELECT 'abc' = 'ABC';";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let surrealdb_mysql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let mut bound = binder::mysql::bind_select(select, &catalog, &my_policy, None).unwrap();
	fold_select(&mut bound, &my_policy).unwrap();
	assert_eq!(bound.targets[0].expr, BoundExpr::Literal(BoundLiteral::Boolean(true)));

	// PG: 'abc' = 'ABC' -> false
	let pg_policy = DialectPolicy::postgres();
	let sql = "SELECT 'abc' = 'ABC';";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let mut bound = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap();
	fold_select(&mut bound, &pg_policy).unwrap();
	assert_eq!(bound.targets[0].expr, BoundExpr::Literal(BoundLiteral::Boolean(false)));
}

#[test]
fn test_pattern_matching_like() {
	let catalog = MemoryCatalog::new();
	let pg_policy = DialectPolicy::postgres();
	let my_policy = DialectPolicy::mysql();

	// PG LIKE (case sensitive): 'hello' LIKE 'hel%' -> true, 'hello' LIKE 'HEL%' -> false
	let sql = "SELECT 'hello' LIKE 'hel%', 'hello' LIKE 'HEL%';";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let mut bound = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap();
	fold_select(&mut bound, &pg_policy).unwrap();
	assert_eq!(bound.targets[0].expr, BoundExpr::Literal(BoundLiteral::Boolean(true)));
	assert_eq!(bound.targets[1].expr, BoundExpr::Literal(BoundLiteral::Boolean(false)));

	// PG ILIKE (case insensitive): 'hello' ILIKE 'HEL%' -> true
	let sql = "SELECT 'hello' ILIKE 'HEL%';";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let mut bound = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap();
	fold_select(&mut bound, &pg_policy).unwrap();
	assert_eq!(bound.targets[0].expr, BoundExpr::Literal(BoundLiteral::Boolean(true)));

	// MySQL LIKE (case insensitive): 'hello' LIKE 'HEL%' -> true
	let sql = "SELECT 'hello' LIKE 'HEL%';";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let surrealdb_mysql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let mut bound = binder::mysql::bind_select(select, &catalog, &my_policy, None).unwrap();
	fold_select(&mut bound, &my_policy).unwrap();
	assert_eq!(bound.targets[0].expr, BoundExpr::Literal(BoundLiteral::Boolean(true)));
}

#[test]
fn test_regex_operators_fold_and_an_invalid_pattern_is_an_error() {
	let catalog = MemoryCatalog::new();
	let policy = DialectPolicy::postgres();
	let fold = |sql: &str| {
		let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
		let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
			panic!("expected SELECT");
		};
		let mut bound = binder::pgsql::bind_select(select, &catalog, &policy, None).unwrap();
		fold_select(&mut bound, &policy).map(|()| bound.targets[0].expr.clone())
	};
	let boolean = |value| Ok(BoundExpr::Literal(BoundLiteral::Boolean(value)));

	assert_eq!(fold("SELECT 'ABC' ~ 'b'"), boolean(false));
	assert_eq!(fold("SELECT 'ABC' ~* 'b'"), boolean(true));
	assert_eq!(fold("SELECT 'ABC' !~ 'b'"), boolean(true));
	assert_eq!(fold("SELECT 'ABC' !~* 'b'"), boolean(false));
	assert_eq!(fold("SELECT 'ABC' ~* '^(a|x)b'"), boolean(true));
	assert!(fold("SELECT 'a' ~ '['").is_err());
	assert!(fold("SELECT 'a' ~* '['").is_err());
	assert!(fold("SELECT 'a' !~ '['").is_err());
}

#[test]
fn test_type_casts() {
	let catalog = MemoryCatalog::new();
	let pg_policy = DialectPolicy::postgres();

	// PG: CAST('123' AS integer) -> 123
	let sql = "SELECT CAST('123' AS integer);";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let mut bound = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap();
	fold_select(&mut bound, &pg_policy).unwrap();
	assert_eq!(bound.targets[0].expr, BoundExpr::Literal(BoundLiteral::Integer(123)));

	// PG: CAST('abc' AS integer) -> 22P02 error
	let sql = "SELECT CAST('abc' AS integer);";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let mut bound = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap();
	let err = fold_select(&mut bound, &pg_policy).unwrap_err();
	assert!(matches!(err, PlanError::InvalidTextRepresentation(_)));
	assert_eq!(err.pg_sqlstate(), "22P02");
}

#[test]
fn test_a_column_alias_list_names_the_columns_of_a_derived_table() {
	let mut catalog = MemoryCatalog::new();
	catalog.add_table(
		TableSchema::new(
			"t",
			vec![
				ColumnInfo::new("a", 0).with_type("int4"),
				ColumnInfo::new("b", 1).with_type("int4"),
			],
		)
		.with_schema("public"),
	);
	let policy = DialectPolicy::postgres();
	let bind = |sql: &str| {
		let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
		let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
			panic!("expected SELECT");
		};
		binder::pgsql::bind_select(select, &catalog, &policy, None)
	};

	// The first columns are renamed in order and the rest keep their names.
	let bound = bind("SELECT x FROM (SELECT a, b FROM t) AS v(x)").unwrap();
	match bound.from.as_ref() {
		Some(BoundTableSource::Subquery {
			query,
			..
		}) => {
			assert_eq!(query.targets[0].alias.as_deref(), Some("x"));
			assert_eq!(query.targets[1].alias, None);
		}
		other => panic!("expected a derived table, got {other:?}"),
	}

	// A quoted name keeps its case, as every other quoted identifier does.
	let bound = bind("SELECT x FROM (SELECT a FROM t) AS v(\"X\")").unwrap();
	match bound.from.as_ref() {
		Some(BoundTableSource::Subquery {
			query,
			..
		}) => assert_eq!(query.targets[0].alias.as_deref(), Some("X")),
		other => panic!("expected a derived table, got {other:?}"),
	}

	// A list on a common table expression names its columns like a list on a derived table.
	let bound = bind("WITH c AS (SELECT a, b FROM t) SELECT x FROM c AS v(x)").unwrap();
	match bound.from.as_ref() {
		Some(BoundTableSource::Subquery {
			query,
			..
		}) => assert_eq!(query.targets[0].alias.as_deref(), Some("x")),
		other => panic!("expected a derived table, got {other:?}"),
	}

	// More names than columns, a list over `*`, a name used twice, a name that a column the list
	// does not reach already has, and a list on a table are refused rather than ignored.
	assert!(bind("SELECT x FROM (SELECT a FROM t) AS v(x, y)").is_err());
	assert!(bind("SELECT x FROM (SELECT * FROM t) AS v(x)").is_err());
	assert!(bind("SELECT x FROM (SELECT a, b FROM t) AS v(x, x)").is_err());
	assert!(bind("SELECT x FROM (SELECT a, b FROM t) AS v(b)").is_err());
	assert!(bind("SELECT x FROM t AS v(x)").is_err());
}

#[test]
fn test_null_ordering_lowering() {
	let mut catalog = MemoryCatalog::new();
	catalog.define_table(None, "users", &["id", "score"]);

	// PG: ORDER BY score ASC (NULLS LAST by default) -> emits 2 orders: [score IS NULL ASC, score
	// ASC]
	let pg_policy = DialectPolicy::postgres();
	let sql = "SELECT score FROM users ORDER BY score ASC;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let bound = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap();
	let op = lower_bound_select(&bound, &pg_policy).unwrap();
	let LogicalOp::Project(proj) = op else {
		panic!("expected Project, got {op:?}");
	};
	let LogicalOp::Sort(sort) = &*proj.input else {
		panic!("expected Sort, got {:?}", proj.input);
	};
	let surrealdb_expr::expr::logical::LogicalOrdering::Order(orders) = &sort.ordering else {
		panic!("expected Order");
	};
	assert_eq!(orders.len(), 2, "PG ASC should emit (score IS NULL) ASC and score ASC");
	assert!(orders[0].direction);
	assert!(orders[1].direction);

	// MySQL: ORDER BY score ASC (NULLS FIRST by default) -> emits 1 order: [score ASC]
	let my_policy = DialectPolicy::mysql();
	let sql = "SELECT score FROM users ORDER BY score ASC;";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let surrealdb_mysql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let bound = binder::mysql::bind_select(select, &catalog, &my_policy, None).unwrap();
	let op = lower_bound_select(&bound, &my_policy).unwrap();
	let LogicalOp::Project(proj) = op else {
		panic!("expected Project, got {op:?}");
	};
	let LogicalOp::Sort(sort) = &*proj.input else {
		panic!("expected Sort, got {:?}", proj.input);
	};
	let surrealdb_expr::expr::logical::LogicalOrdering::Order(orders) = &sort.ordering else {
		panic!("expected Order");
	};
	assert_eq!(orders.len(), 1, "MySQL ASC should emit single score ASC");
	assert!(orders[0].direction);
}

#[test]
fn test_set_operations_union_all_and_distinct() {
	let mut catalog = MemoryCatalog::new();
	catalog.add_table(
		TableSchema::new("t1", vec![ColumnInfo::new("a", 0).with_type("int4")])
			.with_schema("public"),
	);
	catalog.add_table(
		TableSchema::new("t2", vec![ColumnInfo::new("a", 0).with_type("int4")])
			.with_schema("public"),
	);
	let pg_policy = DialectPolicy::postgres();

	// 1. UNION ALL
	let sql = "SELECT a FROM t1 UNION ALL SELECT a FROM t2;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let bound = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap();
	let op = lower_bound_select(&bound, &pg_policy).unwrap();
	assert!(matches!(op, LogicalOp::UnionAll(_)));

	// 2. UNION (distinct)
	let sql = "SELECT a FROM t1 UNION SELECT a FROM t2;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let bound = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap();
	let op = lower_bound_select(&bound, &pg_policy).unwrap();
	assert!(matches!(op, LogicalOp::Distinct(_)));

	// 3. INTERSECT
	let sql = "SELECT a FROM t1 INTERSECT SELECT a FROM t2;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let bound = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap();
	let op = lower_bound_select(&bound, &pg_policy).unwrap();
	let LogicalOp::Distinct(d) = op else {
		panic!("expected Distinct, got {op:?}");
	};
	let LogicalOp::Join(j) = *d.input else {
		panic!("expected Join under Distinct, got {:?}", d.input);
	};
	assert_eq!(j.kind, surrealdb_expr::expr::logical::JoinKind::Semi);

	// 4. EXCEPT
	let sql = "SELECT a FROM t1 EXCEPT SELECT a FROM t2;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let bound = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap();
	let op = lower_bound_select(&bound, &pg_policy).unwrap();
	let LogicalOp::Distinct(d) = op else {
		panic!("expected Distinct, got {op:?}");
	};
	let LogicalOp::Join(j) = *d.input else {
		panic!("expected Join under Distinct, got {:?}", d.input);
	};
	assert_eq!(j.kind, surrealdb_expr::expr::logical::JoinKind::Anti);

	// 5. MySQL UNION with outer ORDER BY & LIMIT
	let my_policy = DialectPolicy::mysql();
	let sql = "SELECT a FROM t1 UNION ALL SELECT a FROM t2 ORDER BY a DESC LIMIT 5;";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let surrealdb_mysql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let bound = binder::mysql::bind_select(select, &catalog, &my_policy, None).unwrap();
	let op = lower_bound_select(&bound, &my_policy).unwrap();
	let LogicalOp::Limit(lim) = op else {
		panic!("expected Limit, got {op:?}");
	};
	let LogicalOp::Sort(sort) = *lim.input else {
		panic!("expected Sort under Limit, got {:?}", lim.input);
	};
	let LogicalOp::UnionAll(_) = *sort.input else {
		panic!("expected UnionAll under Sort, got {:?}", sort.input);
	};
}

#[test]
fn test_aggregate_subquery_keeps_bare_rows_unless_it_has_having_or_order_by() {
	let policy = DialectPolicy::postgres();
	let lower = |sql: &str| {
		let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
		let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
			panic!("expected SELECT");
		};
		let bound = binder::pgsql::bind_select(select, &MemoryCatalog::new(), &policy, None)
			.expect("bind failed");
		(
			lower_bound_subquery(&bound, &policy).expect("lower failed"),
			lower_bound_select(&bound, &policy).expect("lower failed"),
		)
	};

	// The aggregate's rows are the values themselves; a projection over them would read the
	// aggregated field of a row that is not an object. As a statement of its own the result keeps
	// its column.
	let (subquery, statement) = lower("SELECT max(total) FROM orders;");
	assert!(matches!(subquery, LogicalOp::Aggregate(_)), "got {subquery:?}");
	assert!(matches!(statement, LogicalOp::Project(_)), "got {statement:?}");

	// With a HAVING or an ORDER BY the rows stay objects until those clauses have run, and the
	// subquery projects its one column last.
	for sql in [
		"SELECT max(total) FROM orders GROUP BY user_id HAVING count(*) > 1;",
		"SELECT max(total) FROM orders GROUP BY user_id ORDER BY max(total) DESC LIMIT 1;",
	] {
		let (subquery, _) = lower(sql);
		let LogicalOp::Project(project) = &subquery else {
			panic!("{sql}: got {subquery:?}");
		};
		assert!(
			matches!(project.fields, surrealdb_expr::expr::logical::LogicalFields::Value { .. }),
			"{sql}"
		);
	}
}

#[test]
fn test_joins_multi_table_using_and_right() {
	let mut catalog = MemoryCatalog::new();
	catalog.add_table(
		TableSchema::new(
			"users",
			vec![
				ColumnInfo::new("id", 0).with_type("int4"),
				ColumnInfo::new("dept_id", 1).with_type("int4"),
			],
		)
		.with_schema("public"),
	);
	catalog.add_table(
		TableSchema::new(
			"departments",
			vec![
				ColumnInfo::new("id", 0).with_type("int4"),
				ColumnInfo::new("dept_id", 1).with_type("int4"),
				ColumnInfo::new("name", 2).with_type("text"),
			],
		)
		.with_schema("public"),
	);
	let pg_policy = DialectPolicy::postgres();

	// 1. USING join
	let sql = "SELECT users.id, departments.name FROM users JOIN departments USING (dept_id);";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let bound = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap();
	let op = lower_bound_select(&bound, &pg_policy).unwrap();
	let LogicalOp::Project(proj) = op else {
		panic!("expected Project, got {op:?}");
	};
	let LogicalOp::Join(j) = *proj.input else {
		panic!("expected Join under Project, got {:?}", proj.input);
	};
	assert_eq!(j.kind, surrealdb_expr::expr::logical::JoinKind::Inner);
	assert_eq!(j.on.len(), 1);

	// 2. RIGHT JOIN rewrite
	let sql =
		"SELECT users.id FROM users RIGHT JOIN departments ON users.dept_id = departments.dept_id;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let bound = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap();
	// Confirm bound source is inverted to Left join
	let Some(crate::ir::BoundTableSource::Join {
		join_type,
		..
	}) = &bound.from
	else {
		panic!("expected Left join in bound IR, got {:?}", bound.from);
	};
	assert_eq!(*join_type, crate::ir::BoundJoinType::Left);

	// 3. Multi-condition ON
	let sql = "SELECT users.id FROM users JOIN departments ON users.id = departments.id AND users.dept_id = departments.dept_id;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let bound = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap();
	let op = lower_bound_select(&bound, &pg_policy).unwrap();
	let LogicalOp::Project(proj) = op else {
		panic!("expected Project, got {op:?}");
	};
	let LogicalOp::Join(j) = *proj.input else {
		panic!("expected Join under Project, got {:?}", proj.input);
	};
	assert_eq!(j.on.len(), 2, "Expected 2 equi-join pairs extracted from AND condition");
}

#[test]
fn test_subqueries_pgsql() {
	let mut catalog = MemoryCatalog::new();
	catalog.add_table(
		TableSchema::new(
			"users",
			vec![
				ColumnInfo::new("id", 0).with_type("int8"),
				ColumnInfo::new("dept_id", 1).with_type("int8"),
			],
		)
		.with_schema("public"),
	);
	catalog.add_table(
		TableSchema::new(
			"departments",
			vec![
				ColumnInfo::new("id", 0).with_type("int8"),
				ColumnInfo::new("name", 1).with_type("text"),
			],
		)
		.with_schema("public"),
	);

	let pg_policy = DialectPolicy::postgres();

	// 1. Scalar correlated subquery in target
	let sql = "SELECT id, (SELECT name FROM departments WHERE departments.id = users.dept_id) FROM users;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let bound = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap();
	assert_eq!(bound.targets.len(), 2);
	assert!(matches!(bound.targets[1].expr, BoundExpr::ScalarSubquery(_)));
	let op = lower_bound_select(&bound, &pg_policy).unwrap();
	assert!(matches!(op, LogicalOp::Project(_)));

	// 2. EXISTS subquery in WHERE
	let sql = "SELECT id FROM users WHERE EXISTS (SELECT 1 FROM departments WHERE departments.id = users.dept_id);";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let bound = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap();
	assert!(matches!(bound.where_clause, Some(BoundExpr::Exists(_))));
	let op = lower_bound_select(&bound, &pg_policy).unwrap();
	assert!(matches!(op, LogicalOp::Project(_)));

	// 3. IN subquery in WHERE
	let sql = "SELECT id FROM users WHERE dept_id IN (SELECT id FROM departments);";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let bound = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap();
	assert!(matches!(
		bound.where_clause,
		Some(BoundExpr::InSubquery {
			negated: false,
			..
		})
	));
	let op = lower_bound_select(&bound, &pg_policy).unwrap();
	assert!(matches!(op, LogicalOp::Project(_)));

	// 4. Quantified = ANY -> IN subquery
	let sql = "SELECT id FROM users WHERE dept_id = ANY (SELECT id FROM departments);";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let bound = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap();
	assert!(matches!(
		bound.where_clause,
		Some(BoundExpr::InSubquery {
			negated: false,
			..
		})
	));

	// 5. Quantified > ALL -> > (SELECT max(id) FROM departments)
	let sql = "SELECT id FROM users WHERE dept_id > ALL (SELECT id FROM departments);";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let bound = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap();
	assert!(matches!(
		bound.where_clause,
		Some(BoundExpr::Binary {
			op: BoundBinaryOp::GreaterThan,
			..
		})
	));
}

#[test]
fn test_subqueries_mysql() {
	let mut catalog = MemoryCatalog::new();
	catalog.add_table(TableSchema::new(
		"users",
		vec![
			ColumnInfo::new("id", 0).with_type("int"),
			ColumnInfo::new("dept_id", 1).with_type("int"),
		],
	));
	catalog.add_table(TableSchema::new(
		"departments",
		vec![
			ColumnInfo::new("id", 0).with_type("int"),
			ColumnInfo::new("name", 1).with_type("varchar"),
		],
	));

	let my_policy = DialectPolicy::mysql();

	// 1. Scalar correlated subquery in target
	let sql = "SELECT id, (SELECT name FROM departments WHERE departments.id = users.dept_id) FROM users;";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let surrealdb_mysql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let bound = binder::mysql::bind_select(select, &catalog, &my_policy, None).unwrap();
	assert_eq!(bound.targets.len(), 2);
	assert!(matches!(bound.targets[1].expr, BoundExpr::ScalarSubquery(_)));
	let op = lower_bound_select(&bound, &my_policy).unwrap();
	assert!(matches!(op, LogicalOp::Project(_)));

	// 2. EXISTS subquery
	let sql = "SELECT id FROM users WHERE EXISTS (SELECT 1 FROM departments WHERE departments.id = users.dept_id);";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let surrealdb_mysql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let bound = binder::mysql::bind_select(select, &catalog, &my_policy, None).unwrap();
	assert!(matches!(bound.where_clause, Some(BoundExpr::Exists(_))));
	let op = lower_bound_select(&bound, &my_policy).unwrap();
	assert!(matches!(op, LogicalOp::Project(_)));

	// 3. IN subquery
	let sql = "SELECT id FROM users WHERE dept_id IN (SELECT id FROM departments);";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let surrealdb_mysql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let bound = binder::mysql::bind_select(select, &catalog, &my_policy, None).unwrap();
	assert!(matches!(
		bound.where_clause,
		Some(BoundExpr::InSubquery {
			negated: false,
			..
		})
	));
	let op = lower_bound_select(&bound, &my_policy).unwrap();
	assert!(matches!(op, LogicalOp::Project(_)));
}

#[test]
fn test_cte_pgsql() {
	let mut catalog = MemoryCatalog::new();
	catalog.add_table(
		TableSchema::new(
			"users",
			vec![
				ColumnInfo::new("id", 0).with_type("int8"),
				ColumnInfo::new("name", 1).with_type("text"),
				ColumnInfo::new("dept_id", 2).with_type("int8"),
			],
		)
		.with_schema("public"),
	);
	let pg_policy = DialectPolicy::postgres();

	// 1. Basic CTE
	let sql = "WITH cte AS (SELECT id, name FROM users) SELECT id, name FROM cte;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let bound = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap();
	assert!(bound.with.is_some());
	assert_eq!(bound.with.as_ref().unwrap().ctes.len(), 1);
	assert_eq!(bound.targets.len(), 2);
	let op = lower_bound_select(&bound, &pg_policy).unwrap();
	assert!(matches!(op, LogicalOp::Project(_)));

	// 2. CTE with explicit column aliases
	let sql =
		"WITH cte(u_id, u_name) AS (SELECT id, name FROM users) SELECT u_id, u_name FROM cte;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let bound = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap();
	assert!(bound.with.is_some());
	assert_eq!(bound.with.as_ref().unwrap().ctes[0].columns, vec!["u_id", "u_name"]);
	let op = lower_bound_select(&bound, &pg_policy).unwrap();
	assert!(matches!(op, LogicalOp::Project(_)));

	// 3. Multiple CTEs referencing each other
	let sql = "WITH c1 AS (SELECT id FROM users), c2 AS (SELECT id FROM c1) SELECT id FROM c2;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let bound = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap();
	assert_eq!(bound.with.as_ref().unwrap().ctes.len(), 2);
	let op = lower_bound_select(&bound, &pg_policy).unwrap();
	assert!(matches!(op, LogicalOp::Project(_)));

	// 4. Recursive CTE without self-reference passes
	let sql = "WITH RECURSIVE c AS (SELECT 1 AS a) SELECT a FROM c;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let bound = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap();
	assert!(bound.with.as_ref().unwrap().recursive);
	let op = lower_bound_select(&bound, &pg_policy).unwrap();
	assert!(matches!(op, LogicalOp::Project(_)));

	// 5. Recursive CTE with self-reference fails closed
	let sql = "WITH RECURSIVE c AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM c WHERE n < 5) SELECT n FROM c;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let err = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap_err();
	assert!(matches!(err, PlanError::UnsupportedStatement(_)));
}

#[test]
fn test_cte_mysql() {
	let mut catalog = MemoryCatalog::new();
	catalog.add_table(TableSchema::new(
		"users",
		vec![
			ColumnInfo::new("id", 0).with_type("int8"),
			ColumnInfo::new("name", 1).with_type("text"),
			ColumnInfo::new("dept_id", 2).with_type("int8"),
		],
	));
	let my_policy = DialectPolicy::mysql();

	// 1. Basic CTE
	let sql = "WITH cte AS (SELECT id, name FROM users) SELECT id, name FROM cte;";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let surrealdb_mysql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let bound = binder::mysql::bind_select(select, &catalog, &my_policy, None).unwrap();
	assert!(bound.with.is_some());
	assert_eq!(bound.with.as_ref().unwrap().ctes.len(), 1);
	assert_eq!(bound.targets.len(), 2);
	let op = lower_bound_select(&bound, &my_policy).unwrap();
	assert!(matches!(op, LogicalOp::Project(_)));

	// 2. CTE with explicit column aliases
	let sql =
		"WITH cte(u_id, u_name) AS (SELECT id, name FROM users) SELECT u_id, u_name FROM cte;";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let surrealdb_mysql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let bound = binder::mysql::bind_select(select, &catalog, &my_policy, None).unwrap();
	assert!(bound.with.is_some());
	assert_eq!(bound.with.as_ref().unwrap().ctes[0].columns, vec!["u_id", "u_name"]);
	let op = lower_bound_select(&bound, &my_policy).unwrap();
	assert!(matches!(op, LogicalOp::Project(_)));

	// 3. Multiple CTEs referencing each other
	let sql = "WITH c1 AS (SELECT id FROM users), c2 AS (SELECT id FROM c1) SELECT id FROM c2;";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let surrealdb_mysql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let bound = binder::mysql::bind_select(select, &catalog, &my_policy, None).unwrap();
	assert_eq!(bound.with.as_ref().unwrap().ctes.len(), 2);
	let op = lower_bound_select(&bound, &my_policy).unwrap();
	assert!(matches!(op, LogicalOp::Project(_)));

	// 4. Recursive CTE with self-reference fails closed
	let sql = "WITH RECURSIVE c AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM c WHERE n < 5) SELECT n FROM c;";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let surrealdb_mysql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let err = binder::mysql::bind_select(select, &catalog, &my_policy, None).unwrap_err();
	assert!(matches!(err, PlanError::UnsupportedStatement(_)));
}

#[test]
fn test_window_functions_pgsql() {
	let mut catalog = MemoryCatalog::new();
	catalog.add_table(TableSchema::new(
		"emp",
		vec![
			ColumnInfo::new("id", 0).with_type("int8"),
			ColumnInfo::new("dept", 1).with_type("text"),
			ColumnInfo::new("salary", 2).with_type("int8"),
			ColumnInfo::new("hire_date", 3).with_type("text"),
		],
	));
	let pg_policy = DialectPolicy::postgres();

	// 1. Basic OVER ()
	let sql = "SELECT row_number() OVER () FROM emp;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let bound = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap();
	assert_eq!(bound.targets.len(), 1);
	let BoundExpr::WindowFunction {
		name,
		args,
		distinct,
		spec,
	} = &bound.targets[0].expr
	else {
		panic!("expected WindowFunction, got {:?}", &bound.targets[0].expr);
	};
	assert_eq!(name, "row_number");
	assert!(args.is_empty());
	assert!(!distinct);
	assert!(spec.partition_by.is_empty());
	assert!(spec.order_by.is_empty());
	assert!(spec.frame.is_none());
	let desc = bound.describe(&catalog, &pg_policy);
	assert_eq!(desc.columns[0].ty, SqlType::Int8);
	// Lowering computes the call with a `Window` operator under the projection
	let op = lower_bound_select(&bound, &pg_policy).unwrap();
	let LogicalOp::Project(project) = &op else {
		panic!("expected Project, got {op}");
	};
	assert!(matches!(&*project.input, LogicalOp::Window(_)), "{op}");

	// 2. Window function with PARTITION BY and ORDER BY
	let sql = "SELECT rank() OVER (PARTITION BY dept ORDER BY salary DESC) FROM emp;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let bound = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap();
	let BoundExpr::WindowFunction {
		name,
		spec,
		..
	} = &bound.targets[0].expr
	else {
		panic!("expected WindowFunction, got {:?}", &bound.targets[0].expr);
	};
	assert_eq!(name, "rank");
	assert_eq!(spec.partition_by.len(), 1);
	assert_eq!(spec.order_by.len(), 1);
	assert_eq!(spec.order_by[0].direction, BoundOrderDirection::Desc);

	// 3. Named window definition and reference
	let sql = "SELECT rank() OVER w FROM emp WINDOW w AS (PARTITION BY dept ORDER BY salary DESC);";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let bound = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap();
	assert_eq!(bound.windows.len(), 1);
	assert_eq!(bound.windows[0].name, "w");
	assert_eq!(bound.windows[0].spec.partition_by.len(), 1);
	let BoundExpr::WindowFunction {
		spec,
		..
	} = &bound.targets[0].expr
	else {
		panic!("expected WindowFunction, got {:?}", &bound.targets[0].expr);
	};
	assert_eq!(spec.window_name.as_deref(), Some("w"));
	assert_eq!(spec.partition_by.len(), 1);
	assert_eq!(spec.order_by.len(), 1);

	// 4. ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
	let sql = "SELECT sum(salary) OVER (PARTITION BY dept ORDER BY hire_date ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM emp;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let bound = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap();
	let BoundExpr::WindowFunction {
		spec,
		..
	} = &bound.targets[0].expr
	else {
		panic!("expected WindowFunction, got {:?}", &bound.targets[0].expr);
	};
	let frame = spec.frame.as_ref().unwrap();
	assert_eq!(frame.units, BoundWindowFrameUnits::Rows);
	assert!(matches!(frame.start, BoundWindowFrameBound::Preceding(_)));
	assert!(matches!(frame.end, Some(BoundWindowFrameBound::Following(_))));

	// 5. Semantic rejection in WHERE
	let sql = "SELECT id FROM emp WHERE rank() OVER () > 1;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let err = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap_err();
	assert!(matches!(err, PlanError::WindowNotAllowed(clause) if clause.contains("WHERE")));

	// 6. Semantic rejection in GROUP BY
	let sql = "SELECT dept FROM emp GROUP BY rank() OVER ();";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let err = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap_err();
	assert!(matches!(err, PlanError::WindowNotAllowed(clause) if clause.contains("GROUP BY")));

	// 7. Semantic rejection in HAVING
	let sql = "SELECT dept FROM emp HAVING rank() OVER () > 1;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let err = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap_err();
	assert!(matches!(err, PlanError::WindowNotAllowed(clause) if clause.contains("HAVING")));

	// 8. Semantic rejection of nested window calls
	let sql = "SELECT sum(rank() OVER ()) OVER () FROM emp;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let err = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap_err();
	assert!(matches!(err, PlanError::WindowNotAllowed(clause) if clause.contains("arguments")));
}

#[test]
fn test_window_functions_mysql() {
	let mut catalog = MemoryCatalog::new();
	catalog.add_table(TableSchema::new(
		"emp",
		vec![
			ColumnInfo::new("id", 0).with_type("int8"),
			ColumnInfo::new("dept", 1).with_type("text"),
			ColumnInfo::new("salary", 2).with_type("int8"),
			ColumnInfo::new("hire_date", 3).with_type("text"),
		],
	));
	let my_policy = DialectPolicy::mysql();

	// 1. Basic OVER ()
	let sql = "SELECT ROW_NUMBER() OVER () FROM emp;";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let surrealdb_mysql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let bound = binder::mysql::bind_select(select, &catalog, &my_policy, None).unwrap();
	assert_eq!(bound.targets.len(), 1);
	let BoundExpr::WindowFunction {
		name,
		args,
		distinct,
		spec,
	} = &bound.targets[0].expr
	else {
		panic!("expected WindowFunction, got {:?}", &bound.targets[0].expr);
	};
	assert_eq!(name, "row_number");
	assert!(args.is_empty());
	assert!(!distinct);
	assert!(spec.partition_by.is_empty());
	assert!(spec.order_by.is_empty());
	assert!(spec.frame.is_none());
	let desc = bound.describe(&catalog, &my_policy);
	assert_eq!(desc.columns[0].ty, SqlType::Int8);
	// Lowering computes the call with a `Window` operator under the projection
	let op = lower_bound_select(&bound, &my_policy).unwrap();
	let LogicalOp::Project(project) = &op else {
		panic!("expected Project, got {op}");
	};
	assert!(matches!(&*project.input, LogicalOp::Window(_)), "{op}");

	// 2. Window function with PARTITION BY and ORDER BY
	let sql = "SELECT RANK() OVER (PARTITION BY dept ORDER BY salary DESC) FROM emp;";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let surrealdb_mysql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let bound = binder::mysql::bind_select(select, &catalog, &my_policy, None).unwrap();
	let BoundExpr::WindowFunction {
		name,
		spec,
		..
	} = &bound.targets[0].expr
	else {
		panic!("expected WindowFunction, got {:?}", &bound.targets[0].expr);
	};
	assert_eq!(name, "rank");
	assert_eq!(spec.partition_by.len(), 1);
	assert_eq!(spec.order_by.len(), 1);
	assert_eq!(spec.order_by[0].direction, BoundOrderDirection::Desc);

	// 3. Named window definition and reference
	let sql = "SELECT RANK() OVER w FROM emp WINDOW w AS (PARTITION BY dept ORDER BY salary DESC);";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let surrealdb_mysql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let bound = binder::mysql::bind_select(select, &catalog, &my_policy, None).unwrap();
	assert_eq!(bound.windows.len(), 1);
	assert_eq!(bound.windows[0].name, "w");
	assert_eq!(bound.windows[0].spec.partition_by.len(), 1);
	let BoundExpr::WindowFunction {
		spec,
		..
	} = &bound.targets[0].expr
	else {
		panic!("expected WindowFunction, got {:?}", &bound.targets[0].expr);
	};
	assert_eq!(spec.window_name.as_deref(), Some("w"));
	assert_eq!(spec.partition_by.len(), 1);
	assert_eq!(spec.order_by.len(), 1);

	// 4. ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
	let sql = "SELECT SUM(salary) OVER (PARTITION BY dept ORDER BY hire_date ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM emp;";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let surrealdb_mysql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let bound = binder::mysql::bind_select(select, &catalog, &my_policy, None).unwrap();
	let BoundExpr::WindowFunction {
		spec,
		..
	} = &bound.targets[0].expr
	else {
		panic!("expected WindowFunction, got {:?}", &bound.targets[0].expr);
	};
	let frame = spec.frame.as_ref().unwrap();
	assert_eq!(frame.units, BoundWindowFrameUnits::Rows);
	assert!(matches!(frame.start, BoundWindowFrameBound::Preceding(_)));
	assert!(matches!(frame.end, Some(BoundWindowFrameBound::Following(_))));

	// 5. Semantic rejection in WHERE
	let sql = "SELECT id FROM emp WHERE RANK() OVER () > 1;";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let surrealdb_mysql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let err = binder::mysql::bind_select(select, &catalog, &my_policy, None).unwrap_err();
	assert!(matches!(err, PlanError::WindowNotAllowed(clause) if clause.contains("WHERE")));

	// 6. Semantic rejection in GROUP BY
	let sql = "SELECT dept FROM emp GROUP BY RANK() OVER ();";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let surrealdb_mysql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let err = binder::mysql::bind_select(select, &catalog, &my_policy, None).unwrap_err();
	assert!(matches!(err, PlanError::WindowNotAllowed(clause) if clause.contains("GROUP BY")));

	// 7. Semantic rejection in HAVING
	let sql = "SELECT dept FROM emp HAVING RANK() OVER () > 1;";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let surrealdb_mysql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let err = binder::mysql::bind_select(select, &catalog, &my_policy, None).unwrap_err();
	assert!(matches!(err, PlanError::WindowNotAllowed(clause) if clause.contains("HAVING")));

	// 8. Semantic rejection of nested window calls
	let sql = "SELECT SUM(RANK() OVER ()) OVER () FROM emp;";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let surrealdb_mysql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let err = binder::mysql::bind_select(select, &catalog, &my_policy, None).unwrap_err();
	assert!(matches!(err, PlanError::WindowNotAllowed(clause) if clause.contains("arguments")));
}

#[test]
fn test_functions_and_security_isolation() {
	use crate::descriptor::SqlType;
	use crate::dialect::SqlDialect;
	use crate::functions::{FunctionRegistry, SqlFunctionKind};
	use crate::lower::expr::lower_bound_expr;

	let mut catalog = MemoryCatalog::new();
	catalog.define_table(None, "users", &["id", "name"]);
	let pg_policy = DialectPolicy::postgres();
	let my_policy = DialectPolicy::mysql();

	// 1. Direct registry lookup and classification
	let count_def = FunctionRegistry::lookup("count", SqlDialect::Postgres).unwrap();
	assert_eq!(count_def.kind, SqlFunctionKind::Aggregate);
	assert!(count_def.is_aggregate());

	let rank_def = FunctionRegistry::lookup("rank", SqlDialect::Postgres).unwrap();
	assert_eq!(rank_def.kind, SqlFunctionKind::Window);
	assert!(rank_def.is_window());

	let lower_def = FunctionRegistry::lookup("lower", SqlDialect::Postgres).unwrap();
	assert_eq!(lower_def.kind, SqlFunctionKind::Scalar);
	assert!(lower_def.is_scalar());

	// 2. Security isolation: namespace escapes strictly rejected
	assert!(FunctionRegistry::lookup("http::get", SqlDialect::Postgres).is_none());
	assert!(FunctionRegistry::lookup("crypto::md5", SqlDialect::Postgres).is_none());
	assert!(FunctionRegistry::lookup("file::read", SqlDialect::Postgres).is_none());
	assert!(FunctionRegistry::lookup("fn::custom", SqlDialect::Postgres).is_none());
	assert!(FunctionRegistry::lookup("rand::uuid", SqlDialect::Postgres).is_none());
	assert!(FunctionRegistry::lookup("sleep", SqlDialect::Postgres).is_none());
	assert!(FunctionRegistry::lookup("system_exec", SqlDialect::Postgres).is_none());
	assert!(FunctionRegistry::lookup("nonexistent_func", SqlDialect::Postgres).is_none());

	// 3. Dialect scoping: generate_series in PG, not in MySQL; ifnull in MySQL
	assert!(FunctionRegistry::lookup("generate_series", SqlDialect::Postgres).is_some());
	assert!(FunctionRegistry::lookup("generate_series", SqlDialect::MySql).is_none());
	assert!(FunctionRegistry::lookup("ifnull", SqlDialect::MySql).is_some());
	assert!(FunctionRegistry::lookup("coalesce", SqlDialect::Postgres).is_some());
	assert!(FunctionRegistry::lookup("coalesce", SqlDialect::MySql).is_some());

	// 4. PG binding: reject undefined function with SQLSTATE 42883
	let sql = "SELECT sleep(5) FROM users;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let err = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap_err();
	assert!(matches!(err, PlanError::UndefinedFunction(ref f) if f == "sleep"));
	assert_eq!(err.pg_sqlstate(), "42883");

	// 5. MySQL binding: reject undefined function with error code 1305
	let sql = "SELECT sleep(5) FROM users;";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let surrealdb_mysql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let err = binder::mysql::bind_select(select, &catalog, &my_policy, None).unwrap_err();
	assert!(matches!(err, PlanError::UndefinedFunction(ref f) if f == "sleep"));
	assert_eq!(err.mysql_error_code(), 1305);

	// 6. Arity mismatch: min_args check
	let sql = "SELECT round() FROM users;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let err = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap_err();
	assert!(
		matches!(err, PlanError::FunctionArgumentCountMismatch { ref name, got: 0, .. } if name == "round")
	);
	assert_eq!(err.pg_sqlstate(), "42883");

	// 7. Arity mismatch: max_args check
	let sql = "SELECT lower('a', 'b', 'c') FROM users;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let err = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap_err();
	assert!(
		matches!(err, PlanError::FunctionArgumentCountMismatch { ref name, got: 3, .. } if name == "lower")
	);

	// 8. Window function without OVER: SQLSTATE 42809 / MySQL 3593
	let sql = "SELECT rank() FROM users;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let err = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap_err();
	assert!(matches!(err, PlanError::WindowFunctionWithoutOver(ref f) if f == "rank"));
	assert_eq!(err.pg_sqlstate(), "42809");

	let sql = "SELECT rank() FROM users;";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let surrealdb_mysql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let err = binder::mysql::bind_select(select, &catalog, &my_policy, None).unwrap_err();
	assert!(matches!(err, PlanError::WindowFunctionWithoutOver(ref f) if f == "rank"));
	assert_eq!(err.mysql_error_code(), 3593);

	// 9. Scalar function with OVER rejected
	let sql = "SELECT lower(name) OVER () FROM users;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let err = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap_err();
	assert!(matches!(err, PlanError::WindowNotAllowed(_)));

	// 10. Return type inference
	assert_eq!(
		FunctionRegistry::infer_return_type("count", SqlDialect::Postgres, &[SqlType::Int8]),
		SqlType::Int8
	);
	assert_eq!(
		FunctionRegistry::infer_return_type("upper", SqlDialect::Postgres, &[SqlType::Text]),
		SqlType::Text
	);
	assert_eq!(
		FunctionRegistry::infer_return_type("now", SqlDialect::Postgres, &[]),
		SqlType::TimestampTz
	);
	assert_eq!(
		FunctionRegistry::infer_return_type("row_number", SqlDialect::Postgres, &[]),
		SqlType::Int8
	);
	assert_eq!(
		FunctionRegistry::infer_return_type("abs", SqlDialect::Postgres, &[SqlType::Float8]),
		SqlType::Float8
	);
	assert_eq!(
		FunctionRegistry::infer_return_type("abs", SqlDialect::Postgres, &[SqlType::Int4]),
		SqlType::Int4
	);

	// 11. Coalesce lowering to null-coalescing chain
	let bound_coalesce = BoundExpr::FunctionCall {
		name: "coalesce".to_string(),
		args: vec![
			BoundExpr::Literal(BoundLiteral::Null),
			BoundExpr::Literal(BoundLiteral::Integer(42)),
		],
		distinct: false,
	};
	let lowered = lower_bound_expr(&bound_coalesce, &pg_policy).unwrap();
	assert!(matches!(
		lowered,
		surrealdb_expr::expr::logical::LogicalExpr::Binary {
			op: surrealdb_expr::expr::BinaryOperator::NullCoalescing,
			..
		}
	));

	// 12. Nullif lowering to If
	let bound_nullif = BoundExpr::FunctionCall {
		name: "nullif".to_string(),
		args: vec![
			BoundExpr::Literal(BoundLiteral::Integer(10)),
			BoundExpr::Literal(BoundLiteral::Integer(10)),
		],
		distinct: false,
	};
	let lowered = lower_bound_expr(&bound_nullif, &pg_policy).unwrap();
	assert!(matches!(lowered, surrealdb_expr::expr::logical::LogicalExpr::If(_)));

	// 13. PG JSON binary operators
	let sql = "SELECT id FROM users WHERE id = 1;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let bound = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap();
	assert_eq!(bound.targets.len(), 1);
}

#[test]
fn test_pg_catalog_table_resolution_and_lowering() {
	let sql = "SELECT relname, relkind FROM pg_class WHERE relkind = 'r';";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};

	let mut catalog = MemoryCatalog::new();
	catalog.define_table(None, "orders", &["id", "amount"]);
	let policy = DialectPolicy::postgres();

	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).unwrap();
	assert_eq!(bound.targets.len(), 2);

	let op = lower_bound_select(&bound, &policy).unwrap();
	let LogicalOp::Project(proj) = op else {
		panic!("expected Project, got {op:?}");
	};
	let LogicalOp::Filter(filter) = *proj.input else {
		panic!("expected Filter input to Project, got {:?}", proj.input);
	};
	let LogicalOp::Values(values) = *filter.input else {
		panic!("expected Values input to Filter, got {:?}", filter.input);
	};
	let LogicalExpr::Array(rows) = values.expr else {
		panic!("expected Array of rows");
	};
	// Should contain system tables plus the dynamic 'orders' table
	assert!(rows.iter().any(|r| {
		if let LogicalExpr::Object(fields) = r {
			fields.iter().any(|(k, v)| {
				k == "relname"
					&& v == &LogicalExpr::Literal(
						surrealdb_expr::expr::logical::LogicalLiteral::String(
							surrealdb_strand::Strand::from("orders"),
						),
					)
			})
		} else {
			false
		}
	}));
}

#[test]
fn test_pg_type_query_and_columns() {
	let sql = "SELECT typname, typlen FROM pg_catalog.pg_type WHERE oid = 23;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};

	let catalog = MemoryCatalog::new();
	let policy = DialectPolicy::postgres();

	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).unwrap();
	assert_eq!(bound.targets.len(), 2);

	let op = lower_bound_select(&bound, &policy).unwrap();
	assert!(matches!(op, LogicalOp::Project(_)));
}

#[test]
fn test_pg_namespace_and_settings() {
	let sql = "SELECT nspname FROM pg_namespace;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};

	let catalog = MemoryCatalog::new();
	let policy = DialectPolicy::postgres();

	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).unwrap();
	let op = lower_bound_select(&bound, &policy).unwrap();
	let LogicalOp::Project(proj) = op else {
		panic!("expected Project, got {op:?}");
	};
	let LogicalOp::Values(v) = *proj.input else {
		panic!("expected Values, got {:?}", proj.input);
	};
	let LogicalExpr::Array(rows) = v.expr else {
		panic!("expected Array of rows");
	};
	assert_eq!(rows.len(), 4); // pg_catalog, public, information_schema, pg_toast

	let sql_settings = "SELECT name, setting FROM pg_settings;";
	let stmts = surrealdb_pgsql::parse_statements(sql_settings).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).unwrap();
	let op = lower_bound_select(&bound, &policy).unwrap();
	assert!(matches!(op, LogicalOp::Project(_)));
}

#[test]
fn test_information_schema_tables_and_columns() {
	let mut catalog = MemoryCatalog::new();
	catalog.define_table(None, "products", &["id", "name", "price"]);
	let policy = DialectPolicy::postgres();

	let sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).unwrap();
	let op = lower_bound_select(&bound, &policy).unwrap();
	assert!(matches!(op, LogicalOp::Project(_)));

	let sql_cols =
		"SELECT column_name FROM information_schema.columns WHERE table_name = 'products';";
	let stmts = surrealdb_pgsql::parse_statements(sql_cols).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).unwrap();
	let op = lower_bound_select(&bound, &policy).unwrap();
	assert!(matches!(op, LogicalOp::Project(_)));
}

#[test]
fn test_catalog_introspection_functions() {
	let sql = "SELECT version(), current_schema(), current_schemas(true), current_user, session_user, user, pg_backend_pid(), pg_is_in_recovery(), format_type(23, -1), pg_table_is_visible(1259), has_table_privilege('users', 'SELECT');";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};

	let catalog = MemoryCatalog::new();
	let policy = DialectPolicy::postgres();

	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).unwrap();
	assert_eq!(bound.targets.len(), 11);

	let op = lower_bound_select(&bound, &policy).unwrap();
	assert!(matches!(op, LogicalOp::Project(_)));
}

#[test]
fn test_pg_class_join_pg_namespace() {
	let sql = "SELECT c.relname, n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = 'pg_type';";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};

	let catalog = MemoryCatalog::new();
	let policy = DialectPolicy::postgres();

	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).unwrap();
	let op = lower_bound_select(&bound, &policy).unwrap();
	let LogicalOp::Project(proj) = op else {
		panic!("expected Project, got {op:?}");
	};
	let LogicalOp::Filter(filter) = *proj.input else {
		panic!("expected Filter, got {:?}", proj.input);
	};
	let LogicalOp::Join(join) = *filter.input else {
		panic!("expected Join, got {:?}", filter.input);
	};
	assert!(matches!(*join.left(), LogicalOp::Values(_)));
	assert!(matches!(*join.right(), LogicalOp::Values(_)));
}

#[test]
fn test_mysql_user_and_system_variables() {
	let sql =
		"SELECT @@version_comment, @@sql_mode, @@autocommit, @@global.time_zone, @session_var;";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let surrealdb_mysql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};

	let catalog = MemoryCatalog::new();
	let policy = DialectPolicy::mysql();

	let bound = binder::mysql::bind_select(select, &catalog, &policy, None).unwrap();
	assert_eq!(bound.targets.len(), 5);

	let desc = bound.describe(&catalog, &policy);
	assert_eq!(desc.columns.len(), 5);
	assert_eq!(desc.columns[0].name, "@@version_comment");
	assert_eq!(desc.columns[0].ty, SqlType::Text);
	assert_eq!(desc.columns[1].name, "@@sql_mode");
	assert_eq!(desc.columns[1].ty, SqlType::Text);
	assert_eq!(desc.columns[2].name, "@@autocommit");
	assert_eq!(desc.columns[2].ty, SqlType::Int8);
	assert_eq!(desc.columns[3].name, "@@global.time_zone");
	assert_eq!(desc.columns[3].ty, SqlType::Text);
	assert_eq!(desc.columns[4].name, "@session_var");

	let op = lower_bound_select(&bound, &policy).unwrap();
	assert!(matches!(op, LogicalOp::Project(_)));
}

#[test]
fn test_mysql_operators_null_safe_and_int_div() {
	let policy = DialectPolicy::mysql();

	// Query binding, constant folding, and lowering
	let sql = "SELECT 1 <=> 1, NULL <=> NULL, 1 <=> NULL, 10 DIV 3, 10 DIV 0;";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let surrealdb_mysql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let catalog = MemoryCatalog::new();
	let mut bound = binder::mysql::bind_select(select, &catalog, &policy, None).unwrap();
	fold_select(&mut bound, &policy).unwrap();
	assert_eq!(bound.targets[0].expr, BoundExpr::Literal(BoundLiteral::Boolean(true)));
	assert_eq!(bound.targets[1].expr, BoundExpr::Literal(BoundLiteral::Boolean(true)));
	assert_eq!(bound.targets[2].expr, BoundExpr::Literal(BoundLiteral::Boolean(false)));
	assert_eq!(bound.targets[3].expr, BoundExpr::Literal(BoundLiteral::Integer(3)));
	assert_eq!(bound.targets[4].expr, BoundExpr::Literal(BoundLiteral::Null));

	let op = lower_bound_select(&bound, &policy).unwrap();
	assert!(matches!(op, LogicalOp::Project(_)));
}

#[test]
fn test_mysql_is_truth_values() {
	let sql = "SELECT 1 IS TRUE, 0 IS FALSE, NULL IS UNKNOWN, 1 IS NOT FALSE;";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let surrealdb_mysql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};

	let catalog = MemoryCatalog::new();
	let policy = DialectPolicy::mysql();

	let bound = binder::mysql::bind_select(select, &catalog, &policy, None).unwrap();
	assert_eq!(bound.targets.len(), 4);
	let op = lower_bound_select(&bound, &policy).unwrap();
	assert!(matches!(op, LogicalOp::Project(_)));
}

#[test]
fn test_mysql_quantified_subqueries() {
	let mut catalog = MemoryCatalog::new();
	catalog.add_table(TableSchema::new("t", vec![ColumnInfo::new("id", 0).with_type("int8")]));
	catalog.add_table(TableSchema::new("s", vec![ColumnInfo::new("val", 0).with_type("int8")]));

	let policy = DialectPolicy::mysql();

	let sql = "SELECT id FROM t WHERE id > ALL (SELECT val FROM s);";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let surrealdb_mysql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let bound = binder::mysql::bind_select(select, &catalog, &policy, None).unwrap();
	let op = lower_bound_select(&bound, &policy).unwrap();
	assert!(matches!(op, LogicalOp::Project(_)));

	let sql_any = "SELECT id FROM t WHERE id = ANY (SELECT val FROM s);";
	let stmts_any = surrealdb_mysql::parse_statements(sql_any).unwrap();
	let surrealdb_mysql::ast::stmt::Statement::Select(select_any) = &stmts_any[0] else {
		panic!("expected SELECT");
	};
	let bound_any = binder::mysql::bind_select(select_any, &catalog, &policy, None).unwrap();
	let op_any = lower_bound_select(&bound_any, &policy).unwrap();
	assert!(matches!(op_any, LogicalOp::Project(_)));
}

#[test]
fn test_mysql_builtin_functions() {
	let sql = "SELECT version(), found_rows(), row_count(), connection_id(), last_insert_id(), curdate(), curtime(), utc_timestamp(), unix_timestamp(), truncate(12.345, 1), regexp_like('abc', '^a');";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let surrealdb_mysql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};

	let catalog = MemoryCatalog::new();
	let policy = DialectPolicy::mysql();

	let bound = binder::mysql::bind_select(select, &catalog, &policy, None).unwrap();
	assert_eq!(bound.targets.len(), 11);

	let op = lower_bound_select(&bound, &policy).unwrap();
	assert!(matches!(op, LogicalOp::Project(_)));
}

#[test]
fn test_collection_aggregate_forms_that_would_answer_wrongly_are_refused() {
	let policy = DialectPolicy::postgres();
	for sql in [
		"SELECT array_agg(DISTINCT x) FROM t;",
		"SELECT json_agg(DISTINCT x) FROM t;",
		"SELECT string_agg(DISTINCT x, ',') FROM t;",
	] {
		let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
		let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
			panic!("expected SELECT");
		};
		let bound = binder::pgsql::bind_select(select, &MemoryCatalog::new(), &policy, None)
			.expect("bind failed");
		let refused = lower_bound_select(&bound, &policy);
		assert!(
			matches!(refused, Err(crate::error::PlanError::UnsupportedExpression(_))),
			"{sql}: {refused:?}"
		);
	}

	// FILTER would leave a NULL for every filtered row of an aggregate that keeps NULLs.
	for sql in [
		"SELECT array_agg(x) FILTER (WHERE y > 1) FROM t;",
		"SELECT jsonb_agg(x) FILTER (WHERE y > 1) FROM t;",
	] {
		let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
		let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
			panic!("expected SELECT");
		};
		let refused = binder::pgsql::bind_select(select, &MemoryCatalog::new(), &policy, None);
		assert!(
			matches!(refused, Err(crate::error::PlanError::UnsupportedExpression(_))),
			"{sql}: {refused:?}"
		);
	}

	// GROUP_CONCAT(a, b) concatenates the two per row, which is not a separator.
	let policy = DialectPolicy::mysql();
	for sql in ["SELECT GROUP_CONCAT(a, b) FROM t;", "SELECT GROUP_CONCAT(DISTINCT a) FROM t;"] {
		let stmts = surrealdb_mysql::parse_statements(sql).expect("parse failed");
		let surrealdb_mysql::ast::stmt::Statement::Select(select) = &stmts[0] else {
			panic!("expected SELECT");
		};
		let bound = binder::mysql::bind_select(select, &MemoryCatalog::new(), &policy, None)
			.expect("bind failed");
		let refused = lower_bound_select(&bound, &policy);
		assert!(
			matches!(refused, Err(crate::error::PlanError::UnsupportedExpression(_))),
			"{sql}: {refused:?}"
		);
	}
}

#[test]
fn test_qualified_star_over_unaliased_system_view_expands_columns() {
	let sql = "SELECT columns.* FROM information_schema.columns WHERE columns.table_name = 'x';";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let catalog = MemoryCatalog::new();
	let policy = DialectPolicy::postgres();
	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).unwrap();
	assert!(bound.targets.len() > 10);
	assert!(bound.targets.iter().any(|t| t.alias.as_deref() == Some("udt_name")));
	assert!(lower_bound_select(&bound, &policy).is_ok());
}

#[test]
fn test_correlated_subquery_in_join_condition_binds_and_lowers() {
	let sql = "SELECT columns.column_name FROM information_schema.columns LEFT JOIN pg_catalog.pg_attribute AS a ON a.attname = columns.column_name AND a.attrelid = (SELECT cls.oid FROM pg_catalog.pg_class AS cls WHERE cls.relname = columns.table_name);";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let catalog = MemoryCatalog::new();
	let policy = DialectPolicy::postgres();
	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).unwrap();
	assert!(lower_bound_select(&bound, &policy).is_ok());
}

#[test]
fn test_format_type_of_a_column_and_regtype_cast_lower() {
	let sql = "SELECT format_type(a.atttypid, a.atttypmod), ('\"' || 'pg_catalog' || '\".\"' || 'int8' || '\"')::\"regtype\"::text, format_type(NULL, NULL) FROM pg_catalog.pg_attribute AS a;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let catalog = MemoryCatalog::new();
	let policy = DialectPolicy::postgres();
	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).unwrap();
	let op = lower_bound_select(&bound, &policy).unwrap();
	let LogicalOp::Project(proj) = op else {
		panic!("expected projection");
	};
	let rendered = format!("{:?}", proj.fields);
	assert!(rendered.contains("bigint") && rendered.contains("timestamp with time zone"));
}

#[test]
fn test_udt_name_is_the_internal_type_name() {
	use crate::SqlType;
	for (data_type, udt) in [
		("bigint", "int8"),
		("integer", "int4"),
		("boolean", "bool"),
		("text", "text"),
		("jsonb", "jsonb"),
		("text[]", "_text"),
		("timestamp with time zone", "timestamptz"),
		("double precision", "float8"),
		("numeric", "numeric"),
	] {
		assert_eq!(SqlType::from_name(data_type).udt_name().as_deref(), Some(udt), "{data_type}");
	}
	assert_eq!(SqlType::Unknown.udt_name(), None);
}

#[test]
fn unnest_targets_split_a_pipeline_of_their_own_and_read_their_position() {
	use surrealdb_expr::expr::logical::{LogicalField, LogicalFields};
	let catalog = MemoryCatalog::new();
	let policy = DialectPolicy::postgres();
	let lower = |sql: &str| {
		let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
		let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
			panic!("expected SELECT");
		};
		let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).unwrap();
		lower_bound_select(&bound, &policy)
	};
	let field_exprs = |op: &LogicalOp| {
		let LogicalOp::Project(project) = op else {
			panic!("expected Project, got {op:?}");
		};
		let LogicalFields::Select(fields) = &project.fields else {
			panic!("expected Select fields");
		};
		let exprs = fields
			.iter()
			.map(|field| {
				let LogicalField::Single {
					expr,
					..
				} = field
				else {
					panic!("expected a single field");
				};
				expr.to_string()
			})
			.collect::<Vec<_>>();
		(exprs, project.input.clone())
	};

	let (exprs, input) = field_exprs(
		&lower("SELECT UNNEST(conkey) AS parent, UNNEST(confkey) AS child FROM pg_constraint")
			.unwrap(),
	);
	assert_eq!(exprs, ["__sql_unnest[0]", "__sql_unnest[1]"]);
	let LogicalOp::Filter(filter) = *input else {
		panic!("expected the empty rows dropped under the projection, got {input:?}");
	};
	assert_eq!(filter.predicate.to_string(), "array::len(__sql_unnest) > 0");
	let LogicalOp::Subquery(subquery) = *filter.input else {
		panic!("expected the split pipeline under the filter, got {:?}", filter.input);
	};
	let LogicalOp::Split(split) = *subquery.input else {
		panic!("expected Split, got {:?}", subquery.input);
	};
	assert!(matches!(&*split.input, LogicalOp::Project(_)));

	let (exprs, _) = field_exprs(&lower("SELECT UNNEST(conkey) FROM pg_constraint").unwrap());
	assert_eq!(exprs, ["__sql_unnest"]);

	assert!(lower("SELECT UNNEST(conkey) + 1 FROM pg_constraint").is_err());
	assert!(lower("SELECT count(*), UNNEST(conkey) FROM pg_constraint").is_err());
}

#[test]
fn a_quantified_comparison_against_an_array_binds_to_one_internal_call_and_refuses_other_operators()
{
	let catalog = MemoryCatalog::new();
	let policy = DialectPolicy::postgres();
	let bind = |sql: &str| {
		let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
		let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
			panic!("expected SELECT");
		};
		binder::pgsql::bind_select(select, &catalog, &policy, None)
	};
	for (sql, equal, all) in [
		("SELECT x = ANY (arr) FROM t", true, false),
		("SELECT x = SOME (arr) FROM t", true, false),
		("SELECT x <> ALL (arr) FROM t", false, true),
		("SELECT x = ALL (arr) FROM t", true, true),
		("SELECT x <> ANY (arr) FROM t", false, false),
	] {
		let bound = bind(sql).unwrap();
		let BoundExpr::FunctionCall {
			name,
			args,
			..
		} = &bound.targets[0].expr
		else {
			panic!("{sql}: {:?}", bound.targets[0].expr);
		};
		assert_eq!(name, crate::functions::QUANTIFIED_ARRAY);
		assert_eq!(args.len(), 4, "{sql}");
		assert_eq!(args[2], BoundExpr::Literal(crate::ir::BoundLiteral::Boolean(equal)), "{sql}");
		assert_eq!(args[3], BoundExpr::Literal(crate::ir::BoundLiteral::Boolean(all)), "{sql}");
		assert!(lower_bound_select(&bound, &policy).is_ok(), "{sql}");
	}
	for sql in ["SELECT x < ANY (arr) FROM t", "SELECT x >= ALL (arr) FROM t"] {
		let err = bind(sql).unwrap_err();
		assert_eq!(err.pg_sqlstate(), "0A000", "{sql}");
	}
}

fn lower_pgsql_select(
	catalog: &dyn BinderCatalog,
	sql: &str,
) -> Result<LogicalOp, crate::error::PlanError> {
	let policy = DialectPolicy::postgres();
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let bound = binder::pgsql::bind_select(select, catalog, &policy, None)?;
	lower_bound_select(&bound, &policy)
}

#[test]
fn a_lateral_subquery_of_set_returning_targets_splits_the_rows_it_extends() {
	let catalog = MemoryCatalog::new();
	let lowered = lower_pgsql_select(
		&catalog,
		"SELECT k.attnum, k.pos FROM pg_index i CROSS JOIN LATERAL \
		 (SELECT unnest(i.indkey), generate_subscripts(i.indkey, 1) + 1) AS k(attnum, pos) \
		 WHERE k.pos = 2",
	)
	.unwrap();
	let plan = format!("{lowered:?}");
	assert!(plan.contains("Split"), "{plan}");
	assert!(plan.contains("__lateral0_0") && plan.contains("__lateral0_1"), "{plan}");
	// The columns of the relation are read from the object under the alias of its join.
	assert!(plan.contains("_join0"), "{plan}");
}

#[test]
fn a_lateral_subquery_is_a_select_of_expressions_joined_inner_or_cross() {
	let catalog = MemoryCatalog::new();
	for sql in [
		"SELECT 1 FROM pg_index i LEFT JOIN LATERAL (SELECT unnest(i.indkey)) AS k(a) ON true",
		"SELECT 1 FROM pg_index i CROSS JOIN LATERAL (SELECT * FROM pg_class) AS k",
		"SELECT 1 FROM pg_index i CROSS JOIN LATERAL (SELECT i.indkey WHERE true) AS k(a)",
		"SELECT 1 FROM pg_index i CROSS JOIN LATERAL (SELECT lower(unnest(i.indkey))) AS k(a)",
	] {
		assert!(lower_pgsql_select(&catalog, sql).is_err(), "{sql}");
	}
	assert!(
		lower_pgsql_select(
			&catalog,
			"SELECT k.a FROM pg_index i CROSS JOIN LATERAL (SELECT i.indrelid + 1) AS k(a)"
		)
		.is_ok()
	);
}

#[test]
fn a_column_a_using_clause_merged_is_not_ambiguous() {
	let catalog = MemoryCatalog::new();
	assert!(
		lower_pgsql_select(
			&catalog,
			"SELECT constraint_name FROM information_schema.table_constraints \
			 JOIN information_schema.constraint_column_usage USING (constraint_name)"
		)
		.is_ok()
	);
	assert!(
		lower_pgsql_select(
			&catalog,
			"SELECT constraint_name FROM information_schema.table_constraints \
			 JOIN information_schema.constraint_column_usage ON true"
		)
		.is_err()
	);
}

/// The rows a plan reads from the one synthesised relation under its projections and filters.
fn rows_of_the_synthesised_relation(op: &LogicalOp) -> &[LogicalExpr] {
	match op {
		LogicalOp::Project(project) => rows_of_the_synthesised_relation(&project.input),
		LogicalOp::Filter(filter) => rows_of_the_synthesised_relation(&filter.input),
		LogicalOp::Values(values) => {
			let LogicalExpr::Array(rows) = &values.expr else {
				panic!("expected an array of rows, got {:?}", values.expr);
			};
			rows
		}
		other => panic!("expected a synthesised relation, got {other:?}"),
	}
}

#[test]
fn pg_matviews_lowers_to_no_rows_however_it_is_named_and_whatever_tables_exist() {
	let mut catalog = MemoryCatalog::new();
	catalog.define_table(None, "orders", &["id", "amount"]);
	catalog.define_table(None, "typeorm_metadata", &["type", "schema", "name", "value"]);
	for sql in [
		"SELECT * FROM pg_matviews",
		"SELECT * FROM \"pg_matviews\"",
		"SELECT * FROM pg_catalog.pg_matviews",
		"SELECT * FROM \"pg_catalog\".\"pg_matviews\"",
		"SELECT schemaname, matviewname FROM pg_catalog.pg_matviews WHERE schemaname = 'public'",
		"SELECT \"schemaname\", \"matviewname\", \"definition\" FROM \"pg_catalog\".\"pg_matviews\" \
		 WHERE \"hasindexes\" AND NOT \"ispopulated\" AND \"tablespace\" IS NULL",
		"SELECT * FROM pg_matviews AS mv WHERE mv.matviewowner = 'root'",
	] {
		let op = lower_pgsql_select(&catalog, sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
		assert!(rows_of_the_synthesised_relation(&op).is_empty(), "{sql}");
	}
}

#[test]
fn a_bit_test_and_an_array_subscript_lower() {
	let catalog = MemoryCatalog::new();
	assert!(lower_pgsql_select(&catalog, "SELECT indoption[1] & 2 = 2 FROM pg_index").is_ok());
	assert!(lower_pgsql_select(&catalog, "SELECT indoption[1] & 3 FROM pg_index").is_err());
	assert!(lower_pgsql_select(&catalog, "SELECT indoption & indnatts FROM pg_index").is_err());
}

#[test]
fn a_mysql_arithmetic_expression_widens_its_narrow_columns() {
	let catalog = MemoryCatalog::new()
		.with_column_type("t", "a", crate::SqlType::Int4)
		.with_column_type("t", "f", crate::SqlType::Float4);
	let plan = |dialect: &str, sql: &str| {
		if dialect == "mysql" {
			let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
			let surrealdb_mysql::ast::stmt::Statement::Select(select) = &stmts[0] else {
				panic!("expected SELECT");
			};
			let policy = DialectPolicy::mysql();
			let bound = binder::mysql::bind_select(select, &catalog, &policy, None).unwrap();
			format!("{:?}", lower_bound_select(&bound, &policy).unwrap())
		} else {
			format!("{:?}", lower_pgsql_select(&catalog, sql).unwrap())
		}
	};
	assert!(plan("mysql", "SELECT a + a FROM t").contains("Cast(Int)"));
	assert!(plan("mysql", "SELECT f + f FROM t").contains("Cast(Float)"));
	assert!(!plan("pgsql", "SELECT a + a FROM t").contains("Cast(Int)"));
}

#[test]
fn pg_get_indexdef_reads_the_definitions_of_the_indexes_the_catalog_lists() {
	let listed = MemoryCatalog::new().with_listed_tables(vec![
		TableSchema::new("Item", vec![ColumnInfo::new("id", 0), ColumnInfo::new("n", 1)])
			.with_schema("public"),
	]);
	let plan =
		format!("{:?}", lower_pgsql_select(&listed, "SELECT pg_get_indexdef(1000000)").unwrap());
	assert!(
		plan.contains("CREATE UNIQUE INDEX Item_pkey ON public.\\\"Item\\\" USING btree (id)")
			|| plan.contains("CREATE UNIQUE INDEX"),
		"{plan}"
	);
}

#[test]
fn pg_get_serial_sequence_reads_the_sequences_the_catalog_lists() {
	let listed = MemoryCatalog::new().with_listed_tables(vec![
		TableSchema::new(
			"Item",
			vec![ColumnInfo::new("id", 0).with_sequence("Item_id_seq"), ColumnInfo::new("n", 1)],
		)
		.with_schema("public"),
	]);
	let plan = |catalog: &dyn BinderCatalog| {
		format!(
			"{:?}",
			lower_pgsql_select(
				catalog,
				"SELECT pg_get_serial_sequence('\"public\".\"Item\"', 'id')"
			)
			.unwrap()
		)
	};
	let with = plan(&listed);
	assert!(with.contains("Item_id_seq"), "{with}");
	let without = plan(&MemoryCatalog::new());
	assert!(!without.contains("Item_id_seq"), "{without}");
}

#[test]
fn psql_database_listing_functions_lower() {
	let catalog = MemoryCatalog::new();
	let plan = |sql: &str| format!("{:?}", lower_pgsql_select(&catalog, sql).unwrap());
	assert!(plan("SELECT pg_encoding_to_char(6)").contains("UTF8"));
	assert!(plan("SELECT array_upper(string_to_array('a,b', ','), 1)").contains("array::len"));
	let collated = plan("SELECT 'a' COLLATE pg_catalog.default");
	assert!(!collated.contains("Collate"), "{collated}");
	assert!(lower_pgsql_select(&catalog, "SELECT 'a' COLLATE \"de_DE\"").is_err());
}

#[test]
fn text_compared_with_an_oid_column_is_read_as_an_oid() {
	let catalog = MemoryCatalog::new();
	let plan = format!(
		"{:?}",
		lower_pgsql_select(&catalog, "SELECT relkind FROM pg_catalog.pg_class WHERE oid = '1259'")
			.unwrap()
	);
	assert!(!plan.contains("String(\"1259\")") && !plan.contains("'1259'"), "{plan}");
	assert!(plan.contains("1259"), "{plan}");
}

#[test]
fn a_regclass_cast_compared_with_an_oid_column_names_the_oid_of_the_table() {
	let listed = MemoryCatalog::new().with_listed_tables(vec![
		TableSchema::new("first", vec![ColumnInfo::new("id", 0)]).with_schema("public"),
		TableSchema::new("second", vec![ColumnInfo::new("id", 0)]).with_schema("public"),
	]);
	let sql = "SELECT relkind FROM pg_catalog.pg_class WHERE oid = $1::pg_catalog.regclass";
	let plan = format!("{:?}", lower_pgsql_select(&listed, sql).unwrap());
	for oid in ["16384", "16385"] {
		assert!(plan.contains(oid), "{plan}");
	}
	let reversed = "SELECT relkind FROM pg_catalog.pg_class WHERE $1::regclass = oid";
	let plan = format!("{:?}", lower_pgsql_select(&listed, reversed).unwrap());
	assert!(plan.contains("16385"), "{plan}");
}

#[test]
fn a_regtype_array_constant_names_the_oids_of_its_types() {
	let catalog = MemoryCatalog::new();
	let plan = format!(
		"{:?}",
		lower_pgsql_select(&catalog, "SELECT 20 = ANY ('{int,int8,int2}'::regtype[])").unwrap()
	);
	for oid in ["23", "20", "21"] {
		assert!(plan.contains(oid), "{plan}");
	}
}

#[test]
fn test_mysql_case_folding_cte_aliases_and_using() {
	let mut catalog = MemoryCatalog::new();
	catalog.add_table(TableSchema::new(
		"users",
		vec![
			ColumnInfo::new("id", 0).with_type("int8"),
			ColumnInfo::new("dept_id", 1).with_type("int8"),
		],
	));
	catalog.add_table(TableSchema::new(
		"orders",
		vec![
			ColumnInfo::new("id", 0).with_type("int8"),
			ColumnInfo::new("dept_id", 1).with_type("int8"),
		],
	));
	let policy = DialectPolicy::mysql();

	let sql = "WITH MyCte (ColA) AS (SELECT id FROM users) SELECT m.cola, u.dept_id FROM MyCte AS M JOIN users AS U ON M.ColA = U.id JOIN orders AS O USING (DEPT_ID);";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let surrealdb_mysql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let bound = binder::mysql::bind_select(select, &catalog, &policy, None).unwrap();
	let with = bound.with.as_ref().unwrap();
	assert_eq!(with.ctes[0].name, "mycte");
	assert_eq!(with.ctes[0].columns, vec!["cola".to_string()]);
	let op = lower_bound_select(&bound, &policy).unwrap();
	assert!(matches!(op, LogicalOp::Project(_)));
}

#[test]
fn test_mysql_system_catalog_select_allowed_with_active_database() {
	let catalog = MemoryCatalog::new().with_database("mydb");
	let policy = DialectPolicy::mysql();
	for sql in [
		"SELECT * FROM pg_catalog.pg_namespace WHERE nspname = 'public';",
		"SELECT * FROM information_schema.tables;",
	] {
		let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
		let surrealdb_mysql::ast::stmt::Statement::Select(select) = &stmts[0] else {
			panic!("expected SELECT");
		};
		let bound = binder::mysql::bind_select(select, &catalog, &policy, None).unwrap();
		assert!(lower_bound_select(&bound, &policy).is_ok());
	}

	let cross_db = surrealdb_mysql::parse_statements("SELECT * FROM otherdb.users;").unwrap();
	let surrealdb_mysql::ast::stmt::Statement::Select(select) = &cross_db[0] else {
		panic!("expected SELECT");
	};
	assert!(binder::mysql::bind_select(select, &catalog, &policy, None).is_err());
}

#[test]
fn test_sum_and_avg_fold_their_column_as_written() {
	// The empty-group witness of SUM and AVG folds the bare column: a per-row test inside an
	// aggregate argument keeps the scan from running on the stored row.
	let sql = "SELECT city, sum(score), avg(score) FROM record GROUP BY city;";
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let catalog = MemoryCatalog::new();
	let policy = DialectPolicy::postgres();
	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).expect("bind failed");
	let op = lower_bound_select(&bound, &policy).expect("lower failed");
	let LogicalOp::Project(proj) = op else {
		panic!("expected LogicalOp::Project, got {op:?}");
	};
	let LogicalOp::Aggregate(agg) = &*proj.input else {
		panic!("expected LogicalOp::Aggregate inside project, got {:?}", proj.input);
	};
	let fields = format!("{:?}", agg.fields);
	assert!(fields.contains("array::min"), "no empty-group witness in {fields}");
	assert!(
		!fields.contains("args: [If("),
		"a per-row test reached an aggregate argument: {fields}"
	);
}

#[test]
fn test_count_of_a_column_folds_the_column_as_written() {
	// COUNT(column) is the engine's aggregate that counts the values that are neither NULL nor
	// NONE, over the bare column: a per-row test inside an aggregate argument keeps the scan from
	// running on the stored row.
	let sql = "SELECT city, count(score) FROM record GROUP BY city;";
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let catalog = MemoryCatalog::new();
	let policy = DialectPolicy::postgres();
	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).expect("bind failed");
	let op = lower_bound_select(&bound, &policy).expect("lower failed");
	let LogicalOp::Project(proj) = op else {
		panic!("expected LogicalOp::Project, got {op:?}");
	};
	let LogicalOp::Aggregate(agg) = &*proj.input else {
		panic!("expected LogicalOp::Aggregate inside project, got {:?}", proj.input);
	};
	let fields = format!("{:?}", agg.fields);
	assert!(fields.contains("count::present"), "COUNT(column) is not count::present: {fields}");
	assert!(
		!fields.contains("args: [If("),
		"a per-row test reached an aggregate argument: {fields}"
	);
}

#[test]
fn isfinite_compares_with_the_infinite_values_of_its_own_kind() {
	let catalog = MemoryCatalog::new();
	let cases = [
		("SELECT isfinite(CAST('2020-01-01' AS date))", "type::date"),
		("SELECT isfinite(CAST('2020-01-01 00:00:00' AS timestamp))", "type::timestamp"),
		("SELECT isfinite(CAST('2020-01-01 00:00:00+00' AS timestamptz))", "type::datetime"),
	];
	for (sql, constructor) in cases {
		let plan = format!("{:?}", lower_pgsql_select(&catalog, sql).unwrap());
		assert!(plan.contains(constructor), "{sql}: {plan}");
		assert!(plan.contains("infinity") && plan.contains("-infinity"), "{sql}: {plan}");
	}
	let err = lower_pgsql_select(&catalog, "SELECT isfinite('2020-01-01')").unwrap_err();
	assert!(err.to_string().contains("isfinite"), "{err}");
}

#[test]
fn format_type_of_a_column_names_the_declared_modifiers_of_the_catalog() {
	use crate::declared_type::DeclaredSqlType;
	let mut catalog = MemoryCatalog::new();
	catalog.add_table(
		TableSchema::new(
			"decl",
			vec![
				ColumnInfo::new("v", 0).with_type("varchar").with_declared(
					DeclaredSqlType::parse(crate::SqlTypeDialect::PostgreSql, "varchar(20)")
						.expect("a PostgreSQL type"),
				),
			],
		)
		.with_schema("public"),
	);
	let sql = "SELECT format_type(a.atttypid, a.atttypmod) FROM pg_catalog.pg_attribute AS a";
	let lowered = lower_pgsql_select(&catalog, sql).unwrap();
	let plan = format!("{lowered:?}");
	assert!(plan.contains("character varying(20)"), "{plan}");
	assert!(plan.contains("integer"), "{plan}");
	// The lookup keeps the name of the call, against this catalog and against one without the
	// declared column alike.
	assert!(plan.contains("Field(\"format_type\")"), "{plan}");
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let policy = DialectPolicy::postgres();
	for catalog in [&catalog, &MemoryCatalog::new()] {
		let bound = binder::pgsql::bind_select(select, catalog, &policy, None).unwrap();
		assert_eq!(bound.describe(catalog, &policy).columns[0].name, "format_type");
	}
}

#[test]
fn generate_series_over_fractional_bounds_is_refused_when_planned() {
	let catalog = MemoryCatalog::new();
	let policy = DialectPolicy::postgres();
	let lower = |sql: &str| {
		let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
		let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
			panic!("expected SELECT");
		};
		let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).unwrap();
		lower_bound_select(&bound, &policy)
	};
	assert!(lower("SELECT generate_series(1, 3)").is_ok());
	assert!(lower("SELECT generate_series(1.5, 3.5)").is_err());
	assert!(lower("SELECT generate_series(1, 3.5)").is_err());
}

#[test]
fn a_network_column_casts_to_text_and_orders_by_its_declared_type() {
	use surrealdb_expr::expr::statements::define::SqlTypeDialect;
	use surrealdb_expr::expr::statements::define::declared_type::DeclaredSqlType;

	let sql = "SELECT a::text AS shown FROM host ORDER BY a;";
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let mut catalog = MemoryCatalog::new();
	catalog.define_table(None, "host", &["id", "a"]);
	catalog.add_sql_type(
		"host",
		"a",
		DeclaredSqlType::parse(SqlTypeDialect::PostgreSql, "inet").expect("inet parses"),
	);
	let policy = DialectPolicy::postgres();
	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).expect("bind failed");
	let op = lower_bound_select(&bound, &policy).expect("lower failed");
	let plan = format!("{op:?}");
	assert!(plan.contains("sql::text"), "{plan}");
	assert!(plan.contains("sql::network_key"), "{plan}");
}

#[test]
fn a_range_bound_is_described_by_the_range_element_type() {
	use surrealdb_expr::expr::statements::define::SqlTypeDialect;
	use surrealdb_expr::expr::statements::define::declared_type::DeclaredSqlType;

	let sql = "SELECT lower(r) AS lo, upper('[2024-01-01,2024-02-01)'::daterange) AS hi FROM t;";
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let mut catalog = MemoryCatalog::new();
	catalog.define_table(None, "t", &["id", "r"]);
	catalog.add_sql_type(
		"t",
		"r",
		DeclaredSqlType::parse(SqlTypeDialect::PostgreSql, "int4range").expect("int4range parses"),
	);
	catalog.add_column_type("t", "r", SqlType::Range(crate::descriptor::RangeElement::Int4));
	let policy = DialectPolicy::postgres();
	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).expect("bind failed");
	let described = bound.describe(&catalog, &policy);
	let types: Vec<SqlType> = described.columns.iter().map(|column| column.ty).collect();
	assert_eq!(types, [SqlType::Int4, SqlType::Date]);
}

#[test]
fn a_parameter_compared_with_a_network_column_reads_as_a_network() {
	use surrealdb_expr::expr::statements::define::SqlTypeDialect;
	use surrealdb_expr::expr::statements::define::declared_type::DeclaredSqlType;

	let sql = "SELECT id FROM host WHERE a = $1;";
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let mut catalog = MemoryCatalog::new();
	catalog.define_table(None, "host", &["id", "a"]);
	catalog.add_sql_type(
		"host",
		"a",
		DeclaredSqlType::parse(SqlTypeDialect::PostgreSql, "inet").expect("inet parses"),
	);
	let policy = DialectPolicy::postgres();
	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).expect("bind failed");
	let plan = format!("{:?}", lower_bound_select(&bound, &policy).expect("lower failed"));
	assert!(plan.contains("sql::network_text"), "{plan}");
	assert!(plan.contains("String(\"inet\")"), "{plan}");
}

#[test]
fn a_parameter_opposite_a_range_is_described_as_that_range() {
	for sql in [
		"SELECT id FROM t WHERE r && $1;",
		"SELECT id FROM t WHERE r @> $1;",
		"SELECT id FROM t WHERE $1 <@ r;",
	] {
		let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
		let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
			panic!("expected SELECT");
		};
		let mut catalog = MemoryCatalog::new();
		catalog.define_table(None, "t", &["id", "r"]);
		catalog.add_column_type("t", "r", SqlType::Range(crate::descriptor::RangeElement::Int4));
		let policy = DialectPolicy::postgres();
		let bound =
			binder::pgsql::bind_select(select, &catalog, &policy, None).expect("bind failed");
		let params = format!("{:?}", bound.describe_parameters(&catalog));
		assert!(params.contains("Range(Int4)"), "{sql}: {params}");
		// The bound text is read as a value of the range type before the range function sees it.
		let plan = format!("{:?}", lower_bound_select(&bound, &policy).expect("lower failed"));
		assert!(plan.contains("sql::range_"), "{sql}: {plan}");
		assert!(
			plan.contains(
				"Normal(\"sql::fit\"), args: [Param(Param(\"_1\")), Literal(String(\"int4range\"))"
			),
			"{sql}: {plan}"
		);
	}
}

#[test]
fn a_blank_padded_cast_to_text_drops_its_padding() {
	let mut catalog = MemoryCatalog::new();
	catalog.define_table(None, "t", &["id", "v"]);
	catalog.add_column_type("t", "v", SqlType::Varchar);
	for sql in
		["SELECT CAST(CAST(v AS character(4)) AS text) FROM t;", "SELECT v::bpchar::text FROM t;"]
	{
		let plan = format!("{:?}", lower_pgsql_select(&catalog, sql).expect("lower failed"));
		assert!(plan.contains("sql::text"), "{sql}: {plan}");
		assert!(plan.contains("String(\"bpchar\")"), "{sql}: {plan}");
	}
	// A literal is folded before lowering; its padding is dropped as the cast to text is folded.
	let policy = DialectPolicy::postgres();
	for sql in ["SELECT 'ab  '::bpchar::text;", "SELECT 'ab  '::char(4)::varchar;"] {
		let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
		let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
			panic!("expected SELECT");
		};
		let mut bound =
			binder::pgsql::bind_select(select, &catalog, &policy, None).expect("bind failed");
		crate::semantics::fold_select(&mut bound, &policy).expect("fold failed");
		let folded = format!("{:?}", bound.targets[0].expr);
		assert!(folded.contains("String(\"ab\")"), "{sql}: {folded}");
	}
}

#[test]
fn lower_and_upper_of_a_computed_range_read_its_bounds() {
	for sql in [
		"SELECT lower(COALESCE(r, '[1,2)'::int4range)) FROM t;",
		"SELECT upper(CASE WHEN id = 1 THEN r ELSE NULL END) FROM t;",
	] {
		let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
		let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
			panic!("expected SELECT");
		};
		let mut catalog = MemoryCatalog::new();
		catalog.define_table(None, "t", &["id", "r"]);
		catalog.add_column_type("t", "r", SqlType::Range(crate::descriptor::RangeElement::Int4));
		let policy = DialectPolicy::postgres();
		let bound =
			binder::pgsql::bind_select(select, &catalog, &policy, None).expect("bind failed");
		let plan = format!("{:?}", lower_bound_select(&bound, &policy).expect("lower failed"));
		assert!(plan.contains("sql::range_"), "{sql}: {plan}");
		assert!(!plan.contains("string::"), "{sql}: {plan}");
	}
}

#[test]
fn overlap_between_operands_that_are_not_ranges_is_not_a_range_overlap() {
	let sql = "SELECT id FROM t WHERE a && b;";
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let mut catalog = MemoryCatalog::new();
	catalog.define_table(None, "t", &["id", "a", "b"]);
	let policy = DialectPolicy::postgres();
	let error = binder::pgsql::bind_select(select, &catalog, &policy, None)
		.expect_err("&& between non-ranges is not supported");
	assert!(matches!(error, PlanError::UnsupportedExpression(_)), "{error:?}");
}

#[test]
fn a_cidr_literal_with_host_bits_is_refused() {
	use surrealdb_expr::expr::statements::define::SqlTypeDialect;
	use surrealdb_expr::expr::statements::define::declared_type::DeclaredSqlType;

	let sql = "SELECT id FROM net WHERE c = '10.0.0.1/8';";
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let mut catalog = MemoryCatalog::new();
	catalog.define_table(None, "net", &["id", "c"]);
	catalog.add_sql_type(
		"net",
		"c",
		DeclaredSqlType::parse(SqlTypeDialect::PostgreSql, "cidr").expect("cidr parses"),
	);
	let policy = DialectPolicy::postgres();
	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).expect("bind failed");
	let error = lower_bound_select(&bound, &policy).expect_err("host bits are refused");
	assert!(matches!(error, PlanError::InvalidTextRepresentation(_)), "{error:?}");
}

#[test]
fn range_operators_between_different_range_types_are_not_supported() {
	for sql in [
		"SELECT id FROM t WHERE r && d;",
		"SELECT id FROM t WHERE r @> d;",
		"SELECT id FROM t WHERE r <@ d;",
	] {
		let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
		let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
			panic!("expected SELECT");
		};
		let mut catalog = MemoryCatalog::new();
		catalog.define_table(None, "t", &["id", "r", "d"]);
		catalog.add_column_type("t", "r", SqlType::Range(crate::descriptor::RangeElement::Int4));
		catalog.add_column_type("t", "d", SqlType::Range(crate::descriptor::RangeElement::Date));
		let policy = DialectPolicy::postgres();
		let refused = binder::pgsql::bind_select(select, &catalog, &policy, None)
			.and_then(|bound| lower_bound_select(&bound, &policy).map(|_| ()));
		assert!(matches!(refused, Err(PlanError::UnsupportedExpression(_))), "{sql}: {refused:?}");
	}
}

#[test]
fn a_network_valued_coalesce_orders_by_network_key() {
	let sql = "SELECT id FROM host ORDER BY COALESCE(addr, '0.0.0.0'::inet);";
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let mut catalog = MemoryCatalog::new();
	catalog.define_table(None, "host", &["id", "addr"]);
	catalog.add_column_type("host", "addr", SqlType::Inet);
	let policy = DialectPolicy::postgres();
	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).expect("bind failed");
	let plan = format!("{:?}", lower_bound_select(&bound, &policy).expect("lower failed"));
	assert!(plan.contains("sql::network_key"), "{plan}");
}

#[test]
fn a_network_in_list_reads_its_literals_as_networks() {
	let sql = "SELECT id FROM host WHERE addr IN ('10.0.0.2/32', $1);";
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let mut catalog = MemoryCatalog::new();
	catalog.define_table(None, "host", &["id", "addr"]);
	catalog.add_column_type("host", "addr", SqlType::Inet);
	let policy = DialectPolicy::postgres();
	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).expect("bind failed");
	let plan = format!("{:?}", lower_bound_select(&bound, &policy).expect("lower failed"));
	assert!(plan.contains("String(\"10.0.0.2\")"), "{plan}");
	assert!(plan.contains("sql::network_text"), "{plan}");

	let sql = "SELECT id FROM host WHERE addr IN ('not an address');";
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).expect("bind failed");
	let error = lower_bound_select(&bound, &policy).expect_err("the literal is refused");
	assert!(matches!(error, PlanError::InvalidTextRepresentation(_)), "{error:?}");
}

#[test]
fn a_cidr_ordered_against_a_literal_with_host_bits_is_refused() {
	let sql = "SELECT id FROM net WHERE c < '10.1.2.3/24';";
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let mut catalog = MemoryCatalog::new();
	catalog.define_table(None, "net", &["id", "c"]);
	catalog.add_column_type("net", "c", SqlType::Cidr);
	let policy = DialectPolicy::postgres();
	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).expect("bind failed");
	let error = lower_bound_select(&bound, &policy).expect_err("host bits are refused");
	assert!(matches!(error, PlanError::InvalidTextRepresentation(_)), "{error:?}");
}

#[test]
fn null_opposite_a_range_is_read_as_that_range() {
	let sql = "SELECT id FROM t WHERE r && NULL;";
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let mut catalog = MemoryCatalog::new();
	catalog.define_table(None, "t", &["id", "r"]);
	catalog.add_column_type("t", "r", SqlType::Range(crate::descriptor::RangeElement::Int4));
	let policy = DialectPolicy::postgres();
	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).expect("bind failed");
	let plan = format!("{:?}", lower_bound_select(&bound, &policy).expect("lower failed"));
	assert!(plan.contains("sql::range_overlaps"), "{plan}");
}

#[test]
fn cidr_bounds_and_mixed_in_list_items_are_read_as_cidr() {
	for sql in [
		"SELECT id FROM net WHERE c BETWEEN '10.1.2.3/24' AND '10.1.2.255/24';",
		"SELECT id FROM net WHERE c IN (d, '10.1.2.3/24');",
	] {
		let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
		let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
			panic!("expected SELECT");
		};
		let mut catalog = MemoryCatalog::new();
		catalog.define_table(None, "net", &["id", "c", "d"]);
		catalog.add_column_type("net", "c", SqlType::Cidr);
		catalog.add_column_type("net", "d", SqlType::Cidr);
		let policy = DialectPolicy::postgres();
		let bound =
			binder::pgsql::bind_select(select, &catalog, &policy, None).expect("bind failed");
		let error = lower_bound_select(&bound, &policy).expect_err("host bits are refused");
		assert!(matches!(error, PlanError::InvalidTextRepresentation(_)), "{sql}: {error:?}");
	}
}

#[test]
fn an_untyped_in_subject_takes_the_network_type_of_an_item() {
	let sql = "SELECT id FROM host WHERE '10.0.0.2/32' IN (addr);";
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let mut catalog = MemoryCatalog::new();
	catalog.define_table(None, "host", &["id", "addr"]);
	catalog.add_column_type("host", "addr", SqlType::Inet);
	let policy = DialectPolicy::postgres();
	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).expect("bind failed");
	let plan = format!("{:?}", lower_bound_select(&bound, &policy).expect("lower failed"));
	assert!(plan.contains("String(\"10.0.0.2\")"), "{plan}");
	assert!(plan.contains("sql::network_key"), "{plan}");
}

#[test]
fn folded_numeric_arithmetic_beyond_a_numeric_overflows() {
	let digits = BoundLiteral::Numeric("9".repeat(70_000));
	let policy = DialectPolicy::postgres();
	let product =
		crate::semantics::eval_binary_op(&digits, BoundBinaryOp::Multiply, &digits, &policy);
	assert!(matches!(product, Err(PlanError::NumericValueOutOfRange)), "{:?}", product.is_ok());
	let sum = crate::semantics::eval_binary_op(&digits, BoundBinaryOp::Add, &digits, &policy);
	assert!(sum.is_ok());
}

#[test]
fn a_mixed_cidr_and_inet_coalesce_is_an_inet() {
	let sql = "SELECT id FROM net WHERE COALESCE(c, a) = '10.1.2.3/24';";
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let mut catalog = MemoryCatalog::new();
	catalog.define_table(None, "net", &["id", "c", "a"]);
	catalog.add_column_type("net", "c", SqlType::Cidr);
	catalog.add_column_type("net", "a", SqlType::Inet);
	let policy = DialectPolicy::postgres();
	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).expect("bind failed");
	let plan = format!("{:?}", lower_bound_select(&bound, &policy).expect("an inet literal"));
	assert!(plan.contains("String(\"10.1.2.3/24\")"), "{plan}");
}

#[test]
fn parameters_in_a_network_in_list_or_between_are_described_as_networks() {
	for sql in [
		"SELECT id FROM host WHERE addr IN ($1, $2);",
		"SELECT id FROM host WHERE addr BETWEEN $1 AND $2;",
	] {
		let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
		let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
			panic!("expected SELECT");
		};
		let mut catalog = MemoryCatalog::new();
		catalog.define_table(None, "host", &["id", "addr"]);
		catalog.add_column_type("host", "addr", SqlType::Inet);
		let policy = DialectPolicy::postgres();
		let bound =
			binder::pgsql::bind_select(select, &catalog, &policy, None).expect("bind failed");
		let params = format!("{:?}", bound.describe_parameters(&catalog));
		assert_eq!(params.matches("Inet").count(), 2, "{sql}: {params}");
	}
}

#[test]
fn a_mixed_cidr_and_inet_in_list_reads_its_literals_as_inet() {
	let sql = "SELECT id FROM net WHERE c IN (a, '10.1.2.3/24');";
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let mut catalog = MemoryCatalog::new();
	catalog.define_table(None, "net", &["id", "c", "a"]);
	catalog.add_column_type("net", "c", SqlType::Cidr);
	catalog.add_column_type("net", "a", SqlType::Inet);
	let policy = DialectPolicy::postgres();
	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).expect("bind failed");
	let plan = format!("{:?}", lower_bound_select(&bound, &policy).expect("an inet literal"));
	assert!(plan.contains("String(\"10.1.2.3/24\")"), "{plan}");
}

#[test]
fn an_untyped_literal_does_not_type_a_peer_parameter() {
	let sql = "SELECT $1 IN ('01', 2::int4) AS hit;";
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let catalog = MemoryCatalog::new();
	let policy = DialectPolicy::postgres();
	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).expect("bind failed");
	let params = format!("{:?}", bound.describe_parameters(&catalog));
	assert!(params.contains("Int4"), "{params}");
}

#[test]
fn a_column_of_a_domain_over_inet_orders_as_a_network() {
	use surrealdb_expr::expr::statements::define::SqlTypeDialect;
	use surrealdb_expr::expr::statements::define::declared_type::DeclaredSqlType;

	let sql = "SELECT id FROM host ORDER BY addr;";
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let mut catalog = MemoryCatalog::new();
	catalog.define_table(None, "host", &["id", "addr"]);
	catalog.add_sql_type(
		"host",
		"addr",
		DeclaredSqlType::parse(SqlTypeDialect::PostgreSql, "address_t").expect("a domain name"),
	);
	catalog.add_column_type("host", "addr", SqlType::Inet);
	let policy = DialectPolicy::postgres();
	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).expect("bind failed");
	let plan = format!("{:?}", lower_bound_select(&bound, &policy).expect("lower failed"));
	assert!(plan.contains("sql::network_key"), "{plan}");
}

#[test]
fn an_untyped_alternative_of_a_range_coalesce_is_read_as_that_range() {
	let sql = "SELECT lower(COALESCE(r, '[5,9)')) FROM t;";
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let mut catalog = MemoryCatalog::new();
	catalog.define_table(None, "t", &["id", "r"]);
	catalog.add_column_type("t", "r", SqlType::Range(crate::descriptor::RangeElement::Int4));
	let policy = DialectPolicy::postgres();
	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).expect("bind failed");
	let plan = format!("{:?}", lower_bound_select(&bound, &policy).expect("lower failed"));
	assert!(plan.contains("sql::range_lower"), "{plan}");
	assert!(plan.contains("op: Range"), "{plan}");
}

#[test]
fn peer_parameters_take_the_common_type_of_their_typed_peers() {
	let sql = "SELECT $1 IN (1::int4, 2147483648::int8) AS hit;";
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let catalog = MemoryCatalog::new();
	let policy = DialectPolicy::postgres();
	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).expect("bind failed");
	let params = format!("{:?}", bound.describe_parameters(&catalog));
	assert!(params.contains("Int8") && !params.contains("Int4"), "{params}");
}

#[test]
fn a_coalesce_of_different_range_types_is_not_a_range() {
	let sql = "SELECT lower(COALESCE(r, d)) FROM t;";
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let mut catalog = MemoryCatalog::new();
	catalog.define_table(None, "t", &["id", "r", "d"]);
	catalog.add_column_type("t", "r", SqlType::Range(crate::descriptor::RangeElement::Int4));
	catalog.add_column_type("t", "d", SqlType::Range(crate::descriptor::RangeElement::Date));
	let policy = DialectPolicy::postgres();
	let plan = binder::pgsql::bind_select(select, &catalog, &policy, None)
		.and_then(|bound| lower_bound_select(&bound, &policy))
		.map(|plan| format!("{plan:?}"));
	assert!(!plan.as_deref().is_ok_and(|plan| plan.contains("sql::range_lower")), "{plan:?}");
}

#[test]
fn a_domain_column_casts_to_text_by_its_base_type() {
	use surrealdb_expr::expr::statements::define::SqlTypeDialect;
	use surrealdb_expr::expr::statements::define::declared_type::DeclaredSqlType;

	let sql = "SELECT addr::text AS t FROM host;";
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let mut catalog = MemoryCatalog::new();
	catalog.define_table(None, "host", &["id", "addr"]);
	catalog.add_sql_type(
		"host",
		"addr",
		DeclaredSqlType::parse(SqlTypeDialect::PostgreSql, "address_t").expect("a domain name"),
	);
	catalog.add_column_type("host", "addr", SqlType::Inet);
	let policy = DialectPolicy::postgres();
	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).expect("bind failed");
	let plan = format!("{:?}", lower_bound_select(&bound, &policy).expect("lower failed"));
	assert!(plan.contains("sql::text"), "{plan}");
	assert!(plan.contains("String(\"inet\")"), "{plan}");
}

#[test]
fn a_range_coalesce_with_a_typed_non_range_result_is_not_a_range() {
	let sql = "SELECT lower(COALESCE(r, 5)) FROM t;";
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let mut catalog = MemoryCatalog::new();
	catalog.define_table(None, "t", &["id", "r"]);
	catalog.add_column_type("t", "r", SqlType::Range(crate::descriptor::RangeElement::Int4));
	let policy = DialectPolicy::postgres();
	let plan = binder::pgsql::bind_select(select, &catalog, &policy, None)
		.and_then(|bound| lower_bound_select(&bound, &policy))
		.map(|plan| format!("{plan:?}"));
	assert!(!plan.as_deref().is_ok_and(|plan| plan.contains("sql::range_lower")), "{plan:?}");
}

#[test]
fn an_untyped_alternative_of_a_network_coalesce_is_read_as_that_network() {
	let sql = "SELECT id FROM host WHERE COALESCE(addr, '10.0.0.2/32') = '10.0.0.2';";
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let mut catalog = MemoryCatalog::new();
	catalog.define_table(None, "host", &["id", "addr"]);
	catalog.add_column_type("host", "addr", SqlType::Inet);
	let policy = DialectPolicy::postgres();
	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).expect("bind failed");
	let plan = format!("{:?}", lower_bound_select(&bound, &policy).expect("lower failed"));
	assert!(!plan.contains("10.0.0.2/32"), "{plan}");
}

#[test]
fn a_network_coalesce_with_a_typed_non_network_result_is_not_a_network() {
	let sql = "SELECT id FROM host ORDER BY COALESCE(addr, 5);";
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let mut catalog = MemoryCatalog::new();
	catalog.define_table(None, "host", &["id", "addr"]);
	catalog.add_column_type("host", "addr", SqlType::Inet);
	let policy = DialectPolicy::postgres();
	let plan = binder::pgsql::bind_select(select, &catalog, &policy, None)
		.and_then(|bound| lower_bound_select(&bound, &policy))
		.map(|plan| format!("{plan:?}"));
	assert!(!plan.as_deref().is_ok_and(|plan| plan.contains("sql::network_key")), "{plan:?}");
}

#[test]
fn a_compound_network_in_subject_is_compared_by_key() {
	let sql = "SELECT id FROM host WHERE COALESCE(addr, '10.0.0.2/32') IN ('10.0.0.2');";
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let mut catalog = MemoryCatalog::new();
	catalog.define_table(None, "host", &["id", "addr"]);
	catalog.add_column_type("host", "addr", SqlType::Inet);
	let policy = DialectPolicy::postgres();
	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).expect("bind failed");
	let plan = format!("{:?}", lower_bound_select(&bound, &policy).expect("lower failed"));
	assert!(plan.contains("sql::network_key"), "{plan}");
}

#[test]
fn a_parameter_among_cidr_and_inet_peers_is_an_inet() {
	let sql = "SELECT id FROM net WHERE $1 IN (c, a);";
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let mut catalog = MemoryCatalog::new();
	catalog.define_table(None, "net", &["id", "c", "a"]);
	catalog.add_column_type("net", "c", SqlType::Cidr);
	catalog.add_column_type("net", "a", SqlType::Inet);
	let policy = DialectPolicy::postgres();
	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).expect("bind failed");
	let params = format!("{:?}", bound.describe_parameters(&catalog));
	assert!(params.contains("Inet"), "{params}");
}

#[test]
fn a_numeric_beyond_a_float_casts_out_of_range() {
	let policy = DialectPolicy::postgres();
	let wide = BoundLiteral::Numeric("9".repeat(400));
	assert!(matches!(
		crate::semantics::eval_cast(&wide, "float8", &policy),
		Err(PlanError::NumericValueOutOfRange)
	));
	let beyond_real = BoundLiteral::Numeric(format!("1{}", "0".repeat(40)));
	assert!(matches!(
		crate::semantics::eval_cast(&beyond_real, "real", &policy),
		Err(PlanError::NumericValueOutOfRange)
	));
	assert!(crate::semantics::eval_cast(&beyond_real, "float8", &policy).is_ok());
}

#[test]
fn a_numeric_beyond_a_double_beside_a_float_is_out_of_range() {
	let policy = DialectPolicy::postgres();
	let wide = BoundLiteral::Numeric("1e400".to_owned());
	let one = BoundLiteral::Float(1.0);
	for op in [BoundBinaryOp::Add, BoundBinaryOp::Equal] {
		assert!(
			matches!(
				crate::semantics::eval_binary_op(&wide, op, &one, &policy),
				Err(PlanError::NumericValueOutOfRange)
			),
			"{op:?}"
		);
	}
}

#[test]
fn numeric_underflow_and_hidden_float_comparisons_are_out_of_range() {
	let policy = DialectPolicy::postgres();
	let tiny = BoundLiteral::Numeric("1e-400".to_owned());
	assert!(matches!(
		crate::semantics::eval_cast(&tiny, "float8", &policy),
		Err(PlanError::NumericValueOutOfRange)
	));
	assert!(matches!(
		crate::semantics::eval_cast(&BoundLiteral::Numeric("1e-50".to_owned()), "real", &policy),
		Err(PlanError::NumericValueOutOfRange)
	));
	assert!(
		crate::semantics::eval_cast(&BoundLiteral::Numeric("0.0".to_owned()), "real", &policy)
			.is_ok()
	);
	let wide = BoundLiteral::Numeric("1e400".to_owned());
	let zero = BoundLiteral::Float(0.0);
	assert!(matches!(
		crate::semantics::eval_binary_op(&wide, BoundBinaryOp::DistinctFrom, &zero, &policy),
		Err(PlanError::NumericValueOutOfRange)
	));
}

#[test]
fn an_order_by_cidr_compound_refuses_an_untyped_alternative_with_host_bits() {
	let sql = "SELECT id FROM net ORDER BY COALESCE(c, '10.1.2.3/24');";
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
	let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
		panic!("expected SELECT");
	};
	let mut catalog = MemoryCatalog::new();
	catalog.define_table(None, "net", &["id", "c"]);
	catalog.add_column_type("net", "c", SqlType::Cidr);
	let policy = DialectPolicy::postgres();
	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).expect("bind failed");
	let error = lower_bound_select(&bound, &policy).expect_err("host bits are refused");
	assert!(matches!(error, PlanError::InvalidTextRepresentation(_)), "{error:?}");
}

#[test]
fn a_network_compared_with_a_declared_text_column_is_undefined() {
	use surrealdb_expr::expr::statements::define::SqlTypeDialect;
	use surrealdb_expr::expr::statements::define::declared_type::DeclaredSqlType;

	let mut catalog = MemoryCatalog::new();
	catalog.define_table(None, "host", &["id", "addr", "note", "loose"]);
	catalog.add_column_type("host", "addr", SqlType::Inet);
	catalog.add_sql_type(
		"host",
		"note",
		DeclaredSqlType::parse(SqlTypeDialect::PostgreSql, "text").expect("text parses"),
	);
	let policy = DialectPolicy::postgres();
	for (sql, refused) in [
		("SELECT id FROM host WHERE addr = note;", true),
		("SELECT id FROM host WHERE addr < 5;", true),
		("SELECT id FROM host WHERE addr = loose;", false),
		("SELECT id FROM host WHERE addr = '10.0.0.1';", false),
	] {
		let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
		let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
			panic!("expected SELECT");
		};
		let bound =
			binder::pgsql::bind_select(select, &catalog, &policy, None).expect("bind failed");
		let lowered = lower_bound_select(&bound, &policy);
		assert_eq!(
			matches!(lowered, Err(PlanError::UndefinedOperator(_))),
			refused,
			"{sql}: {lowered:?}"
		);
	}
}

#[test]
fn subnormal_float_casts_are_held_and_typed_peers_of_a_network_predicate_are_refused() {
	let policy = DialectPolicy::postgres();
	for (text, ty) in [("1e-310", "float8"), ("1e-40", "real")] {
		assert!(
			crate::semantics::eval_cast(&BoundLiteral::Numeric(text.to_owned()), ty, &policy)
				.is_ok(),
			"{text}::{ty}"
		);
	}
	let mut catalog = MemoryCatalog::new();
	catalog.define_table(None, "host", &["id", "addr"]);
	catalog.add_column_type("host", "addr", SqlType::Inet);
	for sql in [
		"SELECT id FROM host WHERE addr IN (1);",
		"SELECT id FROM host WHERE addr BETWEEN 1 AND 2;",
	] {
		let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
		let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
			panic!("expected SELECT");
		};
		let bound =
			binder::pgsql::bind_select(select, &catalog, &policy, None).expect("bind failed");
		let lowered = lower_bound_select(&bound, &policy);
		assert!(matches!(lowered, Err(PlanError::UndefinedOperator(_))), "{sql}: {lowered:?}");
	}
}

#[test]
fn a_between_parameter_is_typed_by_its_first_comparison() {
	for (sql, expected) in [
		("SELECT $1 BETWEEN 1::int4 AND 2147483648::int8 AS hit;", "[Int4]"),
		("SELECT $1 BETWEEN 1::int4 AND $2 AS hit;", "[Int4, Int4]"),
	] {
		let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
		let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
			panic!("expected SELECT");
		};
		let catalog = MemoryCatalog::new();
		let policy = DialectPolicy::postgres();
		let bound =
			binder::pgsql::bind_select(select, &catalog, &policy, None).expect("bind failed");
		let params = format!("{:?}", bound.describe_parameters(&catalog).param_types);
		assert_eq!(params, expected, "{sql}");
	}
}

#[test]
fn a_numeric_divided_by_a_float_is_converted_to_a_double_first() {
	for sql in ["SELECT 1e400 / 1::float8", "SELECT 1::float8 / 1e400", "SELECT 1e400 ^ 1::float8"]
	{
		assert_eq!(folded_pgsql(sql), Err(PlanError::NumericValueOutOfRange), "{sql}");
	}
	assert_eq!(folded_pgsql("SELECT 3.0 / 2::float8"), Ok(BoundLiteral::Float(1.5)));
}

#[test]
fn an_exact_quotient_past_the_numeric_limit_is_out_of_range() {
	let fold = |sql: &str| {
		let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
		let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
			panic!("expected SELECT");
		};
		let policy = DialectPolicy::postgres();
		let mut bound =
			binder::pgsql::bind_select(select, &MemoryCatalog::new(), &policy, None).unwrap();
		fold_select(&mut bound, &policy)
	};
	for sql in ["SELECT 9e131071 / 0.1", "SELECT 1e131071 / 0.05", "SELECT 9e131071 / 0.5"] {
		assert_eq!(fold(sql).err(), Some(PlanError::NumericValueOutOfRange), "{sql}");
	}
	for sql in ["SELECT 9e131070 / 0.1", "SELECT 2e131071 / 0.25", "SELECT 1.5 / 3"] {
		assert!(fold(sql).is_ok(), "{sql}");
	}
}

#[test]
fn mysql_div_over_a_decimal_truncates_exactly() {
	for (sql, expected) in [
		("SELECT 18446744073709551615 DIV 1", BoundLiteral::Unsigned(u64::MAX)),
		("SELECT 2.9999999999999999999999999999999 DIV 1", BoundLiteral::Integer(2)),
		("SELECT 9223372036854775807.9 DIV 1", BoundLiteral::Integer(i64::MAX)),
		("SELECT -7.5 DIV 2", BoundLiteral::Integer(-3)),
		("SELECT 7.5 DIV -2.5", BoundLiteral::Integer(-3)),
	] {
		assert_eq!(folded_mysql(sql), Ok(expected), "{sql}");
	}
	assert_eq!(
		folded_mysql("SELECT 9223372036854775808.0 DIV 1"),
		Err(PlanError::NumericValueOutOfRange)
	);
}

#[test]
fn mysql_exact_arithmetic_is_bounded_by_65_digits() {
	let wide = format!("{}.0", "9".repeat(40));
	let product = format!("SELECT {wide} * {wide}");
	assert_eq!(folded_mysql(&product), Err(PlanError::NumericValueOutOfRange));
	assert!(folded_pgsql(&product).is_ok(), "PostgreSQL's numeric holds the product");
}

#[test]
fn a_numeric_power_past_the_numeric_integer_digits_is_out_of_range() {
	for sql in [
		"SELECT 9e1000 ^ 132",
		"SELECT 0.1 ^ -131073.0",
		"SELECT 10.0 ^ 131072",
		"SELECT 0.99999999999999999 ^ -1e30",
		"SELECT 1.000000000000000001 ^ 1e35",
	] {
		let err = folded_pgsql(sql).expect_err(sql);
		assert_eq!(err, PlanError::NumericValueOutOfRange, "{sql}");
	}
	// A power in range is left for the engine, unfolded.
	for sql in [
		"SELECT 9e1000 ^ 2",
		"SELECT 10.0 ^ 131071",
		"SELECT 1.0 ^ 1e30",
		"SELECT 2.5 ^ -3",
		"SELECT 1.000000000000000001 ^ 1000",
	] {
		let stmts = surrealdb_pgsql::parse_statements(sql).expect(sql);
		let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
			panic!("expected SELECT");
		};
		let policy = DialectPolicy::postgres();
		let mut bound =
			binder::pgsql::bind_select(select, &MemoryCatalog::new(), &policy, None).expect(sql);
		fold_select(&mut bound, &policy).expect(sql);
	}
}
