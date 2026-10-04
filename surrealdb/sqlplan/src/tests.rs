//! Unit tests for surrealdb-sqlplan binder and lowering.

use surrealdb_expr::expr::logical::{LogicalExpr, LogicalOp};

use crate::catalog::MemoryCatalog;
use crate::dialect::DialectPolicy;
use crate::ir::*;
use crate::*;

#[test]
fn test_single_table_alias_binding() {
	let sql = "SELECT u.id, u.name FROM users AS u;";
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};

	let mut catalog = MemoryCatalog::new();
	catalog.define_table(None, "users", &["id", "name", "email"]);
	let policy = DialectPolicy::postgres();

	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).expect("bind failed");
	assert_eq!(bound.targets.len(), 2);

	// First target: u.id -> relation 0, column "id"
	match &bound.targets[0].expr {
		BoundExpr::Column(col) => {
			assert_eq!(col.relation_index, 0);
			assert_eq!(col.column, "id");
		}
		other => panic!("expected BoundExpr::Column, got {other:?}"),
	}

	// Lower to LogicalOp
	let op = lower_bound_select(&bound, &policy).expect("lower failed");
	match op {
		LogicalOp::Project(proj) => {
			let surrealdb_expr::expr::logical::LogicalFields::Select(fields) = &proj.fields else {
				panic!("expected Select fields");
			};
			assert_eq!(fields.len(), 2);
			// Under relation 0, lowering emits direct Field("id"), not Access(Field("u"), "id")
			let surrealdb_expr::expr::logical::LogicalField::Single {
				expr: expr0,
				..
			} = &fields[0]
			else {
				panic!("expected Single field");
			};
			assert_eq!(expr0, &LogicalExpr::Field("id".into()));

			let surrealdb_expr::expr::logical::LogicalField::Single {
				expr: expr1,
				..
			} = &fields[1]
			else {
				panic!("expected Single field");
			};
			assert_eq!(expr1, &LogicalExpr::Field("name".into()));
		}
		other => panic!("expected LogicalOp::Project, got {other:?}"),
	}
}

#[test]
fn test_join_table_alias_binding() {
	let sql = "SELECT u.id, o.total FROM users u JOIN orders o ON u.id = o.user_id;";
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};

	let mut catalog = MemoryCatalog::new();
	catalog.define_table(None, "users", &["id", "name"]);
	catalog.define_table(None, "orders", &["id", "user_id", "total"]);
	let policy = DialectPolicy::postgres();

	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).expect("bind failed");

	// Verify target 0 (u.id) binds to relation 0
	match &bound.targets[0].expr {
		BoundExpr::Column(col) => {
			assert_eq!(col.relation_index, 0);
			assert_eq!(col.column, "id");
		}
		other => panic!("expected BoundExpr::Column, got {other:?}"),
	}

	// Verify target 1 (o.total) binds to relation 1
	match &bound.targets[1].expr {
		BoundExpr::Column(col) => {
			assert_eq!(col.relation_index, 1);
			assert_eq!(col.column, "total");
		}
		other => panic!("expected BoundExpr::Column, got {other:?}"),
	}

	// Lower to LogicalOp
	let op = lower_bound_select(&bound, &policy).expect("lower failed");
	match op {
		LogicalOp::Project(proj) => {
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
			assert_eq!(expr0, &LogicalExpr::Field("id".into()));

			let surrealdb_expr::expr::logical::LogicalField::Single {
				expr: expr1,
				..
			} = &fields[1]
			else {
				panic!("expected Single field");
			};
			match expr1 {
				LogicalExpr::Access {
					base,
					field,
				} => {
					assert_eq!(base.as_ref(), &LogicalExpr::Field("_join0".into()));
					assert_eq!(field.as_str(), "total");
				}
				other => panic!("expected LogicalExpr::Access to _join0, got {other:?}"),
			}

			// Verify join condition
			match &*proj.input {
				LogicalOp::Join(join) => {
					assert_eq!(join.on.len(), 1);
					let (left, right) = &join.on[0];
					assert_eq!(left, &LogicalExpr::Field("id".into()));
					match right {
						LogicalExpr::Access {
							base,
							field,
						} => {
							assert_eq!(base.as_ref(), &LogicalExpr::Field("_join0".into()));
							assert_eq!(field.as_str(), "user_id");
						}
						other => panic!("expected LogicalExpr::Access to _join0, got {other:?}"),
					}
				}
				other => panic!("expected LogicalOp::Join, got {other:?}"),
			}
		}
		other => panic!("expected LogicalOp::Project, got {other:?}"),
	}
}

#[test]
fn test_positional_group_by_and_order_by() {
	let sql = "SELECT category, count(id) FROM products GROUP BY 1 ORDER BY 2 DESC;";
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};

	let catalog = MemoryCatalog::new();
	let policy = DialectPolicy::postgres();

	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).expect("bind failed");
	assert_eq!(bound.group_by.len(), 1);

	// GROUP BY 1 should resolve to category (target 1 expr)
	match &bound.group_by[0] {
		BoundExpr::Column(col) => {
			assert_eq!(col.column, "category");
		}
		other => panic!("expected BoundExpr::Column, got {other:?}"),
	}

	// ORDER BY 2 DESC should resolve to count(id) output column
	assert_eq!(bound.order_by.len(), 1);
	match &bound.order_by[0].expr {
		BoundExpr::Column(col) => {
			assert_eq!(col.column, "count");
		}
		other => panic!("expected count column, got {other:?}"),
	}
	assert_eq!(bound.order_by[0].direction, BoundOrderDirection::Desc);

	// Lowering should produce an Aggregate node rather than failing or creating array literal
	let op = lower_bound_select(&bound, &policy).expect("lower failed");
	match op {
		LogicalOp::Project(proj) => match &*proj.input {
			LogicalOp::Sort(sort) => match &*sort.input {
				LogicalOp::Aggregate(agg) => {
					assert_eq!(agg.group.len(), 1);
					assert_eq!(agg.group[0], LogicalExpr::Field("category".into()));
				}
				other => panic!("expected LogicalOp::Aggregate inside sort, got {other:?}"),
			},
			other => panic!("expected LogicalOp::Sort inside project, got {other:?}"),
		},
		other => panic!("expected LogicalOp::Project, got {other:?}"),
	}
}

#[test]
fn test_order_by_target_alias() {
	let sql = "SELECT id AS user_id FROM users ORDER BY user_id DESC;";
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};

	let catalog = MemoryCatalog::new();
	let policy = DialectPolicy::postgres();

	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).expect("bind failed");
	assert_eq!(bound.order_by.len(), 1);
	match &bound.order_by[0].expr {
		BoundExpr::Column(col) => {
			assert_eq!(col.column, "user_id");
		}
		other => panic!("expected user_id column, got {other:?}"),
	}
}

#[test]
fn test_schema_qualified_table_and_column() {
	let sql = "SELECT public.users.id FROM public.users;";
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};

	let catalog = MemoryCatalog::new();
	let policy = DialectPolicy::postgres();

	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).expect("bind failed");
	match &bound.targets[0].expr {
		BoundExpr::Column(col) => {
			assert_eq!(col.relation_index, 0);
			assert_eq!(col.column, "id");
		}
		other => panic!("expected BoundExpr::Column, got {other:?}"),
	}
}

#[test]
fn test_case_folding_pg() {
	let sql = "SELECT U.ID FROM USERS AS U;";
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};

	let catalog = MemoryCatalog::new();
	let policy = DialectPolicy::postgres();

	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).expect("bind failed");
	match &bound.targets[0].expr {
		BoundExpr::Column(col) => {
			assert_eq!(col.relation_index, 0);
			assert_eq!(col.column, "id");
		}
		other => panic!("expected BoundExpr::Column, got {other:?}"),
	}
}

#[test]
fn test_wildcard_expansion_with_catalog() {
	let sql = "SELECT * FROM users;";
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
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
	let select = match &stmts[0] {
		surrealdb_mysql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};

	let catalog = MemoryCatalog::new();
	let policy = DialectPolicy::mysql();

	let bound = binder::mysql::bind_select(select, &catalog, &policy, None).expect("bind failed");
	match &bound.where_clause {
		Some(BoundExpr::Binary {
			left,
			op,
			right,
		}) => {
			assert_eq!(*op, BoundBinaryOp::Equal);
			match &**left {
				BoundExpr::Column(col) => assert_eq!(col.column, "id"),
				other => panic!("expected column, got {other:?}"),
			}
			match &**right {
				BoundExpr::Param {
					index,
					name,
				} => {
					assert_eq!(*index, 1);
					assert_eq!(name, "_1");
				}
				other => panic!("expected param, got {other:?}"),
			}
		}
		other => panic!("expected binary where clause, got {other:?}"),
	}
}

#[test]
fn test_result_descriptor_basic_select() {
	let sql = "SELECT id, name, 42, count(id) AS total_count FROM users;";
	let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse failed");
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
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
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
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
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
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
fn test_3vl_constant_folding() {
	let catalog = MemoryCatalog::new();
	let policy = DialectPolicy::postgres();

	// false AND NULL -> false
	let sql = "SELECT false AND NULL;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let mut bound = binder::pgsql::bind_select(select, &catalog, &policy, None).unwrap();
	fold_select(&mut bound, &policy).unwrap();
	assert_eq!(bound.targets[0].expr, BoundExpr::Literal(BoundLiteral::Boolean(false)));

	// true AND NULL -> NULL
	let sql = "SELECT true AND NULL;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let mut bound = binder::pgsql::bind_select(select, &catalog, &policy, None).unwrap();
	fold_select(&mut bound, &policy).unwrap();
	assert_eq!(bound.targets[0].expr, BoundExpr::Literal(BoundLiteral::Null));

	// true OR NULL -> true
	let sql = "SELECT true OR NULL;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let mut bound = binder::pgsql::bind_select(select, &catalog, &policy, None).unwrap();
	fold_select(&mut bound, &policy).unwrap();
	assert_eq!(bound.targets[0].expr, BoundExpr::Literal(BoundLiteral::Boolean(true)));

	// false OR NULL -> NULL
	let sql = "SELECT false OR NULL;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let mut bound = binder::pgsql::bind_select(select, &catalog, &policy, None).unwrap();
	fold_select(&mut bound, &policy).unwrap();
	assert_eq!(bound.targets[0].expr, BoundExpr::Literal(BoundLiteral::Null));

	// NOT NULL -> NULL
	let sql = "SELECT NOT NULL;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
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
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let mut bound = binder::pgsql::bind_select(select, &catalog, &policy, None).unwrap();
	fold_select(&mut bound, &policy).unwrap();
	assert_eq!(bound.targets[0].expr, BoundExpr::Literal(BoundLiteral::Null));

	// NULL IS NOT DISTINCT FROM NULL -> true
	let sql = "SELECT NULL IS NOT DISTINCT FROM NULL;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let mut bound = binder::pgsql::bind_select(select, &catalog, &policy, None).unwrap();
	fold_select(&mut bound, &policy).unwrap();
	assert_eq!(bound.targets[0].expr, BoundExpr::Literal(BoundLiteral::Boolean(true)));

	// 1 IS NOT DISTINCT FROM NULL -> false
	let sql = "SELECT 1 IS NOT DISTINCT FROM NULL;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let mut bound = binder::pgsql::bind_select(select, &catalog, &policy, None).unwrap();
	fold_select(&mut bound, &policy).unwrap();
	assert_eq!(bound.targets[0].expr, BoundExpr::Literal(BoundLiteral::Boolean(false)));

	// NULL IS DISTINCT FROM NULL -> false
	let sql = "SELECT NULL IS DISTINCT FROM NULL;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let mut bound = binder::pgsql::bind_select(select, &catalog, &policy, None).unwrap();
	fold_select(&mut bound, &policy).unwrap();
	assert_eq!(bound.targets[0].expr, BoundExpr::Literal(BoundLiteral::Boolean(false)));
}

#[test]
fn test_division_semantics() {
	let catalog = MemoryCatalog::new();
	let pg_policy = DialectPolicy::postgres();
	let my_policy = DialectPolicy::mysql();

	// PG: 1 / 0 -> DivisionByZero
	let sql = "SELECT 1 / 0;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let mut bound = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap();
	let err = fold_select(&mut bound, &pg_policy).unwrap_err();
	assert!(matches!(err, PlanError::DivisionByZero));

	// PG: 5 / 2 -> integer division 2
	let sql = "SELECT 5 / 2;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let mut bound = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap();
	fold_select(&mut bound, &pg_policy).unwrap();
	assert_eq!(bound.targets[0].expr, BoundExpr::Literal(BoundLiteral::Integer(2)));

	// MySQL: 1 / 0 -> NULL
	let sql = "SELECT 1 / 0;";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_mysql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let mut bound = binder::mysql::bind_select(select, &catalog, &my_policy, None).unwrap();
	fold_select(&mut bound, &my_policy).unwrap();
	assert_eq!(bound.targets[0].expr, BoundExpr::Literal(BoundLiteral::Null));

	// MySQL: 5 / 2 -> 2.5
	let sql = "SELECT 5 / 2;";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_mysql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let mut bound = binder::mysql::bind_select(select, &catalog, &my_policy, None).unwrap();
	fold_select(&mut bound, &my_policy).unwrap();
	assert_eq!(bound.targets[0].expr, BoundExpr::Literal(BoundLiteral::Float(2.5)));

	// MySQL: 5 DIV 2 -> 2
	let sql = "SELECT 5 DIV 2;";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_mysql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let mut bound = binder::mysql::bind_select(select, &catalog, &my_policy, None).unwrap();
	fold_select(&mut bound, &my_policy).unwrap();
	assert_eq!(bound.targets[0].expr, BoundExpr::Literal(BoundLiteral::Integer(2)));
}

#[test]
fn test_mysql_string_coercion() {
	let catalog = MemoryCatalog::new();
	let policy = DialectPolicy::mysql();

	// '100abc' + 20 -> 120
	let sql = "SELECT '100abc' + 20;";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_mysql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let mut bound = binder::mysql::bind_select(select, &catalog, &policy, None).unwrap();
	fold_select(&mut bound, &policy).unwrap();
	assert_eq!(bound.targets[0].expr, BoundExpr::Literal(BoundLiteral::Integer(120)));

	// '2.5' * 2 -> 5.0
	let sql = "SELECT '2.5' * 2;";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_mysql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let mut bound = binder::mysql::bind_select(select, &catalog, &policy, None).unwrap();
	fold_select(&mut bound, &policy).unwrap();
	assert_eq!(bound.targets[0].expr, BoundExpr::Literal(BoundLiteral::Float(5.0)));
}

#[test]
fn test_string_collation() {
	let catalog = MemoryCatalog::new();

	// MySQL: 'abc' = 'ABC' -> true
	let my_policy = DialectPolicy::mysql();
	let sql = "SELECT 'abc' = 'ABC';";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_mysql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let mut bound = binder::mysql::bind_select(select, &catalog, &my_policy, None).unwrap();
	fold_select(&mut bound, &my_policy).unwrap();
	assert_eq!(bound.targets[0].expr, BoundExpr::Literal(BoundLiteral::Boolean(true)));

	// PG: 'abc' = 'ABC' -> false
	let pg_policy = DialectPolicy::postgres();
	let sql = "SELECT 'abc' = 'ABC';";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
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
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let mut bound = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap();
	fold_select(&mut bound, &pg_policy).unwrap();
	assert_eq!(bound.targets[0].expr, BoundExpr::Literal(BoundLiteral::Boolean(true)));
	assert_eq!(bound.targets[1].expr, BoundExpr::Literal(BoundLiteral::Boolean(false)));

	// PG ILIKE (case insensitive): 'hello' ILIKE 'HEL%' -> true
	let sql = "SELECT 'hello' ILIKE 'HEL%';";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let mut bound = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap();
	fold_select(&mut bound, &pg_policy).unwrap();
	assert_eq!(bound.targets[0].expr, BoundExpr::Literal(BoundLiteral::Boolean(true)));

	// MySQL LIKE (case insensitive): 'hello' LIKE 'HEL%' -> true
	let sql = "SELECT 'hello' LIKE 'HEL%';";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_mysql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let mut bound = binder::mysql::bind_select(select, &catalog, &my_policy, None).unwrap();
	fold_select(&mut bound, &my_policy).unwrap();
	assert_eq!(bound.targets[0].expr, BoundExpr::Literal(BoundLiteral::Boolean(true)));
}

#[test]
fn test_type_casts() {
	let catalog = MemoryCatalog::new();
	let pg_policy = DialectPolicy::postgres();

	// PG: CAST('123' AS integer) -> 123
	let sql = "SELECT CAST('123' AS integer);";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let mut bound = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap();
	fold_select(&mut bound, &pg_policy).unwrap();
	assert_eq!(bound.targets[0].expr, BoundExpr::Literal(BoundLiteral::Integer(123)));

	// PG: CAST('abc' AS integer) -> 22P02 error
	let sql = "SELECT CAST('abc' AS integer);";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let mut bound = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap();
	let err = fold_select(&mut bound, &pg_policy).unwrap_err();
	assert!(matches!(err, PlanError::InvalidTextRepresentation(_)));
	assert_eq!(err.pg_sqlstate(), "22P02");
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
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let bound = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap();
	let op = lower_bound_select(&bound, &pg_policy).unwrap();
	match op {
		LogicalOp::Project(proj) => match &*proj.input {
			LogicalOp::Sort(sort) => {
				let surrealdb_expr::expr::logical::LogicalOrdering::Order(orders) = &sort.ordering
				else {
					panic!("expected Order");
				};
				assert_eq!(orders.len(), 2, "PG ASC should emit (score IS NULL) ASC and score ASC");
				assert!(orders[0].direction);
				assert!(orders[1].direction);
			}
			other => panic!("expected Sort, got {other:?}"),
		},
		other => panic!("expected Project, got {other:?}"),
	}

	// MySQL: ORDER BY score ASC (NULLS FIRST by default) -> emits 1 order: [score ASC]
	let my_policy = DialectPolicy::mysql();
	let sql = "SELECT score FROM users ORDER BY score ASC;";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_mysql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let bound = binder::mysql::bind_select(select, &catalog, &my_policy, None).unwrap();
	let op = lower_bound_select(&bound, &my_policy).unwrap();
	match op {
		LogicalOp::Project(proj) => match &*proj.input {
			LogicalOp::Sort(sort) => {
				let surrealdb_expr::expr::logical::LogicalOrdering::Order(orders) = &sort.ordering
				else {
					panic!("expected Order");
				};
				assert_eq!(orders.len(), 1, "MySQL ASC should emit single score ASC");
				assert!(orders[0].direction);
			}
			other => panic!("expected Sort, got {other:?}"),
		},
		other => panic!("expected Project, got {other:?}"),
	}
}
