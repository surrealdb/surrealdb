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
fn test_mysql_sql_mode_pipes_as_concat_and_introduced_strings() {
	let catalog = MemoryCatalog::new();
	let policy = DialectPolicy::mysql();

	// Parse with PIPES_AS_CONCAT
	let settings = surrealdb_mysql::ParserSettings::default()
		.with_sql_mode(surrealdb_mysql::SqlMode::empty().with_pipes_as_concat(true));
	let sql = "SELECT 'hello ' || 'world';";
	let stmts = surrealdb_mysql::parse_statements_with_settings(sql, settings).unwrap();
	let select = match &stmts[0] {
		surrealdb_mysql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
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
	let select = match &stmts[0] {
		surrealdb_mysql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
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
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let bound = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap();
	let op = lower_bound_select(&bound, &pg_policy).unwrap();
	assert!(matches!(op, LogicalOp::UnionAll(_)));

	// 2. UNION (distinct)
	let sql = "SELECT a FROM t1 UNION SELECT a FROM t2;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let bound = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap();
	let op = lower_bound_select(&bound, &pg_policy).unwrap();
	assert!(matches!(op, LogicalOp::Distinct(_)));

	// 3. INTERSECT
	let sql = "SELECT a FROM t1 INTERSECT SELECT a FROM t2;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let bound = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap();
	let op = lower_bound_select(&bound, &pg_policy).unwrap();
	match op {
		LogicalOp::Distinct(d) => match *d.input {
			LogicalOp::Join(j) => assert_eq!(j.kind, surrealdb_expr::expr::logical::JoinKind::Semi),
			other => panic!("expected Join under Distinct, got {other:?}"),
		},
		other => panic!("expected Distinct, got {other:?}"),
	}

	// 4. EXCEPT
	let sql = "SELECT a FROM t1 EXCEPT SELECT a FROM t2;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let bound = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap();
	let op = lower_bound_select(&bound, &pg_policy).unwrap();
	match op {
		LogicalOp::Distinct(d) => match *d.input {
			LogicalOp::Join(j) => assert_eq!(j.kind, surrealdb_expr::expr::logical::JoinKind::Anti),
			other => panic!("expected Join under Distinct, got {other:?}"),
		},
		other => panic!("expected Distinct, got {other:?}"),
	}

	// 5. MySQL UNION with outer ORDER BY & LIMIT
	let my_policy = DialectPolicy::mysql();
	let sql = "SELECT a FROM t1 UNION ALL SELECT a FROM t2 ORDER BY a DESC LIMIT 5;";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_mysql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let bound = binder::mysql::bind_select(select, &catalog, &my_policy, None).unwrap();
	let op = lower_bound_select(&bound, &my_policy).unwrap();
	match op {
		LogicalOp::Limit(lim) => match *lim.input {
			LogicalOp::Sort(sort) => match *sort.input {
				LogicalOp::UnionAll(_) => {}
				other => panic!("expected UnionAll under Sort, got {other:?}"),
			},
			other => panic!("expected Sort under Limit, got {other:?}"),
		},
		other => panic!("expected Limit, got {other:?}"),
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
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let bound = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap();
	let op = lower_bound_select(&bound, &pg_policy).unwrap();
	match op {
		LogicalOp::Project(proj) => match *proj.input {
			LogicalOp::Join(j) => {
				assert_eq!(j.kind, surrealdb_expr::expr::logical::JoinKind::Inner);
				assert_eq!(j.on.len(), 1);
			}
			other => panic!("expected Join under Project, got {other:?}"),
		},
		other => panic!("expected Project, got {other:?}"),
	}

	// 2. RIGHT JOIN rewrite
	let sql =
		"SELECT users.id FROM users RIGHT JOIN departments ON users.dept_id = departments.dept_id;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let bound = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap();
	// Confirm bound source is inverted to Left join
	match &bound.from {
		Some(crate::ir::BoundTableSource::Join {
			join_type,
			..
		}) => {
			assert_eq!(*join_type, crate::ir::BoundJoinType::Left);
		}
		other => panic!("expected Left join in bound IR, got {other:?}"),
	}

	// 3. Multi-condition ON
	let sql = "SELECT users.id FROM users JOIN departments ON users.id = departments.id AND users.dept_id = departments.dept_id;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let bound = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap();
	let op = lower_bound_select(&bound, &pg_policy).unwrap();
	match op {
		LogicalOp::Project(proj) => match *proj.input {
			LogicalOp::Join(j) => {
				assert_eq!(
					j.on.len(),
					2,
					"Expected 2 equi-join pairs extracted from AND condition"
				);
			}
			other => panic!("expected Join under Project, got {other:?}"),
		},
		other => panic!("expected Project, got {other:?}"),
	}
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
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let bound = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap();
	assert_eq!(bound.targets.len(), 2);
	assert!(matches!(bound.targets[1].expr, BoundExpr::ScalarSubquery(_)));
	let op = lower_bound_select(&bound, &pg_policy).unwrap();
	assert!(matches!(op, LogicalOp::Project(_)));

	// 2. EXISTS subquery in WHERE
	let sql = "SELECT id FROM users WHERE EXISTS (SELECT 1 FROM departments WHERE departments.id = users.dept_id);";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let bound = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap();
	assert!(matches!(bound.where_clause, Some(BoundExpr::Exists(_))));
	let op = lower_bound_select(&bound, &pg_policy).unwrap();
	assert!(matches!(op, LogicalOp::Project(_)));

	// 3. IN subquery in WHERE
	let sql = "SELECT id FROM users WHERE dept_id IN (SELECT id FROM departments);";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
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
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
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
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
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
	let select = match &stmts[0] {
		surrealdb_mysql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let bound = binder::mysql::bind_select(select, &catalog, &my_policy, None).unwrap();
	assert_eq!(bound.targets.len(), 2);
	assert!(matches!(bound.targets[1].expr, BoundExpr::ScalarSubquery(_)));
	let op = lower_bound_select(&bound, &my_policy).unwrap();
	assert!(matches!(op, LogicalOp::Project(_)));

	// 2. EXISTS subquery
	let sql = "SELECT id FROM users WHERE EXISTS (SELECT 1 FROM departments WHERE departments.id = users.dept_id);";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_mysql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let bound = binder::mysql::bind_select(select, &catalog, &my_policy, None).unwrap();
	assert!(matches!(bound.where_clause, Some(BoundExpr::Exists(_))));
	let op = lower_bound_select(&bound, &my_policy).unwrap();
	assert!(matches!(op, LogicalOp::Project(_)));

	// 3. IN subquery
	let sql = "SELECT id FROM users WHERE dept_id IN (SELECT id FROM departments);";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_mysql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
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
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
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
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let bound = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap();
	assert!(bound.with.is_some());
	assert_eq!(bound.with.as_ref().unwrap().ctes[0].columns, vec!["u_id", "u_name"]);
	let op = lower_bound_select(&bound, &pg_policy).unwrap();
	assert!(matches!(op, LogicalOp::Project(_)));

	// 3. Multiple CTEs referencing each other
	let sql = "WITH c1 AS (SELECT id FROM users), c2 AS (SELECT id FROM c1) SELECT id FROM c2;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let bound = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap();
	assert_eq!(bound.with.as_ref().unwrap().ctes.len(), 2);
	let op = lower_bound_select(&bound, &pg_policy).unwrap();
	assert!(matches!(op, LogicalOp::Project(_)));

	// 4. Recursive CTE without self-reference passes
	let sql = "WITH RECURSIVE c AS (SELECT 1 AS a) SELECT a FROM c;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let bound = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap();
	assert!(bound.with.as_ref().unwrap().recursive);
	let op = lower_bound_select(&bound, &pg_policy).unwrap();
	assert!(matches!(op, LogicalOp::Project(_)));

	// 5. Recursive CTE with self-reference fails closed
	let sql = "WITH RECURSIVE c AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM c WHERE n < 5) SELECT n FROM c;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
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
	let select = match &stmts[0] {
		surrealdb_mysql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
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
	let select = match &stmts[0] {
		surrealdb_mysql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let bound = binder::mysql::bind_select(select, &catalog, &my_policy, None).unwrap();
	assert!(bound.with.is_some());
	assert_eq!(bound.with.as_ref().unwrap().ctes[0].columns, vec!["u_id", "u_name"]);
	let op = lower_bound_select(&bound, &my_policy).unwrap();
	assert!(matches!(op, LogicalOp::Project(_)));

	// 3. Multiple CTEs referencing each other
	let sql = "WITH c1 AS (SELECT id FROM users), c2 AS (SELECT id FROM c1) SELECT id FROM c2;";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_mysql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let bound = binder::mysql::bind_select(select, &catalog, &my_policy, None).unwrap();
	assert_eq!(bound.with.as_ref().unwrap().ctes.len(), 2);
	let op = lower_bound_select(&bound, &my_policy).unwrap();
	assert!(matches!(op, LogicalOp::Project(_)));

	// 4. Recursive CTE with self-reference fails closed
	let sql = "WITH RECURSIVE c AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM c WHERE n < 5) SELECT n FROM c;";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_mysql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
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
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let bound = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap();
	assert_eq!(bound.targets.len(), 1);
	match &bound.targets[0].expr {
		BoundExpr::WindowFunction {
			name,
			args,
			distinct,
			spec,
		} => {
			assert_eq!(name, "row_number");
			assert!(args.is_empty());
			assert!(!distinct);
			assert!(spec.partition_by.is_empty());
			assert!(spec.order_by.is_empty());
			assert!(spec.frame.is_none());
		}
		other => panic!("expected WindowFunction, got {other:?}"),
	}
	let desc = bound.describe(&catalog, &pg_policy);
	assert_eq!(desc.columns[0].ty, SqlType::Int8);
	// Lowering fails closed
	let err = lower_bound_select(&bound, &pg_policy).unwrap_err();
	assert!(matches!(err, PlanError::UnsupportedExpression(_)));

	// 2. Window function with PARTITION BY and ORDER BY
	let sql = "SELECT rank() OVER (PARTITION BY dept ORDER BY salary DESC) FROM emp;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let bound = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap();
	match &bound.targets[0].expr {
		BoundExpr::WindowFunction {
			name,
			spec,
			..
		} => {
			assert_eq!(name, "rank");
			assert_eq!(spec.partition_by.len(), 1);
			assert_eq!(spec.order_by.len(), 1);
			assert_eq!(spec.order_by[0].direction, BoundOrderDirection::Desc);
		}
		other => panic!("expected WindowFunction, got {other:?}"),
	}

	// 3. Named window definition and reference
	let sql = "SELECT rank() OVER w FROM emp WINDOW w AS (PARTITION BY dept ORDER BY salary DESC);";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let bound = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap();
	assert_eq!(bound.windows.len(), 1);
	assert_eq!(bound.windows[0].name, "w");
	assert_eq!(bound.windows[0].spec.partition_by.len(), 1);
	match &bound.targets[0].expr {
		BoundExpr::WindowFunction {
			spec,
			..
		} => {
			assert_eq!(spec.window_name.as_deref(), Some("w"));
			assert_eq!(spec.partition_by.len(), 1);
			assert_eq!(spec.order_by.len(), 1);
		}
		other => panic!("expected WindowFunction, got {other:?}"),
	}

	// 4. ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
	let sql = "SELECT sum(salary) OVER (PARTITION BY dept ORDER BY hire_date ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM emp;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let bound = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap();
	match &bound.targets[0].expr {
		BoundExpr::WindowFunction {
			spec,
			..
		} => {
			let frame = spec.frame.as_ref().unwrap();
			assert_eq!(frame.units, BoundWindowFrameUnits::Rows);
			assert!(matches!(frame.start, BoundWindowFrameBound::Preceding(_)));
			assert!(matches!(frame.end, Some(BoundWindowFrameBound::Following(_))));
		}
		other => panic!("expected WindowFunction, got {other:?}"),
	}

	// 5. Semantic rejection in WHERE
	let sql = "SELECT id FROM emp WHERE rank() OVER () > 1;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let err = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap_err();
	assert!(matches!(err, PlanError::WindowNotAllowed(clause) if clause.contains("WHERE")));

	// 6. Semantic rejection in GROUP BY
	let sql = "SELECT dept FROM emp GROUP BY rank() OVER ();";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let err = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap_err();
	assert!(matches!(err, PlanError::WindowNotAllowed(clause) if clause.contains("GROUP BY")));

	// 7. Semantic rejection in HAVING
	let sql = "SELECT dept FROM emp HAVING rank() OVER () > 1;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let err = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap_err();
	assert!(matches!(err, PlanError::WindowNotAllowed(clause) if clause.contains("HAVING")));

	// 8. Semantic rejection of nested window calls
	let sql = "SELECT sum(rank() OVER ()) OVER () FROM emp;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
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
	let select = match &stmts[0] {
		surrealdb_mysql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let bound = binder::mysql::bind_select(select, &catalog, &my_policy, None).unwrap();
	assert_eq!(bound.targets.len(), 1);
	match &bound.targets[0].expr {
		BoundExpr::WindowFunction {
			name,
			args,
			distinct,
			spec,
		} => {
			assert_eq!(name, "row_number");
			assert!(args.is_empty());
			assert!(!distinct);
			assert!(spec.partition_by.is_empty());
			assert!(spec.order_by.is_empty());
			assert!(spec.frame.is_none());
		}
		other => panic!("expected WindowFunction, got {other:?}"),
	}
	let desc = bound.describe(&catalog, &my_policy);
	assert_eq!(desc.columns[0].ty, SqlType::Int8);
	// Lowering fails closed
	let err = lower_bound_select(&bound, &my_policy).unwrap_err();
	assert!(matches!(err, PlanError::UnsupportedExpression(_)));

	// 2. Window function with PARTITION BY and ORDER BY
	let sql = "SELECT RANK() OVER (PARTITION BY dept ORDER BY salary DESC) FROM emp;";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_mysql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let bound = binder::mysql::bind_select(select, &catalog, &my_policy, None).unwrap();
	match &bound.targets[0].expr {
		BoundExpr::WindowFunction {
			name,
			spec,
			..
		} => {
			assert_eq!(name, "rank");
			assert_eq!(spec.partition_by.len(), 1);
			assert_eq!(spec.order_by.len(), 1);
			assert_eq!(spec.order_by[0].direction, BoundOrderDirection::Desc);
		}
		other => panic!("expected WindowFunction, got {other:?}"),
	}

	// 3. Named window definition and reference
	let sql = "SELECT RANK() OVER w FROM emp WINDOW w AS (PARTITION BY dept ORDER BY salary DESC);";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_mysql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let bound = binder::mysql::bind_select(select, &catalog, &my_policy, None).unwrap();
	assert_eq!(bound.windows.len(), 1);
	assert_eq!(bound.windows[0].name, "w");
	assert_eq!(bound.windows[0].spec.partition_by.len(), 1);
	match &bound.targets[0].expr {
		BoundExpr::WindowFunction {
			spec,
			..
		} => {
			assert_eq!(spec.window_name.as_deref(), Some("w"));
			assert_eq!(spec.partition_by.len(), 1);
			assert_eq!(spec.order_by.len(), 1);
		}
		other => panic!("expected WindowFunction, got {other:?}"),
	}

	// 4. ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
	let sql = "SELECT SUM(salary) OVER (PARTITION BY dept ORDER BY hire_date ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM emp;";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_mysql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let bound = binder::mysql::bind_select(select, &catalog, &my_policy, None).unwrap();
	match &bound.targets[0].expr {
		BoundExpr::WindowFunction {
			spec,
			..
		} => {
			let frame = spec.frame.as_ref().unwrap();
			assert_eq!(frame.units, BoundWindowFrameUnits::Rows);
			assert!(matches!(frame.start, BoundWindowFrameBound::Preceding(_)));
			assert!(matches!(frame.end, Some(BoundWindowFrameBound::Following(_))));
		}
		other => panic!("expected WindowFunction, got {other:?}"),
	}

	// 5. Semantic rejection in WHERE
	let sql = "SELECT id FROM emp WHERE RANK() OVER () > 1;";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_mysql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let err = binder::mysql::bind_select(select, &catalog, &my_policy, None).unwrap_err();
	assert!(matches!(err, PlanError::WindowNotAllowed(clause) if clause.contains("WHERE")));

	// 6. Semantic rejection in GROUP BY
	let sql = "SELECT dept FROM emp GROUP BY RANK() OVER ();";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_mysql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let err = binder::mysql::bind_select(select, &catalog, &my_policy, None).unwrap_err();
	assert!(matches!(err, PlanError::WindowNotAllowed(clause) if clause.contains("GROUP BY")));

	// 7. Semantic rejection in HAVING
	let sql = "SELECT dept FROM emp HAVING RANK() OVER () > 1;";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_mysql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let err = binder::mysql::bind_select(select, &catalog, &my_policy, None).unwrap_err();
	assert!(matches!(err, PlanError::WindowNotAllowed(clause) if clause.contains("HAVING")));

	// 8. Semantic rejection of nested window calls
	let sql = "SELECT SUM(RANK() OVER ()) OVER () FROM emp;";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_mysql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
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
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let err = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap_err();
	assert!(matches!(err, PlanError::UndefinedFunction(ref f) if f == "sleep"));
	assert_eq!(err.pg_sqlstate(), "42883");

	// 5. MySQL binding: reject undefined function with error code 1305
	let sql = "SELECT sleep(5) FROM users;";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_mysql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let err = binder::mysql::bind_select(select, &catalog, &my_policy, None).unwrap_err();
	assert!(matches!(err, PlanError::UndefinedFunction(ref f) if f == "sleep"));
	assert_eq!(err.mysql_error_code(), 1305);

	// 6. Arity mismatch: min_args check
	let sql = "SELECT round() FROM users;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let err = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap_err();
	assert!(
		matches!(err, PlanError::FunctionArgumentCountMismatch { ref name, got: 0, .. } if name == "round")
	);
	assert_eq!(err.pg_sqlstate(), "42883");

	// 7. Arity mismatch: max_args check
	let sql = "SELECT lower('a', 'b', 'c') FROM users;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let err = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap_err();
	assert!(
		matches!(err, PlanError::FunctionArgumentCountMismatch { ref name, got: 3, .. } if name == "lower")
	);

	// 8. Window function without OVER: SQLSTATE 42809 / MySQL 3593
	let sql = "SELECT rank() FROM users;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let err = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap_err();
	assert!(matches!(err, PlanError::WindowFunctionWithoutOver(ref f) if f == "rank"));
	assert_eq!(err.pg_sqlstate(), "42809");

	let sql = "SELECT rank() FROM users;";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_mysql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let err = binder::mysql::bind_select(select, &catalog, &my_policy, None).unwrap_err();
	assert!(matches!(err, PlanError::WindowFunctionWithoutOver(ref f) if f == "rank"));
	assert_eq!(err.mysql_error_code(), 3593);

	// 9. Scalar function with OVER rejected
	let sql = "SELECT lower(name) OVER () FROM users;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
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
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let bound = binder::pgsql::bind_select(select, &catalog, &pg_policy, None).unwrap();
	assert_eq!(bound.targets.len(), 1);
}

#[test]
fn test_pg_catalog_table_resolution_and_lowering() {
	let sql = "SELECT relname, relkind FROM pg_class WHERE relkind = 'r';";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};

	let mut catalog = MemoryCatalog::new();
	catalog.define_table(None, "orders", &["id", "amount"]);
	let policy = DialectPolicy::postgres();

	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).unwrap();
	assert_eq!(bound.targets.len(), 2);

	let op = lower_bound_select(&bound, &policy).unwrap();
	match op {
		LogicalOp::Project(proj) => match *proj.input {
			LogicalOp::Filter(filter) => match *filter.input {
				LogicalOp::Values(values) => {
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
				other => panic!("expected Values input to Filter, got {other:?}"),
			},
			other => panic!("expected Filter input to Project, got {other:?}"),
		},
		other => panic!("expected Project, got {other:?}"),
	}
}

#[test]
fn test_pg_type_query_and_columns() {
	let sql = "SELECT typname, typlen FROM pg_catalog.pg_type WHERE oid = 23;";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
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
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};

	let catalog = MemoryCatalog::new();
	let policy = DialectPolicy::postgres();

	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).unwrap();
	let op = lower_bound_select(&bound, &policy).unwrap();
	match op {
		LogicalOp::Project(proj) => match *proj.input {
			LogicalOp::Values(v) => {
				let LogicalExpr::Array(rows) = v.expr else {
					panic!("expected Array of rows");
				};
				assert_eq!(rows.len(), 4); // pg_catalog, public, information_schema, pg_toast
			}
			other => panic!("expected Values, got {other:?}"),
		},
		other => panic!("expected Project, got {other:?}"),
	}

	let sql_settings = "SELECT name, setting FROM pg_settings;";
	let stmts = surrealdb_pgsql::parse_statements(sql_settings).unwrap();
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
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
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).unwrap();
	let op = lower_bound_select(&bound, &policy).unwrap();
	assert!(matches!(op, LogicalOp::Project(_)));

	let sql_cols =
		"SELECT column_name FROM information_schema.columns WHERE table_name = 'products';";
	let stmts = surrealdb_pgsql::parse_statements(sql_cols).unwrap();
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).unwrap();
	let op = lower_bound_select(&bound, &policy).unwrap();
	assert!(matches!(op, LogicalOp::Project(_)));
}

#[test]
fn test_catalog_introspection_functions() {
	let sql = "SELECT version(), current_schema(), current_schemas(true), current_user, session_user, user, pg_backend_pid(), pg_is_in_recovery(), format_type(23, -1), pg_table_is_visible(1259), has_table_privilege('users', 'SELECT');";
	let stmts = surrealdb_pgsql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
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
	let select = match &stmts[0] {
		surrealdb_pgsql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};

	let catalog = MemoryCatalog::new();
	let policy = DialectPolicy::postgres();

	let bound = binder::pgsql::bind_select(select, &catalog, &policy, None).unwrap();
	let op = lower_bound_select(&bound, &policy).unwrap();
	match op {
		LogicalOp::Project(proj) => match *proj.input {
			LogicalOp::Filter(filter) => match *filter.input {
				LogicalOp::Join(join) => {
					assert!(matches!(*join.left(), LogicalOp::Values(_)));
					assert!(matches!(*join.right(), LogicalOp::Values(_)));
				}
				other => panic!("expected Join, got {other:?}"),
			},
			other => panic!("expected Filter, got {other:?}"),
		},
		other => panic!("expected Project, got {other:?}"),
	}
}

#[test]
fn test_mysql_user_and_system_variables() {
	let sql =
		"SELECT @@version_comment, @@sql_mode, @@autocommit, @@global.time_zone, @session_var;";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_mysql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
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
	let select = match &stmts[0] {
		surrealdb_mysql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
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
	let select = match &stmts[0] {
		surrealdb_mysql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
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
	let select = match &stmts[0] {
		surrealdb_mysql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let bound = binder::mysql::bind_select(select, &catalog, &policy, None).unwrap();
	let op = lower_bound_select(&bound, &policy).unwrap();
	assert!(matches!(op, LogicalOp::Project(_)));

	let sql_any = "SELECT id FROM t WHERE id = ANY (SELECT val FROM s);";
	let stmts_any = surrealdb_mysql::parse_statements(sql_any).unwrap();
	let select_any = match &stmts_any[0] {
		surrealdb_mysql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};
	let bound_any = binder::mysql::bind_select(select_any, &catalog, &policy, None).unwrap();
	let op_any = lower_bound_select(&bound_any, &policy).unwrap();
	assert!(matches!(op_any, LogicalOp::Project(_)));
}

#[test]
fn test_mysql_builtin_functions() {
	let sql = "SELECT version(), found_rows(), row_count(), connection_id(), last_insert_id(), curdate(), curtime(), utc_timestamp(), unix_timestamp(), format(1234.5, 2), truncate(12.345, 1), regexp_like('abc', '^a');";
	let stmts = surrealdb_mysql::parse_statements(sql).unwrap();
	let select = match &stmts[0] {
		surrealdb_mysql::ast::stmt::Statement::Select(s) => s,
		_ => panic!("expected SELECT"),
	};

	let catalog = MemoryCatalog::new();
	let policy = DialectPolicy::mysql();

	let bound = binder::mysql::bind_select(select, &catalog, &policy, None).unwrap();
	assert_eq!(bound.targets.len(), 12);

	let op = lower_bound_select(&bound, &policy).unwrap();
	assert!(matches!(op, LogicalOp::Project(_)));
}
