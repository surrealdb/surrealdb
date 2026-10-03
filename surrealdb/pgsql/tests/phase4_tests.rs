//! Comprehensive unit and integration tests for Phase 4 PostgreSQL features:
//! - CTEs (`WITH [RECURSIVE] ...`) for SELECT, INSERT, UPDATE, DELETE
//! - Window functions (`OVER (...)`) and `WINDOW` clause
//! - Table and row locking clauses (`FOR UPDATE`, `FOR SHARE`, `NOWAIT`, `SKIP LOCKED`)
//! - Extended DDL (`CREATE INDEX`, `CREATE VIEW`, `CREATE SCHEMA`, `ALTER TABLE`)
//! - Prepared statements (`PREPARE`, `EXECUTE`, `DEALLOCATE`)
//! - Round-trip printing fidelity

use surrealdb_pgsql::ast::expr::{NullsOrder, OrderDirection};
use surrealdb_pgsql::ast::stmt::{AlterTableAction, LockingStrength, LockingWaitPolicy, Statement};
use surrealdb_pgsql::{ToPgSql, parse_expr, parse_statement};

fn assert_expr_roundtrip(sql: &str) {
	let mut ast1 = parse_expr(sql).unwrap_or_else(|e| panic!("failed to parse expr '{sql}': {e}"));
	let printed1 = ast1.to_pgsql_string();
	let mut ast2 = parse_expr(&printed1).unwrap_or_else(|e| {
		panic!("failed to reparse printed expr '{printed1}' (original: '{sql}'): {e}")
	});
	let printed2 = ast2.to_pgsql_string();

	assert_eq!(printed1, printed2, "idempotent printing failed for expr: {sql}");

	ast1.normalize_spans();
	ast2.normalize_spans();
	assert_eq!(ast1, ast2, "AST round-trip equality failed for expr: {sql}");
}

fn assert_stmt_roundtrip(sql: &str) {
	let mut ast1 =
		parse_statement(sql).unwrap_or_else(|e| panic!("failed to parse statement '{sql}': {e}"));
	let printed1 = ast1.to_pgsql_string();
	let mut ast2 = parse_statement(&printed1).unwrap_or_else(|e| {
		panic!("failed to reparse printed stmt '{printed1}' (original: '{sql}'): {e}")
	});
	let printed2 = ast2.to_pgsql_string();

	assert_eq!(printed1, printed2, "idempotent printing failed for statement: {sql}");

	ast1.normalize_spans();
	ast2.normalize_spans();
	assert_eq!(ast1, ast2, "AST round-trip equality failed for statement: {sql}");
}

// ----------------------------------------------------------------------------
// CTE / WITH Clause Tests
// ----------------------------------------------------------------------------

#[test]
fn test_parse_simple_cte() {
	let sql = "WITH regional_sales AS (SELECT region, sum(amount) AS total_sales FROM orders GROUP BY region) SELECT region, total_sales FROM regional_sales";
	let stmt = parse_statement(sql).expect("parse simple CTE");

	match stmt {
		Statement::Select(select) => {
			let with = select.with.expect("WITH clause expected");
			assert!(!with.recursive);
			assert_eq!(with.ctes.len(), 1);
			assert_eq!(with.ctes[0].name, "regional_sales");
			assert!(with.ctes[0].columns.is_empty());
			assert_eq!(with.ctes[0].materialized, None);
		}
		other => panic!("expected Select statement, got {:?}", other),
	}

	assert_stmt_roundtrip(sql);
}

#[test]
fn test_parse_multi_cte_with_column_aliases() {
	let sql = "WITH t1(a, b) AS (SELECT 1, 2), t2(c) AS (SELECT 3) SELECT a, b, c FROM t1, t2";
	let stmt = parse_statement(sql).expect("parse multi CTE");

	match stmt {
		Statement::Select(select) => {
			let with = select.with.expect("WITH clause expected");
			assert_eq!(with.ctes.len(), 2);
			assert_eq!(with.ctes[0].name, "t1");
			assert_eq!(with.ctes[0].columns, vec!["a", "b"]);
			assert_eq!(with.ctes[1].name, "t2");
			assert_eq!(with.ctes[1].columns, vec!["c"]);
		}
		other => panic!("expected Select statement, got {:?}", other),
	}

	assert_stmt_roundtrip(sql);
}

#[test]
fn test_parse_recursive_cte() {
	let sql = "WITH RECURSIVE countdown(val) AS (SELECT 10 UNION ALL SELECT val - 1 FROM countdown WHERE val > 0) SELECT val FROM countdown";
	let stmt = parse_statement(sql).expect("parse recursive CTE");

	match stmt {
		Statement::Select(select) => {
			let with = select.with.expect("WITH clause expected");
			assert!(with.recursive);
			assert_eq!(with.ctes.len(), 1);
			assert_eq!(with.ctes[0].name, "countdown");
		}
		other => panic!("expected Select statement, got {:?}", other),
	}

	assert_stmt_roundtrip(sql);
}

#[test]
fn test_parse_materialized_cte() {
	let sql1 = "WITH m_cte AS MATERIALIZED (SELECT 1) SELECT * FROM m_cte";
	let stmt1 = parse_statement(sql1).expect("parse MATERIALIZED CTE");
	if let Statement::Select(select) = stmt1 {
		let with = select.with.unwrap();
		assert_eq!(with.ctes[0].materialized, Some(true));
	}
	assert_stmt_roundtrip(sql1);

	let sql2 = "WITH nm_cte AS NOT MATERIALIZED (SELECT 2) SELECT * FROM nm_cte";
	let stmt2 = parse_statement(sql2).expect("parse NOT MATERIALIZED CTE");
	if let Statement::Select(select) = stmt2 {
		let with = select.with.unwrap();
		assert_eq!(with.ctes[0].materialized, Some(false));
	}
	assert_stmt_roundtrip(sql2);
}

#[test]
fn test_parse_cte_with_dml() {
	// INSERT with CTE
	let sql_insert = "WITH src AS (SELECT id, val FROM staged) INSERT INTO target (id, val) SELECT id, val FROM src";
	let stmt_insert = parse_statement(sql_insert).expect("parse INSERT with CTE");
	assert!(matches!(stmt_insert, Statement::Insert(_)));
	assert_stmt_roundtrip(sql_insert);

	// UPDATE with CTE
	let sql_update = "WITH src AS (SELECT 42 AS new_val) UPDATE target SET val = src.new_val FROM src WHERE target.id = 1";
	let stmt_update = parse_statement(sql_update).expect("parse UPDATE with CTE");
	assert!(matches!(stmt_update, Statement::Update(_)));
	assert_stmt_roundtrip(sql_update);

	// DELETE with CTE
	let sql_delete = "WITH old_ids AS (SELECT id FROM expired) DELETE FROM target USING old_ids WHERE target.id = old_ids.id";
	let stmt_delete = parse_statement(sql_delete).expect("parse DELETE with CTE");
	assert!(matches!(stmt_delete, Statement::Delete(_)));
	assert_stmt_roundtrip(sql_delete);
}

// ----------------------------------------------------------------------------
// Window Function & WINDOW Clause Tests
// ----------------------------------------------------------------------------

#[test]
fn test_parse_window_function_over_empty() {
	assert_expr_roundtrip("row_number() OVER ()");
	assert_stmt_roundtrip("SELECT row_number() OVER () FROM t");
}

#[test]
fn test_parse_window_function_partition_and_order() {
	let sql = "SELECT emp_name, salary, avg(salary) OVER (PARTITION BY dept_id ORDER BY hire_date ASC NULLS FIRST) FROM employees";
	let stmt = parse_statement(sql).expect("parse window partition and order");
	assert!(matches!(stmt, Statement::Select(_)));
	assert_stmt_roundtrip(sql);
}

#[test]
fn test_parse_window_frames() {
	assert_expr_roundtrip(
		"sum(val) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
	);
	assert_expr_roundtrip("avg(val) OVER (ORDER BY ts RANGE BETWEEN 5 PRECEDING AND 5 FOLLOWING)");
	assert_expr_roundtrip(
		"count(val) OVER (ORDER BY ts GROUPS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW)",
	);
	assert_expr_roundtrip("sum(val) OVER (ORDER BY ts ROWS UNBOUNDED PRECEDING)");
}

#[test]
fn test_parse_window_clause() {
	let sql = "SELECT avg(salary) OVER w, sum(salary) OVER w FROM employees WINDOW w AS (PARTITION BY dept_id ORDER BY salary DESC)";
	let stmt = parse_statement(sql).expect("parse WINDOW clause");

	match stmt {
		Statement::Select(select) => {
			assert_eq!(select.window.len(), 1);
			assert_eq!(select.window[0].name, "w");
			assert_eq!(select.window[0].spec.partition_by.len(), 1);
			assert_eq!(select.window[0].spec.order_by.len(), 1);
		}
		other => panic!("expected Select, got {:?}", other),
	}

	assert_stmt_roundtrip(sql);
}

// ----------------------------------------------------------------------------
// Locking Clauses (FOR UPDATE / SHARE)
// ----------------------------------------------------------------------------

#[test]
fn test_parse_locking_clauses() {
	let sql1 = "SELECT * FROM accounts WHERE id = 1 FOR UPDATE";
	let stmt1 = parse_statement(sql1).expect("parse FOR UPDATE");
	if let Statement::Select(s) = stmt1 {
		assert_eq!(s.locking.len(), 1);
		assert_eq!(s.locking[0].strength, LockingStrength::Update);
		assert_eq!(s.locking[0].wait_policy, LockingWaitPolicy::Block);
	}
	assert_stmt_roundtrip(sql1);

	let sql2 = "SELECT * FROM accounts FOR SHARE NOWAIT";
	let stmt2 = parse_statement(sql2).expect("parse FOR SHARE NOWAIT");
	if let Statement::Select(s) = stmt2 {
		assert_eq!(s.locking[0].strength, LockingStrength::Share);
		assert_eq!(s.locking[0].wait_policy, LockingWaitPolicy::NoWait);
	}
	assert_stmt_roundtrip(sql2);

	let sql3 = "SELECT * FROM accounts FOR NO KEY UPDATE SKIP LOCKED";
	let stmt3 = parse_statement(sql3).expect("parse FOR NO KEY UPDATE SKIP LOCKED");
	if let Statement::Select(s) = stmt3 {
		assert_eq!(s.locking[0].strength, LockingStrength::NoKeyUpdate);
		assert_eq!(s.locking[0].wait_policy, LockingWaitPolicy::SkipLocked);
	}
	assert_stmt_roundtrip(sql3);

	let sql4 = "SELECT * FROM t1, t2 FOR KEY SHARE OF t1, t2 NOWAIT";
	let stmt4 = parse_statement(sql4).expect("parse FOR KEY SHARE OF");
	if let Statement::Select(s) = stmt4 {
		assert_eq!(s.locking[0].strength, LockingStrength::KeyShare);
		assert_eq!(s.locking[0].tables.len(), 2);
	}
	assert_stmt_roundtrip(sql4);
}

// ----------------------------------------------------------------------------
// Extended DDL Tests (CREATE INDEX, VIEW, SCHEMA, ALTER TABLE)
// ----------------------------------------------------------------------------

#[test]
fn test_parse_create_index() {
	let sql1 = "CREATE INDEX idx_users_email ON users (email)";
	let stmt1 = parse_statement(sql1).expect("parse simple create index");
	assert!(matches!(stmt1, Statement::CreateIndex(_)));
	assert_stmt_roundtrip(sql1);

	let sql2 = "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_full ON orders USING btree (created_at DESC NULLS LAST, total ASC NULLS FIRST) WHERE total > 100";
	let stmt2 = parse_statement(sql2).expect("parse complex create index");
	match stmt2 {
		Statement::CreateIndex(idx) => {
			assert!(idx.unique);
			assert!(idx.concurrently);
			assert!(idx.if_not_exists);
			assert_eq!(idx.name.as_deref(), Some("idx_full"));
			assert_eq!(idx.method.as_deref(), Some("btree"));
			assert_eq!(idx.columns.len(), 2);
			assert_eq!(idx.columns[0].direction, Some(OrderDirection::Desc));
			assert_eq!(idx.columns[0].nulls, Some(NullsOrder::Last));
			assert_eq!(idx.columns[1].direction, Some(OrderDirection::Asc));
			assert_eq!(idx.columns[1].nulls, Some(NullsOrder::First));
			assert!(idx.predicate.is_some());
		}
		other => panic!("expected CreateIndex, got {:?}", other),
	}
	assert_stmt_roundtrip(sql2);
}

#[test]
fn test_parse_create_view() {
	let sql1 = "CREATE VIEW active_users AS SELECT id, name FROM users WHERE active = TRUE";
	let stmt1 = parse_statement(sql1).expect("parse create view");
	assert!(matches!(stmt1, Statement::CreateView(_)));
	assert_stmt_roundtrip(sql1);

	let sql2 = "CREATE OR REPLACE TEMPORARY VIEW v_summary (dept, total) AS SELECT dept, sum(salary) FROM emp GROUP BY dept";
	let stmt2 = parse_statement(sql2).expect("parse create or replace temp view");
	match stmt2 {
		Statement::CreateView(v) => {
			assert!(v.or_replace);
			assert!(v.temporary);
			assert!(!v.recursive);
			assert_eq!(v.columns, vec!["dept", "total"]);
		}
		other => panic!("expected CreateView, got {:?}", other),
	}
	assert_stmt_roundtrip(sql2);
}

#[test]
fn test_parse_create_schema() {
	let sql1 = "CREATE SCHEMA my_schema";
	let stmt1 = parse_statement(sql1).expect("parse create schema");
	assert!(matches!(stmt1, Statement::CreateSchema(_)));
	assert_stmt_roundtrip(sql1);

	let sql2 = "CREATE SCHEMA IF NOT EXISTS tenant_123";
	let stmt2 = parse_statement(sql2).expect("parse create schema if not exists");
	match stmt2 {
		Statement::CreateSchema(s) => {
			assert!(s.if_not_exists);
			assert_eq!(s.name, "tenant_123");
		}
		other => panic!("expected CreateSchema, got {:?}", other),
	}
	assert_stmt_roundtrip(sql2);
}

#[test]
fn test_parse_alter_table() {
	let sql = "ALTER TABLE users ADD COLUMN bio TEXT, DROP COLUMN IF EXISTS temp_token CASCADE, ALTER COLUMN age SET DEFAULT 18, ALTER COLUMN email SET NOT NULL, RENAME COLUMN fullname TO full_name";
	let stmt = parse_statement(sql).expect("parse multi-action alter table");

	match stmt {
		Statement::AlterTable(alter) => {
			assert!(!alter.if_exists);
			assert_eq!(alter.name.to_pgsql_string(), "users");
			assert_eq!(alter.actions.len(), 5);
			assert!(matches!(alter.actions[0], AlterTableAction::AddColumn { .. }));
			assert!(matches!(alter.actions[1], AlterTableAction::DropColumn { .. }));
			assert!(matches!(alter.actions[2], AlterTableAction::SetDefault { .. }));
			assert!(matches!(alter.actions[3], AlterTableAction::SetNotNull { .. }));
			assert!(matches!(alter.actions[4], AlterTableAction::RenameColumn { .. }));
		}
		other => panic!("expected AlterTable, got {:?}", other),
	}

	assert_stmt_roundtrip(sql);
}

// ----------------------------------------------------------------------------
// Prepared Statements (PREPARE, EXECUTE, DEALLOCATE)
// ----------------------------------------------------------------------------

#[test]
fn test_parse_prepared_statements() {
	let sql_prep =
		"PREPARE get_user (INT, TEXT) AS SELECT * FROM users WHERE id = $1 AND role = $2";
	let stmt_prep = parse_statement(sql_prep).expect("parse prepare");
	match stmt_prep {
		Statement::Prepare(prep) => {
			assert_eq!(prep.name, "get_user");
			assert_eq!(prep.param_types.len(), 2);
		}
		other => panic!("expected Prepare, got {:?}", other),
	}
	assert_stmt_roundtrip(sql_prep);

	let sql_exec1 = "EXECUTE get_user (42, 'admin')";
	let stmt_exec1 = parse_statement(sql_exec1).expect("parse execute with params");
	match stmt_exec1 {
		Statement::Execute(exec) => {
			assert_eq!(exec.name, "get_user");
			assert_eq!(exec.params.len(), 2);
		}
		other => panic!("expected Execute, got {:?}", other),
	}
	assert_stmt_roundtrip(sql_exec1);

	let sql_exec2 = "EXECUTE reset_all";
	let stmt_exec2 = parse_statement(sql_exec2).expect("parse execute without params");
	assert!(matches!(stmt_exec2, Statement::Execute(_)));
	assert_stmt_roundtrip(sql_exec2);

	let sql_deal1 = "DEALLOCATE get_user";
	let stmt_deal1 = parse_statement(sql_deal1).expect("parse deallocate");
	assert!(matches!(stmt_deal1, Statement::Deallocate(_)));
	assert_stmt_roundtrip(sql_deal1);

	let sql_deal2 = "DEALLOCATE PREPARE get_user";
	let stmt_deal2 = parse_statement(sql_deal2).expect("parse deallocate prepare");
	assert!(matches!(stmt_deal2, Statement::Deallocate(_)));

	let sql_deal3 = "DEALLOCATE ALL";
	let stmt_deal3 = parse_statement(sql_deal3).expect("parse deallocate all");
	match stmt_deal3 {
		Statement::Deallocate(deal) => {
			assert_eq!(deal.name, None);
		}
		other => panic!("expected Deallocate, got {:?}", other),
	}
	assert_stmt_roundtrip(sql_deal3);
}
