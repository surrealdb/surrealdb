//! `surrealdb-sqlplan` - SQL planner, binder, and bound IR for SurrealDB.

#![allow(clippy::absolute_paths)]

pub mod binder;
pub mod catalog;
pub mod descriptor;
pub mod dialect;
pub mod error;
pub mod functions;
pub mod ir;
pub mod lower;
pub mod namespace;
pub mod observability;
pub mod scope;
pub mod semantics;

pub use catalog::{BinderCatalog, ColumnInfo, EmptyCatalog, MemoryCatalog, TableSchema};
pub use descriptor::{
	ParameterDescriptor, ResultColumn, ResultDescriptor, SqlType, StatementDescriptor,
};
pub use dialect::{DialectPolicy, NamePolicy, NullOrderPolicy, SqlDialect};
pub use error::PlanError;
pub use functions::{FunctionRegistry, ReturnTypeStrategy, SqlFunctionDef, SqlFunctionKind};
pub use ir::{
	BoundBinaryOp, BoundColumnRef, BoundDistinct, BoundExpr, BoundJoinCondition, BoundJoinType,
	BoundLiteral, BoundOrder, BoundOrderDirection, BoundSelect, BoundTableSource, BoundTarget,
	BoundUnaryOp,
};
pub use lower::ddl::{
	lower_mysql_create_table, lower_mysql_ddl, lower_mysql_drop_table, lower_mysql_use,
	lower_pgsql_alter_table, lower_pgsql_create_index, lower_pgsql_create_table,
	lower_pgsql_create_view, lower_pgsql_ddl, lower_pgsql_drop, map_mysql_type, map_pgsql_type,
};
pub use lower::write::{
	lower_mysql_delete, lower_mysql_expr, lower_mysql_insert, lower_mysql_replace,
	lower_mysql_truncate, lower_mysql_update, lower_pgsql_delete, lower_pgsql_expr,
	lower_pgsql_insert, lower_pgsql_truncate, lower_pgsql_update,
};
pub use lower::{lower_bound_select, lower_bound_subquery};
pub use namespace::{resolve_mysql_table_name, resolve_pgsql_table_name};
pub use observability::{
	SqlPlanMetrics, SqlPlanMetricsSnapshot, global_metrics, redact_sql_credentials,
};
pub use scope::{QueryScope, RelationEntry};
pub use semantics::{fold_expr, fold_select};
pub use surrealdb_expr::expr::logical::LogicalOp;
use surrealdb_mysql::ast::stmt::SelectStmt as MySelectStmt;
use surrealdb_pgsql::ast::stmt::SelectStmt as PgSelectStmt;

/// Bind, fold, and lower a PostgreSQL `SelectStmt` directly to a `LogicalOp`.
pub fn bind_and_lower_pgsql(
	select: &PgSelectStmt,
	catalog: &dyn BinderCatalog,
	policy: &DialectPolicy,
) -> Result<LogicalOp, PlanError> {
	let mut bound = binder::pgsql::bind_select(select, catalog, policy, None)?;
	semantics::fold_select(&mut bound, policy)?;
	lower_bound_select(&bound, policy)
}

/// Bind, fold, and lower a MySQL `SelectStmt` directly to a `LogicalOp`.
pub fn bind_and_lower_mysql(
	select: &MySelectStmt,
	catalog: &dyn BinderCatalog,
	policy: &DialectPolicy,
) -> Result<LogicalOp, PlanError> {
	let mut bound = binder::mysql::bind_select(select, catalog, policy, None)?;
	semantics::fold_select(&mut bound, policy)?;
	lower_bound_select(&bound, policy)
}

#[cfg(test)]
mod tests;
