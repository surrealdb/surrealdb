//! `surrealdb-sqlplan` - SQL planner, binder, and bound IR for SurrealDB.

pub mod binder;
pub mod catalog;
pub mod descriptor;
pub mod dialect;
pub mod error;
pub mod ir;
pub mod lower;
pub mod scope;
pub mod semantics;

pub use catalog::{BinderCatalog, ColumnInfo, EmptyCatalog, MemoryCatalog, TableSchema};
pub use descriptor::{
	ParameterDescriptor, ResultColumn, ResultDescriptor, SqlType, StatementDescriptor,
};
pub use dialect::{DialectPolicy, NamePolicy, NullOrderPolicy, SqlDialect};
pub use error::PlanError;
pub use ir::{
	BoundBinaryOp, BoundColumnRef, BoundDistinct, BoundExpr, BoundJoinCondition, BoundJoinType,
	BoundLiteral, BoundOrder, BoundOrderDirection, BoundSelect, BoundTableSource, BoundTarget,
	BoundUnaryOp,
};
pub use lower::lower_bound_select;
pub use scope::{QueryScope, RelationEntry};
pub use semantics::{fold_expr, fold_select};
use surrealdb_expr::expr::logical::LogicalOp;
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
