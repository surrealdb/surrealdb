//! `surrealdb-sqlplan` - SQL planner, binder, and bound IR for SurrealDB.

pub mod binder;
pub mod catalog;
pub mod descriptor;
pub mod dialect;
pub mod error;
pub mod ir;
pub mod lower;
pub mod scope;

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
use surrealdb_expr::expr::logical::LogicalOp;

/// Bind and lower a PostgreSQL `SelectStmt` directly to a `LogicalOp`.
pub fn bind_and_lower_pgsql(
	select: &surrealdb_pgsql::ast::stmt::SelectStmt,
	catalog: &dyn BinderCatalog,
	policy: &DialectPolicy,
) -> Result<LogicalOp, PlanError> {
	let bound = binder::pgsql::bind_select(select, catalog, policy, None)?;
	lower_bound_select(&bound)
}

/// Bind and lower a MySQL `SelectStmt` directly to a `LogicalOp`.
pub fn bind_and_lower_mysql(
	select: &surrealdb_mysql::ast::stmt::SelectStmt,
	catalog: &dyn BinderCatalog,
	policy: &DialectPolicy,
) -> Result<LogicalOp, PlanError> {
	let bound = binder::mysql::bind_select(select, catalog, policy, None)?;
	lower_bound_select(&bound)
}

#[cfg(test)]
mod tests;
