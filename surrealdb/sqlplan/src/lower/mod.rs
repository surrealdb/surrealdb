//! Lowering from Bound IR to SurrealDB `LogicalOp`.

pub mod expr;
pub mod plan;
pub mod write;

pub use plan::lower_bound_select;
pub use write::{
	lower_mysql_delete, lower_mysql_expr, lower_mysql_insert, lower_mysql_replace,
	lower_mysql_truncate, lower_mysql_update, lower_pgsql_delete, lower_pgsql_expr,
	lower_pgsql_insert, lower_pgsql_truncate, lower_pgsql_update,
};

#[cfg(test)]
mod write_tests;
