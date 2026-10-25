//! Lowering from Bound IR to SurrealDB `LogicalOp`.

pub mod ddl;
pub mod expr;
pub(crate) mod id_key;
pub(crate) mod link;
pub mod plan;
mod quote_ident;
mod sql_like;
pub mod write;
mod write_null;

pub use ddl::{
	lower_mysql_create_table, lower_mysql_ddl, lower_mysql_drop_table, lower_pgsql_alter_table,
	lower_pgsql_create_index, lower_pgsql_create_table, lower_pgsql_create_view, lower_pgsql_ddl,
	lower_pgsql_drop, map_mysql_type, map_pgsql_type,
};
pub use plan::{lower_bound_select, lower_bound_subquery};
pub use write::{
	lower_mysql_delete, lower_mysql_expr, lower_mysql_insert, lower_mysql_replace,
	lower_mysql_truncate, lower_mysql_update, lower_pgsql_delete, lower_pgsql_expr,
	lower_pgsql_insert, lower_pgsql_truncate, lower_pgsql_update,
};

#[cfg(test)]
mod ddl_tests;
#[cfg(test)]
mod id_key_tests;
#[cfg(test)]
mod json_tests;
#[cfg(test)]
mod link_expr_tests;
#[cfg(test)]
mod link_tests;
#[cfg(test)]
mod link_write_tests;
#[cfg(test)]
mod scalar_subquery_tests;
#[cfg(test)]
mod write_tests;
