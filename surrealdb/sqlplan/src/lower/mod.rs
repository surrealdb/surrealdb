//! Lowering from Bound IR to SurrealDB `LogicalOp`.

pub mod ddl;
pub mod ddl_truncate;
pub(crate) mod enum_rank;
pub mod expr;
pub(crate) mod family_cast;
mod foreign_key;
mod functions;
pub(crate) mod id_key;
pub(crate) mod link;
pub mod network_rank;
pub mod plan;
mod quote_ident;
mod sql_like;
mod temporal;
mod vector_ddl;
mod vector_expr;
mod vector_knn;
mod window;
pub mod write;
mod write_function;
mod write_null;

pub use ddl::{
	lower_mysql_create_table, lower_mysql_ddl, lower_mysql_drop_table, lower_pgsql_alter_table,
	lower_pgsql_create_index, lower_pgsql_create_table, lower_pgsql_create_view, lower_pgsql_ddl,
	lower_pgsql_drop, map_mysql_type, map_pgsql_type,
};
pub use ddl_truncate::{lower_mysql_truncate, lower_pgsql_truncate};
/// Whether `type_name` is a cast target that is a vector: see [`vector_ddl::cast_target`].
pub(crate) fn vector_cast_target(
	type_name: &surrealdb_pgsql::ast::expr::TypeName,
) -> Result<Option<Option<usize>>, crate::error::PlanError> {
	vector_ddl::cast_target(type_name)
}

pub use plan::{lower_bound_select, lower_bound_subquery};
pub use write::{
	lower_mysql_delete, lower_mysql_expr, lower_mysql_insert, lower_mysql_replace,
	lower_mysql_update, lower_pgsql_delete, lower_pgsql_expr, lower_pgsql_insert,
	lower_pgsql_update,
};

#[cfg(test)]
mod all_columns_tests;
#[cfg(test)]
mod ddl_tests;
#[cfg(test)]
mod generated_key_tests;
#[cfg(test)]
mod id_key_tests;
#[cfg(test)]
mod json_tests;
#[cfg(test)]
mod link_expr_tests;
#[cfg(test)]
mod link_join_columns_tests;
#[cfg(test)]
mod link_tests;
#[cfg(test)]
mod link_write_tests;
#[cfg(test)]
mod nested_correlation_tests;
#[cfg(test)]
mod null_join_keys_tests;
#[cfg(test)]
mod omitted_column_tests;
#[cfg(test)]
mod replace_reset_tests;
#[cfg(test)]
mod scalar_subquery_tests;
#[cfg(test)]
mod vector_tests;
#[cfg(test)]
mod window_tests;
#[cfg(test)]
mod write_coercion_tests;
#[cfg(test)]
mod write_tests;
