//! The tables of MySQL system variables the planner reads, which `surrealdb_mysql` owns so that
//! code outside the planner reads the same ones.
//!
//! The binder refuses a reference to a variable MySQL does not have, and the lowering answers
//! `@@name`, `@@session.name`, `@@local.name` and `@@global.name` with the same value: the value
//! the server models for the variable, or NULL for a variable MySQL has and the server does not
//! model. The value of a variable is also the type of its result column.

pub use surrealdb_mysql::system_vars::{
	MYSQL_SERVER_VERSION, SystemVariable, SystemVariableError, check_mysql_system_variable,
	mysql_system_variable,
};

#[cfg(test)]
mod tests;
