use anyhow::Result;
use surrealdb_types::SurrealValue;
use surrealism_types::args::Args;

use crate::bindings::surrealism::plugin::host;

/// Convert a WIT `result<T, string>` error into an `anyhow::Error` with context.
fn host_err(op: &str, e: &str) -> anyhow::Error {
	anyhow::anyhow!("host {op}: {e}")
}

/// Execute a SurrealQL query.
pub fn sql<S, R>(sql: S) -> Result<R>
where
	S: Into<String>,
	R: SurrealValue,
{
	sql_with_vars(sql, surrealdb_types::Variables::new())
}

/// Execute a SurrealQL query with variables.
pub fn sql_with_vars<S, V, R>(sql: S, vars: V) -> Result<R>
where
	S: Into<String>,
	V: IntoIterator<Item = (String, surrealdb_types::Value)>,
	R: SurrealValue,
{
	let sql = sql.into();
	if sql.trim().is_empty() {
		anyhow::bail!("SQL query cannot be empty");
	}

	let vars_vec: Vec<(String, surrealdb_types::Value)> = vars.into_iter().collect();
	let vars_bytes = surrealdb_types::encode_string_key_values(&vars_vec)?;
	let result_bytes = host::sql(&sql, &vars_bytes).map_err(|e| host_err("sql", &e))?;
	let value: surrealdb_types::Value = surrealdb_types::decode(&result_bytes)?;
	Ok(R::from_value(value)?)
}

/// Call a SurrealDB function.
#[allow(clippy::needless_pass_by_value)] // Public guest SDK: ergonomic for plugin authors who often pass owned `String`.
pub fn run<F, A, R>(fnc: F, version: Option<String>, args: A) -> Result<R>
where
	F: Into<String>,
	A: Args,
	R: SurrealValue,
{
	let fnc = fnc.into();
	let args_bytes = surrealdb_types::encode_value_list(&args.to_values())?;
	let result_bytes =
		host::run(&fnc, version.as_deref(), &args_bytes).map_err(|e| host_err("run", &e))?;
	let value: surrealdb_types::Value = surrealdb_types::decode(&result_bytes)?;
	Ok(R::from_value(value)?)
}
