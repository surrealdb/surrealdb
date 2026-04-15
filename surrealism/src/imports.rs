use anyhow::Result;
use surrealdb_types::SurrealValue;
use surrealism_types::args::Args;

use crate::bindings::surrealism::plugin::host;

/// Convert a WIT `result<T, string>` error into an `anyhow::Error` with context.
fn host_err(op: &str, e: String) -> anyhow::Error {
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
	let result_bytes = host::sql(&sql, &vars_bytes).map_err(|e| host_err("sql", e))?;
	let value: surrealdb_types::Value = surrealdb_types::decode(&result_bytes)?;
	Ok(R::from_value(value)?)
}

/// Call a SurrealDB function.
pub fn run<F, A, R>(fnc: F, version: Option<String>, args: A) -> Result<R>
where
	F: Into<String>,
	A: Args,
	R: SurrealValue,
{
	let fnc = fnc.into();
	let args_bytes = surrealdb_types::encode_value_list(&args.to_values())?;
	let result_bytes =
		host::run(&fnc, version.as_deref(), &args_bytes).map_err(|e| host_err("run", e))?;
	let value: surrealdb_types::Value = surrealdb_types::decode(&result_bytes)?;
	Ok(R::from_value(value)?)
}

pub mod kv {
	use std::ops::RangeBounds;

	use anyhow::Result;
	use surrealdb_types::SurrealValue;

	use super::{host, host_err};

	/// Get a value from the KV store.
	pub fn get<K: Into<String>, R: SurrealValue>(key: K) -> Result<Option<R>> {
		let result = host::kv_get(&key.into()).map_err(|e| host_err("kv::get", e))?;
		match result {
			Some(bytes) => {
				let val: surrealdb_types::Value = surrealdb_types::decode(&bytes)?;
				Ok(Some(R::from_value(val)?))
			}
			None => Ok(None),
		}
	}

	/// Set a value in the KV store.
	pub fn set<K: Into<String>, V: SurrealValue>(key: K, value: V) -> Result<()> {
		let value_bytes = surrealdb_types::encode(&value.into_value())?;
		host::kv_set(&key.into(), &value_bytes).map_err(|e| host_err("kv::set", e))
	}

	/// Delete a key from the KV store.
	pub fn del<K: Into<String>>(key: K) -> Result<()> {
		host::kv_del(&key.into()).map_err(|e| host_err("kv::del", e))
	}

	/// Check if a key exists in the KV store.
	pub fn exists<K: Into<String>>(key: K) -> Result<bool> {
		host::kv_exists(&key.into()).map_err(|e| host_err("kv::exists", e))
	}

	fn encode_range<R: RangeBounds<String>>(range: R) -> Result<Vec<u8>> {
		let start = range.start_bound().cloned();
		let end = range.end_bound().cloned();
		surrealdb_types::encode_string_range(&start, &end)
	}

	/// Delete a range of keys from the KV store.
	pub fn del_rng<R: RangeBounds<String>>(range: R) -> Result<()> {
		let range_bytes = encode_range(range)?;
		host::kv_del_rng(&range_bytes).map_err(|e| host_err("kv::del_rng", e))
	}

	/// Get multiple values from the KV store.
	pub fn get_batch<K, I, R>(keys: I) -> Result<Vec<Option<R>>>
	where
		I: IntoIterator<Item = K>,
		K: Into<String>,
		R: SurrealValue,
	{
		let keys: Vec<String> = keys.into_iter().map(|k| k.into()).collect();
		let result_bytes = host::kv_get_batch(&keys).map_err(|e| host_err("kv::get_batch", e))?;
		let vals = surrealdb_types::decode_optional_values(&result_bytes)?;
		vals.into_iter()
			.map(|opt| match opt {
				Some(v) => Ok(Some(R::from_value(v)?)),
				None => Ok(None),
			})
			.collect()
	}

	/// Set multiple values in the KV store.
	pub fn set_batch<K, V, I>(entries: I) -> Result<()>
	where
		I: IntoIterator<Item = (K, V)>,
		K: Into<String>,
		V: SurrealValue,
	{
		let entries: Vec<(String, surrealdb_types::Value)> =
			entries.into_iter().map(|(k, v)| (k.into(), v.into_value())).collect();
		let entries_bytes = surrealdb_types::encode_string_key_values(&entries)?;
		host::kv_set_batch(&entries_bytes).map_err(|e| host_err("kv::set_batch", e))
	}

	/// Delete multiple keys from the KV store.
	pub fn del_batch<K, I>(keys: I) -> Result<()>
	where
		I: IntoIterator<Item = K>,
		K: Into<String>,
	{
		let keys: Vec<String> = keys.into_iter().map(|k| k.into()).collect();
		host::kv_del_batch(&keys).map_err(|e| host_err("kv::del_batch", e))
	}

	/// List keys in a range.
	pub fn keys<R: RangeBounds<String>>(range: R) -> Result<Vec<String>> {
		let range_bytes = encode_range(range)?;
		host::kv_keys(&range_bytes).map_err(|e| host_err("kv::keys", e))
	}

	/// List values in a range.
	pub fn values<R: RangeBounds<String>, T: SurrealValue>(range: R) -> Result<Vec<T>> {
		let range_bytes = encode_range(range)?;
		let result_bytes = host::kv_values(&range_bytes).map_err(|e| host_err("kv::values", e))?;
		let vals = surrealdb_types::decode_value_list(&result_bytes)?;
		vals.into_iter().map(|v| Ok(T::from_value(v)?)).collect()
	}

	/// List key-value pairs in a range.
	pub fn entries<R: RangeBounds<String>, T: SurrealValue>(range: R) -> Result<Vec<(String, T)>> {
		let range_bytes = encode_range(range)?;
		let result_bytes =
			host::kv_entries(&range_bytes).map_err(|e| host_err("kv::entries", e))?;
		let entries = surrealdb_types::decode_string_key_values(&result_bytes)?;
		entries.into_iter().map(|(k, v)| Ok((k, T::from_value(v)?))).collect()
	}

	/// Count entries in a range.
	pub fn count<R: RangeBounds<String>>(range: R) -> Result<u64> {
		let range_bytes = encode_range(range)?;
		host::kv_count(&range_bytes).map_err(|e| host_err("kv::count", e))
	}
}
