use anyhow::Result;
use surrealdb_types::SurrealValue;
use surrealism_types::args::Args;
use surrealism_types::serialize::{Serializable, Serialized};

use crate::p2_bindings::surrealism::plugin::host;

fn ser<T: Serializable>(val: T) -> Result<Vec<u8>> {
	Ok(val.serialize()?.0.to_vec())
}

fn deser<T: Serializable>(bytes: &[u8]) -> Result<T> {
	T::deserialize(Serialized(bytes.to_vec().into()))
}

pub fn sql<S, R>(sql: S) -> Result<R>
where
	S: Into<String>,
	R: SurrealValue,
{
	sql_with_vars(sql, surrealdb_types::Variables::new())
}

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

	let vars_bytes = ser(vars.into_iter().collect::<Vec<_>>())?;
	let result_bytes = host::sql(&sql, &vars_bytes).map_err(|e| anyhow::anyhow!("{e}"))?;
	let inner: anyhow::Result<surrealdb_types::Value> = deser(&result_bytes)?;
	Ok(R::from_value(inner?)?)
}

pub fn run<F, A, R>(fnc: F, version: Option<String>, args: A) -> Result<R>
where
	F: Into<String>,
	A: Args,
	R: SurrealValue,
{
	let fnc = fnc.into();
	let args_bytes = ser(args.to_values())?;
	let result_bytes =
		host::run(&fnc, version.as_deref(), &args_bytes).map_err(|e| anyhow::anyhow!("{e}"))?;
	let inner: anyhow::Result<surrealdb_types::Value> = deser(&result_bytes)?;
	Ok(R::from_value(inner?)?)
}

pub mod kv {
	use std::ops::RangeBounds;

	use anyhow::Result;
	use surrealdb_types::SurrealValue;
	use surrealism_types::serialize::SerializableRange;

	use super::{deser, host, ser};

	pub fn get<K: Into<String>, R: SurrealValue>(key: K) -> Result<Option<R>> {
		let result = host::kv_get(&key.into()).map_err(|e| anyhow::anyhow!("{e}"))?;
		match result {
			Some(bytes) => {
				let val: surrealdb_types::Value = deser(&bytes)?;
				Ok(Some(R::from_value(val)?))
			}
			None => Ok(None),
		}
	}

	pub fn set<K: Into<String>, V: SurrealValue>(key: K, value: V) -> Result<()> {
		let value_bytes = ser(value.into_value())?;
		host::kv_set(&key.into(), &value_bytes).map_err(|e| anyhow::anyhow!("{e}"))
	}

	pub fn del<K: Into<String>>(key: K) -> Result<()> {
		host::kv_del(&key.into()).map_err(|e| anyhow::anyhow!("{e}"))
	}

	pub fn exists<K: Into<String>>(key: K) -> Result<bool> {
		host::kv_exists(&key.into()).map_err(|e| anyhow::anyhow!("{e}"))
	}

	pub fn del_rng<R: RangeBounds<String>>(range: R) -> Result<()> {
		let range_bytes = ser(SerializableRange::from_range_bounds(range)?)?;
		host::kv_del_rng(&range_bytes).map_err(|e| anyhow::anyhow!("{e}"))
	}

	pub fn get_batch<K, I, R>(keys: I) -> Result<Vec<Option<R>>>
	where
		I: IntoIterator<Item = K>,
		K: Into<String>,
		R: SurrealValue,
	{
		let keys: Vec<String> = keys.into_iter().map(|k| k.into()).collect();
		let result_bytes = host::kv_get_batch(&keys).map_err(|e| anyhow::anyhow!("{e}"))?;
		let vals: Vec<Option<surrealdb_types::Value>> = deser(&result_bytes)?;
		vals.into_iter()
			.map(|opt| match opt {
				Some(v) => Ok(Some(R::from_value(v)?)),
				None => Ok(None),
			})
			.collect()
	}

	pub fn set_batch<K, V, I>(entries: I) -> Result<()>
	where
		I: IntoIterator<Item = (K, V)>,
		K: Into<String>,
		V: SurrealValue,
	{
		let entries: Vec<(String, surrealdb_types::Value)> =
			entries.into_iter().map(|(k, v)| (k.into(), v.into_value())).collect();
		let entries_bytes = ser(entries)?;
		host::kv_set_batch(&entries_bytes).map_err(|e| anyhow::anyhow!("{e}"))
	}

	pub fn del_batch<K, I>(keys: I) -> Result<()>
	where
		I: IntoIterator<Item = K>,
		K: Into<String>,
	{
		let keys: Vec<String> = keys.into_iter().map(|k| k.into()).collect();
		host::kv_del_batch(&keys).map_err(|e| anyhow::anyhow!("{e}"))
	}

	pub fn keys<R: RangeBounds<String>>(range: R) -> Result<Vec<String>> {
		let range_bytes = ser(SerializableRange::from_range_bounds(range)?)?;
		host::kv_keys(&range_bytes).map_err(|e| anyhow::anyhow!("{e}"))
	}

	pub fn values<R: RangeBounds<String>, T: SurrealValue>(range: R) -> Result<Vec<T>> {
		let range_bytes = ser(SerializableRange::from_range_bounds(range)?)?;
		let result_bytes = host::kv_values(&range_bytes).map_err(|e| anyhow::anyhow!("{e}"))?;
		let vals: Vec<surrealdb_types::Value> = deser(&result_bytes)?;
		vals.into_iter().map(|v| Ok(T::from_value(v)?)).collect()
	}

	pub fn entries<R: RangeBounds<String>, T: SurrealValue>(range: R) -> Result<Vec<(String, T)>> {
		let range_bytes = ser(SerializableRange::from_range_bounds(range)?)?;
		let result_bytes = host::kv_entries(&range_bytes).map_err(|e| anyhow::anyhow!("{e}"))?;
		let entries: Vec<(String, surrealdb_types::Value)> = deser(&result_bytes)?;
		entries.into_iter().map(|(k, v)| Ok((k, T::from_value(v)?))).collect()
	}

	pub fn count<R: RangeBounds<String>>(range: R) -> Result<u64> {
		let range_bytes = ser(SerializableRange::from_range_bounds(range)?)?;
		host::kv_count(&range_bytes).map_err(|e| anyhow::anyhow!("{e}"))
	}
}
