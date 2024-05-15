use std::collections::HashSet;
use std::{
	path::{Path, PathBuf},
	str::FromStr,
	time::Duration,
};

use surrealdb::dbs::capabilities::{FuncTarget, NetTarget, Targets};

pub(crate) mod parser;

pub(crate) fn path_valid(v: &str) -> Result<String, String> {
	match v {
		"memory" => Ok(v.to_string()),
		v if v.starts_with("file:") => Ok(v.to_string()),
		v if v.starts_with("rocksdb:") => Ok(v.to_string()),
		v if v.starts_with("speedb:") => Ok(v.to_string()),
		v if v.starts_with("surrealkv:") => Ok(v.to_string()),
		v if v.starts_with("tikv:") => Ok(v.to_string()),
		v if v.starts_with("fdb:") => Ok(v.to_string()),
		_ => Err(String::from("Provide a valid database path parameter")),
	}
}

pub(crate) fn path_exists(path: &str) -> Result<PathBuf, String> {
	let path = Path::new(path);
	if !*path.try_exists().as_ref().map_err(ToString::to_string)? {
		return Err(String::from("Ensure the path exists"));
	}
	Ok(path.to_owned())
}

pub(crate) fn file_exists(path: &str) -> Result<PathBuf, String> {
	let path = path_exists(path)?;
	if !path.is_file() {
		return Err(String::from("Ensure the path is a file"));
	}
	Ok(path)
}

#[cfg(all(
	feature = "sql2",
	any(
		feature = "kv-surrealkv",
		feature = "kv-rocksdb",
		feature = "kv-fdb",
		feature = "kv-tikv",
		feature = "kv-speedb"
	)
))]
pub(crate) fn dir_exists(path: &str) -> Result<PathBuf, String> {
	let path = path_exists(path)?;
	if !path.is_dir() {
		return Err(String::from("Ensure the path is a directory"));
	}
	Ok(path)
}

pub(crate) fn endpoint_valid(v: &str) -> Result<String, String> {
	fn split_endpoint(v: &str) -> (&str, &str) {
		match v {
			"memory" => ("mem", ""),
			v => match v.split_once("://") {
				Some(parts) => parts,
				None => v.split_once(':').unwrap_or_default(),
			},
		}
	}

	let scheme = split_endpoint(v).0;
	match scheme {
		"http" | "https" | "ws" | "wss" | "fdb" | "mem" | "rocksdb" | "speedb" | "surrealkv"
		| "file" | "tikv" => Ok(v.to_string()),
		_ => Err(String::from("Provide a valid database connection string")),
	}
}

pub(crate) fn key_valid(v: &str) -> Result<String, String> {
	match v.len() {
		16 => Ok(v.to_string()),
		24 => Ok(v.to_string()),
		32 => Ok(v.to_string()),
		_ => Err(String::from("Ensure your database encryption key is 16, 24, or 32 bytes long")),
	}
}

pub(crate) fn duration(v: &str) -> Result<Duration, String> {
	surrealdb::sql::Duration::from_str(v).map(|d| d.0).map_err(|_| String::from("invalid duration"))
}

pub(crate) fn net_targets(value: &str) -> Result<Targets<NetTarget>, String> {
	if ["*", ""].contains(&value) {
		return Ok(Targets::All);
	}

	let mut result = HashSet::new();

	for target in value.split(',').filter(|s| !s.is_empty()) {
		result.insert(NetTarget::from_str(target)?);
	}

	Ok(Targets::Some(result))
}

pub(crate) fn func_targets(value: &str) -> Result<Targets<FuncTarget>, String> {
	if ["*", ""].contains(&value) {
		return Ok(Targets::All);
	}

	let mut result = HashSet::new();

	for target in value.split(',').filter(|s| !s.is_empty()) {
		result.insert(FuncTarget::from_str(target)?);
	}

	Ok(Targets::Some(result))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_func_targets() {
		assert_eq!(func_targets("*").unwrap(), Targets::<FuncTarget>::All);
		assert_eq!(func_targets("").unwrap(), Targets::<FuncTarget>::All);
		assert_eq!(
			func_targets("foo").unwrap(),
			Targets::<FuncTarget>::Some(vec!["foo".parse().unwrap()].into_iter().collect())
		);
		assert_eq!(
			func_targets("foo,bar").unwrap(),
			Targets::<FuncTarget>::Some(
				vec!["foo".parse().unwrap(), "bar".parse().unwrap()].into_iter().collect()
			)
		);
	}

	#[test]
	fn test_net_targets() {
		assert_eq!(net_targets("*").unwrap(), Targets::<NetTarget>::All);
		assert_eq!(net_targets("").unwrap(), Targets::<NetTarget>::All);
		assert_eq!(
			net_targets("example.com").unwrap(),
			Targets::<NetTarget>::Some(vec!["example.com".parse().unwrap()].into_iter().collect())
		);
		assert_eq!(
			net_targets("127.0.0.1:80,[2001:db8::1]:443,2001:db8::1").unwrap(),
			Targets::<NetTarget>::Some(
				vec![
					"127.0.0.1:80".parse().unwrap(),
					"[2001:db8::1]:443".parse().unwrap(),
					"2001:db8::1".parse().unwrap()
				]
				.into_iter()
				.collect()
			)
		);

		assert!(net_targets("127777.0.0.1").is_err());
		assert!(net_targets("127.0.0.1,127777.0.0.1").is_err());
	}
}
