use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use crate::core::dbs::capabilities::{
	ArbitraryQueryTarget, ExperimentalTarget, FuncTarget, MethodTarget, NetTarget, RouteTarget,
	Targets,
};
use crate::core::kvs::export::TableConfig;
use crate::core::val;

pub(crate) mod parser;

pub(crate) fn path_valid(v: &str) -> Result<String, String> {
	match v {
		"memory" => Ok(v.to_string()),
		v if v.starts_with("file:") => Ok(v.to_string()),
		v if v.starts_with("rocksdb:") => Ok(v.to_string()),
		v if v.starts_with("surrealkv:") => Ok(v.to_string()),
		v if v.starts_with("surrealkv+versioned:") => Ok(v.to_string()),
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
		"http"
		| "https"
		| "ws"
		| "wss"
		| "fdb"
		| "mem"
		| "rocksdb"
		| "surrealkv"
		| "surrealkv+versioned"
		| "file"
		| "tikv" => Ok(v.to_string()),
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
	val::Duration::from_str(v).map(|d| d.0).map_err(|_| String::from("invalid duration"))
}

pub(crate) fn net_targets(value: &str) -> Result<Targets<NetTarget>, String> {
	if ["*", ""].contains(&value) {
		return Ok(Targets::All);
	}

	let mut result = HashSet::new();

	for target in value.split(',').filter(|s| !s.is_empty()) {
		result.insert(NetTarget::from_str(target).map_err(|e| e.to_string())?);
	}

	Ok(Targets::Some(result))
}

pub(crate) fn func_targets(value: &str) -> Result<Targets<FuncTarget>, String> {
	if ["*", ""].contains(&value) {
		return Ok(Targets::All);
	}

	let mut result = HashSet::new();

	for target in value.split(',').filter(|s| !s.is_empty()) {
		result.insert(FuncTarget::from_str(target).map_err(|e| e.to_string())?);
	}

	Ok(Targets::Some(result))
}

pub(crate) fn experimental_targets(value: &str) -> Result<Targets<ExperimentalTarget>, String> {
	if ["*", ""].contains(&value) {
		return Ok(Targets::All);
	}

	let mut result = HashSet::new();

	for target in value.split(',').filter(|s| !s.is_empty()) {
		result.insert(ExperimentalTarget::from_str(target).map_err(|e| e.to_string())?);
	}

	Ok(Targets::Some(result))
}

pub(crate) fn query_arbitrary_targets(
	value: &str,
) -> Result<Targets<ArbitraryQueryTarget>, String> {
	if ["*", ""].contains(&value) {
		return Ok(Targets::All);
	}

	let mut result = HashSet::new();

	for target in value.split(',').filter(|s| !s.is_empty()) {
		result.insert(ArbitraryQueryTarget::from_str(target).map_err(|e| e.to_string())?);
	}

	Ok(Targets::Some(result))
}

pub(crate) fn method_targets(value: &str) -> Result<Targets<MethodTarget>, String> {
	if ["*", ""].contains(&value) {
		return Ok(Targets::All);
	}

	let mut result = HashSet::new();

	for target in value.split(',').filter(|s| !s.is_empty()) {
		result.insert(MethodTarget::from_str(target).map_err(|e| e.to_string())?);
	}

	Ok(Targets::Some(result))
}

pub(crate) fn route_targets(value: &str) -> Result<Targets<RouteTarget>, String> {
	if ["*", ""].contains(&value) {
		return Ok(Targets::All);
	}

	let mut result = HashSet::new();

	for target in value.split(',').filter(|s| !s.is_empty()) {
		result.insert(RouteTarget::from_str(target).map_err(|e| e.to_string())?);
	}

	Ok(Targets::Some(result))
}

pub(crate) fn export_tables(value: &str) -> Result<TableConfig, String> {
	if ["*", "", "true"].contains(&value) {
		return Ok(TableConfig::All);
	}

	if value == "false" {
		return Ok(TableConfig::None);
	}

	Ok(TableConfig::Some(value.split(",").filter(|s| !s.is_empty()).map(str::to_string).collect()))
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

	#[test]
	fn test_method_targets() {
		assert_eq!(method_targets("*").unwrap(), Targets::<MethodTarget>::All);
		assert_eq!(method_targets("").unwrap(), Targets::<MethodTarget>::All);
		assert_eq!(
			method_targets("query").unwrap(),
			Targets::<MethodTarget>::Some(vec!["query".parse().unwrap()].into_iter().collect())
		);
		assert_eq!(
			method_targets("query,authenticate").unwrap(),
			Targets::<MethodTarget>::Some(
				vec!["query".parse().unwrap(), "authenticate".parse().unwrap()]
					.into_iter()
					.collect()
			)
		);
	}

	#[test]
	fn test_route_targets() {
		assert_eq!(route_targets("*").unwrap(), Targets::<RouteTarget>::All);
		assert_eq!(route_targets("").unwrap(), Targets::<RouteTarget>::All);
		assert_eq!(
			route_targets("key").unwrap(),
			Targets::<RouteTarget>::Some(vec!["key".parse().unwrap()].into_iter().collect())
		);
		assert_eq!(
			route_targets("key,sql").unwrap(),
			Targets::<RouteTarget>::Some(
				vec!["key".parse().unwrap(), "sql".parse().unwrap()].into_iter().collect()
			)
		);
	}

	#[test]
	fn test_arbitrary_query_targets() {
		assert_eq!(query_arbitrary_targets("*").unwrap(), Targets::<ArbitraryQueryTarget>::All);
		assert_eq!(query_arbitrary_targets("").unwrap(), Targets::<ArbitraryQueryTarget>::All);
		assert_eq!(
			query_arbitrary_targets("guest").unwrap(),
			Targets::<ArbitraryQueryTarget>::Some(
				vec![ArbitraryQueryTarget::Guest].into_iter().collect()
			)
		);
		assert_eq!(
			query_arbitrary_targets("guest,system").unwrap(),
			Targets::<ArbitraryQueryTarget>::Some(
				vec![ArbitraryQueryTarget::Guest, ArbitraryQueryTarget::System]
					.into_iter()
					.collect()
			)
		);
		assert_eq!(
			query_arbitrary_targets("guest,record,system").unwrap(),
			Targets::<ArbitraryQueryTarget>::Some(
				vec![
					ArbitraryQueryTarget::Guest,
					ArbitraryQueryTarget::Record,
					ArbitraryQueryTarget::System
				]
				.into_iter()
				.collect()
			)
		);
	}
}
