use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use surrealdb_rpc::capabilities::{
	ArbitraryQueryTarget, EvalQueryTarget, ExperimentalTarget, FuncTarget, McpToolTarget,
	MethodTarget, NetTarget, RouteTarget, Targets,
};
use surrealdb_rpc::export::{ExcludedTables, TableConfig};

pub(crate) mod parser;

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
	// Split the endpoint into scheme and path
	fn split_endpoint(v: &str) -> (&str, &str) {
		// Strip query parameters before extracting the scheme
		let without_query = v.split_once('?').map(|(s, _)| s).unwrap_or(v);
		// Split the endpoint into scheme and path
		match without_query {
			"memory" | "mem" => ("mem", ""),
			v => match v.split_once("://") {
				Some(parts) => parts,
				None => v.split_once(':').unwrap_or_default(),
			},
		}
	}
	// Validate the scheme. The removed `rocksdb` and `indxdb` schemes, and the `tikv` scheme
	// of the backend built from its own repository, stay valid at this layer so that
	// connecting names what replaces them.
	match split_endpoint(v).0 {
		"http" | "https" | "ws" | "wss" | "grpc" | "grpcs" | "mem" | "surrealkv" | "tikv"
		| "rocksdb" | "indxdb" => Ok(v.to_string()),
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

pub(crate) fn duration(v: &str) -> Result<std::time::Duration, String> {
	parse_common::duration(v).map_err(|e| format!("Invalid duration: {}", e.message))
}

pub(crate) fn net_targets(value: &str) -> Result<Targets<NetTarget>, String> {
	if ["*", ""].contains(&value) {
		return Ok(Targets::All);
	}

	let mut result = HashSet::new();

	for target in value.split(',').filter(|s| !s.is_empty()) {
		// `ml` named a route this build no longer serves. It is ignored with a warning,
		// so a configuration written for an earlier release still starts.
		if target == "ml" {
			eprintln!(
				"warning: the `ml` route target is retired and ignored: this build serves no ML routes"
			);
			continue;
		}
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

pub(crate) fn eval_query_targets(value: &str) -> Result<Targets<EvalQueryTarget>, String> {
	if ["*", ""].contains(&value) {
		return Ok(Targets::All);
	}

	let mut result = HashSet::new();

	for target in value.split(',').filter(|s| !s.is_empty()) {
		result.insert(EvalQueryTarget::from_str(target).map_err(|e| e.to_string())?);
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

pub(crate) fn mcp_tool_targets(value: &str) -> Result<Targets<McpToolTarget>, String> {
	if ["*", ""].contains(&value) {
		return Ok(Targets::All);
	}

	let mut result = HashSet::new();

	for target in value.split(',').filter(|s| !s.is_empty()) {
		result.insert(McpToolTarget::from_str(target).map_err(|e| e.to_string())?);
	}

	Ok(Targets::Some(result))
}

pub(crate) fn route_targets(value: &str) -> Result<Targets<RouteTarget>, String> {
	if ["*", ""].contains(&value) {
		return Ok(Targets::All);
	}

	let mut result = HashSet::new();

	for target in value.split(',').filter(|s| !s.is_empty()) {
		// `ml` named a route this build no longer serves. It is ignored with a warning,
		// so a configuration written for an earlier release still starts.
		if target == "ml" {
			eprintln!(
				"warning: the `ml` route target is retired and ignored: this build serves no ML routes"
			);
			continue;
		}
		result.insert(RouteTarget::from_str(target).map_err(|e| e.to_string())?);
	}

	Ok(Targets::Some(result))
}

pub(crate) fn cors_origin(value: &str) -> Result<String, String> {
	value
		.parse::<http::HeaderValue>()
		.map(|_| value.to_string())
		.map_err(|_| format!("Invalid CORS origin '{value}': must be a valid HTTP header value"))
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

pub(crate) fn export_tables_exclude(value: &str) -> Result<TableConfig, String> {
	if value.is_empty() {
		return Err(String::from("Provide at least one table name to exclude"));
	}

	Ok(TableConfig::Exclude(ExcludedTables {
		exclude: value.split(",").filter(|s| !s.is_empty()).map(str::to_string).collect(),
	}))
}

/// Parse `--planner-strategy` / `SURREAL_PLANNER_STRATEGY`.
///
/// The option selects nothing: the engine has a single execution path. The
/// three names it has ever accepted stay accepted so that an existing
/// deployment's configuration keeps starting the server; any other value is
/// refused.
pub(crate) fn retired_planner_strategy(v: &str) -> Result<String, String> {
	match v {
		"best-effort" | "compute-only" | "all-read-only" => Ok(v.to_owned()),
		_ => Err(format!(
			"unknown planner strategy: '{v}' (expected 'best-effort', 'compute-only', or 'all-read-only')"
		)),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_endpoint_valid_passes_removed_schemes_to_their_message() {
		assert!(endpoint_valid("rocksdb:/data").is_ok());
		assert!(endpoint_valid("indxdb://name").is_ok());
		assert!(endpoint_valid("surrealkv://data").is_ok());
		assert!(endpoint_valid("nothing://data").is_err());
	}

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
	fn test_mcp_tool_targets() {
		assert_eq!(mcp_tool_targets("*").unwrap(), Targets::<McpToolTarget>::All);
		assert_eq!(mcp_tool_targets("").unwrap(), Targets::<McpToolTarget>::All);
		assert_eq!(
			mcp_tool_targets("graphql").unwrap(),
			Targets::<McpToolTarget>::Some(vec!["graphql".parse().unwrap()].into_iter().collect())
		);
		assert!(mcp_tool_targets("not_a_tool").is_err());
	}

	#[test]
	fn retired_ml_route_target_is_accepted_and_selects_nothing() {
		assert_eq!(route_targets("ml").unwrap(), Targets::<RouteTarget>::Some(HashSet::new()));
		assert_eq!(route_targets("ml,key").unwrap(), route_targets("key").unwrap());
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
			route_targets("postgres,mysql").unwrap(),
			Targets::<RouteTarget>::Some(
				vec![RouteTarget::Postgres, RouteTarget::MySql].into_iter().collect()
			)
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

	#[test]
	fn test_eval_query_targets() {
		assert_eq!(eval_query_targets("*").unwrap(), Targets::<EvalQueryTarget>::All);
		assert_eq!(eval_query_targets("").unwrap(), Targets::<EvalQueryTarget>::All);
		assert_eq!(
			eval_query_targets("system").unwrap(),
			Targets::<EvalQueryTarget>::Some(vec![EvalQueryTarget::System].into_iter().collect())
		);
		assert_eq!(
			eval_query_targets("guest,record,system").unwrap(),
			Targets::<EvalQueryTarget>::Some(
				vec![EvalQueryTarget::Guest, EvalQueryTarget::Record, EvalQueryTarget::System]
					.into_iter()
					.collect()
			)
		);
		assert!(eval_query_targets("nonsense").is_err());
	}

	#[test]
	fn every_retired_strategy_name_is_still_accepted() {
		for name in ["best-effort", "compute-only", "all-read-only"] {
			assert_eq!(retired_planner_strategy(name).unwrap(), name);
		}
	}

	#[test]
	fn files_is_not_an_experimental_target() {
		assert!(experimental_targets("files").is_err());
		assert!(experimental_targets("surrealism,files").is_err());
		assert!(experimental_targets("surrealism").is_ok());
	}

	#[test]
	fn an_unknown_strategy_is_refused() {
		assert!(retired_planner_strategy("nonsense").is_err());
	}
}
