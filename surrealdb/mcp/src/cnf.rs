//! Configuration for the MCP server.
//!
//! Mirrors the `Config` / `ConfigMap` pattern used by
//! [`surrealdb_core::cnf`]: every knob is a regular field on [`McpConfig`]
//! with a default, and an instance is loaded once from a [`ConfigMap`] at
//! service-construction time. The loaded `Arc<McpConfig>` is then plumbed
//! through [`crate::service::McpService`] and [`crate::session::McpSession`]
//! so handlers read their caps off the running configuration rather than
//! a process-wide global.
//!
//! Environment variables prefixed with `SURREAL_MCP_` are picked up by
//! [`McpConfig::from_env`]; the post-strip key (lowercased) maps directly
//! to a field name -- e.g. `SURREAL_MCP_RUN_MAX_ARGS` -> `run_max_args`.
//! Embedders that don't want env-var auto-loading can build a config via
//! [`ConfigMap`] directly and pass it through
//! [`crate::service::McpServiceConfig::with_config`].

use std::sync::Arc;
use std::time::Duration;

use surrealdb_core::cnf::{Config, ConfigMap};

/// Default outer timeout, in seconds, for a single MCP `execute` call.
///
/// The datastore itself already honours per-query `TIMEOUT` and capability
/// limits; we add an outer bound so a runaway statement can never keep an
/// MCP session occupied indefinitely. 60 s comfortably covers routine
/// analytical queries while still giving us a hard ceiling.
const DEFAULT_QUERY_TIMEOUT_SECS: u64 = 60;

/// Default cap, in bytes, on the size of a single serialised tool-result
/// payload.
///
/// Structured JSON larger than this is truncated before being embedded in
/// the `structured_content` / `content` blocks so a pathological query
/// (e.g. `SELECT * FROM huge_table`) cannot blow up the LLM context window
/// or the client's memory. 256 KiB comfortably fits typical results while
/// keeping worst-case payloads bounded.
const DEFAULT_MAX_RESULT_BYTES: usize = 256 * 1024;

/// Default cap on the number of arguments a single `run` tool invocation
/// may pass to a SurrealQL function. Each arg becomes a `Variables`
/// binding and a `$_aN` placeholder; an unbounded array would let a
/// client force linear allocation per call.
const DEFAULT_RUN_MAX_ARGS: usize = 64;

/// Default cap on the number of top-level keys in a `parameters` /
/// `*_data` JSON object passed to a tool. Bounds the work `Variables`
/// construction does for any one call.
///
/// Nesting depth is intentionally not capped at the MCP layer because
/// `serde_json` already enforces a hard 128-level recursion limit
/// during deserialization (`unbounded_depth` is not enabled in this
/// workspace). Adding a stricter cap on top of that walks every value
/// for no security benefit.
const DEFAULT_PARAMS_MAX_KEYS: usize = 256;

/// Default cap on the number of tables that the `surrealdb://schema/...`
/// database-level resource will enrich with per-table fields/indexes/
/// events. Tables beyond the cap keep their bare `DEFINE TABLE` string
/// in the response, and the body carries a `tables_truncated_at` marker
/// so the client can issue a follow-up per-table fetch if needed.
///
/// 200 covers typical application schemas comfortably while keeping
/// worst-case enrichment to 200 round-trips through the datastore.
const DEFAULT_SCHEMA_RESOURCE_MAX_TABLES: usize = 200;

/// Default cap, in bytes, on the body of a single `$ql` SurrealQL
/// pass-through string accepted inside a tool's JSON `*_data` payload.
///
/// Each `$ql` body is parsed by `surrealdb_core::syn::value_legacy_strand`,
/// which honours the parser's per-call object/query depth limits but is
/// still O(n) in the body length. A 4 KiB ceiling comfortably covers the
/// intended use-case (literal decimals, datetimes, durations, record
/// ids, uuids) while preventing a malicious caller from forcing
/// unbounded parser work via a single deeply-nested literal. The
/// validator's depth caps remain the primary defence; this is a cheap
/// secondary bound that fires before the parser is invoked.
const DEFAULT_PARAMS_MAX_QL_BYTES: usize = 4 * 1024;

/// Runtime configuration for the MCP server.
///
/// Loaded once at service construction (typically from the
/// `SURREAL_MCP_*` environment via [`Self::from_env`]) and shared through
/// an `Arc` for the lifetime of the service. Every cap that used to live
/// behind a `LazyLock` env-static now lives here so handlers can read
/// them off the session and tests can override them without touching
/// process-global state.
#[derive(Clone, Debug)]
pub struct McpConfig {
	/// Outer timeout applied to every `McpSession::execute`. `None`
	/// disables the outer bound, in which case the datastore's own
	/// timeouts are the only protection.
	///
	/// Configured via `SURREAL_MCP_QUERY_TIMEOUT_SECS`; a value of `0`
	/// disables the outer bound.
	pub query_timeout: Option<Duration>,
	/// Maximum serialised tool-result size, in bytes, before the payload
	/// is replaced with a truncation marker. `None` disables the cap.
	///
	/// Configured via `SURREAL_MCP_MAX_RESULT_BYTES`; a value of `0`
	/// disables the cap.
	pub max_result_bytes: Option<usize>,
	/// Maximum number of arguments accepted by the `run` tool in a
	/// single invocation.
	///
	/// Configured via `SURREAL_MCP_RUN_MAX_ARGS`; values <= 0 fall back
	/// to the default. There is no way to disable the cap because every
	/// arg has O(N) cost on the hot path.
	pub run_max_args: usize,
	/// Maximum number of top-level keys accepted in a tool's JSON
	/// `parameters` / `*_data` payload.
	///
	/// Configured via `SURREAL_MCP_PARAMS_MAX_KEYS`; values <= 0 fall
	/// back to the default.
	pub params_max_keys: usize,
	/// Maximum byte length of a single `$ql` SurrealQL pass-through
	/// string accepted inside a tool's JSON `*_data` payload.
	///
	/// Configured via `SURREAL_MCP_PARAMS_MAX_QL_BYTES`; values <= 0
	/// fall back to the default. There is no way to disable the cap:
	/// the parser is O(n) in the body length and unbounded input is a
	/// DoS vector.
	pub params_max_ql_bytes: usize,
	/// Maximum number of tables enriched with per-table fields /
	/// indexes / events by the database-level schema resource.
	///
	/// Configured via `SURREAL_MCP_SCHEMA_RESOURCE_MAX_TABLES`; values
	/// <= 0 fall back to the default. There is no way to disable the
	/// cap: enrichment is N round-trips through the datastore and
	/// unbounded N is a DoS vector on databases with many tables.
	pub schema_resource_max_tables: usize,
}

impl Default for McpConfig {
	fn default() -> Self {
		Self {
			query_timeout: Some(Duration::from_secs(DEFAULT_QUERY_TIMEOUT_SECS)),
			max_result_bytes: Some(DEFAULT_MAX_RESULT_BYTES),
			run_max_args: DEFAULT_RUN_MAX_ARGS,
			params_max_keys: DEFAULT_PARAMS_MAX_KEYS,
			params_max_ql_bytes: DEFAULT_PARAMS_MAX_QL_BYTES,
			schema_resource_max_tables: DEFAULT_SCHEMA_RESOURCE_MAX_TABLES,
		}
	}
}

impl Config for McpConfig {
	fn parse(&mut self, map: &ConfigMap) {
		map.parse_key_with("query_timeout_secs", &mut self.query_timeout, |x| {
			// `0` is a sentinel meaning "disable the bound"; any other
			// non-numeric value is ignored (returns `None` from the
			// closure), preserving the previous default-on-bad-input
			// behaviour of the LazyLock-backed implementation.
			x.parse::<u64>().ok().map(|s| {
				if s == 0 {
					None
				} else {
					Some(Duration::from_secs(s))
				}
			})
		})
		.parse_key_with("max_result_bytes", &mut self.max_result_bytes, |x| {
			x.parse::<usize>().ok().map(|b| {
				if b == 0 {
					None
				} else {
					Some(b)
				}
			})
		})
		.parse_key_with("run_max_args", &mut self.run_max_args, positive_usize)
		.parse_key_with("params_max_keys", &mut self.params_max_keys, positive_usize)
		.parse_key_with("params_max_ql_bytes", &mut self.params_max_ql_bytes, positive_usize)
		.parse_key_with(
			"schema_resource_max_tables",
			&mut self.schema_resource_max_tables,
			positive_usize,
		);
	}
}

impl McpConfig {
	/// Load an `McpConfig` from the `SURREAL_MCP_*` environment.
	pub fn from_env() -> Arc<Self> {
		Arc::new(ConfigMap::from_env_prefix("SURREAL_MCP_").load::<Self>())
	}

	/// Load an `McpConfig` from an explicit [`ConfigMap`].
	///
	/// Embedders that build their own config map (e.g. from a TOML file)
	/// should use this instead of [`Self::from_env`]; the keys it consumes
	/// are the same lowercased names as the env-stripped form
	/// (`query_timeout_secs`, `run_max_args`, ...).
	pub fn from_map(map: &ConfigMap) -> Arc<Self> {
		Arc::new(map.load::<Self>())
	}
}

/// Parse a strictly positive `usize` from a string, returning `None` for
/// `0`, negative numbers, or non-numeric input. Used by every
/// "<=0 falls back to default" knob to preserve the previous
/// LazyLock-backed semantics.
fn positive_usize(x: &str) -> Option<usize> {
	x.parse::<usize>().ok().filter(|&n| n > 0)
}

#[cfg(test)]
mod tests {
	use surrealdb_core::cnf::ConfigMap;

	use super::*;

	#[test]
	fn defaults_match_documented_values() {
		let cfg = McpConfig::default();
		assert_eq!(cfg.query_timeout, Some(Duration::from_secs(DEFAULT_QUERY_TIMEOUT_SECS)));
		assert_eq!(cfg.max_result_bytes, Some(DEFAULT_MAX_RESULT_BYTES));
		assert_eq!(cfg.run_max_args, DEFAULT_RUN_MAX_ARGS);
		assert_eq!(cfg.params_max_keys, DEFAULT_PARAMS_MAX_KEYS);
		assert_eq!(cfg.params_max_ql_bytes, DEFAULT_PARAMS_MAX_QL_BYTES);
		assert_eq!(cfg.schema_resource_max_tables, DEFAULT_SCHEMA_RESOURCE_MAX_TABLES);
	}

	#[test]
	fn parses_overrides_from_config_string() {
		let map = ConfigMap::from_config_string(
			"query_timeout_secs=10&max_result_bytes=2048&run_max_args=8\
			 &params_max_keys=16&params_max_ql_bytes=128\
			 &schema_resource_max_tables=4",
		);
		let cfg: McpConfig = map.load();
		assert_eq!(cfg.query_timeout, Some(Duration::from_secs(10)));
		assert_eq!(cfg.max_result_bytes, Some(2048));
		assert_eq!(cfg.run_max_args, 8);
		assert_eq!(cfg.params_max_keys, 16);
		assert_eq!(cfg.params_max_ql_bytes, 128);
		assert_eq!(cfg.schema_resource_max_tables, 4);
	}

	#[test]
	fn zero_disables_query_timeout_and_result_cap() {
		let map = ConfigMap::from_config_string("query_timeout_secs=0&max_result_bytes=0");
		let cfg: McpConfig = map.load();
		assert_eq!(cfg.query_timeout, None);
		assert_eq!(cfg.max_result_bytes, None);
	}

	#[test]
	fn zero_or_invalid_falls_back_to_default_for_positive_caps() {
		let map = ConfigMap::from_config_string(
			"run_max_args=0&params_max_keys=not_a_number\
			 &params_max_ql_bytes=0&schema_resource_max_tables=0",
		);
		let cfg: McpConfig = map.load();
		assert_eq!(cfg.run_max_args, DEFAULT_RUN_MAX_ARGS);
		assert_eq!(cfg.params_max_keys, DEFAULT_PARAMS_MAX_KEYS);
		assert_eq!(cfg.params_max_ql_bytes, DEFAULT_PARAMS_MAX_QL_BYTES);
		assert_eq!(cfg.schema_resource_max_tables, DEFAULT_SCHEMA_RESOURCE_MAX_TABLES);
	}
}
