//! Ceilings and sizing for Surrealism WASM modules.
//!
//! Most of these knobs are the server's half of a module's resource budget:
//! where a module also declares a limit in its `.surli` config, the effective
//! limit is the smaller of the two, so a value here can only tighten what a
//! module asked for, never widen it. The rest size the datastore-wide cache of
//! compiled modules, select the tracing level module output is recorded at,
//! and name the host a `silo::` executable resolves against. This layer owns
//! them because it is where a module is fetched, unpacked, compiled, pooled
//! and invoked.
//!
//! The memory, execution-time, KV and pool ceilings are read while a module is
//! compiled into a cached runtime, so a module already in the cache keeps the
//! ceilings it was built with until it is evicted.

use surrealdb_cnf as cnf;

/// Server-side limits applied to Surrealism WASM modules.
#[derive(Clone, Debug)]
pub(crate) struct SurrealismConfig {
	/// Specifies the number of surrealism modules which can be cached across transactions
	/// (default: 100)
	pub surrealism_cache_size: usize,
	/// Per-module WASM linear memory ceiling in bytes (default: none / unlimited).
	/// When set, each WASM store is limited via `StoreLimits`. Effective limit is
	/// `min(this, module_config.max_memory_bytes)` when both are set.
	pub surrealism_max_memory: Option<usize>,
	/// Per-invocation execution time ceiling in milliseconds for Surrealism WASM modules
	/// (default: none / unlimited). Combined with module config and query context timeout
	/// via `min()` to produce the effective deadline.
	pub surrealism_max_execution_time: Option<u64>,
	/// Per-module KV store entry count ceiling for Surrealism WASM modules (default: none /
	/// unlimited). Effective limit is `min(this, module_config.max_kv_entries)` when both are
	/// set.
	pub surrealism_max_kv_entries: Option<usize>,
	/// Per-module KV store maximum value size in bytes for Surrealism WASM modules
	/// (default: none / unlimited). Effective limit is
	/// `min(this, module_config.max_kv_value_bytes)` when both are set.
	pub surrealism_max_kv_value_bytes: Option<usize>,
	/// Maximum aggregate size in bytes for attached filesystem entries in `.surli` archives
	/// (default: 100 MiB). Applied when unpacking module archives during `DEFINE MODULE` or
	/// eager loading.
	pub surrealism_max_fs_bytes: u64,
	/// Per-module controller pool size ceiling for Surrealism WASM modules (default: 8).
	/// Each pooled controller holds an instantiated WASM store. Effective pool size is
	/// `min(this, module_config.max_pool_size.unwrap_or(this))`.
	///
	/// Not settable through the config map: the `Config::parse` implementation
	/// below has no line for this key, so it always holds its default.
	pub surrealism_max_pool_size: usize,
	/// The tracing level a module's standard output is recorded at, one of `trace`,
	/// `info`, `warn` or `error`; any other value records at `debug` (default:
	/// "debug"). A module's standard error is always recorded at `warn`,
	/// independently of this.
	pub surrealism_log_level: String,
	/// Base URL that a `silo::` module executable is resolved against. A
	/// package is fetched from
	/// `{endpoint}/{organisation}/{package}/{major}.{minor}.{patch}.surli`, so a trailing slash
	/// here is redundant and is stripped before the path is appended. Point this at a mirror to
	/// serve packages from elsewhere.
	pub surrealism_silo_endpoint: String,
}

impl Default for SurrealismConfig {
	fn default() -> Self {
		Self {
			surrealism_cache_size: 100,
			surrealism_max_memory: None,
			surrealism_max_execution_time: None,
			surrealism_max_kv_entries: None,
			surrealism_max_kv_value_bytes: None,
			surrealism_max_fs_bytes: 100 * 1024 * 1024,
			surrealism_max_pool_size: 8,
			surrealism_log_level: "debug".to_string(),
			surrealism_silo_endpoint: "https://silo.surrealdb.com".to_string(),
		}
	}
}

impl cnf::Config for SurrealismConfig {
	fn parse(&mut self, map: &cnf::ConfigMap) {
		map.parse_key("surrealism_cache_size", &mut self.surrealism_cache_size)
			.parse_key_option("surrealism_max_memory", &mut self.surrealism_max_memory)
			.parse_key_option(
				"surrealism_max_execution_time",
				&mut self.surrealism_max_execution_time,
			)
			.parse_key_option("surrealism_max_kv_entries", &mut self.surrealism_max_kv_entries)
			.parse_key_option(
				"surrealism_max_kv_value_bytes",
				&mut self.surrealism_max_kv_value_bytes,
			)
			.parse_key("surrealism_max_fs_bytes", &mut self.surrealism_max_fs_bytes)
			.parse_key_with("surrealism_log_level", &mut self.surrealism_log_level, |s| {
				Some(s.to_string())
			})
			.parse_key_with("surrealism_silo_endpoint", &mut self.surrealism_silo_endpoint, |s| {
				Some(s.trim_end_matches('/').to_string())
			});
	}
}
