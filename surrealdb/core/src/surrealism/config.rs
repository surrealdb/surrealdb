//! Ceilings and sizing for Surrealism WASM modules.
//!
//! Most of these knobs are the server's half of an extension's resource
//! budget: where a definition's `CAPABILITIES` also set a limit, the effective
//! limit is the smaller of the two, so a value here can only tighten what a
//! definition grants, never widen it. The rest size the datastore-wide cache of
//! compiled modules, select the tracing level module output is recorded at,
//! and name the host a `silo::` executable resolves against. This layer owns
//! them because it is where a module is fetched, unpacked, compiled, pooled
//! and invoked.
//!
//! The memory and execution-time ceilings are read when an invocation's grant
//! is built, and grants are cached, so a grant already in the cache keeps the
//! ceilings it was built with until it is evicted.

use std::time::Duration;

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
	/// Maximum aggregate size in bytes for attached filesystem entries in `.surli` archives
	/// (default: 100 MiB). Applied when unpacking extension archives during `DEFINE EXTENSION` or
	/// eager loading.
	pub surrealism_max_fs_bytes: u64,
	/// How many Surrealism invocations may run at once across the datastore
	/// (default: 64). This is the instance pool's slot count: every invocation
	/// runs on a fresh instance in one slot, so it bounds address-space
	/// reservations as well as concurrency: each slot reserves about 4 GiB of
	/// virtual address space (not resident memory) on 64-bit hosts, which a
	/// virtual memory limit such as `RLIMIT_AS` must allow. A quarter of the slots (at least
	/// one, when there are two or more) is kept for invocations made from
	/// inside another extension. A top-level invocation that finds every slot
	/// it may take in use waits, within its query's timeout; a nested one
	/// fails at once.
	pub surrealism_max_concurrent_invocations: u32,
	/// Whether a definition's `CAPABILITIES { strict_timeout: false }` takes
	/// effect (default: false). `false` runs the extension on the engine
	/// without epoch checks, so neither `surrealism_max_execution_time` nor
	/// the query's timeout can stop it, and a busy guest holds its slot and its
	/// worker thread until it returns. Unless this is set, every invocation
	/// runs on the guarded engine whatever the definition says.
	pub surrealism_allow_unbounded_execution: bool,
	/// The tracing level a module's standard output is recorded at, one of `trace`,
	/// `info`, `warn` or `error`; any other value records at `debug` (default:
	/// "debug"). A module's standard error is always recorded at `warn`,
	/// independently of this.
	pub surrealism_log_level: String,
	/// Base URL that a `silo::` executable is resolved against: [`SILO_ENDPOINT`]. There is one
	/// registry, so a release build cannot change it. A build with debug assertions reads
	/// `SURREAL_SURREALISM_SILO_ENDPOINT`, so the integration tests can serve packages from a
	/// local registry; a trailing slash is stripped before the path is appended.
	pub surrealism_silo_endpoint: String,
	/// How long a definition that failed to load answers with that failure
	/// before its source is fetched again:
	/// [`REFETCH_AFTER_REFUSAL`](crate::extension::trust::REFETCH_AFTER_REFUSAL). A release build
	/// cannot change it. A build with debug assertions reads
	/// `SURREAL_SURREALISM_REFETCH_AFTER_REFUSAL`, in milliseconds, so the integration tests can
	/// see each load-time refusal without waiting out the window; `0` repeats no refusal.
	pub surrealism_refetch_after_refusal: Duration,
}

/// The Silo package host, the one registry `silo::` executables resolve against.
pub(crate) const SILO_ENDPOINT: &str = "https://silo.surrealdb.com";

impl Default for SurrealismConfig {
	fn default() -> Self {
		Self {
			surrealism_cache_size: 100,
			surrealism_max_memory: None,
			surrealism_max_execution_time: None,
			surrealism_max_fs_bytes: 100 * 1024 * 1024,
			surrealism_max_concurrent_invocations: 64,
			surrealism_allow_unbounded_execution: false,
			surrealism_log_level: "debug".to_string(),
			surrealism_silo_endpoint: SILO_ENDPOINT.to_string(),
			surrealism_refetch_after_refusal: crate::extension::trust::REFETCH_AFTER_REFUSAL,
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
			.parse_key("surrealism_max_fs_bytes", &mut self.surrealism_max_fs_bytes)
			.parse_key(
				"surrealism_max_concurrent_invocations",
				&mut self.surrealism_max_concurrent_invocations,
			)
			.parse_key(
				"surrealism_allow_unbounded_execution",
				&mut self.surrealism_allow_unbounded_execution,
			)
			.parse_key_with("surrealism_log_level", &mut self.surrealism_log_level, |s| {
				Some(s.to_string())
			});
		#[cfg(debug_assertions)]
		map.parse_key_with("surrealism_silo_endpoint", &mut self.surrealism_silo_endpoint, |s| {
			Some(s.trim_end_matches('/').to_string())
		})
		.parse_key_with(
			"surrealism_refetch_after_refusal",
			&mut self.surrealism_refetch_after_refusal,
			|s| s.parse().map(Duration::from_millis).ok(),
		);
	}
}
