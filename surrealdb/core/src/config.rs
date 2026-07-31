//! Transport for the per-layer configuration a query execution reads.
//!
//! Each layer owns its own knobs, declared beside the code they govern and
//! loaded independently from the shared [`ConfigMap`], so adding a knob touches
//! one layer instead of a struct every layer shares. A layer owns a knob when it
//! is the lowest layer that reads it; layers above read downward, which is why
//! core's legacy evaluator reads [`ExecConfig`] and the index engines' file
//! allowlist without either moving up.
//!
//! This struct is only the transport for the knobs a *running query* reaches
//! through [`Context`](crate::ctx::Context). Settings read once while building
//! something — a transaction's cache size, the datastore's own caches — are
//! handed to that constructor directly and are absent here.
//!
//! It stays crate-internal: these are operator knobs, not API. Nothing outside
//! core reads them, and keeping the per-layer structs unexported is what lets
//! each one move with its layer as the crate split proceeds.

use surrealdb_cnf::ConfigMap;
use surrealdb_syn::ParserConfig;

use crate::exec::config::ExecConfig;
#[cfg(feature = "scripting")]
use crate::fnc::script::config::ScriptConfig;
#[cfg(any(feature = "http", feature = "jwks"))]
use crate::http::config::HttpConfig;
use crate::iam::config::IamConfig;
use crate::idx::config::IdxConfig;
use crate::kvs::ds::config::DatastoreConfig;
use crate::legacy::config::LegacyConfig;
#[cfg(feature = "surrealism")]
use crate::surrealism::config::SurrealismConfig;

/// The configuration a query execution reads through its context.
#[derive(Clone, Debug, Default)]
pub(crate) struct RuntimeConfig {
	/// Depth limits applied to query text parsed during execution — by
	/// `DEFINE API` request decoding, a Surrealism module's own queries, and the
	/// `eval()` builtin.
	pub parser: ParserConfig,
	/// Limits and batch sizes governing the streaming executor.
	pub exec: ExecConfig,
	/// Knobs of the index engines, including the file allowlist their analyzers
	/// enforce.
	pub idx: IdxConfig,
	/// Knobs of the legacy evaluator.
	pub legacy: LegacyConfig,
	/// Authentication behaviour read while evaluating record access.
	pub iam: IamConfig,
	/// The datastore's own knobs.
	///
	/// Here rather than construction-only because the write path decides per
	/// document whether to capture a live-query event, so it reads the engine
	/// selection from the context it already holds.
	pub datastore: DatastoreConfig,
	/// Per-module ceilings applied to Surrealism WASM modules.
	#[cfg(feature = "surrealism")]
	pub surrealism: SurrealismConfig,
	/// Limits applied to the JavaScript function runtime.
	#[cfg(feature = "scripting")]
	pub script: ScriptConfig,
	/// Settings for outbound HTTP clients.
	///
	/// Here rather than construction-only because a Surrealism module's net
	/// targets are known only once the module loads, so its client is built
	/// mid-query from the context.
	#[cfg(any(feature = "http", feature = "jwks"))]
	pub http: HttpConfig,
}

impl RuntimeConfig {
	/// Loads every layer's configuration from one source, so a single
	/// `ConfigMap` — env vars, datastore URL query params, or programmatic keys
	/// — populates all of them.
	pub(crate) fn load(map: &ConfigMap) -> Self {
		Self {
			parser: map.load(),
			exec: map.load(),
			idx: map.load(),
			legacy: map.load(),
			iam: map.load(),
			datastore: map.load(),
			#[cfg(feature = "surrealism")]
			surrealism: map.load(),
			#[cfg(feature = "scripting")]
			script: map.load(),
			#[cfg(any(feature = "http", feature = "jwks"))]
			http: map.load(),
		}
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_cnf::ConfigMap;

	use super::*;

	/// Every configuration key must still reach a field.
	///
	/// [`ConfigMap`] warns when a value fails to parse but says nothing about a
	/// key nobody asks for, so a knob whose `parse_key` line is missing is
	/// silently and permanently inert. Each row below sets one key to a value
	/// distinguishable from its default and asserts the field moved, which
	/// catches both a dropped key and one wired to the wrong field.
	///
	/// Add a row whenever a key is added. The two keys deliberately absent are
	/// `idiom_recursion_limit` and `surrealism_max_pool_size`: neither has ever
	/// had a `parse_key` line, so both always hold their default.
	fn map_with(key: &str, value: &str) -> ConfigMap {
		ConfigMap::empty().with_key_value(key, value)
	}

	#[test]
	fn parser_keys_reach_their_fields() {
		use surrealdb_syn::ParserConfig;
		let c: ParserConfig = map_with("max_object_parsing_depth", "7").load();
		assert_eq!(c.max_object_parsing_depth, 7);
		let c: ParserConfig = map_with("max_query_parsing_depth", "7").load();
		assert_eq!(c.max_query_parsing_depth, 7);
		let c: ParserConfig = map_with("max_expression_parsing_depth", "7").load();
		assert_eq!(c.max_expression_parsing_depth, 7);
	}

	#[test]
	fn exec_keys_reach_their_fields() {
		let c: ExecConfig = map_with("max_computation_depth", "7").load();
		assert_eq!(c.max_computation_depth, 7);
		let c: ExecConfig = map_with("operator_buffer_size", "7").load();
		assert_eq!(c.operator_buffer_size, 7);
		let c: ExecConfig = map_with("scan_batch_size", "7").load();
		assert_eq!(c.scan_batch_size, 7);
		let c: ExecConfig = map_with("max_order_limit_priority_queue_size", "7").load();
		assert_eq!(c.max_order_limit_priority_queue_size, 7);
		let c: ExecConfig = map_with("topk_threshold_pushdown_enabled", "false").load();
		assert!(!c.topk_threshold_pushdown_enabled);
		let c: ExecConfig = map_with("gql_max_join_build_rows", "7").load();
		assert_eq!(c.gql_max_join_build_rows, 7);
		let c: ExecConfig = map_with("gql_max_path_rows", "7").load();
		assert_eq!(c.gql_max_path_rows, 7);
		let c: ExecConfig = map_with("gql_max_output_rows", "7").load();
		assert_eq!(c.gql_max_output_rows, 7);
		let c: ExecConfig = map_with("external_sorting_buffer_limit", "7").load();
		assert_eq!(c.external_sorting_buffer_limit, 7);
		// Parsed as an exponent, not a byte count: `2 << min(x, 28)`.
		let c: ExecConfig = map_with("generation_allocation_limit", "3").load();
		assert_eq!(c.generation_allocation_limit, 2 << 3);
	}

	#[test]
	fn legacy_and_iam_keys_reach_their_fields() {
		let c: LegacyConfig = map_with("max_concurrent_tasks", "7").load();
		assert_eq!(c.max_concurrent_tasks, 7);
		let c: IamConfig = map_with("insecure_forward_access_errors", "true").load();
		assert!(c.insecure_forward_access_errors);
	}

	#[test]
	fn idx_keys_reach_their_fields() {
		let c: IdxConfig = map_with("table_doc_ids_batch_size", "7").load();
		assert_eq!(c.table_doc_ids_batch_size, 7);
		let c: IdxConfig = map_with("hnsw_cache_size", "7").load();
		assert_eq!(c.hnsw_cache_size, 7);
		let c: IdxConfig = map_with("diskann_cache_size", "7").load();
		assert_eq!(c.diskann_cache_size, 7);
		// Canonicalizing, so the value has to name a directory that exists.
		let dir = std::env::temp_dir();
		let c: IdxConfig = map_with("file_allowlist", &dir.to_string_lossy()).load();
		assert!(!c.file_allowlist.is_empty());
	}

	#[test]
	fn datastore_keys_reach_their_fields() {
		use crate::kvs::TransactionConfig;
		use crate::kvs::ds::config::{DatastoreConfig, LiveQueryEngine};

		let c: TransactionConfig = map_with("transaction_cache_size", "7").load();
		assert_eq!(c.transaction_cache_size, 7);
		let c: TransactionConfig = map_with("transaction_max_write_keys", "7").load();
		assert_eq!(c.transaction_max_write_keys, 7);

		let c: DatastoreConfig = map_with("datastore_cache_size", "7").load();
		assert_eq!(c.datastore_cache_size, 7);
		let c: DatastoreConfig = map_with("export_batch_size", "7").load();
		assert_eq!(c.export_batch_size, 7);
		let c: DatastoreConfig = map_with("live_query_engine", "router").load();
		assert_eq!(c.live_query_engine, LiveQueryEngine::Router);
		let c: DatastoreConfig = map_with("live_query_retention", "5m").load();
		assert_eq!(c.live_query_retention, std::time::Duration::from_secs(300));
	}

	#[cfg(any(feature = "http", feature = "jwks"))]
	#[test]
	fn http_keys_reach_their_fields() {
		use crate::http::config::HttpConfig;

		let c: HttpConfig = map_with("max_http_redirects", "7").load();
		assert_eq!(c.max_http_redirects, 7);
		let c: HttpConfig = map_with("max_http_idle_connections_per_host", "7").load();
		assert_eq!(c.max_http_idle_connections_per_host, 7);
		let c: HttpConfig = map_with("http_idle_timeout_secs", "7").load();
		assert_eq!(c.http_idle_timeout_secs, 7);
		let c: HttpConfig = map_with("http_connect_timeout_secs", "7").load();
		assert_eq!(c.http_connect_timeout_secs, 7);
		let c: HttpConfig = map_with("surrealdb_user_agent", "probe").load();
		assert_eq!(c.surrealdb_user_agent, "probe");
	}

	#[cfg(feature = "scripting")]
	#[test]
	fn scripting_keys_reach_their_fields() {
		use crate::fnc::script::config::ScriptConfig;

		let c: ScriptConfig = map_with("scripting_max_stack_size", "7").load();
		assert_eq!(c.scripting_max_stack_size, 7);
		let c: ScriptConfig = map_with("scripting_max_memory_limit", "7").load();
		assert_eq!(c.scripting_max_memory_limit, 7);
		// Read as milliseconds.
		let c: ScriptConfig = map_with("scripting_max_time_limit", "7").load();
		assert_eq!(c.scripting_max_time_limit, std::time::Duration::from_millis(7));
	}

	#[cfg(feature = "surrealism")]
	#[test]
	fn surrealism_keys_reach_their_fields() {
		let c: SurrealismConfig = map_with("surrealism_cache_size", "7").load();
		assert_eq!(c.surrealism_cache_size, 7);
		let c: SurrealismConfig = map_with("surrealism_max_memory", "7").load();
		assert_eq!(c.surrealism_max_memory, Some(7));
		let c: SurrealismConfig = map_with("surrealism_max_execution_time", "7").load();
		assert_eq!(c.surrealism_max_execution_time, Some(7));
		let c: SurrealismConfig = map_with("surrealism_max_kv_entries", "7").load();
		assert_eq!(c.surrealism_max_kv_entries, Some(7));
		let c: SurrealismConfig = map_with("surrealism_max_kv_value_bytes", "7").load();
		assert_eq!(c.surrealism_max_kv_value_bytes, Some(7));
		let c: SurrealismConfig = map_with("surrealism_max_fs_bytes", "7").load();
		assert_eq!(c.surrealism_max_fs_bytes, 7);
		let c: SurrealismConfig = map_with("surrealism_log_level", "trace").load();
		assert_eq!(c.surrealism_log_level, "trace");
	}

	/// Every layer carried by the aggregate is actually loaded.
	///
	/// The per-struct tests above prove a key reaches its field; this proves the
	/// struct itself is wired into [`RuntimeConfig::load`], which is the other
	/// way a knob can end up permanently stuck at its default.
	#[test]
	fn every_layer_is_loaded_into_the_aggregate() {
		let map = ConfigMap::empty()
			.with_key_value("max_object_parsing_depth", "7")
			.with_key_value("operator_buffer_size", "7")
			.with_key_value("table_doc_ids_batch_size", "7")
			.with_key_value("max_concurrent_tasks", "7")
			.with_key_value("insecure_forward_access_errors", "true")
			.with_key_value("datastore_cache_size", "7");
		let c = RuntimeConfig::load(&map);
		assert_eq!(c.parser.max_object_parsing_depth, 7, "parser not loaded");
		assert_eq!(c.exec.operator_buffer_size, 7, "exec not loaded");
		assert_eq!(c.idx.table_doc_ids_batch_size, 7, "idx not loaded");
		assert_eq!(c.legacy.max_concurrent_tasks, 7, "legacy not loaded");
		assert!(c.iam.insecure_forward_access_errors, "iam not loaded");
		assert_eq!(c.datastore.datastore_cache_size, 7, "datastore not loaded");

		#[cfg(feature = "surrealism")]
		{
			let map = ConfigMap::empty().with_key_value("surrealism_cache_size", "7");
			assert_eq!(
				RuntimeConfig::load(&map).surrealism.surrealism_cache_size,
				7,
				"surrealism not loaded"
			);
		}
		#[cfg(feature = "scripting")]
		{
			let map = ConfigMap::empty().with_key_value("scripting_max_stack_size", "7");
			assert_eq!(
				RuntimeConfig::load(&map).script.scripting_max_stack_size,
				7,
				"script not loaded"
			);
		}
		#[cfg(any(feature = "http", feature = "jwks"))]
		{
			let map = ConfigMap::empty().with_key_value("max_http_redirects", "7");
			assert_eq!(RuntimeConfig::load(&map).http.max_http_redirects, 7, "http not loaded");
		}
	}

	/// The two knobs that have never been settable stay that way.
	#[test]
	fn unparsed_knobs_keep_their_defaults() {
		let c: ExecConfig = map_with("idiom_recursion_limit", "7").load();
		assert_eq!(c.idiom_recursion_limit, ExecConfig::default().idiom_recursion_limit);
		#[cfg(feature = "surrealism")]
		{
			let c: SurrealismConfig = map_with("surrealism_max_pool_size", "7").load();
			assert_eq!(
				c.surrealism_max_pool_size,
				SurrealismConfig::default().surrealism_max_pool_size
			);
		}
	}
}
