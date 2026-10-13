//! Tests for core behaviour layered on top of the KV backends: the
//! transaction metrics wrappers, the transaction cache, and the deferred
//! data reclaim task. The raw KV-store behaviour tests live in the
//! `surrealdb-kvs-any` crate.
//!
//! A module gated on `kv-mem` asserts behaviour every backend shares, so the
//! in-memory store is enough to cover it. An ungated module carries per-backend
//! tests, each gated on its own `kv-*` feature.
#![cfg(any(
	feature = "kv-mem",
	feature = "kv-rocksdb",
	feature = "kv-indxdb",
	feature = "kv-tikv",
	feature = "kv-surrealkv",
))]

#[cfg(feature = "kv-mem")]
mod changefeed_gc_test;
#[cfg(feature = "kv-mem")]
mod graph_degree_test;
#[cfg(feature = "kv-mem")]
mod graph_fold_test;
#[cfg(feature = "kv-mem")]
mod graph_migration_test;
#[cfg(feature = "kv-mem")]
mod graph_ordered_limit_test;
#[cfg(feature = "kv-mem")]
mod graph_semijoin_test;
#[cfg(feature = "kv-mem")]
mod inline_cache_test;
#[cfg(feature = "kv-mem")]
mod inline_props_test;
#[cfg(feature = "kv-mem")]
mod keyspace_test;
#[cfg(feature = "kv-mem")]
mod lightweight_test;
mod reclaim_backends_test;
#[cfg(feature = "kv-mem")]
mod reclaim_test;
#[cfg(feature = "kv-mem")]
mod rpc_session_test;
#[cfg(feature = "kv-rocksdb")]
mod select_for_update_test;
#[cfg(feature = "kv-tikv")]
mod sequences_tikv_test;
#[cfg(feature = "kv-mem")]
mod tx_cache_test;
mod tx_cache_versioned_test;
#[cfg(feature = "kv-mem")]
mod tx_engine_test;
mod tx_metrics;
#[cfg(feature = "kv-mem")]
mod version_test;
#[cfg(feature = "kv-mem")]
mod write_guard_test;
