//! Tests for core behaviour layered on top of the KV backends: the
//! transaction metrics wrappers, the transaction cache, and the deferred
//! data reclaim task. The raw KV-store behaviour tests live in the
//! `surrealdb-kvs-any` crate.
//!
//! A module gated on `kv-mem` asserts behaviour every backend shares, so the
//! in-memory store is enough to cover it. An ungated module carries per-backend
//! tests, each gated on its own `kv-*` feature.
#![cfg(any(feature = "kv-mem", feature = "kv-surrealkv",))]

#[cfg(feature = "kv-surrealkv")]
mod blocking_rebuild_index_test;
#[cfg(feature = "kv-mem")]
mod caller_owned_transaction_test;
#[cfg(feature = "kv-mem")]
mod changefeed_gc_test;
#[cfg(feature = "kv-mem")]
mod compatibility_test;
#[cfg(any(feature = "kv-mem", feature = "kv-surrealkv"))]
mod doc_id_claim_test;
#[cfg(feature = "kv-mem")]
mod export_id_components_test;
#[cfg(feature = "kv-mem")]
mod export_implied_element_fields_test;
#[cfg(feature = "kv-mem")]
mod export_legacy_extension_test;
#[cfg(all(feature = "kv-mem", feature = "postgres"))]
mod export_user_types_test;
#[cfg(feature = "kv-mem")]
mod export_views_test;
#[cfg(all(feature = "kv-mem", feature = "surrealism"))]
mod extension_bindings_test;
#[cfg(all(feature = "search", feature = "kv-mem"))]
mod fulltext_nodes_test;
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
#[cfg(any(feature = "kv-mem", feature = "kv-surrealkv"))]
mod held_creates_bench;
#[cfg(feature = "kv-mem")]
mod index_build_sync_test;
#[cfg(feature = "kv-mem")]
mod index_format_level_test;
#[cfg(feature = "kv-mem")]
mod index_key_kernel_test;
#[cfg(feature = "kv-mem")]
mod inline_cache_test;
#[cfg(feature = "kv-mem")]
mod inline_props_test;
#[cfg(feature = "kv-mem")]
mod interrupted_write_test;
#[cfg(feature = "kv-mem")]
mod isolation_level_test;
#[cfg(feature = "kv-mem")]
mod keyspace_test;
#[cfg(any(feature = "kv-mem", feature = "kv-surrealkv"))]
mod latency;
#[cfg(feature = "kv-mem")]
mod layout_oracle_test;
#[cfg(feature = "kv-mem")]
mod lightweight_test;
#[cfg(feature = "kv-mem")]
mod own_write_catalog_test;
#[cfg(feature = "kv-mem")]
mod read_only_transaction_test;
mod reclaim_backends_test;
#[cfg(feature = "kv-mem")]
mod reclaim_test;
#[cfg(any(feature = "kv-mem", feature = "kv-surrealkv"))]
mod roundtrips_test;
#[cfg(feature = "kv-mem")]
mod rpc_session_test;
#[cfg(feature = "kv-surrealkv")]
mod select_for_update_test;
#[cfg(all(feature = "search", feature = "kv-mem"))]
mod shared_store_materialise_test;
#[cfg(feature = "kv-mem")]
mod stale_read_test;
#[cfg(feature = "kv-mem")]
mod table_migration_resilience_test;
#[cfg(feature = "kv-mem")]
mod table_migration_test;
#[cfg(feature = "kv-mem")]
mod tx_cache_test;
mod tx_cache_versioned_test;
#[cfg(feature = "kv-mem")]
mod tx_engine_test;
mod tx_metrics;
#[cfg(feature = "kv-mem")]
mod version_test;
#[cfg(feature = "kv-mem")]
mod warm_write_reads_test;
#[cfg(feature = "kv-mem")]
mod write_guard_test;
