//! Tests for core behaviour layered on top of the KV backends: the
//! transaction metrics wrappers, the transaction cache, and the deferred
//! data reclaim task. The raw KV-store behaviour tests live in the
//! `surrealdb-kvs-any` crate.
#![cfg(any(
	feature = "kv-mem",
	feature = "kv-rocksdb",
	feature = "kv-indxdb",
	feature = "kv-tikv",
	feature = "kv-surrealkv",
))]

#[cfg(feature = "kv-mem")]
mod reclaim_test;
#[cfg(feature = "kv-mem")]
mod rpc_session_test;
#[cfg(feature = "kv-mem")]
mod tx_cache_test;
mod tx_metrics;
#[cfg(feature = "kv-mem")]
mod write_guard_test;
