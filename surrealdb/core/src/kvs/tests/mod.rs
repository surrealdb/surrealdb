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

use std::future::Future;

use uuid::Uuid;

use super::Datastore;

macro_rules! include_tests {
	($new_ds:ident => $($name:ident),* $(,)?) => {
		$(
			super::$name::define_tests!($new_ds);
		)*
	};
}

#[cfg(feature = "kv-rocksdb")]
mod metrics;

#[cfg(feature = "kv-mem")]
mod changefeed_gc_test;
mod conditional_write_fences;
mod multireader;
mod multiwriter_different_keys;
mod multiwriter_same_keys_allow;
mod multiwriter_same_keys_conflict;
mod raw;
#[cfg(feature = "kv-mem")]
mod reclaim_test;
#[cfg(feature = "kv-mem")]
mod rpc_session_test;
mod snapshot;
#[cfg(feature = "kv-mem")]
mod tx_cache_test;
#[cfg(feature = "kv-mem")]
mod write_guard_test;

#[derive(Clone, Debug)]
pub(crate) enum Kvs {
	#[cfg_attr(not(feature = "kv-mem"), expect(dead_code))]
	Mem,
	#[cfg_attr(not(feature = "kv-rocksdb"), expect(dead_code))]
	Rocksdb,
	#[cfg_attr(not(feature = "kv-tikv"), expect(dead_code))]
	Tikv,
	#[cfg_attr(not(feature = "kv-surrealkv"), expect(dead_code))]
	SurrealKV,
}

trait CreateDs {
	async fn create_ds(&self, id: Uuid) -> (Datastore, Kvs);
}

impl<F, Fut> CreateDs for F
where
	F: Fn(Uuid) -> Fut,
	Fut: Future<Output = (Datastore, Kvs)>,
{
	async fn create_ds(&self, id: Uuid) -> (Datastore, Kvs) {
		(self)(id).await
	}
}

#[cfg(feature = "kv-mem")]
mod mem {
	use uuid::Uuid;

	use super::Kvs;
	use crate::CommunityComposer;
	use crate::kvs::Datastore;

	async fn new_ds(id: Uuid) -> (Datastore, Kvs) {
		// Use a memory datastore instance
		let path = "memory";
		// Setup the in-memory datastore
		let ds = Datastore::builder()
			.with_id(id)
			.build_with_factory_path(path, CommunityComposer())
			.await
			.unwrap();
		// Return the datastore
		(ds, Kvs::Mem)
	}

	include_tests!(new_ds =>
		raw,
		snapshot,
		multireader,
		conditional_write_fences,
		multiwriter_different_keys,
		multiwriter_same_keys_conflict,
	);
}

#[cfg(feature = "kv-rocksdb")]
mod rocksdb {
	use temp_dir::TempDir;
	use uuid::Uuid;

	use super::Kvs;
	use crate::CommunityComposer;
	use crate::kvs::Datastore;

	async fn new_ds(id: Uuid) -> (Datastore, Kvs) {
		// Setup the temporary data storage path
		let path = TempDir::new().unwrap().path().to_string_lossy().to_string();
		let path = format!("rocksdb:{path}");
		// Setup the RocksDB datastore
		let ds = Datastore::builder()
			.with_id(id)
			.build_with_factory_path(&path, CommunityComposer())
			.await
			.unwrap();
		// Return the datastore
		(ds, Kvs::Rocksdb)
	}

	include_tests!(new_ds =>
		raw,
		snapshot,
		multireader,
		conditional_write_fences,
		multiwriter_different_keys,
		multiwriter_same_keys_conflict,
		metrics
	);
}

#[cfg(feature = "kv-surrealkv")]
mod surrealkv {
	use temp_dir::TempDir;
	use uuid::Uuid;

	use super::Kvs;
	use crate::CommunityComposer;
	use crate::kvs::Datastore;

	async fn new_ds(id: Uuid) -> (Datastore, Kvs) {
		// Setup the temporary data storage path
		let path = TempDir::new().unwrap().path().to_string_lossy().to_string();
		let path = format!("surrealkv:{path}");
		// Setup the SurrealKV datastore
		let ds = Datastore::builder()
			.with_id(id)
			.build_with_factory_path(&path, CommunityComposer())
			.await
			.unwrap();
		// Return the datastore
		(ds, Kvs::SurrealKV)
	}

	include_tests!(new_ds =>
		raw,
		snapshot,
		multireader,
		conditional_write_fences,
		multiwriter_different_keys,
		multiwriter_same_keys_conflict,
	);
}

#[cfg(feature = "kv-tikv")]
mod tikv {
	use uuid::Uuid;

	use super::Kvs;
	use crate::CommunityComposer;
	use crate::kvs::{Datastore, LockType, TransactionType};

	async fn new_ds(id: Uuid) -> (Datastore, Kvs) {
		// Setup the cluster connection string
		let path = "tikv:127.0.0.1:2379";
		// Setup the TiKV datastore
		let ds = Datastore::builder()
			.with_id(id)
			.build_with_factory_path(path, CommunityComposer())
			.await
			.unwrap();
		// Clear any previous test entries
		let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();
		tx.delr(vec![0u8]..vec![0xffu8]).await.unwrap();
		tx.commit().await.unwrap();
		// Return the datastore
		(ds, Kvs::Tikv)
	}

	include_tests!(new_ds =>
		raw,
		snapshot,
		multireader,
		conditional_write_fences,
		multiwriter_different_keys,
		multiwriter_same_keys_allow,
	);
}
