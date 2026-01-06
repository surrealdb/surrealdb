#![cfg(any(
	feature = "kv-mem",
	feature = "kv-rocksdb",
	feature = "kv-indxdb",
	feature = "kv-tikv",
	feature = "kv-fdb",
	feature = "kv-surrealkv",
	feature = "kv-surrealcs",
))]

use super::Datastore;
use crate::kvs::clock::SizedClock;
use std::{future::Future, sync::Arc};
use uuid::Uuid;

macro_rules! include_tests {
	($new_ds:ident => $($name:ident),* $(,)?) => {
		$(
			super::$name::define_tests!($new_ds);
		)*
	};
}

mod metrics;
mod multireader;
mod multiwriter_different_keys;
mod multiwriter_same_keys_allow;
mod multiwriter_same_keys_conflict;
mod raw;
#[cfg(feature = "kv-rocksdb")]
mod read_and_deletion_only;
#[cfg(any(feature = "kv-rocksdb", feature = "kv-tikv"))]
mod reverse_iterator;
mod snapshot;
mod timestamp_to_versionstamp;

#[derive(Clone, Debug)]
pub(crate) enum Kvs {
	#[allow(dead_code)]
	Mem,
	#[allow(dead_code)]
	Rocksdb,
	#[allow(dead_code)]
	Tikv,
	#[allow(dead_code)]
	Fdb,
	#[allow(dead_code)]
	SurrealKV,
}

// This type is unsused when no store is enabled.
#[allow(dead_code)]
type ClockType = Arc<SizedClock>;

trait CreateDs {
	async fn create_ds(&self, id: Uuid, ty: ClockType) -> (Datastore, Kvs);
}

impl<F, Fut> CreateDs for F
where
	F: Fn(Uuid, ClockType) -> Fut,
	Fut: Future<Output = (Datastore, Kvs)>,
{
	async fn create_ds(&self, id: Uuid, ty: ClockType) -> (Datastore, Kvs) {
		(self)(id, ty).await
	}
}

#[cfg(feature = "kv-mem")]
mod mem {
	use super::{ClockType, Kvs};
	use crate::kvs::Datastore;
	use uuid::Uuid;

	async fn new_ds(id: Uuid, clock: ClockType) -> (Datastore, Kvs) {
		// Use a memory datastore instance
		let path = "memory";
		// Setup the in-memory datastore
		let ds = Datastore::new_with_clock(path, Some(clock)).await.unwrap().with_node_id(id);
		// Return the datastore
		(ds, Kvs::Mem)
	}

	include_tests!(new_ds => raw,snapshot,multireader,multiwriter_different_keys,multiwriter_same_keys_conflict,timestamp_to_versionstamp);
}

#[cfg(feature = "kv-rocksdb")]
mod rocksdb {
	use super::{ClockType, Kvs};
	use crate::kvs::Datastore;
	use uuid::Uuid;

	use temp_dir::TempDir;

	async fn new_ds(id: Uuid, clock: ClockType) -> (Datastore, Kvs) {
		// Setup the temporary data storage path
		let path = TempDir::new().unwrap().path().to_string_lossy().to_string();
		let path = format!("rocksdb:{path}");
		// Setup the RocksDB datastore
		let ds = Datastore::new_with_clock(&path, Some(clock)).await.unwrap().with_node_id(id);
		// Return the datastore
		(ds, Kvs::Rocksdb)
	}

	include_tests!(new_ds => raw,snapshot,multireader,multiwriter_different_keys,multiwriter_same_keys_conflict,timestamp_to_versionstamp,reverse_iterator, read_and_deletion_only, metrics);
}

#[cfg(feature = "kv-surrealkv")]
mod surrealkv {
	use super::{ClockType, Kvs};
	use crate::kvs::Datastore;
	use uuid::Uuid;

	use temp_dir::TempDir;

	async fn new_ds(id: Uuid, clock: ClockType) -> (Datastore, Kvs) {
		// Setup the temporary data storage path
		let path = TempDir::new().unwrap().path().to_string_lossy().to_string();
		let path = format!("surrealkv:{path}");
		// Setup the SurrealKV datastore
		let ds = Datastore::new_with_clock(&path, Some(clock)).await.unwrap().with_node_id(id);
		// Return the datastore
		(ds, Kvs::SurrealKV)
	}

	include_tests!(new_ds => raw,snapshot,multireader,multiwriter_different_keys,multiwriter_same_keys_conflict,timestamp_to_versionstamp);
}

#[cfg(feature = "kv-tikv")]
mod tikv {
	use super::{ClockType, Kvs};
	use crate::kvs::{Datastore, LockType, TransactionType};
	use uuid::Uuid;

	async fn new_ds(id: Uuid, clock: ClockType) -> (Datastore, Kvs) {
		// Setup the cluster connection string
		let path = "tikv:127.0.0.1:2379";
		// Setup the TiKV datastore
		let ds = Datastore::new_with_clock(path, Some(clock)).await.unwrap().with_node_id(id);
		// Clear any previous test entries
		let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();
		tx.delr(vec![0u8]..vec![0xffu8]).await.unwrap();
		tx.commit().await.unwrap();
		// Return the datastore
		(ds, Kvs::Tikv)
	}

	include_tests!(new_ds => raw,snapshot,multireader,multiwriter_different_keys,multiwriter_same_keys_allow,timestamp_to_versionstamp,reverse_iterator);
}

#[cfg(feature = "kv-fdb")]
mod fdb {
	use super::{ClockType, Kvs};
	use crate::kvs::{Datastore, LockType, TransactionType};
	use uuid::Uuid;

	async fn new_ds(id: Uuid, clock: ClockType) -> (Datastore, Kvs) {
		// Setup the cluster connection string
		let path = "fdb:/etc/foundationdb/fdb.cluster";
		// Setup the FoundationDB datastore
		let ds = Datastore::new_with_clock(path, Some(clock)).await.unwrap().with_node_id(id);
		// Clear any previous test entries
		let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();
		tx.delp(vec![]).await.unwrap();
		tx.commit().await.unwrap();
		// Return the datastore
		(ds, Kvs::Fdb)
	}

	include_tests!(new_ds => raw,snapshot,multireader,multiwriter_different_keys,multiwriter_same_keys_allow,timestamp_to_versionstamp);
}
