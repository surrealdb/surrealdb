#![cfg(any(
	feature = "kv-mem",
	feature = "kv-rocksdb",
	feature = "kv-indxdb",
	feature = "kv-tikv",
	feature = "kv-fdb",
	feature = "kv-surrealkv",
))]

use std::future::Future;
use std::sync::Arc;

use uuid::Uuid;

use super::Datastore;
use crate::kvs::clock::SizedClock;

macro_rules! include_tests {
	($new_ds:ident => $($name:ident),* $(,)?) => {
		$(
			super::$name::define_tests!($new_ds);
		)*
	};
}

mod multireader;
mod multiwriter_different_keys;
mod multiwriter_same_keys_allow;
mod multiwriter_same_keys_conflict;
mod raw;
#[cfg(any(feature = "kv-rocksdb", feature = "kv-tikv"))]
mod reverse_iterator;
mod snapshot;
mod timestamp_to_versionstamp;

#[derive(Clone, Debug)]
pub(crate) enum Kvs {
	#[cfg_attr(not(feature = "kv-mem"), expect(dead_code))]
	Mem,
	#[cfg_attr(not(feature = "kv-rocksdb"), expect(dead_code))]
	Rocksdb,
	#[cfg_attr(not(feature = "kv-tikv"), expect(dead_code))]
	Tikv,
	#[cfg_attr(not(feature = "kv-fdb"), expect(dead_code))]
	Fdb,
	#[cfg_attr(not(feature = "kv-surrealkv"), expect(dead_code))]
	SurrealKV,
}

// This type is unused when no store is enabled.
#[cfg_attr(not(test), expect(dead_code))]
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
	use uuid::Uuid;

	use super::{ClockType, Kvs};
	use crate::kvs::Datastore;

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
	use temp_dir::TempDir;
	use uuid::Uuid;

	use super::{ClockType, Kvs};
	use crate::kvs::Datastore;

	async fn new_ds(id: Uuid, clock: ClockType) -> (Datastore, Kvs) {
		// Setup the temporary data storage path
		let path = TempDir::new().unwrap().path().to_string_lossy().to_string();
		let path = format!("rocksdb:{path}");
		// Setup the RocksDB datastore
		let ds = Datastore::new_with_clock(&path, Some(clock)).await.unwrap().with_node_id(id);
		// Return the datastore
		(ds, Kvs::Rocksdb)
	}

	include_tests!(new_ds => raw,snapshot,multireader,multiwriter_different_keys,multiwriter_same_keys_conflict,timestamp_to_versionstamp,reverse_iterator);
}

#[cfg(feature = "kv-surrealkv")]
mod surrealkv {
	use temp_dir::TempDir;
	use uuid::Uuid;

	use super::{ClockType, Kvs};
	use crate::kvs::Datastore;

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
	use uuid::Uuid;

	use super::{ClockType, Kvs};
	use crate::kvs::{Datastore, LockType, TransactionType};

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
	use uuid::Uuid;

	use super::{ClockType, Kvs};
	use crate::kvs::{Datastore, LockType, TransactionType};

	async fn new_ds(id: Uuid, clock: ClockType) -> (Datastore, Kvs) {
		// Setup the cluster connection string
		let path = "fdb:/etc/foundationdb/fdb.cluster";
		// Setup the FoundationDB datastore
		let ds = Datastore::new_with_clock(path, Some(clock)).await.unwrap().with_node_id(id);
		// Clear any previous test entries
		let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();
		tx.delp(&vec![]).await.unwrap();
		tx.commit().await.unwrap();
		// Return the datastore
		(ds, Kvs::Fdb)
	}

	include_tests!(new_ds => raw,snapshot,multireader,multiwriter_different_keys,multiwriter_same_keys_allow,timestamp_to_versionstamp);
}
