#![cfg(any(
	feature = "kv-mem",
	feature = "kv-rocksdb",
	feature = "kv-indxdb",
	feature = "kv-tikv",
	feature = "kv-fdb",
	feature = "kv-surrealkv",
	feature = "kv-surrealcs",
))]

use crate::kvs::clock::SizedClock;
use std::sync::Arc;

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

#[cfg(feature = "kv-mem")]
mod mem {

	async fn new_ds(id: Uuid, clock: ClockType) -> (Datastore, Kvs) {
		// Use a memory datastore instance
		let path = "memory";
		// Setup the in-memory datastore
		let ds = Datastore::new_with_clock(path, Some(clock)).await.unwrap().with_node_id(id);
		// Return the datastore
		(ds, Kvs::Mem)
	}

	async fn new_tx(write: TransactionType, lock: LockType) -> Transaction {
		let nodeid = Uuid::new_v4();
		let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
		new_ds(nodeid, clock).await.0.transaction(write, lock).await.unwrap()
	}

	include!("helper.rs");
	include!("raw.rs");
	include!("snapshot.rs");
	include!("multireader.rs");
	include!("multiwriter_different_keys.rs");
	include!("multiwriter_same_keys_conflict.rs");
	include!("timestamp_to_versionstamp.rs");
	include!("timestamp_to_versionstamp.rs");
}

#[cfg(feature = "kv-rocksdb")]
mod rocksdb {

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

	async fn new_tx(write: TransactionType, lock: LockType) -> Transaction {
		let nodeid = Uuid::new_v4();
		let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
		new_ds(nodeid, clock).await.0.transaction(write, lock).await.unwrap()
	}

	include!("helper.rs");
	include!("raw.rs");
	include!("snapshot.rs");
	include!("multireader.rs");
	include!("multiwriter_different_keys.rs");
	include!("multiwriter_same_keys_conflict.rs");
	include!("timestamp_to_versionstamp.rs");
}

#[cfg(feature = "kv-surrealkv")]
mod surrealkv {

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

	async fn new_tx(write: TransactionType, lock: LockType) -> Transaction {
		let nodeid = Uuid::new_v4();
		let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
		let (ds, _) = new_ds(nodeid, clock).await;
		ds.transaction(write, lock).await.unwrap()
	}

	include!("raw.rs");
	include!("helper.rs");
	include!("snapshot.rs");
	include!("multireader.rs");
	include!("multiwriter_different_keys.rs");
	include!("multiwriter_same_keys_allow.rs");
	include!("timestamp_to_versionstamp.rs");
}

#[cfg(feature = "kv-tikv")]
mod tikv {

	async fn new_ds(id: Uuid, clock: ClockType) -> (Datastore, Kvs) {
		// Setup the cluster connection string
		let path = "tikv:127.0.0.1:2379";
		// Setup the TiKV datastore
		let ds = Datastore::new_with_clock(path, Some(clock)).await.unwrap().with_node_id(id);
		// Clear any previous test entries
		let tx = ds.transaction(Write, Optimistic).await.unwrap();
		tx.delp(vec![]).await.unwrap();
		tx.commit().await.unwrap();
		// Return the datastore
		(ds, Kvs::Tikv)
	}

	async fn new_tx(write: TransactionType, lock: LockType) -> Transaction {
		let nodeid = Uuid::new_v4();
		let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
		new_ds(nodeid, clock).await.0.transaction(write, lock).await.unwrap()
	}

	include!("helper.rs");
	include!("raw.rs");
	include!("snapshot.rs");
	include!("multireader.rs");
	include!("multiwriter_different_keys.rs");
	include!("multiwriter_same_keys_conflict.rs");
	include!("timestamp_to_versionstamp.rs");
}

#[cfg(feature = "kv-fdb")]
mod fdb {

	async fn new_ds(id: Uuid, clock: ClockType) -> (Datastore, Kvs) {
		// Setup the cluster connection string
		let path = "fdb:/etc/foundationdb/fdb.cluster";
		// Setup the FoundationDB datastore
		let ds = Datastore::new_with_clock(path, Some(clock)).await.unwrap().with_node_id(id);
		// Clear any previous test entries
		let tx = ds.transaction(Write, Optimistic).await.unwrap();
		tx.delp(vec![]).await.unwrap();
		tx.commit().await.unwrap();
		// Return the datastore
		(ds, Kvs::Fdb)
	}

	async fn new_tx(write: TransactionType, lock: LockType) -> Transaction {
		let nodeid = Uuid::new_v4();
		let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
		new_ds(nodeid, clock).await.0.transaction(write, lock).await.unwrap()
	}

	include!("helper.rs");
	include!("raw.rs");
	include!("snapshot.rs");
	include!("multireader.rs");
	include!("multiwriter_different_keys.rs");
	include!("multiwriter_same_keys_allow.rs");
	include!("timestamp_to_versionstamp.rs");
}
