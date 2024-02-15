use crate::kvs::clock::SizedClock;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub(crate) enum Kvs {
	#[allow(dead_code)]
	Mem,
	#[allow(dead_code)]
	Rocksdb,
	#[allow(dead_code)]
	Speedb,
	#[allow(dead_code)]
	Tikv,
	#[allow(dead_code)]
	Fdb,
	#[allow(dead_code)]
	SurrealKV,
}

// This type is unsused when no store is enabled.
#[allow(dead_code)]
pub type ClockType = Arc<SizedClock>;

#[cfg(feature = "kv-mem")]
mod mem {

	use crate::kvs::tests::{ClockType, Kvs};
	use crate::kvs::Datastore;
	use crate::kvs::LockType;
	use crate::kvs::Transaction;
	use crate::kvs::TransactionType;
	use serial_test::serial;

	async fn new_ds(node_id: Uuid, clock_override: ClockType) -> (Datastore, Kvs) {
		(
			Datastore::new_full("memory", Some(clock_override))
				.await
				.unwrap()
				.with_node_id(crate::sql::Uuid::from(node_id)),
			Kvs::Mem,
		)
	}

	async fn new_tx(write: TransactionType, lock: LockType) -> Transaction {
		// Shared node id for one-off transactions
		// We should delete this, node IDs should be known.
		let new_tx_uuid = Uuid::parse_str("361893b5-a041-40c0-996c-c3a8828ef06b").unwrap();
		let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
		new_ds(new_tx_uuid, clock).await.0.transaction(write, lock).await.unwrap()
	}

	include!("cluster_init.rs");
	include!("hb.rs");
	include!("helper.rs");
	include!("lq.rs");
	include!("nq.rs");
	include!("raw.rs");
	include!("snapshot.rs");
	include!("tb.rs");
	include!("multireader.rs");
	include!("timestamp_to_versionstamp.rs");
	include!("nd.rs");
	include!("ndlq.rs");
	include!("tblq.rs");
	include!("tbnt.rs");
}

#[cfg(feature = "kv-rocksdb")]
mod rocksdb {

	use crate::kvs::tests::{ClockType, Kvs};
	use crate::kvs::Datastore;
	use crate::kvs::LockType;
	use crate::kvs::Transaction;
	use crate::kvs::TransactionType;
	use serial_test::serial;
	use temp_dir::TempDir;

	async fn new_ds(node_id: Uuid, clock_override: ClockType) -> (Datastore, Kvs) {
		let path = TempDir::new().unwrap().path().to_string_lossy().to_string();
		(
			Datastore::new_full(format!("rocksdb:{path}").as_str(), Some(clock_override))
				.await
				.unwrap()
				.with_node_id(sql::Uuid::from(node_id)),
			Kvs::Rocksdb,
		)
	}

	async fn new_tx(write: TransactionType, lock: LockType) -> Transaction {
		// Shared node id for one-off transactions
		// We should delete this, node IDs should be known.
		let new_tx_uuid = Uuid::parse_str("22358e5e-87bd-4040-8c63-01db896191ab").unwrap();
		let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
		new_ds(new_tx_uuid, clock).await.0.transaction(write, lock).await.unwrap()
	}

	include!("cluster_init.rs");
	include!("hb.rs");
	include!("helper.rs");
	include!("lq.rs");
	include!("nq.rs");
	include!("raw.rs");
	include!("snapshot.rs");
	include!("tb.rs");
	include!("multireader.rs");
	include!("multiwriter_different_keys.rs");
	include!("multiwriter_same_keys_conflict.rs");
	include!("timestamp_to_versionstamp.rs");
	include!("nd.rs");
	include!("ndlq.rs");
	include!("tblq.rs");
	include!("tbnt.rs");
}

#[cfg(feature = "kv-speedb")]
mod speedb {

	use crate::kvs::tests::{ClockType, Kvs};
	use crate::kvs::Transaction;
	use crate::kvs::{Datastore, LockType, TransactionType};
	use serial_test::serial;
	use temp_dir::TempDir;

	async fn new_ds(node_id: Uuid, clock_override: ClockType) -> (Datastore, Kvs) {
		let path = TempDir::new().unwrap().path().to_string_lossy().to_string();
		(
			Datastore::new_full(format!("speedb:{path}").as_str(), Some(clock_override))
				.await
				.unwrap()
				.with_node_id(sql::Uuid::from(node_id)),
			Kvs::Speedb,
		)
	}

	async fn new_tx(write: TransactionType, lock: LockType) -> Transaction {
		// Shared node id for one-off transactions
		// We should delete this, node IDs should be known.
		let new_tx_uuid = Uuid::parse_str("5877e580-12ac-49e4-95e1-3c407c4887f3").unwrap();
		let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
		new_ds(new_tx_uuid, clock).await.0.transaction(write, lock).await.unwrap()
	}

	include!("cluster_init.rs");
	include!("hb.rs");
	include!("helper.rs");
	include!("lq.rs");
	include!("nq.rs");
	include!("raw.rs");
	include!("snapshot.rs");
	include!("tb.rs");
	include!("multireader.rs");
	include!("multiwriter_different_keys.rs");
	include!("multiwriter_same_keys_conflict.rs");
	include!("timestamp_to_versionstamp.rs");
	include!("nd.rs");
	include!("ndlq.rs");
	include!("tblq.rs");
	include!("tbnt.rs");
}

#[cfg(feature = "kv-tikv")]
mod tikv {

	use crate::kvs::tests::{ClockType, Kvs};
	use crate::kvs::Transaction;
	use crate::kvs::{Datastore, LockType, TransactionType};
	use serial_test::serial;

	async fn new_ds(node_id: Uuid, clock_override: ClockType) -> (Datastore, Kvs) {
		let ds = Datastore::new_full("tikv:127.0.0.1:2379", Some(clock_override))
			.await
			.unwrap()
			.with_node_id(sql::uuid::Uuid(node_id));
		// Clear any previous test entries
		let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
		tx.delp(vec![], u32::MAX).await.unwrap();
		tx.commit().await.unwrap();
		// Return the datastore
		(ds, Kvs::Tikv)
	}

	async fn new_tx(write: TransactionType, lock: LockType) -> Transaction {
		// Shared node id for one-off transactions
		// We should delete this, node IDs should be known.
		let new_tx_uuid = Uuid::parse_str("18717a0f-0ab0-421e-b20c-e69fb03e90a3").unwrap();
		let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
		new_ds(new_tx_uuid, clock).await.0.transaction(write, lock).await.unwrap()
	}

	include!("cluster_init.rs");
	include!("hb.rs");
	include!("helper.rs");
	include!("lq.rs");
	include!("nq.rs");
	include!("raw.rs");
	include!("snapshot.rs");
	include!("tb.rs");
	include!("multireader.rs");
	include!("multiwriter_different_keys.rs");
	include!("multiwriter_same_keys_conflict.rs");
	include!("timestamp_to_versionstamp.rs");
	include!("nd.rs");
	include!("ndlq.rs");
	include!("tblq.rs");
	include!("tbnt.rs");
}

#[cfg(feature = "kv-fdb")]
mod fdb {

	use crate::kvs::tests::{ClockType, Kvs};
	use crate::kvs::Transaction;
	use crate::kvs::{Datastore, LockType, TransactionType};
	use serial_test::serial;

	async fn new_ds(node_id: Uuid, clock_override: ClockType) -> (Datastore, Kvs) {
		let ds = Datastore::new_full("fdb:/etc/foundationdb/fdb.cluster", Some(clock_override))
			.await
			.unwrap()
			.with_node_id(sql::Uuid::from(node_id));
		// Clear any previous test entries
		let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
		tx.delp(vec![], u32::MAX).await.unwrap();
		tx.commit().await.unwrap();
		// Return the datastore
		(ds, Kvs::Fdb)
	}

	async fn new_tx(write: TransactionType, lock: LockType) -> Transaction {
		// Shared node id for one-off transactions
		// We should delete this, node IDs should be known.
		let new_tx_uuid = Uuid::parse_str("50f5bdf5-8abe-406b-8002-a79c942f510f").unwrap();
		let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
		new_ds(new_tx_uuid, clock).await.0.transaction(write, lock).await.unwrap()
	}

	include!("cluster_init.rs");
	include!("hb.rs");
	include!("helper.rs");
	include!("lq.rs");
	include!("nq.rs");
	include!("raw.rs");
	include!("snapshot.rs");
	include!("tb.rs");
	include!("multireader.rs");
	include!("multiwriter_different_keys.rs");
	include!("multiwriter_same_keys_allow.rs");
	include!("timestamp_to_versionstamp.rs");
	include!("nd.rs");
	include!("ndlq.rs");
	include!("tblq.rs");
	include!("tbnt.rs");
}

#[cfg(feature = "kv-surrealkv")]
mod surrealkv {

	use crate::kvs::tests::{ClockType, Kvs};
	use crate::kvs::Datastore;
	use crate::kvs::LockType;
	use crate::kvs::Transaction;
	use crate::kvs::TransactionType;
	use serial_test::serial;
	use temp_dir::TempDir;

	async fn new_ds(node_id: Uuid, clock_override: ClockType) -> (Datastore, Kvs) {
		let path = TempDir::new().unwrap().path().to_string_lossy().to_string();
		(
			Datastore::new_full(format!("surrealkv:{path}").as_str(), Some(clock_override))
				.await
				.unwrap()
				.with_node_id(sql::Uuid::from(node_id)),
			Kvs::SurrealKV,
		)
	}

	async fn new_tx(write: TransactionType, lock: LockType) -> Transaction {
		// Shared node id for one-off transactions
		// We should delete this, node IDs should be known.
		let new_tx_uuid = Uuid::parse_str("22358e5e-87bd-4040-8c63-01db896191ab").unwrap();
		let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
		let (ds, _) = new_ds(new_tx_uuid, clock).await;
		ds.transaction(write, lock).await.unwrap()
	}

	include!("raw.rs");
	include!("cluster_init.rs");
	include!("hb.rs");
	include!("helper.rs");
	include!("lq.rs");
	include!("nq.rs");
	include!("snapshot.rs");
	include!("tb.rs");
	include!("multireader.rs");
	include!("multiwriter_different_keys.rs");
	// include!("multiwriter_same_keys_conflict.rs");
	include!("timestamp_to_versionstamp.rs");
	include!("nd.rs");
	include!("ndlq.rs");
	include!("tblq.rs");
	include!("tbnt.rs");
}
