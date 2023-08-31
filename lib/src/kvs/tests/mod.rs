#[cfg(feature = "kv-mem")]
mod mem {

	use crate::kvs::Datastore;
	use crate::kvs::Transaction;
	use serial_test::serial;

	async fn new_ds(node_id: Uuid) -> Datastore {
		Datastore::new_full("memory", sql::Uuid::from(node_id), None).await.unwrap()
	}

	async fn new_tx(write: bool, lock: bool) -> Transaction {
		// Shared node id for one-off transactions
		// We should delete this, node IDs should be known.
		let new_tx_uuid = Uuid::parse_str("361893b5-a041-40c0-996c-c3a8828ef06b").unwrap();
		new_ds(new_tx_uuid).await.transaction(write, lock).await.unwrap()
	}

	include!("cluster_init.rs");
	include!("helper.rs");
	include!("lq.rs");
	include!("nq.rs");
	include!("raw.rs");
	include!("snapshot.rs");
	include!("tb.rs");
	include!("multireader.rs");
	include!("timestamp_to_versionstamp.rs");
}

#[cfg(feature = "kv-rocksdb")]
mod rocksdb {

	use crate::kvs::Datastore;
	use crate::kvs::Transaction;
	use serial_test::serial;
	use temp_dir::TempDir;

	async fn new_ds(node_id: Uuid) -> Datastore {
		let path = TempDir::new().unwrap().path().to_string_lossy().to_string();
		Datastore::new_full(format!("rocksdb:{path}").as_str(), sql::Uuid::from(node_id), None)
			.await
			.unwrap()
	}

	async fn new_tx(write: bool, lock: bool) -> Transaction {
		// Shared node id for one-off transactions
		// We should delete this, node IDs should be known.
		let new_tx_uuid = Uuid::parse_str("22358e5e-87bd-4040-8c63-01db896191ab").unwrap();
		new_ds(new_tx_uuid).await.transaction(write, lock).await.unwrap()
	}

	include!("cluster_init.rs");
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
}

#[cfg(feature = "kv-speedb")]
mod speedb {

	use crate::kvs::Datastore;
	use crate::kvs::Transaction;
	use serial_test::serial;
	use temp_dir::TempDir;

	async fn new_ds(node_id: Uuid) -> Datastore {
		let path = TempDir::new().unwrap().path().to_string_lossy().to_string();
		Datastore::new_full(format!("speedb:{path}").as_str(), sql::Uuid::from(node_id), None)
			.await
			.unwrap()
	}

	async fn new_tx(write: bool, lock: bool) -> Transaction {
		// Shared node id for one-off transactions
		// We should delete this, node IDs should be known.
		let new_tx_uuid = Uuid::parse_str("5877e580-12ac-49e4-95e1-3c407c4887f3").unwrap();
		new_ds(new_tx_uuid).await.transaction(write, lock).await.unwrap()
	}

	include!("cluster_init.rs");
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
}

#[cfg(feature = "kv-tikv")]
mod tikv {

	use crate::kvs::Datastore;
	use crate::kvs::Transaction;
	use serial_test::serial;

	async fn new_ds(node_id: Uuid) -> Datastore {
		let ds = Datastore::new_full("tikv:127.0.0.1:2379", sql::Uuid::from(node_id), None)
			.await
			.unwrap();
		// Clear any previous test entries
		let mut tx = ds.transaction(true, false).await.unwrap();
		tx.delp(vec![], u32::MAX).await.unwrap();
		tx.commit().await.unwrap();
		// Return the datastore
		ds
	}

	async fn new_tx(write: bool, lock: bool) -> Transaction {
		// Shared node id for one-off transactions
		// We should delete this, node IDs should be known.
		let new_tx_uuid = Uuid::parse_str("18717a0f-0ab0-421e-b20c-e69fb03e90a3").unwrap();
		new_ds(new_tx_uuid).await.transaction(write, lock).await.unwrap()
	}

	include!("cluster_init.rs");
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
}

#[cfg(feature = "kv-fdb")]
mod fdb {

	use crate::kvs::Datastore;
	use crate::kvs::Transaction;
	use serial_test::serial;

	async fn new_ds(node_id: Uuid) -> Datastore {
		let ds = Datastore::new_full(
			"fdb:/etc/foundationdb/fdb.cluster",
			sql::Uuid::from(node_id),
			None,
		)
		.await
		.unwrap();
		// Clear any previous test entries
		let mut tx = ds.transaction(true, false).await.unwrap();
		tx.delp(vec![], u32::MAX).await.unwrap();
		tx.commit().await.unwrap();
		// Return the datastore
		ds
	}

	async fn new_tx(write: bool, lock: bool) -> Transaction {
		// Shared node id for one-off transactions
		// We should delete this, node IDs should be known.
		let new_tx_uuid = Uuid::parse_str("50f5bdf5-8abe-406b-8002-a79c942f510f").unwrap();
		new_ds(new_tx_uuid).await.transaction(write, lock).await.unwrap()
	}

	include!("cluster_init.rs");
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
}
