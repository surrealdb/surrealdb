#[cfg(feature = "kv-mem")]
mod mem {

	use crate::kvs::Datastore;
	use crate::kvs::Transaction;
	use serial_test::serial;

	async fn new_ds() -> Datastore {
		Datastore::new("memory").await.unwrap()
	}

	async fn new_tx(write: bool, lock: bool) -> Transaction {
		new_ds().await.transaction(write, lock).await.unwrap()
	}

	include!("helper.rs");
	include!("cluster_init.rs");
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

	async fn new_ds() -> Datastore {
		let path = TempDir::new().unwrap().path().to_string_lossy().to_string();
		Datastore::new(format!("rocksdb:{path}").as_str()).await.unwrap()
	}

	async fn new_tx(write: bool, lock: bool) -> Transaction {
		new_ds().await.transaction(write, lock).await.unwrap()
	}

	include!("helper.rs");
	include!("cluster_init.rs");
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

	async fn new_ds() -> Datastore {
		let path = TempDir::new().unwrap().path().to_string_lossy().to_string();
		Datastore::new(format!("speedb:{path}").as_str()).await.unwrap()
	}

	async fn new_tx(write: bool, lock: bool) -> Transaction {
		new_ds().await.transaction(write, lock).await.unwrap()
	}

	include!("helper.rs");
	include!("cluster_init.rs");
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

	async fn new_ds() -> Datastore {
		let ds = Datastore::new("tikv:127.0.0.1:2379").await.unwrap();
		// Clear any previous test entries
		let mut tx = ds.transaction(true, false).await.unwrap();
		assert!(tx.delp(vec![], u32::MAX).await.is_ok());
		tx.commit().await.unwrap();
		// Return the datastore
		ds
	}

	async fn new_tx(write: bool, lock: bool) -> Transaction {
		new_ds().await.transaction(write, lock).await.unwrap()
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

	async fn new_ds() -> Datastore {
		let ds = Datastore::new("fdb:/etc/foundationdb/fdb.cluster").await.unwrap();
		// Clear any previous test entries
		let mut tx = ds.transaction(true, false).await.unwrap();
		assert!(tx.delp(vec![], u32::MAX).await.is_ok());
		tx.commit().await.unwrap();
		// Return the datastore
		ds
	}

	async fn new_tx(write: bool, lock: bool) -> Transaction {
		new_ds().await.transaction(write, lock).await.unwrap()
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
