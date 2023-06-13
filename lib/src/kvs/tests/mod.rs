mod tb;

#[cfg(feature = "kv-mem")]
#[cfg(test)]
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

	include!("raw.rs");
	include!("snapshot.rs");
	include!("multireader.rs");
}

#[cfg(feature = "kv-rocksdb")]
#[cfg(test)]
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

	include!("raw.rs");
	include!("snapshot.rs");
	include!("multireader.rs");
	include!("multiwriter.rs");
}

#[cfg(feature = "kv-speedb")]
#[cfg(test)]
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

	include!("raw.rs");
	include!("snapshot.rs");
	include!("multireader.rs");
	include!("multiwriter.rs");
}

#[cfg(feature = "kv-tikv")]
#[cfg(test)]
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

	include!("raw.rs");
	include!("snapshot.rs");
	include!("multireader.rs");
	include!("multiwriter.rs");
}

#[cfg(feature = "kv-fdb")]
#[cfg(test)]
mod fdb {

	use crate::kvs::Datastore;
	use crate::kvs::Transaction;
	use serial_test::serial;

	async fn new_ds() -> Datastore {
		let ds = Datastore::new("/etc/foundationdb/fdb.cluster").await.unwrap();
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

	include!("raw.rs");
	include!("snapshot.rs");
	include!("multireader.rs");
	include!("multiwriter.rs");
}
