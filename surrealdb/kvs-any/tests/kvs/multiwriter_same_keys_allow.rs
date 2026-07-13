#![cfg(feature = "kv-tikv")]

use surrealdb_kvs::TransactionType::*;

use super::CreateDs;

pub async fn multiwriter_same_keys_allow(new_ds: impl CreateDs) {
	// Create a new datastore
	let ds = new_ds.create_ds().await;
	// Insert an initial key
	let tx = ds.transaction(Write).await.unwrap();
	tx.set("test".as_bytes().into(), "some text".as_bytes().to_vec()).await.unwrap();
	tx.commit().await.unwrap();
	// Create a writeable transaction
	let tx1 = ds.transaction(Write).await.unwrap();
	tx1.set("test".as_bytes().into(), "other text 1".as_bytes().to_vec()).await.unwrap();
	// Create a writeable transaction
	let tx2 = ds.transaction(Write).await.unwrap();
	tx2.set("test".as_bytes().into(), "other text 2".as_bytes().to_vec()).await.unwrap();
	// Create a writeable transaction
	let tx3 = ds.transaction(Write).await.unwrap();
	tx3.set("test".as_bytes().into(), "other text 3".as_bytes().to_vec()).await.unwrap();
	// Cancel both writeable transactions
	tx1.commit().await.unwrap();
	tx2.commit().await.unwrap();
	tx3.commit().await.unwrap();
	// Check that the key was updated ok
	let tx = ds.transaction(Read).await.unwrap();
	let val = tx.get("test".as_bytes().into(), None).await.unwrap().unwrap();
	assert_eq!(val, b"other text 3");
	tx.cancel().await.unwrap();
	// Create a writeable transaction
	let tx = ds.transaction(Write).await.unwrap();
	tx.set("test".as_bytes().into(), "original text".as_bytes().to_vec()).await.unwrap();
	tx.commit().await.unwrap();
	// Check that the key was updated ok
	let tx = ds.transaction(Read).await.unwrap();
	let val = tx.get("test".as_bytes().into(), None).await.unwrap().unwrap();
	assert_eq!(val, b"original text");
	tx.cancel().await.unwrap();
}

macro_rules! define_tests {
	($new_ds:ident) => {
		#[tokio::test]
		#[serial_test::serial]
		async fn multiwriter_same_keys_allow() {
			super::multiwriter_same_keys_allow::multiwriter_same_keys_allow($new_ds).await;
		}
	};
}
pub(crate) use define_tests;
