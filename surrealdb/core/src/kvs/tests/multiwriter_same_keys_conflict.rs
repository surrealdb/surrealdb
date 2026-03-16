#![cfg(any(feature = "kv-mem", feature = "kv-rocksdb", feature = "kv-surrealkv",))]

use uuid::Uuid;

use super::CreateDs;
use crate::kvs::LockType::*;
use crate::kvs::TransactionType::*;

pub async fn multiwriter_same_keys_conflict(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("96ebbb5c-8040-497a-9459-838e4931aca7").unwrap();
	let (ds, _) = new_ds.create_ds(node_id).await;
	// Insert an initial key
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	tx.set(&"test", &"some text".as_bytes().to_vec(), None).await.unwrap();
	tx.commit().await.unwrap();
	// Create a writeable transaction
	let tx1 = ds.transaction(Write, Optimistic).await.unwrap();
	tx1.set(&"test", &"other text 1".as_bytes().to_vec(), None).await.unwrap();
	// Create a writeable transaction
	let tx2 = ds.transaction(Write, Optimistic).await.unwrap();
	tx2.set(&"test", &"other text 2".as_bytes().to_vec(), None).await.unwrap();
	// Create a writeable transaction
	let tx3 = ds.transaction(Write, Optimistic).await.unwrap();
	tx3.set(&"test", &"other text 3".as_bytes().to_vec(), None).await.unwrap();
	// Cancel both writeable transactions
	tx1.commit().await.unwrap();
	tx2.commit().await.unwrap_err();
	tx3.commit().await.unwrap_err();
	// Check that the key was updated ok
	let tx = ds.transaction(Read, Optimistic).await.unwrap();
	let val = tx.get(&"test", None).await.unwrap().unwrap();
	assert_eq!(val, b"other text 1");
	tx.cancel().await.unwrap();
	// Create a writeable transaction
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	tx.set(&"test", &"original text".as_bytes().to_vec(), None).await.unwrap();
	tx.commit().await.unwrap();
	// Check that the key was updated ok
	let tx = ds.transaction(Read, Optimistic).await.unwrap();
	let val = tx.get(&"test", None).await.unwrap().unwrap();
	assert_eq!(val, b"original text");
	tx.cancel().await.unwrap();
}

macro_rules! define_tests {
	($new_ds:ident) => {
		#[tokio::test]
		#[serial_test::serial]
		async fn multiwriter_same_keys_conflict() {
			super::multiwriter_same_keys_conflict::multiwriter_same_keys_conflict($new_ds).await;
		}
	};
}
pub(crate) use define_tests;
