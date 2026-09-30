#![cfg(feature = "kv-rocksdb")]

use uuid::Uuid;

use super::CreateDs;
use crate::kvs::LockType::*;
use crate::kvs::TransactionType::*;
use crate::kvs::is_retryable_transaction_conflict;

/// A `getu` read arms commit-time conflict detection: if another transaction
/// commits a write to the read key, the reading transaction's commit fails
/// with a retryable conflict, even though its own writes touch other keys.
pub async fn getu_write_conflict(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("5c7a1a36-3a91-4a53-9c3f-6f1a8a4f0d21").unwrap();
	let (ds, _) = new_ds.create_ds(node_id).await;
	// Insert an initial key
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	tx.set(&"test", &"some text".as_bytes().to_vec()).await.unwrap();
	tx.commit().await.unwrap();
	// Create a writeable transaction reading the key for update
	let tx1 = ds.transaction(Write, Optimistic).await.unwrap();
	assert!(tx1.shared_locked_reads());
	let val = tx1.getu(&"test").await.unwrap().unwrap();
	assert_eq!(val, b"some text");
	// A second transaction commits a write to the same key
	let tx2 = ds.transaction(Write, Optimistic).await.unwrap();
	tx2.set(&"test", &"other text".as_bytes().to_vec()).await.unwrap();
	tx2.commit().await.unwrap();
	// The first transaction writes a different key, but its commit must fail
	tx1.set(&"other", &"value".as_bytes().to_vec()).await.unwrap();
	let err = tx1.commit().await.unwrap_err();
	assert!(is_retryable_transaction_conflict(&err), "expected a retryable conflict, got: {err}");
	// Check that only the second transaction's write persisted
	let tx = ds.transaction(Read, Optimistic).await.unwrap();
	assert_eq!(tx.get(&"test", None).await.unwrap().unwrap(), b"other text");
	assert!(tx.get(&"other", None).await.unwrap().is_none());
	tx.cancel().await.unwrap();
}

/// Two overlapping transactions that both `getu` the same key, and each write
/// only a key of their own, both commit: a locked read is invalidated by a
/// committed write to its key, never by another transaction's locked read of
/// it. This is what `shared_locked_reads` declares.
pub async fn getu_shared_read_commits(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("0d3c7f52-8b1e-4c2a-9e57-2a6b4f9c1e08").unwrap();
	let (ds, _) = new_ds.create_ds(node_id).await;
	// Insert an initial key
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	tx.set(&"test", &"some text".as_bytes().to_vec()).await.unwrap();
	tx.commit().await.unwrap();
	// Two overlapping writeable transactions both read the key for update
	let tx1 = ds.transaction(Write, Optimistic).await.unwrap();
	let tx2 = ds.transaction(Write, Optimistic).await.unwrap();
	assert_eq!(tx1.getu(&"test").await.unwrap().unwrap(), b"some text");
	assert_eq!(tx2.getu(&"test").await.unwrap().unwrap(), b"some text");
	// Each writes a key of its own, and both commits succeed
	tx1.set(&"one", &"1".as_bytes().to_vec()).await.unwrap();
	tx2.set(&"two", &"2".as_bytes().to_vec()).await.unwrap();
	tx1.commit().await.unwrap();
	tx2.commit().await.unwrap();
	// Check that both transactions' writes persisted
	let tx = ds.transaction(Read, Optimistic).await.unwrap();
	assert_eq!(tx.get(&"one", None).await.unwrap().unwrap(), b"1");
	assert_eq!(tx.get(&"two", None).await.unwrap().unwrap(), b"2");
	tx.cancel().await.unwrap();
}

macro_rules! define_tests {
	($new_ds:ident) => {
		#[tokio::test]
		#[serial_test::serial]
		async fn getu_write_conflict() {
			super::locked_reads::getu_write_conflict($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn getu_shared_read_commits() {
			super::locked_reads::getu_shared_read_commits($new_ds).await;
		}
	};
}
pub(crate) use define_tests;
