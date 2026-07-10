#![cfg(any(
	feature = "kv-mem",
	feature = "kv-rocksdb",
	feature = "kv-surrealkv",
	feature = "kv-tikv",
))]

use surrealdb_kvs::TransactionType::*;

use super::CreateDs;
use crate::LockType;

/// A conditional create (`putc` with no expected value) is atomic on **every**
/// backend — including last-writer-wins TiKV, where a blind `set` is not (see
/// `multiwriter_same_keys_allow`). Two overlapping transactions that both
/// create the same absent key: both pass the in-snapshot condition check, but
/// only the first committer wins; the second is rejected. This is the invariant
/// the id-allocation and doc-ID get-or-create paths rely on to stay unique on
/// last-writer-wins backends.
pub async fn multiwriter_same_keys_putc(new_ds: impl CreateDs) {
	// Create a new datastore
	let ds = new_ds.create_ds().await;
	// The key must not exist yet, so both writers see the create condition met.
	// Two overlapping writers both conditionally create the same key.
	let tx1 = ds.transaction(Write, LockType::Optimistic).await.unwrap();
	tx1.putc("test".as_bytes().into(), "first".as_bytes().to_vec(), None).await.unwrap();
	let tx2 = ds.transaction(Write, LockType::Optimistic).await.unwrap();
	tx2.putc("test".as_bytes().into(), "second".as_bytes().to_vec(), None).await.unwrap();
	// The first commit wins; the second is rejected (last-writer-wins is NOT
	// allowed to let it through, unlike a blind `set`).
	tx1.commit().await.unwrap();
	tx2.commit().await.unwrap_err();
	// The winner's value is the one that persisted.
	let tx = ds.transaction(Read, LockType::Optimistic).await.unwrap();
	let val = tx.get("test".as_bytes().into(), None).await.unwrap().unwrap();
	assert_eq!(val, b"first");
	tx.cancel().await.unwrap();
}

macro_rules! define_tests {
	($new_ds:ident) => {
		#[tokio::test]
		#[serial_test::serial]
		async fn multiwriter_same_keys_putc() {
			super::multiwriter_same_keys_putc::multiwriter_same_keys_putc($new_ds).await;
		}
	};
}
pub(crate) use define_tests;
