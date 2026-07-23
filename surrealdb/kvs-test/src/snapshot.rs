//! Read-snapshot isolation: readers keep seeing the state from when their
//! transaction started, regardless of concurrent writers.

use surrealdb_kvs::TransactionType::*;

use crate::{TestBackend, kvs_test};

async fn snapshot(b: &TestBackend) {
	// Create a new datastore
	let ds = b.create_ds().await;
	// Insert an initial key
	let tx = ds.transaction(Write).await.unwrap();
	tx.set("test".as_bytes().into(), "some text".as_bytes().to_vec()).await.unwrap();
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let tx1 = ds.transaction(Read).await.unwrap();
	// Check that the key was inserted ok
	let val = tx1.get("test".as_bytes().into(), None).await.unwrap().unwrap();
	assert_eq!(val, b"some text");
	// Create a new writeable transaction
	let txw = ds.transaction(Write).await.unwrap();
	// Update the test key content
	txw.set("test".as_bytes().into(), "other text".as_bytes().to_vec()).await.unwrap();
	// Create a readonly transaction
	let tx2 = ds.transaction(Read).await.unwrap();
	let val = tx2.get("test".as_bytes().into(), None).await.unwrap().unwrap();
	assert_eq!(val, b"some text");
	// Create a readonly transaction
	let tx3 = ds.transaction(Read).await.unwrap();
	let val = tx3.get("test".as_bytes().into(), None).await.unwrap().unwrap();
	assert_eq!(val, b"some text");
	// Update the test key content
	txw.set("test".as_bytes().into(), "extra text".as_bytes().to_vec()).await.unwrap();
	// Check the key from the original transaction
	let val = tx1.get("test".as_bytes().into(), None).await.unwrap().unwrap();
	assert_eq!(val, b"some text");
	// Cancel both readonly transactions
	tx1.cancel().await.unwrap();
	tx2.cancel().await.unwrap();
	tx3.cancel().await.unwrap();
	// Commit the writable transaction
	txw.commit().await.unwrap();
	// Check that the key was updated ok
	let tx = ds.transaction(Read).await.unwrap();
	let val = tx.get("test".as_bytes().into(), None).await.unwrap().unwrap();
	assert_eq!(val, b"extra text");
	tx.cancel().await.unwrap();
}

kvs_test!(snapshot);

/// A transaction reads its own uncommitted writes through point reads.
async fn read_your_own_writes_get(b: &TestBackend) {
	let ds = b.create_ds().await;
	let tx = ds.transaction(Write).await.unwrap();
	tx.set(b"test".into(), b"mine".to_vec()).await.unwrap();
	assert_eq!(tx.get(b"test".into(), None).await.unwrap().as_deref(), Some(&b"mine"[..]));
	assert!(tx.exists(b"test".into(), None).await.unwrap());
	// Overwrites are visible too
	tx.set(b"test".into(), b"updated".to_vec()).await.unwrap();
	assert_eq!(tx.get(b"test".into(), None).await.unwrap().as_deref(), Some(&b"updated"[..]));
	// And deletes
	tx.del(b"test".into()).await.unwrap();
	assert!(tx.get(b"test".into(), None).await.unwrap().is_none());
	tx.cancel().await.unwrap();
}

kvs_test!(read_your_own_writes_get);

/// A transaction reads its own uncommitted writes through range reads.
async fn read_your_own_writes_scan(b: &TestBackend) {
	let ds = b.create_ds().await;
	// Committed baseline interleaved with in-tx writes
	let tx = ds.transaction(Write).await.unwrap();
	tx.set(b"k2".into(), b"committed".to_vec()).await.unwrap();
	tx.commit().await.unwrap();
	let tx = ds.transaction(Write).await.unwrap();
	tx.set(b"k1".into(), b"mine".to_vec()).await.unwrap();
	tx.set(b"k3".into(), b"mine".to_vec()).await.unwrap();
	let rng = surrealdb_kvs::KeyRange::from(&b"k"[..]..&b"l"[..]);
	let keys = tx.keys(rng.as_borrowed(), u32::MAX, 0, None).await.unwrap();
	assert_eq!(keys.keys, vec![b"k1".to_vec(), b"k2".to_vec(), b"k3".to_vec()]);
	let scan = tx.scan(rng.as_borrowed(), u32::MAX, 0, None).await.unwrap();
	assert_eq!(
		scan.values,
		vec![
			(b"k1".to_vec(), b"mine".to_vec()),
			(b"k2".to_vec(), b"committed".to_vec()),
			(b"k3".to_vec(), b"mine".to_vec()),
		]
	);
	assert_eq!(tx.count(rng, None).await.unwrap(), 3);
	tx.cancel().await.unwrap();
}

kvs_test!(read_your_own_writes_scan);

/// Documents that the current backends provide snapshot isolation, not
/// serializability: two transactions that each read the other's key and
/// write their own disjoint key both commit (write skew is permitted).
///
/// Backends that reject the anomaly are excluded and assert prevention in
/// their own tests: the enterprise distributed store (serializable) and
/// IndexedDB (OCC read-set validation fails any transaction whose read keys
/// were concurrently modified).
async fn write_skew_permitted(b: &TestBackend) {
	let ds = b.create_ds().await;
	let tx = ds.transaction(Write).await.unwrap();
	tx.set(b"x".into(), b"0".to_vec()).await.unwrap();
	tx.set(b"y".into(), b"0".to_vec()).await.unwrap();
	tx.commit().await.unwrap();
	// tx1 reads y, writes x; tx2 reads x, writes y
	let tx1 = ds.transaction(Write).await.unwrap();
	let tx2 = ds.transaction(Write).await.unwrap();
	assert_eq!(tx1.get(b"y".into(), None).await.unwrap().as_deref(), Some(&b"0"[..]));
	assert_eq!(tx2.get(b"x".into(), None).await.unwrap().as_deref(), Some(&b"0"[..]));
	tx1.set(b"x".into(), b"1".to_vec()).await.unwrap();
	tx2.set(b"y".into(), b"1".to_vec()).await.unwrap();
	tx1.commit().await.unwrap();
	tx2.commit().await.unwrap();
	// Both writes landed: the anomaly is permitted under SI
	let tx = ds.transaction(Read).await.unwrap();
	assert_eq!(tx.get(b"x".into(), None).await.unwrap().as_deref(), Some(&b"1"[..]));
	assert_eq!(tx.get(b"y".into(), None).await.unwrap().as_deref(), Some(&b"1"[..]));
	tx.cancel().await.unwrap();
}

kvs_test!(write_skew_permitted, except = [surrealds, indxdb]);
