//! Concurrent transactions: multiple readers, non-conflicting writers, and
//! the two same-key write models (first-committer-wins conflict detection
//! vs. TiKV's optimistic last-writer-wins).

use surrealdb_kvs::TransactionType::*;

use crate::{TestBackend, kvs_test};

async fn multireader(b: &TestBackend) {
	// Create a new datastore
	let ds = b.create_ds().await;
	// Insert an initial key
	let tx = ds.transaction(Write).await.unwrap();
	tx.set("test".as_bytes().into(), "some text".as_bytes().to_vec()).await.unwrap();
	tx.commit().await.unwrap();
	// Create a readonly transaction
	let tx1 = ds.transaction(Read).await.unwrap();
	let val = tx1.get(b"test".into(), None).await.unwrap().unwrap();
	assert_eq!(val, b"some text");
	// Create a readonly transaction
	let tx2 = ds.transaction(Read).await.unwrap();
	let val = tx2.get(b"test".into(), None).await.unwrap().unwrap();
	assert_eq!(val, b"some text");
	// Create a readonly transaction
	let tx3 = ds.transaction(Read).await.unwrap();
	let val = tx3.get(b"test".into(), None).await.unwrap().unwrap();
	assert_eq!(val, b"some text");
	// Cancel both readonly transactions
	tx1.cancel().await.unwrap();
	tx2.cancel().await.unwrap();
	tx3.cancel().await.unwrap();
}

kvs_test!(multireader);

async fn multiwriter_different_keys(b: &TestBackend) {
	// Create a new datastore
	let ds = b.create_ds().await;
	// Insert an initial key
	let tx = ds.transaction(Write).await.unwrap();
	tx.set("test".as_bytes().into(), "some text".as_bytes().to_vec()).await.unwrap();
	tx.commit().await.unwrap();
	// Create a writeable transaction
	let tx1 = ds.transaction(Write).await.unwrap();
	tx1.set(b"test1".into(), "other text 1".as_bytes().to_vec()).await.unwrap();
	// Create a writeable transaction
	let tx2 = ds.transaction(Write).await.unwrap();
	tx2.set(b"test2".into(), "other text 2".as_bytes().to_vec()).await.unwrap();
	// Create a writeable transaction
	let tx3 = ds.transaction(Write).await.unwrap();
	tx3.set(b"test3".into(), "other text 3".as_bytes().to_vec()).await.unwrap();

	// Cancel both writeable transactions
	tx1.commit().await.unwrap();
	tx2.commit().await.unwrap();
	tx3.commit().await.unwrap();
	// Check that the key was updated ok
	let tx = ds.transaction(Read).await.unwrap();
	let val = tx.get("test".as_bytes().into(), None).await.unwrap().unwrap();
	assert_eq!(val, b"some text");
	let val = tx.get("test1".as_bytes().into(), None).await.unwrap().unwrap();
	assert_eq!(val, b"other text 1");
	let val = tx.get("test2".as_bytes().into(), None).await.unwrap().unwrap();
	assert_eq!(val, b"other text 2");
	let val = tx.get("test3".as_bytes().into(), None).await.unwrap().unwrap();
	assert_eq!(val, b"other text 3");
	tx.cancel().await.unwrap();
}

kvs_test!(multiwriter_different_keys);

/// On backends with write-write conflict detection, the first committer wins
/// and every later same-key committer fails.
async fn multiwriter_same_keys_conflict(b: &TestBackend) {
	// Create a new datastore
	let ds = b.create_ds().await;
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
	tx2.commit().await.unwrap_err();
	tx3.commit().await.unwrap_err();
	// Check that the key was updated ok
	let tx = ds.transaction(Read).await.unwrap();
	let val = tx.get("test".as_bytes().into(), None).await.unwrap().unwrap();
	assert_eq!(val, b"other text 1");
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

kvs_test!(multiwriter_same_keys_conflict, except = [tikv, indxdb, surrealds]);

/// Last-writer-wins backends allow overlapping same-key writers: every commit
/// succeeds and the last committer's value wins. This holds for TiKV's
/// optimistic model, for IndexedDB, whose OCC validation only inspects the
/// read set — blind writes never conflict — and for the TAPIR-based
/// distributed store, whose write-set validation retries a blind write at a
/// higher timestamp instead of aborting it, so overlapping blind writes
/// serialize in timestamp order.
async fn multiwriter_same_keys_allow(b: &TestBackend) {
	// Create a new datastore
	let ds = b.create_ds().await;
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

kvs_test!(multiwriter_same_keys_allow, only = [tikv, indxdb, surrealds]);

/// A conditional create (`putc` with no expected value) is atomic on **every**
/// backend — including last-writer-wins TiKV, where a blind `set` is not (see
/// `multiwriter_same_keys_allow`). Two overlapping transactions that both
/// create the same absent key: both pass the in-snapshot condition check, but
/// only the first committer wins; the second is rejected. This is the invariant
/// the id-allocation and doc-ID get-or-create paths rely on to stay unique on
/// last-writer-wins backends.
async fn multiwriter_same_keys_putc(b: &TestBackend) {
	// Create a new datastore
	let ds = b.create_ds().await;
	// The key must not exist yet, so both writers see the create condition met.
	// Two overlapping writers both conditionally create the same key.
	let tx1 = ds.transaction(Write).await.unwrap();
	tx1.putc("test".as_bytes().into(), "first".as_bytes().to_vec(), None).await.unwrap();
	let tx2 = ds.transaction(Write).await.unwrap();
	tx2.putc("test".as_bytes().into(), "second".as_bytes().to_vec(), None).await.unwrap();
	// The first commit wins; the second is rejected (last-writer-wins is NOT
	// allowed to let it through, unlike a blind `set`).
	tx1.commit().await.unwrap();
	tx2.commit().await.unwrap_err();
	// The winner's value is the one that persisted.
	let tx = ds.transaction(Read).await.unwrap();
	let val = tx.get("test".as_bytes().into(), None).await.unwrap().unwrap();
	assert_eq!(val, b"first");
	tx.cancel().await.unwrap();
}

kvs_test!(multiwriter_same_keys_putc);
